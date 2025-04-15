use std::{
    collections::VecDeque,
    future::Future,
    ops::ControlFlow,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use sqlx_core::{
    bytes::Buf as _,
    net::{BufferedSocket, Socket},
    rt::spawn,
};

use crate::message::{BackendMessageFormat, FrontendMessage, ReceivedMessage, Terminate};

use super::PgStream;

mod message;
mod pipe;

pub use message::{IoRequest, MessageBuf, PipeUntil};
pub use pipe::Pipe;

#[derive(PartialEq)]
enum WorkerState {
    // The connection is open and ready for business.
    Open,
    // Sent/sending a [Terminate] message but did not close the socket. Responding to the last
    // messages but not receiving new ones.
    Closing,
    // The connection is terminated, this step closes the socket and stops the background task.
    Closed,
}

pub struct Worker {
    state: WorkerState,
    should_flush: bool,
    chan: UnboundedReceiver<IoRequest>,
    // Holds the requests from the `PgConnection` that still need a response.
    back_log: VecDeque<IoRequest>,
    socket: BufferedSocket<Box<dyn Socket>>,
    // A channel that is used to send asynchronous notifications back to the `PgConnection` struct.
    notif_chan: UnboundedSender<ReceivedMessage>,
    // Buffered responses that we couldn't handle
    buffered_msg: VecDeque<ReceivedMessage>,
}

impl Worker {
    pub fn spawn(
        stream: PgStream,
        notif_chan: UnboundedSender<ReceivedMessage>,
    ) -> UnboundedSender<IoRequest> {
        let (tx, rx) = unbounded();

        let conn = Worker {
            state: WorkerState::Open,
            should_flush: false,
            chan: rx,
            back_log: VecDeque::new(),
            socket: stream.inner,
            notif_chan,
            buffered_msg: VecDeque::new(),
        };

        spawn(conn);
        tx
    }

    // Tries to receive the next message from the channel. Also handles termination if needed.
    #[inline(always)]
    fn poll_next_request(&mut self, cx: &mut Context<'_>) -> Option<IoRequest> {
        // We only try to receive new messages if we are open.
        if self.state != WorkerState::Open {
            return None;
        }

        match self.chan.poll_next_unpin(cx) {
            Poll::Pending => None,
            Poll::Ready(Some(request)) => Some(request),
            Poll::Ready(None) => {
                // Channel was closed, explicitly or because the sender was dropped. Either way
                // we should start a gracefull shutdown.
                self.socket
                    .write_raw(&[Terminate::FORMAT as u8, 0, 0, 0, 4])
                    .expect("Terminate message should fit in buffer");

                self.state = WorkerState::Closing;
                self.should_flush = true;
                None
            }
        }
    }

    #[inline(always)]
    fn poll_receiver(&mut self, cx: &mut Context<'_>) -> sqlx_core::Result<()> {
        // Try to receive a new io request from the channel.
        while let Some(msg) = self.poll_next_request(cx) {
            // Write the received message in the write buffer.
            //
            // TODO: This could grow the write buffer quite big when pipelining queries.
            // There should be a mechanism that shrinks the buffer once in a while.
            self.socket.write_raw(&msg.data)?;

            // Ensure we flush the write buffer.
            self.should_flush = true;

            // Only wait for a response if we're expecting one.
            if !matches!(msg.pipe_until, PipeUntil::None) {
                self.back_log.push_back(msg);
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn handle_poll_flush(&mut self, cx: &mut Context<'_>) -> sqlx_core::Result<()> {
        if self.should_flush {
            if let Poll::Ready(_) = self.socket.poll_flush(cx) {
                self.should_flush = false;
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn handle_response_and_done(&mut self, msg: &IoRequest, response: ReceivedMessage) -> bool {
        let format = response.format;

        // Filter out notifications.
        if format == BackendMessageFormat::NotificationResponse {
            let _ = self.notif_chan.unbounded_send(response);
            // We're only ready here if we actually want a [NotificationResponse].
            return matches!(msg.pipe_until, PipeUntil::Notification);
        }

        match msg.pipe_until {
            PipeUntil::None => unreachable!("We should never send a response back."),
            PipeUntil::ReadyForQuery => {
                let _ = msg.chan.unbounded_send(response);

                format == BackendMessageFormat::ReadyForQuery
            }
            PipeUntil::ReadyForQueryOrCopyIn => {
                let _ = msg.chan.unbounded_send(response);

                format == BackendMessageFormat::ReadyForQuery
                    || format == BackendMessageFormat::CopyInResponse
            }
            PipeUntil::Notification => {
                // All the notifications are already filtered out, we only get other responses that
                // we don't want. Make sure we store them in the buffer so we don't lose them.
                let _ = self.buffered_msg.push_back(response);

                // When the channel is closed it means the `Future` of `PgListener::try_next` (or
                // `PgListener::try_next`) has been dropped, we shouldn't try to receive a
                // notification again.
                msg.chan.is_closed()
            }
        }
    }

    #[inline(always)]
    fn poll_backlog(&mut self, cx: &mut Context<'_>) -> Poll<sqlx_core::Result<()>> {
        while let Some(msg) = self.back_log.pop_front() {
            loop {
                // Try and get a response, these could be buffered or _fresh_.
                let response = match self.poll_next_message(cx) {
                    Poll::Ready(response) => response?,
                    Poll::Pending => {
                        // We don't want to keep trying to receive a notification if the connection
                        // doesn't want any.
                        if !(matches!(msg.pipe_until, PipeUntil::Notification)
                            && msg.chan.is_closed())
                        {
                            // Not ready for receiving messages. Push the message to the front so this
                            // is the first request on the next poll.
                            self.back_log.push_front(msg);
                        }
                        return Poll::Pending;
                    }
                };

                if self.handle_response_and_done(&msg, response) {
                    // We're done handling this message, move on to the next.
                    break;
                }
            }
        }

        // Once the backlog is done and we sent out all the responses, were closing the socket.
        if self.state == WorkerState::Closing && self.back_log.is_empty() {
            self.state = WorkerState::Closed;
            return self.socket.poll_shutdown(cx).map_err(Into::into);
        }
        Poll::Pending
    }

    #[inline(always)]
    pub fn poll_next_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<sqlx_core::Result<ReceivedMessage>> {
        // Try and get a buffered message first.
        if let Some(buffered) = self.buffered_msg.pop_front() {
            return Poll::Ready(Ok(buffered));
        }

        self.socket.poll_try_read(cx, |buf| {
            // all packets in postgres start with a 5-byte header
            // this header contains the message type and the total length of the message
            let Some(mut header) = buf.get(..5) else {
                return Ok(ControlFlow::Continue(5));
            };

            let format = BackendMessageFormat::try_from_u8(header.get_u8())?;

            let message_len = header.get_u32() as usize;

            let expected_len = message_len
                .checked_add(1)
                // this shouldn't really happen but is mostly a sanity check
                .ok_or_else(|| err_protocol!("message_len + 1 overflows usize: {message_len}"))?;

            if buf.len() < expected_len {
                return Ok(ControlFlow::Continue(expected_len));
            }

            // `buf` SHOULD NOT be modified ABOVE this line

            // pop off the format code since it's not counted in `message_len`
            buf.advance(1);

            // consume the message, including the length prefix
            let mut contents = buf.split_to(message_len).freeze();

            // cut off the length prefix
            contents.advance(4);

            Ok(ControlFlow::Break(ReceivedMessage { format, contents }))
        })
    }

    #[inline(always)]
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if self.state == WorkerState::Closed {
            // The buffer is closed, a [Terminate] message has been sent, now try and close the socket.
            self.socket.poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl Future for Worker {
    type Output = sqlx_core::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Only poll the future if we're not closed/closing.
        ready!(self.poll_shutdown(cx)?);

        // Push as many new requests in the write buffer as we can.
        self.poll_receiver(cx)?;

        // Flush the write buffer if needed.
        self.handle_poll_flush(cx)?;

        // Try to receive responses from the database and handle them.
        self.poll_backlog(cx)
    }
}
