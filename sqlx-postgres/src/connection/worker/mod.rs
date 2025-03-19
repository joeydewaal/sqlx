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

mod manager;
mod message;

pub use manager::ConnManager;
pub use message::{IoRequest, MessageBuf, PipeUntil};

#[derive(PartialEq)]
enum WorkerState {
    // The connection is open and in business.
    Open,
    // Responding to the last messages but not receiving new ones.
    // Sent a terminate message and closing the socket.
    Closing,
    // Connection is fully closed.
    Closed,
}

pub struct Worker {
    state: WorkerState,
    should_flush: bool,
    chan: UnboundedReceiver<IoRequest>,
    back_log: VecDeque<IoRequest>,
    conn: BufferedSocket<Box<dyn Socket>>,
}

impl Worker {
    pub fn spawn(stream: PgStream) -> UnboundedSender<IoRequest> {
        let (tx, rx) = unbounded();

        let conn = Worker {
            state: WorkerState::Open,
            should_flush: false,
            chan: rx,
            back_log: VecDeque::new(),
            conn: stream.inner,
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
                // Channel was closed, explicitly or because the connection was dropped. Either way
                // we should start a gracefull shutdown.
                self.conn
                    .write_raw(&[Terminate::FORMAT as u8, 0, 0, 0, 4])
                    .unwrap();
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
            self.conn.write_raw(&msg.data)?;

            // Ensure we flush the write buffer.
            self.should_flush = true;

            // Only put the message in the write buffer if we need to respond.
            if !matches!(msg.send_until, PipeUntil::NumResponses(0)) {
                self.back_log.push_back(msg);
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn handle_poll_flush(&mut self, cx: &mut Context<'_>) -> sqlx_core::Result<()> {
        if self.should_flush {
            if let Poll::Ready(_) = self.conn.poll_flush(cx) {
                self.should_flush = false;
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn poll_backlog(&mut self, cx: &mut Context<'_>) -> Poll<sqlx_core::Result<()>> {
        while let Some(mut msg) = self.back_log.pop_front() {
            loop {
                let response = match self.poll_next_message(cx) {
                    Poll::Ready(response) => response?,
                    Poll::Pending => {
                        // Not ready for receiving messages.
                        // Push the message to the front so this is the first message on the next
                        // poll.
                        self.back_log.push_front(msg);
                        return Poll::Pending;
                    }
                };

                let format = response.format;

                // Send the response back to the sender. We ignore the error because even if the
                // sender is closed we should keep receiving responses from the database.
                let _ = msg.chan.unbounded_send(response).is_err();

                // See if we should keep sending responses back.
                if msg.handle_done(format) {
                    // Remove the received message and move on to the next one.
                    drop(msg);
                    break;
                }
            }
        }

        // Once the backlog is done and we sent out all the responses, were closing the socket.
        if self.state == WorkerState::Closing && self.back_log.is_empty() {
            self.state = WorkerState::Closed;
            return self.conn.poll_shutdown(cx).map_err(Into::into);
        }
        Poll::Pending
    }

    #[inline(always)]
    pub fn poll_next_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<sqlx_core::Result<ReceivedMessage>> {
        self.conn.poll_try_read(cx, |buf| {
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
            self.conn.poll_shutdown(cx)
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

        // Push as many new messages in the write buffer as we can.
        self.poll_receiver(cx)?;

        // Flush the write buffer if needed.
        self.handle_poll_flush(cx)?;

        // Try to receive responses from the database and send them back.
        self.poll_backlog(cx)
    }
}
