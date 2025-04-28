use std::{
    collections::VecDeque,
    future::Future,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use sqlx_core::{
    bytes::{Buf as _, BytesMut},
    net::{Framed, Socket},
    rt::spawn,
};
use tracing::{info, trace};

use crate::message::{
    BackendMessageFormat, BatchReceivedMessage, FrontendMessage, Notification, ReceivedMessage,
    Terminate,
};

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
    socket: Framed<Box<dyn Socket>>,
    // A channel that is used to send asynchronous notifications back to the `PgConnection` struct.
    notif_chan: UnboundedSender<Notification>,
    // Buffered responses that we couldn't handle
    buffered_msg: VecDeque<ReceivedMessage>,
    currently_handling: Option<IoRequest>,
}

impl Worker {
    pub fn spawn(
        stream: PgStream,
        notif_chan: UnboundedSender<Notification>,
    ) -> UnboundedSender<IoRequest> {
        let (tx, rx) = unbounded();

        let conn = Worker {
            state: WorkerState::Open,
            should_flush: false,
            chan: rx,
            back_log: VecDeque::new(),
            socket: Framed::new(stream.inner),
            notif_chan,
            buffered_msg: VecDeque::new(),
            currently_handling: None,
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

        trace!("polling next request");
        match self.chan.poll_next_unpin(cx) {
            Poll::Pending => {
                trace!("waiting for request...");
                None
            }
            Poll::Ready(Some(request)) => {
                trace!("got request");
                Some(request)
            }
            Poll::Ready(None) => {
                // Channel was closed, explicitly or because the sender was dropped. Either way
                // we should start a gracefull shutdown.
                self.socket
                    .write_raw(&[Terminate::FORMAT as u8, 0, 0, 0, 4]);

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
            info!("got new request");
            // Write the received message in the write buffer.
            //
            // TODO: This could grow the write buffer quite big when pipelining queries.
            // There should be a mechanism that shrinks the buffer once in a while.
            self.socket.write_raw(&msg.data);

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
            trace!("poll_flush");
            if let Poll::Ready(_) = self.socket.poll_flush(cx) {
                trace!("ready flushing");
                self.should_flush = false;
            } else {
                trace!("not ready flushing");
            }
        }
        Ok(())
    }

    fn send_back(&mut self, response: BatchReceivedMessage) {
        let handling = if let Some(cur) = &mut self.currently_handling {
            cur
        } else if let Some(cur) = self.back_log.pop_front() {
            self.currently_handling.insert(cur)
        } else {
            // self.buffered_msg.push_back(response);
            return;
        };

        info!("sending response back");
        let _ = handling.chan.unbounded_send(response);
    }

    #[inline(always)]
    fn poll_backlog(&mut self, cx: &mut Context<'_>) -> sqlx_core::Result<()> {
        trace!("polling backlog");

        // Try and get a response from db.
        while let Poll::Ready(mut response) = self.poll_next_message(cx)? {
            if response.is_notif() {
                trace!("got notif");
                let notif: Notification = response.next()?.unwrap().decode()?;
                let _ = self.notif_chan.unbounded_send(notif);
                continue;
            }

            let is_done = response.is_done();
            self.send_back(response);

            if is_done {
                if let Some(next_request) = self.back_log.pop_front() {
                    trace!("we're pipelinging");
                    self.currently_handling = Some(next_request);
                } else {
                    self.currently_handling = None;
                    trace!("done handling request");
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn get_next_messages(
        buf: &mut BytesMut,
    ) -> sqlx_core::Result<ControlFlow<BatchReceivedMessage, usize>> {
        let mut index = 0;
        let mut start = 0;

        loop {
            // all packets in postgres start with a 5-byte header
            // this header contains the message type and the total length of the message
            let Some(mut header) = buf.get(start..(start + 5)) else {
                if index == 0 {
                    return Ok(ControlFlow::Continue(5));
                } else {
                    info!("Not done: {index:?}");
                    return Ok(ControlFlow::Break(BatchReceivedMessage::not_done(
                        buf.split_to(start),
                    )));
                }
            };

            let format = BackendMessageFormat::try_from_u8(header.get_u8())?;

            let message_len = header.get_u32() as usize;

            let expected_len = message_len
                .checked_add(1)
                // this shouldn't really happen but is mostly a sanity check
                .ok_or_else(|| err_protocol!("message_len + 1 overflows usize: {message_len}"))?
                + start;

            if buf.len() < expected_len {
                if index == 0 {
                    return Ok(ControlFlow::Continue(expected_len));
                } else {
                    return Ok(ControlFlow::Break(BatchReceivedMessage::not_done(
                        buf.split_to(start),
                    )));
                }
            }

            match format {
                BackendMessageFormat::ReadyForQuery | BackendMessageFormat::CopyInResponse => {
                    return Ok(ControlFlow::Break(BatchReceivedMessage::done(
                        buf.split_to(expected_len),
                    )));
                }
                BackendMessageFormat::NotificationResponse => {
                    if index == 0 {
                        return Ok(ControlFlow::Break(BatchReceivedMessage::notification(
                            buf.split_to(expected_len),
                        )));
                    } else {
                        return Ok(ControlFlow::Break(BatchReceivedMessage::not_done(
                            buf.split_to(start),
                        )));
                    }
                }
                _ => {
                    index += 1;
                }
            }

            start = expected_len;
        }
    }

    #[inline(always)]
    pub fn poll_next_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<sqlx_core::Result<BatchReceivedMessage>> {
        trace!("polling next message");
        // Try and get a buffered message first.
        // if let Some(buffered) = self.buffered_msg.pop_front() {
        //     return Poll::Ready(Ok(buffered));
        // }

        self.socket.poll_try_read(cx, Self::get_next_messages)
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
        let start = Instant::now();
        // Only poll the future if we're not closed/closing.
        // ready!(self.poll_shutdown(cx)?);

        // Try to receive responses from the database and handle them.
        self.poll_backlog(cx)?;

        // Push as many new requests in the write buffer as we can.
        self.poll_receiver(cx)?;

        // Flush the write buffer if needed.
        self.handle_poll_flush(cx)?;
        info!("done: {:?}", start.elapsed());
        Poll::Pending
    }
}
