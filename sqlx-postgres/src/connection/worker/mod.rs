use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::message::{
    BackendMessageFormat, FrontendMessage, Notice, Notification, ParameterStatus, ReadyForQuery,
    ReceivedMessage, Terminate,
};
use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_util::{SinkExt, StreamExt};
use sqlx_core::{
    net::{Framed, Socket},
    rt::spawn,
    Result,
};

use super::{codec::PostgresCodec, stream::parse_server_version, PgStream};

mod pipe;
mod request;
mod shared;

pub use pipe::Pipe;
pub use request::{IoRequest, MessageBuf};
pub use shared::Shared;

#[derive(PartialEq, Debug)]
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
    back_log: VecDeque<UnboundedSender<ReceivedMessage>>,
    socket: Framed<Box<dyn Socket>, PostgresCodec>,
    notif_chan: UnboundedSender<Notification>,
    shared: Shared,
}

impl Worker {
    pub fn spawn(
        stream: PgStream,
        notif_chan: UnboundedSender<Notification>,
        shared: Shared,
    ) -> UnboundedSender<IoRequest> {
        let (tx, rx) = unbounded();

        let worker = Worker {
            state: WorkerState::Open,
            should_flush: false,
            chan: rx,
            back_log: VecDeque::new(),
            socket: stream.into_inner(),
            notif_chan,
            shared,
        };

        spawn(worker);
        tx
    }

    // Tries to receive the next message from the channel. Also handles termination if needed.
    #[inline(always)]
    fn poll_next_request(&mut self, cx: &mut Context<'_>) -> Poll<IoRequest> {
        if self.state != WorkerState::Open {
            return Poll::Pending;
        }

        match self.chan.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(request)) => Poll::Ready(request),
            Poll::Ready(None) => {
                // Channel was closed, explicitly or because the sender was dropped. Either way
                // we should start a gracefull shutdown.
                self.socket
                    .write_buffer_mut()
                    .put_slice(&[Terminate::FORMAT as u8, 0, 0, 0, 4]);

                self.state = WorkerState::Closing;
                self.should_flush = true;
                Poll::Pending
            }
        }
    }

    #[inline(always)]
    fn poll_receiver(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.state != WorkerState::Open {
            return Poll::Ready(Ok(()));
        }

        loop {
            ready!(self.socket.poll_ready_unpin(cx))?;

            let request = ready!(self.poll_next_request(cx));

            self.socket.start_send_unpin(request.data)?;
            self.should_flush = true;

            if let Some(chan) = request.chan {
                // We should send the responses back
                println!("got request with response");
                self.back_log.push_back(chan);
            } else {
                println!("got request without response");
            }
        }
    }

    #[inline(always)]
    fn handle_poll_flush(&mut self, cx: &mut Context<'_>) -> Result<()> {
        if self.should_flush {
            if let Poll::Ready(_) = self.socket.poll_flush_unpin(cx) {
                self.should_flush = false;
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn send_back(&mut self, response: ReceivedMessage) -> Result<()> {
        println!("sending back {:?}", response.format);
        if let Some(chan) = self.back_log.front_mut() {
            let _ = chan.unbounded_send(response);
            Ok(())
        } else {
            todo!("Received response but did not expect one.");
        }
    }

    #[inline(always)]
    fn poll_backlog(&mut self, cx: &mut Context<'_>) -> Result<()> {
        while let Some(response) = self.poll_next_message(cx)? {
            match response.format {
                BackendMessageFormat::ReadyForQuery => {
                    let rfq: ReadyForQuery = response.clone().decode()?;
                    self.shared.set_transaction_status(rfq.transaction_status);
                    self.send_back(response)?;
                    // Remove from the backlog so we dont send more responses back.
                    println!("done");
                    let _ = self.back_log.pop_front();
                }
                BackendMessageFormat::CopyInResponse => {
                    // End of response
                    self.send_back(response)?;
                    // Remove from the backlog so we dont send more responses back.
                    let _ = self.back_log.pop_front();
                }
                BackendMessageFormat::NotificationResponse => {
                    // Notification
                    let notif: Notification = response.decode()?;
                    let _ = self.notif_chan.unbounded_send(notif);
                }
                BackendMessageFormat::ParameterStatus => {
                    // Asynchronous response
                    let ParameterStatus { name, value } = response.decode()?;
                    // TODO: handle `client_encoding`, `DateStyle` change

                    match name.as_str() {
                        "server_version" => {
                            self.shared
                                .set_server_version_num(parse_server_version(&value));
                        }
                        _ => {
                            self.shared.insert_parameter_status(name, value);
                        }
                    }

                    continue;
                }
                BackendMessageFormat::NoticeResponse => {
                    // Asynchronous response
                    let notice: Notice = response.decode()?;
                    notice.emit_notice();
                }
                _ => self.send_back(response)?,
            }
        }

        if self.state != WorkerState::Open && self.back_log.is_empty() {
            self.state = WorkerState::Closed;
        }
        Ok(())
    }

    #[inline(always)]
    pub fn poll_next_message(&mut self, cx: &mut Context<'_>) -> Result<Option<ReceivedMessage>> {
        match self.socket.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(resp))) => Ok(Some(resp)),
            Poll::Ready(Some(Err(e))) => Err(e),
            Poll::Ready(None) => Ok(None),
            Poll::Pending => Ok(None),
        }
    }

    #[inline(always)]
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.state == WorkerState::Closed {
            // The buffer is closed, a [Terminate] message has been sent, now try and close the socket.
            self.socket.poll_close_unpin(cx)
        } else {
            Poll::Pending
        }
    }
}

impl Future for Worker {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Try to receive responses from the database and handle them.
        self.poll_backlog(cx)?;

        // Push as many new requests in the write buffer as we can.
        if let Poll::Ready(Err(e)) = self.poll_receiver(cx) {
            return Poll::Ready(Err(e));
        };

        // Flush the write buffer if needed.
        self.handle_poll_flush(cx)?;

        // Close this socket if we're done.
        self.poll_shutdown(cx)
    }
}
