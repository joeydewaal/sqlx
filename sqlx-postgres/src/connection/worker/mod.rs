use std::{
    collections::VecDeque,
    future::Future,
    io::Write,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use sqlx_core::{
    bytes::Buf as _,
    net::{BufferedSocket, Socket},
    rt::spawn,
};

use crate::message::{BackendMessageFormat, ReceivedMessage};

use super::PgStream;

mod manager;
mod message;

pub use manager::ConnManager;
pub use message::{IoRequest, MessageBuf, PipeUntil};

pub struct Worker {
    ids: usize,
    messages_between_flush: usize,
    should_flush: bool,
    chan: UnboundedReceiver<IoRequest>,
    back_log: VecDeque<IoRequest>,
    conn: BufferedSocket<Box<dyn Socket>>,
}

impl Worker {
    pub fn spawn(stream: PgStream) -> UnboundedSender<IoRequest> {
        let (tx, rx) = unbounded();

        let conn = Worker {
            ids: 0,
            messages_between_flush: 0,
            should_flush: false,
            chan: rx,
            back_log: VecDeque::new(),
            conn: stream.inner,
        };

        spawn(conn);
        tx
    }
}

impl Future for Worker {
    type Output = sqlx_core::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Push as many new messages in the write buffer.
        // let start = Instant::now();
        while let Poll::Ready(Some(mut msg)) = self.chan.poll_next_unpin(cx) {
            msg.id = self.ids;
            self.ids += 1;
            self.messages_between_flush += 1;
            self.should_flush = true;
            let write_buff = self.conn.write_buffer_mut();
            let buff = write_buff.buf_mut();

            buff.write_all(&msg.data)?;
            write_buff.bytes_written += msg.data.len();
            write_buff.sanity_check();

            if !matches!(msg.ends_at, PipeUntil::NumResponses(0)) {
                self.back_log.push_back(msg);
            }
        }

        // Flush the write buffer if needed.
        if self.should_flush {
            if let Poll::Ready(_) = self.conn.poll_flush(cx) {
                self.should_flush = false;
                self.messages_between_flush = 0;
            }
        }
        while let Some(mut msg) = self.back_log.pop_front() {
            loop {
                let response = match poll_next_message(&mut self.conn, cx) {
                    Poll::Ready(response) => response?,
                    Poll::Pending => {
                        // Not ready for receiving messages.
                        // Push front so this is the first message next time.
                        self.back_log.push_front(msg);
                        return Poll::Pending;
                    }
                };
                msg.decrease_num_request();
                println!("BG got: {:?} {:?}", response.format, msg.id);
                let format = response.format;
                let is_rfq = response.format == BackendMessageFormat::ReadyForQuery;
                let is_closed = msg.chan.unbounded_send(response).is_err();
                if is_closed {
                    println!("Skipping: {:?} {:?}", format, msg.id);
                }

                match msg.ends_at {
                    PipeUntil::ReadyForQuery => {
                        if is_rfq {
                            break;
                        }
                    }
                    PipeUntil::NumResponses(num_responses) => {
                        if num_responses == 0 {
                            break;
                        }
                    }
                    PipeUntil::Either { left, right } => {
                        if format == left || format == right {
                            break;
                        }
                    }
                }
            }
        }
        Poll::Pending
    }
}
pub fn poll_next_message(
    conn: &mut BufferedSocket<Box<dyn Socket>>,
    cx: &mut Context<'_>,
) -> Poll<sqlx_core::Result<ReceivedMessage>> {
    let x = conn.poll_try_read(cx, |buf| {
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
    });
    x
}
