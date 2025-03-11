use std::{
    collections::VecDeque,
    future::Future,
    io::Write,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures_channel::mpsc::{unbounded, UnboundedReceiver};
use futures_util::StreamExt;
use sqlx_core::{
    bytes::Buf as _,
    net::{BufferedSocket, Socket},
    rt::spawn,
};

use crate::message::{BackendMessageFormat, ReceivedMessage};

use super::PgStream;

mod channel;
mod manager;
mod message;

pub use channel::WorkerConn;
pub use manager::ConnManager;
pub use message::{IoRequest, MessageBuf, PipeUntil};

pub struct Worker {
    should_flush: bool,
    chan: UnboundedReceiver<IoRequest>,
    back_log: VecDeque<IoRequest>,
    conn: BufferedSocket<Box<dyn Socket>>,
}

impl Worker {
    pub fn spawn(stream: PgStream) -> WorkerConn {
        let (tx, rx) = unbounded();

        let conn = Worker {
            should_flush: false,
            chan: rx,
            back_log: VecDeque::new(),
            conn: stream.inner,
        };

        spawn(conn);
        WorkerConn::new(tx)
    }
}

impl Future for Worker {
    type Output = sqlx_core::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Push as many new messages in the write buffer.
        while let Poll::Ready(Some(msg)) = self.chan.poll_next_unpin(cx) {
            println!("Got msg from fg");
            self.should_flush = true;
            let write_buff = self.conn.write_buffer_mut();
            let buff = write_buff.buf_mut();
            buff.write_all(&msg.data)?;
            write_buff.bytes_written = msg.data.len();
            write_buff.sanity_check();

            if !matches!(msg.ends_at, PipeUntil::NumMessages { num_responses: 0 }) {
                self.back_log.push_back(msg);
            }
        }

        // Flush the write buffer if needed.
        if self.should_flush {
            println!("Flushing");
            if let Poll::Ready(_) = self.conn.poll_flush(cx) {
                println!("Done flushing");
                self.should_flush = false;
            }
        }

        while let Some(mut msg) = self.back_log.pop_front() {
            loop {
                let response = match poll_message(&mut self.conn, cx) {
                    Poll::Ready(response) => response?,
                    Poll::Pending => {
                        // Not ready for receiving messages.
                        // Push front so this is the first message next time.
                        self.back_log.push_front(msg);
                        return Poll::Pending;
                    }
                };
                msg.decrease_num_request();

                let is_rfq = response.format == BackendMessageFormat::ReadyForQuery;
                let _ = msg.chan.unbounded_send(response);
                match msg.ends_at {
                    PipeUntil::ReadyForQuery => {
                        println!("Waited for rfq");
                        if is_rfq {
                            break;
                        }
                    }
                    PipeUntil::NumMessages { num_responses } => {
                        println!("{num_responses}");
                        if num_responses == 0 {
                            break;
                        }
                    }
                }
            }
        }

        Poll::Pending
    }
}
pub fn poll_message(
    conn: &mut BufferedSocket<Box<dyn Socket>>,
    cx: &mut Context<'_>,
) -> Poll<sqlx_core::Result<ReceivedMessage>> {
    conn.poll_try_read(cx, |buf| {
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
