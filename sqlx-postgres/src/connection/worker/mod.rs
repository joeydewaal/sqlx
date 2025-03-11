use std::{
    collections::VecDeque,
    future::Future,
    io::Write,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
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
    ids: usize,
    messages_between_flush: usize,
    should_flush: bool,
    chan: UnboundedReceiver<IoRequest>,
    back_log: VecDeque<IoRequest>,
    conn: BufferedSocket<Box<dyn Socket>>,
}

impl Worker {
    pub fn spawn(stream: PgStream) -> WorkerConn {
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
        WorkerConn::new(tx)
    }
}

impl Future for Worker {
    type Output = sqlx_core::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("---->");
        // Push as many new messages in the write buffer.
        let start = Instant::now();
        // println!("polling channel");
        while let Poll::Ready(Some(mut msg)) = self.chan.poll_next_unpin(cx) {
            println!("BG: got message");
            msg.id = self.ids;
            self.ids += 1;
            self.messages_between_flush += 1;
            // println!("Got message");
            self.should_flush = true;
            let write_buff = self.conn.write_buffer_mut();
            println!("------ buf before: {}", write_buff.bytes_written);
            let buff = write_buff.buf_mut();

            buff.write_all(&msg.data)?;
            write_buff.bytes_written += msg.data.len();
            write_buff.sanity_check();
            println!("- ------ buf after: {}", write_buff.bytes_written);
            println!("{} {}", write_buff.bytes_written, write_buff.bytes_flushed);

            if !matches!(msg.ends_at, PipeUntil::NumMessages { num_responses: 0 }) {
                // println!("Pushing into backlog");
                self.back_log.push_back(msg);
            } else {
                // println!("Not sending back to {}", msg.id);
            }
        }
        let write_buff = self.conn.write_buffer();
        println!("{} {}", write_buff.bytes_written, write_buff.bytes_flushed);
        println!("{:?} recv done", start.elapsed());

        let flush_start = Instant::now();
        // Flush the write buffer if needed.
        if self.should_flush {
            println!("Flushing {}", self.messages_between_flush);
            if let Poll::Ready(_) = self.conn.poll_flush(cx) {
                // println!("Done flushing {}", self.messages_between_flush);
                self.should_flush = false;
                self.messages_between_flush = 0;
            }
            println!("{:?} flush done after", flush_start.elapsed());
        } else {
            println!("Did not have to poll_flush");
        }

        let backlog_start = Instant::now();
        // println!("Going through backlog");
        while let Some(mut msg) = self.back_log.pop_front() {
            // println!("Handling message from backlog id: {}", msg.id);
            loop {
                let response = match poll_next_message(&mut self.conn, cx) {
                    Poll::Ready(response) => response?,
                    Poll::Pending => {
                        // Not ready for receiving messages.
                        // Push front so this is the first message next time.
                        // println!("Waiting on message");
                        let push_back = Instant::now();
                        self.back_log.push_front(msg);
                        println!("push back in {:?}", push_back.elapsed());
                        println!("{:?} backlog done after", backlog_start.elapsed());
                        println!("<-----");
                        return Poll::Pending;
                    }
                };
                msg.decrease_num_request();
                // println!("{:?} ---- Got response for {}", response.format, msg.id);

                let is_rfq = response.format == BackendMessageFormat::ReadyForQuery;
                println!("Sending through chan for {} {:?}", msg.id, response.format);
                let _ = msg.chan.unbounded_send(response);
                match msg.ends_at {
                    PipeUntil::ReadyForQuery => {
                        // println!("Waited for rfq for {}", msg.id);
                        if is_rfq {
                            break;
                        }
                    }
                    PipeUntil::NumMessages { num_responses } => {
                        // println!("{num_responses} for {}", msg.id);
                        if num_responses == 0 {
                            break;
                        }
                    }
                }
            }
        }
        println!("{:?} backlog done after", backlog_start.elapsed());
        println!("{:?} Returning Poll::Pending ", start.elapsed());
        println!("<----");
        Poll::Pending
    }
}
pub fn poll_next_message(
    conn: &mut BufferedSocket<Box<dyn Socket>>,
    cx: &mut Context<'_>,
) -> Poll<sqlx_core::Result<ReceivedMessage>> {
    // println!("Poll message");
    let start = Instant::now();
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
    println!("{:?} Poll next message done in", start.elapsed());
    x
}
