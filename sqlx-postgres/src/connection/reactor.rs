use std::{
    collections::VecDeque,
    future::Future,
    io::Write,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures_channel::mpsc::{
    channel, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use futures_util::{SinkExt, StreamExt};
use sqlx_core::{
    bytes::Buf as _,
    io::ProtocolEncode,
    net::{BufferedSocket, Socket},
    rt::spawn,
    Error,
};

use crate::{
    message::{
        BackendMessage, BackendMessageFormat, EncodeMessage, FrontendMessage, ParameterStatus,
        ReceivedMessage,
    },
    PgDatabaseError,
};

pub struct ReactorReiver {
    chan: UnboundedReceiver<ReceivedMessage>,
}

impl ReactorReiver {
    pub fn new(chan: UnboundedReceiver<ReceivedMessage>) -> Self {
        Self { chan }
    }
}

use super::PgStream;

#[derive(Debug, PartialEq)]
enum WaitType {
    NumMessages { num_responses: usize },
    ReadyForQuery,
}

pub struct IoRequestBuilder {
    ends_at: WaitType,
    data: Vec<u8>,
}

impl IoRequestBuilder {
    pub fn new() -> Self {
        Self {
            ends_at: WaitType::NumMessages { num_responses: 0 },
            data: Vec::new(),
        }
    }
    #[inline(always)]
    pub fn write<'en, T>(&mut self, value: T) -> sqlx_core::Result<()>
    where
        T: ProtocolEncode<'en, ()>,
    {
        self.write_with(value, ())
    }

    #[inline(always)]
    pub fn write_with<'en, T, C>(&mut self, value: T, context: C) -> sqlx_core::Result<()>
    where
        T: ProtocolEncode<'en, C>,
    {
        value.encode_with(&mut self.data, context)?;
        self.inc_response_count();
        Ok(())
    }

    pub fn inc_response_count(&mut self) {
        match &mut self.ends_at {
            WaitType::NumMessages { num_responses } => {
                *num_responses += 1;
            }
            _ => {}
        }
    }
    #[inline(always)]
    pub(crate) fn write_msg(&mut self, message: impl FrontendMessage) -> Result<(), Error> {
        self.write(EncodeMessage(message))
    }

    pub fn wait_rfq(&mut self) {
        self.ends_at = WaitType::ReadyForQuery
    }

    pub fn finish(self) -> (IoRequest, ReactorReiver) {
        let (tx, rx) = unbounded();
        let req = IoRequest {
            ends_at: self.ends_at,
            data: self.data,
            chan: tx,
        };

        let receiver = ReactorReiver::new(rx);
        (req, receiver)
    }
}

#[derive(Debug)]
pub struct IoRequest {
    chan: UnboundedSender<ReceivedMessage>,
    data: Vec<u8>,
    ends_at: WaitType,
}

impl IoRequest {
    fn decrease_num_request(&mut self) {
        match &mut self.ends_at {
            WaitType::NumMessages { num_responses } => {
                *num_responses -= 1;
            }
            _ => {}
        }
    }
}

pub struct ReactorSender {
    chan: Sender<IoRequest>,
    last_recver: Option<ReactorReiver>,
}

impl ReactorSender {
    fn new(chan: Sender<IoRequest>) -> Self {
        ReactorSender {
            chan,
            last_recver: None,
        }
    }

    pub async fn send(&mut self, builder: IoRequestBuilder) -> sqlx_core::Result<()> {
        let (req, rx1) = builder.finish();
        self.chan
            .send(req)
            .await
            .map_err(|_| sqlx_core::Error::WorkerCrashed)?;
        self.last_recver = Some(rx1);
        Ok(())
    }
    // Expect a specific type and format
    pub async fn recv_expect<B: BackendMessage>(&mut self) -> Result<B, Error> {
        self.recv().await?.decode()
    }

    // Get the next message from the server
    // May wait for more data from the server
    pub(crate) async fn recv(&mut self) -> Result<ReceivedMessage, Error> {
        loop {
            println!("ReactorReceiver::recv");
            let x = self
                .last_recver
                .as_mut()
                .ok_or(sqlx_core::Error::WorkerCrashed)?;
            let message = x.chan.next().await.ok_or(Error::WorkerCrashed)?;
            println!("fg worker got: {message:?}");

            match message.format {
                BackendMessageFormat::ErrorResponse => {
                    // An error returned from the database server.
                    return Err(message.decode::<PgDatabaseError>()?.into());
                }

                BackendMessageFormat::NotificationResponse => {
                    // if let Some(buffer) = &mut self.notifications {
                    //     let notification: Notification = message.decode()?;
                    //     let _ = buffer.send(notification).await;

                    continue;
                    // }
                }

                BackendMessageFormat::ParameterStatus => {
                    // informs the frontend about the current (initial)
                    // setting of backend parameters

                    let ParameterStatus { name, value } = message.decode()?;
                    // TODO: handle `client_encoding`, `DateStyle` change

                    // match name.as_str() {
                    //     "server_version" => {
                    //         self.server_version_num = parse_server_version(&value);
                    //     }
                    //     _ => {
                    //         self.parameter_statuses.insert(name, value);
                    //     }
                    // }

                    continue;
                }

                BackendMessageFormat::NoticeResponse => {
                    // do we need this to be more configurable?
                    // if you are reading this comment and think so, open an issue

                    // let notice: Notice = message.decode()?;

                    // let (log_level, tracing_level) = match notice.severity() {
                    //     PgSeverity::Fatal | PgSeverity::Panic | PgSeverity::Error => {
                    //         (Level::Error, tracing::Level::ERROR)
                    //     }
                    //     PgSeverity::Warning => (Level::Warn, tracing::Level::WARN),
                    //     PgSeverity::Notice => (Level::Info, tracing::Level::INFO),
                    //     PgSeverity::Debug => (Level::Debug, tracing::Level::DEBUG),
                    //     PgSeverity::Info | PgSeverity::Log => (Level::Trace, tracing::Level::TRACE),
                    // };

                    // let log_is_enabled = log::log_enabled!(
                    //     target: "sqlx::postgres::notice",
                    //     log_level
                    // ) || sqlx_core::private_tracing_dynamic_enabled!(
                    //     target: "sqlx::postgres::notice",
                    //     tracing_level
                    // );
                    // if log_is_enabled {
                    //     sqlx_core::private_tracing_dynamic_event!(
                    //         target: "sqlx::postgres::notice",
                    //         tracing_level,
                    //         message = notice.message()
                    //     );
                    // }

                    continue;
                }

                _ => {}
            }

            return Ok(message);
        }
    }
}

pub struct Reactor {
    should_flush: bool,
    chan: Receiver<IoRequest>,
    back_log: VecDeque<IoRequest>,
    conn: BufferedSocket<Box<dyn Socket>>,
}

impl Reactor {
    pub fn spawn(stream: PgStream) -> ReactorSender {
        let (tx, rx) = channel(12);

        let conn = Reactor {
            should_flush: false,
            chan: rx,
            back_log: VecDeque::new(),
            conn: stream.inner,
        };

        spawn(conn);
        ReactorSender::new(tx)
    }
}

impl Future for Reactor {
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

            if !matches!(msg.ends_at, WaitType::NumMessages { num_responses: 0 }) {
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
                    WaitType::ReadyForQuery => {
                        println!("Waited for rfq");
                        if is_rfq {
                            break;
                        }
                    }
                    WaitType::NumMessages { num_responses } => {
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
