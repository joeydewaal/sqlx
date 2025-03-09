use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use sqlx_core::Error;

use crate::{
    message::{
        BackendMessage, BackendMessageFormat, ParameterStatus, ReadyForQuery, ReceivedMessage,
    },
    PgDatabaseError,
};

use super::{message::WaitType, IoRequest, MessageBuf};

pub struct ConnManager {
    chan: UnboundedSender<IoRequest>,
    last_receiver: Option<UnboundedReceiver<ReceivedMessage>>,
}

impl ConnManager {
    pub fn new(chan: UnboundedSender<IoRequest>) -> Self {
        Self {
            chan,
            last_receiver: None,
        }
    }

    pub fn send_message<F>(&mut self, callback: F) -> sqlx_core::Result<()>
    where
        F: FnOnce(&mut MessageBuf) -> sqlx_core::Result<WaitType>,
    {
        let mut buffer = MessageBuf::new();
        let wait_type = (callback)(&mut buffer)?;
        buffer.ends_at = wait_type;
        let (request, receiver) = buffer.finish();
        self.chan
            .unbounded_send(request)
            .map_err(|_| Error::WorkerCrashed)?;
        self.last_receiver = Some(receiver);
        Ok(())
    }
    // Expect a specific type and format
    pub async fn recv_expect<B: BackendMessage>(&mut self) -> Result<B, Error> {
        self.recv().await?.decode()
    }

    pub async fn recv_ready_for_query(&mut self) -> Result<(), Error> {
        let _: ReadyForQuery = self.recv_expect().await?;
        Ok(())
    }

    // wait for CloseComplete to indicate a statement was closed
    pub async fn wait_for_close_complete(&mut self, mut count: usize) -> Result<(), Error> {
        // we need to wait for the [CloseComplete] to be returned from the server
        while count > 0 {
            match self.recv().await? {
                message if message.format == BackendMessageFormat::PortalSuspended => {
                    // there was an open portal
                    // this can happen if the last time a statement was used it was not fully executed
                }

                message if message.format == BackendMessageFormat::CloseComplete => {
                    // successfully closed the statement (and freed up the server resources)
                    count -= 1;
                }

                message => {
                    return Err(err_protocol!(
                        "expecting PortalSuspended or CloseComplete but received {:?}",
                        message.format
                    ));
                }
            }
        }

        Ok(())
    }

    // Get the next message from the server
    // May wait for more data from the server
    pub(crate) async fn recv(&mut self) -> Result<ReceivedMessage, Error> {
        loop {
            println!("ReactorReceiver::recv");
            let x = self
                .last_receiver
                .as_mut()
                .ok_or(sqlx_core::Error::WorkerCrashed)?;

            let message = x.next().await.ok_or(Error::WorkerCrashed)?;
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
