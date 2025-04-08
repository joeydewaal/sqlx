use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::Level;
use sqlx_core::Error;

use crate::{
    connection::stream::parse_server_version,
    message::{
        BackendMessage, BackendMessageFormat, Notice, Notification, ParameterStatus, ReadyForQuery,
        ReceivedMessage,
    },
    PgConnection, PgDatabaseError, PgSeverity,
};

pub struct ConnManager<'c> {
    conn: &'c PgConnection,
    receiver: UnboundedReceiver<ReceivedMessage>,
}

impl<'c> ConnManager<'c> {
    pub fn new(
        receiver: UnboundedReceiver<ReceivedMessage>,
        conn: &'c PgConnection,
    ) -> ConnManager<'c> {
        Self { receiver, conn }
    }

    pub fn into_inner(self) -> (&'c PgConnection, UnboundedReceiver<ReceivedMessage>) {
        (self.conn, self.receiver)
    }

    // Expect a specific type and format
    pub(crate) async fn recv_expect<B: BackendMessage>(&mut self) -> Result<B, Error> {
        self.recv().await?.decode()
    }

    pub async fn recv_ready_for_query(&mut self) -> Result<(), Error> {
        let r: ReadyForQuery = self.recv_expect().await?;
        self.conn.set_transaction_status(r.transaction_status);
        Ok(())
    }

    pub(crate) async fn wait_ready_for_query(&mut self) -> Result<(), Error> {
        loop {
            let message = self.recv().await?;

            if let BackendMessageFormat::ReadyForQuery = message.format {
                let r: ReadyForQuery = message.decode()?;

                self.conn.set_transaction_status(r.transaction_status);
                break;
            }
        }

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

    pub(crate) async fn recv_unchecked(&mut self) -> Result<ReceivedMessage, Error> {
        self.receiver
            .next()
            .await
            .ok_or_else(|| Error::WorkerCrashed)
    }

    // Get the next message from the server
    // May wait for more data from the server
    pub(crate) async fn recv(&mut self) -> Result<ReceivedMessage, Error> {
        loop {
            let message = self.recv_unchecked().await?;
            match message.format {
                BackendMessageFormat::ErrorResponse => {
                    // An error returned from the database server.
                    return Err(message.decode::<PgDatabaseError>()?.into());
                }

                BackendMessageFormat::NotificationResponse => {
                    if let Some(buffer) = &self.conn.inner.notifications {
                        let notification: Notification = message.decode()?;
                        let _ = buffer.unbounded_send(notification);

                        continue;
                    }
                }

                BackendMessageFormat::ParameterStatus => {
                    // informs the frontend about the current (initial)
                    // setting of backend parameters

                    let ParameterStatus { name, value } = message.decode()?;
                    // TODO: handle `client_encoding`, `DateStyle` change

                    match name.as_str() {
                        "server_version" => {
                            if let Some(version) = parse_server_version(&value) {
                                self.conn.set_server_version_num(version);
                            }
                        }
                        _ => {
                            self.conn
                                .with_lock(|inner| inner.parameter_statuses.insert(name, value));
                        }
                    }

                    continue;
                }

                BackendMessageFormat::NoticeResponse => {
                    // do we need this to be more configurable?
                    // if you are reading this comment and think so, open an issue

                    let notice: Notice = message.decode()?;
                    let (log_level, tracing_level) = match notice.severity() {
                        PgSeverity::Fatal | PgSeverity::Panic | PgSeverity::Error => {
                            (Level::Error, tracing::Level::ERROR)
                        }
                        PgSeverity::Warning => (Level::Warn, tracing::Level::WARN),
                        PgSeverity::Notice => (Level::Info, tracing::Level::INFO),
                        PgSeverity::Debug => (Level::Debug, tracing::Level::DEBUG),
                        PgSeverity::Info | PgSeverity::Log => (Level::Trace, tracing::Level::TRACE),
                    };

                    let log_is_enabled = log::log_enabled!(
                        target: "sqlx::postgres::notice",
                        log_level
                    ) || sqlx_core::private_tracing_dynamic_enabled!(
                        target: "sqlx::postgres::notice",
                        tracing_level
                    );
                    if log_is_enabled {
                        sqlx_core::private_tracing_dynamic_event!(
                            target: "sqlx::postgres::notice",
                            tracing_level,
                            message = notice.message()
                        );
                    }

                    continue;
                }

                _ => {}
            }

            return Ok(message);
        }
    }
}
