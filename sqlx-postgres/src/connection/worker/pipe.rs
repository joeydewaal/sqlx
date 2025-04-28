use std::{
    future::poll_fn,
    task::{ready, Context, Poll},
};

use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::Level;
use sqlx_core::Error;

use crate::{
    connection::stream::parse_server_version,
    message::{
        BackendMessage, BackendMessageFormat, BatchReceivedMessage, Notice, ParameterStatus,
        ReadyForQuery, ReceivedMessage,
    },
    PgConnection, PgDatabaseError, PgSeverity,
};

pub struct Pipe<'c> {
    pub conn: &'c PgConnection,
    parts: Parts,
}

pub struct Parts {
    receiver: UnboundedReceiver<BatchReceivedMessage>,
    buffered: Option<BatchReceivedMessage>,
}

impl<'c> Pipe<'c> {
    pub fn new(
        receiver: UnboundedReceiver<BatchReceivedMessage>,
        conn: &'c PgConnection,
    ) -> Pipe<'c> {
        Self {
            conn,
            parts: Parts {
                receiver,
                buffered: None,
            },
        }
    }

    pub fn into_parts(self) -> (&'c PgConnection, Parts) {
        (self.conn, self.parts)
    }

    pub fn from_parts(conn: &'c PgConnection, parts: Parts) -> Self {
        Self { conn, parts }
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

    // Get the next message from the server
    // May wait for more data from the server
    pub(crate) async fn recv(&mut self) -> Result<ReceivedMessage, Error> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ReceivedMessage, Error>> {
        loop {
            let buffered = if let Some(x) = &mut self.parts.buffered {
                x
            } else {
                let new = ready!(self.parts.receiver.poll_next_unpin(cx))
                    .ok_or_else(|| Error::WorkerCrashed)?;
                self.parts.buffered.insert(new)
            };

            if let Some(next) = buffered.next()? {
                return Poll::Ready(Ok(next));
            } else {
                let _ = self.parts.buffered.take();
            }
        }
    }

    // Get the next message from the server
    // May wait for more data from the server
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ReceivedMessage, Error>> {
        loop {
            let message = ready!(self.poll_next(cx)?);
            match message.format {
                BackendMessageFormat::ErrorResponse => {
                    // An error returned from the database server.
                    return Poll::Ready(Err(message.decode::<PgDatabaseError>()?.into()));
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

            return Poll::Ready(Ok(message));
        }
    }
}
