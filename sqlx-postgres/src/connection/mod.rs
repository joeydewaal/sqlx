use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Mutex;

use futures_channel::mpsc::UnboundedSender;
use futures_core::future::BoxFuture;
use sqlx_core::io::ProtocolEncode;
use stmt_cache::{SharedStatementCache, StatementStatus};
use type_cache::TypeCache;
use worker::{ConnManager, IoRequest, MessageBuf, PipeUntil};

use crate::error::Error;
use crate::io::StatementId;
use crate::message::{
    Close, EncodeMessage, FrontendMessage, Notification, Query, TransactionStatus,
};
use crate::statement::PgStatementMetadata;
use crate::transaction::Transaction;
use crate::{PgConnectOptions, Postgres};

pub(crate) use sqlx_core::connection::*;

pub use self::stream::PgStream;

pub(crate) mod describe;
mod establish;
mod executor;
mod sasl;
mod stmt_cache;
mod stream;
mod tls;
mod type_cache;
pub(crate) mod worker;

/// A connection to a PostgreSQL database.
///
/// See [`PgConnectOptions`] for connection URL reference.
pub struct PgConnection {
    pub(crate) inner: Box<PgConnectionInner>,
}

pub struct PgConnectionInner {
    // Channel to the background worker
    pub(crate) chan: UnboundedSender<IoRequest>,

    // buffer of unreceived notification messages from `PUBLISH`
    // this is set when creating a PgListener and only written to if that listener is
    // re-used for query execution in-between receiving messages
    pub(crate) notifications: Option<UnboundedSender<Notification>>,

    shared_inner: Mutex<PgSharedInner>,

    // process id of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    process_id: u32,

    // secret key of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    secret_key: u32,

    // cache statement by query string to the id and columns
    cache_statement: SharedStatementCache,

    cache_type: TypeCache,

    log_settings: LogSettings,
}

pub(crate) struct PgSharedInner {
    // sequence of statement IDs for use in preparing statements
    // in PostgreSQL, the statement is prepared to a user-supplied identifier
    next_statement_id: StatementId,

    pub(crate) parameter_statuses: BTreeMap<String, String>,

    pub(crate) server_version_num: Option<u32>,

    // current transaction status
    transaction_status: TransactionStatus,
    pub(crate) transaction_depth: usize,
}

impl PgConnection {
    /// the version number of the server in `libpq` format
    pub fn server_version_num(&self) -> Option<u32> {
        self.with_lock(|inner| inner.server_version_num)
    }

    pub(super) fn set_server_version_num(&self, version_num: u32) {
        self.with_lock(|inner| inner.server_version_num = Some(version_num));
    }

    /// Queue a simple query (not prepared) to execute the next time this connection is used.
    ///
    /// Used for rolling back transactions and releasing advisory locks.
    #[inline(always)]
    pub(crate) fn queue_simple_query(&self, query: &str) -> Result<ConnManager, Error> {
        self.start_pipe(|buf| {
            buf.write_msg(Query(query))?;
            Ok(PipeUntil::ReadyForQuery)
        })
    }

    pub(crate) fn transaction_depth(&self) -> usize {
        self.with_lock(|inner| inner.transaction_depth)
    }

    pub(crate) fn increment_transaction_depth(&self) {
        self.with_lock(|inner| inner.transaction_depth += 1);
    }

    pub(crate) fn decrement_transaction_depth(&self) {
        self.with_lock(|inner| inner.transaction_depth -= 1);
    }

    pub(crate) fn set_transaction_status(&self, status: TransactionStatus) {
        self.with_lock(|inner| inner.transaction_status = status);
    }

    /// This should generally not be used, see `PipeUntil::NumResponses` for more info.
    pub(crate) fn pipe_msg_once<'c, 'en, T>(
        &'c self,
        value: T,
    ) -> sqlx_core::Result<ConnManager<'c>>
    where
        T: FrontendMessage,
    {
        self.pipe_once(EncodeMessage(value))
    }

    /// This should generally not be used, see `PipeUntil::NumResponses` for more info.
    fn pipe_once<'c, 'en, T>(&'c self, value: T) -> sqlx_core::Result<ConnManager<'c>>
    where
        T: ProtocolEncode<'en, ()>,
    {
        self.start_pipe(|buf| {
            buf.write(value)?;
            Ok(PipeUntil::NumResponses(1))
        })
    }

    /// Starts a temporary pipe to the background worker, the worker sends responses back until
    /// the the condition of `PipeUntil` is met.
    pub fn start_pipe<'c, F>(&'c self, callback: F) -> sqlx_core::Result<ConnManager<'c>>
    where
        F: FnOnce(&mut MessageBuf) -> sqlx_core::Result<PipeUntil>,
    {
        let mut buffer = MessageBuf::new();
        let wait_type = (callback)(&mut buffer)?;
        let (request, receiver) = buffer.finish(wait_type);
        self.inner
            .chan
            .unbounded_send(request)
            .map_err(|_| sqlx_core::Error::WorkerCrashed)?;

        Ok(ConnManager::new(receiver, &self))
    }

    pub(crate) fn in_transaction(&self) -> bool {
        self.with_lock(|inner| match inner.transaction_status {
            TransactionStatus::Transaction => true,
            TransactionStatus::Error | TransactionStatus::Idle => false,
        })
    }

    pub(crate) fn with_lock<F, O>(&self, callback: F) -> O
    where
        F: FnOnce(&mut PgSharedInner) -> O,
    {
        let mut locked = self
            .inner
            .shared_inner
            .lock()
            .expect("BUG: Could not get lock");
        callback(&mut locked)
    }

    fn new(options: &PgConnectOptions, chan: UnboundedSender<IoRequest>) -> Self {
        Self {
            inner: Box::new(PgConnectionInner {
                chan,
                notifications: None,
                shared_inner: PgSharedInner {
                    next_statement_id: StatementId::NAMED_START,
                    parameter_statuses: BTreeMap::new(),
                    server_version_num: None,
                    transaction_status: TransactionStatus::Idle,
                    transaction_depth: 0,
                }
                .into(),
                process_id: 0,
                secret_key: 0,
                cache_statement: SharedStatementCache::new(options.statement_cache_capacity),
                cache_type: TypeCache::new(),
                log_settings: options.log_settings.clone(),
            }),
        }
    }
}

impl Debug for PgConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgConnection").finish()
    }
}

impl Connection for PgConnection {
    type Database = Postgres;

    type Options = PgConnectOptions;

    fn close(self) -> BoxFuture<'static, Result<(), Error>> {
        // The normal, graceful termination procedure is that the frontend sends a Terminate
        // message and immediately closes the connection.

        // On receipt of this message, the backend closes the
        // connection and terminates.

        Box::pin(async move {
            // Closing the channel makes the background worker to close the connection.
            self.inner.chan.close_channel();
            Ok(())
        })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            // Closing the channel makes the background worker to close the connection.
            self.inner.chan.close_channel();
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        // Users were complaining about this showing up in query statistics on the server.
        // By sending a comment we avoid an error if the connection was in the middle of a rowset
        // self.execute("/* SQLx ping */").map_ok(|_| ()).boxed()

        Box::pin(async move {
            let mut manager = self.start_pipe(|buf| {
                // The simplest call-and-response that's possible.
                buf.write_sync()
            })?;
            manager.recv_ready_for_query().await?;
            Ok(())
        })
    }

    fn begin(&mut self) -> BoxFuture<'_, Result<Transaction<'_, Self::Database>, Error>>
    where
        Self: Sized,
    {
        Transaction::begin(self, None)
    }

    fn begin_with(
        &mut self,
        statement: impl Into<Cow<'static, str>>,
    ) -> BoxFuture<'_, Result<Transaction<'_, Self::Database>, Error>>
    where
        Self: Sized,
    {
        Transaction::begin(self, Some(statement.into()))
    }

    fn cached_statements_size(&self) -> usize {
        self.inner.cache_statement.cached_statements_size()
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.inner.cache_type.clear_oid_cache();

            let mut cleared = 0_usize;

            let mut manager = self.start_pipe(|messages| {
                let mut stmt_cache = self.inner.cache_statement.lock();

                while let Some(stmt) = stmt_cache.remove_lru() {
                    let statement_id = match stmt {
                        StatementStatus::Cached {
                            statement_id,
                            metadata: _,
                        } => statement_id,
                        StatementStatus::InFlight { semaphore: _ } => {
                            continue;
                        }
                    };
                    messages.write_msg(Close::Statement(statement_id))?;
                    cleared += 1;
                }
                drop(stmt_cache);

                if cleared > 0 {
                    // Pipe messages until we receive a `ReadyForQuery` message.
                    messages.write_sync()
                } else {
                    // This sends an empty message that waits for 0 responses.
                    Ok(PipeUntil::NumResponses(0))
                }
            })?;

            if cleared > 0 {
                manager.wait_for_close_complete(cleared).await?;
                manager.recv_ready_for_query().await?;
            }
            Ok(())
        })
    }

    fn shrink_buffers(&mut self) {
        // No-op
    }

    #[doc(hidden)]
    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        // No-op
        Box::pin(async { Ok(()) })
    }

    #[doc(hidden)]
    fn should_flush(&self) -> bool {
        false
    }
}

// Implement `AsMut<Self>` so that `PgConnection` can be wrapped in
// a `PgAdvisoryLockGuard`.
//
// See: https://github.com/launchbadge/sqlx/issues/2520
impl AsMut<PgConnection> for PgConnection {
    fn as_mut(&mut self) -> &mut PgConnection {
        self
    }
}
