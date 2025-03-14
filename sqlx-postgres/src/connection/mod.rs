use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::RwLock;

use futures_channel::mpsc::UnboundedSender;
use futures_core::future::BoxFuture;
use sqlx_core::io::ProtocolEncode;
use stmt_cache::{SharedStatementCache, StatementStatus};
use type_cache::TypeCache;
use worker::{ConnManager, IoRequest, MessageBuf, PipeUntil};

use crate::error::Error;
use crate::io::{StatementId, StatementIdManager};
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

    // underlying TCP or UDS stream,
    // wrapped in a potentially TLS stream,
    // wrapped in a buffered stream
    // pub(crate) stream: PgStream,
    pub(crate) notifications: Option<UnboundedSender<Notification>>,

    pub(crate) parameter_statuses: RwLock<BTreeMap<String, String>>,

    pub(crate) server_version_num: AtomicU32,

    // process id of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    process_id: u32,

    // secret key of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    secret_key: u32,

    // sequence of statement IDs for use in preparing statements
    // in PostgreSQL, the statement is prepared to a user-supplied identifier
    stmt_id_manager: StatementIdManager,

    // cache statement by query string to the id and columns
    // cache_statement: StatementCache<(StatementId, Arc<PgStatementMetadata>)>,
    stmt_cache: SharedStatementCache,

    type_cache: TypeCache,

    // current transaction status
    #[allow(dead_code)]
    transaction_status: AtomicU8,
    pub(crate) transaction_depth: AtomicUsize,

    log_settings: LogSettings,
}

impl PgConnection {
    /// the version number of the server in `libpq` format
    pub fn server_version_num(&self) -> Option<u32> {
        let version = self.inner.server_version_num.load(Ordering::Relaxed);
        (version != 0).then(|| version)
    }

    pub(super) fn set_server_version_num(&self, version_num: u32) {
        self.inner
            .server_version_num
            .store(version_num, Ordering::Relaxed);
    }

    /// Queue a simple query (not prepared) to execute the next time this connection is used.
    ///
    /// Used for rolling back transactions and releasing advisory locks.
    #[inline(always)]
    pub(crate) fn queue_simple_query(&self, query: &str) -> Result<ConnManager, Error> {
        self.pipe_message(|buff| {
            buff.write_msg(Query(query))?;
            Ok(PipeUntil::ReadyForQuery)
        })
    }

    pub(crate) fn transaction_depth(&self) -> usize {
        self.inner.transaction_depth.load(Ordering::Acquire)
    }

    pub(crate) fn increment_transaction_depth(&self) {
        self.inner.transaction_depth.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn decrement_transaction_depth(&self) {
        self.inner.transaction_depth.fetch_sub(1, Ordering::AcqRel);
    }
    fn set_transaction_status(&self, status: TransactionStatus) {
        self.inner
            .transaction_status
            .store(status as u8, Ordering::Release);
    }

    fn pipe_msg_once<'c, 'en, T>(&'c self, value: T) -> sqlx_core::Result<ConnManager<'c>>
    where
        T: FrontendMessage,
    {
        self.pipe_once(EncodeMessage(value))
    }

    fn pipe_once<'c, 'en, T>(&'c self, value: T) -> sqlx_core::Result<ConnManager<'c>>
    where
        T: ProtocolEncode<'en, ()>,
    {
        self.pipe_message(|buff| {
            buff.write(value)?;
            Ok(PipeUntil::NumResponses(1))
        })
    }

    pub fn pipe_message<'c, F>(&'c self, callback: F) -> sqlx_core::Result<ConnManager<'c>>
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
        match self.inner.transaction_status.load(Ordering::Relaxed) {
            b'T' => true,
            b'E' | b'I' => false,
            _ => panic!(),
        }
    }

    fn new(options: &PgConnectOptions, chan: UnboundedSender<IoRequest>) -> Self {
        Self {
            inner: Box::new(PgConnectionInner {
                chan,
                notifications: None,
                parameter_statuses: BTreeMap::new().into(),
                server_version_num: AtomicU32::new(0),
                process_id: 0,
                secret_key: 0,
                stmt_id_manager: StatementIdManager::new(StatementId::NAMED_START),
                stmt_cache: SharedStatementCache::new(options.statement_cache_capacity),
                type_cache: TypeCache::new(),
                transaction_status: AtomicU8::new(0),
                transaction_depth: AtomicUsize::new(0),
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
            let mut manager = self.pipe_message(|message| {
                // The simplest call-and-response that's possible.
                message.write_sync()
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
        self.inner.stmt_cache.cached_statements_size()
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.inner.type_cache.clear_oid_cache();

            let mut cleared = 0_usize;

            let mut manager = self.pipe_message(|messages| {
                let mut stmt_cache = self.inner.stmt_cache.lock();

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
