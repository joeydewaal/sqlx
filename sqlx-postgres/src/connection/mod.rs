use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::HashMap;
use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use worker::{IoRequest, MessageBuf, Pipe};

use crate::common::StatementCache;
use crate::error::Error;
use crate::ext::ustr::UStr;
use crate::io::StatementId;
use crate::message::{
    BackendMessageFormat, FrontendMessage, Notification, Query, ReadyForQuery, ReceivedMessage,
    TransactionStatus,
};
use crate::statement::PgStatementMetadata;
use crate::transaction::Transaction;
use crate::types::Oid;
use crate::{PgConnectOptions, PgTypeInfo, Postgres};

pub(crate) use sqlx_core::connection::*;

pub use self::stream::PgStream;

mod codec;
pub(crate) mod describe;
mod establish;
mod executor;
mod sasl;
mod stream;
mod tls;
mod worker;

/// A connection to a PostgreSQL database.
///
/// See [`PgConnectOptions`] for connection URL reference.
pub struct PgConnection {
    pub(crate) inner: Box<PgConnectionInner>,
}

pub struct PgConnectionInner {
    // underlying TCP or UDS stream,
    // wrapped in a potentially TLS stream,
    // wrapped in a buffered stream
    pub(crate) stream: PgStream,

    // process id of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    process_id: u32,

    // secret key of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    secret_key: u32,

    pub(crate) server_version_num: Option<u32>,

    // sequence of statement IDs for use in preparing statements
    // in PostgreSQL, the statement is prepared to a user-supplied identifier
    next_statement_id: StatementId,

    // cache statement by query string to the id and columns
    cache_statement: StatementCache<(StatementId, Arc<PgStatementMetadata>)>,

    // cache user-defined types by id <-> info
    cache_type_info: HashMap<Oid, PgTypeInfo>,
    cache_type_oid: HashMap<UStr, Oid>,
    cache_elem_type_to_array: HashMap<Oid, Oid>,
    pub(crate) pending_ready_for_query_count: usize,

    // current transaction status
    transaction_status: TransactionStatus,
    pub(crate) transaction_depth: usize,

    log_settings: LogSettings,

    chan: UnboundedSender<IoRequest>,

    notifications: UnboundedReceiver<Notification>,
}

impl PgConnection {
    pub fn server_version_num(&self) -> Option<u32> {
        self.inner.server_version_num
    }

    fn new(
        options: &PgConnectOptions,
        chan: UnboundedSender<IoRequest>,
        notifications: UnboundedReceiver<Notification>,
        stream: PgStream,
    ) -> Self {
        Self {
            inner: Box::new(PgConnectionInner {
                stream,
                chan,
                notifications,
                log_settings: options.log_settings.clone(),
                process_id: 0,
                secret_key: 0,
                next_statement_id: StatementId::NAMED_START,
                cache_statement: StatementCache::new(options.statement_cache_capacity),
                cache_type_info: HashMap::new(),
                cache_type_oid: HashMap::new(),
                cache_elem_type_to_array: HashMap::new(),
                transaction_status: TransactionStatus::Idle,
                transaction_depth: 0,
                server_version_num: None,
                pending_ready_for_query_count: 0,
            }),
        }
    }

    /// Queue a simple query (not prepared) to execute the next time this connection is used.
    ///
    /// Used for rolling back transactions and releasing advisory locks.
    #[inline(always)]
    pub(crate) fn queue_simple_query(&mut self, query: &str) -> Result<Pipe, Error> {
        self.pipe(|buf| buf.write_msg(Query(query)))
    }

    pub(crate) fn in_transaction(&self) -> bool {
        match self.inner.transaction_status {
            TransactionStatus::Transaction => true,
            TransactionStatus::Error | TransactionStatus::Idle => false,
        }
    }

    fn create_request<F>(&self, callback: F) -> sqlx_core::Result<IoRequest>
    where
        F: FnOnce(&mut MessageBuf) -> sqlx_core::Result<()>,
    {
        let mut buffer = MessageBuf::new();
        (callback)(&mut buffer)?;
        Ok(buffer.finish())
    }

    fn send_request(&self, request: IoRequest) -> sqlx_core::Result<()> {
        self.inner
            .chan
            .unbounded_send(request)
            .map_err(|_| sqlx_core::Error::WorkerCrashed)
    }

    pub(crate) fn pipe<F>(&self, callback: F) -> sqlx_core::Result<Pipe>
    where
        F: FnOnce(&mut MessageBuf) -> sqlx_core::Result<()>,
    {
        let mut req = self.create_request(callback)?;
        let (tx, rx) = unbounded();
        req.chan = Some(tx);

        self.send_request(req)?;
        Ok(Pipe::new(rx))
    }

    pub(crate) fn pipe_and_forget<T>(&self, value: T) -> sqlx_core::Result<()>
    where
        T: FrontendMessage,
    {
        let req = self.create_request(|buf| buf.write_msg(value))?;
        self.send_request(req)
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
            self.inner.chan.close_channel();
            Ok(())
        })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            self.inner.chan.close_channel();
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        // Users were complaining about this showing up in query statistics on the server.
        // By sending a comment we avoid an error if the connection was in the middle of a rowset
        // self.execute("/* SQLx ping */").map_ok(|_| ()).boxed()

        Box::pin(async move {
            // The simplest call-and-response that's possible.
            let mut pipe = self.pipe(|buf| Ok(buf.write_sync()))?;
            pipe.recv_ready_for_query().await
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
        self.inner.cache_statement.len()
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.inner.cache_type_oid.clear();

            // let mut cleared = 0_usize;

            Ok(())
            // TODO
            // while let Some((id, _)) = self.inner.cache_statement.remove_lru() {
            //     self.inner.stream.write_msg(Close::Statement(id))?;
            //     cleared += 1;
            // }

            // if cleared > 0 {
            //     self.write_sync();
            //     self.inner.stream.flush().await?;

            //     self.wait_for_close_complete(cleared).await?;
            //     self.recv_ready_for_query().await?;
            // }

            // Ok(())
        })
    }

    fn shrink_buffers(&mut self) {
        // No-op
    }

    #[doc(hidden)]
    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        async { Ok(()) }.boxed()
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
