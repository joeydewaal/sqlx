use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::HashMap;
use futures_core::future::BoxFuture;
use reactor::WorkerChan;
use worker::WaitType;

use crate::common::StatementCache;
use crate::error::Error;
use crate::ext::ustr::UStr;
use crate::io::StatementId;
use crate::message::{Close, ReadyForQuery, ReceivedMessage, TransactionStatus};
use crate::statement::PgStatementMetadata;
use crate::transaction::Transaction;
use crate::types::Oid;
use crate::{PgConnectOptions, PgTypeInfo, Postgres};

pub(crate) use sqlx_core::connection::*;

pub use self::stream::PgStream;

pub(crate) mod describe;
mod establish;
mod executor;
mod reactor;
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
    // Channel to the background worker
    pub(crate) chan: WorkerChan,

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

    // sequence of statement IDs for use in preparing statements
    // in PostgreSQL, the statement is prepared to a user-supplied identifier
    next_statement_id: StatementId,

    // cache statement by query string to the id and columns
    cache_statement: StatementCache<(StatementId, Arc<PgStatementMetadata>)>,

    // cache user-defined types by id <-> info
    cache_type_info: HashMap<Oid, PgTypeInfo>,
    cache_type_oid: HashMap<UStr, Oid>,
    cache_elem_type_to_array: HashMap<Oid, Oid>,

    // number of ReadyForQuery messages that we are currently expecting
    pub(crate) pending_ready_for_query_count: usize,

    // current transaction status
    transaction_status: TransactionStatus,
    pub(crate) transaction_depth: usize,

    log_settings: LogSettings,
}

impl PgConnection {
    /// the version number of the server in `libpq` format
    pub fn server_version_num(&self) -> Option<u32> {
        todo!();
        // self.inner.stream.server_version_num
    }

    #[inline(always)]
    fn handle_ready_for_query(&mut self, message: ReceivedMessage) -> Result<(), Error> {
        self.inner.pending_ready_for_query_count = self
            .inner
            .pending_ready_for_query_count
            .checked_sub(1)
            .ok_or_else(|| err_protocol!("received more ReadyForQuery messages than expected"))?;

        self.inner.transaction_status = message.decode::<ReadyForQuery>()?.transaction_status;

        Ok(())
    }

    /// Queue a simple query (not prepared) to execute the next time this connection is used.
    ///
    /// Used for rolling back transactions and releasing advisory locks.
    #[inline(always)]
    pub(crate) fn queue_simple_query(&mut self, query: &str) -> Result<(), Error> {
        // self.inner.stream.write_msg(Query(query))?;
        // self.inner.pending_ready_for_query_count += 1;

        Ok(())
    }

    pub(crate) fn in_transaction(&self) -> bool {
        match self.inner.transaction_status {
            TransactionStatus::Transaction => true,
            TransactionStatus::Error | TransactionStatus::Idle => false,
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

    fn close(mut self) -> BoxFuture<'static, Result<(), Error>> {
        // The normal, graceful termination procedure is that the frontend sends a Terminate
        // message and immediately closes the connection.

        // On receipt of this message, the backend closes the
        // connection and terminates.

        Box::pin(async move {
            // self.inner.stream.send(Terminate).await?;
            // self.inner.stream.shutdown().await?;

            Ok(())
        })
    }

    fn close_hard(mut self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            // self.inner.stream.shutdown().await?;

            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        // Users were complaining about this showing up in query statistics on the server.
        // By sending a comment we avoid an error if the connection was in the middle of a rowset
        // self.execute("/* SQLx ping */").map_ok(|_| ()).boxed()

        Box::pin(async move {
            // The simplest call-and-response that's possible.
            let mut manager = self.inner.chan.manager();

            manager.send_message(|message| {
                message.write_sync();
                Ok(WaitType::ReadyForQuery)
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
        self.inner.cache_statement.len()
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.inner.cache_type_oid.clear();

            let mut cleared = 0_usize;

            let mut manager = self.inner.chan.manager();

            manager.send_message(|messages| {
                while let Some((id, _)) = self.inner.cache_statement.remove_lru() {
                    messages.write_msg(Close::Statement(id))?;
                    cleared += 1;
                }

                if cleared > 0 {
                    messages.write_sync();
                    return Ok(WaitType::ReadyForQuery);
                }

                Ok(WaitType::NumMessages { num_responses: 0 })
            })?;

            if cleared > 0 {
                manager.wait_for_close_complete(cleared).await?;
                manager.recv_ready_for_query().await?;
            }
            Ok(())
        })
    }

    fn shrink_buffers(&mut self) {
        // self.inner.stream.shrink_buffers();
    }

    #[doc(hidden)]
    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }

    #[doc(hidden)]
    fn should_flush(&self) -> bool {
        // !self.inner.stream.write_buffer().is_empty()
        todo!();
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
