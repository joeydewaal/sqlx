use std::sync::{Arc, Mutex, MutexGuard};

use sqlx_core::{common::StatementCache, rt::Notify, Either};

use super::{PgStatementMetadata, StatementId};

#[derive(Clone)]
pub enum StmtStatus {
    Cached(StatementId, Arc<PgStatementMetadata>),
    InFlight(Arc<Notify>),
}

impl StmtStatus {
    fn in_flight() -> Self {
        StmtStatus::InFlight(Arc::new(Notify::new()))
    }
}

pub struct SharedStmtCache {
    inner: Mutex<StatementCache<StmtStatus>>,
}

impl SharedStmtCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(StatementCache::new(capacity)),
        }
    }

    pub fn lock<'c>(&'c self) -> MutexGuard<'c, StatementCache<StmtStatus>> {
        const ERR_MSG: &'static str = "ERROR: failed to get statement cache lock";
        self.inner.lock().expect(ERR_MSG)
    }

    pub async fn get(
        &self,
        sql: &str,
        store_to_cache: bool,
    ) -> Option<(StatementId, Arc<PgStatementMetadata>)> {
        match self.get_or_start_in_flight(sql, store_to_cache) {
            // We either got the statement cached and can use it or don't have it and are
            // going to prepare it.
            Either::Left(maybe_prepared) => maybe_prepared,
            // The statement is being prepared.
            Either::Right(wait_event) => {
                // We wait for the statement to be prepared and try again.
                wait_event.notified().await;
                Box::pin(self.get(sql, store_to_cache)).await
            }
        }
    }

    fn get_or_start_in_flight(
        &self,
        sql: &str,
        store_to_cache: bool,
    ) -> Either<Option<(StatementId, Arc<PgStatementMetadata>)>, Arc<Notify>> {
        let mut this = self.lock();
        match this.get_mut(sql).cloned() {
            Some(StmtStatus::Cached(stmt, meta)) => return Either::Left(Some((stmt, meta))),
            Some(StmtStatus::InFlight(wait_event)) => Either::Right(wait_event),
            None => {
                // If the query is not going to be stored in cache so we shouldn't let other queries
                // wait for this query to be done.
                if store_to_cache {
                    let _ = this.insert(sql, StmtStatus::in_flight());
                }

                Either::Left(None)
            }
        }
    }

    pub fn checked_insert(
        &self,
        sql: &str,
        (stmt, meta): (StatementId, Arc<PgStatementMetadata>),
    ) -> Option<(StatementId, Arc<PgStatementMetadata>)> {
        let mut this = self.lock();

        if !this.is_enabled() {
            return None;
        }

        match this.insert(sql, StmtStatus::Cached(stmt, meta)) {
            Some(StmtStatus::InFlight(wait_event)) => {
                // Once we're done preparing the statement and have inserted it in cache we can
                // notify all tasks that are waiting.
                wait_event.notify_all();
                None
            }
            Some(StmtStatus::Cached(stmt, meta)) => Some((stmt, meta)),
            None => None,
        }
    }

    pub fn remove_and_notify(&self, sql: &str) {
        let mut this = self.lock();

        if let Some(StmtStatus::InFlight(wait_event)) = this.remove(sql) {
            wait_event.notify_all();
        }
    }

    pub fn cached_statements_size(&self) -> usize {
        let this = self.lock();
        this.iter()
            .filter(|(_, v)| matches!(v, StmtStatus::Cached { .. }))
            .count()
    }
}
