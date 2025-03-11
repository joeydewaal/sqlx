use std::sync::{Arc, Mutex, MutexGuard};

use sqlx_core::common::StatementCache;

use super::{PgStatementMetadata, StatementId};

pub struct SharedStatementCache {
    inner: Mutex<StatementCache<(StatementId, Arc<PgStatementMetadata>)>>,
}

impl SharedStatementCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(StatementCache::new(capacity)),
        }
    }

    pub fn lock<'c>(
        &'c self,
    ) -> MutexGuard<'c, StatementCache<(StatementId, Arc<PgStatementMetadata>)>> {
        self.inner.lock().expect("ERROR")
    }

    pub fn get(&self, stmt: &str) -> Option<(StatementId, Arc<PgStatementMetadata>)> {
        let mut this = self.lock();

        this.get_mut(stmt).cloned()
    }

    pub fn checked_insert(
        &self,
        sql: &str,
        statement: (StatementId, Arc<PgStatementMetadata>),
    ) -> Option<(StatementId, Arc<PgStatementMetadata>)> {
        let mut this = self.lock();

        if !this.is_enabled() {
            None
        } else {
            this.insert(sql, statement)
        }
    }

    pub fn cached_statements_size(&self) -> usize {
        let this = self.lock();
        this.len()
    }
}
