use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use sqlx_core::{common::StatementCache, rt::ManualResetEvent};

use super::{PgStatementMetadata, StatementId};

#[derive(Clone)]
pub enum StatementStatus {
    Cached {
        statement_id: StatementId,
        metadata: Arc<PgStatementMetadata>,
    },
    InFlight {
        semaphore: Arc<ManualResetEvent>,
    },
}

pub struct SharedStatementCache {
    inner: Mutex<StatementCache<StatementStatus>>,
}

impl SharedStatementCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(StatementCache::new(capacity)),
        }
    }

    pub fn lock<'c>(&'c self) -> MutexGuard<'c, StatementCache<StatementStatus>> {
        self.inner.lock().expect("ERROR")
    }

    pub fn remove_notify(&self, stmt: &str) {
        let mut this = self.lock();
        match this.get_mut(stmt) {
            Some(StatementStatus::InFlight { semaphore }) => {
                semaphore.set();
                let _ = this.remove(stmt);
            }
            _ => {}
        }
    }

    pub async fn get(&self, stmt: &str) -> Option<(StatementId, Arc<PgStatementMetadata>)> {
        for _ in 0..2 {
            let opt_semaphore: Option<Arc<ManualResetEvent>> = {
                let mut this = self.lock();
                if let Some(state) = this.get_mut(stmt).cloned() {
                    match state {
                        StatementStatus::InFlight { semaphore } => {
                            let waiting = semaphore.clone();
                            Some(waiting)
                        }
                        StatementStatus::Cached {
                            statement_id,
                            metadata,
                        } => {
                            return Some((statement_id, metadata));
                        }
                    }
                } else {
                    this.insert(
                        stmt,
                        StatementStatus::InFlight {
                            semaphore: Arc::new(ManualResetEvent::new(false)),
                        },
                    );
                    return None;
                }
            };

            if let Some(sem) = opt_semaphore {
                let result = sqlx_core::rt::timeout(Duration::from_secs(2), sem.wait()).await;
                if result.is_err() {
                }
            }
        }
        None
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
            let old = this.insert(
                sql,
                StatementStatus::Cached {
                    statement_id: statement.0,
                    metadata: statement.1,
                },
            );

            if let Some(old) = old {
                match old {
                    StatementStatus::Cached {
                        statement_id,
                        metadata,
                    } => return Some((statement_id, metadata)),
                    StatementStatus::InFlight { semaphore } => {
                        semaphore.set();
                        return None;
                    }
                }
            } else {
                None
            }
        }
    }

    pub fn cached_statements_size(&self) -> usize {
        let this = self.lock();
        this.len()
    }
}
