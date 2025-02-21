use std::{
    fmt::Debug,
    ops::DerefMut,
    sync::{Mutex, MutexGuard},
};

use executor::ERROR_MSG;
use manager::PipelineManager;
mod cache;
mod executor;
mod manager;
mod state;
mod flush;

// Parse -> Bind -> Execute -> Close

// -- Create prepared statement --
// Resolve types for OID -> OIDS
// Parse (OID's, query, statement_id) -> ()
//
// Describe (statement_id) -> RowDescription, ParamDescription (PgMetadata)
//      - Fetch by oid)
//
// --
// Apply patches (Parameters)
//      - Fetch by OID
//
// Bind (portal, statement_id, num_params, params) -> ()
//
// Execute (portal, limit) -> ()
//
// Close Portal (portal) -> ()

// Pipelining
// 1. Fetch custom types for all queries
//      - Resolve OIDS (Query)
//      - Apply patches
//
// 2. Push Parse + Describe in buffer if needed (prepare the statement)
//      - Parse
//      - Describe
//
// 3. Push Bind with portal, and args + execute

use crate::PgConnection;

pub struct PgPipeline<C: DerefMut<Target = PgConnection>> {
    inner: Mutex<Option<PipelineManager<C>>>,
}

impl<C: Debug + DerefMut<Target = PgConnection>> Debug for PgPipeline<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // FIXME give better debug msg
        write!(f, "PgPipeline")
    }
}

impl<C: DerefMut<Target = PgConnection>> PgPipeline<C> {
    pub fn new(conn: C) -> Self {
        Self {
            inner: Mutex::new(Some(PipelineManager::new(conn))),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<PipelineManager<C>>> {
        self.inner.lock().expect("Could not get `Pipeline` lock")
    }

    pub async fn join(&self) -> sqlx_core::Result<()> {
        let lock = self.lock().take().expect(ERROR_MSG);
        lock.join().await
    }
}
