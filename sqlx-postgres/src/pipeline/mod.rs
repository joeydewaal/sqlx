use crate::{PgConnection, PgQueryResult, PgRow, Postgres};
use context::PipelineContext;
use manager::JoinManager;
use sqlx_core::{executor::Execute, Either, Error};
use state::QueryState;
use std::{
    fmt::Debug,
    ops::DerefMut,
    sync::{Mutex, MutexGuard},
};

use executor::ERROR_MSG;

mod cache;
mod context;
mod executor;
mod manager;
mod state;

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

pub struct PgPipeline<C: DerefMut<Target = PgConnection>> {
    inner: Mutex<Option<MutableInner<C>>>,
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
            inner: Mutex::new(Some(MutableInner::new(conn))),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<MutableInner<C>>> {
        self.inner.lock().expect("Could not get `Pipeline` lock")
    }

    pub async fn join(&self) -> sqlx_core::Result<()> {
        let lock = self.lock().take().expect(ERROR_MSG);
        lock.join().await
    }
}

pub(super) struct MutableInner<C: DerefMut<Target = PgConnection>> {
    conn: C,
    queries: Vec<Option<QueryState>>,
}

impl<'q, C: DerefMut<Target = PgConnection>> MutableInner<C> {
    pub(crate) fn new(conn: C) -> MutableInner<C> {
        MutableInner {
            conn,
            queries: Vec::new(),
        }
    }

    pub(crate) fn push(
        &mut self,
        mut query: impl Execute<'q, Postgres>,
    ) -> flume::Receiver<Option<Result<Either<PgQueryResult, PgRow>, Error>>> {
        let args = query.take_arguments();
        // TODO: Arguments
        let (q_state, rx) = QueryState::new(query.sql().to_string(), args.unwrap().unwrap());
        self.queries.push(Some(q_state));
        rx
    }

    pub async fn join(mut self) -> sqlx_core::Result<()> {
        // Manages the queries.
        let mut join_manager = JoinManager::new();

        // Holds the connection + state shared between queries.
        let mut context = PipelineContext::new(self.conn.deref_mut());

        // Make sure we are ready. Flush out the buffer, etc.
        context.wait_until_ready().await?;

        while !join_manager.is_done() {
            join_manager.setup();

            for opt_query in &mut self.queries {
                join_manager.handle_next(opt_query, &mut context).await?;
            }
        }

        Ok(())
    }
}
