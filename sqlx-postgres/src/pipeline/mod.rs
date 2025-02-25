use crate::PgConnection;
use flume::Sender;
use futures::TryFutureExt;
use sqlx_core::Error;
use std::fmt::Debug;
use worker::{Command, PipelineWorker};

mod cache;
mod context;
mod executor;
mod manager;
mod state;
mod worker;

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

pub struct PgPipeline {
    tx: Sender<Command>,
}

impl Debug for PgPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // FIXME give better debug msg
        write!(f, "PgPipeline")
    }
}

impl PgPipeline {
    pub fn new(conn: PgConnection) -> Self {
        let (tx, worker) = PipelineWorker::new(conn);
        worker.spawn();
        Self { tx }
    }

    pub async fn close(self) -> sqlx_core::Result<PgConnection> {
        let (tx, rx) = flume::bounded(1);
        self.tx
            .send(Command::Close(tx))
            .map_err(|_| Error::WorkerCrashed)?;

        let conn = rx.recv_async().map_err(|_| Error::WorkerCrashed).await?;
        Ok(conn)
    }
}
