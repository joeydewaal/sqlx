use crate::describe::Describe;
use std::{
    fmt::Debug,
    ops::DerefMut,
    sync::{Mutex, MutexGuard},
};

use futures_core::{future::BoxFuture, stream::BoxStream};
use manager::PipelineManager;
use sqlx_core::{
    executor::{Execute, Executor},
    Either, Error,
};
use state::PipelineState;

mod cache;
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

use crate::{PgConnection, PgQueryResult, PgRow, PgStatement, PgTypeInfo, Postgres};

pub struct Pipeline<C: DerefMut<Target = PgConnection>> {
    inner: Mutex<Option<PipelineManager<C>>>,
}

impl<C: Debug + DerefMut<Target = PgConnection>> Debug for Pipeline<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!();
    }
}

impl<C: DerefMut<Target = PgConnection>> Pipeline<C> {
    pub fn new(conn: C) -> Self {
        Self {
            inner: Mutex::new(Some(PipelineManager::new(conn))),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<PipelineManager<C>>> {
        self.inner.lock().expect("Could not get `Pipeline` lock")
    }

    pub async fn join(&self) -> sqlx_core::Result<()> {
        let lock = self.lock().take().unwrap();
        lock.join().await
    }
}

impl<'c, C: Send + Debug + Sync + DerefMut<Target = PgConnection>> Executor<'c>
    for &'c Pipeline<C>
{
    type Database = Postgres;

    fn fetch_many<'e, 'q, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let mut pipeline = self.lock();
        let p = Option::as_mut(&mut pipeline).expect("Dropped");
        let rx = p.push(query);


        Box::pin(try_stream! {
            while let Ok(Some(v)) = rx.recv_async().await {
                let v = v?;
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q, E>(self, query: E) -> BoxFuture<'e, Result<Option<PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let mut pipeline = self.lock();
        let pipeline = Option::as_mut(&mut pipeline).expect("Dropped");
        let rx = pipeline.push(query);

        Box::pin(async move {
            let mut ret = None;
            while let Ok(Some(result)) = rx.recv_async().await {
                match result? {
                    Either::Right(r) if ret.is_none() => ret = Some(r),
                    _ => {}
                }
            }
            Ok(ret)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement<'q>, Error>>
    where
        'c: 'e,
    {
        todo!()
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        todo!()
    }
}
