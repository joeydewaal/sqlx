use crate::{describe::Describe, message::Parse, types::Oid};
use std::{
    fmt::Debug,
    ops::DerefMut,
    sync::{Mutex, MutexGuard},
};

use futures_core::{future::BoxFuture, stream::BoxStream};
use sqlx_core::{
    executor::{Execute, Executor},
    Either, Error,
};
use state::{PipelineState, QueryState};

mod state;
mod cache;
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
    inner: Mutex<Option<MutabableInner<C>>>,
}

impl<C: Debug + DerefMut<Target = PgConnection>> Debug for Pipeline<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!();
    }
}

impl<C: DerefMut<Target = PgConnection>> Pipeline<C> {
    pub fn new(conn: C) -> Self {
        Self {
            inner: Mutex::new(Some(MutabableInner {
                conn,
                queries: Vec::new(),
                state: PipelineState::new()
            })),
        }
    }

    fn get_mut(&self) -> MutexGuard<'_, Option<MutabableInner<C>>> {
        self.inner.lock().unwrap()
    }

    pub async fn join(&self) -> sqlx_core::Result<()> {
        let lock = self.inner.lock().unwrap().take().unwrap();
        lock.join().await
    }
}

struct MutabableInner<C: DerefMut<Target = PgConnection>> {
    conn: C,
    queries: Vec<QueryState>,
    state: PipelineState
}

impl<'q, C: DerefMut<Target = PgConnection>> MutabableInner<C> {
    fn push(
        &mut self,
        mut query: impl Execute<'q, Postgres>,
    ) -> flume::Receiver<Option<Result<Either<PgQueryResult, PgRow>, Error>>> {
        let (tx, rx) = flume::unbounded();
        let args = query.take_arguments();
        let execute = QueryState {
            next_step: state::PipelineStep::GetOrPrepare,
            is_done: false,
            sql: query.sql().to_string(),
            arguments: args.unwrap().unwrap(),
            should_flush_before_next: false,
            sender: tx,
        };
        self.queries.push(execute);
        rx
    }

    pub async fn join(mut self) -> sqlx_core::Result<()> {
        let conn = self.conn.deref_mut();
        conn.wait_until_ready().await?;

        loop {
            let mut all_done = true;
            let mut has_flushed = false;
            let mut i: u128 = 0;
            for query in &mut self.queries {
                // println!("---- {i} {}", query.is_done);
                if query.is_done {
                    continue;
                } else {
                    all_done = false;
                }
                i += 1;
                if query.should_flush_before_next {
                    if !has_flushed {
                        // println!("is flushing");
                        conn.inner.stream.flush().await?;
                        has_flushed = true;
                    }

                    query.should_flush_before_next = false;
                }
                query.next(conn, &mut self.state).await?;
            }

            if all_done {
                break;
            }
        }

        Ok(())
    }

    pub async fn resolve_types(
        args: &[PgTypeInfo],
        conn: &mut PgConnection,
    ) -> sqlx_core::Result<Vec<Oid>> {
        let mut oids = Vec::new();
        for ty in args {
            oids.push(conn.resolve_type_id(&ty.0).await?);
        }
        Ok(oids)
    }

    fn start_parse(
        conn: &mut PgConnection,
        sql: &str,
        param_types: &[Oid],
    ) -> sqlx_core::Result<()> {
        let id = conn.inner.next_statement_id;
        conn.inner.next_statement_id = id.next();

        // next we send the PARSE command to the server
        conn.inner.stream.write_msg(Parse {
            param_types: &param_types,
            query: sql,
            statement: id,
        })?;
        Ok(())
    }
}

impl<'c, C: Send + Debug + Sync + DerefMut<Target = PgConnection>> Executor<'c>
    for &'c Pipeline<C>
{
    type Database = Postgres;

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        todo!()
    }

    fn fetch_optional<'e, 'q, E>(self, query: E) -> BoxFuture<'e, Result<Option<PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let mut pipeline = self.get_mut();
        let pipeline = Option::as_mut(&mut pipeline).expect("Dropped");
        let rx = pipeline.push(query);

        Box::pin(async move {
            let mut ret = None;
            while let Ok(Some(result)) = rx.recv_async().await {
                match result {
                    Ok(Either::Right(r)) if ret.is_none() => ret = Some(r),
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
