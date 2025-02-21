use std::ops::DerefMut;

use super::{
    flush::FlushManager,
    state::{PipelineState, QueryState},
};
use crate::{PgConnection, PgQueryResult, PgRow, Postgres};
use sqlx_core::{executor::Execute, Either, Error};

pub(super) struct PipelineManager<C: DerefMut<Target = PgConnection>> {
    all_done: bool,
    conn: C,
    queries: Vec<Option<QueryState>>,
    state: PipelineState,
}

impl<'q, C: DerefMut<Target = PgConnection>> PipelineManager<C> {
    pub(crate) fn new(conn: C) -> PipelineManager<C> {
        PipelineManager {
            all_done: false,
            conn,
            queries: Vec::new(),
            state: PipelineState::new(),
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
        let conn = self.conn.deref_mut();
        conn.wait_until_ready().await?;
        let mut flush_manager = FlushManager::new();

        while !self.all_done {
            flush_manager.reset();
            let mut all_done = true;

            for opt_query in &mut self.queries {
                if let Some(query) = opt_query {
                    all_done = false;
                    flush_manager.handle_flush(query, conn).await?;
                    query.handle_next(conn, &mut self.state).await?;

                    if query.is_done {
                        opt_query.take();
                    }
                }
            }

            if all_done {
                break;
            }
        }

        Ok(())
    }
}
