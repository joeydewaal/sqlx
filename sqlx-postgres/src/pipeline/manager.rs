use std::ops::DerefMut;

use super::state::{PipelineState, QueryState};
use crate::{PgConnection, PgQueryResult, PgRow, Postgres};
use sqlx_core::{executor::Execute, Either, Error};

pub(crate) struct PipelineManager<C: DerefMut<Target = PgConnection>> {
    conn: C,
    queries: Vec<QueryState>,
    state: PipelineState,
}

impl<'q, C: DerefMut<Target = PgConnection>> PipelineManager<C> {
    pub(crate) fn new(conn: C) -> PipelineManager<C> {
        PipelineManager {
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
        let (q_state, rx) = QueryState::new(query.sql().to_string(), args.unwrap().unwrap());
        self.queries.push(q_state);
        rx
    }

    pub async fn join(mut self) -> sqlx_core::Result<()> {
        let conn = self.conn.deref_mut();
        conn.wait_until_ready().await?;

        loop {
            let mut all_done = true;
            let mut has_flushed = false;
            for query in &mut self.queries {
                if query.is_done {
                    continue;
                } else {
                    all_done = false;
                }
                if query.should_flush_before_next {
                    if !has_flushed {
                        // println!("is flushing");
                        conn.inner.stream.flush().await?;
                        has_flushed = true;
                    }

                    query.should_flush_before_next = false;
                }
                let result = query.next(conn, &mut self.state).await;
                if let Err(result) = result {
                    query.handle_error(&mut self.state, conn).await?;
                    // panic!("{result}");
                    query.is_done = true;
                    dbg!(&result);
                    let _ = query.sender.send(Some(Err(result)));
                }
            }

            if all_done {
                break;
            }
        }

        Ok(())
    }
}
