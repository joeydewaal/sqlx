use flume::{Receiver, Sender};
use futures_util::FutureExt;
use sqlx_core::{executor::Execute, Either, Error};

use crate::{PgConnection, PgQueryResult, PgRow, Postgres};

use super::context::PipelineContext;
use super::manager::JoinManager;
use super::state::QueryState;

pub(super) enum Command {
    Close(Sender<PgConnection>),
    Query(QueryState),
}

pub(super) struct PipelineWorker {
    conn: PgConnection,
    rx: Receiver<Command>,
    queries: Vec<Option<QueryState>>,
}

impl PipelineWorker {
    pub(super) fn new(conn: PgConnection) -> (Sender<Command>, Self) {
        let (tx, rx) = flume::unbounded();
        (
            tx,
            PipelineWorker {
                conn,
                rx,
                queries: Vec::new(),
            },
        )
    }

    async fn main_loop(mut self) {
        // TODO
        let mut context = PipelineContext::new(&mut self.conn);
        context.wait_until_ready().await.unwrap();

        let mut join_manager = JoinManager::new();

        loop {
            join_manager.setup();
            futures::select! {
                message = self.rx.recv_async().fuse() => {
                    let Ok(message) = message else {
                        break;
                    };
                    match message {
                        Command::Close(tx) => {
                            context.wait_until_ready().await.unwrap();
                            let _ = tx.send(self.conn);
                            return;
                        },
                        Command::Query(query) => {
                            self.queries.push(Some(query));
                        }
                    }
                },
                _ = Self::next(&mut context, &mut self.queries, &mut join_manager).fuse() => {

                }
            }
        }
    }

    async fn next<'c>(
        context: &mut PipelineContext<'c>,
        queries: &mut Vec<Option<QueryState>>,
        join_manager: &mut JoinManager,
    ) {
        for opt_query in queries {
            join_manager.handle_next(opt_query, context).await.unwrap();
        sqlx_core::rt::yield_now().await;
        }

    }

    pub(super) fn spawn(self) {
        let _ = sqlx_core::rt::spawn(self.main_loop());
    }

    pub(crate) fn push<'q>(
        &mut self,
        mut query: impl Execute<'q, Postgres>,
    ) -> flume::Receiver<Option<Result<Either<PgQueryResult, PgRow>, Error>>> {
        let args = query.take_arguments();

        // TODO: Arguments
        let (q_state, rx) = QueryState::new(query.sql().to_string(), args.unwrap().unwrap());

        // TODO: reuse old queries.
        self.queries.push(Some(q_state));
        rx
    }
}
