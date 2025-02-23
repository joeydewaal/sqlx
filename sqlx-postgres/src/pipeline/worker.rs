use flume::{Receiver, Sender};
use futures_util::FutureExt;

use crate::PgConnection;

use super::context::PipelineContext;
use super::manager::{JoinManager, QueryManager};
use super::state::QueryState;

pub(super) enum Command {
    Close(Sender<PgConnection>),
    Query(QueryState),
}

pub(super) struct PipelineWorker {
    conn: PgConnection,
    rx: Receiver<Command>,
}

impl PipelineWorker {
    pub(super) fn new(conn: PgConnection) -> (Sender<Command>, Self) {
        let (tx, rx) = flume::unbounded();
        (tx, PipelineWorker { conn, rx })
    }

    async fn main_loop(mut self) {
        // TODO
        let mut context = PipelineContext::new(&mut self.conn);
        context.wait_until_ready().await.unwrap();

        let mut join_manager = JoinManager::new();

        let mut query_manager = QueryManager::new();

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
                            query_manager.push(query);
                        }
                    }
                },
                _ = query_manager.handle_next(&mut context, &mut join_manager).fuse() => {}
            }
        }
    }

    pub(super) fn spawn(self) {
        let _ = sqlx_core::rt::spawn(self.main_loop());
    }
}
