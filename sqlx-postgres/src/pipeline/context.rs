use crate::PgConnection;

use super::state::PipelineState;

// Holds the connection and state that is shared between queries.
pub(super) struct PipelineContext<'c> {
    pub(super) state: PipelineState,
    pub(super) conn: &'c mut PgConnection,
}

impl<'c> PipelineContext<'c> {
    pub(super) fn new(conn: &'c mut PgConnection) -> PipelineContext<'c> {
        Self {
            state: PipelineState::new(),
            conn,
        }
    }

    pub(super) async fn wait_until_ready(&mut self) -> sqlx_core::Result<()> {
        self.conn.wait_until_ready().await
    }
}
