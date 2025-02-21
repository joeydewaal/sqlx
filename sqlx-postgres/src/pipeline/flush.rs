use crate::PgConnection;

use super::state::QueryState;

pub(super) struct FlushManager {
    has_flushed: bool,
}

impl FlushManager {
    pub(super) fn new() -> FlushManager {
        FlushManager { has_flushed: false }
    }

    pub(super) fn reset(&mut self) {
        self.has_flushed = false;
    }

    pub(super) async fn handle_flush(
        &mut self,
        query: &mut QueryState,
        conn: &mut PgConnection,
    ) -> sqlx_core::Result<()> {
        if !self.has_flushed && query.should_flush_before_next {
            conn.inner.stream.flush().await?;
            self.has_flushed = true;
        }
        query.should_flush_before_next = false;
        Ok(())
    }
}
