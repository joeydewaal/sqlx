use crate::PgConnection;

use super::{context::PipelineContext, state::QueryState};

pub(super) struct JoinManager {
    has_flushed: bool,
}

impl JoinManager {
    pub(super) fn new() -> JoinManager {
        JoinManager {
            // Write buffer was flushe this iteration.
            has_flushed: false,
        }
    }

    #[inline(always)]
    pub(super) fn setup(&mut self) {
        self.has_flushed = false;
    }

    pub(super) async fn handle_next(
        &mut self,
        opt_query: &mut Option<QueryState>,
        context: &mut PipelineContext<'_>,
    ) -> sqlx_core::Result<()> {
        if let Some(query) = opt_query {
            // Flush buffer but only if we need to.
            self.handle_flush(query, context.conn).await?;

            // Call the state machine.
            query.handle_next(context).await?;

            if query.is_done {
                // Remove the state machine if the query is done.
                *opt_query = None;
            }
        }

        Ok(())
    }

    // Flushes the buffer but only if needed. This is only called once per iteration.
    async fn handle_flush(
        &mut self,
        query: &mut QueryState,
        conn: &mut PgConnection,
    ) -> sqlx_core::Result<()> {
        if !self.has_flushed && query.should_flush_before_next {
            // Flush the write buffer so the database can handle them.
            conn.inner.stream.flush().await?;

            // Make sure we don't flush again this iteration.
            self.has_flushed = true;
        }

        // Reset the flush status for the queries.
        query.should_flush_before_next = false;
        Ok(())
    }
}
