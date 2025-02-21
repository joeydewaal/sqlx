use crate::PgConnection;

use super::{context::PipelineContext, state::QueryState};

pub(super) struct JoinManager {
    has_flushed: bool,
    all_completed: bool,
}

impl JoinManager {
    pub(super) fn new() -> JoinManager {
        JoinManager {
            // Write buffer was flushe this iteration.
            has_flushed: false,
            // All queries are executed.
            all_completed: false,
        }
    }

    #[inline(always)]
    pub(super) fn is_done(&self) -> bool {
        self.all_completed
    }

    #[inline(always)]
    pub(super) fn set_not_completed(&mut self) {
        self.all_completed = false
    }

    #[inline(always)]
    pub(super) fn setup(&mut self) {
        self.has_flushed = false;
        self.all_completed = true
    }

    pub(super) async fn handle_next(
        &mut self,
        opt_query: &mut Option<QueryState>,
        context: &mut PipelineContext<'_>,
    ) -> sqlx_core::Result<()> {
        if let Some(query) = opt_query {
            // Make sure that we get run again.
            self.set_not_completed();

            // Flush buffer but only if we need to.
            self.handle_flush(query, context.conn).await?;

            // Call the state machine.
            query.handle_next(context).await?;

            // Remove the state machine if the query is done.
            if query.is_done {
                *opt_query = None;
            }
        } else {
            // Query done executing.
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
