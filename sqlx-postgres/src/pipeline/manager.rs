use crate::PgConnection;

use super::{context::PipelineContext, state::QueryState};

pub(super) struct JoinManager {
    has_flushed: bool,
}

impl JoinManager {
    pub(super) fn new() -> JoinManager {
        JoinManager {
            // Write buffer was flushed this iteration.
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
    #[inline]
    async fn handle_flush(
        &mut self,
        query: &mut QueryState,
        conn: &mut PgConnection,
    ) -> sqlx_core::Result<()> {
        // Only flush the buffer if it hasn't been flushed this iteration and is needed.
        if !self.has_flushed && query.should_flush_before_next {
            conn.inner.stream.flush().await?;

            // Make sure we don't flush again this iteration.
            self.has_flushed = true;
        }

        // Reset the flush status for the queries.
        query.should_flush_before_next = false;
        Ok(())
    }
}

pub(super) struct QueryManager {
    queries: Vec<Option<QueryState>>,
}

impl QueryManager {
    pub(crate) fn new() -> QueryManager {
        QueryManager {
            queries: Vec::new(),
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, query: QueryState) {
        self.queries.push(Some(query));
    }

    #[inline]
    pub(crate) async fn handle_next<'c>(
        &mut self,
        context: &mut PipelineContext<'c>,
        join_manager: &mut JoinManager,
    ) {
        println!("iteration");
        for opt_query in &mut self.queries {
            join_manager.handle_next(opt_query, context).await.unwrap();
        }
    }
}
