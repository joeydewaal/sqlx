use std::sync::Arc;

use crate::{
    io::StatementId,
    message::{ParameterDescription, ParseComplete, RowDescription},
    recv_desc_params, recv_desc_rows,
    statement::PgStatementMetadata,
    PgConnection,
};

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

    pub(super) fn get_prepared(
        &mut self,
        sql: &str,
    ) -> Option<(StatementId, Arc<PgStatementMetadata>)> {
        self.conn
            .inner
            .cache_statement
            .get_mut(sql)
            .map(|(stmt_id, meta)| (*stmt_id, meta.clone()))
    }

    pub(super) fn is_preparing_this_iter(&self, sql: &str) -> bool {
        self.state.preparing.contains(sql)
    }

    pub(super) fn register_prepare(&mut self, sql: String) {
        self.state.preparing.insert(sql);
    }

    pub(super) fn next_stmt_id(&mut self) -> StatementId {
        let id = self.conn.inner.next_statement_id;
        self.conn.inner.next_statement_id = id.next();
        id
    }

    #[inline(always)]
    pub(super) async fn receive_parse_complete(&mut self) -> sqlx_core::Result<()> {
        self.conn
            .inner
            .stream
            .recv_expect::<ParseComplete>()
            .await?;
        Ok(())
    }

    pub(super) async fn handle_param_row_description(
        &mut self,
    ) -> sqlx_core::Result<(ParameterDescription, Option<RowDescription>)> {
        let parameters = recv_desc_params(self.conn).await?;

        let rows = recv_desc_rows(self.conn).await?;

        Ok((parameters, rows))
    }

    pub(super) async fn statement_metadata(
        &mut self,
        parameters: ParameterDescription,
        rows: Option<RowDescription>,
    ) -> sqlx_core::Result<Arc<PgStatementMetadata>> {
        let parameters = self.conn.handle_parameter_description(parameters).await?;

        let (columns, column_names) = self.conn.handle_row_description(rows, true).await?;
        let metadata = Arc::new(PgStatementMetadata {
            parameters,
            columns,
            column_names: Arc::new(column_names),
        });
        Ok(metadata)
    }

    pub(super) fn store_to_cache(
        &mut self,
        stmt_id: StatementId,
        sql: &str,
        metadata: Arc<PgStatementMetadata>,
    ) -> sqlx_core::Result<()> {
        // Store prepared statement in connection cache, remove old (push `Close` in write buffer).
        self.conn.store_to_cache(sql, (stmt_id, metadata.clone()))?;

        // Remove query from the shared cache, it is now cached so queries that are waiting can use
        // it.
        self.state.preparing.remove(sql);
        Ok(())
    }
}
