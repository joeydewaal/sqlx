use sqlparser::ast::Select;

use crate::{context::Context, expr::get_nullable_col, join_resolver::JoinResolver};

impl Context {
    pub fn update_from_where(
        &mut self,
        select: &Select,
        join_resolvers: &mut [JoinResolver],
    ) -> anyhow::Result<()> {
        let Some(ref selection) = select.selection else {
            return Ok(());
        };

        get_nullable_col(selection, self, join_resolvers)?;
        Ok(())
    }
}
