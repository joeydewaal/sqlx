use anyhow::Context as _;
use sqlparser::ast::Insert;

use crate::{
    context::Context,
    nullable::{GetNullable, StatementNullable},
};

impl GetNullable for Insert {
    fn nullable_for(context: &mut Context, insert: &Self) -> anyhow::Result<StatementNullable> {
        if let Some(returning) = &insert.returning {
            let table = context
                .source
                .find_by_original_name(&insert.table_name.0)
                .context("Could not find")?;
            context.push(table.clone());

            return context.nullable_for(returning);
        }
        Ok(StatementNullable::new())
    }
}
