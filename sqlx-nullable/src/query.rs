use sqlparser::ast::Query;

use crate::{
    context::Context,
    nullable::{GetNullable, StatementNullable},
};

impl GetNullable for Query {
    fn nullable_for(context: &mut Context, query: &Self) -> anyhow::Result<StatementNullable> {
        if let Some(with) = &query.with {
            context.add_with(&with)?;
        }

        context.nullable_for(&query.body)
    }
}
