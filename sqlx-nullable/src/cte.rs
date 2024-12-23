use sqlparser::ast::Cte;

use crate::{context::Context, nullable::{GetNullable, StatementNullable}};

impl GetNullable for Cte {
    fn nullable_for(
        context: &mut Context,
        cte: &Self,
    ) -> anyhow::Result<crate::nullable::StatementNullable> {
        let nullable = context.nullable_for(&cte.query)?.flatten();

        let table = nullable.to_table(vec![cte.alias.name.clone()]);

        context.source.push(table);
        Ok(StatementNullable::new())
    }
}
