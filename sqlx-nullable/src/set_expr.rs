use sqlparser::ast::SetExpr;

use crate::nullable::{GetNullable, StatementNullable};

impl GetNullable for SetExpr {
    fn nullable_for(
        context: &mut crate::context::Context,
        expr: &Self,
    ) -> anyhow::Result<crate::nullable::StatementNullable> {
        match expr {
            SetExpr::Select(ref select) => context.nullable_for(select),
            SetExpr::SetOperation { left, right, .. } => {
                let mut nullable = StatementNullable::new();
                nullable.combine(context.nullable_for(right)?);
                nullable.combine(context.nullable_for(left)?);
                Ok(nullable)
            }
            SetExpr::Values(values) => context.nullable_for(values),
            SetExpr::Insert(insert) => context.nullable_for(insert),
            SetExpr::Update(update) => context.nullable_for(update),
            SetExpr::Query(query) => context.nullable_for(query),
            SetExpr::Table(table) => context.nullable_for(table),
        }
    }
}
