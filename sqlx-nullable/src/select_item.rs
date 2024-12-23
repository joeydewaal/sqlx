use sqlparser::ast::SelectItem;

use crate::{
    context::Context,
    expr::visit_expr,
    nullable::{GetNullable, Nullable, NullableResult},
};

impl GetNullable for Vec<SelectItem> {
    fn nullable_for(
        context: &mut crate::context::Context,
        items: &Self,
    ) -> anyhow::Result<crate::nullable::StatementNullable> {
        let mut nullable = Nullable::empty();
        for item in items {
            nullable.append(&mut visit_select_item(item, context)?);
        }
        Ok(nullable.into())
    }
}

pub fn visit_select_item(
    select_item: &SelectItem,
    context: &mut Context,
) -> anyhow::Result<Vec<NullableResult>> {
    match select_item {
        SelectItem::UnnamedExpr(expr) => Ok(vec![visit_expr(&expr, None, context)?]),
        SelectItem::ExprWithAlias { expr, alias } => {
            Ok(vec![visit_expr(&expr, Some(alias.clone()), context)?])
        }
        SelectItem::Wildcard(_) => {
            let mut results = Vec::new();

            for table in context.iter_tables() {
                for column in table.columns.iter() {
                    results.push(context.nullable_for_table_col(&table, &column)?);
                }
            }

            Ok(results)
        }
        SelectItem::QualifiedWildcard(table_name, _wildcard) => {
            let mut results = Vec::new();

            let table = context.find_table_by_idents_table(&table_name.0).unwrap();

            for column in &table.columns {
                results.push(context.nullable_for_table_col(table, column)?);
            }

            Ok(results)
        }
    }
}
