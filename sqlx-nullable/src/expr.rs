use anyhow::{anyhow, Context as _};
use sqlparser::ast::{BinaryOperator, CastKind, Expr, Ident, Value};

use crate::{
    context::Context,
    func::visit_func,
    join_resolver::JoinResolver,
    nullable::{Nullable, NullableResult},
    TableColumn,
};

pub fn visit_expr(
    expr: &Expr,
    alias: Option<Ident>,
    context: &mut Context,
) -> anyhow::Result<NullableResult> {
    match expr {
        Expr::CompoundIdentifier(idents) => {
            let value = context.nullable_for_ident(&idents)?.set_alias(alias);
            Ok(value)
        }
        Expr::Identifier(col_name) => {
            let value = context
                .nullable_for_ident(&[col_name.clone()])?
                .set_alias(alias);
            Ok(value)
        }
        Expr::Function(func) => {
            let o = visit_func(func, context)?.set_alias(alias);
            Ok(o)
        }
        Expr::Exists {
            subquery: _,
            negated: _,
        } => Ok(NullableResult::unnamed(Some(false))),
        Expr::Value(value) => match value {
            Value::Null => Ok(NullableResult::unnamed(Some(true)).set_alias(alias)),
            Value::Placeholder(param) => {
                Ok(NullableResult::unnamed(context.nullable_for_param(&param)?).set_alias(alias))
            }
            _ => Ok(NullableResult::unnamed(Some(false)).set_alias(alias)),
        },
        Expr::Cast {
            kind: CastKind::DoubleColon,
            expr,
            data_type: _,
            format: _,
        } => visit_expr(expr, alias, context),
        Expr::Tuple(_tuple) => Ok(NullableResult::unnamed(Some(false)).set_alias(alias)),
        Expr::Nested(nested) => visit_expr(&nested, alias, context),
        Expr::BinaryOp { left, op: _, right } => {
            let left_nullable = visit_expr(&left, alias.clone(), context)?;
            let right_nullable = visit_expr(&right, alias, context)?;

            if left_nullable.value == Some(false) && right_nullable.value == Some(false) {
                return Ok(NullableResult::unnamed(Some(false)));
            } else if left_nullable.value == Some(true) || right_nullable.value == Some(true) {
                return Ok(NullableResult::unnamed(Some(true)));
            } else {
                return Ok(NullableResult::unnamed(None));
            }
        }
        Expr::Subquery(query) => {
            let r = context
                .nullable_for(query)
                .map(|r| r.get_nullable().iter().any(|n| *n == Some(true)))?;
            Ok(NullableResult::unnamed(Some(r)).set_alias(alias))
        }
        Expr::Array(array) => {
            let mut nullable = Nullable::empty();
            for expr in &array.elem {
                nullable.push(visit_expr(expr, None, context)?);
            }
            nullable.to_result().ok_or(anyhow!("Geen output gevonden"))
        }
        Expr::CompositeAccess { expr, key } => visit_expr(expr, Some(key.clone()), context),
        Expr::InList {
            expr,
            list,
            negated: _,
        } => {
            let mut nullable = Nullable::new(vec![visit_expr(expr, alias, context)?]);
            for expr in list {
                nullable.push(visit_expr(expr, None, context)?);
            }
            nullable.to_result().ok_or(anyhow!("Geen output gevonden"))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: _,
        } => {
            let result = visit_expr(expr, alias, context)?;

            let mut nullable = context.nullable_for(subquery)?.flatten();
            nullable.push(result);

            nullable.to_result().ok_or(anyhow!("Geen output gevonden"))
        }
        Expr::InUnnest {
            expr,
            array_expr,
            negated: _,
        } => {
            let nullable = Nullable::new(vec![
                visit_expr(expr, alias, context)?,
                visit_expr(array_expr, None, context)?,
            ]);
            nullable.to_result().ok_or(anyhow!("Geen output gevonden"))
        }
        Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsUnknown(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::IsNotUnknown(_) => Ok(NullableResult::unnamed(Some(false)).set_alias(alias)),
        _ => unimplemented!("{:?}", expr),
    }
}

pub fn get_nullable_col(
    expr: &Expr,
    context: &mut Context,
    join_resolvers: &mut [JoinResolver],
) -> anyhow::Result<()> {
    match expr {
        Expr::IsNotNull(not_null) => {
            if let Some(column) = get_column(&not_null, context)? {
                context
                    .wal
                    .add_column(column.table_id, column.column_id, false);
                for t in join_resolvers {
                    t.set_nullable(column.table_id, Some(false));
                }
            }
            Ok(())
        }
        Expr::BinaryOp { left, op, right } => {
            if let (Some(left_col), Some(false)) = (
                get_column(&left, context)?,
                visit_expr(&right, None, context)?.value,
            ) {
                context
                    .wal
                    .add_column(left_col.table_id, left_col.column_id, false);
                for t in &mut *join_resolvers {
                    t.set_nullable(left_col.table_id, Some(false));
                }
            }

            if let (Some(right_col), Some(false)) = (
                get_column(&right, context)?,
                visit_expr(&left, None, context)?.value,
            ) {
                context
                    .wal
                    .add_column(right_col.table_id, right_col.column_id, false);
                for t in &mut *join_resolvers {
                    t.set_nullable(right_col.table_id, Some(false));
                }
            }

            if *op != BinaryOperator::And {
                return Ok(());
            }
            get_nullable_col(left, context, join_resolvers)?;
            get_nullable_col(right, context, join_resolvers)?;

            return Ok(());
        }
        Expr::CompoundIdentifier(_) => Ok(()),
        Expr::Identifier(_ident) => Ok(()),
        Expr::Value(_) => Ok(()),
        _ => unimplemented!("{expr:?}"),
    }
}

fn get_column(expr: &Expr, context: &mut Context) -> anyhow::Result<Option<TableColumn>> {
    match expr {
        Expr::CompoundIdentifier(idents) => {
            let (col, _table) = context
                .find_col_by_idents(&idents)
                .context(format!("table not found: {expr:?}"))?;
            Ok(Some(col))
        }
        Expr::Identifier(ident) => {
            let (col, _table) = context
                .find_col_by_idents(&[ident.clone()])
                .context(format!("table not found: {expr:?}"))?;
            Ok(Some(col))
        }
        _ => Ok(None),
    }
}
