use sqlparser::ast::Values;

use crate::{
    context::Context,
    expr::visit_expr,
    nullable::{GetNullable, Nullable, StatementNullable},
};

impl GetNullable for Values {
    fn nullable_for(context: &mut Context, values: &Self) -> anyhow::Result<StatementNullable> {
        let mut statement = StatementNullable::new();
        for row in &values.rows {
            let mut nullables = Nullable::empty();

            for col in row {
                let nullable = visit_expr(&col, None, context)?;
                nullables.push(nullable);
            }
            statement.push(nullables);
        }
        Ok(statement)
    }
}
