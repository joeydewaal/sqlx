use sqlparser::ast::Statement;

use crate::{
    context::Context,
    nullable::{GetNullable, StatementNullable},
};

impl GetNullable for Statement {
    fn nullable_for(context: &mut Context, statement: &Self) -> anyhow::Result<StatementNullable> {
        match statement {
            Statement::Query(query) => context.nullable_for(query),
            Statement::CreateTable(_)
            | Statement::CreateView { .. }
            | Statement::CreateIndex(_)
            | Statement::CreateType { .. }
            | Statement::CreateExtension { .. }
            | Statement::CreateRole { .. }
            | Statement::CreateSchema { .. }
            | Statement::Copy { .. }
            | Statement::Call(_)
            | Statement::Close { .. }
            | Statement::CreateVirtualTable { .. }
            | Statement::CreateSecret { .. }
            | Statement::Drop { .. }
            | Statement::DropFunction { .. }
            | Statement::DropProcedure { .. }
            | Statement::DropSecret { .. }
            | Statement::Declare { .. }
            | Statement::Fetch { .. }
            | Statement::Flush { .. }
            | Statement::SetTimeZone { .. }
            | Statement::ShowVariable { .. } => Ok(StatementNullable::new()),
            Statement::Update {
                table, returning, ..
            } => {
                if let Some(returning) = returning {
                    context.add_active_tables(table)?;
                    return context.nullable_for(returning);
                }
                Ok(StatementNullable::new())
            }
            Statement::Insert(insert) => context.nullable_for(insert),
            Statement::Delete(delete) => context.nullable_for(delete),
            _ => unimplemented!("{statement:?}"),
        }
    }
}
