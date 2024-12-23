use sqlparser::ast::{Delete, FromTable};

use crate::{
    context::Context,
    nullable::{GetNullable, StatementNullable},
};

impl GetNullable for Delete {
    fn nullable_for(context: &mut Context, delete: &Self) -> anyhow::Result<StatementNullable> {
        match &delete.from {
            FromTable::WithFromKeyword(tables) => {
                for table in tables {
                    context.add_active_tables(table)?;
                }
            }
            FromTable::WithoutKeyword(_) => (),
        }

        if let Some(returning) = &delete.returning {
            return context.nullable_for(returning);
        }
        Ok(StatementNullable::new())
    }
}
