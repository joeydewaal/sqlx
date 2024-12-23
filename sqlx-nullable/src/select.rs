use sqlparser::ast::Select;

use crate::{
    context::Context,
    nullable::{GetNullable, Nullable},
    select_item::visit_select_item,
};

impl GetNullable for Select {
    fn nullable_for(
        context: &mut Context,
        select: &Self,
    ) -> anyhow::Result<crate::nullable::StatementNullable> {
        for table in &select.from {
            context.add_active_tables(table)?;
        }

        let mut resolvers = context.update_from_join(select)?;
        // dbg!(&resolvers);
        context.update_from_where(select, &mut resolvers)?;
        // dbg!(&resolvers);
        // dbg!(&context.tables);
        // dbg!(&context.wal);

        for join_resolver in resolvers {
            // dbg!(&join_resolver);
            let join_nullable = join_resolver.get_nullables();
            // dbg!(&join_nullable);
            for (table_id, nullable) in join_nullable {
                context.wal.add_table(table_id, nullable);
            }
        }

        let n: Vec<_> = select
            .projection
            .iter()
            .map(|c| visit_select_item(c, context).unwrap())
            .flatten()
            .collect();

        Ok(Nullable::new(n).into())
    }
}
