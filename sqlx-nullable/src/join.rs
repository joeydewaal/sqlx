use std::collections::HashSet;

use anyhow::Context as _;
use sqlparser::ast::{JoinConstraint, JoinOperator, Select};

use crate::{context::Context, join_resolver::JoinResolver, Table, TableId};

impl Context {
    pub fn update_from_join(&mut self, select: &Select) -> anyhow::Result<Vec<JoinResolver>> {
        let mut resolvers = Vec::new();

        for table in &select.from {
            if table.joins.is_empty() {
                continue;
            }

            let base_table = self
                .find_table_by_table_factor(&table.relation)
                .context(format!("Could not find {:?}", table.relation))?;

            let mut join_resolver = JoinResolver::from_base(base_table.table_id);

            for join in &table.joins {
                let left_table = self
                    .find_table_by_table_factor(&join.relation)
                    .context(format!("Could not find {:?}", join.relation))?;

                match &join.join_operator {
                    JoinOperator::LeftOuter(inner) => {
                        self.handle_join_constraint(
                            &mut join_resolver,
                            &inner,
                            &base_table,
                            &left_table,
                            |left_table, right_table, resolver| {
                                // println!("left joined {:?} on {:?}", &left_table, right_table);

                                for right_table in right_table {
                                    resolver.add_leaf(*right_table, left_table, None);
                                }
                                resolver.set_nullable(left_table, Some(true));
                            },
                        );
                    }
                    JoinOperator::Inner(inner) => {
                        self.handle_join_constraint(
                            &mut join_resolver,
                            &inner,
                            &base_table,
                            &left_table,
                            |left_table, right_table, resolver| {
                                // println!("inner joined {:?} on {:?}", &left_table, right_table);
                                for right_table in right_table {
                                    resolver.add_leaf(*right_table, left_table, None);
                                }
                                for r_table in right_table {
                                    if *r_table != left_table {
                                        resolver.set_nullable_if_base(*r_table, false);
                                    }
                                }
                            },
                        );
                    }
                    JoinOperator::CrossJoin => {
                        join_resolver.add_leaf(base_table.table_id, left_table.table_id, None);
                        join_resolver.set_nullable_if_base(base_table.table_id, false);
                    }
                    JoinOperator::RightOuter(inner) => {
                        self.handle_join_constraint(
                            &mut join_resolver,
                            &inner,
                            &base_table,
                            &left_table,
                            |left_table, right_table, resolver| {
                                // println!("right joined {:?} on {:?}", &left_table, right_table);
                                resolver.set_new_base(left_table);
                                for r_table in right_table {
                                    if *r_table != left_table {
                                        resolver.collapsing_set_nullable(*r_table, true);
                                    }
                                }
                                resolver.set_nullable(left_table, Some(false));
                            },
                        );
                    }
                    JoinOperator::FullOuter(inner) => {
                        self.handle_join_constraint(
                            &mut join_resolver,
                            &inner,
                            &base_table,
                            &left_table,
                            |left_table, right_table, resolver| {
                                // println!("right joined {:?} on {:?}", &left_table, right_table);
                                for right_table in right_table {
                                    resolver.add_leaf(*right_table, left_table, None);
                                }
                                for r_table in right_table {
                                    if *r_table != left_table {
                                        resolver.set_nullable(*r_table, Some(true));
                                    }
                                }
                                resolver.set_nullable(left_table, Some(true));
                            },
                        );
                    }
                    operator => unimplemented!("{operator:?}"),
                }
            }

            resolvers.push(join_resolver);
            // dbg!(&join_resolver);
            // let join_nullable = join_resolver.get_nullables();
            // dbg!(&join_nullable);
            // for (table_id, nullable) in join_nullable {
            //     self.wal.add_table(table_id, nullable);
            // }
        }
        Ok(resolvers)
    }

    fn handle_join_constraint(
        &mut self,
        join_resolver: &mut JoinResolver,
        constraint: &JoinConstraint,
        base_table: &Table,
        left_joined_table: &Table,
        callback: impl Fn(TableId, &[TableId], &mut JoinResolver),
    ) {
        match &constraint {
            JoinConstraint::On(expr) => {
                let mut t = HashSet::new();
                self.recursive_find_joined_tables(expr, &mut t);
                let right_tables: Vec<_> = t.into_iter().map(|t| t.table_id).collect();

                let left_table = right_tables
                    .iter()
                    .find(|table| **table == left_joined_table.table_id)
                    .unwrap();

                let _ = (callback)(*left_table, &right_tables, join_resolver);
            }
            JoinConstraint::Using(col_name) => {
                let right_tables: Vec<_> = self
                    .tables
                    .find_cols_by_idents(col_name)
                    .into_iter()
                    .map(|(_, t)| t.table_id)
                    .collect();
                let left_table = right_tables
                    .iter()
                    .find(|table| **table == left_joined_table.table_id)
                    .unwrap();

                for right_table in &right_tables {
                    join_resolver.add_leaf(*right_table, *left_table, None);
                }

                let _ = (callback)(*left_table, &right_tables, join_resolver);
            }
            JoinConstraint::Natural => {
                let right_tables = vec![base_table.table_id, left_joined_table.table_id];

                let left_table = right_tables
                    .iter()
                    .find(|table| **table == left_joined_table.table_id)
                    .unwrap();

                for right_table in &right_tables {
                    join_resolver.add_leaf(*right_table, *left_table, None);
                }

                let _ = (callback)(*left_table, &right_tables, join_resolver);
            }
            JoinConstraint::None => {
                panic!("not sure what to do here?");
            }
        }
    }
}
