use crate::TableId;

#[derive(Debug, Clone)]
pub struct JoinResolver {
    data: JoinEntry,
    leafs: Vec<JoinResolver>,
}

#[derive(Debug, Clone)]
pub struct JoinEntry {
    table_id: TableId,
    nullable: Option<bool>,
}

impl JoinResolver {
    pub fn from_base(table_id: TableId) -> Self {
        Self {
            data: JoinEntry {
                table_id,
                nullable: Some(false),
            },
            leafs: Vec::new(),
        }
    }

    pub fn add_leaf(&mut self, base: TableId, leaf_id: TableId, leaf_nullable: Option<bool>) {
        if base == leaf_id {
            return;
        } else if self.data.table_id == base {
            self.leafs.push(JoinResolver {
                data: JoinEntry {
                    table_id: leaf_id,
                    nullable: leaf_nullable,
                },
                leafs: Vec::new(),
            });
        } else {
            for leaf in &mut self.leafs {
                leaf.add_leaf(base, leaf_id, leaf_nullable);
            }
        }
    }

    pub fn set_nullable(&mut self, table_id: TableId, nullable: Option<bool>) {
        self.recursive_set_nullable(table_id, nullable, 1);
    }

    pub fn set_nullable_if_base(&mut self, table_id: TableId, nullable: bool) {
        if self.data.table_id == table_id {
            self.data.nullable = Some(nullable);
        }
    }

    pub fn set_new_base(&mut self, base: TableId) {
        let mut new_base = JoinResolver::from_base(base);
        new_base.leafs = vec![self.clone()];
        *self = new_base;
    }

    pub fn collapsing_set_nullable(&mut self, table_id: TableId, nullable: bool) {
        self.recursive_collapsing_set_nullable(table_id, nullable);
    }

    pub fn recursive_collapsing_set_nullable(&mut self, table_id: TableId, nullable: bool) -> bool {
        if self.data.table_id == table_id {
            self.data.nullable = Some(nullable);
            return true;
        }

        for t in &mut self.leafs {
            if t.recursive_collapsing_set_nullable(table_id, nullable) {
                self.data.nullable = Some(nullable);
                return true;
            }
        }
        return false;
    }

    // pub fn bubbling_not_null(&mut self, table_id: TableId) {
    //     println!("bubling {table_id:?}");
    //     self.recursive_bubbling_not_null(table_id);
    // }

    // pub fn recursive_bubbling_not_null(&mut self, table_id: TableId) -> bool {
    //     if self.data.table_id == table_id {
    //         self.data.nullable = Some(false);
    //         println!("setting {table_id:?} false");
    //         return true;
    //     }

    //     for table in &mut self.leafs {
    //         if table.recursive_bubbling_not_null(table_id) {
    //             self.data.nullable = Some(false);
    //             return true;
    //         }
    //     }
    //     return false;
    // }

    pub fn recursive_set_nullable(
        &mut self,
        table_id: TableId,
        nullable: Option<bool>,
        depth: usize,
    ) {
        if self.data.table_id == table_id {
            if depth == 1 && nullable.is_some() {
                // println!("setting table {table_id:?} to {nullable:?}");
                self.data.nullable = nullable;
            } else if depth != 1 {
                self.data.nullable = nullable;
                // println!("setting table {table_id:?} to {nullable:?}");
            }
            return;
        }
        for leaf in &mut self.leafs {
            leaf.recursive_set_nullable(table_id, nullable, depth + 1);
        }
    }

    pub fn get_nullables(self) -> Vec<(TableId, bool)> {
        let mut nullables = Vec::new();
        let null = Self::null(self.data.nullable.unwrap(), self.data.nullable);
        nullables.push((self.data.table_id, null));

        for leaf in self.leafs {
            leaf.r_nullables(null, &mut nullables);
        }

        nullables
    }

    fn r_nullables(self, parent_nullable: bool, nullables: &mut Vec<(TableId, bool)>) {
        let null = Self::null(parent_nullable, self.data.nullable);
        nullables.push((self.data.table_id, null));

        for leaf in self.leafs {
            leaf.r_nullables(null, nullables);
        }
    }

    fn null(parent_nullable: bool, nullable: Option<bool>) -> bool {
        if let Some(inferred) = nullable {
            return inferred;
        }

        parent_nullable
    }
}
