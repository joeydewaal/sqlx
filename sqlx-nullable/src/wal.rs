use crate::{ColumnId, Table, TableId};

#[derive(Debug)]
pub enum WalEntry {
    TableNullable {
        table_id: TableId,
        nullable: bool,
    },
    ColumnNullable {
        table_id: TableId,
        column_id: ColumnId,
        nullable: bool,
    },
}

#[derive(Debug)]
pub struct Wal {
    pub data: Vec<WalEntry>,
}

impl Wal {
    pub fn new() -> Self {
        Self { data: vec![] }
    }

    pub fn add_table(&mut self, table_id: TableId, nullable: bool) {
        self.data
            .push(WalEntry::TableNullable { table_id, nullable });
    }

    pub fn add_column(&mut self, table_id: TableId, column_id: ColumnId, nullable: bool) {
        self.data.push(WalEntry::ColumnNullable {
            table_id,
            column_id,
            nullable,
        });
    }

    pub fn nullable_for_col(&self, table: &Table, _column_id: ColumnId) -> Option<bool> {
        for row in self.data.iter().rev() {
            match row {
                WalEntry::ColumnNullable {
                    table_id,
                    column_id,
                    nullable,
                } if *table_id == table.table_id && *column_id == _column_id => {
                    return Some(*nullable)
                }
                _ => continue,
            }
        }
        None
    }
}
