use sqlparser::ast::Ident;

use crate::Table;

#[derive(Debug, Clone)]
pub struct Source {
    tables: Vec<Table>,
    pub params: Vec<bool>,
    pub next_param_index: usize,
}

impl Source {
    pub fn new(tables: Vec<Table>) -> Self {
        Source {
            tables,
            params: Vec::new(),
            next_param_index: 0,
        }
    }

    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            params: Vec::new(),
            next_param_index: 0,
        }
    }

    pub fn find_by_original_name(&self, name: &[Ident]) -> Option<Table> {
        self.tables
            .iter()
            .find(|t| t.original_name.as_deref() == Some(name))
            .cloned()
    }

    pub fn push(&mut self, table: Table) {
        self.tables.push(table);
    }

    pub fn add_params(&mut self, mut params: Vec<bool>) {
        self.params.append(&mut params);
    }
}
