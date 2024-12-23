use std::collections::HashSet;

use anyhow::{anyhow, Context as _};
use sqlparser::ast::{Expr, Ident, TableFactor, TableWithJoins, With};

use crate::{
    expr::visit_expr,
    nullable::{Nullable, NullableResult},
    source::Source,
    wal::{Wal, WalEntry},
    SqlFlavour, Table, TableColumn, TableId, Tables,
};

pub struct Context {
    pub tables: Tables,
    pub source: Source,
    pub wal: Wal,
    pub flavour: SqlFlavour,
}

impl Context {
    pub fn new(tables: Tables, source: Source, wal: Wal, flavour: SqlFlavour) -> Context {
        Self {
            tables,
            source,
            wal,
            flavour,
        }
    }

    pub fn add_active_tables(&mut self, table: &TableWithJoins) -> anyhow::Result<()> {
        self.visit_table_factor(&&table.relation)?;
        for join_table in &table.joins {
            self.visit_table_factor(&&join_table.relation)?;
        }
        Ok(())
    }

    pub fn visit_table_factor(&mut self, table: &TableFactor) -> anyhow::Result<()> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let mut table = self
                    .source
                    .find_by_original_name(&name.0)
                    .context(format!("could not find table by original name: {name:?}"))?;
                table.add_alias(alias);
                self.push(table);
                Ok(())
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
            } => {
                let nullables = self.nullable_for(subquery)?;
                // dbg!(&nullables);
                let mut table = nullables.flatten(); //.to_table(alias);
                                                     //
                if let Some(alias) = alias {
                    for (col, col_name) in table.iter_mut().zip(alias.columns.clone()) {
                        col.column_name = Some(col_name);
                    }
                }
                // dbg!(&table);
                self.push(table.to_table(alias));
                Ok(())
            }
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: _,
                with_offset_alias: _,
                with_ordinality: _,
            } => {
                let mut table = Table::new(None);

                let nullable = {
                    let results: Vec<_> = array_exprs
                        .iter()
                        .map(|expr| visit_expr(expr, None, self))
                        .flatten()
                        .collect();

                    Nullable::new(results).nullable_index(0).unwrap_or(true)
                };

                if let Some(table_alias) = alias {
                    table = table.push_column(&table_alias.name.value, nullable)
                } else {
                    table = table.push_column("unnest", nullable)
                };
                self.push(table);
                Ok(())
            }
            rest => unimplemented!("{rest:#?}"),
        }
    }

    pub fn find_table_by_table_factor(&self, factor: &TableFactor) -> Option<Table> {
        match &factor {
            TableFactor::Table { name, alias, .. } => {
                if let Some(alias) = alias {
                    return self
                        .tables
                        .0
                        .iter()
                        .find(|t| t.table_name.as_deref() == Some(&[alias.name.clone()]))
                        .cloned();
                }
                self.tables.find_table_by_idents_table(&name.0).cloned()
            }
            _ => None,
        }
    }

    pub fn recursive_find_joined_tables(&self, expr: &Expr, tables: &mut HashSet<Table>) {
        match expr {
            Expr::CompoundIdentifier(idents) => {
                let table = self.tables.find_col_by_idents(&idents).unwrap();

                tables.insert(table.1.clone());
            }
            Expr::BinaryOp { left, op: _, right } => {
                self.recursive_find_joined_tables(&left, tables);
                self.recursive_find_joined_tables(&right, tables);
            }
            Expr::Subscript { expr, subscript: _ } => {
                self.recursive_find_joined_tables(expr, tables)
            }
            Expr::Value(_) => (),
            others => unimplemented!("{others:?}"),
        }
    }

    pub fn add_with(&mut self, with: &With) -> anyhow::Result<()> {
        for cte in &with.cte_tables {
            let _ = self.nullable_for(cte)?;
        }
        Ok(())
    }

    pub fn iter_tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.0.iter()
    }

    pub fn find_table_by_idents_table(&self, name: &[Ident]) -> Option<&Table> {
        self.tables
            .0
            .iter()
            .find(|t| t.table_name.as_deref() == Some(name))
    }

    pub fn nullable_for_table_col(
        &self,
        table: &Table,
        col: &TableColumn,
    ) -> anyhow::Result<NullableResult> {
        let col_name = col.column_name.clone();

        // check col nullable in wal
        if let Some(wal_nullable) = self.wal.nullable_for_col(table, col.column_id) {
            // println!("found col null {} {col_name:?}", wal_nullable);
            return Ok(NullableResult::new(Some(wal_nullable), col_name));
        }

        // check table nullable in wal
        if let Some(wal_nullable) = self.nullable_for_table(table) {
            // println!(
                // "found table null {} {col_name:?} {:?}",
                // wal_nullable, table.table_id
            // );
            if wal_nullable {
                return Ok(NullableResult::new(Some(wal_nullable), col_name));
            }
        }

        Ok(NullableResult::new(Some(col.catalog_nullable), col_name))
    }
    pub fn nullable_for_ident(&self, name: &[Ident]) -> anyhow::Result<NullableResult> {
        let (col, table) = self.find_col_by_idents(name)?;
        self.nullable_for_table_col(table, &col)
    }
    pub fn find_col_by_idents(&self, name: &[Ident]) -> anyhow::Result<(TableColumn, &Table)> {
        // search for col
        if name.len() == 1 {
            for table in self.tables.0.iter() {
                for col in &table.columns {
                    if col.column_name.as_ref() == Some(&name[0]) {
                        return Ok((col.clone(), table));
                    }
                }
            }
        }

        // look for original name: `table_alias`.`col_name`
        if let Some(table) = self
            .tables
            .0
            .iter()
            .find(|table| table.table_name.as_deref() == Some(&name[..name.len() - 1]))
        {
            if let Some(col) = table
                .columns
                .iter()
                .find(|column| column.column_name.as_ref() == name.last())
            {
                return Ok((col.clone(), table));
            }
        }

        // look for original name: `original_table_name`.`col_name`
        if let Some(table) = self
            .tables
            .0
            .iter()
            .find(|table| table.original_name.as_deref() == Some(&name[..name.len() - 1]))
        {
            if let Some(col) = table
                .columns
                .iter()
                .find(|column| column.column_name.as_ref() == name.last())
            {
                return Ok((col.clone(), table));
            }
        }

        return Err(anyhow!("Not found"));
    }

    pub fn push(&mut self, mut table: Table) {
        for cur_table in self.tables.0.iter() {
            // don't insert duplicate tables
            if cur_table.equals(&table) {
                return;
            }
        }

        table.table_id = TableId::new(self.tables.len());

        for col in table.columns.iter_mut() {
            col.table_id = table.table_id
        }

        self.tables.0.push(table)
    }

    pub fn nullable_for_table(&self, table: &Table) -> Option<bool> {
        for row in self.wal.data.iter().rev() {
            match row {
                WalEntry::TableNullable { table_id, nullable } if *table_id == table.table_id => {
                    return Some(*nullable)
                }
                _ => continue,
            }
        }
        None
    }
}
