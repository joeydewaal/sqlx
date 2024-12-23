use std::time::Instant;

use sqlparser::{ast::Statement, parser::Parser};

use crate::{context::Context, source::Source, wal::Wal, SqlFlavour, Tables};

pub struct NullableState {
    parsed_query: Vec<Statement>,
    source: Source,
    #[allow(unused)]
    started: Instant,
    flavour: SqlFlavour,
}

impl NullableState {
    pub fn new(query: &str, source: Source, flavour: SqlFlavour) -> Self {
        let query = Parser::parse_sql(flavour.to_dialect(), query).unwrap();

        Self {
            parsed_query: query,
            source,
            started: Instant::now(),
            flavour,
        }
    }

    pub fn get_nullable(&mut self, cols: &[&str]) -> Vec<bool> {
        // dbg!(&self.parsed_query);
        let s = self.parsed_query.first().unwrap();

        let mut context =
            Context::new(Tables::new(), self.source.clone(), Wal::new(), self.flavour);

        let inferred_nullable = context.nullable_for(s).unwrap();
        // println!("{:?}", self.started.elapsed());
        inferred_nullable.get_nullable_final(cols)
    }
}
