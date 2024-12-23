mod context;
mod cte;
mod delete;
mod expr;
mod func;
mod insert;
mod join;
mod join_resolver;
mod nullable;
mod params;
mod query;
mod select;
mod select_item;
mod set_expr;
mod source;
mod state;
mod statement;
mod table;
mod values;
mod wal;
mod where_;

pub use source::Source;
use sqlparser::dialect::{Dialect, PostgreSqlDialect, SQLiteDialect};
pub use state::NullableState;
pub use table::*;

#[derive(Debug, Clone, Copy)]
pub enum SqlFlavour {
    Postgres,
    Sqlite,
}

impl SqlFlavour {
    fn to_dialect(&self) -> &'static dyn Dialect {
        match self {
            SqlFlavour::Postgres => &PostgreSqlDialect {},
            SqlFlavour::Sqlite => &SQLiteDialect {},
        }
    }
}
