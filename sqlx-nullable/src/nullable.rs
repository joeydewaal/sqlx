use sqlparser::ast::Ident;

use crate::{context::Context, Table, ToOptName};

#[derive(Debug, Clone)]
pub struct NullableResult {
    pub column_name: Option<Ident>,
    pub value: Option<bool>,
}

impl NullableResult {
    pub fn new(value: Option<bool>, column_name: Option<Ident>) -> Self {
        Self {
            value,
            column_name
        }
    }

    pub fn unnamed(value: Option<bool>) -> Self {
        Self {
            value,
            column_name: None,
        }
    }

    pub fn set_alias(mut self, alias: Option<Ident>) -> Self {
        if alias.is_some() {
            self.column_name = alias;
        }
        self
    }

    pub fn combine(&mut self, other: NullableResult) {
        self.value = match (self.value, other.value) {
            (Some(first), Some(second)) => Some(first || second),
            (Some(first), None) => Some(first),
            (None, Some(second)) => Some(second),
            (None, None) => None,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct Nullable(Vec<NullableResult>);

impl Nullable {
    pub fn empty() -> Self {
        Self::new(vec![])
    }

    pub fn append(&mut self, nullable: &mut Vec<NullableResult>) {
        self.0.append(nullable);
    }

    pub fn new(inner: Vec<NullableResult>) -> Self {
        Self(inner)
    }

    pub fn push(&mut self, null: NullableResult) {
        self.0.push(null);
    }

    pub fn to_result(mut self) -> Option<NullableResult> {
        let mut result = self.0.pop()?;
        for other in self.0 {
            result.combine(other);
        }
        Some(result)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut NullableResult> {
        self.0.iter_mut()
    }

    pub fn nullable(&self, col_name: &str, index: usize) -> Option<bool> {
        let col_name = Ident::new(col_name);

        if let Some((left_index, left_nullable)) = self.l_find_index(&col_name) {
            if let Some((right_index, _right_nullable)) = self.r_find_index(&col_name) {
                if left_index == right_index {
                    return left_nullable;
                }
            }
        }

        self.0[index].value
    }

    pub fn nullable_index(&self, index: usize) -> Option<bool> {
        self.0[index].value
    }

    fn l_find_index(&self, col_name: &Ident) -> Option<(usize, Option<bool>)> {
        self.0.iter().enumerate().find_map(|(index, nullable)| {
            if nullable.column_name.as_ref() == Some(col_name) {
                Some((index, nullable.value))
            } else {
                None
            }
        })
    }

    fn r_find_index(&self, col_name: &Ident) -> Option<(usize, Option<bool>)> {
        let mut index = self.0.len();

        while index != 0 {
            index -= 1;

            match &self.0[index].column_name {
                Some(name) if name == col_name => return Some((index, self.0[index].value)),
                _ => (),
            }
        }
        None
    }

    pub fn to_table(self, table_name: impl ToOptName) -> Table {
        let mut table = Table::new(table_name);

        for row in self.0 {
            table = table.push_column2(row.column_name, row.value.unwrap_or(true))
        }

        table
    }
}

#[derive(Debug)]
pub struct StatementNullable {
    nullables: Vec<Nullable>,
}

impl From<Nullable> for StatementNullable {
    fn from(value: Nullable) -> Self {
        Self {
            nullables: vec![value],
        }
    }
}

impl StatementNullable {
    pub fn new() -> Self {
        Self {
            nullables: Vec::new(),
        }
    }

    pub fn push(&mut self, nullable: Nullable) {
        self.nullables.push(nullable);
    }

    pub fn combine(&mut self, mut null: Self) {
        self.nullables.append(&mut null.nullables)
    }

    pub fn get_nullable(mut self) -> Vec<Option<bool>> {
        let Some(mut inferred_nullable): Option<Vec<Option<bool>>> = self
            .nullables
            .pop()
            .map(|e| e.0.into_iter().map(|e| e.value).collect())
        else {
            return vec![];
        };

        for row in self.nullables.iter() {
            for (i, col) in row.0.iter().enumerate() {
                inferred_nullable[i] = match (inferred_nullable[i], col.value) {
                    (Some(first), Some(second)) => Some(first || second),
                    (Some(first), None) => Some(first),
                    (None, Some(second)) => Some(second),
                    (None, None) => None,
                };
            }
        }
        inferred_nullable
    }

    pub fn flatten(mut self) -> Nullable {
        let Some(mut first) = self.nullables.pop() else {
            return Nullable::empty();
        };

        for row in self.nullables.into_iter() {
            for (i, col) in row.0.into_iter().enumerate() {
                let value = match (first.0[i].value, col.value) {
                    (Some(first), Some(second)) => Some(first || second),
                    (Some(first), None) => Some(first),
                    (None, Some(second)) => Some(second),
                    (None, None) => None,
                };

                if first.0[i].column_name.is_some() {
                    first.0[i].value = value;
                    continue;
                } else if col.column_name.is_some() {
                    first.0[i].column_name = col.column_name;
                    first.0[i].value = value;
                    continue;
                }
            }
        }
        first
    }

    pub fn get_nullable_final(self, cols: &[&str]) -> Vec<bool> {
        let nullables = self.flatten();

        let mut results = Vec::new();

        for (index, col) in cols.iter().enumerate() {
            results.push(nullables.nullable(col, index).unwrap_or(true));
        }

        results
    }
}

pub trait GetNullable {
    fn nullable_for(context: &mut Context, ty: &Self) -> anyhow::Result<StatementNullable>;
}

impl Context {
    pub fn nullable_for<T>(&mut self, ty: &T) -> anyhow::Result<StatementNullable>
    where
        T: GetNullable,
    {
        T::nullable_for(self, ty)
    }
}

impl<T> GetNullable for Box<T>
where
    T: GetNullable,
{
    fn nullable_for(context: &mut Context, ty: &Self) -> anyhow::Result<StatementNullable> {
        GetNullable::nullable_for(context, ty.as_ref())
    }
}
