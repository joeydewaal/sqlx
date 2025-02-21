use std::sync::Arc;

use sqlx_core::from_row::FromRow;

use crate::{
    connection::describe::{TypCategory, TypType},
    type_info::{PgCustomType, PgType},
    types::Oid,
    PgRow, PgTypeInfo, PgTypeKind,
};

pub fn recursive_find(fetched: &mut Vec<FetchByOid>, oid: Oid) -> Option<PgTypeInfo> {
    if let Some(_built_in) = PgTypeInfo::try_from_oid(oid) {
        return Some(_built_in);
    }

    let found_row = fetched.swap_remove(fetched.iter().position(|r| oid == r.fetched_oid)?);
    let typ_type = TypType::try_from(found_row.typ_type);
    let category = TypCategory::try_from(found_row.category);

    match (typ_type, category) {
        (Ok(TypType::Domain), _) => {
            let domain_type = recursive_find(fetched, found_row.base_type)?;
            let d = PgTypeInfo(PgType::Custom(Arc::new(PgCustomType {
                oid,
                name: found_row.name.into(),
                kind: PgTypeKind::Domain(domain_type),
            })));

            Some(d)
        }

        (Ok(TypType::Base), Ok(TypCategory::Array)) => {
            todo!()
            // Ok(PgTypeInfo(PgType::Custom(Arc::new(PgCustomType {
            //     kind: PgTypeKind::Array(
            //         self.maybe_fetch_type_info_by_oid(element, true).await?,
            //     ),
            //     name: name.into(),
            //     oid,
            // }))))
        }

        (Ok(TypType::Pseudo), Ok(TypCategory::Pseudo)) => {
            let pseudo_type = PgTypeInfo(PgType::Custom(Arc::new(PgCustomType {
                oid: found_row.fetched_oid,
                name: found_row.name.into(),
                kind: PgTypeKind::Pseudo,
            })));
            Some(pseudo_type)
        }

        (Ok(TypType::Range), Ok(TypCategory::Range)) => {
            todo!()
            // self.fetch_range_by_oid(oid, name).await
        }

        (Ok(TypType::Enum), Ok(TypCategory::Enum)) => {
            let pg_enum = PgTypeInfo(PgType::Custom(Arc::new(PgCustomType {
                oid: found_row.fetched_oid,
                name: found_row.name.into(),
                kind: PgTypeKind::Enum(Arc::from(found_row.enum_labels)),
            })));

            Some(pg_enum)
        }

        (Ok(TypType::Composite), Ok(TypCategory::Composite)) => {
            let mut fields = Vec::new();

            for (name, col_oid) in found_row.composite_fields {
                fields.push((name, recursive_find(fetched, col_oid)?));
            }
            let composite_type = PgTypeInfo(PgType::Custom(Arc::new(PgCustomType {
                oid: found_row.fetched_oid,
                name: found_row.name.into(),
                kind: PgTypeKind::Composite(Arc::from(fields)),
            })));

            Some(composite_type)
        }

        _ => None,
    }
}

#[derive(Debug)]
pub struct FetchByOid {
    name: String,
    pub fetched_oid: Oid,
    typ_type: i8,
    category: i8,
    base_type: Oid,
    enum_labels: Vec<String>,
    composite_fields: Vec<(String, Oid)>,
    pub type_name: Option<String>,
}

impl FetchByOid {
    pub fn from_row(row: &PgRow) -> sqlx_core::Result<Self> {
        let row: (
            String,
            Oid,
            i8,
            i8,
            Oid,
            Vec<String>,
            Vec<(String, Oid)>,
            Option<String>,
        ) = FromRow::from_row(row)?;
        Ok(FetchByOid {
            name: row.0,
            fetched_oid: row.1,
            typ_type: row.2,
            category: row.3,
            base_type: row.4,
            enum_labels: row.5,
            composite_fields: row.6,
            type_name: row.7,
        })
    }
}
