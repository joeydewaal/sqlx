use std::sync::Arc;

use sqlx_core::{ext::ustr::UStr, from_row::FromRow, query_as::query_as, Either, Error};

use crate::{
    connection::describe::{TypCategory, TypType},
    type_info::{PgCustomType, PgType},
    types::Oid,
    PgConnection, PgRow, PgTypeInfo, PgTypeKind,
};

// impl PgConnection {
//     async fn fetch_by_oid(&mut self, oid: Oid) -> Result<PgTypeInfo, Error> {
//         let mut recursive_values: Vec<FetchByOid> =
//             query_as::<_, (String, Oid, i8, i8, Oid, Vec<String>, Vec<(String, Oid)>)>(
//                 r#"
// WITH RECURSIVE fetch_type AS (
//     SELECT
//         oid::regtype::text AS name,
//         pg_type.oid,
//         typtype,
//         typcategory,
//         typrelid,
//         typelem,
//         typbasetype,
//         ARRAY(
//             SELECT enumlabel
//             FROM pg_enum
//             WHERE enumtypid = pg_type.oid
//         ) AS enum_labels,
//         ARRAY(
//             SELECT (attname, atttypid)
//             FROM pg_attribute AS attr
//             WHERE
//                 attr.attrelid = pg_type.typrelid
//                 AND NOT attr.attisdropped
//                 AND attr.attnum > 0
//         ) AS attr_oids
//     FROM
//         pg_type
//     WHERE
//         pg_type.oid = $1

//     UNION ALL

//     SELECT
//         t.oid::regtype::text AS name,
//         t.oid,
//         t.typtype,
//         t.typcategory,
//         t.typrelid,
//         t.typelem,
//         t.typbasetype,
//         ARRAY(
//             SELECT enumlabel
//             FROM pg_enum
//             WHERE enumtypid = t.oid
//         ) AS enum_labels,
//         ARRAY(
//             SELECT (attname, atttypid)
//             FROM pg_attribute AS attr
//             WHERE
//                 attr.attrelid = t.typrelid
//                 AND NOT attr.attisdropped
//                 AND attr.attnum > 0
//         ) AS attr_oids
//     FROM
//         pg_type t
//     INNER JOIN
//         fetch_type ft1
//         ON t.oid = ft1.typbasetype
//            OR t.oid = ft1.typelem
//            OR t.oid = ft1.typrelid
//            OR t.oid = ANY(ARRAY(
//                SELECT atttypid
//                FROM pg_attribute AS attr
//                WHERE
//                    attr.attrelid = ft1.typrelid
//                    AND NOT attr.attisdropped
//                    AND attr.attnum > 0
//            ))
// )
// SELECT
//     name,
//     oid,
//     typtype,
//     typcategory,
//     typbasetype,
//     enum_labels,
//     attr_oids
// FROM
//     fetch_type
// ORDER BY
//     oid
//         "#,
//             )
//             .bind(oid)
//             .fetch_all(&mut *self)
//             .await
//             .unwrap()
//             .into_iter()
//             .map(|row| FetchByOid {
//                 name: row.0,
//                 fetched_oid: row.1,
//                 typ_type: row.2,
//                 category: row.3,
//                 base_type: row.4,
//                 enum_labels: row.5,
//                 composite_fields: row.6,
//             })
//             .collect();

//         recursive_find(&mut recursive_values, oid).ok_or(Error::RowNotFound)
//     }
// }

pub fn recursive_find(
    fetched: &mut Vec<FetchByOid>,
    oid: Oid,
) -> Option<PgTypeInfo> {
    // println!("recursive {oid:?}");
    if let Some(_built_in) = PgTypeInfo::try_from_oid(oid) {
        return Some(_built_in);
    }

    // dbg!(&fetched, &oid);
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
