use std::sync::Arc;

use parking_lot::Mutex;
use sqlx_core::{ext::ustr::UStr, HashMap};

use crate::{types::Oid, PgTypeInfo};

#[derive(Clone, Debug)]
pub struct TypeCache(Arc<Mutex<TypeCacheInner>>);

#[derive(Debug)]
pub(crate) struct TypeCacheInner {
    pub(crate) cache_type_info: HashMap<Oid, PgTypeInfo>,
    pub(crate) cache_type_oid: HashMap<UStr, Oid>,
    pub(crate) cache_elem_type_to_array: HashMap<Oid, Oid>,
}

impl TypeCache {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(TypeCacheInner {
            cache_type_info: HashMap::new(),
            cache_type_oid: HashMap::new(),
            cache_elem_type_to_array: HashMap::new(),
        })))
    }

    pub(crate) fn with_lock<F, O>(&self, callback: F) -> O
    where
        F: Fn(&mut TypeCacheInner) -> O,
    {
        let mut lock = self.0.lock();
        (callback)(&mut lock)
    }
}
