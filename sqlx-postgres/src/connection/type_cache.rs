use crate::{type_info::PgArrayOf, types::Oid, PgTypeInfo};
use sqlx_core::ext::ustr::UStr;
use sqlx_core::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A cache of user defined types.
#[derive(Clone, Debug)]
pub(crate) struct TypeCache {
    inner: Arc<RwLock<TypeCacheInner>>,
}

#[derive(Debug)]
struct TypeCacheInner {
    // cache user-defined types by id <-> info
    cache_type_info: HashMap<Oid, PgTypeInfo>,
    cache_type_oid: HashMap<UStr, Oid>,
    cache_elem_type_to_array: HashMap<Oid, Oid>,
}

impl TypeCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(TypeCacheInner {
                cache_type_info: HashMap::new(),
                cache_type_oid: HashMap::new(),
                cache_elem_type_to_array: HashMap::new(),
            })
            .into(),
        }
    }

    fn write<'a>(&'a self) -> RwLockWriteGuard<'a, TypeCacheInner> {
        self.inner.write().expect("ERROR: failed to get write lock")
    }

    fn read<'a>(&'a self) -> RwLockReadGuard<'a, TypeCacheInner> {
        self.inner.read().expect("ERROR: failed to get read lock")
    }

    pub fn clear_oid_cache(&self) {
        let mut this = self.write();
        this.cache_type_oid.clear();
    }

    pub fn oid_by_name(&self, name: &str) -> Option<Oid> {
        let this = self.read();
        this.cache_type_oid.get(name).copied()
    }

    pub fn insert_named(&self, name: impl Into<String>, oid: Oid) {
        let mut this = self.write();

        this.cache_type_oid.insert(name.into().into(), oid);
    }

    pub fn array_oid_by_name(&self, array: &PgArrayOf) -> Option<Oid> {
        let this = self.read();

        let oid = this.cache_type_oid.get(&array.elem_name)?;
        this.cache_elem_type_to_array.get(oid).copied()
    }

    pub fn insert_array(&self, array: &PgArrayOf, elem_oid: Oid, array_oid: Oid) {
        let mut this = self.write();

        // Avoids copying `elem_name` until necessary
        this.cache_type_oid
            .entry_ref(&array.elem_name)
            .insert(elem_oid);
        this.cache_elem_type_to_array.insert(elem_oid, array_oid);
    }

    pub fn type_info_from_oid(&self, oid: &Oid) -> Option<PgTypeInfo> {
        let this = self.read();

        this.cache_type_info.get(oid).cloned()
    }

    pub fn insert_type_info(&self, oid: Oid, info: PgTypeInfo) {
        let mut this = self.write();

        let name = info.name().to_string();

        this.cache_type_info.insert(oid, info);
        this.cache_type_oid.insert(name.into(), oid);
    }
}
