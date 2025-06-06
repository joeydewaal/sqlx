use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::message::TransactionStatus;

#[derive(Clone)]
pub struct Shared(Arc<Mutex<SharedInner>>);

impl Shared {
    pub fn new() -> Self {
        Shared(Arc::new(Mutex::new(SharedInner {
            transaction_status: TransactionStatus::default(),
            parameter_statuses: BTreeMap::new(),
            server_version_num: None,
        })))
    }
}

struct SharedInner {
    transaction_status: TransactionStatus,
    parameter_statuses: BTreeMap<String, String>,
    server_version_num: Option<u32>,
}

impl Shared {
    pub fn is_explain_available(&self) -> bool {
        let parameter_statuses = &self.0.lock().unwrap().parameter_statuses;
        let is_cockroachdb = parameter_statuses.contains_key("crdb_version");
        let is_materialize = parameter_statuses.contains_key("mz_version");
        let is_questdb = parameter_statuses.contains_key("questdb_version");
        !is_cockroachdb && !is_materialize && !is_questdb
    }

    pub fn set_server_version_num(&self, num: Option<u32>) {
        self.0.lock().unwrap().server_version_num = num
    }

    pub fn server_version_num(&self) -> Option<u32> {
        self.0.lock().unwrap().server_version_num
    }

    pub fn insert_parameter_status(&self, name: String, value: String) {
        let mut lock = self.0.lock().unwrap();
        lock.parameter_statuses.insert(name, value);
    }

    pub fn transaction_status(&self) -> TransactionStatus {
        self.0.lock().unwrap().transaction_status
    }

    pub fn set_transaction_status(&self, status: TransactionStatus) {
        self.0.lock().unwrap().transaction_status = status;
    }
}
