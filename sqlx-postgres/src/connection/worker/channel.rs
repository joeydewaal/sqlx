use futures_channel::mpsc::UnboundedSender;

use super::{ConnManager, IoRequest};

pub struct WorkerConn {
    pub chan: UnboundedSender<IoRequest>,
}

impl WorkerConn {
    pub fn new(chan: UnboundedSender<IoRequest>) -> Self {
        Self { chan }
    }

    pub fn manager(&self) -> ConnManager {
        // FIXME: give a ref back
        ConnManager::new(self.chan.clone())
    }

    pub fn close(&self) {
        self.chan.close_channel();
    }
}
