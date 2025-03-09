use futures_channel::mpsc::UnboundedSender;

use super::worker::{ConnManager, IoRequest};

pub struct WorkerChan {
    chan: UnboundedSender<IoRequest>,
}

impl WorkerChan {
    pub fn new(chan: UnboundedSender<IoRequest>) -> Self {
        Self { chan }
    }

    pub fn manager(&self) -> ConnManager {
        // FIXME: give a ref back
        ConnManager::new(self.chan.clone())
    }
}
