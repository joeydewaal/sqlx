use futures_channel::mpsc::UnboundedSender;

use super::IoRequest;

pub struct WorkerConn {
    pub chan: UnboundedSender<IoRequest>,
}

impl WorkerConn {
    pub fn new(chan: UnboundedSender<IoRequest>) -> Self {
        Self { chan }
    }

    pub fn close(&self) {
        self.chan.close_channel();
    }
}
