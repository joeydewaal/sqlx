use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use sqlx_core::{io::ProtocolEncode, Error};

use crate::message::{self, EncodeMessage, FrontendMessage, ReceivedMessage};

#[derive(Debug, PartialEq)]
pub enum WaitType {
    NumMessages { num_responses: usize },
    ReadyForQuery,
}

#[derive(Debug)]
pub struct IoRequest {
    pub chan: UnboundedSender<ReceivedMessage>,
    pub data: Vec<u8>,
    pub ends_at: WaitType,
}

impl IoRequest {
    pub fn decrease_num_request(&mut self) {
        match &mut self.ends_at {
            WaitType::NumMessages { num_responses } => {
                *num_responses -= 1;
            }
            _ => {}
        }
    }
}

pub struct MessageBuf {
    num_messages: usize,
    data: Vec<u8>,
}

impl MessageBuf {
    pub fn new() -> Self {
        Self {
            num_messages: 0,
            data: Vec::new(),
        }
    }
    #[inline(always)]
    pub fn write<'en, T>(&mut self, value: T) -> sqlx_core::Result<()>
    where
        T: ProtocolEncode<'en, ()>,
    {
        self.write_with(value, ())
    }

    #[inline(always)]
    pub fn write_with<'en, T, C>(&mut self, value: T, context: C) -> sqlx_core::Result<()>
    where
        T: ProtocolEncode<'en, C>,
    {
        value.encode_with(&mut self.data, context)?;
        self.inc_response_count();
        Ok(())
    }

    pub fn write_sync(&mut self) {
        self.write_msg(message::Sync)
            .expect("BUG: Sync should not be too big for protocol");
    }

    pub fn inc_response_count(&mut self) {
        self.num_messages += 1;
    }
    #[inline(always)]
    pub(crate) fn write_msg(&mut self, message: impl FrontendMessage) -> Result<(), Error> {
        self.write(EncodeMessage(message))
    }

    pub fn finish(self, ends_at: WaitType) -> (IoRequest, UnboundedReceiver<ReceivedMessage>) {
        let (tx, rx) = unbounded();
        let req = IoRequest {
            ends_at,
            data: self.data,
            chan: tx,
        };

        (req, rx)
    }
}
