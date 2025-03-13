use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use sqlx_core::{io::ProtocolEncode, Error};

use crate::message::{self, BackendMessageFormat, EncodeMessage, FrontendMessage, ReceivedMessage};

/// Pipes responses from the background worker until one of the variants.
#[derive(Debug, PartialEq)]
pub enum PipeUntil {
    /// Waits until the worker got n number of messages. In general this should only be used if the
    /// number of responses is 0. There could be any types of request be sent asynchronously, like
    /// [NotificationResponse], [ParameterStatus] or [NoticeResponse].
    NumResponses(usize),
    /// Waits until the worker received a [ReadyForQuery] response.
    ReadyForQuery,
    /// Waits until the worker received one of these responses.
    Either {
        left: BackendMessageFormat,
        right: BackendMessageFormat,
    },
}

/// A request for the background worker.
#[derive(Debug)]
pub struct IoRequest {
    pub chan: UnboundedSender<ReceivedMessage>,
    pub data: Vec<u8>,
    pub send_until: PipeUntil,
    pub id: usize,
}

pub struct MessageBuf {
    data: Vec<u8>,
}

impl MessageBuf {
    pub fn new() -> Self {
        Self { data: Vec::new() }
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
        value.encode_with(&mut self.data, context)
    }

    /// Writes a [Sync] message in the buffe and returns a `PipeUntil::ReadyForQuery` for
    /// convenience.
    #[inline(always)]
    pub fn write_sync(&mut self) -> sqlx_core::Result<PipeUntil> {
        self.write_msg(message::Sync)
            .expect("BUG: Sync should not be too big for protocol");
        Ok(PipeUntil::ReadyForQuery)
    }

    #[inline(always)]
    pub(crate) fn write_msg(&mut self, message: impl FrontendMessage) -> Result<(), Error> {
        self.write(EncodeMessage(message))
    }

    pub fn finish(self, ends_at: PipeUntil) -> (IoRequest, UnboundedReceiver<ReceivedMessage>) {
        let (chan, receiver) = unbounded();
        let req = IoRequest {
            id: 0,
            send_until: ends_at,
            data: self.data,
            chan,
        };

        (req, receiver)
    }
}
