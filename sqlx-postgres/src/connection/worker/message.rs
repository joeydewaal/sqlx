use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use sqlx_core::{io::ProtocolEncode, Error};

use crate::message::{self, BatchReceivedMessage, EncodeMessage, FrontendMessage, ReceivedMessage};

/// Pipes responses from the background worker to the sender until one of these conditions is met.
#[derive(Debug, PartialEq)]
pub enum PipeUntil {
    /// This is like a fire and forget action, it sends the bytes over the wire but doesn't wait
    /// for any responses.
    None,
    /// Worker sends responses back until it received a [ReadyForQuery] response.
    ReadyForQuery,
    /// Worker sends responses back until it either received a [CopyIn] or a
    /// [ReadyForQuery] response.
    ReadyForQueryOrCopyIn,
}

/// A request for the background worker.
#[derive(Debug)]
pub struct IoRequest {
    pub chan: UnboundedSender<BatchReceivedMessage>,
    pub data: Vec<u8>,
    pub pipe_until: PipeUntil,
}

/// TODO: We could share a `BytesMut` in the connection itself and use that as the backing buffer
/// instead of allocating a `Vec` every time, but this requires the `ProtocolEncode` trait to be
/// changed.
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
        value.encode(&mut self.data)
    }

    /// Writes a [Sync] message in the buffer and returns a `PipeUntil::ReadyForQuery` for
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

    pub(crate) fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn finish(self, pipe_until: PipeUntil) -> (IoRequest, UnboundedReceiver<BatchReceivedMessage>) {
        // We're using an unbounded channel here for sending responses back mostly for
        // convenience. Should this be changed to a bounded one?
        let (chan, receiver) = unbounded();

        let req = IoRequest {
            pipe_until,
            data: self.data,
            chan,
        };

        (req, receiver)
    }
}
