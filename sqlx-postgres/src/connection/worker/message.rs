use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use sqlx_core::{io::ProtocolEncode, Error};

use crate::message::{self, BackendMessageFormat, EncodeMessage, FrontendMessage, ReceivedMessage};

/// Pipes responses from the background worker to the sender until one of these conditions is met.
#[derive(Debug, PartialEq)]
pub enum PipeUntil {
    /// Waits until the worker got n number of messages. In general this should only be used if the
    /// number of responses is 0. There could be any types of request be sent asynchronously, like
    /// [NotificationResponse], [ParameterStatus] or [NoticeResponse].
    NumResponses(usize),
    /// Worker waits until it received a [ReadyForQuery] response.
    ReadyForQuery,
    /// Worker waits until it received one of these responses.
    RfqOr(BackendMessageFormat),
}

/// A request for the background worker.
#[derive(Debug)]
pub struct IoRequest {
    pub chan: UnboundedSender<ReceivedMessage>,
    pub data: Vec<u8>,
    pub send_until: PipeUntil,
}

impl IoRequest {
    pub fn handle_done(&mut self, format: BackendMessageFormat) -> bool {
        match self.send_until {
            PipeUntil::NumResponses(ref mut num) => {
                // num can never be 0 here because a `PipeUntil::NumResponses` that needs 0
                // responses is never pushed into the backlog.
                *num -= 1;
                *num == 0
            }
            PipeUntil::ReadyForQuery => format == BackendMessageFormat::ReadyForQuery,
            PipeUntil::RfqOr(wanted_msg) => {
                wanted_msg == format || format == BackendMessageFormat::ReadyForQuery
            }
        }
    }
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

    pub fn finish(self, ends_at: PipeUntil) -> (IoRequest, UnboundedReceiver<ReceivedMessage>) {
        // We're using an unbounded channel here for sending responses back mostly for
        // convenience. Should this be changed to a bounded one?
        let (chan, receiver) = unbounded();

        let req = IoRequest {
            send_until: ends_at,
            data: self.data,
            chan,
        };

        (req, receiver)
    }
}
