use sqlx_core::{
    bytes::{Buf, BytesMut},
    io::Codec,
};

use crate::message::{BackendMessageFormat, ReceivedMessage};

pub struct PostgresCodec;

impl PostgresCodec {
    pub fn new() -> Self {
        Self {}
    }
}

impl Codec for PostgresCodec {
    type Request = Vec<u8>;
    fn encode(&mut self, buf: &mut Vec<u8>, request: Self::Request) -> sqlx_core::Result<()> {
        buf.extend_from_slice(&request);
        Ok(())
    }

    type Response = ReceivedMessage;

    fn decode(&mut self, buf: &mut BytesMut) -> sqlx_core::Result<Option<Self::Response>> {
        // all packets in postgres start with a 5-byte header
        // this header contains the message type and the total length of the message
        let Some(mut header) = buf.get(..5) else {
            buf.reserve(5);
            return Ok(None);
        };

        let format = BackendMessageFormat::try_from_u8(header.get_u8())?;

        let message_len = header.get_u32() as usize;

        let expected_len = message_len
            .checked_add(1)
            // this shouldn't really happen but is mostly a sanity check
            .ok_or_else(|| err_protocol!("message_len + 1 overflows usize: {message_len}"))?;

        if buf.len() < expected_len {
            buf.reserve(expected_len);
            return Ok(None);
        }

        // `buf` SHOULD NOT be modified ABOVE this line

        // pop off the format code since it's not counted in `message_len`
        buf.advance(1);

        // consume the message, including the length prefix
        let mut contents = buf.split_to(message_len).freeze();

        // cut off the length prefix
        contents.advance(4);

        Ok(Some(ReceivedMessage { format, contents }))
    }
}
