use std::ops::{ControlFlow, Deref, DerefMut};

use futures_channel::mpsc::UnboundedSender;
use sqlx_core::bytes::Buf;

use crate::connection::tls::MaybeUpgradeTls;
use crate::error::Error;
use crate::message::{
    BackendMessageFormat, EncodeMessage, FrontendMessage, Notification, ReceivedMessage,
};
use crate::net::{self, BufferedSocket, Socket};
use crate::PgConnectOptions;

// the stream is a separate type from the connection to uphold the invariant where an instantiated
// [PgConnection] is a **valid** connection to postgres

// when a new connection is asked for, we work directly on the [PgStream] type until the
// connection is fully established

// in other words, `self` in any PgConnection method is a live connection to postgres that
// is fully prepared to receive queries

pub struct PgStream {
    // A trait object is okay here as the buffering amortizes the overhead of both the dynamic
    // function call as well as the syscall.
    pub inner: BufferedSocket<Box<dyn Socket>>,

    // buffer of unreceived notification messages from `PUBLISH`
    // this is set when creating a PgListener and only written to if that listener is
    // re-used for query execution in-between receiving messages
    pub(crate) notifications: Option<UnboundedSender<Notification>>,
}

impl PgStream {
    pub(super) async fn connect(options: &PgConnectOptions) -> Result<Self, Error> {
        let socket_result = match options.fetch_socket() {
            Some(ref path) => net::connect_uds(path, MaybeUpgradeTls(options)).await?,
            None => net::connect_tcp(&options.host, options.port, MaybeUpgradeTls(options)).await?,
        };

        let socket = socket_result?;

        Ok(Self {
            inner: BufferedSocket::new(socket),
            notifications: None,
        })
    }

    #[inline(always)]
    pub(crate) fn write_msg(&mut self, message: impl FrontendMessage) -> Result<(), Error> {
        self.write(EncodeMessage(message))
    }

    pub(crate) async fn recv_unchecked(&mut self) -> Result<ReceivedMessage, Error> {
        // NOTE: to not break everything, this should be cancel-safe;
        // DO NOT modify `buf` unless a full message has been read
        self.inner
            .try_read(|buf| {
                // all packets in postgres start with a 5-byte header
                // this header contains the message type and the total length of the message
                let Some(mut header) = buf.get(..5) else {
                    return Ok(ControlFlow::Continue(5));
                };

                let format = BackendMessageFormat::try_from_u8(header.get_u8())?;

                let message_len = header.get_u32() as usize;

                let expected_len = message_len
                    .checked_add(1)
                    // this shouldn't really happen but is mostly a sanity check
                    .ok_or_else(|| {
                        err_protocol!("message_len + 1 overflows usize: {message_len}")
                    })?;

                if buf.len() < expected_len {
                    return Ok(ControlFlow::Continue(expected_len));
                }

                // `buf` SHOULD NOT be modified ABOVE this line

                // pop off the format code since it's not counted in `message_len`
                buf.advance(1);

                // consume the message, including the length prefix
                let mut contents = buf.split_to(message_len).freeze();

                // cut off the length prefix
                contents.advance(4);

                Ok(ControlFlow::Break(ReceivedMessage { format, contents }))
            })
            .await
    }
}

impl Deref for PgStream {
    type Target = BufferedSocket<Box<dyn Socket>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PgStream {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
