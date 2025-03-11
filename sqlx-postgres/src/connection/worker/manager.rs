use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use sqlx_core::Error;
use std::str::FromStr;

use crate::{
    message::{
        BackendMessage, BackendMessageFormat, ParameterStatus, ReadyForQuery, ReceivedMessage,
    },
    PgDatabaseError,
};

use super::{message::WaitType, IoRequest, MessageBuf};

pub struct ConnManager {
    chan: UnboundedSender<IoRequest>,
    last_receiver: Option<UnboundedReceiver<ReceivedMessage>>,
}

impl ConnManager {
    pub fn new(chan: UnboundedSender<IoRequest>) -> Self {
        Self {
            chan,
            last_receiver: None,
        }
    }

    pub fn send_message<F>(&mut self, callback: F) -> sqlx_core::Result<()>
    where
        F: FnOnce(&mut MessageBuf) -> sqlx_core::Result<WaitType>,
    {
        let mut buffer = MessageBuf::new();
        let wait_type = (callback)(&mut buffer)?;
        let (request, receiver) = buffer.finish(wait_type);
        self.chan
            .unbounded_send(request)
            .map_err(|_| Error::WorkerCrashed)?;
        self.last_receiver = Some(receiver);
        Ok(())
    }
    // Expect a specific type and format
    pub async fn recv_expect<B: BackendMessage>(&mut self) -> Result<B, Error> {
        self.recv().await?.decode()
    }

    pub async fn recv_ready_for_query(&mut self) -> Result<(), Error> {
        let _: ReadyForQuery = self.recv_expect().await?;
        Ok(())
    }

    // wait for CloseComplete to indicate a statement was closed
    pub async fn wait_for_close_complete(&mut self, mut count: usize) -> Result<(), Error> {
        // we need to wait for the [CloseComplete] to be returned from the server
        while count > 0 {
            match self.recv().await? {
                message if message.format == BackendMessageFormat::PortalSuspended => {
                    // there was an open portal
                    // this can happen if the last time a statement was used it was not fully executed
                }

                message if message.format == BackendMessageFormat::CloseComplete => {
                    // successfully closed the statement (and freed up the server resources)
                    count -= 1;
                }

                message => {
                    return Err(err_protocol!(
                        "expecting PortalSuspended or CloseComplete but received {:?}",
                        message.format
                    ));
                }
            }
        }

        Ok(())
    }

    // Get the next message from the server
    // May wait for more data from the server
    pub(crate) async fn recv(&mut self) -> Result<ReceivedMessage, Error> {
        loop {
            println!("ReactorReceiver::recv");
            let x = self
                .last_receiver
                .as_mut()
                .ok_or(sqlx_core::Error::WorkerCrashed)?;

            let message = x.next().await.ok_or(Error::WorkerCrashed)?;
            println!("fg worker got: {message:?}");

            match message.format {
                BackendMessageFormat::ErrorResponse => {
                    // An error returned from the database server.
                    return Err(message.decode::<PgDatabaseError>()?.into());
                }

                BackendMessageFormat::NotificationResponse => {
                    // if let Some(buffer) = &mut self.notifications {
                    //     let notification: Notification = message.decode()?;
                    //     let _ = buffer.send(notification).await;

                    continue;
                    // }
                }

                BackendMessageFormat::ParameterStatus => {
                    // informs the frontend about the current (initial)
                    // setting of backend parameters

                    let ParameterStatus { name, value } = message.decode()?;
                    // TODO: handle `client_encoding`, `DateStyle` change

                    match name.as_str() {
                        "server_version" => {
                            // self.server_version_num = parse_server_version(&value);
                        }
                        _ => {
                            // self.parameter_statuses.insert(name, value);
                        }
                    }

                    continue;
                }

                BackendMessageFormat::NoticeResponse => {
                    // do we need this to be more configurable?
                    // if you are reading this comment and think so, open an issue

                    // let notice: Notice = message.decode()?;

                    // let (log_level, tracing_level) = match notice.severity() {
                    //     PgSeverity::Fatal | PgSeverity::Panic | PgSeverity::Error => {
                    //         (Level::Error, tracing::Level::ERROR)
                    //     }
                    //     PgSeverity::Warning => (Level::Warn, tracing::Level::WARN),
                    //     PgSeverity::Notice => (Level::Info, tracing::Level::INFO),
                    //     PgSeverity::Debug => (Level::Debug, tracing::Level::DEBUG),
                    //     PgSeverity::Info | PgSeverity::Log => (Level::Trace, tracing::Level::TRACE),
                    // };

                    // let log_is_enabled = log::log_enabled!(
                    //     target: "sqlx::postgres::notice",
                    //     log_level
                    // ) || sqlx_core::private_tracing_dynamic_enabled!(
                    //     target: "sqlx::postgres::notice",
                    //     tracing_level
                    // );
                    // if log_is_enabled {
                    //     sqlx_core::private_tracing_dynamic_event!(
                    //         target: "sqlx::postgres::notice",
                    //         tracing_level,
                    //         message = notice.message()
                    //     );
                    // }

                    continue;
                }

                _ => {}
            }

            return Ok(message);
        }
    }
}

// reference:
// https://github.com/postgres/postgres/blob/6feebcb6b44631c3dc435e971bd80c2dd218a5ab/src/interfaces/libpq/fe-exec.c#L1030-L1065
fn parse_server_version(s: &str) -> Option<u32> {
    let mut parts = Vec::<u32>::with_capacity(3);

    let mut from = 0;
    let mut chs = s.char_indices().peekable();
    while let Some((i, ch)) = chs.next() {
        match ch {
            '.' => {
                if let Ok(num) = u32::from_str(&s[from..i]) {
                    parts.push(num);
                    from = i + 1;
                } else {
                    break;
                }
            }
            _ if ch.is_ascii_digit() => {
                if chs.peek().is_none() {
                    if let Ok(num) = u32::from_str(&s[from..]) {
                        parts.push(num);
                    }
                    break;
                }
            }
            _ => {
                if let Ok(num) = u32::from_str(&s[from..i]) {
                    parts.push(num);
                }
                break;
            }
        };
    }

    let version_num = match parts.as_slice() {
        [major, minor, rev] => (100 * major + minor) * 100 + rev,
        [major, minor] if *major >= 10 => 100 * 100 * major + minor,
        [major, minor] => (100 * major + minor) * 100,
        [major] => 100 * 100 * major,
        _ => return None,
    };

    Some(version_num)
}

#[cfg(test)]
mod tests {
    use super::parse_server_version;

    #[test]
    fn test_parse_server_version_num() {
        // old style
        assert_eq!(parse_server_version("9.6.1"), Some(90601));
        // new style
        assert_eq!(parse_server_version("10.1"), Some(100001));
        // old style without minor version
        assert_eq!(parse_server_version("9.6devel"), Some(90600));
        // new style without minor version, e.g.  */
        assert_eq!(parse_server_version("10devel"), Some(100000));
        assert_eq!(parse_server_version("13devel87"), Some(130000));
        // unknown
        assert_eq!(parse_server_version("unknown"), None);
    }
}
