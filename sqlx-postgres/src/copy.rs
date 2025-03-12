use std::borrow::Cow;
use std::ops::{Deref, DerefMut};

use futures_channel::mpsc::UnboundedReceiver;
use futures_core::future::BoxFuture;

use futures_core::stream::BoxStream;
use futures_core::Stream;
use sqlx_core::bytes::Bytes;

use crate::connection::worker::ConnManager;
use crate::connection::PgConnection;
use crate::error::{Error, Result};
use crate::message::{
    BackendMessageFormat, CommandComplete, CopyData, CopyDone, CopyFail, CopyInResponse,
    CopyOutResponse, CopyResponseData, Query, ReadyForQuery, ReceivedMessage,
};
use crate::pool::{Pool, PoolConnection};
use crate::{PipeUntil, Postgres};

impl PgConnection {
    /// Issue a `COPY FROM STDIN` statement and transition the connection to streaming data
    /// to Postgres. This is a more efficient way to import data into Postgres as compared to
    /// `INSERT` but requires one of a few specific data formats (text/CSV/binary).
    ///
    /// If `statement` is anything other than a `COPY ... FROM STDIN ...` command, an error is
    /// returned.
    ///
    /// Command examples and accepted formats for `COPY` data are shown here:
    /// <https://www.postgresql.org/docs/current/sql-copy.html>
    ///
    /// ### Note
    /// [PgCopyIn::finish] or [PgCopyIn::abort] *must* be called when finished or the connection
    /// will return an error the next time it is used.
    pub async fn copy_in_raw(&mut self, statement: &str) -> Result<PgCopyIn<&mut Self>> {
        PgCopyIn::begin(self, statement).await
    }

    /// Issue a `COPY TO STDOUT` statement and transition the connection to streaming data
    /// from Postgres. This is a more efficient way to export data from Postgres but
    /// arrives in chunks of one of a few data formats (text/CSV/binary).
    ///
    /// If `statement` is anything other than a `COPY ... TO STDOUT ...` command,
    /// an error is returned.
    ///
    /// Note that once this process has begun, unless you read the stream to completion,
    /// it can only be canceled in two ways:
    ///
    /// 1. by closing the connection, or:
    /// 2. by using another connection to kill the server process that is sending the data as shown
    ///    [in this StackOverflow answer](https://stackoverflow.com/a/35319598).
    ///
    /// If you don't read the stream to completion, the next time the connection is used it will
    /// need to read and discard all the remaining queued data, which could take some time.
    ///
    /// Command examples and accepted formats for `COPY` data are shown here:
    /// <https://www.postgresql.org/docs/current/sql-copy.html>
    #[allow(clippy::needless_lifetimes)]
    pub async fn copy_out_raw<'c>(
        &'c mut self,
        statement: &str,
    ) -> Result<BoxStream<'c, Result<Bytes>>> {
        let stream = pg_begin_copy_out(self, statement).await?;
        Ok(stream)
    }
}

/// Implements methods for directly executing `COPY FROM/TO STDOUT` on a [`PgPool`][crate::PgPool].
///
/// This is a replacement for the inherent methods on `PgPool` which could not exist
/// once the Postgres driver was moved out into its own crate.
pub trait PgPoolCopyExt {
    /// Issue a `COPY FROM STDIN` statement and begin streaming data to Postgres.
    /// This is a more efficient way to import data into Postgres as compared to
    /// `INSERT` but requires one of a few specific data formats (text/CSV/binary).
    ///
    /// A single connection will be checked out for the duration.
    ///
    /// If `statement` is anything other than a `COPY ... FROM STDIN ...` command, an error is
    /// returned.
    ///
    /// Command examples and accepted formats for `COPY` data are shown here:
    /// <https://www.postgresql.org/docs/current/sql-copy.html>
    ///
    /// ### Note
    /// [PgCopyIn::finish] or [PgCopyIn::abort] *must* be called when finished or the connection
    /// will return an error the next time it is used.
    fn copy_in_raw<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<PgCopyIn<PoolConnection<Postgres>>>>;

    /// Issue a `COPY TO STDOUT` statement and begin streaming data
    /// from Postgres. This is a more efficient way to export data from Postgres but
    /// arrives in chunks of one of a few data formats (text/CSV/binary).
    ///
    /// If `statement` is anything other than a `COPY ... TO STDOUT ...` command,
    /// an error is returned.
    ///
    /// Note that once this process has begun, unless you read the stream to completion,
    /// it can only be canceled in two ways:
    ///
    /// 1. by closing the connection, or:
    /// 2. by using another connection to kill the server process that is sending the data as shown
    ///    [in this StackOverflow answer](https://stackoverflow.com/a/35319598).
    ///
    /// If you don't read the stream to completion, the next time the connection is used it will
    /// need to read and discard all the remaining queued data, which could take some time.
    ///
    /// Command examples and accepted formats for `COPY` data are shown here:
    /// <https://www.postgresql.org/docs/current/sql-copy.html>
    fn copy_out_raw<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<impl Stream<Item = Result<Bytes>> + Send + 'static>>;
}

impl PgPoolCopyExt for Pool<Postgres> {
    fn copy_in_raw<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<PgCopyIn<PoolConnection<Postgres>>>> {
        Box::pin(async { PgCopyIn::begin(self.acquire().await?, statement).await })
    }

    fn copy_out_raw<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<impl Stream<Item = Result<Bytes>> + Send + 'static>> {
        Box::pin(async { pg_begin_copy_out(self.acquire().await?, statement).await })
    }
}

// (1 GiB - 1) - 1 - length prefix (4 bytes)
pub const PG_COPY_MAX_DATA_LEN: usize = 0x3fffffff - 1 - 4;

/// A connection in streaming `COPY FROM STDIN` mode.
///
/// Created by [PgConnection::copy_in_raw] or [Pool::copy_out_raw].
///
/// ### Note
/// [PgCopyIn::finish] or [PgCopyIn::abort] *must* be called when finished or the connection
/// will return an error the next time it is used.
#[must_use = "connection will error on next use if `.finish()` or `.abort()` is not called"]
pub struct PgCopyIn<C: DerefMut<Target = PgConnection>> {
    conn: Option<C>,
    response: CopyResponseData,
}

impl<C: DerefMut<Target = PgConnection>> PgCopyIn<C> {
    async fn begin(conn: C, statement: &str) -> Result<Self> {
        let mut manager = conn.pipe_message(|buff| {
            buff.write_msg(Query(statement))?;
            // FIXME
            Ok(PipeUntil::ReadyForQuery)
        })?;

        let response = match manager.recv_expect::<CopyInResponse>().await {
            Ok(res) => res.0,
            Err(e) => {
                // FIXME(JoeydeWaal): Should be ReadyForQuery???
                // conn.inner.stream.recv().await?;
                return Err(e);
            }
        };

        Ok(PgCopyIn {
            conn: Some(conn),
            response,
        })
    }

    /// Returns `true` if Postgres is expecting data in text or CSV format.
    pub fn is_textual(&self) -> bool {
        self.response.format == 0
    }

    /// Returns the number of columns expected in the input.
    pub fn num_columns(&self) -> usize {
        assert_eq!(
            self.response.num_columns.unsigned_abs() as usize,
            self.response.format_codes.len(),
            "num_columns does not match format_codes.len()"
        );
        self.response.format_codes.len()
    }

    /// Check if a column is expecting data in text format (`true`) or binary format (`false`).
    ///
    /// ### Panics
    /// If `column` is out of range according to [`.num_columns()`][Self::num_columns].
    pub fn column_is_textual(&self, column: usize) -> bool {
        self.response.format_codes[column] == 0
    }

    /// Send a chunk of `COPY` data.
    ///
    /// The data is sent in chunks if it exceeds the maximum length of a `CopyData` message (1 GiB - 6
    /// bytes) and may be partially sent if this call is cancelled.
    ///
    /// If you're copying data from an `AsyncRead`, maybe consider [Self::read_from] instead.
    pub async fn send(&mut self, data: impl Deref<Target = [u8]>) -> Result<&mut Self> {
        for chunk in data.deref().chunks(PG_COPY_MAX_DATA_LEN) {
            let conn = self.conn.as_deref().expect("");
            conn.pipe_message(|buff| {
                buff.write_msg(CopyData(chunk))?;
                Ok(PipeUntil::NumMessages { num_responses: 0 })
            })?;
        }

        Ok(self)
    }

    /// Copy data directly from `source` to the database without requiring an intermediate buffer.
    ///
    /// `source` will be read to the end.
    ///
    /// ### Note: Completion Step Required
    /// You must still call either [Self::finish] or [Self::abort] to complete the process.
    ///
    /// ### Note: Runtime Features
    /// This method uses the `AsyncRead` trait which is re-exported from either Tokio or `async-std`
    /// depending on which runtime feature is used.
    ///
    /// The runtime features _used_ to be mutually exclusive, but are no longer.
    /// If both `runtime-async-std` and `runtime-tokio` features are enabled, the Tokio version
    /// takes precedent.
    // TODO: Joey
    // pub async fn read_from(&mut self, mut source: impl AsyncRead + Unpin) -> Result<&mut Self> {
    //     let conn: &mut PgConnection = self.conn.as_deref_mut().expect("copy_from: conn taken");
    //     loop {
    //         let buf = conn.inner.stream.write_buffer_mut();

    //         // Write the CopyData format code and reserve space for the length.
    //         // This may end up sending an empty `CopyData` packet if, after this point,
    //         // we get canceled or read 0 bytes, but that should be fine.
    //         buf.put_slice(b"d\0\0\0\x04");

    //         let read = buf.read_from(&mut source).await?;

    //         if read == 0 {
    //             break;
    //         }

    //         // Write the length
    //         let read32 = i32::try_from(read)
    //             .map_err(|_| err_protocol!("number of bytes read exceeds 2^31 - 1: {}", read))?;

    //         (&mut buf.get_mut()[1..]).put_i32(read32 + 4);

    //         conn.inner.stream.flush().await?;
    //     }

    //     Ok(self)
    // }

    /// Signal that the `COPY` process should be aborted and any data received should be discarded.
    ///
    /// The given message can be used for indicating the reason for the abort in the database logs.
    ///
    /// The server is expected to respond with an error, so only _unexpected_ errors are returned.
    pub async fn abort(self, msg: impl Into<String>) -> Result<()> {
        let conn = self.conn.as_deref().expect("");
        let mut manager = conn.pipe_message(|buff| {
            buff.write_msg(CopyFail::new(msg))?;
            // TODO: Joey
            Ok(PipeUntil::ReadyForQuery)
        })?;

        match manager.recv().await {
            Ok(msg) => Err(err_protocol!(
                "fail_with: expected ErrorResponse, got: {:?}",
                msg.format
            )),
            Err(Error::Database(e)) => {
                match e.code() {
                    Some(Cow::Borrowed("57014")) => {
                        // postgres abort received error code
                        manager.recv_expect::<ReadyForQuery>().await?;
                        Ok(())
                    }
                    _ => Err(Error::Database(e)),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Signal that the `COPY` process is complete.
    ///
    /// The number of rows affected is returned.
    pub async fn finish(self) -> Result<u64> {
        let conn = self.conn.as_deref().expect("");
        let mut manager = conn.pipe_message(|buff| {
            buff.write_msg(CopyDone)?;
            Ok(PipeUntil::ReadyForQuery)
        })?;

        let cc: CommandComplete = match manager.recv_expect().await {
            Ok(cc) => cc,
            Err(e) => {
                // FIXME(JoeydeWaal): huh???
                manager.recv().await?;
                return Err(e);
            }
        };

        manager.recv_expect::<ReadyForQuery>().await?;

        Ok(cc.rows_affected())
    }
}

impl<C: DerefMut<Target = PgConnection>> Drop for PgCopyIn<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _ = conn
                .pipe_message(|buff| {
                    buff.write_msg(CopyFail::new(
                        "PgCopyIn dropped without calling finish() or fail()",
                    ))?;
                    Ok(PipeUntil::NumMessages { num_responses: 0 })
                })
                .expect("BUG: PgCopyIn abort message should not be too large");
        }
    }
}

async fn pg_begin_copy_out<'c, C: DerefMut<Target = PgConnection> + Send + 'c>(
    conn: C,
    statement: &str,
) -> Result<BoxStream<'c, Result<Bytes>>> {
    let mut manager = conn.pipe_message(|buff| {
        buff.write_msg(Query(statement))?;
        Ok(PipeUntil::ReadyForQuery)
    })?;

    let _: CopyOutResponse = manager.recv_expect().await?;

    let (_, rx) = manager.into_inner();

    let stream = PgCopyStream::new(conn, rx);

    Ok(stream.into_stream())
}

struct PgCopyStream<C> {
    #[allow(unused)]
    conn: C,
    manager: Option<UnboundedReceiver<ReceivedMessage>>,
}

impl<'c, C> PgCopyStream<C>
where
    C: DerefMut<Target = PgConnection> + Send + 'c,
{
    fn new(conn: C, manager: UnboundedReceiver<ReceivedMessage>) -> Self {
        Self {
            conn,
            manager: Some(manager),
        }
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        let Some(rx) = self.manager.take() else {
            return Ok(None);
        };

        let mut manager = ConnManager::new(rx, self.conn.deref());
        match manager.recv().await {
            Err(e) => {
                manager.recv_expect::<ReadyForQuery>().await?;
                return Err(e);
            }
            Ok(msg) => match msg.format {
                BackendMessageFormat::CopyData => {
                    let (_, rx) = manager.into_inner();
                    self.manager = Some(rx);
                    return Ok(Some(msg.decode::<CopyData<Bytes>>()?.0));
                }
                BackendMessageFormat::CopyDone => {
                    let _ = msg.decode::<CopyDone>()?;
                    manager.recv_expect::<CommandComplete>().await?;
                    manager.recv_expect::<ReadyForQuery>().await?;
                    return Ok(None);
                }
                _ => {
                    return Err(err_protocol!(
                        "unexpected message format during copy out: {:?}",
                        msg.format
                    ))
                }
            },
        }
    }

    fn into_stream(self) -> BoxStream<'c, Result<Bytes>> {
        Box::pin(futures_util::stream::try_unfold(
            self,
            |mut this| async move {
                let item = this.next().await?;
                if let Some(data) = item {
                    return Ok(Some((data, this)));
                } else {
                    return Ok(None);
                }
            },
        ))
    }
}
