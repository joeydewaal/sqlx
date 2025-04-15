use std::fmt::{self, Debug};
use std::str::from_utf8;

use futures_channel::mpsc::UnboundedReceiver;
use futures_core::future::BoxFuture;
use futures_core::stream::{BoxStream, Stream};
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use sqlx_core::acquire::Acquire;
use sqlx_core::transaction::Transaction;
use sqlx_core::Either;

use crate::describe::Describe;
use crate::error::Error;
use crate::executor::{Execute, Executor};
use crate::message::{Notification, ReceivedMessage};
use crate::pool::PoolOptions;
use crate::pool::{Pool, PoolConnection};
use crate::{PgConnection, PgQueryResult, PgRow, PgStatement, PgTypeInfo, Postgres};

/// A stream of asynchronous notifications from Postgres.
///
/// This listener will auto-reconnect. If the active
/// connection being used ever dies, this listener will detect that event, create a
/// new connection, will re-subscribe to all of the originally specified channels, and will resume
/// operations as normal.
pub struct PgListener {
    pool: Pool<Postgres>,
    connection: PoolConnection<Postgres>,
    channels: Vec<String>,
    ignore_close_event: bool,
    eager_reconnect: bool,
}

/// An asynchronous notification from Postgres.
pub struct PgNotification(Notification);

impl PgListener {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        // Create a pool of 1 without timeouts (as they don't apply here)
        // We only use the pool to handle re-connections
        let pool = PoolOptions::<Postgres>::new()
            .max_connections(1)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect(url)
            .await?;

        let mut this = Self::connect_with(&pool).await?;
        // We don't need to handle close events
        this.ignore_close_event = true;

        Ok(this)
    }

    pub async fn connect_with(pool: &Pool<Postgres>) -> Result<Self, Error> {
        // Pull out an initial connection
        let connection = pool.acquire().await?;

        Ok(Self {
            pool: pool.clone(),
            connection,
            channels: Vec::new(),
            ignore_close_event: false,
            eager_reconnect: true,
        })
    }

    /// Set whether or not to ignore [`Pool::close_event()`]. Defaults to `false`.
    ///
    /// By default, when [`Pool::close()`] is called on the pool this listener is using
    /// while [`Self::recv()`] or [`Self::try_recv()`] are waiting for a message, the wait is
    /// cancelled and `Err(PoolClosed)` is returned.
    ///
    /// This is because `Pool::close()` will wait until _all_ connections are returned and closed,
    /// including the one being used by this listener.
    ///
    /// Otherwise, `pool.close().await` would have to wait until `PgListener` encountered a
    /// need to acquire a new connection (timeout, error, etc.) and dropped the one it was
    /// currently holding, at which point `.recv()` or `.try_recv()` would return `Err(PoolClosed)`
    /// on the attempt to acquire a new connection anyway.
    ///
    /// However, if you want `PgListener` to ignore the close event and continue waiting for a
    /// message as long as it can, set this to `true`.
    ///
    /// Does nothing if this was constructed with [`PgListener::connect()`], as that creates an
    /// internal pool just for the new instance of `PgListener` which cannot be closed manually.
    pub fn ignore_pool_close_event(&mut self, val: bool) {
        self.ignore_close_event = val;
    }

    /// Set whether a lost connection in `try_recv()` should be re-established before it returns
    /// `Ok(None)`, or on the next call to `try_recv()`.
    ///
    /// By default, this is `true` and the connection is re-established before returning `Ok(None)`.
    ///
    /// If this is set to `false` then notifications will continue to be lost until the next call
    /// to `try_recv()`. If your recovery logic uses a different database connection then
    /// notifications that occur after it completes may be lost without any way to tell that they
    /// have been.
    pub fn eager_reconnect(&mut self, val: bool) {
        self.eager_reconnect = val;
    }

    /// Starts listening for notifications on a channel.
    /// The channel name is quoted here to ensure case sensitivity.
    pub async fn listen(&mut self, channel: &str) -> Result<(), Error> {
        self.connection
            .execute(&*format!(r#"LISTEN "{}""#, ident(channel)))
            .await?;

        self.channels.push(channel.to_owned());

        Ok(())
    }

    /// Starts listening for notifications on all channels.
    pub async fn listen_all(
        &mut self,
        channels: impl IntoIterator<Item = &str>,
    ) -> Result<(), Error> {
        let beg = self.channels.len();
        self.channels.extend(channels.into_iter().map(|s| s.into()));

        let query = build_listen_all_query(&self.channels[beg..]);
        self.connection.execute(&*query).await?;

        Ok(())
    }

    /// Stops listening for notifications on a channel.
    /// The channel name is quoted here to ensure case sensitivity.
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), Error> {
        // use RAW connection and do NOT re-connect automatically, since this is not required for
        // UNLISTEN (we've disconnected anyways)
        self.connection
            .execute(&*format!(r#"UNLISTEN "{}""#, ident(channel)))
            .await?;

        if let Some(pos) = self.channels.iter().position(|s| s == channel) {
            self.channels.remove(pos);
        }

        Ok(())
    }

    /// Stops listening for notifications on all channels.
    pub async fn unlisten_all(&mut self) -> Result<(), Error> {
        // use RAW connection and do NOT re-connect automatically, since this is not required for
        // UNLISTEN (we've disconnected anyways)
        self.connection.execute("UNLISTEN *").await?;

        self.channels.clear();

        Ok(())
    }

    /// Receives the next notification available from any of the subscribed channels.
    ///
    /// If the connection to PostgreSQL is lost, it is automatically reconnected on the next
    /// call to `recv()`, and should be entirely transparent (as long as it was just an
    /// intermittent network failure or long-lived connection reaper).
    ///
    /// As notifications are transient, any received while the connection was lost, will not
    /// be returned. If you'd prefer the reconnection to be explicit and have a chance to
    /// do something before, please see [`try_recv`](Self::try_recv).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::postgres::PgListener;
    /// #
    /// # sqlx::__rt::test_block_on(async move {
    /// let mut listener = PgListener::connect("postgres:// ...").await?;
    /// loop {
    ///     // ask for next notification, re-connecting (transparently) if needed
    ///     let notification = listener.recv().await?;
    ///
    ///     // handle notification, do something interesting
    /// }
    /// # Result::<(), sqlx::Error>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn recv(&mut self) -> Result<PgNotification, Error> {
        loop {
            if let Some(notification) = self.try_recv().await? {
                return Ok(notification);
            }
        }
    }

    /// Receives the next notification available from any of the subscribed channels.
    ///
    /// If the connection to PostgreSQL is lost, `None` is returned, and the connection is
    /// reconnected either immediately, or on the next call to `try_recv()`, depending on
    /// the value of [`eager_reconnect`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::postgres::PgListener;
    /// #
    /// # sqlx::__rt::test_block_on(async move {
    /// # let mut listener = PgListener::connect("postgres:// ...").await?;
    /// loop {
    ///     // start handling notifications, connecting if needed
    ///     while let Some(notification) = listener.try_recv().await? {
    ///         // handle notification
    ///     }
    ///
    ///     // connection lost, do something interesting
    /// }
    /// # Result::<(), sqlx::Error>::Ok(())
    /// # }).unwrap();
    /// ```
    ///
    /// [`eager_reconnect`]: PgListener::eager_reconnect
    pub async fn try_recv(&mut self) -> Result<Option<PgNotification>, Error> {
        // Fetch our `CloseEvent` listener, if applicable.
        let mut close_event = (!self.ignore_close_event).then(|| self.pool.close_event());

        loop {
            // Let the background worker know that we would like a notification. This does not send a
            // notification back through the channel that was given. Instead it sends it back though
            // the notification channel in the connection.
            //
            // The receiver that is returned acts like a guard, when it is dropped the background
            // worker stops trying to get a notification. This makes this function cancellation
            // safe.
            let _guard = self.schedule_notif().await?;
            let next_message = self.connection.inner.notifications.next();

            let res = if let Some(ref mut close_event) = close_event {
                // cancels the wait and returns `Err(PoolClosed)` if the pool is closed
                // before `next_message` returns, or if the pool was already closed
                close_event.do_until(next_message).await?
            } else {
                next_message.await
            };

            match res {
                None => {
                    // Reconnect and try again.
                    self.reconnect().await?
                }
                Some(message) => {
                    return Ok(Some(PgNotification(message.decode()?)));
                }
            }
        }
    }

    pub async fn reconnect(&mut self) -> sqlx_core::Result<()> {
        // If the worker crashed make sure get a new connection.
        let connection = self.pool.acquire().await?;

        connection
            .execute(&*build_listen_all_query(&self.channels))
            .await?;

        Ok(())
    }

    pub async fn schedule_notif(
        &mut self,
    ) -> sqlx_core::Result<UnboundedReceiver<ReceivedMessage>> {
        let result = self.connection.schedule_notification();

        if result.is_err() {
            // If the worker crashed make sure get a new connection.
            self.reconnect().await?;
            return self.connection.schedule_notification();
        } else {
            return result;
        }
    }

    /// Receives the next notification that already exists in the connection buffer, if any.
    ///
    /// This is similar to `try_recv`, except it will not wait if the connection has not yet received a notification.
    ///
    /// This is helpful if you want to retrieve all buffered notifications and process them in batches.
    pub fn next_buffered(&mut self) -> Option<PgNotification> {
        if let Ok(Some(notification)) = self.connection.inner.notifications.try_next() {
            Some(PgNotification(notification.decode().ok()?))
        } else {
            None
        }
    }

    /// Consume this listener, returning a `Stream` of notifications.
    ///
    /// The backing connection will be automatically reconnected should it be lost.
    ///
    /// This has the same potential drawbacks as [`recv`](PgListener::recv).
    ///
    pub fn into_stream(mut self) -> impl Stream<Item = Result<PgNotification, Error>> + Unpin {
        Box::pin(try_stream! {
            loop {
                r#yield!(self.recv().await?);
            }
        })
    }
}

impl Drop for PgListener {
    fn drop(&mut self) {
        let _ = self.connection.queue_simple_query("UNLISTEN *");
    }
}

impl<'c> Acquire<'c> for &'c mut PgListener {
    type Database = Postgres;
    type Connection = &'c mut PgConnection;

    fn acquire(self) -> BoxFuture<'c, Result<Self::Connection, Error>> {
        Box::pin(async { Ok(self.connection.as_mut()) })
    }

    fn begin(self) -> BoxFuture<'c, Result<Transaction<'c, Self::Database>, Error>> {
        self.connection.begin()
    }
}

impl<'c> Executor<'c> for &'c mut PgListener {
    type Database = Postgres;

    fn fetch_many<'e, 'q, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        futures_util::stream::once(async move {
            // need some basic type annotation to help the compiler a bit
            let res: Result<_, Error> = Ok(self.connection.fetch_many(query));
            res
        })
        .try_flatten()
        .boxed()
    }

    fn fetch_optional<'e, 'q, E>(self, query: E) -> BoxFuture<'e, Result<Option<PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        async move { self.connection.fetch_optional(query).await }.boxed()
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        query: &'q str,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement<'q>, Error>>
    where
        'c: 'e,
    {
        async move { self.connection.prepare_with(query, parameters).await }.boxed()
    }

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        query: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        async move { self.connection.describe(query).await }.boxed()
    }
}

impl PgNotification {
    /// The process ID of the notifying backend process.
    #[inline]
    pub fn process_id(&self) -> u32 {
        self.0.process_id
    }

    /// The channel that the notify has been raised on. This can be thought
    /// of as the message topic.
    #[inline]
    pub fn channel(&self) -> &str {
        from_utf8(&self.0.channel).unwrap()
    }

    /// The payload of the notification. An empty payload is received as an
    /// empty string.
    #[inline]
    pub fn payload(&self) -> &str {
        from_utf8(&self.0.payload).unwrap()
    }
}

impl Debug for PgListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgListener").finish()
    }
}

impl Debug for PgNotification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgNotification")
            .field("process_id", &self.process_id())
            .field("channel", &self.channel())
            .field("payload", &self.payload())
            .finish()
    }
}

fn ident(mut name: &str) -> String {
    // If the input string contains a NUL byte, we should truncate the
    // identifier.
    if let Some(index) = name.find('\0') {
        name = &name[..index];
    }

    // Any double quotes must be escaped
    name.replace('"', "\"\"")
}

fn build_listen_all_query(channels: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    channels.into_iter().fold(String::new(), |mut acc, chan| {
        acc.push_str(r#"LISTEN ""#);
        acc.push_str(&ident(chan.as_ref()));
        acc.push_str(r#"";"#);
        acc
    })
}

#[test]
fn test_build_listen_all_query_with_single_channel() {
    let output = build_listen_all_query(&["test"]);
    assert_eq!(output.as_str(), r#"LISTEN "test";"#);
}

#[test]
fn test_build_listen_all_query_with_multiple_channels() {
    let output = build_listen_all_query(&["channel.0", "channel.1"]);
    assert_eq!(output.as_str(), r#"LISTEN "channel.0";LISTEN "channel.1";"#);
}
