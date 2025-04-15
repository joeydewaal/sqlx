use crate::connection::worker::PipeUntil;
use crate::describe::Describe;
use crate::error::Error;
use crate::executor::{Execute, Executor};
use crate::io::{PortalId, StatementId};
use crate::logger::QueryLogger;
use crate::message::{
    self, BackendMessageFormat, Bind, Close, CommandComplete, DataRow, ParameterDescription, Parse,
    ParseComplete, Query, ReadyForQuery, RowDescription,
};
use crate::statement::PgStatementMetadata;
use crate::{
    statement::PgStatement, PgArguments, PgConnection, PgQueryResult, PgRow, PgTypeInfo,
    PgValueFormat, Postgres,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_core::Stream;
use futures_util::TryStreamExt;
use sqlx_core::arguments::Arguments;
use sqlx_core::Either;
use std::{borrow::Cow, pin::pin, sync::Arc};

use super::worker::Pipe;

async fn prepare(
    conn: &PgConnection,
    sql: &str,
    parameters: &[PgTypeInfo],
    metadata: Option<Arc<PgStatementMetadata>>,
) -> Result<(StatementId, Arc<PgStatementMetadata>), Error> {
    let id = conn.with_lock(|inner| {
        let id = inner.next_statement_id;
        inner.next_statement_id = id.next();
        id
    });

    // build a list of type OIDs to send to the database in the PARSE command
    // we have not yet started the query sequence, so we are *safe* to cleanly make
    // additional queries here to get any missing OIDs

    let mut param_types = Vec::with_capacity(parameters.len());

    for ty in parameters {
        param_types.push(conn.resolve_type_id(&ty.0).await?);
    }

    let mut pipe = conn.start_pipe(|message| {
        message.write_msg(Parse {
            param_types: &param_types,
            query: sql,
            statement: id,
        })?;
        if metadata.is_none() {
            // get the statement columns and parameters
            message.write_msg(message::Describe::Statement(id))?;
        }

        // we ask for the server to immediately send us the result of the PARSE command
        message.write_sync()
    })?;

    // indicates that the SQL query string is now successfully parsed and has semantic validity
    pipe.recv_expect::<ParseComplete>().await?;

    let metadata = if let Some(metadata) = metadata {
        // each SYNC produces one READY FOR QUERY
        pipe.recv_ready_for_query().await?;

        // we already have metadata
        metadata
    } else {
        let parameters = recv_desc_params(&mut pipe).await?;

        let rows = recv_desc_rows(&mut pipe).await?;

        // each SYNC produces one READY FOR QUERY
        pipe.recv_ready_for_query().await?;

        let parameters = conn.handle_parameter_description(parameters).await?;

        let (columns, column_names) = conn.handle_row_description(rows, true).await?;

        Arc::new(PgStatementMetadata {
            parameters,
            columns,
            column_names: Arc::new(column_names),
        })
    };

    Ok((id, metadata))
}

async fn recv_desc_params(conn: &mut Pipe<'_>) -> Result<ParameterDescription, Error> {
    conn.recv_expect().await
}

async fn recv_desc_rows(conn: &mut Pipe<'_>) -> Result<Option<RowDescription>, Error> {
    let rows: Option<RowDescription> = match conn.recv().await? {
        // describes the rows that will be returned when the statement is eventually executed
        message if message.format == BackendMessageFormat::RowDescription => {
            Some(message.decode()?)
        }

        // no data would be returned if this statement was executed
        message if message.format == BackendMessageFormat::NoData => None,

        message => {
            return Err(err_protocol!(
                "expecting RowDescription or NoData but received {:?}",
                message.format
            ));
        }
    };

    Ok(rows)
}

impl PgConnection {
    async fn get_or_prepare<'a>(
        &self,
        sql: &str,
        parameters: &[PgTypeInfo],
        // should we store the result of this prepare to the cache
        store_to_cache: bool,
        // optional metadata that was provided by the user, this means they are reusing
        // a statement object
        metadata: Option<Arc<PgStatementMetadata>>,
    ) -> Result<(StatementId, Arc<PgStatementMetadata>), Error> {
        if let Some(stmt) = self.inner.cache_statement.get(sql, store_to_cache).await {
            return Ok(stmt);
        }

        let statement = prepare(self, sql, parameters, metadata)
            .await
            // If preparing the statement failed, we should update the status so it's not in flight
            // anymore and also notify the waiting tasks.
            .inspect_err(|_| self.inner.cache_statement.remove_and_notify(sql))?;

        if store_to_cache {
            if let Some((id, _)) = self
                .inner
                .cache_statement
                .checked_insert(sql, statement.clone())
            {
                let mut pipe = self.start_pipe(|message| {
                    message.write_msg(Close::Statement(id))?;
                    message.write_sync()
                })?;
                pipe.wait_for_close_complete(1).await?;
                pipe.recv_ready_for_query().await?;
            }
        }

        Ok(statement)
    }

    pub(crate) async fn run<'e, 'c: 'e, 'q: 'e>(
        &'c self,
        query: &'q str,
        arguments: Option<PgArguments>,
        persistent: bool,
        metadata_opt: Option<Arc<PgStatementMetadata>>,
    ) -> Result<impl Stream<Item = Result<Either<PgQueryResult, PgRow>, Error>> + 'e, Error> {
        let mut logger = QueryLogger::new(query, self.inner.log_settings.clone());

        let mut metadata: Arc<PgStatementMetadata>;

        let mut pipe;

        let format = if let Some(mut arguments) = arguments {
            // Check this before we write anything to the stream.
            //
            // Note: Postgres actually interprets this value as unsigned,
            // making the max number of parameters 65535, not 32767
            // https://github.com/launchbadge/sqlx/issues/3464
            // https://www.postgresql.org/docs/current/limits.html
            let num_params = u16::try_from(arguments.len()).map_err(|_| {
                err_protocol!(
                    "PgConnection::run(): too many arguments for query: {}",
                    arguments.len()
                )
            })?;

            // prepare the statement if this our first time executing it
            // always return the statement ID here
            let (statement, metadata_) = self
                .get_or_prepare(query, &arguments.types, persistent, metadata_opt)
                .await?;

            metadata = metadata_;

            // patch holes created during encoding
            arguments.apply_patches(self, &metadata.parameters).await?;

            pipe = self.start_pipe(|messages| {
                // bind to attach the arguments to the statement and create a portal
                messages.write_msg(Bind {
                    portal: PortalId::UNNAMED,
                    statement,
                    formats: &[PgValueFormat::Binary],
                    num_params,
                    params: &arguments.buffer,
                    result_formats: &[PgValueFormat::Binary],
                })?;

                // executes the portal up to the passed limit
                // the protocol-level limit acts nearly identically to the `LIMIT` in SQL
                //
                // From https://www.postgresql.org/docs/current/protocol-flow.html:
                //
                // "An unnamed portal is destroyed at the end of the transaction, or as
                // soon as the next Bind statement specifying the unnamed portal as
                // destination is issued. (Note that a simple Query message also
                // destroys the unnamed portal."
                messages.write_msg(message::Execute {
                    portal: PortalId::UNNAMED,
                    limit: 0,
                })?;

                // we ask the database server to close the unnamed portal and free the associated resources
                // earlier - after the execution of the current query.
                messages.write_msg(Close::Portal(PortalId::UNNAMED))?;

                // finally, [Sync] asks postgres to process the messages that we sent and respond with
                // a [ReadyForQuery] message when it's completely done. Theoretically, we could send
                // dozens of queries before a [Sync] and postgres can handle that. Execution on the server
                // is still serial but it would reduce round-trips. Some kind of builder pattern that is
                // termed batching might suit this.
                messages.write_sync()
            })?;

            // prepared statements are binary
            PgValueFormat::Binary
        } else {
            pipe = self.start_pipe(|messages| {
                messages.write_msg(Query(query))?;
                // Query will trigger a ReadyForQuery
                Ok(PipeUntil::ReadyForQuery)
            })?;

            // metadata starts out as "nothing"
            metadata = Arc::new(PgStatementMetadata::default());

            // and unprepared statements are text
            PgValueFormat::Text
        };

        Ok(try_stream! {
            loop {
                let message = pipe.recv().await?;

                match message.format {
                    BackendMessageFormat::BindComplete
                    | BackendMessageFormat::ParseComplete
                    | BackendMessageFormat::ParameterDescription
                    | BackendMessageFormat::NoData
                    // unnamed portal has been closed
                    | BackendMessageFormat::CloseComplete
                    => {
                        // harmless messages to ignore
                    }

                    // "Execute phase is always terminated by the appearance of
                    // exactly one of these messages: CommandComplete,
                    // EmptyQueryResponse (if the portal was created from an
                    // empty query string), ErrorResponse, or PortalSuspended"
                    BackendMessageFormat::CommandComplete => {
                        // a SQL command completed normally
                        let cc: CommandComplete = message.decode()?;

                        let rows_affected = cc.rows_affected();
                        logger.increase_rows_affected(rows_affected);
                        r#yield!(Either::Left(PgQueryResult {
                            rows_affected,
                        }));
                    }

                    BackendMessageFormat::EmptyQueryResponse => {
                        // empty query string passed to an unprepared execute
                    }

                    // Message::ErrorResponse is handled in self.stream.recv()

                    // incomplete query execution has finished
                    BackendMessageFormat::PortalSuspended => {}

                    BackendMessageFormat::RowDescription => {
                        // indicates that a *new* set of rows are about to be returned
                        let (columns, column_names) = self
                            .handle_row_description(Some(message.decode()?), false)
                            .await?;

                        metadata = Arc::new(PgStatementMetadata {
                            column_names: Arc::new(column_names),
                            columns,
                            parameters: Vec::default(),
                        });
                    }

                    BackendMessageFormat::DataRow => {
                        logger.increment_rows_returned();

                        // one of the set of rows returned by a SELECT, FETCH, etc query
                        let data: DataRow = message.decode()?;
                        let row = PgRow {
                            data,
                            format,
                            metadata: Arc::clone(&metadata),
                        };

                        r#yield!(Either::Right(row));
                    }

                    BackendMessageFormat::ReadyForQuery => {
                        // processing of the query string is complete
                        let rfq: ReadyForQuery = message.decode()?;
                        self.set_transaction_status(rfq.transaction_status);
                        break;
                    }

                    _ => {
                        return Err(err_protocol!(
                            "execute: unexpected message: {:?}",
                            message.format
                        ));
                    }
                }
            }

            Ok(())
        })
    }
}

impl<'c> Executor<'c> for &'c PgConnection {
    type Database = Postgres;

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let sql = query.sql();
        // False positive: https://github.com/rust-lang/rust-clippy/issues/12560
        #[allow(clippy::map_clone)]
        let metadata = query.statement().map(|s| Arc::clone(&s.metadata));
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();

        Box::pin(try_stream! {
            let arguments = arguments?;
            let mut s = pin!(self.run(sql, arguments, persistent, metadata).await?);

            while let Some(v) = s.try_next().await? {
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q, E>(self, mut query: E) -> BoxFuture<'e, Result<Option<PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let sql = query.sql();
        // False positive: https://github.com/rust-lang/rust-clippy/issues/12560
        #[allow(clippy::map_clone)]
        let metadata = query.statement().map(|s| Arc::clone(&s.metadata));
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();

        Box::pin(async move {
            let arguments = arguments?;
            let mut s = pin!(self.run(sql, arguments, persistent, metadata).await?);

            // With deferred constraints we need to check all responses as we
            // could get a OK response (with uncommitted data), only to get an
            // error response after (when the deferred constraint is actually
            // checked).
            let mut ret = None;
            while let Some(result) = s.try_next().await? {
                match result {
                    Either::Right(r) if ret.is_none() => ret = Some(r),
                    _ => {}
                }
            }
            Ok(ret)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement<'q>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let (_, metadata) = self.get_or_prepare(sql, parameters, true, None).await?;

            Ok(PgStatement {
                sql: Cow::Borrowed(sql),
                metadata,
            })
        })
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let (stmt_id, metadata) = self.get_or_prepare(sql, &[], true, None).await?;

            let nullable = self.get_nullable_for_columns(stmt_id, &metadata).await?;

            Ok(Describe {
                columns: metadata.columns.clone(),
                nullable,
                parameters: Some(Either::Left(metadata.parameters.clone())),
            })
        })
    }
}

// We keep the Executor impl for &mut PgConnection here for backwards compatibility.
impl<'c> Executor<'c> for &'c mut PgConnection {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<
                <Self::Database as sqlx_core::database::Database>::QueryResult,
                <Self::Database as sqlx_core::database::Database>::Row,
            >,
            Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        <&PgConnection as Executor>::fetch_many(self, query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as sqlx_core::database::Database>::Row>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        <&PgConnection as Executor>::fetch_optional(self, query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as sqlx_core::database::Database>::TypeInfo],
    ) -> BoxFuture<
        'e,
        Result<<Self::Database as sqlx_core::database::Database>::Statement<'q>, Error>,
    >
    where
        'c: 'e,
    {
        <&PgConnection as Executor>::prepare_with(self, sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        <&PgConnection as Executor>::describe(self, sql)
    }
}
