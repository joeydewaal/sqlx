use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures_core::Stream;
use futures_util::FutureExt;
use sqlx_core::{logger::QueryLogger, Either};

use crate::{
    message::{BackendMessageFormat, CommandComplete, DataRow, ReadyForQuery},
    statement::PgStatementMetadata,
    PgQueryResult, PgRow, PgValueFormat,
};

use super::worker::Pipe;

pub struct PgRowStream<'c, 'q> {
    pipe: Pipe<'c>,
    logger: QueryLogger<'q>,
    metadata: Arc<PgStatementMetadata>,
    format: PgValueFormat,
}

impl<'c, 'q> PgRowStream<'c, 'q> {
    pub fn new(
        pipe: Pipe<'c>,
        logger: QueryLogger<'q>,
        metadata: Arc<PgStatementMetadata>,
        format: PgValueFormat,
    ) -> Self {
        PgRowStream {
            pipe,
            logger,
            metadata,
            format,
        }
    }
}

impl<'c, 'q> Stream for PgRowStream<'c, 'q> {
    type Item = sqlx_core::Result<Either<PgQueryResult, PgRow>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let message = ready!(self.pipe.poll_recv(cx))?;

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
                        self.logger.increase_rows_affected(rows_affected);
                        return Poll::Ready(Some(Ok(Either::Left(PgQueryResult {
                            rows_affected,
                        }))));
                    }

                    BackendMessageFormat::EmptyQueryResponse => {
                        // empty query string passed to an unprepared execute
                    }

                    // Message::ErrorResponse is handled in self.stream.recv()

                    // incomplete query execution has finished
                    BackendMessageFormat::PortalSuspended => {}

                    BackendMessageFormat::RowDescription => {
                        // indicates that a *new* set of rows are about to be returned
                        let (columns, column_names) = self.pipe.conn
                            .handle_row_description(Some(message.decode()?), false)
                            .now_or_never().unwrap()?;

                        self.metadata = Arc::new(PgStatementMetadata {
                            column_names: Arc::new(column_names),
                            columns,
                            parameters: Vec::default(),
                        });
                    }

                    BackendMessageFormat::DataRow => {
                        self.logger.increment_rows_returned();

                        // one of the set of rows returned by a SELECT, FETCH, etc query
                        let data: DataRow = message.decode()?;
                        let row = PgRow {
                            data,
                            format: self.format,
                            metadata: Arc::clone(&self.metadata),
                        };

                        return Poll::Ready(Some(Ok( Either::Right(row))));
                    }

                    BackendMessageFormat::ReadyForQuery => {
                        // processing of the query string is complete
                        let rfq: ReadyForQuery = message.decode()?;
                        self.pipe.conn.set_transaction_status(rfq.transaction_status);
                        return Poll::Ready(None);
                    }

                    _ => {
                        return Poll::Ready(Some(Err(err_protocol!(
                            "execute: unexpected message: {:?}",
                            message.format
                        ))));
                    }
                }
        }
    }
}
