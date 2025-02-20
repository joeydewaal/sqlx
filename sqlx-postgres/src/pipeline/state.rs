use std::sync::Arc;

use sqlx_core::{arguments::Arguments, Either, Error};

use crate::{
    io::{PortalId, StatementId},
    message::{
        self, BackendMessageFormat, Bind, Close, CommandComplete, DataRow, ParameterDescription,
        Parse, ParseComplete, RowDescription,
    },
    statement::PgStatementMetadata,
    type_info::PgType,
    types::Oid,
    PgArguments, PgConnection, PgQueryResult, PgRow, PgValueFormat,
};

pub struct PipelineState {
    pub next_step: PipelineStep,
    pub sql: String,
    pub arguments: PgArguments,
    pub should_flush_before_next: bool,
    pub sender: flume::Sender<Result<Either<PgQueryResult, PgRow>, Error>>,
    pub is_done: bool
}

pub enum PipelineStep {
    ParseDescribe,
    ReceiveParseDescribe(StatementId),
    ApplyPatch(StatementId, Arc<PgStatementMetadata>),
    BindExecuteClose(StatementId, Arc<PgStatementMetadata>),
    ReceiveData(Arc<PgStatementMetadata>),
    FillCache(Box<PipelineStep>),
}

fn get_oid(ty: &PgType, conn: &PgConnection) -> Option<Oid> {
    if let Some(oid) = ty.try_oid() {
        return Some(oid);
    } else {
        match ty {
            PgType::DeclareWithName(name) => conn.inner.cache_type_oid.get(name).copied(),
            PgType::DeclareArrayOf(array) => conn
                .inner
                .cache_type_oid
                .get(&array.elem_name)
                .and_then(|elem_oid| conn.inner.cache_elem_type_to_array.get(elem_oid))
                .copied(),
            _ => unreachable!(),
        }
    }
}

impl PipelineState {
    pub async fn next(&mut self, conn: &mut PgConnection) -> sqlx_core::Result<()> {
        self.next_step = match &self.next_step {
            PipelineStep::ParseDescribe => {
                let id = conn.inner.next_statement_id;
                conn.inner.next_statement_id = id.next();

                let mut oids = Vec::new();

                for ty in &self.arguments.types {
                    if let Some(oid) = get_oid(&ty.0, conn) {
                        oids.push(oid);
                    } else {
                        // Eagerly fill cache
                        // Make sure we get back when the cache is full
                        self.next_step =
                            PipelineStep::FillCache(Box::new(PipelineStep::ParseDescribe));
                    }
                }

                // println!("Pushing Parse");
                conn.inner.stream.write_msg(Parse {
                    param_types: &oids,
                    query: &self.sql,
                    statement: id,
                })?;

                // println!("Pushing Describe");
                conn.inner
                    .stream
                    .write_msg(message::Describe::Statement(id))?;

                conn.write_sync();

                self.should_flush_before_next = true;
                PipelineStep::ReceiveParseDescribe(id)
            }
            PipelineStep::ReceiveParseDescribe(statement_id) => {
                // println!("Expect `ParseComplete`");
                conn.inner.stream.recv_expect::<ParseComplete>().await?;
                // println!("Received ParseComplete");
                let parameters = recv_desc_params(conn).await?;
                // println!("Received recv_desc_params");

                let rows = recv_desc_rows(conn).await?;
                // println!("got rows");

                // each SYNC produces one READY FOR QUERY
                // conn.recv_ready_for_query().await?;

                let parameters = conn.handle_parameter_description(parameters).await?;
                // println!("Handled parameter_description");

                let (columns, column_names) = conn.handle_row_description(rows, true).await?;

                // ensure that if we did fetch custom data, we wait until we are fully ready before
                // continuing
                // conn.wait_until_ready().await?;
                conn.recv_ready_for_query().await?;

                let metadata = Arc::new(PgStatementMetadata {
                    parameters,
                    columns,
                    column_names: Arc::new(column_names),
                });
                PipelineStep::ApplyPatch(*statement_id, metadata)
            }
            PipelineStep::ApplyPatch(statment_id, param) => {
                // println!("ApplyPatch");
                self.arguments
                    .apply_patches(conn, &param.parameters)
                    .await?;
                PipelineStep::BindExecuteClose(*statment_id, param.clone())
            }
            PipelineStep::BindExecuteClose(statement_id, param) => {
                // dbg!(&statement_id, param);
                // println!("Writing BIND");
                conn.inner.stream.write_msg(Bind {
                    portal: PortalId::UNNAMED,
                    statement: *statement_id,
                    formats: &[PgValueFormat::Binary],
                    num_params: self.arguments.len() as u16, // FIXME
                    params: &self.arguments.buffer,
                    result_formats: &[PgValueFormat::Binary],
                })?;
                // println!("Writing EXECUTE");
                conn.inner.stream.write_msg(message::Execute {
                    portal: PortalId::UNNAMED,
                    limit: 0, //FIXME
                })?;
                // println!("Writing CLOSE");
                conn.inner
                    .stream
                    .write_msg(Close::Portal(PortalId::UNNAMED))?;

                self.should_flush_before_next = true;
                conn.write_sync();
                PipelineStep::ReceiveData(param.clone())
            }
            PipelineStep::ReceiveData(metadata) => {
                // println!("ReceiveData");
                loop {
                    let message = conn.inner.stream.recv().await?;
                    // println!("Got");

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
                        // logger.increase_rows_affected(rows_affected);
                        self.sender.send(Ok(Either::Left(PgQueryResult {
                            rows_affected,
                        })));
                    }

                    BackendMessageFormat::EmptyQueryResponse => {
                        // empty query string passed to an unprepared execute
                    }

                    // Message::ErrorResponse is handled in self.stream.recv()

                    // incomplete query execution has finished
                    BackendMessageFormat::PortalSuspended => {}

                    BackendMessageFormat::RowDescription => {
                        // indicates that a *new* set of rows are about to be returned
                        // let (columns, column_names) = self
                        //     .handle_row_description(Some(message.decode()?), false)
                        //     .await?;

                        // metadata = Arc::new(PgStatementMetadata {
                        //     column_names: Arc::new(column_names),
                        //     columns,
                        //     parameters: Vec::default(),
                        // });
                    }

                    BackendMessageFormat::DataRow => {
                        // println!("Got data");
                        // logger.increment_rows_returned();

                        // one of the set of rows returned by a SELECT, FETCH, etc query
                        let data: DataRow = message.decode()?;
                        let row = PgRow {
                            data,
                            format: PgValueFormat::Binary,
                            metadata: Arc::clone(&metadata),
                        };

                        let _ = self.sender.send(Ok(Either::Right(row)));
                    }

                    BackendMessageFormat::ReadyForQuery => {
                        // processing of the query string is complete
                        conn.handle_ready_for_query(message)?;
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
                // println!("is done");
                self.is_done = true;
                return Ok(());
            }
            _ => todo!(),
        };

        Ok(())
    }
}

async fn recv_desc_params(conn: &mut PgConnection) -> Result<ParameterDescription, Error> {
    conn.inner.stream.recv_expect().await
}

async fn recv_desc_rows(conn: &mut PgConnection) -> Result<Option<RowDescription>, Error> {
    let rows: Option<RowDescription> = match conn.inner.stream.recv().await? {
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
