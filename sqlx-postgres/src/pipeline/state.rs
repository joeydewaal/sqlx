use std::{collections::HashSet, sync::Arc};

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
    PgArguments, PgConnection, PgQueryResult, PgRow, PgTypeInfo, PgValueFormat,
};

pub struct PipelineState {
    preparing: HashSet<String>,
}

impl PipelineState {
    pub fn new() -> Self {
        Self {
            preparing: HashSet::new(),
        }
    }
}

pub struct QueryState {
    pub next_step: PipelineStep,
    pub sql: String,
    pub arguments: PgArguments,
    pub should_flush_before_next: bool,
    pub sender: flume::Sender<Result<Either<PgQueryResult, PgRow>, Error>>,
    pub is_done: bool,
}

pub enum PipelineStep {
    GetOrPrepare,
    ParseDescribe,
    ReceiveParseDescribe(StatementId),
    BindExecuteClose(StatementId, Arc<PgStatementMetadata>),
    ReceiveData(Arc<PgStatementMetadata>),
    FillCache(Box<PipelineStep>),
}

fn get_pg_type(oid: Oid, conn: &PgConnection) -> Option<PgTypeInfo> {
    if let Some(t) = PgTypeInfo::try_from_oid(oid) {
        return Some(t);
    } else {
        conn.inner.cache_type_info.get(&oid).cloned()
    }
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

impl QueryState {
    pub async fn next(
        &mut self,
        conn: &mut PgConnection,
        state: &mut PipelineState,
    ) -> sqlx_core::Result<()> {
        self.next_step = match &self.next_step {
            PipelineStep::GetOrPrepare => {
                // dbg!(&conn.inner.cache_statement);
                if let Some((statement_id, metadata)) =
                    conn.inner.cache_statement.get_mut(&self.sql)
                {
                    // println!("Got cached");
                    PipelineStep::BindExecuteClose(*statement_id, metadata.clone())
                } else {
                    if state.preparing.contains(&self.sql) {
                        // println!("Is already preqaring stmt");
                        PipelineStep::GetOrPrepare
                    } else {
                        // println!("Preparing");
                        state.preparing.insert(self.sql.clone());
                        PipelineStep::ParseDescribe
                    }
                }
            }
            PipelineStep::ParseDescribe => {
                let id = conn.inner.next_statement_id;
                conn.inner.next_statement_id = id.next();

                let mut oids = Vec::new();
                let mut argument_types = Vec::new();

                for ty in &self.arguments.types {
                    println!("Got: {ty:?}");
                    if let Some(oid) = get_oid(&ty.0, conn) {
                        if let Some(ty) = get_pg_type(oid, conn) {
                            println!("Wanted: {ty:?}");
                            argument_types.push(ty);
                        } else {
                            unimplemented!("Could not resolv OID {:?}", oid);
                        }

                        oids.push(oid);
                    } else {
                        unimplemented!("Could not resolve pg type: {ty:?}");
                    }
                }

                let r = conn.inner.cache_type_info.get(&oids[0]);
                // dbg!(&r);

                // dbg!(&oids);
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

                self.arguments.apply_patches(conn, &argument_types).await?;

                self.should_flush_before_next = true;
                PipelineStep::ReceiveParseDescribe(id)
            }
            PipelineStep::ReceiveParseDescribe(statement_id) => {
                conn.inner.stream.recv_expect::<ParseComplete>().await?;
                let parameters = recv_desc_params(conn).await?;

                let rows = recv_desc_rows(conn).await?;

                // each SYNC produces one READY FOR QUERY
                // conn.recv_ready_for_query().await?;

                let parameters = conn.handle_parameter_description(parameters).await?;

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

                if conn.inner.cache_statement.is_enabled() {
                    if let Some((_, _)) = conn
                        .inner
                        .cache_statement
                        .insert(&self.sql, (*statement_id, metadata.clone()))
                    {
                        // TODO
                    }
                }
                state.preparing.remove(&self.sql);

                PipelineStep::BindExecuteClose(*statement_id, metadata)
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
                loop {
                    let message = conn.inner.stream.recv().await?;

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
                        if self.sender.send(Ok(Either::Left(PgQueryResult {
                            rows_affected,
                        }))).is_err() {
                            return Ok(());
                        };
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
