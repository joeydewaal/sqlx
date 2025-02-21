use std::{collections::HashSet, sync::Arc};

use flume::Receiver;
use sqlx_core::{arguments::Arguments, ext::ustr::UStr, Either, Error};

use crate::{
    io::{PortalId, StatementId},
    message::{
        self, BackendMessageFormat, Bind, Close, CommandComplete, DataRow, ParameterDescription,
        Parse, ParseComplete, RowDescription,
    },
    pipeline::cache::{recursive_find, FetchByOid},
    statement::PgStatementMetadata,
    type_info::PgType,
    types::Oid,
    PgArguments, PgConnection, PgQueryResult, PgRow, PgTypeInfo, PgValueFormat,
};

pub struct PipelineState {
    depth: usize,
    preparing: HashSet<String>,
}

impl PipelineState {
    pub fn new() -> Self {
        Self {
            preparing: HashSet::new(),
            depth: 0,
        }
    }
}

#[derive(Clone)]
pub struct QueryState {
    pub next_step: PipelineStep,
    pub sql: String,
    pub arguments: PgArguments,
    pub should_flush_before_next: bool,
    pub sender: flume::Sender<Option<Result<Either<PgQueryResult, PgRow>, Error>>>,
    pub is_done: bool,
}

impl QueryState {
    pub fn new(
        sql: String,
        arguments: PgArguments,
    ) -> (
        QueryState,
        flume::Receiver<Option<Result<Either<PgQueryResult, PgRow>, Error>>>,
    ) {
        let (tx, rx) = flume::unbounded();
        (
            QueryState {
                next_step: PipelineStep::GetOrPrepare,
                sql,
                arguments,
                should_flush_before_next: false,
                sender: tx,
                is_done: false,
            },
            rx,
        )
    }
}

#[derive(Clone)]
pub enum PipelineStep {
    GetOrPrepare,
    ParseDescribe,
    ReceiveParseDescribe(StatementId),
    BindExecuteClose(StatementId, Arc<PgStatementMetadata>),
    ReceiveData(Arc<PgStatementMetadata>),
    FillCache(
        Box<QueryState>,
        Receiver<Option<Result<Either<PgQueryResult, PgRow>, Error>>>,
        Vec<Oid>,
        Vec<String>,
    ),
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
    pub async fn handle_next(
        &mut self,
        conn: &mut PgConnection,
        state: &mut PipelineState,
    ) -> sqlx_core::Result<()> {
        let result = self.next(conn, state).await;
        if let Err(result) = result {
            self.handle_error(state, conn).await?;
            self.is_done = true;
            dbg!(&result);
            let _ = self.sender.send(Some(Err(result)));
        }
        Ok(())
    }

    pub async fn next(
        &mut self,
        conn: &mut PgConnection,
        state: &mut PipelineState,
    ) -> sqlx_core::Result<()> {
        print!("{} - ", state.depth);
        self.next_step = match &mut self.next_step {
            PipelineStep::GetOrPrepare => {
                println!("GetOrPrepare");
                // dbg!(&conn.inner.cache_statement);
                if let Some((statement_id, metadata)) =
                    conn.inner.cache_statement.get_mut(&self.sql)
                {
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
                println!("ParseDescribe");
                let id = conn.inner.next_statement_id;
                conn.inner.next_statement_id = id.next();

                let mut unresolved_oids = Vec::new();
                let mut unresolved_typ = Vec::new();

                let mut oids = Vec::new();
                let mut argument_types = Vec::new();

                for ty in &self.arguments.types {
                    if let Some(oid) = get_oid(&ty.0, conn) {
                        if let Some(ty) = get_pg_type(oid, conn) {
                            argument_types.push(ty);
                        } else {
                            unresolved_oids.push(oid);
                        }

                        oids.push(oid);
                    } else {
                        unresolved_typ.push(ty.name().to_string());
                    }
                }

                if !unresolved_oids.is_empty() || !unresolved_typ.is_empty() {
                    // println!("Fetching types: {unresolved_oids:?} {unresolved_typ:?}");

                    let mut arguments = PgArguments::default();

                    arguments.add(&unresolved_oids).map_err(Error::Encode)?;
                    arguments.add(&unresolved_typ).map_err(Error::Encode)?;

                    state.depth += 1;
                    let (q_state, rx) = QueryState::new(SQL.to_string(), arguments);
                    PipelineStep::FillCache(Box::new(q_state), rx, unresolved_oids, unresolved_typ)
                } else {
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
            }
            PipelineStep::ReceiveParseDescribe(statement_id) => {
                println!("ReceiveParseDescribe");
                conn.inner.stream.recv_expect::<ParseComplete>().await?;
                let parameters = recv_desc_params(conn).await?;

                let rows = recv_desc_rows(conn).await?;

                let parameters = conn.handle_parameter_description(parameters).await?;

                let (columns, column_names) = conn.handle_row_description(rows, true).await?;

                // ensure that if we did fetch custom data, we wait until we are fully ready before
                // continuing
                conn.recv_ready_for_query().await?;

                let metadata = Arc::new(PgStatementMetadata {
                    parameters,
                    columns,
                    column_names: Arc::new(column_names),
                });

                conn.store_to_cache(&self.sql, (*statement_id, metadata.clone()))?;
                state.preparing.remove(&self.sql);

                PipelineStep::BindExecuteClose(*statement_id, metadata)
            }
            PipelineStep::BindExecuteClose(statement_id, param) => {
                println!("BindExecuteClose");
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
                println!("ReceiveData");
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
                        self.is_done = true;

                        let rows_affected = cc.rows_affected();
                        // logger.increase_rows_affected(rows_affected);
                        if self.sender.send(Some(Ok(Either::Left(PgQueryResult {
                            rows_affected,
                        })))).is_err() {
                            // println!("Done");
                            return Ok(());
                        }
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

                        let _ = self.sender.send(Some(Ok(Either::Right(row))));
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
                let _ = self.sender.send(None);
                self.is_done = true;
                return Ok(());
            }
            PipelineStep::FillCache(query, rx, oids, named_types) => {
                println!("FillCache");
                let mut query = query.clone();
                Box::pin(async { query.next(conn, state).await }).await?;

                self.should_flush_before_next = query.should_flush_before_next;

                // println!("Is_done: {}", query.is_done);

                if query.is_done {
                    let mut rows = Vec::new();
                    while let Ok(Some(res)) = rx.recv_async().await {
                        let res = res?;
                        if let Either::Right(row) = res {
                            let typed_row = FetchByOid::from_row(&row)?;
                            if let Some(ref name) = typed_row.type_name {
                                conn.inner.cache_type_oid.insert(
                                    UStr::Shared(name.clone().into()),
                                    typed_row.fetched_oid,
                                );
                            }

                            rows.push(typed_row);
                        }
                    }
                    for oid in oids {
                        let ty = recursive_find(&mut rows, *oid).expect("Bug");
                        conn.inner.cache_type_info.insert(*oid, ty);
                    }

                    for named in named_types {
                        let oid = conn
                            .inner
                            .cache_type_oid
                            .get(&UStr::Shared(named.clone().into()))
                            .copied()
                            .expect("BUG");
                        let ty = recursive_find(&mut rows, oid).expect("Bug");
                        conn.inner.cache_type_info.insert(oid, ty);
                    }
                    // dbg!(&conn.inner.cache_type_oid);
                    // dbg!(&conn.inner.cache_type_info);
                    state.depth -= 1;
                    PipelineStep::ParseDescribe
                } else {
                    PipelineStep::FillCache(query, rx.clone(), oids.to_vec(), named_types.to_vec())
                }
            }
        };

        Ok(())
    }

    pub async fn handle_error(
        &mut self,
        state: &mut PipelineState,
        conn: &mut PgConnection,
    ) -> sqlx_core::Result<()> {
        let _ = state.preparing.remove(&self.sql);

        conn.recv_ready_for_query().await?;
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

const SQL: &'static str = "
WITH RECURSIVE fetch_type AS (
    SELECT
        oid::regtype::text AS name,
        pg_type.oid,
        typtype,
        typcategory,
        typrelid,
        typelem,
        typbasetype,
        ARRAY(
            SELECT enumlabel
            FROM pg_enum
            WHERE enumtypid = pg_type.oid
        ) AS enum_labels,
        ARRAY(
            SELECT (attname, atttypid)
            FROM pg_attribute AS attr
            WHERE
                attr.attrelid = pg_type.typrelid
                AND NOT attr.attisdropped
                AND attr.attnum > 0
        ) AS attr_oids,
       (select t
        			  from unnest($2::text[])
        			     as t
        			     where t::regtype::oid = pg_type.oid)
    FROM
        pg_type
    WHERE
        pg_type.oid = any(
        	array_cat(
        		$1::OID[],
        		ARRAY(
        			select t::regtype::oid
        			  from unnest($2::text[])
        			     as t
        	)))
    UNION ALL

    SELECT
        t.oid::regtype::text AS name,
        t.oid,
        t.typtype,
        t.typcategory,
        t.typrelid,
        t.typelem,
        t.typbasetype,
        ARRAY(
            SELECT enumlabel
            FROM pg_enum
            WHERE enumtypid = t.oid
        ) AS enum_labels,
        ARRAY(
            SELECT (attname, atttypid)
            FROM pg_attribute AS attr
            WHERE
                attr.attrelid = t.typrelid
                AND NOT attr.attisdropped
                AND attr.attnum > 0
        ) AS attr_oids,
       	null as t
    FROM
        pg_type t
    INNER JOIN
        fetch_type ft1
        ON t.oid = ft1.typbasetype
           OR t.oid = ft1.typelem
           OR t.oid = ft1.typrelid
           OR t.oid = ANY(ARRAY(
               SELECT atttypid
               FROM pg_attribute AS attr
               WHERE
                   attr.attrelid = ft1.typrelid
                   AND NOT attr.attisdropped
                   AND attr.attnum > 0
           ))
)
SELECT
    name,
    oid,
    typtype,
    typcategory,
    typbasetype,
    enum_labels,
    attr_oids,
    t
FROM
    fetch_type
ORDER BY
    oid
";
