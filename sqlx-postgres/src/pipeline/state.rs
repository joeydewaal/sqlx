use futures_util::stream::StreamExt;
use std::sync::Arc;

use flume::Receiver;
use sqlx_core::{ext::ustr::UStr, Either, Error};

use crate::{
    io::{PortalId, StatementId},
    message::{self, Bind, Close, Parse},
    pipeline::cache::{recursive_find, FetchByOid},
    statement::PgStatementMetadata,
    type_info::PgType,
    types::Oid,
    PgArguments, PgConnection, PgQueryResult, PgRow, PgTypeInfo, PgValueFormat,
};

use super::context::PipelineContext;

// Holds the state machine and data needed to execute a query.
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

    async fn handle_error(&self) -> sqlx_core::Result<()> {
        match &self.next_step {
            PipelineStep::FillCache(q, _, _, _) => Box::pin(q.handle_error()).await,
            _ => Ok(()),
        }
    }
}

// All the states needed to execute a prepared statement.
// The states are in the order that they are executed.
#[derive(Clone)]
pub enum PipelineStep {
    // Lookup in the statement cache if the query is already prepared.
    //  -> Go to `ParseDescribe` is it is not in cache. The statement needs to be prepared.
    //
    //  -> Go to `BindExecuteClose` if it is cached. The query is ready to be executed.
    GetOrPrepare,

    // If there are any custom types in the query arguments see if they are cached
    // -> If they are not cached go to FillCache and fetch them
    //
    // -> If they are in the type cache, push a `Parse` and `Describe` in the write buffer. Parse
    // prepares the query, describe fetches metadata for the rows that are returned
    ParseDescribe,

    // Receives `ParseComplete` for the `Parse` message, `ParameterDescription` and
    // `RowDescription` for the `Describe` message.
    //
    // Lookup (custom) types in the rows in cache.
    // -> If they are not cached, go to `FillCache` and look them up.
    // -> If all the types are in cache/known, go to `BindExecuteClose`
    ReceiveParseDescribe(StatementId),

    // Push a `Bind`, `Execute`, `Close` and `Sync` message in the write buffer. This step executes
    // the query. After this Postgres sends back data rows. Go to `ReceiveData` to receive the data.
    BindExecuteClose(StatementId, Arc<PgStatementMetadata>),

    // Receives the data rows. After this the query is done executing After this the query is done
    // executing.
    ReceiveData(Arc<PgStatementMetadata>),

    // This step fetches the given Oids and named types and stores them in cache.
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
        context: &mut PipelineContext<'_>,
    ) -> sqlx_core::Result<()> {
        if let Err(err) = self.next(context).await {
            // Remove the query from the actively preparing queries.
            let _ = context.preparing.remove(&self.sql);

            self.handle_error().await?;

            self.is_done = true;
            let _ = self.sender.send(Some(Err(err)));
        }
        Ok(())
    }

    #[inline(always)]
    fn write_parse_into(
        &self,
        conn: &mut PgConnection,
        oids: &[Oid],
        stmt_id: StatementId,
    ) -> sqlx_core::Result<()> {
        conn.inner.stream.write_msg(Parse {
            param_types: &oids,
            query: &self.sql,
            statement: stmt_id,
        })
    }

    #[inline(always)]
    fn write_describe_into(
        &self,
        conn: &mut PgConnection,
        stmt_id: StatementId,
    ) -> sqlx_core::Result<()> {
        conn.inner
            .stream
            .write_msg(message::Describe::Statement(stmt_id))
    }

    #[inline(always)]
    fn write_bind_into(
        &self,
        conn: &mut PgConnection,
        statement: StatementId,
        portal: PortalId,
    ) -> sqlx_core::Result<()> {
        conn.inner.stream.write_msg(Bind {
            portal,
            statement,
            formats: &[PgValueFormat::Binary],
            num_params: self.arguments.safe_len()?,
            params: &self.arguments.buffer,
            result_formats: &[PgValueFormat::Binary],
        })
    }

    #[inline(always)]
    fn write_execute_into(
        &self,
        conn: &mut PgConnection,
        portal: PortalId,
        limit: u32,
    ) -> sqlx_core::Result<()> {
        conn.inner
            .stream
            .write_msg(message::Execute { portal, limit })
    }

    #[inline(always)]
    fn write_close_into(&self, conn: &mut PgConnection, portal: PortalId) -> sqlx_core::Result<()> {
        conn.inner.stream.write_msg(Close::Portal(portal))
    }

    pub async fn next(&mut self, context: &mut PipelineContext<'_>) -> sqlx_core::Result<()> {
        self.next_step = match &mut self.next_step {
            PipelineStep::GetOrPrepare => {
                // println!("GetOrPrepare");

                if let Some((stmt_id, meta)) = context.get_prepared(&self.sql) {
                    // If this statement is prepared, go and execute it.
                    PipelineStep::BindExecuteClose(stmt_id, meta)
                } else if context.is_preparing_this_iter(&self.sql) {
                    // If this statement is already being prepared, we do nothing. Check the cache
                    // again next iteration.
                    PipelineStep::GetOrPrepare
                } else {
                    // Else we need to prepare the query.

                    // Make sure we don't prepare this query again this iteration.
                    context.register_prepare(self.sql.clone());
                    PipelineStep::ParseDescribe
                }
            }
            PipelineStep::ParseDescribe => {
                println!("ParseDescribe");

                // Get the id for the next named prepared statement
                let stmt_id = context.next_stmt_id();

                let mut unresolved_oids = Vec::new();
                let mut unresolved_typ = Vec::new();

                let mut oids = Vec::new();
                let mut argument_types = Vec::new();

                for ty in &self.arguments.types {
                    if let Some(oid) = get_oid(&ty.0, context.conn) {
                        if let Some(ty) = get_pg_type(oid, context.conn) {
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
                    println!("Fetching types: {unresolved_oids:?} {unresolved_typ:?}");

                    let mut arguments = PgArguments::default();

                    arguments.add(&unresolved_oids).map_err(Error::Encode)?;
                    arguments.add(&unresolved_typ).map_err(Error::Encode)?;

                    context.depth += 1;
                    let (q_state, rx) = QueryState::new(SQL.to_string(), arguments);
                    PipelineStep::FillCache(Box::new(q_state), rx, unresolved_oids, unresolved_typ)
                } else {
                    // All oid of arguments (Domain, Composite,...) are fetched and in cache. This
                    // query is ready to be prepared.

                    // Write a `Parse` message. This prepares the query.
                    self.write_parse_into(&mut context.conn, &oids, stmt_id)?;

                    // Write a `Describe` message, this gives info about the prepared statement so
                    // SQLx knows how to interpered the data rows.
                    self.write_describe_into(&mut context.conn, stmt_id)?;

                    // Write a `Sync` message. This makes sure that we get a response on the next
                    // iteration.
                    context.conn.write_sync();

                    // Apply patches that are needed to the given arguments. This should NEVER
                    // fetch types.
                    self.arguments
                        .apply_patches(context.conn, &argument_types)
                        .await?;

                    // To ensure we get a response from Postgres we should flush the buffer before
                    // the next state is executed.
                    self.should_flush_before_next = true;

                    PipelineStep::ReceiveParseDescribe(stmt_id)
                }
            }
            PipelineStep::ReceiveParseDescribe(statement_id) => {
                println!("ReceiveParseDescribe");

                // Receive a `ParseComplete` message from our previous `Parse` command.
                context.receive_parse_complete().await?;

                // Receive `ParameterDescription` and `RowDescription` from the previous
                // `Describe` command.
                let (parameters, rows) = context.handle_param_row_description().await?;

                let mut unresolved_oids = Vec::new();
                if let Some(rows) = &rows {
                    for field in &rows.fields {
                        if PgTypeInfo::try_from_oid(field.data_type_id).is_none()
                            && context
                                .conn
                                .inner
                                .cache_type_info
                                .get(&field.data_type_id)
                                .is_none()
                        {
                            unresolved_oids.push(field.data_type_id);
                        }
                    }
                }

                if !unresolved_oids.is_empty() {
                    dbg!(&unresolved_oids);
                    context.conn.recv_ready_for_query().await?;
                    let mut arguments = PgArguments::default();

                    arguments.add(&unresolved_oids).map_err(Error::Encode)?;
                    arguments.add(&[""; 0]).map_err(Error::Encode)?;

                    context.depth += 1;
                    println!("Unresolved oids: {unresolved_oids:?}");
                    let (q_state, rx) = QueryState::new(SQL.to_string(), arguments);
                    PipelineStep::FillCache(Box::new(q_state), rx, unresolved_oids, Vec::new())
                } else {
                    // All custom types (from arguments and row fields) are in the type cache, we
                    // are ready to execute the query.

                    // Get statement metadata from connection type cache.
                    let metadata = context.statement_metadata(parameters, rows).await?;

                    // ensure that if we did fetch custom data, we wait until we are fully ready before
                    // continuing
                    context.conn.recv_ready_for_query().await?;

                    // Store the prepared statement to the connection cache, also remove the query
                    // from the shared pipeline state.
                    context.store_to_cache(*statement_id, &self.sql, metadata.clone())?;

                    PipelineStep::BindExecuteClose(*statement_id, metadata)
                }
            }
            PipelineStep::BindExecuteClose(stmt_id, param) => {
                println!("BindExecuteClose");
                let param = param.clone();

                // Write a `Bind` message into the buffer with the arguments.
                let stmt = *stmt_id;
                self.write_bind_into(context.conn, stmt, PortalId::UNNAMED)?;

                // println!("Writing EXECUTE");
                // Write an `Execute` message into the buffer to execute the prepared statement.
                self.write_execute_into(context.conn, PortalId::UNNAMED, 0)?;

                // println!("Writing CLOSE");
                // Write a `Close` message to close the open portal.
                self.write_close_into(context.conn, PortalId::UNNAMED)?;

                // Write a `Sync` message to get a result back from Postgres.
                context.conn.write_sync();

                // To ensure we get a response on the next iteration we schedule a flush.
                self.should_flush_before_next = true;

                PipelineStep::ReceiveData(param)
            }
            PipelineStep::ReceiveData(meta) => {
                println!("ReceiveData");

                let mut stream =
                    context
                        .conn
                        .receive_rows(&self.sql, meta.clone(), PgValueFormat::Binary)?;

                while let Some(value) = stream.next().await {
                    if self.sender.send(Some(value)).is_err() {
                        self.is_done = true;
                    };
                }
                let _ = self.sender.send(None);
                self.is_done = true;
                return Ok(());
            }
            PipelineStep::FillCache(query, rx, oids, named_types) => {
                println!("FillCache");
                Box::pin(query.next(context)).await?;

                self.should_flush_before_next = query.should_flush_before_next;

                if query.is_done {
                    let mut rows = Vec::new();
                    while let Ok(Some(res)) = rx.recv_async().await {
                        let res = res?;
                        if let Either::Right(row) = res {
                            let typed_row = FetchByOid::from_row(&row)?;
                            if let Some(ref name) = typed_row.type_name {
                                context.conn.inner.cache_type_oid.insert(
                                    UStr::Shared(name.clone().into()),
                                    typed_row.fetched_oid,
                                );
                            }

                            rows.push(typed_row);
                        }
                    }
                    for oid in oids {
                        let ty = recursive_find(&mut rows, *oid).expect("Bug");
                        context.conn.inner.cache_type_info.insert(*oid, ty);
                    }

                    for named in named_types {
                        let oid = context
                            .conn
                            .inner
                            .cache_type_oid
                            .get(&UStr::Shared(named.clone().into()))
                            .copied()
                            .expect("BUG");
                        let ty = recursive_find(&mut rows, oid).expect("Bug");
                        context.conn.inner.cache_type_info.insert(oid, ty);
                    }
                    // dbg!(&conn.inner.cache_type_oid);
                    // dbg!(&conn.inner.cache_type_info);
                    context.depth -= 1;
                    PipelineStep::ParseDescribe
                } else {
                    println!("Here");
                    PipelineStep::FillCache(
                        query.clone(),
                        rx.clone(),
                        oids.to_vec(),
                        named_types.to_vec(),
                    )
                }
            }
        };

        Ok(())
    }
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
