use crate::{describe::Describe, PgQueryResult, PgRow, PgStatement, PgTypeInfo, Postgres};
use futures_core::{future::BoxFuture, stream::BoxStream};
use sqlx_core::{
    executor::{Execute, Executor},
    Either, Error,
};

use super::{state::QueryState, PgPipeline};

impl<'c> Executor<'c> for &'c PgPipeline {
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
        let args = query.take_arguments();

        // TODO: Arguments
        let (q_state, rx) = QueryState::new(query.sql().to_string(), args.unwrap().unwrap());
        let _ = self.tx.send(super::worker::Command::Query(q_state));

        Box::pin(try_stream! {
            while let Ok(Some(v)) = rx.recv_async().await {
                let v = v?;
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
        let args = query.take_arguments();

        // TODO: Arguments
        let (q_state, rx) = QueryState::new(query.sql().to_string(), args.unwrap().unwrap());
        let _ = self.tx.send(super::worker::Command::Query(q_state));
        Box::pin(async move {
            let mut ret = None;
            while let Ok(Some(result)) = rx.recv_async().await {
                match result? {
                    Either::Right(r) if ret.is_none() => ret = Some(r),
                    _ => {}
                }
            }
            Ok(ret)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        _sql: &'q str,
        _parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement<'q>, Error>>
    where
        'c: 'e,
    {
        todo!()
    }

    fn describe<'e, 'q: 'e>(
        self,
        _sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        todo!()
    }
}
