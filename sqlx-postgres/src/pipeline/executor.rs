use crate::{
    describe::Describe, PgConnection, PgQueryResult, PgRow, PgStatement, PgTypeInfo, Postgres,
};
use std::{fmt::Debug, ops::DerefMut};

use futures_core::{future::BoxFuture, stream::BoxStream};
use sqlx_core::{
    executor::{Execute, Executor},
    Either, Error,
};

use super::PgPipeline;

pub(super) const ERROR_MSG: &'static str = "BUG: Pipeline::join cannot be called multiple times";

impl<'c, C: Send + Debug + Sync + DerefMut<Target = PgConnection>> Executor<'c>
    for &'c PgPipeline<C>
{
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
        let mut lock = self.lock();
        let pipeline = Option::as_mut(&mut lock).expect(ERROR_MSG);

        let rx = pipeline.push(query);

        Box::pin(try_stream! {
            while let Ok(Some(v)) = rx.recv_async().await {
                let v = v?;
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q, E>(self, query: E) -> BoxFuture<'e, Result<Option<PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let mut lock = self.lock();
        let pipeline = Option::as_mut(&mut lock).expect(ERROR_MSG);
        let rx = pipeline.push(query);

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
