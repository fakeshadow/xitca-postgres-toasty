mod r#type;
mod value;

use core::{
    fmt,
    future::Future,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll, ready},
};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_core::stream::Stream;
use toasty_core::{
    Result,
    driver::{Capability, Driver, Operation, Response},
    schema::db::{Schema, Table},
    stmt,
    stmt::ValueRecord,
};
use toasty_sql::{self as sql, serializer::Placeholder};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use xitca_postgres::{
    Client, Config, Execute, RowStreamOwned, Statement, iter::AsyncLendingIterator, types::Type,
};

use crate::{r#type::TypeExt, value::Value};

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type CachedStatement = Arc<Statement>;

pub struct PostgreSQL {
    cfg: Config,
    concurrent_level: usize,
    conn: tokio::sync::Mutex<Option<Arc<_Connection>>>,
}

impl fmt::Debug for PostgreSQL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresSQL Driver")
    }
}

const DEFAULT_CONCURRENT_LEVEL: usize = 512;

impl PostgreSQL {
    pub fn new(url: &str) -> Result<Self> {
        let cfg = Config::try_from(url)?;
        Ok(Self::from_config(cfg))
    }

    pub fn from_config(cfg: Config) -> Self {
        Self {
            cfg,
            concurrent_level: DEFAULT_CONCURRENT_LEVEL,
            conn: Default::default(),
        }
    }

    /// adjust how many concurrent connections can be made from this driver.
    ///
    /// The lowerbound concurrency is 1
    /// The uppperbound concurrency is determined by tokio's [`Semaphore::MAX_PERMITS`]
    pub fn concurrent_level(&mut self, size: usize) -> &mut Self {
        assert!(
            size != 0 && size < Semaphore::MAX_PERMITS,
            "concurrent level is beyond it's range bound"
        );
        self.concurrent_level = size;
        self
    }
}

pub struct Connection {
    _permit: OwnedSemaphorePermit,
    inner: Arc<_Connection>,
}

impl Deref for Connection {
    type Target = _Connection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct _Connection {
    client: Client,
    permits: Arc<Semaphore>,
    concurrent_level: usize,
    cache: Mutex<HashMap<String, CachedStatement>>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("PostgresConnection")
    }
}

const SEMAPHORE_UNWRAP_MSG: &str = "Semaphore must not be closed when Connection is still alive";

impl _Connection {
    /// Initialize a Toasty PostgreSQL driver using an initialized connection.
    fn new(client: Client, concurrent_level: usize) -> Arc<Self> {
        Arc::new(_Connection {
            client,
            permits: Arc::new(Semaphore::new(concurrent_level as _)),
            concurrent_level,
            cache: Mutex::new(HashMap::new()),
        })
    }

    async fn connect(cfg: Config, concurrent_level: usize) -> Result<Arc<Self>> {
        let (client, mut driver) = xitca_postgres::Postgres::new(cfg).connect().await?;

        tokio::spawn(async move {
            loop {
                match driver.try_next().await {
                    Ok(Some(_)) => {}
                    Ok(None) => return,
                    Err(e) => eprintln!("connection error: {e}"),
                }
            }
        });

        Ok(Self::new(client, concurrent_level))
    }
}

impl Connection {
    /// Creates a table.
    pub async fn create_table(&self, schema: &Schema, table: &Table) -> Result<()> {
        let serializer = sql::Serializer::postgresql(schema);

        let mut params = Vec::new();
        let sql = serializer.serialize(
            &sql::Statement::create_table(table, &Capability::POSTGRESQL),
            &mut params,
        );

        assert!(
            params.is_empty(),
            "creating a table shouldn't involve any parameters"
        );

        sql.execute(&self.client).await?;

        // NOTE: `params` is guaranteed to be empty based on the assertion above. If
        // that changes, `params.clear()` should be called here.
        for index in &table.indices {
            if index.primary_key {
                continue;
            }

            let sql = serializer.serialize(&sql::Statement::create_index(index), &mut params);

            assert!(
                params.is_empty(),
                "creating an index shouldn't involve any parameters"
            );

            sql.execute(&self.client).await?;
        }

        Ok(())
    }

    /// Drops a table.
    pub async fn drop_table(&self, schema: &Schema, table: &Table, if_exists: bool) -> Result<()> {
        let serializer = sql::Serializer::postgresql(schema);
        let mut params = Vec::new();

        let sql = if if_exists {
            serializer.serialize(&sql::Statement::drop_table_if_exists(table), &mut params)
        } else {
            serializer.serialize(&sql::Statement::drop_table(table), &mut params)
        };

        assert!(
            params.is_empty(),
            "dropping a table shouldn't involve any parameters"
        );

        sql.execute(&self.client).await?;
        Ok(())
    }

    async fn prepare_cached(&self, sql: String, types: &[Type]) -> Result<CachedStatement> {
        let stmt = self.cache.lock().unwrap().get(&sql).cloned();
        match stmt {
            Some(stmt) => Ok(stmt),
            None => {
                let stmt = Statement::named(&sql, types)
                    .execute(&self.client)
                    .await?
                    .leak();

                let stmt = Arc::new(stmt);

                self.cache.lock().unwrap().insert(sql, stmt.clone());

                Ok(stmt)
            }
        }
    }
}

impl Driver for PostgreSQL {
    fn connect<'s, 'f>(
        &'s self,
    ) -> BoxedFuture<'f, Result<Box<dyn toasty_core::driver::Connection>>>
    where
        's: 'f,
    {
        Box::pin(async move {
            let conn = {
                let mut inner = self.conn.lock().await;
                match *inner {
                    Some(ref conn) if !conn.client.closed() => conn.clone(),
                    _ => {
                        let conn =
                            _Connection::connect(self.cfg.clone(), self.concurrent_level).await?;
                        *inner = Some(conn.clone());
                        let _permit = conn
                            .permits
                            .clone()
                            .try_acquire_owned()
                            .expect(SEMAPHORE_UNWRAP_MSG);
                        return Ok(Box::new(Connection {
                            _permit,
                            inner: conn,
                        }) as _);
                    }
                }
            };

            let _permit = conn
                .permits
                .clone()
                .acquire_owned()
                .await
                .expect(SEMAPHORE_UNWRAP_MSG);

            Ok(Box::new(Connection {
                _permit,
                inner: conn,
            }) as _)
        })
    }

    fn max_connections(&self) -> Option<usize> {
        Some(self.concurrent_level as _)
    }
}

impl toasty_core::driver::Connection for Connection {
    fn capability(&self) -> &'static Capability {
        &Capability::POSTGRESQL
    }

    fn exec<'s, 'sch, 'f>(
        &'s mut self,
        schema: &'sch Arc<Schema>,
        op: Operation,
    ) -> BoxedFuture<'f, Result<Response>>
    where
        's: 'f,
        'sch: 'f,
    {
        Box::pin(async move {
            let (sql, ret_tys) = match op {
                Operation::Insert(op) => (sql::Statement::from(op.stmt), Vec::new()),
                Operation::QuerySql(query) => {
                    assert!(
                        query.last_insert_id_hack.is_none(),
                        "last_insert_id_hack is MySQL-specific and should not be set for PostgreSQL"
                    );
                    (query.stmt.into(), query.ret.unwrap_or_default())
                }
                Operation::Transaction(tx) => {
                    let _permit = self
                        .permits
                        .acquire_many(self.concurrent_level as u32 - 1)
                        .await
                        .expect(SEMAPHORE_UNWRAP_MSG);
                    todo!("op={:#?}", Operation::Transaction(tx))
                }
                op => todo!("op={:#?}", op),
            };

            let width = sql.returning_len();

            let mut params = Params::default();
            let sql_as_str = sql::Serializer::postgresql(schema).serialize(&sql, &mut params);

            let types = if width.is_none() {
                Vec::new()
            } else {
                params
                    .iter()
                    .map(|param| param.infer_ty().to_postgres_type())
                    .collect::<Vec<_>>()
            };

            let stmt = self.prepare_cached(sql_as_str, &types).await?;

            let stmt = stmt.bind(params.iter());

            if width.is_none() {
                let count = stmt.execute(&self.client).await?;
                Ok(Response::count(count))
            } else {
                let stream = stmt.into_owned().query(&self.client).await?;
                Ok(Response::value_stream(RowStream {
                    types: ret_tys,
                    stream,
                }))
            }
        })
    }

    fn reset_db<'s, 'sch, 'f>(&'s mut self, schema: &'sch Schema) -> BoxedFuture<'f, Result<()>>
    where
        's: 'f,
        'sch: 'f,
    {
        Box::pin(async {
            for table in &schema.tables {
                self.drop_table(schema, table, true).await?;
                self.create_table(schema, table).await?;
            }
            Ok(())
        })
    }
}

pin_project_lite::pin_project! {
    struct RowStream {
        types: Vec<stmt::Type>,
        #[pin]
        stream: RowStreamOwned
    }
}

impl From<RowStream> for stmt::ValueStream {
    fn from(stream: RowStream) -> Self {
        Self::from_stream(stream)
    }
}

impl Stream for RowStream {
    type Item = Result<stmt::Value>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = ready!(this.stream.poll_next(cx)?).map(|row| {
            let fields = row
                .columns()
                .iter()
                .enumerate()
                .map(|(i, column)| value::from_sql(i, &row, column, &this.types[i]))
                .collect::<Vec<_>>();
            Ok(ValueRecord::from_vec(fields).into())
        });

        Poll::Ready(res)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        Stream::size_hint(&self.stream)
    }
}

#[derive(Default, Debug)]
struct Params(Vec<Value>);

impl Deref for Params {
    type Target = Vec<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl toasty_sql::Params for Params {
    fn push(&mut self, param: &stmt::Value) -> Placeholder {
        self.0.push(Value::from(param.clone()));
        Placeholder(self.0.len())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connect() {
        let conn = PostgreSQL::new("postgres://postgres:postgres@localhost:5432").unwrap();

        let db = toasty::Db::builder()
            .register::<User>()
            .build(conn)
            .await
            .unwrap();

        db.reset_db().await.unwrap();

        #[derive(Debug, toasty::Model)]
        struct User {
            #[key]
            id: i32,
            name: String,
        }

        User::create()
            .id(123)
            .name(String::from("john"))
            .exec(&db)
            .await
            .unwrap();

        let user = User::get_by_id(&db, 123).await.unwrap();

        assert_eq!(user.id, 123);
        assert_eq!(user.name, "john")
    }
}
