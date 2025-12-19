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
use tokio::{sync::Semaphore, task::JoinHandle};
use xitca_postgres::{
    Client, Config, Execute, RowStreamOwned, Statement, iter::AsyncLendingIterator,
};

use crate::{r#type::TypeExt, value::Value};

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct PostgreSQL {
    cfg: Config,
    concurrency: usize,
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
            concurrency: DEFAULT_CONCURRENT_LEVEL,
            conn: Default::default(),
        }
    }

    /// adjust how many concurrent connections can be made from this driver
    ///
    /// concurrent connections are multiple shared client backed by a single database driver
    ///
    /// The lowerbound concurrency is 1
    /// The uppperbound concurrency is determined by tokio's [`Semaphore::MAX_PERMITS`]
    pub fn concurrency(&mut self, size: usize) -> &mut Self {
        assert!(
            size != 0 && size < Semaphore::MAX_PERMITS,
            "concurrent level is beyond it's range bound"
        );
        self.concurrency = size;
        self
    }
}

pub struct Connection {
    inner: Arc<_Connection>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("PostgresConnection")
    }
}

impl Deref for Connection {
    type Target = _Connection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct _Connection {
    client: Client,
    handle: Mutex<Option<JoinHandle<Result<(), xitca_postgres::Error>>>>,
    permits: Arc<Semaphore>,
    concurrency: u32,
    cache: tokio::sync::Mutex<HashMap<String, Statement>>,
}

const SEMAPHORE_UNWRAP_MSG: &str = "Semaphore must not be closed when Connection is still alive";

impl _Connection {
    async fn connect(cfg: Config, concurrency: usize) -> Result<Arc<Self>> {
        let (client, mut driver) = xitca_postgres::Postgres::new(cfg).connect().await?;

        let handle = tokio::spawn(async move {
            while driver.try_next().await?.is_some() {}
            Ok::<_, xitca_postgres::Error>(())
        });

        Ok(Arc::new(_Connection {
            client,
            handle: Mutex::new(Some(handle)),
            permits: Arc::new(Semaphore::new(concurrency)),
            concurrency: concurrency
                .try_into()
                .expect("PostgreSQL::concurrency received an illformed size"),
            cache: Default::default(),
        }))
    }

    async fn join_error(&self) -> Result<()> {
        let handle = self.handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.await??;
        }
        Ok(())
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

        self.execute(sql.execute(&self.client)).await?;
        Ok(())
    }

    async fn execute<F, T>(&self, exec: F) -> Result<T>
    where
        F: Future<Output = Result<T, xitca_postgres::Error>>,
    {
        match exec.await {
            Ok(res) => Ok(res),
            Err(e) => {
                let is_driver_down = e.is_driver_down();
                let mut e = e.into();
                // try to join the driver task when driver is gone. it would offer more
                // detailed error message if there is any
                if is_driver_down {
                    e = self.join_error().await.err().unwrap_or(e);
                }
                Err(e)
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
                        let conn = _Connection::connect(self.cfg.clone(), self.concurrency).await?;
                        *inner = Some(conn.clone());
                        conn
                    }
                }
            };

            Ok(Box::new(Connection { inner: conn }) as _)
        })
    }

    fn max_connections(&self) -> Option<usize> {
        Some(self.concurrency)
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
                    // acquire all possible permits before interacting with db driver
                    // this is for query need exclusive access to db driver e.g: transaction, copy in
                    let _permit = self
                        .permits
                        .acquire_many(self.concurrency)
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

            // acquire one permit before interacting with db driver
            // this is for query that can be operated concurrently.
            let permit = self.permits.acquire().await.expect(SEMAPHORE_UNWRAP_MSG);

            let mut cache = self.cache.lock().await;

            let stmt = match cache.get(&sql_as_str) {
                Some(stmt) => stmt,
                None => {
                    let stmt = self
                        .execute(Statement::named(&sql_as_str, &types).execute(&self.client))
                        .await?
                        .leak();

                    cache.insert(sql_as_str.clone(), stmt);

                    cache.get(&sql_as_str).unwrap()
                }
            };

            let stmt = stmt.bind(params.iter());

            if width.is_none() {
                let fut = stmt.execute(&self.client);

                drop(cache);
                // at this point the interaction in direction from client to driver has finished.
                // release permit so other concurrent client can observe the state change
                drop(permit);

                self.execute(fut).await.map(Response::count)
            } else {
                let fut = stmt.into_owned().query(&self.client);

                drop(cache);
                drop(permit);

                self.execute(fut).await.map(|stream| {
                    Response::value_stream(RowStream {
                        types: ret_tys,
                        stream,
                    })
                })
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
