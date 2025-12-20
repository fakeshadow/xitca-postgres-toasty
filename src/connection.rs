use core::{fmt, ops::Deref};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use toasty_core::{
    Result, async_trait,
    driver::{Capability, Connection as ConnectionTrait, Operation, Response},
    schema::db::{Schema, Table},
    stmt,
};
use toasty_sql::{self as sql, serializer::Placeholder};
use tokio::{sync::Semaphore, task::JoinHandle};
use xitca_postgres::{Client, Config, Execute, Statement, iter::AsyncLendingIterator};

use crate::{r#type::TypeExt, value::Value};

pub struct Connection {
    inner: Arc<_Connection>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("PostgresConnection")
    }
}

struct _Connection {
    client: Client,
    handle: Mutex<Option<JoinHandle<Result<(), xitca_postgres::Error>>>>,
    permits: Arc<Semaphore>,
    concurrency: u32,
    cache: tokio::sync::Mutex<HashMap<String, Statement>>,
}

const SEMAPHORE_UNWRAP_MSG: &str = "Semaphore must not be closed when Connection is still alive";

impl Connection {
    pub(crate) async fn connect(cfg: Config, concurrency: usize) -> Result<Self> {
        let (client, mut driver) = xitca_postgres::Postgres::new(cfg).connect().await?;

        let handle = tokio::spawn(async move {
            while driver.try_next().await?.is_some() {}
            Ok::<_, xitca_postgres::Error>(())
        });

        Ok(Connection {
            inner: Arc::new(_Connection {
                client,
                handle: Mutex::new(Some(handle)),
                permits: Arc::new(Semaphore::new(concurrency)),
                concurrency: concurrency
                    .try_into()
                    .expect("PostgreSQL::concurrency received an illformed size"),
                cache: Default::default(),
            }),
        })
    }

    pub(crate) fn try_clone(&self) -> Option<Self> {
        if self.inner.client.closed() {
            None
        } else {
            Some(Connection {
                inner: self.inner.clone(),
            })
        }
    }
}

impl _Connection {
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
                if is_driver_down && let Err(err) = self.join_error().await {
                    e = err;
                }
                Err(e)
            }
        }
    }

    async fn join_error(&self) -> Result<()> {
        let handle = self.handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.await??;
        }
        Ok(())
    }

    // Creates a table.
    async fn create_table(
        &self,
        schema: &Schema,
        table: &Table,
    ) -> Result<(), xitca_postgres::Error> {
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

    // Drops a table.
    async fn drop_table(
        &self,
        schema: &Schema,
        table: &Table,
        if_exists: bool,
    ) -> Result<(), xitca_postgres::Error> {
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

    async fn exec(
        &self,
        schema: &Arc<Schema>,
        op: Operation,
    ) -> Result<Response, xitca_postgres::Error> {
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
                .collect()
        };

        // acquire one permit before interacting with db driver
        // this is for query that can be operated concurrently.
        let permit = self.permits.acquire().await.expect(SEMAPHORE_UNWRAP_MSG);

        let mut cache = self.cache.lock().await;

        let stmt = match cache.get(&sql_as_str) {
            Some(stmt) => stmt,
            None => {
                let stmt = Statement::named(&sql_as_str, &types)
                    .execute(&self.client)
                    .await?
                    .leak();
                cache.entry(sql_as_str).or_insert(stmt)
            }
        };

        let stmt = stmt.bind(params.iter());

        if width.is_none() {
            let fut = stmt.execute(&self.client);

            drop(cache);
            // at this point the interaction in direction from client to driver has finished.
            // release permit so other concurrent client can observe the state change
            drop(permit);

            fut.await.map(Response::count)
        } else {
            let fut = stmt.into_owned().query(&self.client);

            drop(cache);
            drop(permit);

            fut.await
                .map(|stream| Response::value_stream(crate::async_iter::stream(stream, ret_tys)))
        }
    }

    async fn reset_db(&self, schema: &Schema) -> Result<(), xitca_postgres::Error> {
        for table in &schema.tables {
            self.drop_table(schema, table, true).await?;
            self.create_table(schema, table).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ConnectionTrait for Connection {
    fn capability(&self) -> &'static Capability {
        &Capability::POSTGRESQL
    }

    #[inline]
    async fn exec(&mut self, schema: &Arc<Schema>, op: Operation) -> Result<Response> {
        self.inner.execute(self.inner.exec(schema, op)).await
    }

    async fn reset_db(&mut self, schema: &Schema) -> Result<()> {
        self.inner.execute(self.inner.reset_db(schema)).await
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
