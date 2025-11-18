mod value;
pub(crate) use value::Value;

use core::{fmt, future::Future, pin::Pin};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use toasty_core::{
    Driver, Result,
    driver::{Capability, Operation, Response},
    schema::db::{Schema, Table},
    stmt,
    stmt::ValueRecord,
};
use toasty_sql as sql;
use xitca_postgres::{
    Client, Column, Config, Execute, Statement,
    iter::AsyncLendingIterator,
    row::Row,
    types::{ToSql, Type},
};

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type CachedStatement = Arc<Statement>;

pub struct PostgreSQL {
    client: Client,
    cache: Mutex<HashMap<String, CachedStatement>>,
}

impl fmt::Debug for PostgreSQL {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("PostgreSQL_Client")
    }
}

impl PostgreSQL {
    /// Initialize a Toasty PostgreSQL driver using an initialized connection.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Connects to a PostgreSQL database using a connection string.
    ///
    /// See [`postgres::Client::connect`] for more information.
    pub async fn connect(url: &str) -> Result<Self> {
        let cfg = Config::try_from(url)?;
        Self::connect_with_config(cfg).await
    }

    /// Connects to a PostgreSQL database using a [`postgres::Config`].
    ///
    /// See [`postgres::Client::configure`] for more information.
    pub async fn connect_with_config(cfg: Config) -> Result<Self> {
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

        Ok(Self::new(client))
    }

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

impl From<Client> for PostgreSQL {
    #[inline]
    fn from(client: Client) -> Self {
        Self::new(client)
    }
}

impl Driver for PostgreSQL {
    fn capability(&self) -> &Capability {
        &Capability::POSTGRESQL
    }

    fn register_schema<'s, 'sch, 'f>(
        &'s mut self,
        _schema: &'sch Schema,
    ) -> BoxedFuture<'f, Result<()>>
    where
        's: 'f,
        'sch: 'f,
    {
        Box::pin(async { Ok(()) })
    }

    fn exec<'s, 'sch, 'f>(
        &'s self,
        schema: &'sch Arc<Schema>,
        op: Operation,
    ) -> BoxedFuture<'f, Result<Response>>
    where
        's: 'f,
        'sch: 'f,
    {
        let (sql, ret_tys): (sql::Statement, _) = match op {
            Operation::Insert(op) => (op.stmt.into(), None),
            Operation::QuerySql(query) => (query.stmt.into(), query.ret),
            op => todo!("op={:#?}", op),
        };

        let width = sql.returning_len();

        let mut params = Vec::new();
        let sql_as_str = sql::Serializer::postgresql(schema).serialize(&sql, &mut params);

        let params = params.into_iter().map(Value::from).collect::<Vec<_>>();

        Box::pin(async move {
            let args = params.iter().map(|param| param as &(dyn ToSql + Sync));

            if width.is_none() {
                let count = self
                    .prepare_cached(sql_as_str, &[])
                    .await?
                    .bind(args)
                    .execute(&self.client)
                    .await?;

                return Ok(Response::count(count));
            }

            let types = params
                .iter()
                .map(|param| postgres_ty_for_value(&param.0))
                .collect::<Vec<_>>();

            let stmt = self.prepare_cached(sql_as_str, &types).await?;

            let mut stream = stmt.bind(args).query(&self.client).into_inner()?;

            if width.is_none() {
                let row = stream.try_next().await?.unwrap();
                let total = row.get::<i64>(0);
                let condition_matched = row.get::<i64>(1);

                if total == condition_matched {
                    Ok(Response::count(total as _))
                } else {
                    anyhow::bail!("update condition did not match");
                }
            } else {
                let ret_tys = ret_tys.as_ref().unwrap().clone();

                let mut iter = Vec::new();

                while let Some(row) = stream.try_next().await? {
                    let mut results = Vec::new();
                    for (i, column) in row.columns().iter().enumerate() {
                        results.push(postgres_to_toasty(i, &row, column, &ret_tys[i]));
                    }
                    iter.push(Ok(ValueRecord::from_vec(results)));
                }

                Ok(Response::value_stream(stmt::ValueStream::from_iter(
                    iter.into_iter(),
                )))
            }
        })
    }

    fn reset_db<'s, 'sch, 'f>(&'s self, schema: &'sch Schema) -> BoxedFuture<'f, Result<()>>
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

/// Converts a PostgreSQL value within a row to a [`toasty_core::stmt::Value`].
fn postgres_to_toasty(
    index: usize,
    row: &Row<'_>,
    column: &Column,
    expected_ty: &stmt::Type,
) -> stmt::Value {
    // NOTE: unfortunately, the inner representation of the PostgreSQL type enum is not
    // accessible, so we must manually match each type like so.
    if column.r#type() == &Type::TEXT || column.r#type() == &Type::VARCHAR {
        row.get::<Option<String>>(index)
            .map(|v| match expected_ty {
                stmt::Type::String => stmt::Value::String(v),
                _ => stmt::Value::String(v), // Default to string
            })
            .unwrap_or(stmt::Value::Null)
    } else if column.r#type() == &Type::BOOL {
        row.get::<Option<bool>>(index)
            .map(stmt::Value::Bool)
            .unwrap_or(stmt::Value::Null)
    } else if column.r#type() == &Type::INT2 {
        row.get::<Option<i16>>(index)
            .map(|v| match expected_ty {
                stmt::Type::I8 => stmt::Value::I8(v as i8),
                stmt::Type::I16 => stmt::Value::I16(v),
                stmt::Type::U8 => stmt::Value::U8(
                    u8::try_from(v).unwrap_or_else(|_| panic!("u8 value out of range: {v}")),
                ),
                stmt::Type::U16 => stmt::Value::U16(v as u16),
                _ => panic!("unexpected type for INT2: {expected_ty:#?}"),
            })
            .unwrap_or(stmt::Value::Null)
    } else if column.r#type() == &Type::INT4 {
        row.get::<Option<i32>>(index)
            .map(|v| match expected_ty {
                stmt::Type::I32 => stmt::Value::I32(v),
                stmt::Type::U16 => stmt::Value::U16(
                    u16::try_from(v).unwrap_or_else(|_| panic!("u16 value out of range: {v}")),
                ),
                stmt::Type::U32 => stmt::Value::U32(v as u32),
                _ => stmt::Value::I32(v), // Default fallback
            })
            .unwrap_or(stmt::Value::Null)
    } else if column.r#type() == &Type::INT8 {
        row.get::<Option<i64>>(index)
            .map(|v| match expected_ty {
                stmt::Type::I64 => stmt::Value::I64(v),
                stmt::Type::U32 => stmt::Value::U32(
                    u32::try_from(v).unwrap_or_else(|_| panic!("u32 value out of range: {v}")),
                ),
                stmt::Type::U64 => stmt::Value::U64(
                    u64::try_from(v).unwrap_or_else(|_| panic!("u64 value out of range: {v}")),
                ),
                _ => stmt::Value::I64(v), // Default fallback
            })
            .unwrap_or(stmt::Value::Null)
    } else {
        todo!(
            "implement PostgreSQL to toasty conversion for `{:#?}`",
            column.r#type()
        );
    }
}

fn postgres_ty_for_value(value: &stmt::Value) -> Type {
    match value {
        stmt::Value::Bool(_) => Type::BOOL,
        stmt::Value::I8(_) => Type::INT2,
        stmt::Value::I16(_) => Type::INT2,
        stmt::Value::I32(_) => Type::INT4,
        stmt::Value::I64(_) => Type::INT8,
        stmt::Value::U8(_) => Type::INT2,
        stmt::Value::U16(_) => Type::INT4,
        stmt::Value::U32(_) => Type::INT8,
        stmt::Value::U64(_) => Type::INT8,
        stmt::Value::Id(_) => Type::TEXT,
        stmt::Value::String(_) => Type::TEXT,
        stmt::Value::Null => Type::TEXT, // Default for NULL values
        _ => todo!("postgres_ty_for_value: {value:#?}"),
    }
}
