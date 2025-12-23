use core::fmt;

use std::sync::Arc;

use toasty_core::{
    Result, async_trait,
    driver::{Capability, Connection as ConnectionTrait, Operation, Response},
    schema::db::{Schema, Table},
    stmt,
};
use toasty_sql::{self as sql, serializer::Placeholder};
use xitca_postgres::{Execute, Statement, pool::Pool, types::Type};

use crate::{r#type::TypeExt, value::Value};

pub struct Connection {
    pool: Arc<Pool>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("PostgresConnection")
    }
}

impl Connection {
    pub(crate) fn from_pool(pool: Arc<Pool>) -> Self {
        Self { pool }
    }
}

impl Connection {
    // Creates a table.
    async fn create_table(
        &mut self,
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

        let conn = self.pool.get().await?;

        sql.execute(&conn).await?;

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

            sql.execute(&conn).await?;
        }

        Ok(())
    }

    // Drops a table.
    async fn drop_table(
        &mut self,
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

        let conn = self.pool.get().await?;

        sql.execute(&conn).await?;
        Ok(())
    }
}

#[async_trait]
impl ConnectionTrait for Connection {
    fn capability(&self) -> &'static Capability {
        &Capability::POSTGRESQL
    }

    async fn exec(&mut self, schema: &Arc<Schema>, op: Operation) -> Result<Response> {
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
                todo!("op={:#?}", Operation::Transaction(tx))
            }
            op => todo!("op={:#?}", op),
        };

        let width = sql.returning_len();

        let mut params = Params::default();
        let stmt = sql::Serializer::postgresql(schema).serialize(&sql, &mut params);

        let Params { ty, val } = params;

        let mut conn = self.pool.get().await?;

        let stmt = Statement::named(&stmt, &ty).execute(&mut conn).await?;

        let stmt = stmt.bind(val.iter());

        if width.is_none() {
            let fut = stmt.execute(&conn);

            drop(conn);

            let res = fut.await?;
            Ok(Response::count(res))
        } else {
            let fut = stmt.into_owned().query(&conn);

            drop(conn);

            let stream = fut.await?;
            Ok(Response::value_stream(crate::async_iter::stream(
                stream, ret_tys,
            )))
        }
    }

    async fn reset_db(&mut self, schema: &Schema) -> Result<()> {
        for table in &schema.tables {
            self.drop_table(schema, table, true).await?;
            self.create_table(schema, table).await?;
        }
        Ok(())
    }
}

#[derive(Default, Debug)]
struct Params {
    ty: Vec<Type>,
    val: Vec<Value>,
}

impl toasty_sql::Params for Params {
    fn push(&mut self, param: &stmt::Value) -> Placeholder {
        self.ty.push(param.infer_ty().to_postgres_type());
        self.val.push(Value::from(param.clone()));
        Placeholder(self.val.len())
    }
}
