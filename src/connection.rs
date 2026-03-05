use core::fmt;

use toasty_core::{
    Error, async_trait,
    driver::{
        Capability, Connection as ConnectionTrait, Operation, Response, operation::Transaction,
    },
    schema::db::{AppliedMigration, Migration, Schema, Table},
    stmt,
};
use toasty_sql::{TypedValue, serializer::Placeholder};
use xitca_postgres::{
    Execute,
    iter::AsyncLendingIterator,
    pool::{PoolConnectionOwned, PoolOwned},
    statement::{Statement, StatementNamed},
    types::Type,
};

use crate::{r#type::TypeExt, value::Value};

pub struct Connection {
    params: Params,
    pool: PoolOwned,
    tx_state: TransactionState,
}

// state tracking transaction state of current connection.
// when a transaction starts we keep the connection and use it for execution instead of connection pool.
enum TransactionState {
    Uninit,
    Started(PoolConnectionOwned),
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("PostgresConnection")
    }
}

impl Connection {
    pub(crate) fn from_pool(pool: PoolOwned) -> Self {
        Self {
            params: Params::default(),
            pool,
            tx_state: TransactionState::Uninit,
        }
    }
}

impl Connection {
    // Creates a table.
    async fn create_table(
        &mut self,
        schema: &Schema,
        table: &Table,
    ) -> Result<(), xitca_postgres::Error> {
        let serializer = toasty_sql::Serializer::postgresql(schema);

        let mut params = Vec::<TypedValue>::new();
        let sql = serializer.serialize(
            &toasty_sql::Statement::create_table(table, &Capability::POSTGRESQL),
            &mut params,
        );

        assert!(
            params.is_empty(),
            "creating a table shouldn't involve any parameters"
        );

        sql.execute(&self.pool).await?;

        // NOTE: `params` is guaranteed to be empty based on the assertion above. If
        // that changes, `params.clear()` should be called here.
        for idx in table.indices.iter().filter(|idx| !idx.primary_key) {
            let sql = serializer.serialize(&toasty_sql::Statement::create_index(idx), &mut params);

            assert!(
                params.is_empty(),
                "creating an index shouldn't involve any parameters"
            );

            sql.execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn exec_tx(
        &mut self,
        schema: &Schema,
        tx: Transaction,
    ) -> Result<Response, xitca_postgres::Error> {
        if matches!(tx, Transaction::Start { .. }) {
            let conn = self.pool.get().await?;
            self.tx_state = TransactionState::Started(conn);
        }

        let TransactionState::Started(ref conn) = self.tx_state else {
            unreachable!()
        };

        toasty_sql::Serializer::postgresql(schema)
            .serialize_transaction(&tx)
            .execute(conn)
            .await?;

        if matches!(tx, Transaction::Commit | Transaction::Rollback) {
            self.tx_state = TransactionState::Uninit;
        }

        Ok(Response::count(0))
    }

    async fn _exec(
        &mut self,
        schema: &Schema,
        op: Operation,
    ) -> Result<Response, xitca_postgres::Error> {
        let (sql, ret_tys) = match op {
            Operation::QuerySql(query) => {
                assert!(
                    query.last_insert_id_hack.is_none(),
                    "last_insert_id_hack is MySQL-specific and should not be set for PostgreSQL"
                );
                (query.stmt.into(), query.ret.unwrap_or_default())
            }
            Operation::Insert(op) => (toasty_sql::Statement::from(op.stmt), Vec::new()),
            Operation::Transaction(tx) => return self.exec_tx(schema, tx).await,
            op => todo!("op={:#?}", op),
        };

        let width = sql.returning_len();

        self.params.clear();
        let stmt = toasty_sql::Serializer::postgresql(schema).serialize(&sql, &mut self.params);
        let stmt = Statement::named(&stmt, &self.params.ty).bind(self.params.val.iter());

        if width.is_none() {
            match self.tx_state {
                TransactionState::Started(ref mut conn) => stmt.execute(conn).await?.await,
                TransactionState::Uninit => stmt.execute(&self.pool).await,
            }
            .map(Response::count)
        } else {
            match self.tx_state {
                TransactionState::Started(ref mut conn) => stmt.query(conn).await,
                TransactionState::Uninit => stmt.query(&self.pool).await,
            }
            .map(|stream| Response::value_stream(crate::async_iter::stream(stream, ret_tys)))
        }
    }

    async fn _applied_migrations(
        &mut self,
    ) -> Result<Vec<AppliedMigration>, xitca_postgres::Error> {
        // Ensure the migrations table exists
        CREATE_MIGRATION_TABLE.execute(&self.pool).await?;

        // Query all applied migrations
        let mut rows = SELECT_MIGRATION.bind_none().query(&self.pool).await?;

        let mut migrations = Vec::new();

        while let Some(row) = rows.try_next().await? {
            let id = row.get::<i64>(0);
            migrations.push(AppliedMigration::new(id as u64))
        }

        Ok(migrations)
    }

    async fn _apply_migration(
        &mut self,
        id: u64,
        name: String,
        migration: &Migration,
    ) -> Result<(), xitca_postgres::Error> {
        // Ensure the migrations table exists
        CREATE_MIGRATION_TABLE.execute(&self.pool).await?;

        let tx = self.pool.get().await?.transaction_owned().await?;

        for stmt in migration.statements() {
            if let Err(e) = stmt.execute(&tx).await {
                tx.rollback().await?;
                return Err(e);
            }
        }

        if let Err(e) = RECORD_MIGRATION
            .bind_dyn(&[&(id as i64), &name])
            .execute(&tx)
            .await
        {
            tx.rollback().await?;
            return Err(e);
        }

        tx.commit().await
    }
}

const CREATE_MIGRATION_TABLE: &str = "CREATE TABLE IF NOT EXISTS __toasty_migrations (id BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMP NOT NULL)";

const SELECT_MIGRATION: StatementNamed<'_> = Statement::named(
    "SELECT id FROM __toasty_migrations ORDER BY applied_at",
    &[],
);

const RECORD_MIGRATION: StatementNamed<'_> = Statement::named(
    "INSERT INTO __toasty_migrations (id, name, applied_at) VALUES ($1, $2, NOW())",
    &[Type::INT8, Type::TEXT],
);

#[async_trait]
impl ConnectionTrait for Connection {
    async fn exec(&mut self, schema: &Schema, op: Operation) -> Result<Response, Error> {
        self._exec(schema, op)
            .await
            .map_err(Error::driver_operation_failed)
    }

    async fn push_schema(&mut self, schema: &Schema) -> Result<(), Error> {
        for table in &schema.tables {
            self.create_table(schema, table)
                .await
                .map_err(Error::driver_operation_failed)?;
        }
        Ok(())
    }

    async fn applied_migrations(&mut self) -> Result<Vec<AppliedMigration>, Error> {
        self._applied_migrations()
            .await
            .map_err(Error::driver_operation_failed)
    }

    async fn apply_migration(
        &mut self,
        id: u64,
        name: String,
        migration: &Migration,
    ) -> Result<(), Error> {
        self._apply_migration(id, name, migration)
            .await
            .map_err(Error::driver_operation_failed)
    }
}

#[derive(Default, Debug)]
struct Params {
    ty: Vec<Type>,
    val: Vec<Value>,
}

impl Params {
    fn clear(&mut self) {
        self.ty.clear();
        self.val.clear();
    }
}

impl toasty_sql::Params for Params {
    fn push(&mut self, param: &stmt::Value, hint: Option<&stmt::Type>) -> Placeholder {
        self.ty.push((param, hint).to_postgres_type());
        self.val.push(Value::from(param.clone()));
        Placeholder(self.val.len())
    }
}
