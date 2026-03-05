use core::fmt;

use toasty_core::{
    Error, Result, async_trait,
    driver::{Capability, Driver},
    schema::db::{Migration, SchemaDiff},
};
use xitca_postgres::{
    Execute, Statement,
    pool::{Pool, PoolOwned},
    types::Type,
};

use crate::connection::Connection;

pub use xitca_postgres::Config;

/// async postgresql driver for toasty ORM
///
/// # Pros
/// - Multiplexing and Pipelining enabled for better concurrency and low latency over lossy network
///
/// # Examples
/// ```rust
/// // This is a desugared example for showcasing features of driver.
/// // In real world usage all the details are handled by toasty ORM automatically
/// #
/// # use toasty_core::{driver::Operation, schema::db::Schema};
/// use toasty::driver::{Connection, Driver};
///
/// # async fn multiplexing(schema: &Schema, op1: Operation, op2: Operation) -> toasty_core::Result<()> {
/// // construct a driver and obtain a connection manually.
/// let driver = xitca_postgres_toasty::PostgreSQL::new("postgres://postgres:postgres@localhost:5432")?;
/// let mut conn = driver.connect().await?;
///
/// // driver can multiplexing user facing connections arbitrarily.
/// // The real network connections are managed by driver separately and the query traffic is scheduled in M:N manner
/// for _ in 0..10000 {
///     conn = driver.connect().await?;
/// }
///
/// // user facing connection can pipeline queries together when possible and reduce latency
/// // future join can execute these queries concurrently if possible.
/// let (_, _) = futures::future::try_join(
///     driver.connect().await?.exec(schema, op1),
///     driver.connect().await?.exec(schema, op2)
/// ).await?;
///
/// # Ok(())
/// # }
/// ```
pub struct PostgreSQL {
    pool: PoolOwned,
}

impl fmt::Debug for PostgreSQL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresSQL Driver")
    }
}

impl PostgreSQL {
    /// create a new driver with given url string.
    ///
    /// # Examples
    /// ```rust
    /// let driver = xitca_postgres_toasty::PostgreSQL::new("postgres://postgres:postgres@localhost:5432")
    ///     .expect("panic if url is illformated");
    /// ```
    pub fn new(url: &str) -> Result<Self> {
        Self::builder(url).build()
    }

    /// create a new driver with given [`Config`]
    pub fn from_config(cfg: Config) -> Result<Self> {
        Self::builder(cfg).build()
    }

    /// create a builder type where more options can be configed before making the final driver
    pub fn builder<C>(cfg: C) -> PostgreSQLBuilder
    where
        Config: TryFrom<C>,
        <Config as TryFrom<C>>::Error: core::error::Error + Send + Sync + 'static,
    {
        PostgreSQLBuilder::new(cfg.try_into().map_err(Error::driver_operation_failed))
    }

    #[doc(hidden)]
    /// Expose `xitca-postgres` crate internal for testing purpose. this API does not offer any stability and can
    /// be changed without proper versioning
    pub fn pool(&self) -> &PoolOwned {
        &self.pool
    }
}

/// Builder type for [`PostgreSQL`] driver. offer additional configuration before finalizing.
pub struct PostgreSQLBuilder {
    cfg: Result<Config>,
    concurrency: usize,
}

impl PostgreSQLBuilder {
    pub const DEFAULT_CONCURRENT_LEVEL: usize = 4;

    fn new(cfg: Result<Config>) -> Self {
        Self {
            cfg,
            concurrency: Self::DEFAULT_CONCURRENT_LEVEL,
        }
    }

    /// Adjust how many concurrent network connections to database can be made.
    ///
    /// Driver is able to multiplex any amount of toasty connections regardless this concurency setting.
    ///
    /// Increase concurrency MAY improve performance. e.g: transaction and/or copy in/out.
    /// Increase concurrency WILL increase system resource usage. Mostly in the form of more network connections.
    ///
    /// It should be noted this setting is driver specific and has nothing to do with toasty's integrated connection pool.
    ///
    /// # Defaults
    ///
    /// Default value is [`Self::DEFAULT_CONCURRENT_LEVEL`]
    pub fn concurrency(mut self, size: usize) -> Self {
        assert!(size != 0, "concurrent level must not be zero");
        self.concurrency = size;
        self
    }

    /// finalize the building process and make the driver
    pub fn build(self) -> Result<PostgreSQL> {
        let cfg = self.cfg?;
        Ok(PostgreSQL {
            pool: Pool::builder(cfg)
                .capacity(self.concurrency)
                .build_owned()
                .expect("Config is already parsed"),
        })
    }
}

#[async_trait]
impl Driver for PostgreSQL {
    fn url(&self) -> std::borrow::Cow<'_, str> {
        unimplemented!()
    }

    fn capability(&self) -> &'static Capability {
        &Capability::POSTGRESQL
    }

    async fn connect(&self) -> Result<Box<dyn toasty_core::driver::Connection>> {
        Ok(Box::new(Connection::from_pool(self.pool.clone())))
    }

    fn generate_migration(&self, schema_diff: &SchemaDiff<'_>) -> Migration {
        use toasty_sql::{MigrationStatement, Serializer, TypedValue};

        let statements = MigrationStatement::from_diff(schema_diff, self.capability());

        let sql_strings = statements
            .iter()
            .map(|stmt| {
                let mut params = Vec::<TypedValue>::new();
                let sql =
                    Serializer::postgresql(stmt.schema()).serialize(stmt.statement(), &mut params);
                assert!(
                    params.is_empty(),
                    "migration statements should not have parameters"
                );
                sql
            })
            .collect::<Vec<_>>();

        Migration::new_sql(sql_strings.join("\n"))
    }

    async fn reset_db(&self) -> Result<()> {
        // We cannot drop a database we are currently connected to, so we need a temp database.
        const TEMP_NAME: &str = "__toasty_reset_temp";

        // Step 1: Create a temp DB
        format!("DROP DATABASE IF EXISTS \"{TEMP_NAME}\"")
            .execute(&self.pool)
            .await
            .map_err(Error::driver_operation_failed)?;
        format!("CREATE DATABASE \"{TEMP_NAME}\"")
            .execute(&self.pool)
            .await
            .map_err(Error::driver_operation_failed)?;

        // Step 2: Connect to the temp DB, drop and recreate the target
        {
            let dbname = self.pool.config().get_dbname().unwrap_or("postgres");

            let mut cfg = self.pool.config().clone();
            cfg.dbname(TEMP_NAME);

            let (conn, drv) = xitca_postgres::Postgres::new(cfg)
                .connect()
                .await
                .map_err(Error::driver_operation_failed)?;
            tokio::task::spawn(drv.into_future());

            Statement::named("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", &[Type::TEXT])
                 .bind([dbname])
                 .execute(&conn)
                 .await
                 .map_err(Error::driver_operation_failed)?;

            format!("DROP DATABASE IF EXISTS \"{dbname}\"")
                .execute(&conn)
                .await
                .map_err(Error::driver_operation_failed)?;
            format!("CREATE DATABASE \"{dbname}\"")
                .execute(&conn)
                .await
                .map_err(Error::driver_operation_failed)?;
        }

        // Step 3: Connect back to the target and clean up the temp DB
        format!("DROP DATABASE IF EXISTS \"{TEMP_NAME}\"")
            .execute(&self.pool)
            .await
            .map_err(Error::driver_operation_failed)?;

        Ok(())
    }
}
