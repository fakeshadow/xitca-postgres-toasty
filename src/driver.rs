use core::fmt;

use std::sync::Arc;

use toasty_core::{Error, Result, async_trait, driver::Driver};

use crate::connection::Connection;

pub use xitca_postgres::{Config, pool::Pool};

/// async postgresql driver for toasty ORM
///
/// # Pros
/// - Multiplexing and Pipelining enabled for better concurrency and low latency over lossy network
///
/// # Examples
/// ```rust
/// // This is a desugared example for showcasing features of driver.
/// // In real world usage all the details are handled by toasty ORM automatically
/// # use std::sync::Arc;
/// #
/// # use toasty_core::{driver::Operation, schema::db::Schema};
/// use toasty::driver::{Connection, Driver};
///
/// # async fn multiplexing(schema: &Arc<Schema>, op1: Operation, op2: Operation) -> toasty_core::Result<()> {
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
    pool: Arc<Pool>,
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
        PostgreSQLBuilder::new(cfg.try_into().map_err(Error::driver))
    }

    #[doc(hidden)]
    /// Expose `xitca-postgres` crate internal for testing purpose. this API does not offer any stability and can
    /// be changed without proper versioning
    pub fn pool(&self) -> &Pool {
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
            pool: Arc::new(
                Pool::builder(cfg)
                    .capacity(self.concurrency)
                    .build()
                    .expect("Config is already parsed"),
            ),
        })
    }
}

#[async_trait]
impl Driver for PostgreSQL {
    async fn connect(&self) -> Result<Box<dyn toasty_core::driver::Connection>> {
        Ok(Box::new(Connection::from_pool(self.pool.clone())))
    }
}
