use core::fmt;

use std::sync::Arc;

use toasty_core::{Error, Result, async_trait, driver::Driver};

use crate::connection::Connection;

pub use xitca_postgres::{Config, pool::Pool};

/// async postgresql driver for toasty ORM
///
/// # Pro
/// - Multiplexing and Pipelining enabled for better concurrency and low latency over lossy network
pub struct PostgreSQL {
    pool: Arc<Pool>,
}

impl fmt::Debug for PostgreSQL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresSQL Driver")
    }
}

const DEFAULT_CONCURRENT_LEVEL: usize = 4;

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
        Error: From<<Config as TryFrom<C>>::Error>,
    {
        PostgreSQLBuilder::new(cfg.try_into().map_err(Into::into))
    }
}

pub struct PostgreSQLBuilder {
    cfg: Result<Config>,
    concurrency: usize,
}

impl PostgreSQLBuilder {
    fn new(cfg: Result<Config>) -> Self {
        Self {
            cfg,
            concurrency: DEFAULT_CONCURRENT_LEVEL,
        }
    }

    /// Adjust how many concurrent connections can be made
    /// 
    /// It should be noted this setting is driver specific and has nothing to do with toasty's integrated connection pool.
    /// In other words this driver offers a second layer of pooling to bypass toasty's connection pool limitation. 
    /// 
    /// The goal is to achieve better concurrency, lantency and connection resource management
    ///
    /// # Defaults
    /// 
    /// Default value is 4
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
