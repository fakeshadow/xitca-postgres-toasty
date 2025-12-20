use core::fmt;

use toasty_core::{Result, driver::Driver};
use tokio::sync::{Mutex, Semaphore};

use crate::{BoxedFuture, connection::Connection};

pub use xitca_postgres::Config;

pub struct PostgreSQL {
    cfg: Config,
    concurrency: usize,
    conn: Mutex<Option<Connection>>,
}

impl fmt::Debug for PostgreSQL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresSQL Driver")
    }
}

const DEFAULT_CONCURRENT_LEVEL: usize = 512;

impl PostgreSQL {
    /// create a new driver with given url string.
    ///
    /// # Examples
    /// ```rust
    /// let driver = xitca_postgres_toasty::PostgreSQL::new("postgres://postgres:postgres@localhost:5432")
    ///     .expect("panic if url is illformated");
    /// ```
    pub fn new(url: &str) -> Result<Self> {
        let cfg = Config::try_from(url)?;
        Ok(Self::from_config(cfg))
    }

    /// create a new driver with given [`Config`]
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
    pub fn concurrency(mut self, size: usize) -> Self {
        assert!(
            size != 0 && size < Semaphore::MAX_PERMITS,
            "concurrent level is beyond it's range bound"
        );
        self.concurrency = size;
        self
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
            let mut inner = self.conn.lock().await;

            if let Some(ref conn) = *inner
                && let Some(conn) = conn.try_clone()
            {
                return Ok(Box::new(conn) as _);
            }

            inner.take();
            let conn = Connection::connect(self.cfg.clone(), self.concurrency).await?;
            *inner = conn.try_clone();

            Ok(Box::new(conn) as _)
        })
    }

    fn max_connections(&self) -> Option<usize> {
        Some(self.concurrency)
    }
}
