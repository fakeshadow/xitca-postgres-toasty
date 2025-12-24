use toasty_core::{async_trait, driver::Driver};
use toasty_driver_integration_suite::Setup;
use xitca_postgres::Execute;
use xitca_postgres_toasty::PostgreSQL;

struct DriverSetup;

impl DriverSetup {
    fn new() -> PostgreSQL {
        PostgreSQL::new("postgres://postgres:postgres@localhost:5432").unwrap()
    }
}

#[async_trait]
impl Setup for DriverSetup {
    fn driver(&self) -> Box<dyn Driver> {
        Box::new(Self::new())
    }

    async fn delete_table(&self, name: &str) {
        let drv = Self::new();
        let conn = drv.pool().get().await.unwrap();
        format!("DROP TABLE IF EXISTS \"{}\" CASCADE", name)
            .execute(&conn)
            .await
            .unwrap();
    }
}

toasty_driver_integration_suite::generate_driver_tests!(DriverSetup);
