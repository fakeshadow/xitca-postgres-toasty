mod async_iter;
mod connection;
mod driver;
mod r#type;
mod value;

type BoxedFuture<'a, T> = core::pin::Pin<Box<dyn core::future::Future<Output = T> + Send + 'a>>;

pub use crate::driver::{Config, PostgreSQL};

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connect() {
        let drv = PostgreSQL::new("postgres://postgres:postgres@localhost:5432")
            .unwrap()
            .concurrency(123);

        let db = toasty::Db::builder()
            .register::<User>()
            .build(drv)
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
