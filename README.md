# async postgresql driver for [toasty ORM](https://github.com/tokio-rs/toasty)

## Quick Start
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // define model with toasty
    #[derive(Debug, toasty::Model)]
    struct User {
        #[key]
        id: i32,
        name: String,
    }

    // construct a xitca-postgres driver
    let drv = xitca_postgres_toasty::PostgreSQL::new("postgres://postgres:postgres@localhost:5432")?;

    // start the orm manager with our postgres driver
    let mut orm = toasty::Db::builder()
        .register::<User>()
        .build(drv)
        .await?;

    // interact with database

    orm.push_schema().await?;

    User::create()
        .id(123)
        .name(String::from("john"))
        .exec(&mut orm)
        .await?;

    let user = User::get_by_id(&mut orm, 123).await?;

    assert_eq!(user.id, 123);
    assert_eq!(user.name, "john");

    Ok(())
}
