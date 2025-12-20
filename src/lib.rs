#![doc = include_str!("../README.md")]

mod async_iter;
mod connection;
mod driver;
mod r#type;
mod value;

type BoxedFuture<'a, T> = core::pin::Pin<Box<dyn core::future::Future<Output = T> + Send + 'a>>;

pub use crate::driver::{Config, PostgreSQL};
