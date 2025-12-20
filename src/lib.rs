#![doc = include_str!("../README.md")]

mod async_iter;
mod connection;
mod driver;
mod r#type;
mod value;

pub use crate::driver::{Config, PostgreSQL};
