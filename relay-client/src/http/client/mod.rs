// #![allow(clippy::module_name_repetitions)]
// #![allow(clippy::module_inception)]
// mod client;
// mod consumer;
mod client;
mod errors;

pub use errors::{Error, Result};
// pub use client::{Builder, Client};
// pub use consumer::{Builder as ConsumerBuilder, Consumer};
