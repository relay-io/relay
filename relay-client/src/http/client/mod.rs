// #![allow(clippy::module_name_repetitions)]
// #![allow(clippy::module_inception)]
// mod client;
// mod consumer;
mod errors;
mod low_level_client;

pub use errors::{Error, Result};
pub use low_level_client::{Builder, Client};
// pub use consumer::{Builder as ConsumerBuilder, Consumer};
