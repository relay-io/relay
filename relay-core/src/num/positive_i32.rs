use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Represents a positive i32.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct PositiveI32(i32);

impl PositiveI32 {
    #[must_use]
    pub const fn new(value: i32) -> Option<Self> {
        if value >= 0 {
            Some(Self(value))
        } else {
            None
        }
    }

    #[must_use]
    pub const fn get(&self) -> i32 {
        self.0
    }
}

impl TryFrom<i32> for PositiveI32 {
    type Error = ParseError;

    fn try_from(i: i32) -> std::result::Result<Self, Self::Error> {
        Self::new(i).ok_or(ParseError::NegativeOverflow { value: i })
    }
}

impl AsRef<i32> for PositiveI32 {
    fn as_ref(&self) -> &i32 {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PositiveI32 {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let i: i32 = Deserialize::deserialize(deserializer)?;
        i.try_into().map_err(serde::de::Error::custom)
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Indicates that the inter is not a valid i32.
    #[error(transparent)]
    ParseInt32Error {
        #[from]
        source: std::num::ParseIntError,
    },

    /// Indicates that the integer is too small to store in the target integer type.
    #[error("Integer is too small to store in target integer type")]
    NegativeOverflow { value: i32 },
}
