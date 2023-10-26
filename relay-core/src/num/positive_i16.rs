use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Represents a positive i16.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct PositiveI16(i16);

impl PositiveI16 {
    #[must_use]
    pub const fn new(value: i16) -> Option<Self> {
        if value >= 0 {
            Some(Self(value))
        } else {
            None
        }
    }

    #[must_use]
    pub const fn get(&self) -> i16 {
        self.0
    }
}

impl TryFrom<i16> for PositiveI16 {
    type Error = ParseError;

    fn try_from(i: i16) -> std::result::Result<Self, Self::Error> {
        Self::new(i).ok_or(ParseError::NegativeOverflow { value: i })
    }
}

impl AsRef<i16> for PositiveI16 {
    fn as_ref(&self) -> &i16 {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PositiveI16 {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let i: i16 = Deserialize::deserialize(deserializer)?;
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
    NegativeOverflow { value: i16 },
}
