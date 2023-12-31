use thiserror::Error;
use uuid::Uuid;

/// The result of a Postgres operation.
pub type Result<T> = std::result::Result<T, Error>;

/// Job error types.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    /// indicates a Job with the existing ID and Queue already exists.
    #[error("job with id `{job_id}` already exists in queue `{queue}`.")]
    JobExists { job_id: String, queue: String },

    /// indicates a Job with the existing ID and Queue could not be found.
    #[error("job with id `{job_id}` not found in queue `{queue}` with {run_id}.")]
    JobNotFound {
        job_id: String,
        queue: String,
        run_id: Uuid,
    },

    #[error("Backend error: {message}. Is retryable: {is_retryable}.")]
    Backend { message: String, is_retryable: bool },
}

impl Error {
    /// Returns the queue name for the error if available.
    #[inline]
    #[must_use]
    pub fn queue(&self) -> String {
        match self {
            Error::JobExists { queue, .. } | Error::JobNotFound { queue, .. } => queue.clone(),
            Error::Backend { .. } => "unknown".to_string(),
        }
    }

    /// Returns the error type for the error as a string.
    #[inline]
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Error::JobExists { .. } => "job_exists".to_string(),
            Error::JobNotFound { .. } => "job_not_found".to_string(),
            Error::Backend { .. } => "backend".to_string(),
        }
    }

    /// Returns if this error is known to be retryable or not.
    #[inline]
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Error::JobExists { .. } | Error::JobNotFound { .. } => false,
            Error::Backend { is_retryable, .. } => *is_retryable,
        }
    }
}
