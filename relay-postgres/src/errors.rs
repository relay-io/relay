use thiserror::Error;

/// The Job Result.
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
        run_id: String,
    },

    #[error("Backend error: {message}. Is retryable: {is_retryable}.")]
    Backend { message: String, is_retryable: bool },
}

impl Error {
    #[inline]
    #[must_use]
    pub fn queue(&self) -> String {
        match self {
            Error::JobExists { queue, .. } | Error::JobNotFound { queue, .. } => queue.clone(),
            Error::Backend { .. } => "unknown".to_string(),
        }
    }

    #[inline]
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Error::JobExists { .. } => "job_exists".to_string(),
            Error::JobNotFound { .. } => "job_not_found".to_string(),
            Error::Backend { .. } => "backend".to_string(),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::JobExists { .. } | Error::JobNotFound { .. } => false,
            Error::Backend { is_retryable, .. } => *is_retryable,
        }
    }
}
