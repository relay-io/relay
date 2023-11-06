use super::errors::Result;
use crate::http::client::Error;
use backoff_rs::{Exponential, ExponentialBackoffBuilder};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use relay_core::job::{EnqueueMode, Existing, New};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

/// A Builder can be used to create a custom `Client`.
pub struct Builder {
    url: String,
    poll_backoff: Exponential,
    retry_backoff: Exponential,
    max_retries: Option<usize>,
    client: reqwest::Client,
}

impl Builder {
    /// Initializes a new Builder with sane defaults to create a custom `Client`
    ///
    /// # Panics
    ///
    /// If internal configuration of the `reqwest::Client` is invalid.
    #[must_use]
    pub fn new(url: &str) -> Self {
        let next_backoff = ExponentialBackoffBuilder::default()
            .interval(Duration::from_millis(200))
            .jitter(Duration::from_millis(25))
            .max(Duration::from_secs(1))
            .build();

        let retry_backoff = ExponentialBackoffBuilder::default()
            .interval(Duration::from_millis(100))
            .jitter(Duration::from_millis(25))
            .max(Duration::from_secs(1))
            .build();

        let client = reqwest::ClientBuilder::default()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_timeout(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .pool_max_idle_per_host(512)
            .build()
            .expect("valid default HTTP Client configuration");

        Self {
            url: url.to_string(),
            poll_backoff: next_backoff,
            retry_backoff,
            max_retries: None, // no max retries
            client,
        }
    }

    /// Set a custom backoff used when polling for `Job`s.
    #[must_use]
    pub fn poll_backoff(mut self, backoff: Exponential) -> Self {
        self.poll_backoff = backoff;
        self
    }

    /// Set a custom backoff used when retrying temporary errors interacting with the Relay server.
    #[must_use]
    pub fn retry_backoff(mut self, backoff: Exponential) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Set the maximum number of retries to perform before considering it a permanent error.
    ///
    /// # Default
    ///
    /// None - retry forever.
    #[must_use]
    pub fn max_retries(mut self, max_retries: Option<usize>) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets a custom HTTP [Client](https://docs.rs/reqwest/latest/reqwest/struct.Client.html#) for use.
    #[must_use]
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Creates a `Client` which uses the Builders configuration.
    #[must_use]
    pub fn build(self) -> Client {
        Client {
            url: self.url,
            client: self.client,
            poll_backoff: self.poll_backoff,
            retry_backoff: self.retry_backoff,
            max_retries: self.max_retries,
        }
    }
}

/// Relay HTTP Client.
pub struct Client {
    url: String,
    client: reqwest::Client,
    poll_backoff: Exponential,
    retry_backoff: Exponential,
    max_retries: Option<usize>,
}

impl Client {
    /// Enqueues a batch of one or more `Job`s for processing using the provided `EnqueueMode`.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network error.
    pub async fn enqueue<P, S>(&self, mode: EnqueueMode, jobs: &[New<P, S>]) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!("{}/v2/queues/jobs?mode={}", self.url, mode);

        self.with_retry(|| async {
            let res = self.client.post(&url).json(jobs).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::ACCEPTED {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Deletes a `Job`.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    pub async fn delete(&self, queue: &str, job_id: &str) -> Result<()> {
        let url = format!(
            "{}/v2/queues/{}/jobs/{}",
            self.url,
            url_encode(queue),
            url_encode(job_id)
        );

        self.with_retry(|| async {
            let res = self.client.delete(&url).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::OK {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Completes an in-flight a `Job`.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - The `Job` doesn't exist.
    pub async fn complete(&self, queue: &str, job_id: &str, run_id: &Uuid) -> Result<()> {
        let url = format!(
            "{}/v2/queues/{}/jobs/{}/run-id/{}",
            self.url,
            url_encode(queue),
            url_encode(job_id),
            run_id
        );

        self.with_retry(|| async {
            let res = self.client.delete(&url).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::OK {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Returns if a `Job` exists.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network error.
    pub async fn exists(&self, queue: &str, job_id: &str) -> Result<bool> {
        let url = format!(
            "{}/v2/queues/{}/jobs/{}",
            self.url,
            url_encode(queue),
            url_encode(job_id)
        );

        self.with_retry(|| async {
            let res = self.client.head(&url).send().await?;

            match res.status() {
                StatusCode::OK => Ok(true),
                StatusCode::NOT_FOUND => Ok(false),
                sc => {
                    res.error_for_status()?;
                    Err(Error::Request {
                        status_code: Some(sc),
                        is_retryable: false,
                        is_poll: false,
                        message: "unexpected HTTP response code".to_string(),
                    })
                }
            }
        })
        .await
    }

    /// Attempts to return the a `Job` in order to report or display it's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if the `Job` doesn't exist.
    pub async fn get<P, S>(&self, queue: &str, job_id: &str) -> Result<Existing<P, S>>
    where
        P: DeserializeOwned,
        S: DeserializeOwned,
    {
        let url = format!(
            "{}/v2/queues/{}/jobs/{}",
            self.url,
            url_encode(queue),
            url_encode(job_id)
        );

        self.with_retry(|| async {
            let res = self.client.get(&url).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::OK {
                Ok(res.json().await?)
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Attempts to retrieve the next `Job` for processing.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - no `Job`s currently exists.
    pub async fn next<P, S>(&self, queue: &str, num_jobs: usize) -> Result<Vec<Existing<P, S>>>
    where
        P: DeserializeOwned,
        S: DeserializeOwned,
    {
        let queue = url_encode(queue);
        let url = format!("{}/v2/queues/{queue}/jobs?num_jobs={num_jobs}", self.url);

        self.with_retry(|| async {
            let res = self.client.get(&url).send().await?;

            match res.status() {
                StatusCode::OK => Ok(res.json().await?),
                StatusCode::NO_CONTENT => Err(Error::Request {
                    status_code: Some(StatusCode::NO_CONTENT),
                    is_retryable: true,
                    is_poll: true,
                    message: "no jobs found for processing".to_string(),
                }),
                sc => {
                    res.error_for_status()?;
                    Err(Error::Request {
                        status_code: Some(sc),
                        is_retryable: false,
                        is_poll: false,
                        message: "unexpected HTTP response code".to_string(),
                    })
                }
            }
        })
        .await
    }

    /// Sends a heartbeat request to an in-flight `Job` indicating it is still processing, resetting
    /// the timeout. Optionally you can update the `Job` state during the same request.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if the `Job` doesn't exist.
    pub async fn heartbeat<S>(
        &self,
        queue: &str,
        job_id: &str,
        run_id: &Uuid,
        state: Option<S>,
    ) -> Result<()>
    where
        S: Serialize,
    {
        let url = format!(
            "{}/v2/queues/{}/jobs/{}/run-id/{}",
            self.url,
            url_encode(queue),
            url_encode(job_id),
            run_id
        );

        self.with_retry(|| async {
            let mut request = self.client.patch(&url);

            if let Some(state) = &state {
                request = request.json(state);
            }
            let res = request.send().await?;
            let status_code = res.status();

            if status_code == StatusCode::ACCEPTED {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Re-queues the an existing in-flight Job to be run again or spawn a new set of jobs
    /// atomically.
    ///
    /// The Jobs queue, id and `run_id` must match an existing in-flight Job. This is primarily used
    /// to schedule a new/the next run of a singleton `Job`. This provides the ability for
    /// self-perpetuating scheduled jobs in an atomic manner.
    ///
    /// Reschedule also allows you to change the `Job`'s `queue` and `id` during the reschedule.
    /// This is allowed to facilitate advancing a `Job` through a distributed pipeline/state
    /// machine atomically if that is more appropriate than advancing using the `Job`'s state alone.
    ///
    /// The mode will be used to determine the behaviour if a conflicting record already exists,
    /// just like when enqueuing jobs.
    ///
    /// If the `Job` no longer exists or is not in-flight, this will return without error and will
    /// not enqueue any jobs.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if one of the `Job`'s exists when mode is unique.
    pub async fn requeue<P, S>(
        &self,
        mode: EnqueueMode,
        queue: &str,
        job_id: &str,
        run_id: &Uuid,
        jobs: &[New<P, S>],
    ) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!(
            "{}/v2/queues/{}/jobs/{}/run-id/{}?mode={}",
            self.url,
            url_encode(queue),
            url_encode(job_id),
            run_id,
            mode
        );
        self.with_retry(|| async {
            let res = self.client.put(&url).json(jobs).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::ACCEPTED {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Polls the Relay server until a `Job` becomes available.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network error.
    pub async fn poll<P, S>(&self, queue: &str, num_jobs: usize) -> Result<Vec<Existing<P, S>>>
    where
        P: DeserializeOwned,
        S: DeserializeOwned,
    {
        self.with_retry(|| self.next(queue, num_jobs)).await
    }

    async fn with_retry<F, Fut, R>(&self, mut f: F) -> Result<R>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        let mut attempt = 0;
        loop {
            let result = f().await;
            return match result {
                Err(e) => {
                    if e.is_retryable() {
                        if let Some(max_retries) = self.max_retries {
                            if attempt >= max_retries {
                                return Err(e);
                            }
                        }
                        if e.is_poll_retryable() {
                            sleep(self.poll_backoff.duration(attempt)).await;
                        } else {
                            sleep(self.retry_backoff.duration(attempt)).await;
                        }
                        attempt += 1;
                        continue;
                    }
                    Err(e)
                }
                _ => result,
            };
        }
    }
}

#[inline]
fn url_encode(input: &str) -> Cow<str> {
    utf8_percent_encode(input, NON_ALPHANUMERIC).into()
}
