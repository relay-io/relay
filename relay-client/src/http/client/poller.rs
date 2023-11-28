use crate::http::client::{Client, Result};
use anyhow::Context;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use relay_core::job::{EnqueueMode, Existing, New};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::{select, task};

#[async_trait]
/// Runner is a trait used by the `Poller` to execute a `Job` after fetching it to be processed.
pub trait Runner<P, S> {
    /// Runs the provided `Job` and returns a `Result` indicating if the `Job` was successfully.
    ///
    /// Relay currently does nothing with the returned `Result` but is intended to be used by the
    /// post processing middleware for extending the pollers functionality such as recording job
    /// history.
    async fn run(&self, helper: JobHelper<P, S>);
}

/// Is used to provide a `Existing` job and Relay client instance to the `Runner` for processing.
///
/// It contains helper functions allowing abstraction away complexity of interacting with Relay.
pub struct JobHelper<P, S> {
    client: Arc<Client>,
    job: Existing<P, S>,
}

impl<P, S> JobHelper<P, S>
where
    S: Serialize,
{
    /// Returns a reference to the job to be processed.
    pub const fn job(&self) -> &Existing<P, S> {
        &self.job
    }

    /// Returns a reference to the inner Relay client instance for interacting with Relay when the
    /// needing to do things the helper functions for the inner job don't apply to such as spawning
    /// one-off jobs not related to the existing running job in any way.
    ///
    /// It is rare to need the inner client and helper functions should be preferred in most cases.
    pub fn inner_client(&self) -> &Client {
        &self.client
    }

    /// Completes this in-flight `Existing` job.
    ///
    /// # Panics
    ///
    /// If the `Existing` job doesn't have a `run_id` set.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - The `Existing` job doesn't exist.
    pub async fn complete(&self) -> Result<()> {
        self.client
            .complete(&self.job.queue, &self.job.id, &self.job.run_id.unwrap())
            .await
    }

    /// Deletes the `Existing` job.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    pub async fn delete(&self) -> Result<()> {
        self.client.delete(&self.job.queue, &self.job.id).await
    }

    /// Returns if the `Existing` job still exists.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network error.
    pub async fn exists(&self) -> Result<bool> {
        self.client.exists(&self.job.queue, &self.job.id).await
    }

    /// Sends a heartbeat request to this in-flight `Existing` job indicating it is still processing, resetting
    /// the timeout. Optionally you can update the `Existing` jobs state during the same request.
    ///
    /// # Panics
    ///
    /// If the `Existing` job doesn't have a `run_id` set.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if the `Existing` job doesn't exist.
    pub async fn heartbeat(&self, state: Option<S>) -> Result<()> {
        self.client
            .heartbeat(
                &self.job.queue,
                &self.job.id,
                &self.job.run_id.unwrap(),
                state,
            )
            .await
    }

    /// Re-queues this in-flight `Existing` job to be run again or spawn a new set of jobs
    /// atomically.
    ///
    /// The `Existing` jobs queue, id and `run_id` must match an existing in-flight Job.
    /// This is primarily used to schedule a new/the next run of a singleton job. This provides the
    /// ability for self-perpetuating scheduled jobs in an atomic manner.
    ///
    /// Reschedule also allows you to change the jobs `queue` and `id` during the reschedule.
    /// This is allowed to facilitate advancing a job through a distributed pipeline/state
    /// machine atomically if that is more appropriate than advancing using the jobs state alone.
    ///
    /// The mode will be used to determine the behaviour if a conflicting record already exists,
    /// just like when enqueuing jobs.
    ///
    /// If the `Existing` job no longer exists or is not in-flight, this will return without error and will
    /// not enqueue any jobs.
    ///
    /// # Panics
    ///
    /// If the `Existing` job doesn't have a `run_id` set.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if one of the `Existing` jobs exists when mode is unique.
    pub async fn requeue(&self, mode: EnqueueMode, jobs: &[New<P, S>]) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        self.client
            .requeue(
                mode,
                &self.job.queue,
                &self.job.id,
                &self.job.run_id.unwrap(),
                jobs,
            )
            .await
    }
}

/// Is used to configure and build Poller for use.
pub struct Builder<P, S, R> {
    client: Arc<Client>,
    queue: String,
    runner: Arc<R>,
    num_workers: usize,
    _payload: PhantomData<P>,
    _state: PhantomData<S>,
}

impl<P, S, R> Builder<P, S, R>
where
    P: DeserializeOwned + Send + Sync + 'static,
    S: DeserializeOwned + Send + Sync + 'static,
    R: Runner<P, S> + Send + Sync + 'static,
{
    #[inline]
    pub fn new(client: Arc<Client>, queue: &str, runner: R) -> Self {
        Self {
            client,
            num_workers: 10,
            runner: Arc::new(runner),
            queue: queue.to_string(),
            _payload: PhantomData,
            _state: PhantomData,
        }
    }

    /// Sets the maximum number of backend async workers indicating the maximum number of in-flight
    /// `Job`s.
    pub const fn max_workers(mut self, max_workers: usize) -> Self {
        self.num_workers = max_workers;
        self
    }

    // TODO: Add immutable middleware like interceptor functions allowing pre or post processing of
    //       jobs. eg. recording job start and end times, logging, metrics, history.

    /// Creates a new `Poller` using the Builders configuration.
    #[inline]
    pub fn build(self) -> std::result::Result<Poller<P, S, R>, anyhow::Error> {
        if self.num_workers == 0 {
            return Err(anyhow::anyhow!("max_workers must be greater than 0"));
        }
        Ok(Poller {
            client: self.client,
            num_workers: self.num_workers,
            runner: self.runner,
            queue: self.queue,
            _payload: PhantomData,
            _state: PhantomData,
        })
    }
}

/// Poller is used to abstract away polling and running multiple `Job`s calling the provided `Fn`.
pub struct Poller<P, S, R> {
    client: Arc<Client>,
    num_workers: usize,
    runner: Arc<R>,
    queue: String,
    _payload: PhantomData<P>,
    _state: PhantomData<S>,
}

impl<P, S, R> Poller<P, S, R>
where
    P: DeserializeOwned + Send + Sync + 'static,
    S: DeserializeOwned + Send + Sync + 'static,
    R: Runner<P, S> + Send + Sync + 'static,
{
    /// Starts the Relay HTTP Poller/Consumer.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network or shutdown issue.
    pub async fn start(
        &self,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) -> std::result::Result<(), anyhow::Error> {
        let (tx, rx) = async_channel::bounded(self.num_workers);
        let (tx_sem, rx_sem) = async_channel::bounded(self.num_workers);

        let handles = (0..self.num_workers)
            .map(|_| {
                let rx = rx.clone();
                let rx_rem = rx_sem.clone();
                task::spawn(self.worker(rx_rem, rx))
            })
            .collect::<Vec<_>>();

        task::spawn(self.poller(cancel, tx_sem, tx))
            .await
            .context("spawned task failure")??;

        for handle in handles {
            handle.await?;
        }
        Ok(())
    }

    fn poller(
        &self,
        cancel: impl Future<Output = ()> + Send + 'static,
        tx_sem: Sender<()>,
        tx: Sender<Existing<P, S>>,
    ) -> impl Future<Output = std::result::Result<(), anyhow::Error>> + Send + 'static {
        let client = Arc::clone(&self.client);
        let queue = self.queue.clone();

        async move {
            tokio::pin!(cancel);

            let mut num_jobs = 0;

            'outer: loop {
                if num_jobs == 0 {
                    tx_sem.send(()).await?;
                    num_jobs += 1;
                }
                while tx_sem.try_send(()).is_ok() {
                    num_jobs += 1;
                }

                let jobs = select! {
                  () = &mut cancel => {
                        break 'outer;
                    },
                    res = client.poll::<P, S>(&queue, num_jobs) => res?
                };
                let l = jobs.len();
                for job in jobs {
                    tx.send(job).await?;
                }
                num_jobs -= l;
            }
            Ok(())
        }
    }

    fn worker(&self, rx_sem: Receiver<()>, rx: Receiver<Existing<P, S>>) -> impl Future<Output = ()>
    where
        P: Send + Sync + 'static,
        S: Send + Sync + 'static,
    {
        let client = Arc::clone(&self.client);
        let runner = Arc::clone(&self.runner);
        async move {
            while let Ok(job) = rx.recv().await {
                runner
                    .run(JobHelper {
                        client: client.clone(),
                        job,
                    })
                    .await;
                rx_sem
                    .recv()
                    .await
                    .expect("semaphore shutdown in correct order");
            }
        }
    }
}
