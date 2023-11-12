use crate::http::client::{Client, Result};
use anyhow::Context;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use relay_core::job::Existing;
use serde::de::DeserializeOwned;
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

pub struct JobHelper<P, S> {
    client: Arc<Client>,
    job: Existing<P, S>,
}

impl<P, S> JobHelper<P, S> {
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

    /// Completes an in-flight a `Job`.
    ///
    /// # Panics
    ///
    /// IF the `Job` doesn't have a `run_id` set.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - The `Job` doesn't exist.
    pub async fn complete(&self) -> Result<()> {
        self.client
            .complete(&self.job.queue, &self.job.id, &self.job.run_id.unwrap())
            .await
    }
}

pub struct Builder<R, P, S> {
    client: Arc<Client>,
    queue: String,
    runner: Arc<R>,
    max_workers: usize,
    _payload: PhantomData<P>,
    _state: PhantomData<S>,
}

impl<R, P, S> Builder<R, P, S>
where
    R: Runner<P, S> + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
    S: DeserializeOwned + Send + Sync + 'static,
{
    #[inline]
    pub fn new(client: Arc<Client>, queue: &str, runner: R) -> Self {
        Self {
            client,
            max_workers: 10,
            runner: Arc::new(runner),
            queue: queue.to_string(),
            _payload: PhantomData,
            _state: PhantomData,
        }
    }

    /// Sets the maximum number of backend async workers indicating the maximum number of in-flight
    /// `Job`s.
    pub const fn max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = max_workers;
        self
    }

    // TODO: Add immutable middleware like interceptor functions allowing pre or post processing of
    //       jobs. eg. recording job start and end times, logging, metrics, history.

    /// Creates a new `Poller` using the Builders configuration.
    #[inline]
    pub fn build(self) -> std::result::Result<Poller<R, P, S>, anyhow::Error> {
        if self.max_workers == 0 {
            return Err(anyhow::anyhow!("max_workers must be greater than 0"));
        }
        Ok(Poller {
            client: self.client,
            max_workers: self.max_workers,
            runner: self.runner,
            queue: self.queue,
            _payload: PhantomData,
            _state: PhantomData,
        })
    }
}

/// Poller is used to abstract away polling and running multiple `Job`s calling the provided `Fn`.
pub struct Poller<W, P, S> {
    client: Arc<Client>,
    max_workers: usize,
    runner: Arc<W>,
    queue: String,
    _payload: PhantomData<P>,
    _state: PhantomData<S>,
}

impl<R, P, S> Poller<R, P, S>
where
    R: Runner<P, S> + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
    S: DeserializeOwned + Send + Sync + 'static,
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
        let (tx, rx) = async_channel::bounded(self.max_workers);
        let (tx_sem, rx_sem) = async_channel::bounded(self.max_workers);

        let handles = (0..self.max_workers)
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
