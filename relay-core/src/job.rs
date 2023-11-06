use crate::num::{PositiveI16, PositiveI32};
use anydate::serde::deserialize::anydate_utc_option;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fmt::{Display, Formatter};
use uuid::Uuid;

/// This is a custom enqueue mode that determines the behaviour of the enqueue function.
#[derive(PartialEq, Eq)]
#[repr(u8)]
pub enum EnqueueMode {
    /// This ensures the Job is unique by Job ID and will return an error id any Job already exists.
    Unique,
    /// This will silently do nothing if the Job that already exists.
    Ignore,
    /// This will replace the existing Job with the new Job changing the job to be immediately no longer in-flight.
    Replace,
}

impl Display for EnqueueMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EnqueueMode::Unique => write!(f, "unique"),
            EnqueueMode::Ignore => write!(f, "ignore"),
            EnqueueMode::Replace => write!(f, "replace"),
        }
    }
}

// Is a structure used to enqueue a new Job.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct New<P, S> {
    /// Is used to differentiate different job types that can be picked up by job runners/workers.
    ///
    /// The maximum size is 1024 characters.
    pub queue: String,

    /// The unique Job ID which is also CAN be used to ensure the Job is unique within a `queue`.
    ///
    /// The maximum size is 1024 characters.
    pub id: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported when specifying None.
    pub max_retries: Option<PositiveI16>,

    /// The immutable raw JSON payload that the job runner will receive and used to execute the Job.
    pub payload: P,

    /// The mutable raw JSON state payload that the job runner will receive, update and use to track Job progress.
    pub state: Option<S>,

    /// Indicates the time that a `Job` is eligible to be run. Defaults to now if not specified.
    pub run_at: Option<DateTime<Utc>>,
}

/// Job defines all information about a Job.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Existing<P, S> {
    /// Is used to differentiate different job types that can be picked up by job runners/workers.
    ///
    /// The maximum size is 1024 characters.
    pub queue: String,

    /// The unique Job ID which is also CAN be used to ensure the Job is unique within a `queue`.
    ///
    /// The maximum size is 1024 characters.
    pub id: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported when specifying None.
    pub max_retries: Option<PositiveI16>,

    /// Specifies how many more times the Job can be retried before being considered permanently failed and deleted
    pub retries_remaining: Option<PositiveI16>,

    /// The immutable raw JSON payload that the job runner will receive and used to execute the Job.
    pub payload: P,

    /// The mutable raw JSON state payload that the job runner will receive, update and use to track Job progress.
    pub state: Option<S>,

    /// Is the current Jobs unique `run_id`. When there is a value here it signifies that the job is
    /// currently in-flight being processed.
    pub run_id: Option<Uuid>,

    /// Indicates the time that a `Job` is eligible to be run. Defaults to now if not specified.
    pub run_at: DateTime<Utc>,

    /// This indicates the last time the `Job` was updated either through enqueue, requeue or
    /// heartbeat.
    pub updated_at: DateTime<Utc>,

    /// This indicates the time the `Job` was originally created.
    pub created_at: DateTime<Utc>,
}

impl From<Existing<Box<RawValue>, Box<RawValue>>> for OldV1<Box<RawValue>, Box<RawValue>> {
    fn from(value: Existing<Box<RawValue>, Box<RawValue>>) -> Self {
        OldV1 {
            id: value.id,
            queue: value.queue,
            timeout: value.timeout.get(),
            max_retries: if let Some(max_retries) = value.max_retries {
                i32::from(max_retries.get())
            } else {
                -1
            },
            payload: value.payload,
            state: value.state,
            run_at: Some(value.run_at),
            updated_at: Some(value.updated_at),
        }
    }
}

#[deprecated(
    note = "please update to using Relay v2 endpoints and clients that use job::Existing & job::New."
)]
/// Job defines all information needed to process a job.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OldV1<P, S> {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: String,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: i32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported by using a negative number eg. -1
    #[serde(default)]
    pub max_retries: i32,

    /// The immutable raw JSON payload that the job runner will receive and used to execute the Job.
    pub payload: P,

    /// The mutable raw JSON state payload that the job runner will receive, update and use to track Job progress.
    pub state: Option<S>,

    /// Indicates the time that a `Job` is eligible to be run. Defaults to now if not specified.
    #[serde(default, deserialize_with = "anydate_utc_option")]
    pub run_at: Option<DateTime<Utc>>,

    /// This indicates the last time the Job was updated either through enqueue, reschedule or
    /// heartbeat.
    /// This value is for reporting purposes only and will be ignored when enqueuing and rescheduling.
    pub updated_at: Option<DateTime<Utc>>,
}

impl From<OldV1<Box<RawValue>, Box<RawValue>>> for New<Box<RawValue>, Box<RawValue>> {
    fn from(value: OldV1<Box<RawValue>, Box<RawValue>>) -> Self {
        New {
            id: value.id,
            queue: value.queue,
            timeout: PositiveI32::new(value.timeout)
                .unwrap_or_else(|| PositiveI32::new(0).unwrap()),
            max_retries: if value.max_retries < 0 {
                None
            } else if value.max_retries > i32::from(i16::MAX) {
                Some(PositiveI16::new(i16::MAX).unwrap())
            } else if value.max_retries < i32::from(i16::MIN) {
                None
            } else {
                Some(
                    PositiveI16::new(i16::try_from(value.max_retries).unwrap())
                        .unwrap_or_else(|| PositiveI16::new(0).unwrap()),
                )
            },
            payload: value.payload,
            state: value.state,
            run_at: value.run_at,
        }
    }
}
