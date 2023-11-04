use crate::num::{PositiveI16, PositiveI32};
use anydate::serde::deserialize::anydate_utc_option;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: String,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported by using a negative number eg. -1
    pub max_retries: Option<PositiveI16>,

    /// The raw JSON payload that the job runner will receive.
    pub payload: P,

    /// The raw JSON payload that the job runner will receive.
    pub state: Option<S>,

    /// With this you can optionally schedule/set a Job to be run only at a specific time in the
    /// future. This option should mainly be used for one-time jobs and scheduled jobs that have
    /// the option of being self-perpetuated in combination with the reschedule endpoint.
    pub run_at: Option<DateTime<Utc>>,
}

/// Job defines all information about a Job.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Job<P, S> {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: String,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported by using a negative number eg. -1
    pub max_retries: Option<PositiveI16>,

    /// Specifies how many more times the Job can be retried.
    pub retries_remaining: Option<PositiveI16>,

    /// The raw payload that the `Job` requires to run.
    pub payload: P,

    /// The raw `Job` state stored during enqueue, reschedule or heartbeat while in-flight..
    pub state: Option<S>,

    /// Is the current Jobs unique `run_id`. When there is a value here it signifies that the job is
    /// currently in-flight being processed.
    pub run_id: Option<Uuid>,

    /// Indicates the time that a `Job` is eligible to be run.
    pub run_at: DateTime<Utc>,

    /// This indicates the last time the `Job` was updated either through enqueue, reschedule or
    /// heartbeat.
    pub updated_at: DateTime<Utc>,

    /// This indicates the time the `Job` was originally created. this value does now change when a
    /// Job is rescheduled.
    pub created_at: DateTime<Utc>,
}

#[deprecated(note = "please update to using Relay v2 endpoints and clients that use NewJob & Job.")]
/// Job defines all information needed to process a job.
#[derive(Default, Debug, Deserialize, Clone, PartialEq, Eq)]
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

    /// The raw JSON payload that the job runner will receive.
    pub payload: P,

    /// The raw JSON payload that the job runner will receive.
    pub state: Option<S>,

    /// With this you can optionally schedule/set a Job to be run only at a specific time in the
    /// future. This option should mainly be used for one-time jobs and scheduled jobs that have
    /// the option of being self-perpetuated in combination with the reschedule endpoint.
    #[serde(default, deserialize_with = "anydate_utc_option")]
    pub run_at: Option<DateTime<Utc>>,

    /// This indicates the last time the Job was updated either through enqueue, reschedule or
    /// heartbeat.
    /// This value is for reporting purposes only and will be ignored when enqueuing and rescheduling.
    pub updated_at: Option<DateTime<Utc>>,
}
