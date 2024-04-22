//! This module contains all the Job definition and transformation logic..
use crate::num::{PositiveI16, PositiveI32};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::{Display, Formatter};
use uuid::Uuid;

/// This is a custom enqueue mode that determines the behaviour of the enqueue function.
#[derive(PartialEq, Eq)]
#[repr(u8)]
pub enum EnqueueMode {
    /// This ensures the job is unique by job ID and will return an error id any Job already exists.
    Unique,
    /// This will silently do nothing if the job that already exists.
    Ignore,
    /// This will replace the `Existing` job with the `New` bob changing the job to be immediately no longer in-flight.
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

/// Defines all information needed to enqueue or requeue a job.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct New<P, S> {
    // The unique job identifier.
    pub identifier: Identifier,

    /// Denotes the duration, in seconds, after a job has started processing or since the last
    /// heartbeat request occurred before considering the job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported when specifying None.
    pub max_retries: Option<PositiveI16>,

    /// The immutable raw JSON payload that the job runner will receive and used to execute the Job.
    pub payload: P,

    /// The mutable raw JSON state payload that the job runner will receive, update and use to track job progress.
    pub state: Option<S>,

    /// Indicates the time that a job is eligible to be run. Defaults to now if not specified.
    pub run_at: Option<DateTime<Utc>>,
}

/// Defines all information about an existing Job or Workflow Job.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Existing<P, S> {
    // The unique job identifier.
    pub identifier: Identifier,

    /// Denotes the duration, in seconds, after a job has started processing or since the last
    /// heartbeat request occurred before considering the job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported when specifying None.
    pub max_retries: Option<PositiveI16>,

    /// Specifies how many more times the Job can be retried before being considered permanently failed and deleted
    pub retries_remaining: Option<PositiveI16>,

    /// The immutable raw JSON payload that the job runner will receive and used to execute the job.
    pub payload: P,

    /// The mutable raw JSON state payload that the job runner will receive, update and use to track job progress.
    pub state: Option<S>,

    /// Is the current Jobs unique `run_id`. When there is a value here it signifies that the job is
    /// currently in-flight being processed.
    pub run_id: Option<Uuid>,

    /// Indicates the time that a job is/was eligible to be run. Defaults to now if not specified.
    pub run_at: DateTime<Utc>,

    /// This indicates the last time the job was updated either through enqueue, requeue or
    /// heartbeat.
    pub updated_at: DateTime<Utc>,

    /// This indicates the time the job was originally created.
    pub created_at: DateTime<Utc>,

    /// This indicates that this Job is a child of another, aka part of a workflow. If it is None
    /// then this Job is a top level Job.
    pub workflow: Option<WorkflowInfo>,
}

/// Defines all information about a jobs workflow.
///
/// If the parent is None then this is a top level workflow.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct WorkflowInfo {
    /// This indicates the unique identification information of the parent of the containing `Job`
    /// or `WorkflowDefinition`.
    ///
    /// When this is None then the containing Job is a top level WorkFlowDefinition Job.
    pub parent: Option<Identifier>,

    /// This indicates the unique identification information of the top level `WorkFlowDefinition`
    /// that the containing `Job` or `WorkflowDefinition` belongs to.
    pub ultimate_parent: Identifier,

    /// This indicates the number of children remaining to be processed for the workflow.
    pub children_remaining: PositiveI16,

    pub results: Vec<WorkflowResult>, // TODO: how to structure this?
}

// WorkflowResult is a result of a workflow job.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct WorkflowResult {
    // The child identifier the results came from.
    pub child: Identifier,

    // The result of the workflow job.
    pub result: Value,
}

// WorkflowIdentifier is a unique identifier for a workflow.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Identifier {
    /// Is used to differentiate different job types that can be picked up by job runners/workers
    /// and in this case represents the workflow job queue the containing job belongs to.
    ///
    /// The maximum size is 1024 characters.
    pub queue: String,

    /// The unique workflow job ID which is also CAN be used to ensure the Job is unique within a
    /// `queue` and in this case represents the workflow job ID the containing job belongs to.
    ///
    /// The maximum size is 1024 characters.
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum Workflow<P, S> {
    Workflow(WorkFlowDefinition<P, S>),
    Job(New<P, S>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct WorkFlowDefinition<P, S> {
    // The unique job identifier.
    pub identifier: Identifier,

    /// Denotes the duration, in seconds, after a job has started processing or since the last
    /// heartbeat request occurred before considering the job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported when specifying None.
    ///
    /// NOTE: When max retries occurs for a workflow job the entire WorkflowDefinition is considered
    ///       failed and all jobs both child and parent are removed.
    pub max_retries: Option<PositiveI16>,

    /// The immutable raw JSON payload that the job runner will receive and used to execute the job.
    pub payload: P,

    /// The mutable raw JSON state payload that the job runner will receive, update and use to track job progress.
    pub state: Option<S>,

    // The jobs that are part of the workflow.
    pub jobs: Vec<Workflow<P, S>>,
}
