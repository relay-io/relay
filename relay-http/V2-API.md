# Relay v2 API

This outlines the HTTP serve that exposes relays functionality. 

It is recommended to use the language clients where possible that abstract away much of the complexity.

## API

### Job Structure

**NOTE:** The Queue + Id are what makes a Job unique.

| name                | limitations                                   | description                                                                                                                                                                                                                                                                                                                           |
|---------------------|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `queue`             | 1024 characters                               | The Queue is used to differentiate different job types that can be picked up by job runners/workers.                                                                                                                                                                                                                                  |
| `id`                | 1024 characters                               | The Id is used to ensure the Job is unique within a Queue.                                                                                                                                                                                                                                                                            |
| `timeout`           | 0 - 2147483647 (approx 68 years)              | Denotes the duration, in seconds, after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue.                                                                                                                                                |
| `max_retries`       | 0 - 32767 or None for no max                  | Determines how many times the Job can be retried, due to timeouts, before being considered unprocessable.                                                                                                                                                                                                                             |
| `retries_remaining` | 0 - 32767 or None for no limit                | The number of retries remaining for the Job.                                                                                                                                                                                                                                                                                          |
| `payload`           | 1GB (but don't this is a DB column after all) | The immutable raw JSON payload that the job runner will receive and used to execute the Job.                                                                                                                                                                                                                                          |
| `state`             | 1GB (but don't this is a DB column after all) | The mutable raw JSON state payload that the job runner will receive, update and use to track Job progress.                                                                                                                                                                                                                            |
| `run_id`            | Uuid                                          | The unique, set when runner/worker retrieves the Job(s) for processing, run id. This is used to not only differentiate between different runs of the same Job but also allows updating of the Job in-place, removing the run id, so that the runner/worker can not longer interact with it so it knows when to stop processing async. |
| `run_at`            | RFC339 timestamp (default now)                | Schedule/set a Job to be run only at a specific time in the future. When not set defaults to `now`.                                                                                                                                                                                                                                   |
| `updated_at`        | RFC339 timestamp                              | The last time the Job was updated/heartbeat. This is mainly informational for callers.                                                                                                                                                                                                                                                |
| `created_at`        | RFC339 timestamp                              | The time the Job was created during `enqueue` or `requeue`. This is mainly informational for callers.                                                                                                                                                                                                                                 |



### Mode
First is will be good to understand the possible modes when enqueuing `Jobs`:

| name      | description                                                                                                          |
|-----------|----------------------------------------------------------------------------------------------------------------------|
| `unique`  | Ensures all jobs are unique or will return 409 on first encountered conflict rolling back the transaction.           |
| `ignore`  | On conflict ignores the Job(s) being inserted that conflicted.                                                       |
| `replace` | On conflict replaces the Job(s) being inserted that conflicted and modifies them taking them out of being in-flight. |

### enqueue `POST /v2/queues/jobs`

Enqueues Job(s) to be processed using the provided mode.

#### Query Params

| name   | description                                                               |
|--------|---------------------------------------------------------------------------|
| `mode` | The mode to use when enqueuing the Job(s). See [Mode](#mode) for details. |

#### Request Body
```json
[
    {
        "queue": "my-queue",
        "id": "1",
        "timeout": 30,
        "max_retries": 0,
        "payload": "RAW JSON",
        "state": "RAW JSON",
        "run_at": "2022-09-05T04:37:23Z"
    }
]
```

### Response Codes

NOTE: The body of the response will have more detail about the specific error.

| code | description                                                                                             |
|------|---------------------------------------------------------------------------------------------------------|
| 202  | Job(s) enqueued and accepted for processing.                                                            |
| 400  | A bad/ill-formed request.                                                                               |
| 409  | An conflicting Job already exists with the provided id and queue. Reminder when enqueue mode is unique. |
| 429  | A retryable error occurred.                                                                             |
| 422  | A permanent error has occurred, most likely with the backing datastore.                                 |
| 500  | An unknown error has occurred server side.                                                              |



### next GET /v2/queues/{queue}/jobs`

Retrieves the next `Job(s)` to be processed from the provided `queue`.

#### Query Params
In this case the only arguments are path and query params.

| name       | required | description                                                                    |
|------------|----------|--------------------------------------------------------------------------------|
| `num_jobs` | false    | Specifies how many Jobs to pull to process next in one request. Defaults to 1. |

#### Response Body
Some fields may not be present such as `state` when none exists.
```json
[
  {
    "queue": "my-queue",
    "id": "1",
    "timeout": 30,
    "max_retries": null,
    "retries_remaining": null,
    "payload": "RAW JSON",
    "state": "RAW JSON",
    "run_id": "UUID",
    "run_at": "2022-09-05T04:37:23Z",
    "updated_at": "2022-09-05T04:40:01Z",
    "created_at": "2022-09-05T04:37:23Z"
  }
]
```

#### Response Codes
NOTE: The body of the response will have more detail about the specific error.

| code | description                                                                        |
|------|------------------------------------------------------------------------------------|
| 200  | Job successfully retrieved.                                                        |
| 204  | There is currently no Job in the provided queue to return. Backoff an retry later. |
| 429  | A retryable error occurred.                                                        |
| 500  | An unknown error has occurred server side.                                         |



### heartbeat `PATCH /v2/queues/{queue}/jobs/{id}/run-id/{run_id}`

Updates an in-flight Job incrementing its timestamp and optionally setting some state.


#### Request Body
Any JSON data. 

This payload is persisted in order to save job intermediate state.

#### Response Codes
NOTE: The body of the response will have more detail about the specific error in plain text.

| code | description                                                           |
|------|-----------------------------------------------------------------------|
| 202  | Heartbeat successfully applied to the Job.                            |
| 429  | A retryable error occurred.                                           |
| 404  | Job was not found for updating.                                       |
| 422  | A permanent error has occurred, likely relating to the state payload. |
| 500  | An unknown error has occurred server side.                            |


### requeue `PUT /v2/queues/{queue}/jobs/{id}/run-id/{run_id}`

This endpoint allows atomically requeue a Job that is in-flight. 
The backed process consists of deleting the current `Job` and
inserting the new `Job(s)` specified.

This allows for:
- Rescheduling a `Job` to run again.
- Creating new `Job(s)` from the results of the current one.
- Stepping through a distributed state machine where different steps of the `job` may be better run on different machines eg. compute vs io workloads.

#### Query Params

| name   | description                                                               |
|--------|---------------------------------------------------------------------------|
| `mode` | The mode to use when enqueuing the Job(s). See [Mode](#mode) for details. |

#### Request Body
```json
[
  {
    "queue": "my-queue",
    "id": "1",
    "timeout": 30,
    "max_retries": 0,
    "payload": "RAW JSON",
    "state": "RAW JSON",
    "run_at": "2022-09-05T04:37:23Z"
  }
]
```

### Response Codes

NOTE: The body of the response will have more detail about the specific error.

| code | description                                |
|------|--------------------------------------------|
| 202  | Job(s) accepted for processing.            |
| 400  | For a bad/ill-formed request.              |
| 429  | A retryable error occurred.                |
| 422  | A permanent error has occurred.            |
| 500  | An unknown error has occurred server side. |


### delete `DELETE /v2/queues/{queues}/jobs/{id}`

Delete a Job.

### Response Codes

NOTE: The body of the response will have more detail about the specific error in plain text.

| code  | description                                                                 |
|-------|-----------------------------------------------------------------------------|
| 200   | Job successfully completed.                                                 |
| 429   | A retryable error occurred. Most likely the backing storage having issues.  |
| 422   | A permanent error has occurred.                                             |
| 500   | An unknown error has occurred server side.                                  |


### complete `DELETE /v2/queues/{queues}/jobs/{id}/run-id/{run_id}`

Completes an in-flight Job by deleting it. This differs from deletion in that in that only an in-flight job can complete itself and not possibly delete a `Job` that has been replaced.

### Response Codes

NOTE: The body of the response will have more detail about the specific error in plain text.

| code | description                                                         |
|------|---------------------------------------------------------------------|
| 200  | Job successfully completed.                                         |
| 429  | A retryable error occurred.                                         |
| 422  | A permanent error has occurred, likely related to the query params. |
| 500  | An unknown error has occurred server side.                          |


### exists `HEAD /v2/queues/{queue}/jobs/{id}`

Using HTTP response codes returns if the Job exists.

### Response Codes

NOTE: The body of the response will have more detail about the specific error in plain text.

| code | description                                                         |
|------|---------------------------------------------------------------------|
| 200  | Job exists                                                          |
| 429  | A retryable error occurred.                                         |
| 404  | Job was not found and so did not exist.                             |
| 422  | A permanent error has occurred, likely related to the query params. |
| 500  | An unknown error has occurred server side.                          |


### get `GET /v2/queues/{queue}/jobs/{id}`

Fetches the Job from the database if it exists.

#### Request Body
```json
{
  "queue": "my-queue",
  "id": "1",
  "timeout": 30,
  "max_retries": null,
  "retries_remaining": null,
  "payload": "RAW JSON",
  "state": "RAW JSON",
  "run_id": "UUID",
  "run_at": "2022-09-05T04:37:23Z",
  "updated_at": "2022-09-05T04:40:01Z",
  "created_at": "2022-09-05T04:37:23Z"
}
```

### Response Codes

NOTE: The body of the response will have more detail about the specific error in plain text.

| code | description                                |
|------|--------------------------------------------|
| 200  | Job found and in the response body.        |
| 400  | For a bad/ill-formed request.              |
| 404  | Job was not found.                         |
| 429  | A retryable error occurred.                |
| 422  | A permanent error has occurred.            |
| 500  | An unknown error has occurred server side. |