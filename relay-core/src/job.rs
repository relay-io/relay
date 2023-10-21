/// This is a custom enqueue mode that determines the behaviour of the enqueue function.
pub enum EnqueueMode {
    /// This ensures the Job is unique by Job ID and will return an error id any Job already exists.
    Unique,
    /// This will silently do nothing if the Job that already exists.
    Ignore,
    /// This will replace the existing Job with the new Job changing the job to be immediately no longer in-flight.
    Replace,
}
