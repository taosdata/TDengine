use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, Clone)]
pub enum JobSchedulerError {
    CantRemove,
    CantAdd,
    CantInit,
    TickError,
    CantGetTimeUntil,
    Shutdown,
    ShutdownNotifier,
    AddShutdownNotifier,
    RemoveShutdownNotifier,
    FetchJob,
    SaveJob,
    StartScheduler,
    ErrorLoadingGuidList,
    ErrorLoadingJob,
    CouldNotGetTimeUntilNextTick,
    GetJobData,
    GetJobStore,
    JobTick,
    UpdateJobData,
    NoNextTick,
    CantListGuids,
    CantListNextTicks,
    NotifyOnStateError,
    ParseSchedule,
    JobTypeNotSet,
    RunOrRunAsyncNotSet,
    ScheduleNotSet,
    DurationNotSet,
}

impl Display for JobSchedulerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for JobSchedulerError {}
