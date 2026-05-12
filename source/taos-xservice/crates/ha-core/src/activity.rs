use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[serde(rename = "-")]
    Unknown,
    Created,
    Queued,
    Running,
    Tick,
    Stopping,
    Stopped,
    Completed,
    Failed,
}

impl TaskStatus {
    pub fn is_stopped(&self) -> bool {
        matches!(
            self,
            TaskStatus::Stopped | TaskStatus::Completed | TaskStatus::Failed
        )
    }

    pub fn is_running(&self) -> bool {
        matches!(
            self,
            TaskStatus::Queued | TaskStatus::Running | TaskStatus::Tick | TaskStatus::Stopping
        )
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TaskStatus::Unknown => "-",
            TaskStatus::Created => "created",
            TaskStatus::Queued => "queued",
            TaskStatus::Running => "running",
            TaskStatus::Tick => "tick",
            TaskStatus::Stopping => "stopping",
            TaskStatus::Stopped => "stopped",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
        };
        write!(f, "{s}")
    }
}

impl std::convert::From<&TaskStatus> for TaskStatus {
    fn from(value: &TaskStatus) -> Self {
        *value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    #[serde(rename = "-")]
    Unknown,
    Idle,
    Waiting,
    Connected,
    Transferring,
    Disconnected,
}

impl AgentStatus {
    pub fn is_connected(&self) -> bool {
        matches!(self, AgentStatus::Connected)
    }

    pub fn is_transferring(&self) -> bool {
        matches!(self, AgentStatus::Transferring)
    }

    pub fn is_waiting(&self) -> bool {
        matches!(self, AgentStatus::Waiting)
    }

    pub fn is_disconnected(&self) -> bool {
        matches!(self, AgentStatus::Disconnected)
    }

    pub fn is_idle(&self) -> bool {
        matches!(self, AgentStatus::Idle)
    }
}

impl std::fmt::Display for AgentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            AgentStatus::Unknown => "-",
            AgentStatus::Idle => "idle",
            AgentStatus::Waiting => "waiting",
            AgentStatus::Connected => "connected",
            AgentStatus::Transferring => "transferring",
            AgentStatus::Disconnected => "disconnected",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    #[serde(rename = "-")]
    Unknown,
    Initial,
    Ready,
    Idle,
    Active,
    Pending,
    Busy,
    Bounce,
    SourceError,
    SinkError,
    Fatal,
}

impl HealthStatus {
    pub fn level(&self) -> ActivityLevel {
        match self {
            HealthStatus::Unknown => ActivityLevel::Info,
            HealthStatus::Initial => ActivityLevel::Info,
            HealthStatus::Ready => ActivityLevel::Info,
            HealthStatus::Idle => ActivityLevel::Info,
            HealthStatus::Active => ActivityLevel::Info,
            HealthStatus::Pending => ActivityLevel::Info,
            HealthStatus::Busy => ActivityLevel::Warn,
            HealthStatus::Bounce => ActivityLevel::Warn,
            HealthStatus::SourceError => ActivityLevel::Error,
            HealthStatus::SinkError => ActivityLevel::Error,
            HealthStatus::Fatal => ActivityLevel::Error,
        }
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            HealthStatus::Unknown => "-",
            HealthStatus::Initial => "initial",
            HealthStatus::Ready => "ready",
            HealthStatus::Idle => "idle",
            HealthStatus::Active => "active",
            HealthStatus::Pending => "pending",
            HealthStatus::Busy => "busy",
            HealthStatus::Bounce => "bounce",
            HealthStatus::SourceError => "source_error",
            HealthStatus::SinkError => "sink_error",
            HealthStatus::Fatal => "fatal",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum ActivityStatus {
    Task(TaskStatus),
    Agent(AgentStatus),
    Health(HealthStatus),
}

impl std::fmt::Display for ActivityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityStatus::Task(status) => write!(f, "{}", status),
            ActivityStatus::Agent(status) => write!(f, "{}", status),
            ActivityStatus::Health(status) => write!(f, "{}", status),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for ActivityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityLevel::Error => write!(f, "error"),
            ActivityLevel::Warn => write!(f, "warn"),
            ActivityLevel::Info => write!(f, "info"),
            ActivityLevel::Debug => write!(f, "debug"),
            ActivityLevel::Trace => write!(f, "trace"),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Activity {
    pub agent_id: i64,
    pub task_id: i64,
    pub job_id: i64,
    pub at: chrono::DateTime<Utc>,
    pub level: ActivityLevel,
    pub activity: String,
    pub status: ActivityStatus,
}

impl Activity {
    pub fn stopping(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "Task stopping".to_string(),
            status: ActivityStatus::Task(TaskStatus::Stopping),
        }
    }
    pub fn stopped(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "Task has been stopped".to_string(),
            status: ActivityStatus::Task(TaskStatus::Stopped),
        }
    }
    pub fn stopping_timeout(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "Stopping task timed out.".to_string(),
            status: ActivityStatus::Task(TaskStatus::Stopped),
        }
    }

    pub fn queued(task_id: i64, job_id: i64, sid: Uuid) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: format!("Enqueue task ({task_id},{job_id}) by schedule id: {sid}"),
            status: ActivityStatus::Task(TaskStatus::Queued),
        }
    }

    /// Info-level activity under running state.
    pub fn running(task_id: i64, job_id: i64, message: impl Into<String>) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: message.into(),
            status: ActivityStatus::Task(TaskStatus::Running),
        }
    }

    pub fn agent_idle(agent_id: i64, message: String) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: message,
            status: ActivityStatus::Agent(AgentStatus::Idle),
        }
    }

    /// Info-level activity under running state.
    pub fn agent_transferring(agent_id: i64, message: String) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: message,
            status: ActivityStatus::Agent(AgentStatus::Transferring),
        }
    }

    /// Error-level activity under running state.
    pub fn error(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Error,
            activity: message,
            status: ActivityStatus::Task(TaskStatus::Unknown),
        }
    }

    pub fn agent_error(agent_id: i64, task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Error,
            activity: message,
            status: ActivityStatus::Agent(AgentStatus::Unknown),
        }
    }

    /// Warn-level activity under running state.
    pub fn warn(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Warn,
            activity: message,
            status: ActivityStatus::Task(TaskStatus::Unknown),
        }
    }

    pub fn info(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: message,
            status: ActivityStatus::Task(TaskStatus::Unknown),
        }
    }

    pub fn completed(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "task completed".to_string(),
            status: ActivityStatus::Task(TaskStatus::Completed),
        }
    }

    pub fn tick(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "Wait for next tick in schedule".to_string(),
            status: ActivityStatus::Task(TaskStatus::Tick),
        }
    }

    pub fn ipc_started(agent_id: i64) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "Agent is putting data".to_string(),
            status: ActivityStatus::Agent(AgentStatus::Unknown),
        }
    }
    pub fn ipc_finished(agent_id: i64, task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "IPC finished".to_string(),
            status: ActivityStatus::Agent(AgentStatus::Unknown),
        }
    }

    pub fn failed(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Error,
            activity: format!("Failed with error: {message}"),
            status: ActivityStatus::Task(TaskStatus::Failed),
        }
    }

    pub fn agent_waiting(task_id: i64, job_id: i64, agent_id: i64, message: String) -> Self {
        Self {
            agent_id,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Warn,
            activity: message,
            status: ActivityStatus::Agent(AgentStatus::Waiting),
        }
    }

    pub fn agent_resumed(task_id: i64, job_id: i64, agent_id: i64) -> Self {
        Self {
            agent_id,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Warn,
            activity: format!("Agent {agent_id} resumed"),
            status: ActivityStatus::Agent(AgentStatus::Connected),
        }
    }

    pub fn health_state(task_id: i64, job_id: i64, at: DateTime<Utc>, state: HealthStatus) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at,
            level: state.level(),
            activity: state.to_string(),
            status: ActivityStatus::Health(state),
        }
    }

    pub fn agent_disconnect(agent_id: i64) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: ActivityLevel::Warn,
            activity: "Agent disconnected".to_string(),
            status: ActivityStatus::Agent(AgentStatus::Disconnected),
        }
    }

    pub fn agent_connect(agent_id: i64, client: &str) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: format!("Agent is connected with client addr {client}"),
            status: ActivityStatus::Agent(AgentStatus::Connected),
        }
    }

    pub fn level(self, level: ActivityLevel) -> Self {
        Self { level, ..self }
    }

    pub fn activity(self, activity: String) -> Self {
        Self { activity, ..self }
    }

    pub fn message(self, message: String) -> Self {
        Self {
            activity: message,
            ..self
        }
    }

    pub fn status(self, status: ActivityStatus) -> Self {
        Self { status, ..self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_task_status(status: ActivityStatus, expected: TaskStatus) {
        match status {
            ActivityStatus::Task(status) => assert_eq!(status, expected),
            _ => panic!("expected task status"),
        }
    }

    fn assert_agent_status(status: ActivityStatus, expected: AgentStatus) {
        match status {
            ActivityStatus::Agent(status) => assert_eq!(status, expected),
            _ => panic!("expected agent status"),
        }
    }

    fn assert_health_status(status: ActivityStatus, expected: &str) {
        match status {
            ActivityStatus::Health(status) => assert_eq!(status.to_string(), expected),
            _ => panic!("expected health status"),
        }
    }

    #[test]
    fn task_status_helpers_classify_running_and_stopped_states() {
        for status in [
            TaskStatus::Queued,
            TaskStatus::Running,
            TaskStatus::Tick,
            TaskStatus::Stopping,
        ] {
            assert!(status.is_running());
            assert!(!status.is_stopped());
        }

        for status in [
            TaskStatus::Stopped,
            TaskStatus::Completed,
            TaskStatus::Failed,
        ] {
            assert!(status.is_stopped());
            assert!(!status.is_running());
        }

        assert!(!TaskStatus::Unknown.is_running());
        assert!(!TaskStatus::Created.is_stopped());
    }

    #[test]
    fn statuses_and_levels_use_stable_display_values() {
        assert_eq!(TaskStatus::Unknown.to_string(), "-");
        assert_eq!(TaskStatus::Running.to_string(), "running");
        assert_eq!(AgentStatus::Disconnected.to_string(), "disconnected");
        assert_eq!(HealthStatus::SourceError.to_string(), "source_error");
        assert_eq!(ActivityStatus::Task(TaskStatus::Tick).to_string(), "tick");
        assert_eq!(
            ActivityStatus::Agent(AgentStatus::Transferring).to_string(),
            "transferring"
        );
        assert_eq!(ActivityLevel::Trace.to_string(), "trace");
        assert_eq!(ActivityLevel::Error.to_string(), "error");
    }

    #[test]
    fn agent_status_helpers_match_only_their_own_state() {
        assert!(AgentStatus::Connected.is_connected());
        assert!(AgentStatus::Transferring.is_transferring());
        assert!(AgentStatus::Waiting.is_waiting());
        assert!(AgentStatus::Disconnected.is_disconnected());
        assert!(AgentStatus::Idle.is_idle());

        assert!(!AgentStatus::Unknown.is_connected());
        assert!(!AgentStatus::Connected.is_idle());
        assert!(!AgentStatus::Idle.is_transferring());
    }

    #[test]
    fn health_status_maps_to_expected_activity_level() {
        for status in [
            HealthStatus::Unknown,
            HealthStatus::Initial,
            HealthStatus::Ready,
            HealthStatus::Idle,
            HealthStatus::Active,
            HealthStatus::Pending,
        ] {
            assert_eq!(status.level(), ActivityLevel::Info);
        }

        assert_eq!(HealthStatus::Busy.level(), ActivityLevel::Warn);
        assert_eq!(HealthStatus::Bounce.level(), ActivityLevel::Warn);
        assert_eq!(HealthStatus::SourceError.level(), ActivityLevel::Error);
        assert_eq!(HealthStatus::SinkError.level(), ActivityLevel::Error);
        assert_eq!(HealthStatus::Fatal.level(), ActivityLevel::Error);
    }

    #[test]
    fn task_activity_factories_populate_ids_levels_messages_and_statuses() {
        let stopping = Activity::stopping(1, 2);
        assert_eq!(stopping.agent_id, -1);
        assert_eq!(stopping.task_id, 1);
        assert_eq!(stopping.job_id, 2);
        assert_eq!(stopping.level, ActivityLevel::Info);
        assert_eq!(stopping.activity, "Task stopping");
        assert_task_status(stopping.status, TaskStatus::Stopping);

        let stopped = Activity::stopped(3, 4);
        assert_eq!(stopped.activity, "Task has been stopped");
        assert_task_status(stopped.status, TaskStatus::Stopped);

        let timeout = Activity::stopping_timeout(5, 6);
        assert_eq!(timeout.activity, "Stopping task timed out.");
        assert_task_status(timeout.status, TaskStatus::Stopped);

        let completed = Activity::completed(7, 8);
        assert_eq!(completed.activity, "task completed");
        assert_task_status(completed.status, TaskStatus::Completed);

        let tick = Activity::tick(9, 10);
        assert_eq!(tick.activity, "Wait for next tick in schedule");
        assert_task_status(tick.status, TaskStatus::Tick);
    }

    #[test]
    fn queued_running_and_error_factories_preserve_messages() {
        let schedule_id = Uuid::nil();
        let queued = Activity::queued(11, 12, schedule_id);
        assert_eq!(
            queued.activity,
            "Enqueue task (11,12) by schedule id: 00000000-0000-0000-0000-000000000000"
        );
        assert_task_status(queued.status, TaskStatus::Queued);

        let running = Activity::running(13, 14, "copying");
        assert_eq!(running.activity, "copying");
        assert_eq!(running.level, ActivityLevel::Info);
        assert_task_status(running.status, TaskStatus::Running);

        let error = Activity::error(15, 16, "bad".to_string());
        assert_eq!(error.activity, "bad");
        assert_eq!(error.level, ActivityLevel::Error);
        assert_task_status(error.status, TaskStatus::Unknown);

        let warn = Activity::warn(17, 18, "slow".to_string());
        assert_eq!(warn.level, ActivityLevel::Warn);
        assert_eq!(warn.activity, "slow");
        assert_task_status(warn.status, TaskStatus::Unknown);

        let failed = Activity::failed(19, 20, "boom".to_string());
        assert_eq!(failed.activity, "Failed with error: boom");
        assert_eq!(failed.level, ActivityLevel::Error);
        assert_task_status(failed.status, TaskStatus::Failed);
    }

    #[test]
    fn agent_activity_factories_populate_agent_statuses() {
        let idle = Activity::agent_idle(21, "ready".to_string());
        assert_eq!(idle.agent_id, 21);
        assert_eq!(idle.task_id, -1);
        assert_eq!(idle.job_id, -1);
        assert_eq!(idle.activity, "ready");
        assert_agent_status(idle.status, AgentStatus::Idle);

        let transferring = Activity::agent_transferring(22, "sending".to_string());
        assert_eq!(transferring.activity, "sending");
        assert_agent_status(transferring.status, AgentStatus::Transferring);

        let waiting = Activity::agent_waiting(23, 24, 25, "waiting".to_string());
        assert_eq!(waiting.task_id, 23);
        assert_eq!(waiting.job_id, 24);
        assert_eq!(waiting.agent_id, 25);
        assert_eq!(waiting.level, ActivityLevel::Warn);
        assert_agent_status(waiting.status, AgentStatus::Waiting);

        let resumed = Activity::agent_resumed(26, 27, 28);
        assert_eq!(resumed.activity, "Agent 28 resumed");
        assert_agent_status(resumed.status, AgentStatus::Connected);
    }

    #[test]
    fn agent_connection_and_ipc_factories_have_expected_defaults() {
        let connected = Activity::agent_connect(31, "127.0.0.1:6055");
        assert_eq!(
            connected.activity,
            "Agent is connected with client addr 127.0.0.1:6055"
        );
        assert_eq!(connected.level, ActivityLevel::Info);
        assert_agent_status(connected.status, AgentStatus::Connected);

        let disconnected = Activity::agent_disconnect(32);
        assert_eq!(disconnected.activity, "Agent disconnected");
        assert_eq!(disconnected.level, ActivityLevel::Warn);
        assert_agent_status(disconnected.status, AgentStatus::Disconnected);

        let ipc_started = Activity::ipc_started(33);
        assert_eq!(ipc_started.activity, "Agent is putting data");
        assert_agent_status(ipc_started.status, AgentStatus::Unknown);

        let ipc_finished = Activity::ipc_finished(34, 35, 36);
        assert_eq!(ipc_finished.activity, "IPC finished");
        assert_eq!(ipc_finished.task_id, 35);
        assert_eq!(ipc_finished.job_id, 36);
        assert_agent_status(ipc_finished.status, AgentStatus::Unknown);
    }

    #[test]
    fn health_state_uses_provided_time_and_status_level() {
        let at = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let activity = Activity::health_state(41, 42, at, HealthStatus::Fatal);

        assert_eq!(activity.task_id, 41);
        assert_eq!(activity.job_id, 42);
        assert_eq!(activity.at, at);
        assert_eq!(activity.level, ActivityLevel::Error);
        assert_eq!(activity.activity, "fatal");
        assert_health_status(activity.status, "fatal");
    }

    #[test]
    fn builder_methods_override_selected_fields() {
        let activity = Activity::info(51, 52, "original".to_string())
            .level(ActivityLevel::Debug)
            .activity("activity override".to_string())
            .message("message override".to_string())
            .status(ActivityStatus::Task(TaskStatus::Created));

        assert_eq!(activity.task_id, 51);
        assert_eq!(activity.job_id, 52);
        assert_eq!(activity.level, ActivityLevel::Debug);
        assert_eq!(activity.activity, "message override");
        assert_task_status(activity.status, TaskStatus::Created);
    }
}
