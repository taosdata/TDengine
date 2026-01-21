use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Created,
    Queued,
    Running,
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
        matches!(self, TaskStatus::Queued | TaskStatus::Running)
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TaskStatus::Created => "created",
            TaskStatus::Queued => "queued",
            TaskStatus::Running => "running",
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
        match self {
            AgentStatus::Idle => write!(f, "idle"),
            AgentStatus::Waiting => write!(f, "waiting"),
            AgentStatus::Connected => write!(f, "connected"),
            AgentStatus::Transferring => write!(f, "transferring"),
            AgentStatus::Disconnected => write!(f, "disconnected"),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
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
        match self {
            HealthStatus::Initial => write!(f, "initial"),
            HealthStatus::Ready => write!(f, "ready"),
            HealthStatus::Idle => write!(f, "idle"),
            HealthStatus::Active => write!(f, "active"),
            HealthStatus::Pending => write!(f, "pending"),
            HealthStatus::Busy => write!(f, "busy"),
            HealthStatus::Bounce => write!(f, "bounce"),
            HealthStatus::SourceError => write!(f, "source_error"),
            HealthStatus::SinkError => write!(f, "sink_error"),
            HealthStatus::Fatal => write!(f, "fatal"),
        }
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
    pub status: Option<ActivityStatus>,
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
            status: Some(ActivityStatus::Task(TaskStatus::Stopping)),
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
            status: Some(ActivityStatus::Task(TaskStatus::Stopped)),
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
            status: Some(ActivityStatus::Task(TaskStatus::Stopped)),
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
            status: Some(ActivityStatus::Task(TaskStatus::Queued)),
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
            status: Some(ActivityStatus::Task(TaskStatus::Running)),
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
            status: Some(ActivityStatus::Agent(AgentStatus::Idle)),
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
            status: Some(ActivityStatus::Agent(AgentStatus::Transferring)),
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
            status: None,
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
            status: None,
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
            status: None,
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
            status: None,
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
            status: Some(ActivityStatus::Task(TaskStatus::Completed)),
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
            status: None,
        }
    }
    pub fn ipc_finished(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: ActivityLevel::Info,
            activity: "IPC finished".to_string(),
            status: None,
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
            status: Some(ActivityStatus::Task(TaskStatus::Failed)),
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
            status: Some(ActivityStatus::Agent(AgentStatus::Waiting)),
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
            status: Some(ActivityStatus::Agent(AgentStatus::Connected)),
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
            status: None,
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
            status: Some(ActivityStatus::Agent(AgentStatus::Disconnected)),
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
            status: Some(ActivityStatus::Agent(AgentStatus::Connected)),
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
        Self {
            status: Some(status),
            ..self
        }
    }
}
