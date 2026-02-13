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
            TaskStatus::Queued | TaskStatus::Running | TaskStatus::Tick
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
    pub fn ipc_finished(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
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
