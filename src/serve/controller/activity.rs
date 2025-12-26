use chrono::{DateTime, Utc};
use ha_core::types::{AgentStatus, TaskStatus};
use taosx_core::task_set::prelude::HealthState;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum LevelFilter {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct Activity {
    pub agent_id: i64,
    pub task_id: i64,
    pub job_id: i64,
    pub at: chrono::DateTime<Utc>,
    pub level: LevelFilter,
    pub activity: String,
    pub status: Option<String>,
    pub context: Option<String>,
}

impl Activity {
    pub fn stopping(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Task stopping".to_string(),
            status: Some(TaskStatus::Stopping.to_string()),
            context: None,
        }
    }
    pub fn stopped(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Task has been stopped".to_string(),
            status: Some(TaskStatus::Stopped.to_string()),
            context: None,
        }
    }
    pub fn stopping_timeout(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Stopping task timed out.".to_string(),
            status: Some(TaskStatus::Stopped.to_string()),
            context: None,
        }
    }

    pub fn queued(task_id: i64, job_id: i64, jid: Uuid) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Enqueue task ({task_id},{job_id}) by job id: {jid}"),
            status: Some(TaskStatus::Queued.to_string()),
            context: None,
        }
    }

    /// Info-level activity under running state.
    pub fn running(task_id: i64, job_id: i64, message: impl Into<String>) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: message.into(),
            status: Some(TaskStatus::Running.to_string()),
            context: None,
        }
    }

    /// Info-level activity under running state.
    pub fn agent_transferring(agent_id: i64, message: String) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: message,
            status: Some(AgentStatus::Transferring.to_string()),
            context: None,
        }
    }

    /// Error-level activity under running state.
    pub fn error(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Error,
            activity: message,
            status: None,
            context: None,
        }
    }
    /// Warn-level activity under running state.
    pub fn warn(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: message,
            status: None,
            context: None,
        }
    }

    pub fn info(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: message,
            status: None,
            context: None,
        }
    }

    pub fn completed(task_id: i64, job_id: i64, jid: Uuid) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Finished with job id: {jid}."),
            status: Some(TaskStatus::Completed.to_string()),
            context: None,
        }
    }

    pub fn ipc_started(agent_id: i64) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Agent is putting data".to_string(),
            status: None,
            context: None,
        }
    }
    pub fn ipc_finished(task_id: i64, job_id: i64) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "IPC finished".to_string(),
            status: None,
            context: None,
        }
    }

    pub fn failed(task_id: i64, job_id: i64, message: String) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Error,
            activity: format!("Failed with error: {message}"),
            status: Some(TaskStatus::Failed.to_string()),
            context: None,
        }
    }

    pub fn waiting(
        task_id: i64,
        job_id: i64,
        agent_id: i64,
        message: impl std::fmt::Display,
    ) -> Self {
        Self {
            agent_id,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: message.to_string(),
            status: Some(AgentStatus::Waiting.to_string()),
            context: None,
        }
    }

    pub fn agent_resumed(task_id: i64, job_id: i64, agent_id: i64) -> Self {
        Self {
            agent_id,
            task_id,
            job_id,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: format!("Agent {agent_id} resumed"),
            status: Some(AgentStatus::Connected.to_string()),
            context: None,
        }
    }

    pub fn health_state(task_id: i64, job_id: i64, at: DateTime<Utc>, state: HealthState) -> Self {
        Self {
            agent_id: -1,
            task_id,
            job_id,
            at,
            level: if state >= HealthState::Busy {
                LevelFilter::Warn
            } else {
                LevelFilter::Info
            },
            activity: state.to_string(),
            status: None,
            context: None,
        }
    }

    pub fn agent_disconnect(agent_id: i64, context: Option<serde_json::Value>) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: format!("Agent {agent_id} disconnected"),
            status: Some(AgentStatus::Disconnected.to_string()),
            context: context.map(|v| v.to_string()),
        }
    }

    pub fn agent_connect(agent_id: i64, client: &str) -> Self {
        Self {
            agent_id,
            task_id: -1,
            job_id: -1,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Agent is connected with client addr {client}"),
            status: Some(AgentStatus::Connected.to_string()),
            context: Some(format!("client: {client}")),
        }
    }
}
