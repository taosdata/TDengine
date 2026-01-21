// ============ xnode ============

pub const HEARTBEAT_REQ: &str = "xnode_heartbeat";
pub const HEARTBEAT_RESP: &str = "xnode_heartbeat_resp";

pub const PLAN_TASK_REQ: &str = "xnode_plan_task";
pub const PLAN_TASK_RESP: &str = "xnode_plan_task_resp";

pub const START_TASK_JOB_REQ: &str = "xnode_start_task_job";
pub const START_TASK_JOB_RESP: &str = "xnode_start_task_job_resp";

pub const STOP_TASK_JOB_REQ: &str = "xnode_stop_task_job";
pub const STOP_TASK_JOB_RESP: &str = "xnode_stop_task_job_resp";

pub const LIST_TASK_JOB_STATES_REQ: &str = "xnode_list_task_job_states";
pub const LIST_TASK_JOB_STATES_RESP: &str = "xnode_list_task_job_states_resp";

pub const TASK_PREVIEW_REQ: &str = "xnode_task_preview";
pub const TASK_PREVIEW_RESP: &str = "xnode_task_preview_resp";

pub const ADD_AGENTS_REQ: &str = "xnode_add_agents";
pub const ADD_AGENTS_RESP: &str = "xnode_add_agents_resp";

pub const DEL_AGENTS_REQ: &str = "xnode_del_agents";
pub const DEL_AGENTS_RESP: &str = "xnode_del_agents_resp";

pub const LIST_AGENTS_REQ: &str = "xnode_list_agents";
pub const LIST_AGENTS_RESP: &str = "xnode_list_agents_resp";

pub const TASK_JOB_DRAIN_REQ: &str = "xnode_task_job_drain";
pub const TASK_JOB_DRAIN_RESP: &str = "xnode_task_job_drain_resp";

pub const CHECK_VALID_REQ: &str = "xnode_check_valid";
pub const CHECK_VALID_RESP: &str = "xnode_check_valid_resp";

pub const GET_SAMPLES_REQ: &str = "xnode_get_samples";
pub const GET_SAMPLES_RESP: &str = "xnode_get_samples_resp";

pub const TASK_METRICS: &str = "xnode_metrics";
pub const XNODE_ACTIVITIES: &str = "xnode_activities";
pub const TASK_JOB_FINISH: &str = "xnode_task_job_finish";

pub const GET_X_HTTP_PORT_REQ: &str = "xnode_get_http_port";
pub const GET_X_HTTP_PORT_RESP: &str = "xnode_get_http_port_resp";

pub const DROP_CONNECTION: &str = "xnode_drop_connection";

pub const TASK_ACTIVITIES_STABLE: &str = "xnode_task_activities";
pub const AGENT_ACTIVITIES_STABLE: &str = "xnode_agent_activities";
pub const TASK_METRICS_STABLE: &str = "xnode_task_metrics";

// ============ agent ============

pub const ACTION_RUN: &str = "agent_task_run";
pub const ACTION_STOP: &str = "agent_task_stop";
pub const ACTION_CANCEL: &str = "agent_task_cancel";
pub const ACTION_LIST_DATA_SETS: &str = "agent_list_datasets";
pub const ACTION_CHECK: &str = "agent_task_check";
pub const ACTION_GET_SAMPLE: &str = "agent_sample";
pub const ACTION_PUT_FILE: &str = "agent_put_file";
pub const ACTION_QUERY_DATA_SOURCE: &str = "agent_query_data_source";
pub const ACTION_SPLIT_TASK: &str = "agent_split_task";
pub const ACTION_EXIT: &str = "agent_exit";

pub const ACTION_TASK_STATUS: &str = "agent_task_status";
pub const ACTION_GET_MONITOR_CONFIG: &str = "agent_get_monitor_config";

pub const MESSAGE_AGENT_ACTIVITY: &str = "agent_activity";
pub const MESSAGE_TASK_ACTIVITY: &str = "agent_task_activity";
pub const MESSAGE_HEARTBEAT_OK: &str = "agent_heartbeat_ok";
pub const MESSAGE_HEARTBEAT: &str = "agent_heartbeat";
pub const MESSAGE_TASK_METRICS: &str = "agent_task_metrics";
pub const MESSAGE_METRICS_EVENTS: &str = "agent_metrics_events";
