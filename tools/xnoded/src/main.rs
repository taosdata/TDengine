use axum::{
    Json, Router, extract::Query, http::StatusCode, response::Json as AxumJson, routing::{get, post}
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use env_logger::Target;
use log::{info, error, warn};

/// 命令行参数结构
#[derive(Parser, Debug)]
#[command(name = "xnoded")]
#[command(about = "XNode daemon service")]
struct Args {
    /// 配置文件目录
    #[arg(short = 'c', long)]
    config_dir: String,
    
    /// 节点ID
    #[arg(short = 'd', long)]
    node_id: i32,
}

/// 创建XNode请求
#[derive(Deserialize)]
struct CreateXNodeRequest {
    xnode: String,
    user: Option<String>,
    pass: Option<String>,
}

/// Drop XNode请求
#[derive(Deserialize)]
struct DropXNodeRequest {
    xnode_id: i32,
    force: bool,
}

/// Drain XNode请求
#[derive(Deserialize)]
struct DrainXNodeRequest {
    xnode: String,
    force: bool,
}

/// xnode status 请求
#[derive(Deserialize)]
struct XNodeStatusRequest {
    xnode_ep: String,
}

#[derive(Serialize)]
struct XNodeStatusResp {
    status: String,
}

/// 任务参数校验请求
#[derive(Deserialize)]
struct CheckParamRequest {
    task_id: i32,
    job_id: i32,
    from: String,
    to: String,
    parser: String,
}

/// 启动任务请求
#[derive(Deserialize)]
struct StartTaskRequest {
    task_id: i32,
    from: serde_json::Value,
    to: serde_json::Value,
    parser: serde_json::Value,
}

/// 停止任务请求
#[derive(Deserialize)]
struct StopTaskRequest {
    task_id: i32,
    job_id: i32,
}

/// 通用响应结构
#[derive(Serialize)]
struct ApiResponse {
    code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg: Option<String>,
}

/// 创建XNode处理器
async fn create_xnode(Json(payload): Json<CreateXNodeRequest>) -> (StatusCode, AxumJson<ApiResponse>) {
    info!("Received create xnode request: xnode={}, user={:?}", payload.xnode, payload.user);
    
    if payload.xnode.is_empty() {
        let response = ApiResponse {
            code: "err".to_string(),
            msg: Some("xnode cannot be empty".to_string()),
        };
        warn!("Create xnode failed: xnode is empty");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    
    let response = ApiResponse {
        code: "ok".to_string(),
        msg: None,
    };
    info!("Create xnode successful: {}", payload.xnode);
    (StatusCode::OK, AxumJson(response))
}

/// Drop XNode处理器
async fn drop_xnode(Json(payload): Json<DropXNodeRequest>) -> (StatusCode, AxumJson<ApiResponse>) {
    info!("Received drop xnode request: xnode_id={}, force={}", payload.xnode_id, payload.force);
    
    if payload.xnode_id <= 0 {
        let response = ApiResponse {
            code: "err".to_string(),
            msg: Some("invalid xnode_id".to_string()),
        };
        warn!("Drop xnode failed: invalid xnode_id");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    
    let response = ApiResponse {
        code: "ok".to_string(),
        msg: None,
    };
    info!("Drop xnode successful: id={}", payload.xnode_id);
    (StatusCode::OK, AxumJson(response))
}

/// Drain XNode处理器
async fn drain_xnode(Json(payload): Json<DrainXNodeRequest>) -> (StatusCode, AxumJson<ApiResponse>) {
    info!("Received drain xnode request: xnode={}, force={}", payload.xnode, payload.force);
    
    // 模拟处理逻辑
    if payload.xnode.is_empty() {
        let response = ApiResponse {
            code: "err".to_string(),
            msg: Some("xnode cannot be empty".to_string()),
        };
        warn!("Drain xnode failed: xnode is empty");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    let response = ApiResponse {
        code: "ok".to_string(),
        msg: None,
    };
    info!("Drain xnode successful: {}", payload.xnode);
    (StatusCode::OK, AxumJson(response))
}

async fn xnode_status(Query(payload): Query<XNodeStatusRequest>) -> (StatusCode, AxumJson<XNodeStatusResp>) {
    info!("Received get xnode status request: xnode_ep={}", payload.xnode_ep);
    
    // 模拟处理逻辑
    if payload.xnode_ep.is_empty() {
        let response = XNodeStatusResp {
            status: "offline".to_string(),
        };
        warn!("Get xnode status failed: xnode_ep is empty");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    let response = XNodeStatusResp {
        status: "online".to_string(),
    };
    (StatusCode::OK, AxumJson(response))
}


/// 参数校验处理器
async fn check_param(Json(payload): Json<CheckParamRequest>) -> (StatusCode, AxumJson<ApiResponse>) {
    info!("Received check param request: task_id={}, job_id={}", payload.task_id, payload.job_id);
    
    if payload.task_id <= 0 {
        let response = ApiResponse {
            code: "err".to_string(),
            msg: Some("param error: invalid task_id".to_string()),
        };
        warn!("Check param failed: invalid task_id");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    if payload.from.is_empty() || payload.to.is_empty() {
        let response = ApiResponse {
            code: "err".to_string(),
            msg: Some("param error: from/to field cannot be empty".to_string()),
        };
        warn!("Check param failed: empty from/to field");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    let response = ApiResponse {
        code: "ok".to_string(),
        msg: None,
    };
    info!("Check param successful: task_id={}", payload.task_id);
    (StatusCode::OK, AxumJson(response))
}

/// 启动任务处理器
async fn start_task(Json(payload): Json<StartTaskRequest>) -> (StatusCode, AxumJson<ApiResponse>) {
    info!("Received start task request: task_id={}", payload.task_id);
    
    // 模拟任务启动逻辑
    if payload.task_id <= 0 {
        let response = ApiResponse {
            code: "err".to_string(),
            msg: Some("invalid task_id".to_string()),
        };
        warn!("Start task failed: invalid task_id");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    // 检查from字段是否包含mysql且需要密码
    if let Some(from_str) = payload.from.as_str() {
        if from_str.contains("mysql") && !from_str.contains("password") {
            let response = ApiResponse {
                code: "err".to_string(),
                msg: Some("from field: mysql need password".to_string()),
            };
            warn!("Start task failed: mysql needs password");
            return (StatusCode::BAD_REQUEST, AxumJson(response));
        }
    }
    
    let response = ApiResponse {
        code: "ok".to_string(),
        msg: None,
    };
    info!("Start task successful: task_id={}", payload.task_id);
    (StatusCode::OK, AxumJson(response))
}

/// 停止任务处理器
async fn stop_task(Json(payload): Json<StopTaskRequest>) -> (StatusCode, AxumJson<ApiResponse>) {
    info!("Received stop task request: task_id={}, job_id={}", payload.task_id, payload.job_id);
    
    // 模拟任务停止逻辑
    if payload.task_id <= 0 || payload.job_id < 0 {
        let response = ApiResponse {
            code: "err".to_string(),
            msg: Some("invalid task_id or job_id".to_string()),
        };
        warn!("Stop task failed: invalid task_id or job_id");
        return (StatusCode::BAD_REQUEST, AxumJson(response));
    }
    
    let response = ApiResponse {
        code: "ok".to_string(),
        msg: None,
    };
    info!("Stop task successful: task_id={}, job_id={}", payload.task_id, payload.job_id);
    (StatusCode::OK, AxumJson(response))
}

/// 初始化日志系统
fn init_logger() -> anyhow::Result<()> {
    // 确保日志目录存在
    let log_dir = "/var/log/taos";
    std::fs::create_dir_all(log_dir)?;
    
    // 配置env_logger输出到文件
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/var/log/taos/xnoded.log")?;
    
    env_logger::Builder::from_default_env()
        .target(Target::Pipe(Box::new(log_file)))
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} {} {}:{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .init();
    
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 解析命令行参数
    // let args = Args::parse();
    // 初始化日志
    init_logger()?;
    // info!("Config directory: {}, Node Id: {}", args.config_dir, args.node_id);
    
    // 创建路由
    let app = Router::new()
        .route("/xnode/create", post(create_xnode))
        .route("/xnode/drop", post(drop_xnode))
        .route("/xnode/drain", post(drain_xnode))
        .route("/xnode/status", get(xnode_status))
        .route("/check_param", post(check_param))
        .route("/task/start", post(start_task))
        .route("/task/stop", post(stop_task));
    
    // 绑定地址, todo: 支持ipv6
    let addr = "127.0.0.1:6051".parse::<std::net::SocketAddr>()?;
    info!("HTTP server listening on {}", addr);
    
    // 启动服务器
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}