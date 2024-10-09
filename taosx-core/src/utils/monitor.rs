//! 提供一个 Channel 用于 runner 模块传递子进程的 ID 和 对应的 task_id 给收集连接器 metrics 的模块
//!
//! ---------        --------------------------         -------------------
//! | runner |  ---> | channel<(pid, task_id)> | ----> | metrics_collector |
//! ----------       --------------------------         -------------------
//!

use flume::{Receiver, Sender};
use lazy_static::lazy_static;
use metrics::gauge;
use metrics::IntoLabels;
use sysinfo::Pid;

lazy_static! {
    static ref CHANNEL: (Sender<SubInfo>, Receiver<SubInfo>) = flume::bounded::<SubInfo>(100);
}

#[derive(Debug, Clone)]
pub struct SubInfo {
    sub_pid: u32,
    task_id: i64,
    datasource_name: String,
}

impl SubInfo {
    pub fn new(sub_pid: u32, task_id: i64, datasource_name: &str) -> Self {
        Self {
            sub_pid,
            task_id,
            datasource_name: datasource_name.to_string(),
        }
    }
}

pub fn send_sub_process_info(sub_pid: Option<u32>, task_id: Option<i64>, datasource_name: &str) {
    if task_id.is_none() {
        tracing::debug!("task id is None");
        return;
    }
    let task_id = task_id.unwrap();
    if let Some(sub_pid) = sub_pid {
        let sub_info = SubInfo::new(sub_pid, task_id, datasource_name);
        let sender = CHANNEL.0.clone();
        if let Err(err) = sender.send(sub_info) {
            tracing::error!("send sub process info error: {}", err);
        }
    } else {
        tracing::error!("sub process id is None");
    }
}

pub fn update_sub_connector_process_metrics(
    sys: &sysinfo::System,
    taosx_id: String,
    parent_process_id: sysinfo::Pid,
    monitor_interval: f64,
    cpu_cores: f64,
) {
    let mut living_sub_processes = Vec::<SubInfo>::new();
    while let Ok(sub_info) = CHANNEL.1.try_recv() {
        let sub_process_id = Pid::from_u32(sub_info.sub_pid);
        let task_id = sub_info.task_id.to_string();
        let ds_name = sub_info.datasource_name.clone();
        let sub_process = sys.process(sub_process_id);
        if sub_process.is_none() {
            tracing::debug!("sub process {} not found", sub_process_id);
            continue;
        }
        let sub_process = sub_process.unwrap();
        let parent = sub_process.parent();
        match parent {
            Some(parent) => {
                // 双重检查。如果某个 pid 不是当前进程的子进程 id，那么肯定不是连接器进程
                if parent != parent_process_id {
                    tracing::debug!(
                        "sub process {} parent process id not match {}",
                        sub_process_id,
                        parent_process_id
                    );
                    continue;
                }
            }
            None => {
                tracing::debug!("sub process {} parent process not found", sub_process_id);
                continue;
            }
        }
        let stable_key = "stable".to_string();
        let stable = "taosx_connector".to_string();
        let taosx_id_key = "taosx_id".to_string();
        let task_id_key = "task_id".to_string();
        let ds_name_key = "ds_name".to_string();
        let labels = vec![
            (stable_key, stable),
            (taosx_id_key, taosx_id.clone()),
            (ds_name_key, ds_name),
            (task_id_key, task_id),
        ];
        let labels = labels.into_labels();
        gauge!("process_id", labels.clone()).set(sub_info.sub_pid as f64);
        let cpu = sub_process.cpu_usage();
        gauge!("process_cpu_percent", labels.clone()).set(cpu as f64 / cpu_cores);
        let mem = sub_process.memory() as f64 / sys.total_memory() as f64 * 100.0;
        gauge!("process_memory_percent", labels.clone()).set(mem);
        let disk = sub_process.disk_usage();
        gauge!("process_disk_read_bytes", labels.clone())
            .set(disk.read_bytes as f64 / monitor_interval);
        gauge!("process_disk_written_bytes", labels.clone())
            .set(disk.written_bytes as f64 / monitor_interval);
        gauge!("process_uptime", labels.clone()).set(sub_process.run_time() as f64);
        living_sub_processes.push(sub_info);
    }

    // 对于还活着的子进程，重新入队，等待下次更新 metrics
    for sub_info in living_sub_processes {
        if let Err(err) = CHANNEL.0.send(sub_info) {
            tracing::error!("enqueue sub process info error: {}", err);
        }
    }
}
