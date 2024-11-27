use chrono::{DateTime, Local, Utc};
use std::time::Duration;
use tokio_cron_scheduler::{JobBuilder, JobScheduler};

#[tokio::main]
async fn main() {
    println!("start at: {}", Local::now().to_rfc3339().to_string());

    // 创建 scheduler
    let mut sched = JobScheduler::new().await.unwrap();

    let datetime = DateTime::parse_from_rfc3339("2024-11-24T00:00:00+08:00")
        .unwrap()
        .with_timezone(&Utc);
    const INTERVAL: u64 = 5;

    // 创建 job
    let job = JobBuilder::new()
        // 设置 job 的时区为 Utc
        .with_timezone(Utc)
        // 设置 job 的任务类型为 repeated
        .with_repeated_job_type()
        // 设置 job 的类型为 repeated
        .every_seconds(INTERVAL)
        // 设置 job 的开始时间
        .start_at(datetime)
        // TODO：设置 job 的任务重叠策略
        // 设置 job 的异步执行函数
        .with_run_async(Box::new(|uuid, mut l| {
            Box::pin(async move {
                println!(">>> {}", Local::now().to_rfc3339().to_string());
                tokio::time::sleep(Duration::from_secs(INTERVAL + 2)).await;
                println!("<<< {}", Local::now().to_rfc3339().to_string());
            })
        }))
        .build()
        .unwrap();

    // 将 job 添加到 scheduler
    sched.add(job).await.unwrap();

    // 启动 scheduler
    if let Err(err) = sched.start().await {
        eprintln!("failed to start scheduler, cause: {:?}", err);
    }

    // 等待
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;

    // 停止 scheduler
    sched.shutdown().await.unwrap();
    println!("stop at: {}", Local::now().to_rfc3339().to_string());
}
