use actix_web::http::StatusCode;
use actix_web::{App, test};

use crate::serve::controller::TaskControllerRef;
use crate::serve::routes::utils::*;
use crate::serve::scheduler::agent::AgentWorker;
use crate::serve::scheduler::{SchedulerNotify, TaskScheduler};
use std::sync::Arc;

async fn setup_test_controller() -> anyhow::Result<TaskControllerRef> {
    let (_agent_activity_sender, _agent_activity_receiver) = tokio::sync::broadcast::channel(1024);
    let (_agent_notify_sender, agent_notify_receiver) = tokio::sync::broadcast::channel(1024);
    let (scheduler_notify_sender, _) = tokio::sync::broadcast::channel::<SchedulerNotify>(1024);
    let scheduler_notify_sender = Arc::new(scheduler_notify_sender);
    let weak_notify_sender = Arc::downgrade(&scheduler_notify_sender);
    let (_agent_spawn_sender, agent_spawn_receiver) = flume::bounded(0);

    let agent_worker = AgentWorker::new(
        _agent_activity_sender,
        agent_notify_receiver,
        weak_notify_sender,
        agent_spawn_receiver,
    )
    .await;

    let scheduler = TaskScheduler::new(scheduler_notify_sender, agent_worker)
        .await
        .unwrap();

    let controller = TaskControllerRef::new(scheduler);
    Ok(controller)
}

#[cfg(not(target_env = "msvc"))]
#[tokio::test]
async fn test_handle_get_heap() {
    let app = test::init_service(App::new().service(handle_get_heap)).await;

    let req = test::TestRequest::get()
        .uri("/debug/pprof/heap")
        .to_request();

    let resp = test::call_service(&app, req).await;
    // Should return either OK with data or Forbidden if jemalloc not enabled
    assert!(resp.status() == StatusCode::OK || resp.status() == StatusCode::FORBIDDEN);
}

#[cfg(target_env = "msvc")]
#[tokio::test]
async fn test_handle_get_heap_windows() {
    let app = test::init_service(App::new().service(handle_get_heap)).await;

    let req = test::TestRequest::get()
        .uri("/debug/pprof/heap")
        .to_request();

    let resp = test::call_service(&app, req).await;
    // Should return Forbidden on Windows
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
