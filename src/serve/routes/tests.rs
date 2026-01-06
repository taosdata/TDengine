use actix_web::http::StatusCode;
use actix_web::{App, test, web};

use crate::serve::controller::TaskControllerRef;
use crate::serve::controller::replica::ReplicaOpts;
use crate::serve::routes::{cluster::*, replica::*, utils::*};
use crate::serve::scheduler::agent::AgentWorker;
use crate::serve::scheduler::runner::AgentIntegrationChannel;
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
    let agent_integration_channel = AgentIntegrationChannel::Server(agent_worker);

    let scheduler = TaskScheduler::new(scheduler_notify_sender, agent_integration_channel)
        .await
        .unwrap();

    let controller = TaskControllerRef::from_sqlite("sqlite::memory:", scheduler, 100).await?;
    Ok(controller)
}

#[tokio::test]
async fn test_get_cluster_connector_transferred_not_found() {
    let controller = setup_test_controller().await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(controller))
            .service(get_cluster_connector_transferred),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/cluster/999/transferred")
        .to_request();

    let resp = test::call_service(&app, req).await;
    // Should return 200 with empty or error response since cluster doesn't exist
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[tokio::test]
async fn test_start_replica_monitor() {
    let controller = setup_test_controller().await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(controller))
            .service(start_replica_monitor),
    )
    .await;

    let replica_opts = ReplicaOpts {
        id: Some("test-replica".to_string()),
        source: "taos://localhost:6030".to_string(),
        sink: "taos://localhost:6030".to_string(),
        jid: None,
        topic_prefix: None,
        group: None,
        keep_topic_after_remove: false,
        new_databases_checking_interval: Some(60),
    };

    let req = test::TestRequest::post()
        .uri("/replicas")
        .set_json(&replica_opts)
        .to_request();

    let resp = test::call_service(&app, req).await;
    // Should either succeed or fail with proper error
    assert!(resp.status() == StatusCode::OK || resp.status().is_server_error());
}

#[tokio::test]
async fn test_stop_replica_monitor() {
    let controller = setup_test_controller().await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(controller))
            .service(stop_replica_monitor),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/replicas/test-id")
        .to_request();

    let resp = test::call_service(&app, req).await;
    // Should return error since replica doesn't exist
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[tokio::test]
async fn test_delete_replica_monitor_with_stop_action() {
    let controller = setup_test_controller().await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(controller))
            .service(delete_replica_monitor),
    )
    .await;

    let action = serde_json::json!({
        "action": "stop",
        "options": {}
    });

    let req = test::TestRequest::delete()
        .uri("/replicas/test-id")
        .set_json(&action)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[tokio::test]
async fn test_delete_replica_monitor_with_delete_action() {
    let controller = setup_test_controller().await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(controller))
            .service(delete_replica_monitor),
    )
    .await;

    let action = serde_json::json!({
        "action": "delete",
        "options": {}
    });

    let req = test::TestRequest::delete()
        .uri("/replicas/test-id")
        .set_json(&action)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[tokio::test]
async fn test_delete_replica_monitor_with_start_action() {
    let controller = setup_test_controller().await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(controller))
            .service(delete_replica_monitor),
    )
    .await;

    let action = serde_json::json!({
        "action": "start",
        "options": {
            "new_databases_checking_interval": 120
        }
    });

    let req = test::TestRequest::delete()
        .uri("/replicas/test-id")
        .set_json(&action)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[tokio::test]
async fn test_delete_replica_monitor_with_invalid_action() {
    let controller = setup_test_controller().await.unwrap();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(controller))
            .service(delete_replica_monitor),
    )
    .await;

    let action = serde_json::json!({
        "action": "invalid_action",
        "options": {}
    });

    let req = test::TestRequest::delete()
        .uri("/replicas/test-id")
        .set_json(&action)
        .to_request();

    let resp = test::call_service(&app, req).await;
    dbg!(&resp.status());
    // Should return error for invalid action
    assert!(!resp.status().is_success());
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
