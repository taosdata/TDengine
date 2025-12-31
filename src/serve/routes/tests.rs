use actix_web::http::StatusCode;
use actix_web::{App, test};

use crate::serve::routes::utils::*;

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
