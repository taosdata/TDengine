use actix_web::{http::Method, test, web, App};

use super::*;

#[actix_web::test]
async fn test_large_task_expand() {
    tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::ACTIVE)
        .with_max_level(tracing::Level::DEBUG)
        .compact()
        .init();
    let mut args = Args::default();
    const EXPLORER_PORT: u16 = 6060;
    const EXPLORER_CLUSTER: &str = "http://localhost:6041";
    const EXPLORER_X_PAI: &str = "http://localhost:6050";
    args.port.get_or_insert(EXPLORER_PORT);
    args.profile
        .cluster
        .get_or_insert(EXPLORER_CLUSTER.to_string());
    args.profile.x_api.get_or_insert(EXPLORER_X_PAI.to_string());
    let args = web::Data::new(args);
    let app = test::init_service(
        App::new()
            .wrap(TracingLogger::default())
            .app_data(web::Data::new(Client::new()))
            .app_data(args.clone())
            .route("/api/x/{api:.*}", web::to(x_api)),
    )
    .await;
    let data = include_bytes!("../tests/assets/large.json.zst");

    let mut buf = Vec::new();
    zstd::stream::copy_decode(&data[..], &mut buf).unwrap();
    dbg!(&buf.len());
    let req = test::TestRequest::default()
        .app_data(web::Data::new(Client::new()))
        .app_data(args.clone())
        .uri("/api/x/tasks")
        .param("expand", "true")
        .method(Method::POST)
        .set_json(&buf[..])
        .to_request();
    let resp = test::call_service(&app, req).await;
    dbg!(&resp);
    assert!(resp.status().is_success());
    let req = test::TestRequest::default()
        .app_data(web::Data::new(Client::new()))
        .app_data(args.clone())
        .uri(&format!("/api/x/tasks"))
        .param("expand", "true")
        .method(Method::GET)
        .to_request();
    let resp = test::call_service(&app, req).await;
    dbg!(&resp);
    assert!(resp.status().is_success());
}
