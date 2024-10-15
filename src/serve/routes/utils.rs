use actix_web::{get, HttpResponse, Responder};

#[cfg(not(target_env = "msvc"))]
#[get("/debug/pprof/heap")]
pub(super) async fn handle_get_heap() -> impl Responder {
    #[cfg(feature = "jemallocator")]
    {
        let ctl = jemalloc_pprof::PROF_CTL.as_ref();
        if ctl.is_none() {
            tracing::warn!("Get JemallocProfCtl failed");
            return HttpResponse::Forbidden().finish();
        }
        let mut prof_ctl = ctl.unwrap().lock().await;
        if !prof_ctl.activated() {
            tracing::warn!("jemalloc profiling is disabled and cannot be activated");
            return HttpResponse::Forbidden().finish();
        }

        let pprof = prof_ctl.dump_pprof();
        match pprof {
            Ok(pprof) => HttpResponse::Ok()
                .content_type("application/octet-stream")
                .body(pprof),
            Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
        }
    }
    #[cfg(not(feature = "jemallocator"))]
    {
        HttpResponse::Forbidden().finish()
    }
}

#[cfg(target_env = "msvc")]
#[get("/debug/pprof/heap")]
pub(super) async fn handle_get_heap() -> impl Responder {
    tracing::warn!("Not supported on Windows");
    HttpResponse::Forbidden().finish()
}
