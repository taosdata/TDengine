use std::borrow::Cow;

use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::Version;
use actix_web::Error;
use tracing::Span;
use tracing_actix_web::RootSpanBuilder;

use taosx_core::utils::trace::set_trace_id_for_current_span;

pub struct TaosXRootSpanBuilder;

#[inline]
pub fn http_flavor(version: Version) -> Cow<'static, str> {
    match version {
        Version::HTTP_09 => "0.9".into(),
        Version::HTTP_10 => "1.0".into(),
        Version::HTTP_11 => "1.1".into(),
        Version::HTTP_2 => "2.0".into(),
        Version::HTTP_3 => "3.0".into(),
        other => format!("{other:?}").into(),
    }
}

///
/// RootSpanBuilder for TracingLogger middleware.
///
impl RootSpanBuilder for TaosXRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> Span {
        let span = tracing::info_span!("http", TID = tracing::field::Empty);
        let trace_id = request
            .headers()
            .get("Trace-Id")
            .map(|h| h.to_str().unwrap_or(""))
            .unwrap_or("");

        span.in_scope(|| {
            if !trace_id.is_empty() {
                set_trace_id_for_current_span(trace_id);
            }
            let connection_info = request.connection_info();
            let schema = connection_info.scheme();
            let flavor = http_flavor(request.version());
            let user_agent = request
                .headers()
                .get("User-Agent")
                .map(|h| h.to_str().unwrap_or(""))
                .unwrap_or("");
            let client_ip = connection_info.realip_remote_addr().unwrap_or("");
            let method = request.method().as_str();
            let target = request
                .uri()
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("");
            tracing::info!("{client_ip} \"{method} {target} {schema}/{flavor}\" {user_agent}");
        });
        span
    }

    fn on_request_end<B: MessageBody>(span: Span, outcome: &Result<ServiceResponse<B>, Error>) {
        match &outcome {
            Ok(response) => {
                span.in_scope(|| {
                    let code = response.response().status().as_u16();
                    let size = response.response().body().size();
                    let request = response.request();
                    let method = request.method().as_str();
                    let target = request
                        .uri()
                        .path_and_query()
                        .map(|p| p.as_str())
                        .unwrap_or("");
                    tracing::info!("\"{method} {target}\" status code: {code}, body: {size:?}");
                });
            }
            Err(_error) => {
                // do nothing
            }
        }
    }
}
