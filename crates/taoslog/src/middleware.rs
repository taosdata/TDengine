use std::{borrow::Cow, marker::PhantomData};

use tracing_actix_web::{root_span, RootSpanBuilder};

use crate::{
    utils::{QidMetadataGetter, QidMetadataSetter},
    QidManager,
};

pub struct TaosRootSpanBuilder<Q>(PhantomData<Q>);

impl<Q> RootSpanBuilder for TaosRootSpanBuilder<Q>
where
    Q: QidManager,
{
    fn on_request_start(request: &actix_web::dev::ServiceRequest) -> tracing::Span {
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

        let mut span = root_span!(level = tracing::Level::INFO, request);
        // get qid from upstream header
        if let Some(qid) = request.headers().get_qid::<Q>() {
            span.set_qid(&qid);
        } else {
            span.set_qid(&Q::init_on_request(request));
        }
        span.in_scope(|| {
            tracing::info!("{client_ip} \"{method} {target} {schema}/{flavor}\" {user_agent}");
        });

        span
    }

    fn on_request_end<B: actix_web::body::MessageBody>(
        span: tracing::Span,
        outcome: &Result<actix_web::dev::ServiceResponse<B>, actix_web::error::Error>,
    ) {
        if let Ok(response) = outcome {
            let code = response.response().status().as_u16();
            let size = response.response().body().size();
            let request = response.request();
            let method = request.method().as_str();
            let target = request
                .uri()
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("");
            span.in_scope(|| {
                tracing::info!("\"{method} {target}\" status code: {code}, body: {size:?}");
            });
        }
    }
}

pub fn http_flavor(version: actix_web::http::Version) -> Cow<'static, str> {
    match version {
        actix_web::http::Version::HTTP_09 => "0.9".into(),
        actix_web::http::Version::HTTP_10 => "1.0".into(),
        actix_web::http::Version::HTTP_11 => "1.1".into(),
        actix_web::http::Version::HTTP_2 => "2.0".into(),
        actix_web::http::Version::HTTP_3 => "3.0".into(),
        other => format!("{other:?}").into(),
    }
}
