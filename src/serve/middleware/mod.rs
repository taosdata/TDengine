use std::fmt::{Debug, Display};

use actix_web::{Error, ResponseError};
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::StatusCode;
use tracing::field::Empty;
use tracing::Span;
use tracing_actix_web::RootSpanBuilder;

pub struct TaosXRootSpanBuilder;

///
/// RootSpanBuilder for TracingLogger middleware.
impl RootSpanBuilder for TaosXRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> Span {
        let span = tracing::info_span!("TEST_ROOT_SPAN",
            http.status_code=Empty,
            exception.message=Empty,
            exception.details=Empty,

        );
        span.in_scope(|| {
            tracing::info!("get request: {}", request.method());
        });
        span
    }

    fn on_request_end<B: MessageBody>(span: Span, outcome: &Result<ServiceResponse<B>, Error>) {
        match &outcome {
            Ok(response) => {
                if let Some(error) = response.response().error() {
                    // use the status code already constructed for the outgoing HTTP response
                    record_error_info(span, response.status(), error.as_response_error());
                } else {
                    let code: i32 = response.response().status().as_u16().into();
                    span.record("http.status_code", code);
                }
            }
            Err(error) => {
                let response_error = error.as_response_error();
                record_error_info(span, response_error.status_code(), response_error);
            }
        };
    }
}

fn record_error_info(span: Span, status_code: StatusCode, response_error: &dyn ResponseError) {
    // pre-formatting errors is a workaround for https://github.com/tokio-rs/tracing/issues/1565
    let display = format!("{response_error}");
    let debug = format!("{response_error:?}");
    span.record("exception.message", &tracing::field::display(display));
    span.record("exception.details", &tracing::field::display(debug));
    let code: i32 = status_code.as_u16().into();
    span.record("http.status_code", code);
}