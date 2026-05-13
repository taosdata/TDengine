use actix_web::{
    HttpResponse, Responder,
    web::{Json, Query},
};
use serde::Deserialize;

use super::Error;
use taosx_core::plugins::transform::sample::DsSamples;

type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize, Debug)]
pub struct TzQuery {
    /// Timezone name, e.g. "Asia/Shanghai"
    tz: Option<String>,
}

/// Flat stream transform sample data simulation
pub async fn sample_flat(data: Json<DsSamples>, tz: Query<TzQuery>) -> Result<impl Responder> {
    let output = match data.into_inner() {
        DsSamples::Simple(sample_in) => sample_in.transform(tz.tz.as_deref())?,
        DsSamples::MultiSchema(samples) => samples.transform(tz.tz.as_deref())?,
    };

    Ok(HttpResponse::Ok().json(output))
}

/// Stable model preview
pub async fn stable_preview(data: Json<DsSamples>) -> Result<impl Responder> {
    let response = match data.into_inner() {
        DsSamples::Simple(sample_in) => {
            let output = sample_in.stable_preview()?;
            HttpResponse::Ok().json(output)
        }
        DsSamples::MultiSchema(sample_in) => {
            let output = sample_in.stable_preview()?;
            HttpResponse::Ok().json(output)
        }
    };

    Ok(response)
}
