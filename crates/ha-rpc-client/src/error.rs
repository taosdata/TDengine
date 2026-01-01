use arrow::error::ArrowError;
use arrow_flight::error::FlightError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Flight error"))]
    Flight { source: FlightError },
    #[snafu(display("Timeout"))]
    Timeout,
    #[snafu(display("Request cancelled"))]
    RequestCancelled,
    #[snafu(display("Handshake error"))]
    Handshake { source: FlightError },
    #[snafu(display("JWT error"))]
    Jwt { source: jsonwebtoken::errors::Error },
    #[snafu(display("Add header error"))]
    AddHeader { source: FlightError },
    #[snafu(display("Serialize request error"))]
    SerializeReq { source: serde_json::Error },
    #[snafu(display("Deserialize response error"))]
    DeserializeResp { source: serde_json::Error },
    #[snafu(display("Build request batch error"))]
    BuildReqBatch { source: ArrowError },
    #[snafu(display("Build batch iterator error"))]
    BuildBatchIter { source: anyhow::Error },
    #[snafu(display("Response no context"))]
    ResponseNoContext,
    #[snafu(display("Event loop dropped"))]
    EventLoopDropped,
    #[snafu(display("Ack waiter dropped unexpectedly"))]
    AckWaiterDroppedUnexpectedly,
    #[snafu(display("Do exchange error"))]
    DoExchange { source: FlightError },
    #[snafu(display("Response fail: {error}"))]
    ResponseFail { error: String },
}
