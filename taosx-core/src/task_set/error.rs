#[derive(thiserror::Error, Debug)]
#[error("kind:{step_kind}, detail:{source}")]
pub struct RunError {
    step_kind: ErrorStepKind,
    cause_kind: ErrorCauseKind,
    context: String,
    #[source]
    source: anyhow::Error,
}

#[derive(Debug, strum::Display, strum::AsRefStr)]
pub enum ErrorStepKind {
    /// Error is recoverable.
    Recoverable,
    /// Plain error.
    Plain,
    /// Error is fatal.
    Fatal,
}

#[derive(Debug, strum::Display, strum::AsRefStr)]
pub enum ErrorCauseKind {
    /// Error is from source.
    Source,
    /// Error is from transform module or ipc.
    Transform,
    /// Error is from sink.
    Sink,
    /// Error is from framework, taosx internally.
    Framework,
    /// Error is from system/os etc.
    System,
}
