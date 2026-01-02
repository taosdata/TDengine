#[derive(Debug)]
#[allow(dead_code)]
pub enum Signal {
    Interrupt,
    Terminate,
    Hangup,
    Quit,
}

impl std::fmt::Display for Signal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Signal::Interrupt => write!(f, "interrupt"),
            Signal::Terminate => write!(f, "terminate"),
            Signal::Hangup => write!(f, "hangup"),
            Signal::Quit => write!(f, "quit"),
        }
    }
}

#[cfg(unix)]
pub async fn wait_signal() -> std::io::Result<Signal> {
    use futures_ext::select::{Select4, select4};
    use tokio::signal::unix::{SignalKind, signal};
    match select4(
        signal(SignalKind::interrupt())?.recv(),
        signal(SignalKind::terminate())?.recv(),
        signal(SignalKind::hangup())?.recv(),
        signal(SignalKind::quit())?.recv(),
    )
    .await
    {
        Select4::T1(_) => Ok(Signal::Interrupt),
        Select4::T2(_) => Ok(Signal::Terminate),
        Select4::T3(_) => Ok(Signal::Hangup),
        Select4::T4(_) => Ok(Signal::Quit),
    }
}

#[cfg(not(unix))]
pub async fn wait_signal() -> std::io::Result<Signal> {
    tokio::signal::ctrl_c().await?;
    Ok(Signal::Interrupt)
}
