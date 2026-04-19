use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use taosx_utils::taos_conn::TaosConn;

const SHOW_MNODES_EXIT_CODE: i32 = 1;
const SHOW_MNODES_CONNECTION_ERRNO: i32 = 0x000B;
const SHOW_MNODES_MAX_CONSECUTIVE_CONNECTION_ERRORS: usize = 5;

#[derive(Debug, serde::Deserialize)]
struct MNodeStatus {
    role: String,
    endpoint: String,
    role_time: DateTime<Utc>,
}

fn update_connection_error_streak(
    consecutive_connection_errors: &mut usize,
    err_code: Option<i32>,
) -> bool {
    if err_code == Some(SHOW_MNODES_CONNECTION_ERRNO) {
        *consecutive_connection_errors += 1;
        return *consecutive_connection_errors >= SHOW_MNODES_MAX_CONSECUTIVE_CONNECTION_ERRORS;
    }

    *consecutive_connection_errors = 0;
    false
}

#[instrument(skip_all)]
pub async fn start_monitor(
    dsn: String,
    leader_ep: String,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("start monitor");
    let _guard = cancel.drop_guard_ref();

    let conn = TaosConn::create(dsn, 3)
        .await
        .context("create db connection error")?;

    let mut role_time: Option<DateTime<Utc>> = None;
    let mut consecutive_connection_errors = 0usize;
    loop {
        match cancel
            .run_until_cancelled(conn.query::<MNodeStatus>("SHOW MNODES"))
            .await
        {
            Some(Ok(mnodes)) => {
                consecutive_connection_errors = 0;
                for status in mnodes {
                    if status.role_time == DateTime::UNIX_EPOCH {
                        continue;
                    }
                    if status.role != "leader" {
                        continue;
                    }
                    if status.endpoint != leader_ep {
                        tracing::error!(
                            "fetch leader endpoint {} not eq {}",
                            status.endpoint,
                            leader_ep
                        );
                        return Ok(());
                    }

                    let rt = role_time.get_or_insert(status.role_time);
                    if rt != &status.role_time {
                        tracing::error!(
                            "fetch leader role time {} not eq {}",
                            status.role_time,
                            rt,
                        );
                        return Ok(());
                    }
                    break;
                }
            }
            Some(Err(e)) => {
                let err_code = e.code().map(i32::from);
                if update_connection_error_streak(&mut consecutive_connection_errors, err_code) {
                    tracing::error!(
                        code = SHOW_MNODES_CONNECTION_ERRNO,
                        consecutive_connection_errors,
                        "exec SHOW MNODES hit connection error {} consecutive times, reached the limit ({}) and exiting",
                        consecutive_connection_errors,
                        SHOW_MNODES_MAX_CONSECUTIVE_CONNECTION_ERRORS,
                    );
                    std::process::exit(SHOW_MNODES_EXIT_CODE);
                }
                tracing::error!("exec `SHOW MNODES` error: {:#}", anyhow::Error::new(e))
            }
            None => break,
        }

        if cancel
            .run_until_cancelled(tokio::time::sleep(Duration::from_secs(60)))
            .await
            .is_none()
        {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SHOW_MNODES_CONNECTION_ERRNO, update_connection_error_streak};

    #[test]
    fn exits_after_five_consecutive_0x000b_errors() {
        let mut consecutive_connection_errors = 0;

        for _ in 0..4 {
            assert!(!update_connection_error_streak(
                &mut consecutive_connection_errors,
                Some(SHOW_MNODES_CONNECTION_ERRNO),
            ));
        }
        assert_eq!(consecutive_connection_errors, 4);

        assert!(update_connection_error_streak(
            &mut consecutive_connection_errors,
            Some(SHOW_MNODES_CONNECTION_ERRNO),
        ));
        assert_eq!(consecutive_connection_errors, 5);
    }

    #[test]
    fn resets_streak_after_non_0x000b_error() {
        let mut consecutive_connection_errors = 0;

        for _ in 0..3 {
            assert!(!update_connection_error_streak(
                &mut consecutive_connection_errors,
                Some(SHOW_MNODES_CONNECTION_ERRNO),
            ));
        }

        assert!(!update_connection_error_streak(
            &mut consecutive_connection_errors,
            Some(0x1234),
        ));
        assert_eq!(consecutive_connection_errors, 0);
    }

    #[test]
    fn resets_streak_after_success() {
        let mut consecutive_connection_errors = 0;

        for _ in 0..2 {
            assert!(!update_connection_error_streak(
                &mut consecutive_connection_errors,
                Some(SHOW_MNODES_CONNECTION_ERRNO),
            ));
        }

        assert!(!update_connection_error_streak(
            &mut consecutive_connection_errors,
            None,
        ));
        assert_eq!(consecutive_connection_errors, 0);
    }
}
