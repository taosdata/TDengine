use std::sync::Arc;

use actix_web::{
    rt,
    web::{Data, Payload},
    Error, HttpRequest, HttpResponse,
};
use actix_ws::{CloseCode, CloseReason, Session};
use actix_ws::{Closed, Message};
use futures_util::{
    future::{self, Either},
    StreamExt as _,
};
use taosx_core::core_metrics::CoreMetrics;
use tokio::{pin, time::interval};
use tracing::instrument;

use crate::serve::{controller::TaskControllerRef, Failed};
use tokio::time::{sleep, Duration};

use super::get_task_metrics_string;
use super::try_get_metrics_from_task_detail;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(70);
const SEND_METRICS_INTERVAL: Duration = Duration::from_secs(2);

pub async fn echo_heartbeat_ws(
    mut session: actix_ws::Session,
    mut msg_stream: actix_ws::MessageStream,
) {
    tracing::info!("connected");

    let mut last_heartbeat = std::time::Instant::now();
    let mut interval = interval(HEARTBEAT_INTERVAL);

    let reason = loop {
        // create "next client timeout check" future
        let tick = interval.tick();
        // required for select()
        pin!(tick);

        // waits for either `msg_stream` to receive a message from the client or the heartbeat
        // interval timer to tick, yielding the value of whichever one is ready first
        match future::select(msg_stream.next(), tick).await {
            // received message from WebSocket client
            Either::Left((Some(Ok(msg)), _)) => {
                tracing::info!("msg: {msg:?}");

                match msg {
                    Message::Text(text) => {
                        session.text(text).await.unwrap();
                    }

                    Message::Binary(bin) => {
                        session.binary(bin).await.unwrap();
                    }

                    Message::Close(reason) => {
                        break reason;
                    }

                    Message::Ping(bytes) => {
                        last_heartbeat = std::time::Instant::now();
                        let _ = session.pong(&bytes).await;
                    }

                    Message::Pong(_) => {
                        last_heartbeat = std::time::Instant::now();
                    }

                    Message::Continuation(_) => {
                        tracing::warn!("no support for continuation frames");
                    }

                    // no-op; ignore
                    Message::Nop => {}
                };
            }

            // client WebSocket stream error
            Either::Left((Some(Err(err)), _)) => {
                tracing::error!("{}", err);
                break None;
            }

            // client WebSocket stream ended
            Either::Left((None, _)) => break None,

            // heartbeat interval ticked
            Either::Right((_inst, _)) => {
                // if no heartbeat ping/pong received recently, close the connection
                if std::time::Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
                    tracing::info!(
                        "client has not sent heartbeat in over {CLIENT_TIMEOUT:?}; disconnecting"
                    );

                    break None;
                }

                // send heartbeat ping
                let _ = session.ping(b"").await;
            }
        }
    };

    // attempt to close connection gracefully
    let _ = session.close(reason).await;

    tracing::info!("disconnected");
}

async fn send_task_metrics_ws(task_id: i64, req: HttpRequest, mut session: Session) {
    let task_store = req.app_data::<Data<TaskControllerRef>>().unwrap();
    let get_task_result = task_store.get(task_id).await;
    if let Err(err) = get_task_result {
        let resson = Some(CloseReason {
            code: CloseCode::Abnormal,
            description: Some(format!("{:#}", err)),
        });
        let _ = session.close(resson).await;
        return;
    }
    let task = get_task_result.unwrap();
    if task.is_none() {
        let resson = Some(CloseReason {
            code: CloseCode::Abnormal,
            description: Some(format!("task {} not found", task_id)),
        });
        tracing::info!("close session since task not found");
        let _ = session.close(resson).await;
        return;
    }
    let task = task.unwrap();
    let mut metrics_opt: Option<Arc<CoreMetrics>> = None;
    while metrics_opt.is_none() {
        metrics_opt = try_get_metrics_from_task_detail(&task).await;
        sleep(SEND_METRICS_INTERVAL).await;
    }
    let metrics = metrics_opt.unwrap();

    loop {
        let get_task_result = task_store.get(task_id).await;
        if get_task_result.is_err() {
            let _ = session
                .close(Some(CloseReason {
                    code: CloseCode::Abnormal,
                    description: Some(format!("{:#}", get_task_result.unwrap_err())),
                }))
                .await;
            break;
        }
        let task = get_task_result.unwrap();
        if task.is_none() {
            let resson = Some(CloseReason {
                code: CloseCode::Normal,
                description: Some(format!("task {} not found", task_id)),
            });
            tracing::info!("close session since task not found");
            let _ = session.close(resson).await;
            break;
        }
        let task = task.unwrap();
        let status = task.status();
        tracing::trace!("task status: {:?}", status);
        let metrics_string = get_task_metrics_string(status, metrics.clone());
        if let Err(Closed) = session.text(metrics_string).await {
            tracing::info!("ws session closed");
            break;
        }
        sleep(SEND_METRICS_INTERVAL).await;
    }
}

#[instrument(skip_all)]
pub(crate) async fn send_task_metrics(
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    let match_info = req.match_info();
    let task_id = match_info.get("task_id").unwrap();
    let task_id = task_id.parse::<i64>();
    if let Err(err) = task_id {
        return Err(Error::from(Failed::from_error(err)));
    }
    let task_id = task_id.unwrap();

    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
    // spawn websocket handler (and don't await it) so that the response is returned immediately
    rt::spawn(send_task_metrics_ws(task_id, req, session.clone()));
    rt::spawn(echo_heartbeat_ws(session.clone(), msg_stream));
    Ok(res)
}
