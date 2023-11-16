use crate::serve::task::{get_task_metrics_from_db, get_task_metrics_from_snapshot};

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
use tokio::{pin, time::interval};

use metrics_util::debugging::Snapshotter;
use taos::Code;
use tracing::instrument;

use crate::serve::{controller::TaskControllerRef, task::Failed};
use tokio::time::{sleep, Duration};

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
    let snapshotter = req.app_data::<Data<Snapshotter>>().unwrap();
    loop {
        match get_task_metrics_from_snapshot(snapshotter, task_store, task_id).await {
            Some(metrics) => {
                if let Err(Closed) = session.text(metrics).await {
                    tracing::info!("ws session closed");
                    break;
                }
            }
            None => {
                if let Some(metrics) = get_task_metrics_from_db(task_id) {
                    if let Err(Closed) = session.text(metrics).await {
                        tracing::info!("ws session closed");
                        break;
                    }
                } else {
                    let resson = Some(CloseReason {
                        code: CloseCode::Abnormal,
                        description: Some("no metrics found".to_string()),
                    });
                    let _ = session.close(resson).await;
                    break;
                }
            }
        };
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
    let task_id = i64::from_str_radix(task_id, 10);
    if let Err(err) = task_id {
        return Err(Error::from(Failed {
            code: Code::FAILED,
            message: format!("{:#}", err),
        }));
    }
    let task_id = task_id.unwrap();

    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
    // spawn websocket handler (and don't await it) so that the response is returned immediately
    rt::spawn(send_task_metrics_ws(task_id, req, session.clone()));
    rt::spawn(echo_heartbeat_ws(session.clone(), msg_stream));
    Ok(res)
}
