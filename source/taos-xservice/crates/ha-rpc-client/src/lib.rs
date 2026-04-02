use std::{collections::HashMap, time::Duration};

use arrow::array::RecordBatch;
use arrow_flight::{FlightClient, encode::FlightDataEncoderBuilder, error::FlightError};
use futures::{StreamExt, stream::FuturesUnordered};
use ha_core::{
    batch::{BatchIter, SCHEMA},
    jwt::xnoded::jwt_encode,
    types::{RpcClientType, XnodedId},
};
use snafu::ResultExt;
use tokio::sync::oneshot;
use tokio_util::{future::FutureExt, sync::CancellationToken};
use tonic::transport::Channel;
use tracing::Instrument;

use crate::{
    client::HaRpcClient,
    error::{
        AddHeaderSnafu, FlightSnafu, HandshakeSnafu, JwtSnafu, RequestCancelledSnafu, Result,
        TimeoutSnafu,
    },
};

static REQ_TIMEOUT: Duration = Duration::from_secs(60);

pub mod client;
pub mod error;

type FlightResult = std::result::Result<RecordBatch, FlightError>;
type RpcRequest = (
    RecordBatch,
    Option<(u64, oneshot::Sender<Result<RecordBatch>>)>,
);

pub struct ClientBuilder<'a> {
    channel: Channel,
    xnoded_id: Option<&'a XnodedId>,
    event_tx: flume::Sender<FlightResult>,
    cancel: CancellationToken,
    parallel: usize,
    client_type: RpcClientType,
}

impl<'a> ClientBuilder<'a> {
    pub fn new_xnoded_client(
        channel: Channel,
        xnoded_id: &'a XnodedId,
        event_tx: flume::Sender<FlightResult>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            channel,
            xnoded_id: Some(xnoded_id),
            event_tx,
            cancel,
            parallel: 100,
            client_type: RpcClientType::Xnoded,
        }
    }

    pub fn new_guest_client(
        channel: Channel,
        event_tx: flume::Sender<FlightResult>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            channel,
            xnoded_id: None,
            event_tx,
            cancel,
            parallel: 100,
            client_type: RpcClientType::Guest,
        }
    }

    pub fn parallel(mut self, parallel: usize) -> Self {
        self.parallel = parallel;
        self
    }

    pub async fn build(self) -> Result<HaRpcClient> {
        let Self {
            channel,
            xnoded_id,
            event_tx,
            cancel,
            parallel,
            client_type,
        } = self;
        // 用户发来的请求
        let (request_tx, request_rx) = flume::bounded::<RpcRequest>(1000);

        let mut flight_client = FlightClient::new(channel);
        flight_client
            .add_header("x-client-type", client_type.as_str())
            .context(AddHeaderSnafu)?;

        let token = match (client_type, xnoded_id) {
            (RpcClientType::Xnoded, Some(xnoded_id)) => jwt_encode(xnoded_id).context(JwtSnafu)?,
            (RpcClientType::Guest, _) => String::new(),
            (_, _) => unimplemented!("agent client type is not supported"),
        };
        flight_client
            .add_header("x-token", &token)
            .context(AddHeaderSnafu)?;

        flight_client
            .handshake(token)
            .await
            .context(HandshakeSnafu)?;

        tokio::spawn(async move {
            let mut futs = FuturesUnordered::new();
            let (message_tx, message_rx) = flume::bounded(1000);
            let mut flight_reqs =
                HashMap::<u64, oneshot::Sender<FlightResult>>::with_capacity(1000);

            let data = FlightDataEncoderBuilder::new()
                .with_schema(SCHEMA.clone())
                .build(message_rx.into_stream());
            let mut stream = match flight_client.do_exchange(data).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("rpc client do exchange error: {e:#}");
                    return;
                }
            };

            loop {
                tokio::select! {
                    biased;
                    res = futs.next(), if !futs.is_empty() => {
                        let Some(req_id) = res else {
                            break
                        };
                        flight_reqs.remove(&req_id);
                    }
                    _ = cancel.cancelled() => {
                        break
                    }
                    // 接收用户请求，发送给 server
                    res = request_rx.recv_async(), if flight_reqs.len() < parallel => {
                        let Ok((message, req_id_tx)) = res else {
                            break;
                        };
                        match cancel.run_until_cancelled(message_tx.send_async(Ok(message))).await {
                            Some(Ok(_)) => {},
                            Some(Err(_)) => {
                                tracing::error!("rpc client do exchange stream dropped");
                                break
                            }
                            None => break,
                        }
                        let Some((req_id, tx)) = req_id_tx else {
                            continue;
                        };
                        let (ack_tx, ack_rx) = oneshot::channel::<FlightResult>();
                        futs.push({
                            let cancel = cancel.clone();
                            async move {
                                let res = match ack_rx
                                    .timeout(REQ_TIMEOUT)
                                    .with_cancellation_token_owned(cancel.child_token())
                                    .await
                                {
                                    Some(Ok(Ok(res))) => res.context(FlightSnafu),
                                    Some(Ok(Err(_))) => unreachable!(),
                                    Some(Err(_)) => TimeoutSnafu.fail(),
                                    None => RequestCancelledSnafu.fail(),
                                };
                                if tx.send(res).is_err() {
                                    tracing::warn!("Request {req_id} dropped");
                                }
                                req_id
                            }
                        });
                        flight_reqs.insert(req_id, ack_tx);
                    }
                    // 接收 server 发来的消息
                    res = stream.next() => {
                        let Some(message) = res else {
                            break
                        };
                        let batch = match message {
                            Ok(batch) => batch,
                            Err(e) => match cancel.run_until_cancelled(event_tx.send_async(Err(e))).await {
                                Some(Ok(_)) => continue,
                                Some(Err(_)) => {
                                    tracing::warn!("rpc client event channel dropped");
                                    break
                                }
                                None => break,
                            }
                        };
                        let Ok(mut iter) = BatchIter::new(&batch) else {
                            continue;
                        };

                        let Some(record) = iter.next() else {
                            continue;
                        };

                        if !record.action.ends_with("_resp") {
                            let batch = record.try_into().map_err(FlightError::Arrow);
                            match cancel.run_until_cancelled(event_tx.send_async(batch)).await {
                                Some(Ok(_)) => continue,
                                Some(Err(_)) => {
                                    tracing::warn!("rpc client event channel dropped");
                                    break
                                }
                                None => break,
                            }
                        }
                        let req_id = record.req_id;
                        match flight_reqs.remove(&req_id) {
                            Some(ack_tx) => {
                                if ack_tx.send(Ok(batch)).is_err() {
                                    tracing::warn!("rpc client ack waiter {req_id} dropped");
                                }
                            },
                            None => {
                                tracing::warn!("rpc client ack waiter {req_id} removed");
                            },
                        }
                    }
                }
            }
        }.in_current_span());

        Ok(HaRpcClient::new(request_tx))
    }
}

pub async fn create_client(
    channel: Channel,
    xnoded_id: &XnodedId,
    event_tx: flume::Sender<FlightResult>,
    cancel: CancellationToken,
) -> Result<HaRpcClient> {
    ClientBuilder::new_xnoded_client(channel, xnoded_id, event_tx, cancel)
        .build()
        .await
}

pub async fn create_guest(
    channel: Channel,
    event_tx: flume::Sender<FlightResult>,
    cancel: CancellationToken,
) -> Result<HaRpcClient> {
    ClientBuilder::new_guest_client(channel, event_tx, cancel)
        .build()
        .await
}
