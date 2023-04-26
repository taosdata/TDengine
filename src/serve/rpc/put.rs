use arrow_flight::{FlightData, PutResult};
use futures::{Stream, TryStreamExt};
use tonic::{Status, Streaming};

use crate::serve::controller::{Task, TaskControllerRef};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
}

impl PutStream {
    pub(super) fn new(controller: TaskControllerRef, task_id: i64, req: Streaming<FlightData>) -> Self {
        Self {
            req,
            controller,
            task_id,
        }
    }
    pub async fn into_flight_put_result(
        self,
    ) -> impl Stream<Item = Result<PutResult, Status>> + std::marker::Send {
        // todo: directly use task detail instead of id.
        let task = self
            .controller
            .get(self.task_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))
            .unwrap()
            .unwrap();

        self.req.map_ok(|message| {
            let app_metadata = message.app_metadata;
            PutResult { app_metadata }
        })
    }
}

unsafe impl Sync for PutStream {}
unsafe impl Send for PutStream {}
