use std::future::Future;

use futures::future::Either;
use tokio_util::sync::CancellationToken;

pub async fn select_two<F1, F2, L, R>(fut1: F1, fut2: F2) -> Either<L, R>
where
    F1: Future<Output = L>,
    F2: Future<Output = R>,
{
    tokio::select! {
        res = fut1 => Either::Left(res),
        res = fut2 => Either::Right(res),
    }
}

pub async fn select_cancel<F, T>(fut: F, cancel: &CancellationToken) -> Option<T>
where
    F: Future<Output = T>,
{
    match select_two(fut, cancel.cancelled()).await {
        Either::Left(res) => Some(res),
        Either::Right(_) => None,
    }
}
