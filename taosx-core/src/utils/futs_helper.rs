use std::future::Future;

use tokio_util::sync::CancellationToken;

pub async fn select_cancel<F, T>(fut: F, cancel: &CancellationToken) -> Option<T>
where
    F: Future<Output = T>,
{
    if cancel.is_cancelled() {
        return None;
    }
    cancel.run_until_cancelled(fut).await
}

macro_rules! select_n {
    ($n:expr, $( $num: expr),+) => {
        paste::paste! {
            pub enum [<Select$n>]<$([<T$num>]),*> {
                $([<T$num>]([<T$num>])),+
            }

            pub async fn [<select$n>]<$([<F$num>], [<T$num>]),+>(
                $([<fut$num>]: [<F$num>]),+
            ) -> [<Select$n>]<$([<T$num>]),+>
            where
                $([<F$num>]: std::future::Future<Output = [<T$num>]>),+
            {
                tokio::select! {
                    $(
                        res = [<fut$num>] => [<Select$n>]::[<T$num>](res)
                    ),+
                }
            }
        }
    };
}

select_n!(2, 1, 2);
select_n!(3, 1, 2, 3);
