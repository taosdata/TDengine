pub mod codec;
pub mod csv_reader;
pub mod custom_base;
pub mod fake_arrow;
pub mod fake_json;
pub mod topic;
pub mod topic_fuzzy;

macro_rules! define_select {
    ($n:expr, $( $num: expr),+) => {
        paste::paste! {
            pub enum [<Select$n>]<$([<T$num>]),*> {
                $([<T$num>]([<T$num>])),*
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

define_select!(2, 1, 2);
define_select!(3, 1, 2, 3);
