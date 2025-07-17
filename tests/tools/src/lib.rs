use rand::distributions::{Alphanumeric, DistString};

pub mod codec;
pub mod csv_reader;
pub mod custom_base;
pub mod fake_arrow;
pub mod fake_json;
pub mod fake_spb;
pub mod signal;
pub mod topic;
pub mod topic_fuzzy;

pub fn generate_random_string(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), length)
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
select_n!(4, 1, 2, 3, 4);
