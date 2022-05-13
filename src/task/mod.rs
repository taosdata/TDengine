use linked_hash_map::LinkedHashMap;
use serde_json::Value;

///
// {
//   "source": [
//     "dsn1",
//     "dsn2"
//   ],
//   "transformer": {
//     "AddTag": {
//       "city": {
//         "fromOptions": "host"
//       }
//     },
//     "AlterTable": {
//       "prefix": "taosx_"
//     }
//   },
//   "sink": [
//     "sink1",
//     "sink2"
//   ]
// }
pub struct Task {
    source: Vec<Source>,
    transformer: Transformers,
    sink: Vec<Sink>,
}

pub struct Source {
    plugin: String,
    dsn: String,
    options: Value,
}

pub struct Sink {
    dsn: String,
    options: LinkedHashMap<String, String>,
}

impl Sink {
    pub fn plugin<T>(&self) -> Box<dyn SinkPlugin<Stream = T>> {
        unimplemented!()
    }
}

pub trait SinkPlugin {
    type Stream;

    fn accepted_content(&self) -> Vec<String>;

    fn consume(&mut self, stream: Self::Stream);
}

pub struct Transformers(LinkedHashMap<String, TransformerOptions>);

pub struct TransformerOptions(Value);

#[cfg(test)]
mod tests {
    use futures::sink::{self, SinkExt};
    use taos::prelude::*;

    #[taos::test]
    async fn sink_unfold(taos: &Taos, database: &str) -> anyhow::Result<()> {
        let unfold = sink::unfold(0, |mut sum, i: i32| async move {
            sum += i;
            eprintln!("sum: {sum}, i = {}", i);
            Ok::<_, futures::never::Never>(sum)
        });
        futures::pin_mut!(unfold);
        unfold.send(5).await?;
        unfold.send(5).await?;
        unfold.send(5).await?;
        Ok(())
    }
}
