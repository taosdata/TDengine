# taos-log

A logging toolbox used written in the Rust language.

## Usage

### TaosLayer

A [tracing_subscriber Layer](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/layer/trait.Layer.html) implementation, used when initializing the global tracing subscriber.

1. Customize a type that implements the `QidManager` trait.

   ```rust
   use taos_log::QidManager;

   #[derive(Clone)]
   pub(crate) struct Qid(u64);

   impl QidManager for Qid {
       fn init() -> Self {
           Self(9223372036854775807)
       }

       fn get(&self) -> u64 {
           self.0
       }
   }

   impl From<u64> for Qid {
       fn from(value: u64) -> Self {
           Self(value)
       }
   }
   ```

2. Init the global subscriber

   ```rust
   use taos_log::writer::RollingFileAppender;

   let appender = RollingFileAppender::builder("/var/log/taos")
       .compress(true)
       .reserved_disk_size("1GB")
       .rotation_count(3)
       .rotation_size("1GB")
       .build()
       .unwrap();

   tracing_subscriber::registry()
       .with(TaosLayer::<Qid>::new(appender))
       .try_init()
       .unwrap();
   ```

### Utils

```rust
use taos_log::utils::Span;

let qid = Qid::from(1234567890);

tracing::info_span!("test").in_scope(|| {
    Span.set_qid(qid);
    let qid: Qid = Span.get_qid().unwrap();
});
```
