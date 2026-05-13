# taoslog

A logging toolbox used by taosdata services written in the Rust language.

## Usage

### TaosLayer

A [tracing_subscriber Layer](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/layer/trait.Layer.html) implementation，used when initializing the global tracing subscriber.

1. Customize a type that implements the `QidManager` trait.

```rust
use taoslog::QidManager;

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
use taoslog::writer::RollingFileAppender;

let appender = RollingFileAppender::builder("/var/log/taos", "taosx", 16)
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

### TaosRootSpanBuilder

A [RootSpanBuilder](https://docs.rs/tracing-actix-web/latest/tracing_actix_web/trait.RootSpanBuilder.html) implementation, used in the actix-web framework to generate a new tracing span when receiving a new HTTP request.

```rust
use tracing_actix_web::TracingLogger;
use taoslog::middleware::TaosRootSpanBuilder;

let server = HttpServer::new(move || {
    App::new()
        .wrap(Compat::new(TracingLogger::<TaosRootSpanBuilder>::new()))
})
.bind(addr)
.unwrap()
.run();
```

### Utils

```rust
let qid = Qid::from(12345);

// for tracing span
use taoslog::utils::Span;

tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
    Span.set_qid(qid);
    let qid: Qid = Span.get_qid().unwrap();
});

// for http header
let mut headers = HeaderMap::new();
headers.set_qid(qid.clone());
let qid: Qid = headers.get_qid().unwrap();

// for RecordBatch Schema
let mut schema = Schema::empty();
schema.set_qid(qid.clone());
let qid: Qid = schema.get_qid().unwrap();
```