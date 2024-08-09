use itertools::Itertools;
use taos::{AsyncFetchable, AsyncQueryable, Code, TaosPool};
use tokio_util::sync::CancellationToken;

struct RestBuilder {
    taos: TaosPool,
}

#[derive(Debug, serde::Serialize)]
struct RestOkResponse {
    code: Code,
    column_meta: Vec<(String, String, u32)>,
    data: Vec<Vec<serde_json::Value>>,
    rows: u64,
}
#[derive(Debug, serde::Serialize)]
struct RestErrResponse {
    code: Code,
    desc: String,
}
impl From<taos::Error> for RestErrResponse {
    fn from(err: taos::Error) -> Self {
        let err_str = err.to_string();
        let parts = err_str.split_terminator(['[', ']']).collect_vec();
        // dbg!(parts);
        if parts.len() == 3 {
            let code = i32::from_str_radix(&parts[1][2..], 16).unwrap_or(0xFFFF);
            let desc = parts[2].to_string();

            RestErrResponse {
                code: Code::new(code),
                desc,
            }
        } else {
            RestErrResponse {
                code: Code::FAILED,
                desc: err_str,
            }
        }
    }
}
impl RestBuilder {
    pub async fn query(&self, sql: &str) -> Result<RestOkResponse, RestErrResponse> {
        tracing::info!("SQL: {sql}");
        let conn = self.taos.get().await.map_err(|err| RestErrResponse {
            code: Code::FAILED,
            desc: err.to_string(),
        })?;
        tracing::info!("Got connection, querying");
        let mut set = conn.query(sql).await?;
        let column_meta = set
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), f.ty().to_string(), f.bytes()))
            .collect_vec();
        tracing::info!("Got fields {column_meta:?}, fetching data.");
        let data = set
            .to_records()
            .await?
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        taos::Value::Timestamp(ts) => {
                            serde_json::Value::String(ts.to_datetime_with_tz().to_rfc3339())
                        }
                        _ => v.to_json_value(),
                    })
                    .collect_vec()
            })
            .collect_vec();
        tracing::info!("SQL result: {data:?}");
        Ok(RestOkResponse {
            code: Code::SUCCESS,
            column_meta,
            rows: data.len() as _,
            data,
        })
    }
}

#[allow(dead_code)]
pub fn spawn_rest_service(
    pool: TaosPool,
    port: u16,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    use actix_web::*;
    let builder = RestBuilder { taos: pool };

    #[post("/sql")]
    async fn sql(rest: web::Data<RestBuilder>, sql: String) -> HttpResponse {
        match rest.query(&sql).await {
            Ok(ok) => HttpResponse::Ok().json(ok),
            Err(err) => {
                tracing::info!(
                    "query sql error code :{}, message:{} ",
                    err.code.to_string(),
                    err.desc
                );
                HttpResponse::InternalServerError().json(err)
            }
        }
    }
    #[get("/ping")]
    async fn ping() -> &'static str {
        "pong"
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()?;
    // This factory closure is called on each worker thread independently.
    let state = web::Data::new(builder);
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(middleware::Logger::default())
            .service(sql)
            .service(ping)
    })
    .bind(&format!("127.0.0.1:{port}"))?
    .run();
    let _ = runtime.block_on(async move {
        tokio::select! {
            _ = server => {
                tracing::info!("Server stopped");
            },
            _ = cancellation.cancelled() => {
                tracing::info!("Server cancelled");
            }
        }
    });
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn service() -> anyhow::Result<()> {
    use taos::{AsyncTBuilder, TaosBuilder};
    let taos = TaosBuilder::from_dsn("taos+ws://localhost:6041/test")
        .expect("connect")
        .pool()?;

    let cancellation = CancellationToken::new();
    let cancellation2 = cancellation.clone();
    let thread = std::thread::spawn(move || spawn_rest_service(taos, 6055, cancellation2));

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        cancellation.cancelled().await;
    });
    thread.join().unwrap()?;
    Ok(())
}
