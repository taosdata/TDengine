use itertools::Itertools;
use taos::{AsyncFetchable, AsyncQueryable, Code, TBuilder, TaosBuilder, TaosPool, Value};
use tokio::task::JoinHandle;

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
                code: Code::Failed,
                desc: err_str,
            }
        }
    }
}
impl RestBuilder {
    pub async fn query(&self, sql: &str) -> Result<RestOkResponse, RestErrResponse> {
        let conn = self.taos.get().map_err(|err| RestErrResponse {
            code: Code::Failed,
            desc: err.to_string(),
        })?;

        let mut set = conn.query(sql).await?;
        let column_meta = set
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), f.ty().to_string(), f.bytes()))
            .collect_vec();
        let data = set
            .to_records()?
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
        Ok(RestOkResponse {
            code: Code::Success,
            column_meta,
            rows: data.len() as _,
            data,
        })
    }
}

pub fn spawn_rest_service(
    pool: TaosPool,
    port: u16,
) -> anyhow::Result<JoinHandle<Result<(), std::io::Error>>> {
    use actix_web::*;
    let builder = RestBuilder { taos: pool };

    #[post("/sql")]
    async fn sql(rest: web::Data<RestBuilder>, sql: String) -> HttpResponse {
        match rest.query(&sql).await {
            Ok(ok) => HttpResponse::Ok().json(ok),
            Err(err) => HttpResponse::InternalServerError().json(err),
        }
    }
    #[get("/ping")]
    async fn ping() -> &'static str {
        "pong"
    }

    // This factory closure is called on each worker thread independently.
    let state = web::Data::new(builder);
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(sql)
            .service(ping)
    })
    .bind(&format!("127.0.0.1:{port}"))?
    .run();
    let h = tokio::spawn(async move { server.await });
    Ok(h)
}

#[tokio::test(flavor = "multi_thread")]
async fn service() -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn("taos:///").expect("connect").pool()?;
    // let rest = RestBuilder { taos };
    // let res = rest.query("show dddd").await;
    // dbg!(serde_json::to_string(&res));
    // dbg!(res);

    let handle = spawn_rest_service(taos, 6055)?;
    tokio::time::timeout(std::time::Duration::from_secs(50), handle).await???;
    Ok(())
}
