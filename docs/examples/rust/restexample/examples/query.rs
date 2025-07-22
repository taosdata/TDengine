use chrono::DateTime;
use chrono::Local;
use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
    taos.exec_many([
        "drop database if exists power",
        "create database power",
        "use power",
        "create table meters (ts timestamp, current float, voltage int, phase float) tags (groupid int, location varchar(64))",
        "insert into d0 using meters tags(2, 'California.SanFrancisco') values (now, 10.3, 219, 0.31)",
        "insert into d1 using meters tags(3, 'California.SanFrancisco') values (now, 12.6, 218, 0.33)",
    ])
    .await?;

    // ANCHOR: query_data
    // query data, make sure the database and table are created before
    let sql = "SELECT ts, current, location FROM power.meters limit 100";
    match taos.query(sql).await {
        Ok(mut result) => {
            for field in result.fields() {
                println!("got field: {}", field.name());
            }

            let mut rows = result.rows();
            let mut nrows = 0;
            while let Some(row) = rows.try_next().await? {
                for (col, (name, value)) in row.enumerate() {
                    println!(
                        "[{}] got value in col {} (named `{:>8}`): {}",
                        nrows, col, name, value
                    );
                }
                nrows += 1;
            }
        }
        Err(err) => {
            eprintln!(
                "Failed to query data from power.meters, sql: {}, ErrMessage: {}",
                sql, err
            );
            return Err(err.into());
        }
    }
    // ANCHOR_END: query_data

    // ANCHOR: query_data_2
    // query data, make sure the database and table are created before
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        // deserialize timestamp to chrono::DateTime<Local>
        ts: DateTime<Local>,
        // float to f32
        current: Option<f32>,
        // binary/varchar to String
        location: String,
    }

    let sql = "SELECT ts, current, location FROM power.meters limit 100";
    match taos
        .query("SELECT ts, current, location FROM power.meters limit 100")
        .await
    {
        Ok(mut query) => match query.deserialize::<Record>().try_collect::<Vec<_>>().await {
            Ok(records) => {
                dbg!(records);
            }
            Err(err) => {
                eprintln!("Failed to deserialize query results; ErrMessage: {}", err);
                return Err(err.into());
            }
        },
        Err(err) => {
            eprintln!(
                "Failed to query data from power.meters, sql: {}, ErrMessage: {}",
                sql, err
            );
            return Err(err.into());
        }
    }
    // ANCHOR_END: query_data_2

    // ANCHOR: query_with_req_id
    let req_id: u64 = 3;
    match taos
        .query_with_req_id(
            "SELECT ts, current, location FROM power.meters limit 1",
            req_id,
        )
        .await
    {
        Ok(mut result) => {
            for field in result.fields() {
                println!("got field: {}", field.name());
            }

            let mut rows = result.rows();
            let mut nrows = 0;
            while let Some(row) = rows.try_next().await? {
                for (col, (name, value)) in row.enumerate() {
                    println!(
                        "[{}] got value in col {} (named `{:>8}`): {}",
                        nrows, col, name, value
                    );
                }
                nrows += 1;
            }
        }
        Err(err) => {
            eprintln!(
                "Failed to execute sql with reqId: {}, ErrMessage: {}",
                req_id, err
            );
            return Err(err.into());
        }
    }
    // ANCHOR_END: query_with_req_id

    Ok(())
}
