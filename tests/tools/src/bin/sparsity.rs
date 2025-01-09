use std::future;

use anyhow::Context;
use clap::Parser;
use futures::stream::FuturesUnordered;
use serde::Deserialize;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, StreamExt, TaosBuilder, TryStreamExt};

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(long)]
    host: String,
    #[arg(long)]
    database: String,
    #[arg(long)]
    stable: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args {
        host,
        database,
        stable,
    } = &Args::parse();

    let client = TaosBuilder::from_dsn(format!("taos+http://{host}:6041/{database}"))?.pool()?;

    let mut binding= client.get().await?.query(format!("select * from information_schema.ins_columns where db_name = '{database}' and table_name='{stable}' and col_name != 'ts';")).await?;
    let rows = binding.rows();

    #[derive(Deserialize)]
    struct Column {
        col_name: String,
    }
    let mut cols = rows.and_then(|mut row| future::ready(Column::deserialize(&mut row)));
    let total: usize = client
        .get()
        .await?
        .query_one(format!("select count(ts) from {database}.{stable}"))
        .await?
        .context("count result not found")?;
    println!("total: {total}");
    let mut total_non_ts_non_null = 0usize;
    let mut non_ts_col_num = 0;
    let mut futs = FuturesUnordered::new();
    while let Some(col) = cols.next().await.transpose()? {
        non_ts_col_num += 1;
        let client = client.get().await?;
        futs.push(async move {
            client
                .query_one::<_, usize>(format!(
                    "select count(ts) from {database}.{stable} where `{}` is not null",
                    &col.col_name
                ))
                .await
                .map(|v| v.map(|v| (col.col_name.clone(), v)))
        });
    }
    while let Some((name, count)) = futs.next().await.transpose()?.unwrap_or_default() {
        println!("{name} non null: {count}");
        total_non_ts_non_null += count;
    }

    let sparsity = total_non_ts_non_null as f64 / (total as f64 * non_ts_col_num as f64) * 100f64;
    println!("total non null / total = {sparsity}%");

    Ok(())
}
