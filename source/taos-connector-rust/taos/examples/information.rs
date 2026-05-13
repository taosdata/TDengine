use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::formatted_timed_builder().init();
    println!(
        "start with {version}-{commit}",
        version = taos::build::PKG_VERSION,
        commit = taos::build::SHORT_COMMIT,
    );
    let dsn = "taos+ws://root:taosdata@";

    let pool = TaosBuilder::from_dsn(dsn)?.pool()?;
    tracing::trace!("start");

    let taos = pool.get().await?;
    tracing::trace!("got connection");

    let mut result = taos
        .query("select * from information_schema.ins_databases")
        .await?;

    for field in result.fields() {
        println!("got field: {}", field.name());
    }

    // Query option 1, use rows stream.
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
    Ok(())
}
