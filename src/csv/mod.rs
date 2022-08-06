use anyhow::Result;
use futures::TryStreamExt;
use taos::{AsyncFetchable, AsyncQueryable, Dsn, TBuilder, Taos, TaosBuilder};

struct TableLike {
    taos: Taos,
    sql: String,
}

pub async fn query_to_csv(mut from: Dsn, to: Dsn) -> Result<()> {
    let sql = from.params.remove("query").unwrap();
    let taos = TaosBuilder::from_dsn(from)?.build()?;
    let mut rs = taos.query(sql).await?;
    let names = rs.filed_names();

    let file = to.fragment.expect("csv file not found");
    let file = tokio::fs::File::create(file).await?;
    let mut csv = csv_async::AsyncWriter::from_writer(file);

    csv.write_record(names).await?;

    let mut rows = rs.rows();

    while let Some(row) = rows.try_next().await? {
        // let values = row.into_values();
        csv.write_record(row.map(|(_, v)| format!("{}", v))).await?;
    }

    csv.flush().await?;

    Ok(())
}
