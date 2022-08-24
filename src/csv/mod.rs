use anyhow::Result;
use futures::TryStreamExt;
use taos::{
    AsyncFetchable, AsyncQueryable, Dsn, Itertools, TBuilder, TaosBuilder,
};

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
        csv.write_record(
            row.into_value_iter()
                .map(|v| format!("{}", v))
                .collect_vec(),
        )
        .await?;
    }

    csv.flush().await?;

    Ok(())
}
