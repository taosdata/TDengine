use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use clap::Parser;
use orc_rust::ArrowWriterBuilder;
use taosx_tools::fake_arrow;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(long)]
    schema: PathBuf,
    #[arg(short, long)]
    output: PathBuf,
    #[arg(long, default_value = "1000")]
    batch_size: usize,
    #[arg(long)]
    total_batch: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let datafaker = fake_arrow::DataFaker::from_file(args.batch_size, args.schema)?;

    let output = File::create(&args.output)?;
    let schema = Arc::new(Schema::new(
        datafaker
            .get_schema()
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let mut writer = ArrowWriterBuilder::new(output, schema.clone())
        .with_batch_size(args.batch_size)
        .try_build()?;
    for _ in 0..args.total_batch {
        let batch = datafaker.rand_record_batch()?;
        let columns = batch
            .columns()
            .iter()
            .map(|array| match array.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, None) => {
                    let array = arrow::compute::cast(array, &DataType::Int64)?;
                    arrow::compute::cast(&array, &DataType::Utf8)
                }
                _ => arrow::compute::cast(array, &DataType::Utf8),
            })
            .collect::<Result<Vec<_>, ArrowError>>()
            .context("cast array type to utf8 error")?;
        let batch = RecordBatch::try_new(schema.clone(), columns).context("build batch error")?;
        writer.write(&batch).context("write batch error")?;
    }

    writer.close().context("flush and close orc file error")?;

    Ok(())
}
