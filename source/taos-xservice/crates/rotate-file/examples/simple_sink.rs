use faststr::FastStr;
use futures::sink;
use rotate_file::{RotateWriterBuilder, SinkFn, utils::time_unit_dt_fmt};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cache_writer = RotateWriterBuilder::new()
        .id(999)
        .dir("/tmp/test/cache")
        .prefix("cache")
        .file_dt_fmt(time_unit_dt_fmt("m")?)
        .rotate_count(5)
        .max_size_value(2)
        .max_size_unit("MB")
        .keep_time_value(10)
        .keep_time_unit("m")
        .gen_sink(Box::new(
            |file_path: PathBuf| -> Result<SinkFn<FastStr, std::io::Error>, anyhow::Error> {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&file_path)
                    .map_err(|e| anyhow::anyhow!("open file error: {:?}", e))?;

                let sink = sink::unfold(file, |mut file: File, line: FastStr| async move {
                    file.write_all(line.as_bytes())?;
                    file.flush()?;
                    Ok(file)
                });
                Ok(Box::pin(sink))
            },
        ))
        .build()?;

    for i in 0..100 {
        cache_writer.write(format!("test {}\n", i).into()).await?;
    }
    Ok(())
}
