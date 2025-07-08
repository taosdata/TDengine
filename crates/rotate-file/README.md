# rotate-file
Rotate-file is a rotating file writer that automatically rotates files when file size or time limit is reached. This lib forcus on managing files within a specific directory, and utilize the Sink trait from [futures](https://crates.io/crates/futures) to handle data, it is highly flexible, allowing anyone customize a sink operator for their specific file format.

## usage
```
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
```

feel free to visit examples/simple_sink.rs and examples/open_file_each_write.rs