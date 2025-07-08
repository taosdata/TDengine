# rotate-file
这是一个自动旋转文件写入器，当文件大小或时间限制到达时会自动旋转管理文件。该库关注于特定目录内的文件管理，利用 [futures](https://crates.io/crates/futures) 中的 Sink 特征处理数据，具有高度的灵活性，允许任何人为特定文件格式自定义一个 sink 运算符。

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

具体使用可以参考 examples/simple_sink.rs 和 examples/open_file_each_write.rs