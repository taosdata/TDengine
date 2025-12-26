pub mod common;

use persist_queue::{
    RawReader,
    fs::{EntryPosition, FsQueue, ReadFrom},
};
use tempfile::tempdir;

#[tokio::test]
async fn read_empty() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let queue = FsQueue::builder(dir.path()).build().await?;

    let mut reader = queue.new_reader(ReadFrom::Earliest).await?;
    let entries = reader.read(10).await?;
    assert!(entries.is_empty());

    let mut reader = queue.new_reader(ReadFrom::Latest).await?;
    let entries = reader.read(10).await?;
    assert!(entries.is_empty());

    let mut reader = queue
        .new_reader(ReadFrom::LastPosition(EntryPosition::new(1, 19)))
        .await?;
    let entries = reader.read(10).await?;
    assert!(entries.is_empty());
    Ok(())
}

#[tokio::test]
async fn read_vacuum() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let queue = FsQueue::builder(dir.path()).build().await?;

    for path in ["00000000000000000000.seg", "00000000000000000001.seg"].map(|v| dir.path().join(v))
    {
        tokio::fs::File::create(path).await?;
    }

    // 创建两个进度不同的 reader
    let mut reader_0 = queue.new_reader(ReadFrom::Earliest).await?;
    let mut reader_1 = queue.new_reader(ReadFrom::Latest).await?;

    // reader_0 执行 vacuum，不会删除任何文件
    reader_0.vacuum().await?;
    assert!(tokio::fs::try_exists(dir.path().join("00000000000000000000.seg")).await?);
    assert!(tokio::fs::try_exists(dir.path().join("00000000000000000001.seg")).await?);

    // raeder_1 执行 vacuum，因为 reader_0 存在，所以不会删除
    reader_1.vacuum().await?;
    assert!(tokio::fs::try_exists(dir.path().join("00000000000000000000.seg")).await?);
    assert!(tokio::fs::try_exists(dir.path().join("00000000000000000001.seg")).await?);

    // reader_0 退出，reader_1 可以删除 segment 0
    drop(reader_0);
    reader_1.vacuum().await?;
    assert!(!tokio::fs::try_exists(dir.path().join("00000000000000000000.seg")).await?);
    assert!(tokio::fs::try_exists(dir.path().join("00000000000000000001.seg")).await?);
    Ok(())
}
