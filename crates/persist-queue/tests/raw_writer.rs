use std::{collections::BTreeMap, path::Path};

use bytes::Bytes;
use persist_queue::{
    RawWriter,
    fs::{FsQueue, writer},
};
use tempfile::tempdir;

mod common;

#[tokio::test]
async fn multi_writer() -> anyhow::Result<()> {
    let dir = tempdir()?;
    async fn build_writer(dir: &Path) -> anyhow::Result<writer::Writer<Bytes>> {
        let mut queue = FsQueue::builder(dir).build().await?;
        Ok(queue.new_writer().await?)
    }

    let _writer = build_writer(dir.path()).await?;
    assert!(build_writer(dir.path()).await.is_err());

    Ok(())
}

#[tokio::test]
async fn write_no_rotate() -> anyhow::Result<()> {
    let dir = tempdir()?;

    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    let mut writer = queue.new_writer().await?;
    assert!(queue.new_writer::<Bytes>().await.is_err());

    // 写入一条
    writer
        .write(vec![Bytes::from_static("hello, world!".as_bytes())])
        .await?;
    writer.sync_data().await?;

    assert_eq!(
        queue.segments(),
        BTreeMap::from([(0, dir.path().join("00000000000000000000.seg"))])
    );
    let len = tokio::fs::metadata(dir.path().join("00000000000000000000.seg"))
        .await?
        .len();
    assert_eq!(len, 18);

    // 写入多条，刚好超出 segment_size，不会触发文件滚动
    writer.write(common::payloads(5)).await?;
    writer.sync_data().await?;

    assert_eq!(
        queue.segments(),
        BTreeMap::from([(0, dir.path().join("00000000000000000000.seg")),])
    );
    let len = tokio::fs::metadata(dir.path().join("00000000000000000000.seg"))
        .await?
        .len();
    assert_eq!(len, 108);

    Ok(())
}

#[tokio::test]
async fn write_rotate() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    let mut writer = queue.new_writer().await?;
    assert!(queue.new_writer::<Bytes>().await.is_err());
    // 写入多条，触发文件滚动
    writer.write(common::payloads(10)).await?;
    writer.sync_data().await?;

    assert_eq!(
        queue.segments(),
        BTreeMap::from([
            (0, dir.path().join("00000000000000000000.seg")),
            (1, dir.path().join("00000000000000000001.seg"))
        ])
    );
    let len = tokio::fs::metadata(dir.path().join("00000000000000000000.seg"))
        .await?
        .len();
    assert_eq!(len, 108);
    let len = tokio::fs::metadata(dir.path().join("00000000000000000001.seg"))
        .await?
        .len();
    assert_eq!(len, 72);
    Ok(())
}
