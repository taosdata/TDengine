use std::time::Duration;

use bytes::Bytes;
use persist_queue::{
    RawReader, RawWriter,
    fs::{EntryPosition, FsQueue, ReadFrom},
};
use tempfile::tempdir;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

mod common;

#[tokio::test]
async fn rw_rotate_earliest() -> anyhow::Result<()> {
    let dir = tempdir()?;

    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    // 写入 6 条，此时还没有触发文件滚动
    let mut writer = queue.new_writer().await?;
    writer.write(common::payloads(6)).await?;
    writer.sync_data().await?;

    let mut reader = queue.new_reader(ReadFrom::Earliest).await?;
    let entries = reader.read(10).await?;
    assert_eq!(entries.len(), 6);
    for (id, entry) in entries.into_iter().enumerate() {
        assert_eq!(
            entry.position,
            EntryPosition::new(0, ((id + 1) * 18) as u64)
        );
        assert_eq!(
            entry.payload,
            Bytes::from_static("hello, world!".as_bytes())
        );
    }

    // 再写五条，触发文件滚动
    writer.write(common::payloads(5)).await?;
    writer.sync_data().await?;

    let entries = reader.read(10).await?;
    assert_eq!(entries.len(), 5);
    for (id, entry) in entries.into_iter().enumerate() {
        assert_eq!(
            entry.position,
            EntryPosition::new(1, ((id + 1) * 18) as u64)
        );
        assert_eq!(
            entry.payload,
            Bytes::from_static("hello, world!".as_bytes())
        );
    }

    Ok(())
}

#[tokio::test]
async fn rw_rotate_latest() -> anyhow::Result<()> {
    let dir = tempdir()?;

    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    // 写入 6 条，此时还没有触发文件滚动
    let mut writer = queue.new_writer().await?;
    writer.write(common::payloads(3)).await?;
    writer.sync_data().await?;

    let mut reader = queue.new_reader(ReadFrom::Latest).await?;
    let entries = reader.read(3).await?;
    assert_eq!(entries.len(), 0);

    // 再写五条，触发文件滚动
    writer.write(common::payloads(5)).await?;
    writer.sync_data().await?;

    let entries = reader.read(20).await?;
    assert_eq!(entries.len(), 5);
    for (id, entry) in entries.into_iter().enumerate() {
        if id <= 2 {
            assert_eq!(
                entry.position,
                EntryPosition::new(0, ((id + 4) * 18) as u64)
            );
        } else {
            assert_eq!(
                entry.position,
                EntryPosition::new(1, ((id - 2) * 18) as u64)
            );
        }

        assert_eq!(
            entry.payload,
            Bytes::from_static("hello, world!".as_bytes())
        );
    }
    Ok(())
}

#[tokio::test]
async fn rw_rotate_last_position() -> anyhow::Result<()> {
    let dir = tempdir()?;

    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    // 写入 6 条，此时还没有触发文件滚动
    let mut writer = queue.new_writer().await?;
    writer.write(common::payloads(10)).await?;
    writer.sync_data().await?;

    // 从第 3 条开始读
    let mut reader = queue
        .new_reader(ReadFrom::LastPosition(EntryPosition::new(0, 36)))
        .await?;
    let entries = reader.read(20).await?;
    assert_eq!(entries.len(), 8);
    for (id, entry) in entries.into_iter().enumerate() {
        if id <= 3 {
            assert_eq!(
                entry.position,
                EntryPosition::new(0, ((id + 3) * 18) as u64)
            );
        } else {
            assert_eq!(
                entry.position,
                EntryPosition::new(1, ((id - 3) * 18) as u64)
            );
        }

        assert_eq!(
            entry.payload,
            Bytes::from_static("hello, world!".as_bytes())
        );
    }
    Ok(())
}

#[tokio::test]
async fn rw_rotate_last_position_not_found_write_first() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    // 写入 3 条，生成一个 segment 0
    let mut writer = queue.new_writer().await?;
    writer.write(common::payloads(3)).await?;
    writer.sync_data().await?;

    // 尝试读取 segment 1，会找不到，读到空
    // 初始化时目录中最大是 segment 0，而我们在这里要求从 segment 2 开始读，所以初始化会重置为从 segment 0 的下一个，即 segment 0 开始读
    let mut reader = queue
        .new_reader(ReadFrom::LastPosition(EntryPosition::new(2, 36)))
        .await?;
    // 读两遍
    let entries = reader.read(10).await?;
    assert!(entries.is_empty());
    let entries = reader.read(10).await?;
    assert!(entries.is_empty());

    // 写入多条数据，生成 segment 1
    writer.write(common::payloads(8)).await?;
    writer.sync_data().await?;

    // 再次读，在 segment 2 中读到 6 条数据，在 segment 3 读到 1 条
    let entries = reader.read(10).await?;
    assert_eq!(entries.len(), 5);
    for (id, entry) in entries.into_iter().enumerate() {
        assert_eq!(
            entry.position,
            EntryPosition::new(1, ((id + 1) * 18) as u64)
        );

        assert_eq!(
            entry.payload,
            Bytes::from_static("hello, world!".as_bytes())
        );
    }
    Ok(())
}

#[tokio::test]
async fn rw_rotate_last_position_not_found_read_first() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    // 尝试读取 segment 1，会找不到，当前目录中没有文件，则重置为从 Earliest 开始读
    // 初始化时目录中最大是 segment 0，而我们在这里要求从 segment 2 开始读，所以初始化会重置为从 segment 0 的下一个，即 segment 0 开始读
    let mut reader = queue
        .new_reader(ReadFrom::LastPosition(EntryPosition::new(2, 36)))
        .await?;

    // 读两遍
    let entries = reader.read(10).await?;
    assert!(entries.is_empty());
    let entries = reader.read(10).await?;
    assert!(entries.is_empty());

    // 写入 3 条，生成一个 segment 0
    let mut writer = queue.new_writer().await?;
    writer.write(common::payloads(3)).await?;
    writer.sync_data().await?;

    // 再次读，在 segment 2 中读到 6 条数据，在 segment 3 读到 1 条
    let entries = reader.read(10).await?;
    assert_eq!(entries.len(), 3);
    for (id, entry) in entries.into_iter().enumerate() {
        assert_eq!(
            entry.position,
            EntryPosition::new(0, ((id + 1) * 18) as u64)
        );

        assert_eq!(
            entry.payload,
            Bytes::from_static("hello, world!".as_bytes())
        );
    }
    Ok(())
}

#[tokio::test]
async fn bad_checksum_rotate() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut queue = FsQueue::builder(dir.path()).build().await?;

    let mut writer = queue.new_writer().await?;
    writer.write(common::payloads(5)).await?;
    writer.sync_data().await?;

    let mut reader = queue.new_reader(ReadFrom::Earliest).await?;
    let entries = reader.read(5).await?;
    assert_eq!(entries.len(), 5);

    // 破坏文件
    let mut file = tokio::fs::File::options()
        .write(true)
        .open(dir.path().join("00000000000000000000.seg"))
        .await?;
    file.seek(std::io::SeekFrom::End(0)).await?;
    file.write_all(&"abcd".as_bytes().repeat(30)).await?;
    file.sync_all().await?;

    // 再次读，会报错
    assert!(reader.read(10).await.is_err());
    Ok(())
}

#[tokio::test]
async fn rw_timeout_read() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let cancel = CancellationToken::new();

    let mut queue = FsQueue::builder(dir.path()).build().await?;

    let mut reader = queue.new_reader(ReadFrom::Earliest).await?;

    // 当前目录下没有 segment，等待超时
    let entries = reader
        .read_util(1, 1, Some(Duration::from_secs(1)), &cancel)
        .await?;
    assert!(entries.is_empty());

    // 写入部分数据
    let mut writer = queue.new_writer().await?;
    writer.write(common::payloads(5)).await?;
    writer.sync_data().await?;

    // 现在可以读取到数据，但直到超时也只读取到 5 条
    let entries = reader
        .read_util(5, 10, Some(Duration::from_secs(1)), &cancel)
        .await?;
    assert_eq!(entries.len(), 5);

    // 同时读写
    tokio::try_join!(
        async {
            writer.write(common::payloads(5)).await?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            writer.write(common::payloads(7)).await?;
            writer.sync_data().await?;
            anyhow::Ok(())
        },
        async {
            let entries = reader
                .read_util(12, 20, Some(Duration::from_secs(3)), &cancel)
                .await?;
            assert_eq!(entries.len(), 12);
            anyhow::Ok(())
        }
    )?;
    Ok(())
}

#[tokio::test]
async fn rw_timeout_rotate_read() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let cancel = CancellationToken::new();

    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;

    let mut reader = queue.new_reader(ReadFrom::Earliest).await?;
    let mut writer = queue.new_writer().await?;

    tokio::try_join!(
        async {
            writer.write(common::payloads(10)).await?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            writer.write(common::payloads(10)).await?;
            writer.sync_data().await?;
            anyhow::Ok(())
        },
        async {
            let entries = reader.read_util(20, 20, None, &cancel).await?;
            assert_eq!(entries.len(), 20);
            anyhow::Ok(())
        }
    )?;
    Ok(())
}
