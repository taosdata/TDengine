use std::time::Duration;

use bytes::Bytes;
use persist_queue::{
    fs::{EntryPosition, FsQueue, ReadFrom},
    writer,
};
use tempfile::tempdir;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rw() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut queue = FsQueue::builder(dir.path())
        .segment_size(100)
        .build()
        .await?;
    let token = CancellationToken::new();

    let mut tasks = JoinSet::new();

    // 启动读后台线程
    let (read_tx, read_rx) = flume::bounded(1);
    let reader = persist_queue::reader::Reader::builder(
        queue.new_reader(ReadFrom::Earliest).await?,
        read_tx,
    )
    .vacuum_interval(Duration::from_millis(10))
    .build();
    tasks.spawn(reader.run(token.child_token()));

    // 启动写后台线程
    let (write_tx, write_rx) = flume::bounded(0);
    let (write_req_tx, write_req_rx) = flume::bounded(1);
    let writer = persist_queue::writer::Writer::builder(queue.new_writer().await?, write_rx)
        .sync_interval(Duration::from_millis(100))
        .chunk_size(10)
        .request_rx(write_req_rx)
        .build();
    tasks.spawn(writer.run(token.child_token()));

    // 同时读写
    tokio::try_join!(
        async {
            for payload in common::payloads(200) {
                write_tx.send_async(payload).await?;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let (req_tx, req_rx) = tokio::sync::oneshot::channel();
            write_req_tx
                .send_async(writer::Request::Position(req_tx))
                .await?;
            assert_eq!(req_rx.await, Ok(EntryPosition::new(33, 36)));
            drop(write_tx);
            drop(write_req_tx);
            anyhow::Ok(())
        },
        async {
            let mut count = 0;
            while let Ok(entry) = read_rx.recv_async().await {
                assert_eq!(
                    entry.payload,
                    Bytes::from_static("hello, world!".as_bytes())
                );
                count += 1;
                if count >= 200 {
                    break;
                }
            }
            tokio::time::timeout(Duration::from_millis(100), read_rx.recv_async())
                .await
                .ok();
            anyhow::Ok(())
        }
    )?;

    assert_eq!(queue.segments().len(), 1);

    token.cancel();

    tasks.join_all().await;

    Ok(())
}
