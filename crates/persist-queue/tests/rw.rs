use std::time::Duration;

use bytes::Bytes;
use persist_queue::fs::{FsQueue, ReadFrom};
use tempfile::tempdir;
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

    // 启动读后台线程
    let (read_tx, read_rx) = flume::bounded(0);
    let reader = persist_queue::reader::Reader::builder(
        queue.new_reader(ReadFrom::Earliest).await?,
        read_tx,
    )
    .vacuum_interval(Duration::from_millis(10))
    .build();
    let read_handle = tokio::spawn(reader.run(token.child_token()));

    // 启动写后台线程
    let (write_tx, write_rx) = flume::bounded(10);
    let writer = persist_queue::writer::Writer::builder(queue.new_writer().await?, write_rx)
        .sync_interval(Duration::from_millis(100))
        .chunk_size(10)
        .build();
    let write_handle = tokio::spawn(writer.run(token.child_token()));

    // 同时读写
    tokio::try_join!(
        async {
            for payload in common::payloads(200) {
                write_tx.send_async(payload).await?;
            }
            drop(write_tx);
            match write_handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    panic!("write task exit with error: {e}");
                }
                Err(e) => {
                    panic!("write task panicked: {e}");
                }
            }
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
            anyhow::Ok(())
        }
    )?;

    token.cancel();

    match read_handle.await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            panic!("write task exit with error: {e}");
        }
        Err(e) => {
            panic!("write task panicked: {e}");
        }
    }

    Ok(())
}
