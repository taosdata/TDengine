use std::{fmt::Debug, sync::Arc};

use futures::{future, StreamExt};
use parquet::{
    basic::Compression,
    column::writer::*,
    data_type::{ByteArray, ByteArrayType},
    file::{
        properties::WriterProperties,
        writer::{FileWriter, ParquetWriter, SerializedFileWriter},
    },
    schema::types::Type,
};

use taos::{block::BlockStream, helpers::ColumnMeta, BlockExt};
use taosx::{TaosBlock, TaosDescribe, TaosTag};

pub struct Serialize<W: ParquetWriter + 'static> {
    writer: SerializedFileWriter<W>,
}

impl<W: ParquetWriter + 'static> Serialize<W> {
    pub fn new(target: W, compression: Compression, schema: Arc<Type>) -> Self {
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(compression)
                .build(),
        );
        let writer = SerializedFileWriter::new(target, schema, props).unwrap();

        Self { writer }
    }

    fn serialize<T>(&mut self, source: T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Debug,
    {
        let mut row_group = self.writer.next_row_group().unwrap();
        let mut column_writer = row_group.next_column().unwrap().unwrap();
        let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut column_writer);
        let encoded = bincode::serialize(&source).unwrap();
        typed
            .write_batch(&[ByteArray::from(encoded)], Some(&[1]), Some(&[0]))
            .unwrap();
        row_group.close_column(column_writer).unwrap();
        self.writer.close_row_group(row_group).unwrap();
    }

    pub fn serialze_table_meta(&mut self, name: &str, describe: Vec<ColumnMeta>) {
        let taos_describe = TaosDescribe::new(name.to_string(), describe);
        self.serialize(taos_describe);
    }

    pub async fn serialize_tag(&mut self, name: &str, stream: BlockStream<'_>) {
        stream
            .enumerate()
            .for_each(|(_, block)| {
                let taos_tags = TaosTag::new(name.to_string(), block.iter_rows());
                self.serialize(taos_tags);
                future::ready(())
            })
            .await;
    }

    pub async fn serialize_data(&mut self, name: &str, stream: BlockStream<'_>) {
        stream
            .enumerate()
            .for_each(|(_, block)| {
                let taos_block = TaosBlock::new(name.to_string(), block.columns_iter());
                self.serialize(taos_block);
                future::ready(())
            })
            .await;
    }
}

impl<W: ParquetWriter + 'static> Drop for Serialize<W> {
    fn drop(&mut self) {
        self.writer.close().unwrap();
    }
}
