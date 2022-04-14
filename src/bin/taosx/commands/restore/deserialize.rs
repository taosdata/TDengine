use parquet::{
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    record::Field,
};
use std::io::BufReader;
use std::{fs::File, path::PathBuf};

use taosx::Database;

pub fn deserialize_database(path: PathBuf) -> Database {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).unwrap()
}

pub struct Deserialize<T: serde::de::DeserializeOwned> {
    parquet_reader: SerializedFileReader<File>,
    pub output: Vec<T>,
}

impl<T: serde::de::DeserializeOwned> Deserialize<T> {
    pub fn new(source: String) -> Self {
        let parquet_reader = SerializedFileReader::try_from(source).unwrap();
        let output: Vec<T> = Vec::new();
        Self {
            parquet_reader,
            output,
        }
    }

    pub async fn deserialize(&mut self) {
        let iter = self.parquet_reader.get_row_iter(None).unwrap();
        for row in iter {
            let (_, field) = row.get_column_iter().next().unwrap();
            if let Field::ListInternal(list) = field {
                for element in list.elements() {
                    if let Field::Group(g) = element {
                        let (_, fields) = g.get_column_iter().next().unwrap();
                        if let Field::Bytes(b) = fields {
                            self.output
                                .push(bincode::deserialize::<T>(b.data()).unwrap());
                        }
                    }
                }
            }
        }
    }
}
