use std::{
    collections::HashMap,
    io::{Read, Write},
    str::FromStr,
    sync::Arc,
};

use arrow::{
    array::{BinaryArray, Int32Array},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use thiserror::Error;

pub struct LushAck {
    code: i32,
    message: Option<String>,
    context: Option<String>,
}

impl LushAck {
    pub fn success(&self) -> bool {
        self.code == 0
    }
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
    pub fn context(&self) -> Option<&str> {
        self.context.as_deref()
    }

    pub fn ok() -> Self {
        LushAck {
            code: 0,
            message: None,
            context: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AckType {
    None,
    Code,
    Lush,
}

impl AckType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AckType::None => "none",
            AckType::Code => "code",
            AckType::Lush => "lush",
        }
    }
}

impl FromStr for AckType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(AckType::None),
            "code" => Ok(AckType::Code),
            "lush" => Ok(AckType::Lush),
            _ => Err(s.to_string()),
        }
    }
}

pub struct AckReader<R: Read> {
    ack: AckType,
    schema: Option<Schema>,
    reader: Option<R>,
    ipc_reader: Option<StreamReader<R>>,
}

impl<R: Read> AckReader<R> {
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }
    pub fn ack(&self) -> AckType {
        self.ack
    }
}

impl<R: Read> Iterator for AckReader<R> {
    type Item = LushAck;
    fn next(&mut self) -> Option<LushAck> {
        match self.ack {
            AckType::None => Some(LushAck {
                code: 0,
                message: None,
                context: None,
            }),
            AckType::Code => {
                let mut bytes = [0u8; 4];
                self.reader
                    .as_mut()
                    .unwrap()
                    .read_exact(&mut bytes)
                    .unwrap();
                let code = i32::from_le_bytes(bytes);
                Some(LushAck {
                    code,
                    message: None,
                    context: None,
                })
            }
            AckType::Lush => self
                .ipc_reader
                .as_mut()
                .unwrap()
                .next()
                .and_then(|r| r.ok())
                .map(|r| {
                    let code = r
                        .column_by_name("code")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .map(|arr| arr.value(0))
                        .unwrap_or(0);
                    let message = r
                        .column_by_name("message")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|arr| arr.value(0))
                        .map(|b| std::str::from_utf8(b).unwrap().to_string());
                    let context = r
                        .column_by_name("message")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|arr| arr.value(0))
                        .map(|b| std::str::from_utf8(b).unwrap().to_string());
                    LushAck {
                        code,
                        message,
                        context,
                    }
                }),
        }
    }
}

pub struct AckReaderBuilder {
    ack: AckType,
    metadata: HashMap<String, String>,
}

impl AckReaderBuilder {
    pub fn new(ack: AckType) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("ack".to_string(), "".to_string());
        Self { ack, metadata }
    }

    pub fn from_schema(schema: &Schema) -> Self {
        let ack = schema
            .metadata()
            .get("ack")
            .and_then(|ack| ack.parse().ok())
            .unwrap_or(AckType::Lush);
        Self::new(ack)
    }

    pub fn with_meta(&mut self, k: impl Into<String>, v: impl Into<String>) -> &mut Self {
        let (k, v) = (k.into(), v.into());
        self.metadata.insert(k, v);
        self
    }

    pub fn open<R: Read>(&self, reader: R) -> AckReader<R> {
        let ack = self.ack;
        match ack {
            AckType::None => AckReader {
                ack,
                schema: None,
                reader: Some(reader),
                ipc_reader: None,
            },
            AckType::Code => AckReader {
                ack,
                schema: None,
                reader: Some(reader),
                ipc_reader: None,
            },
            AckType::Lush => {
                let fields = vec![
                    Field::new("code", DataType::Int32, true),
                    Field::new("message", DataType::Binary, true),
                    Field::new("context", DataType::Binary, true),
                ];
                let schema = Schema::new(fields).with_metadata(self.metadata.clone());
                AckReader {
                    ack: self.ack,
                    schema: Some(schema),
                    reader: None,
                    ipc_reader: Some(StreamReader::try_new(reader, None).unwrap()),
                }
            }
        }
    }
}

pub struct AckWriter<W: Write> {
    ack: AckType,
    writer: Option<W>,
    ipc_writer: Option<StreamWriter<W>>,
    ipc_schema: Option<Arc<Schema>>,
}

#[derive(Error, Debug)]
pub enum AckWriterError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}

impl<W: Write> AckWriter<W> {
    pub fn write_ok(&mut self) -> Result<(), AckWriterError> {
        self.ack(LushAck::ok())
    }
    pub fn ack(&mut self, ack: LushAck) -> Result<(), AckWriterError> {
        match self.ack {
            AckType::None => Ok(()),
            AckType::Code => {
                self.writer
                    .as_mut()
                    .unwrap()
                    .write_all(&ack.code.to_le_bytes())?;
                Ok(())
            }
            AckType::Lush => {
                let writer = self.ipc_writer.as_mut().unwrap();
                let schema = self.ipc_schema.as_ref().unwrap().clone();
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from(vec![ack.code])),
                        Arc::new(BinaryArray::from(vec![ack
                            .message
                            .as_ref()
                            .map(|s| s.as_bytes())])),
                        Arc::new(BinaryArray::from(vec![ack
                            .context
                            .as_ref()
                            .map(|s| s.as_bytes())])),
                    ],
                )?;
                writer.write(&batch)?;
                Ok(())
            }
        }
    }
}

pub struct AckWriterBuilder {
    ack: AckType,
    metadata: HashMap<String, String>,
}

impl AckWriterBuilder {
    pub fn new(ack: AckType) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("ack".to_string(), "".to_string());
        Self { ack, metadata }
    }

    pub fn from_schema(schema: &Schema) -> Self {
        let ack = schema
            .metadata()
            .get("ack")
            .and_then(|ack| ack.parse().ok())
            .unwrap_or(AckType::Lush);
        Self::new(ack)
    }

    pub fn with_meta(&mut self, k: impl Into<String>, v: impl Into<String>) -> &mut Self {
        let (k, v) = (k.into(), v.into());
        self.metadata.insert(k, v);
        self
    }

    pub fn open<W: Write>(&self, writer: W) -> AckWriter<W> {
        let ack = self.ack;
        match ack {
            AckType::None => AckWriter {
                ack,
                writer: Some(writer),
                ipc_writer: None,
                ipc_schema: None,
            },
            AckType::Code => AckWriter {
                ack,
                writer: Some(writer),
                ipc_writer: None,
                ipc_schema: None,
            },
            AckType::Lush => {
                let fields = vec![
                    Field::new("code", DataType::Int32, true),
                    Field::new("message", DataType::Binary, true),
                    Field::new("context", DataType::Binary, true),
                ];
                let schema = Schema::new(fields).with_metadata(self.metadata.clone());
                AckWriter {
                    ack: self.ack,
                    writer: None,
                    ipc_writer: Some(StreamWriter::try_new(writer, &schema).unwrap()),
                    ipc_schema: Some(Arc::new(schema)),
                }
            }
        }
    }
}
