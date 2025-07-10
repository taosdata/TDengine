use std::{
    collections::HashMap,
    fmt::Display,
    io::{BufReader, Read, Write},
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
use taos::Code;
use thiserror::Error;

#[derive(Debug)]
pub struct LushAck {
    pub code: i32,
    pub message: Option<String>,
    pub context: Option<String>,
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
    pub fn code(&self) -> Code {
        Code::from(self.code)
    }

    pub fn ok() -> Self {
        LushAck {
            code: 0,
            message: None,
            context: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl Display for AckType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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
    ipc_reader: Option<StreamReader<BufReader<R>>>,
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
                self.reader.as_mut().unwrap().read_exact(&mut bytes).ok()?;
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
                        .column_by_name("context")
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

    pub fn open<R: Read>(&self, reader: R) -> Result<AckReader<R>, ArrowError> {
        let ack = self.ack;
        match ack {
            AckType::None => Ok(AckReader {
                ack,
                schema: None,
                reader: Some(reader),
                ipc_reader: None,
            }),
            AckType::Code => Ok(AckReader {
                ack,
                schema: None,
                reader: Some(reader),
                ipc_reader: None,
            }),
            AckType::Lush => {
                let fields = vec![
                    Field::new("code", DataType::Int32, true),
                    Field::new("message", DataType::Binary, true),
                    Field::new("context", DataType::Binary, true),
                ];
                let schema = Schema::new(fields).with_metadata(self.metadata.clone());
                let reader = std::io::BufReader::new(reader);
                Ok(AckReader {
                    ack: self.ack,
                    schema: Some(schema),
                    reader: None,
                    ipc_reader: Some(StreamReader::try_new(reader, None)?),
                })
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

impl From<AckWriterError> for ArrowError {
    fn from(e: AckWriterError) -> Self {
        match e {
            AckWriterError::IoError(e) => ArrowError::IoError("ACK writer error".to_string(), e),
            AckWriterError::ArrowError(e) => e,
        }
    }
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
        metadata.insert("ack".to_string(), format!("{ack}"));
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

    pub fn open<W: Write>(&self, writer: W) -> Result<AckWriter<W>, ArrowError> {
        let ack = self.ack;
        Ok(match ack {
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
                    Field::new("code", DataType::Int32, false),
                    Field::new("message", DataType::Binary, true),
                    Field::new("context", DataType::Binary, true),
                ];
                let schema = Schema::new(fields).with_metadata(self.metadata.clone());
                let schema = Arc::new(schema);
                let writer = StreamWriter::try_new(writer, &schema)?;

                AckWriter {
                    ack: self.ack,
                    writer: None,
                    ipc_writer: Some(writer),
                    ipc_schema: Some(schema),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Fields;

    use super::*;
    use std::io::Cursor;

    #[test]
    fn ack_type() {
        assert_eq!(AckType::None.as_str(), "none");
        assert_eq!(AckType::Code.as_str(), "code");
        assert_eq!(AckType::Lush.as_str(), "lush");
        assert_eq!(AckType::None, "none".parse().unwrap());
        assert_eq!(AckType::Code, "code".parse().unwrap());
        assert_eq!(AckType::Lush, "lush".parse().unwrap());

        assert!("unknown".parse::<AckType>().is_err());
    }

    #[test]
    fn test_with_schema() {
        let fields = Fields::from(vec![
            Field::new("code", DataType::Int32, false),
            Field::new("message", DataType::Binary, true),
            Field::new("context", DataType::Binary, true),
        ]);
        let schema = Schema::new(fields.clone());
        let builder = AckWriterBuilder::from_schema(&schema);
        assert_eq!(builder.ack, AckType::Lush);

        let schema = Schema::new(fields.clone()).with_metadata(
            [("ack", "code")]
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        );
        let builder = AckWriterBuilder::from_schema(&schema);
        assert_eq!(builder.ack, AckType::Code);

        let schema = Schema::new(fields.clone()).with_metadata(
            [("ack", "lush")]
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        );
        let builder = AckWriterBuilder::from_schema(&schema);
        assert_eq!(builder.ack, AckType::Lush);

        let schema = Schema::new(fields.clone()).with_metadata(
            [("ack", "none")]
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        );
        let builder = AckWriterBuilder::from_schema(&schema);
        assert_eq!(builder.ack, AckType::None);

        let schema = Schema::new(fields.clone()).with_metadata(
            [("ack", "unknown")]
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        );
        let builder = AckWriterBuilder::from_schema(&schema);
        assert_eq!(builder.ack, AckType::Lush);
    }

    #[test]
    fn test_ack_reader() {
        let mut buf = Cursor::new(vec![]);
        let mut writer = AckWriterBuilder::new(AckType::Lush)
            .with_meta("version", "1.0")
            .with_meta("custom", "meta")
            .open(&mut buf)
            .unwrap();
        writer.write_ok().unwrap();
        writer
            .ack(LushAck {
                code: 1,
                message: Some("hello".to_string()),
                context: Some("world".to_string()),
            })
            .unwrap();
        drop(writer);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf);
        let mut reader = AckReaderBuilder::new(AckType::Lush)
            .with_meta("ack", "lush")
            .open(&mut buf)
            .unwrap();
        let schema = reader.schema().unwrap();
        assert_eq!(schema.metadata().get("ack"), Some(&"lush".to_string()));

        let _ = AckReaderBuilder::from_schema(schema);
        assert_eq!(reader.ack(), AckType::Lush);
        let ack = reader.next().unwrap();
        assert_eq!(ack.code, 0);
        assert!(ack.success());
        assert_eq!(ack.message(), Some(""));
        assert_eq!(ack.context(), Some(""));
        let ack = reader.next().unwrap();
        assert_eq!(ack.code, 1);
        assert!(!ack.success());
        assert_eq!(ack.message(), Some("hello"));
        assert_eq!(ack.context(), Some("world"));
    }
    #[test]
    fn test_ack_code() {
        let mut buf = Cursor::new(vec![]);
        let mut writer = AckWriterBuilder::new(AckType::Code)
            .with_meta("version", "1.0")
            .with_meta("custom", "meta")
            .open(&mut buf)
            .unwrap();
        writer.write_ok().unwrap();
        writer
            .ack(dbg!(LushAck {
                code: 1,
                message: Some("hello".to_string()),
                context: Some("world".to_string()),
            }))
            .unwrap();
        drop(writer);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf);
        let mut reader = AckReaderBuilder::new(AckType::Code)
            .with_meta("ack", "lush")
            .open(&mut buf)
            .unwrap();
        assert_eq!(reader.ack(), AckType::Code);
        let ack = reader.next().unwrap();
        assert_eq!(ack.code, 0);
        assert!(ack.success());
        assert_eq!(ack.message(), None);
        assert_eq!(ack.context(), None);
        let ack = reader.next().unwrap();
        assert_eq!(ack.code, 1);
        assert!(!ack.success());
        assert_eq!(ack.message(), None);
        assert_eq!(ack.context(), None);
    }

    #[test]
    fn test_ack_none() {
        let mut buf = Cursor::new(vec![]);
        let mut writer = AckWriterBuilder::new(AckType::None)
            .with_meta("version", "1.0")
            .with_meta("custom", "meta")
            .open(&mut buf)
            .unwrap();
        writer.write_ok().unwrap();
        writer
            .ack(LushAck {
                code: 1,
                message: Some("hello".to_string()),
                context: Some("world".to_string()),
            })
            .unwrap();
        drop(writer);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf);
        let mut reader = AckReaderBuilder::new(AckType::None)
            .with_meta("ack", "lush")
            .open(&mut buf)
            .unwrap();
        assert_eq!(reader.ack(), AckType::None);
        let ack = reader.next().unwrap();
        assert_eq!(ack.code, 0);
        assert!(ack.success());
        assert_eq!(ack.message(), None);
        assert_eq!(ack.context(), None);
        let ack = reader.next().unwrap();
        assert_eq!(ack.code, 0);
        assert!(ack.success());
        assert_eq!(ack.message(), None);
        assert_eq!(ack.context(), None);
    }

    #[test]
    fn test_ack_write_error() {
        let mut buf = Cursor::new([0u8; 0]);
        let mut writer = AckWriterBuilder::new(AckType::Code)
            .with_meta("version", "1.0")
            .with_meta("custom", "meta")
            .open(&mut buf)
            .unwrap();
        assert!(writer.write_ok().is_err());
    }
}
