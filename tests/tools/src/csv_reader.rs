pub fn new_reader(
    has_header: bool,
    path: impl AsRef<std::path::Path>,
) -> Result<csv::StringRecordsIntoIter<std::fs::File>, csv::Error> {
    csv_reader(has_header, std::fs::File::open(path)?)
}

fn csv_reader<R>(has_header: bool, reader: R) -> Result<csv::StringRecordsIntoIter<R>, csv::Error>
where
    R: std::io::Read,
{
    let reader = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_reader(reader);
    Ok(reader.into_records())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn csv_read_test() -> anyhow::Result<()> {
        let mut iter = csv_reader(false, std::io::Cursor::new(String::from(r#"a,"b\nc""#)))?;
        let record = iter.next().transpose()?;
        assert_eq!(record, Some(csv::StringRecord::from(vec!["a", r"b\nc"])));
        assert_eq!(record.unwrap().get(1).unwrap().len(), 4);
        assert!(iter.next().is_none());

        let mut records = csv_reader(
            false,
            std::io::Cursor::new(String::from(r#"a,"{""a"": ""b\nc""}"#)),
        )?;
        let headers = csv::StringRecord::from(vec!["topic".to_string(), "payload".to_string()]);
        let record = records
            .next()
            .transpose()?
            .map(|i| i.deserialize::<(String, String)>(Some(&headers)))
            .transpose()?;
        assert_eq!(
            record,
            Some(("a".to_string(), r#"{"a": "b\nc"}"#.to_string()))
        );
        assert!(serde_json::from_str::<serde_json::Value>(&record.unwrap().1).is_ok());

        Ok(())
    }
}
