use arrow::array::{StringArray, StringBuilder};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Topic pattern is empty"))]
    Empty,
    #[snafu(display("Topic {topic} and pattern {pattern} not match"))]
    NotMatch { topic: String, pattern: String },
    #[snafu(display("Invalid topic pattern {pattern}"))]
    Invalid { pattern: String },
}

type Result<T> = std::result::Result<T, Error>;

const IGNORE_WILDCARD: &str = "_";

#[derive(Debug, PartialEq, Clone)]
pub struct TopicPattern(String);

impl TopicPattern {
    pub fn keys(&self) -> Vec<String> {
        self.0
            .split('/')
            .filter(|s| !s.is_empty() && *s != IGNORE_WILDCARD)
            .map(|s| s.to_owned())
            .collect()
    }

    pub fn parse_topic(&self, topic: &str) -> Result<Vec<(String, String)>> {
        let pattern = &self.0;
        let mut pattern_split = pattern.split('/');
        let mut topic_split = topic.split('/');
        let mut res = Vec::new();
        loop {
            match (pattern_split.next(), topic_split.next()) {
                (None, None) => return Ok(res),
                (None, Some(_)) | (Some(_), None) => {
                    return NotMatchSnafu { topic, pattern }.fail();
                }
                (Some(l), Some(r)) if l.is_empty() || r.is_empty() || l == IGNORE_WILDCARD => {
                    continue;
                }
                (Some(l), Some(r)) => res.push((l.to_owned(), r.to_owned())),
            }
        }
    }
}

impl std::str::FromStr for TopicPattern {
    type Err = Error;

    fn from_str(pattern: &str) -> Result<Self> {
        snafu::ensure!(!pattern.is_empty(), EmptySnafu);
        snafu::ensure!(!pattern.contains("//"), InvalidSnafu { pattern });
        snafu::ensure!(
            pattern
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '/'),
            InvalidSnafu { pattern }
        );
        snafu::ensure!(pattern != "topic", InvalidSnafu { pattern });
        snafu::ensure!(pattern != "qos", InvalidSnafu { pattern });
        snafu::ensure!(pattern != "ts", InvalidSnafu { pattern });
        snafu::ensure!(pattern != "payload", InvalidSnafu { pattern });
        Ok(Self(pattern.to_owned()))
    }
}

impl std::fmt::Display for TopicPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub struct TopicParser {
    pattern: TopicPattern,
    builders: Vec<(String, StringBuilder)>,
}

impl TopicParser {
    pub fn new(pattern: TopicPattern) -> Self {
        let builders = pattern
            .keys()
            .into_iter()
            .map(|k| (k, StringBuilder::new()))
            .collect();
        Self { pattern, builders }
    }

    pub fn append_value(&mut self, topic: &str) -> anyhow::Result<()> {
        let parts = self.pattern.parse_topic(topic)?;
        let mut parts = parts.into_iter();

        let mut builders = self.builders.iter_mut();
        loop {
            match (builders.next(), parts.next()) {
                (None, None) => return Ok(()),
                (None, _) | (_, None) => {
                    anyhow::bail!("parse topic error: pattern={}, topic={topic}", self.pattern)
                }
                (Some((builder_key, _)), Some((topic_key, _))) if *builder_key != topic_key => {
                    anyhow::bail!(
                        "topic pattern key not match: expected {builder_key}, found {topic_key}"
                    )
                }
                (Some((_, builder)), Some((_, part))) => builder.append_value(&part),
            }
        }
    }

    pub fn finish(&mut self) -> Vec<StringArray> {
        self.builders
            .iter_mut()
            .map(|(_, builder)| builder.finish())
            .collect()
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashSet, iter};

    use anyhow::Context;

    use super::*;

    #[test]
    fn parse_pattern_test() -> anyhow::Result<()> {
        assert!("".parse::<TopicPattern>().is_err());
        assert!("a//b".parse::<TopicPattern>().is_err());
        assert_eq!(
            "a/b/c".parse::<TopicPattern>()?,
            TopicPattern("a/b/c".to_string())
        );
        Ok(())
    }

    #[test]
    fn parse_topic_test() -> anyhow::Result<()> {
        let pattern: TopicPattern = "a/_/c/d".parse()?;
        assert_eq!(
            pattern.parse_topic("a1/b1/c1/d1")?,
            vec![
                ("a".to_string(), "a1".to_string()),
                ("c".to_string(), "c1".to_string()),
                ("d".to_string(), "d1".to_string())
            ]
        );
        assert!(pattern.parse_topic("a1/b1").is_err());
        assert!(pattern.parse_topic("a1/b1/c1/d1/e1").is_err());

        let pattern: TopicPattern = "a".parse()?;
        assert_eq!(
            pattern.parse_topic("a1")?,
            vec![("a".to_string(), "a1".to_string())]
        );

        Ok(())
    }

    #[test]
    fn pattern_keys_test() -> anyhow::Result<()> {
        let pattern: TopicPattern = "a".parse()?;
        assert_eq!(pattern.keys(), vec!["a".to_string()]);

        let pattern: TopicPattern = "a/b".parse()?;
        assert_eq!(pattern.keys(), vec!["a".to_string(), "b".to_string()]);

        let pattern: TopicPattern = "_/a/_/b".parse()?;
        assert_eq!(pattern.keys(), vec!["a".to_string(), "b".to_string()]);
        Ok(())
    }

    #[test]
    fn parser_test() -> anyhow::Result<()> {
        let mut parser = TopicParser::new("_/a/_/b".parse()?);
        parser.append_value("w/x/y/z")?;
        assert!(parser.append_value("/1/2/3/4").is_err());
        assert!(parser.append_value("1").is_err());
        parser.append_value("1/2/3/4")?;
        assert_eq!(
            parser.finish(),
            vec![
                StringArray::from(vec!["x", "2"]),
                StringArray::from(vec!["z", "4"])
            ]
        );
        Ok(())
    }

    #[test]
    fn topic_parser_test() -> anyhow::Result<()> {
        let mut topic_parser = TopicParser::new("a/_/c".parse()?);
        topic_parser.append_value("this/is/test")?;
        topic_parser.append_value("test/parse/topic")?;
        assert_eq!(
            topic_parser.finish(),
            vec![
                StringArray::from(vec!["this", "test"]),
                StringArray::from(vec!["test", "topic"])
            ]
        );
        Ok(())
    }

    #[test]
    #[ignore]
    fn parse_csv_topic_test() -> anyhow::Result<()> {
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path("/root/longbow_recording.csv")?
            .into_records();
        let mut writers = [
            csv::Writer::from_path("/root/topics-6.csv")?,
            csv::Writer::from_path("/root/topics-7.csv")?,
            csv::Writer::from_path("/root/topics-8.csv")?,
            csv::Writer::from_path("/root/topics-9.csv")?,
        ];
        writers[0].write_record(["topic", "site_controller_id", "point_name", "data_type"])?;
        writers[0].flush()?;

        writers[1].write_record([
            "topic",
            "site_controller_id",
            "source_device_type",
            "source_device_id",
            "point_name",
            "data_type",
        ])?;
        writers[1].flush()?;

        writers[2].write_record([
            "topic",
            "site_controller_id",
            "unit_controller_id",
            "point_name",
            "data_type",
        ])?;
        writers[2].flush()?;

        writers[3].write_record([
            "topic",
            "site_controller_id",
            "unit_controller_id",
            "source_device_type",
            "source_device_id",
            "point_name",
            "data_type",
        ])?;
        writers[3].flush()?;
        let patterns : [TopicPattern;4]= [
            "_/_/site_controller_id/_/point_name/data_type".parse()?,
            "_/_/site_controller_id/source_device_type/source_device_id/point_name/data_type".parse()?,
            "_/_/site_controller_id/_/unit_controller_id/_/point_name/data_type".parse()?,
            "_/_/site_controller_id/_/unit_controller_id/source_device_type/source_device_id/point_name/data_type".parse()?
        ];
        let mut topics = HashSet::new();
        for record in reader {
            let record = record?;
            let topic = record.get(0).context("topic not found")?.to_string();
            if !topic.starts_with("ems") {
                continue;
            }
            topics.insert(topic);
        }
        println!("loaded {} topics", topics.len());
        let mut count = 0;
        for topic in topics {
            match topic.split("/").count() {
                6 => {
                    let parts = patterns[0].parse_topic(&topic)?;
                    writers[0].write_record(
                        iter::once(topic.as_str()).chain(parts.iter().map(|v| v.1.as_str())),
                    )?;
                }
                7 => {
                    let parts = patterns[1].parse_topic(&topic)?;
                    writers[1].write_record(
                        iter::once(topic.as_str()).chain(parts.iter().map(|v| v.1.as_str())),
                    )?;
                }
                8 => {
                    let parts = patterns[2].parse_topic(&topic)?;
                    writers[2].write_record(
                        iter::once(topic.as_str()).chain(parts.iter().map(|v| v.1.as_str())),
                    )?;
                }
                9 => {
                    let parts = patterns[3].parse_topic(&topic)?;
                    writers[3].write_record(
                        iter::once(topic.as_str()).chain(parts.iter().map(|v| v.1.as_str())),
                    )?;
                }
                _ => {
                    panic!("topic: {topic}")
                }
            }
            count += 1;
            if count >= 1000 {
                println!("write {count} topics");
            }
        }
        for writer in writers.as_mut() {
            writer.flush()?;
        }
        println!("write {count} topics");
        Ok(())
    }
}
