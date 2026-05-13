use std::{num::ParseIntError, str::FromStr};

use rand::{
    Rng,
    distributions::{Alphanumeric, DistString, Slice},
};
use snafu::{ResultExt, ensure};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    EmptyTopic,
    EmptyTopicPart,
    InvalidRange { source: ParseIntError, num: String },
    RangeNotFound,
}

type Result<T> = std::result::Result<T, Error>;

/// {charset:min:max}
/// {::max}
/// {charset:fixed}
/// {:fixed}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicFaker {
    delimiter: char,
    pattern: String,
}

impl TopicFaker {
    pub fn new(delimiter: char, pattern: &str) -> Result<Self> {
        ensure!(!pattern.trim().is_empty(), EmptyTopicSnafu);
        ensure!(
            !pattern.contains(&String::from_iter(std::iter::repeat_n(delimiter, 2))),
            EmptyTopicPartSnafu
        );
        Ok(Self {
            delimiter,
            pattern: pattern.to_string(),
        })
    }
}

impl FromStr for TopicFaker {
    type Err = Error;

    fn from_str(pattern: &str) -> Result<Self> {
        Self::new('/', pattern)
    }
}

fn rand_topic_part(part: &str) -> Result<String> {
    ensure!(!part.trim().is_empty(), EmptyTopicPartSnafu);
    let Some(part) = part
        .strip_prefix("{")
        .and_then(|part| part.strip_suffix("}"))
        .filter(|part| !part.is_empty())
    else {
        return Ok(part.to_string());
    };

    let mut specs = part.split(":");

    let mut rng = rand::thread_rng();

    let (charset, length) = match (
        specs.next().filter(|s| !s.is_empty()),
        specs.next().filter(|s| !s.is_empty()),
        specs.next().filter(|s| !s.is_empty()),
    ) {
        (charset, Some(min), Some(max)) => (
            charset,
            rng.gen_range(
                min.parse().context(InvalidRangeSnafu { num: min })?
                    ..max.parse().context(InvalidRangeSnafu { num: max })?,
            ),
        ),
        (charset, None, Some(max)) => (
            charset,
            rng.gen_range(1..max.parse().context(InvalidRangeSnafu { num: max })?),
        ),
        (charset, Some(fixed), None) => (
            charset,
            fixed.parse().context(InvalidRangeSnafu { num: fixed })?,
        ),
        (_, None, None) => return RangeNotFoundSnafu.fail(),
    };

    gen_rand_part(charset, length)
}

fn gen_rand_part(charset: Option<&str>, length: usize) -> Result<String> {
    let mut rng = rand::thread_rng();
    match charset {
        Some(charset) => Ok(rng
            .sample_iter(Slice::new(charset.as_bytes()).unwrap())
            .take(length)
            .map(|c| *c as char)
            .collect()),
        None => Ok(Alphanumeric.sample_string(&mut rng, length)),
    }
}

impl TopicFaker {
    pub fn next(&self) -> Result<String> {
        Ok(self
            .pattern
            .split(self.delimiter)
            .map(rand_topic_part)
            .collect::<Result<Vec<_>>>()?
            .join(&self.delimiter.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_rand_part_test() -> anyhow::Result<()> {
        let s = gen_rand_part(Some("abc"), 10)?;
        assert_eq!(s.len(), 10);
        let ss = s
            .chars()
            .filter(|c| *c == 'a' || *c == 'b' || *c == 'c')
            .collect::<String>();
        assert_eq!(s, ss);
        Ok(())
    }

    #[test]
    fn rand_topic_part_test() -> anyhow::Result<()> {
        let part = rand_topic_part("{abc:2:5}")?;
        assert!(part.len() >= 2 && part.len() <= 5);
        assert!(part.chars().all(|c| c == 'a' || c == 'b' || c == 'c'));

        let part = rand_topic_part("{::5}")?;
        assert!(!part.is_empty() && part.len() <= 5);
        assert!(part.chars().all(|c| c.is_alphanumeric()));

        let part = rand_topic_part("{def:5}")?;
        assert_eq!(part.len(), 5);
        assert!(part.chars().all(|c| c == 'd' || c == 'e' || c == 'f'));

        let part = rand_topic_part("{:5}")?;
        assert_eq!(part.len(), 5);
        assert!(part.chars().all(|c| c.is_alphanumeric()));

        assert!(rand_topic_part("{abc}").is_err());
        assert!(rand_topic_part("{abc::}").is_err());
        Ok(())
    }
}
