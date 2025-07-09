use faststr::FastStr;
use snafu::ResultExt;

use crate::custom_base;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    MismatchPattern,
    CustomBase { source: custom_base::Error },
}

type Result<T> = std::result::Result<T, Error>;

pub struct TopicFuzzer {
    base: FastStr,
    patterns: Vec<FastStr>,
}

impl TopicFuzzer {
    pub fn new(base: FastStr, patterns: Vec<FastStr>) -> Self {
        Self { base, patterns }
    }

    pub fn fuzzy(&self, topic: &str) -> Result<String> {
        let mut ret = Vec::new();
        'outer: for pattern in &self.patterns {
            ret.clear();
            let topic_splits = topic.split('/').collect::<Vec<_>>();
            let pattern_splits = pattern.split('/').collect::<Vec<_>>();
            if pattern_splits.len() < topic_splits.len() && !pattern_splits.ends_with(&["#"]) {
                continue;
            }
            let mut topic_splits = topic_splits.into_iter();
            let mut pattern_splits = pattern_splits.into_iter();

            loop {
                match (topic_splits.next(), pattern_splits.next()) {
                    (Some(part), Some("_")) => {
                        let encoded =
                            custom_base::encode_to_custom_base(part.as_bytes(), &self.base)
                                .context(CustomBaseSnafu)?;
                        if encoded.len() > 64 {
                            ret.push(encoded[..64].into());
                        } else {
                            ret.push(encoded)
                        }
                    }
                    (Some(part), Some("+")) => {
                        ret.push(part.into());
                    }
                    (Some(part), Some("#")) => {
                        ret.push(part.into());
                        ret.extend(topic_splits.map(|s| s.into()));
                        break 'outer;
                    }
                    (Some(part), Some(pat)) if part == pat => {
                        ret.push(part.into());
                    }
                    (_, None) => break 'outer,
                    _ => return MismatchPatternSnafu.fail(),
                }
            }
        }

        Ok(ret.join("/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzzy_test() -> anyhow::Result<()> {
        let fuzzer = TopicFuzzer::new(
            "PEjkU4iqcDRXrNgH3v6pOmWKxfT8BMnt15SVAw0oLsQyuG9dIhFza2ZeYbC7Jl".into(),
            vec![
                "_/_/_/_/_/+".into(),
                "_/_/_/_/_/_/+".into(),
                "_/_/_/_/_/_/_/+".into(),
                "_/_/_/_/_/_/_/_/+".into(),
            ],
        );
        assert_eq!(fuzzer.fuzzy("1/2/3/4/5/6/7/8/9")?, "h/F/z/a/2/Z/e/Y/9");
        assert_eq!(fuzzer.fuzzy("1/2/3/4/5/6/7/8")?, "h/F/z/a/2/Z/e/8");
        assert_eq!(fuzzer.fuzzy("1/2/3/4/5/6/7")?, "h/F/z/a/2/Z/7");
        assert_eq!(fuzzer.fuzzy("1/2/3/4/5/6")?, "h/F/z/a/2/6");
        Ok(())
    }
}
