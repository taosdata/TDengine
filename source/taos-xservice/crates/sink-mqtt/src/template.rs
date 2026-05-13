use std::collections::HashMap;

use snafu::OptionExt;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("found multi embedded opening brace"))]
    MultiOpeningBrace,
    #[snafu(display("template string brace not closed"))]
    OpeningBraceUnclosed,
    #[snafu(display("unexpected closed brace in template string"))]
    UnexpectedClosedBrace,
    #[snafu(display("key {key} not found in template"))]
    KeyNotFound { key: String },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Template {
    template: String,
    keys: HashMap<String, String>,
}

impl Template {
    pub fn new<S: Into<String>>(template: S) -> Result<Self> {
        let template: String = template.into();
        let mut chars = template.chars();

        let mut keys = HashMap::new();
        loop {
            let Some(char) = chars.next() else {
                break;
            };
            match char {
                '{' => {
                    let mut key = String::new();
                    loop {
                        let Some(char) = chars.next() else {
                            return OpeningBraceUnclosedSnafu.fail();
                        };
                        match char {
                            '}' => break,
                            '{' => {
                                return MultiOpeningBraceSnafu.fail();
                            }
                            c => key.push(c),
                        }
                    }
                    let pattern = format!("{{{key}}}");
                    keys.insert(key, pattern);
                }
                '}' => return UnexpectedClosedBraceSnafu.fail(),
                _ => {}
            }
        }

        Ok(Self { template, keys })
    }

    pub fn render(&self, map: &serde_json::Map<String, serde_json::Value>) -> Result<String> {
        let mut ret = self.template.clone();
        for (key, pattern) in &self.keys {
            let value = map.get(key).context(KeyNotFoundSnafu { key })?;
            let value = match value {
                serde_json::Value::String(s) => s.clone(),
                v => v.to_string(),
            };
            ret = ret.replace(pattern, &value);
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_test() {
        let a = Template::new("a_{b}_{c}_");
        assert_eq!(
            a.unwrap().keys,
            HashMap::from_iter([("b".into(), "{b}".into()), ("c".into(), "{c}".into())])
        );

        let a = Template::new("{a_{b}_{c}_");
        assert!(a.is_err());

        let a = Template::new("{a_{b}_{c}_}");
        assert!(a.is_err());

        let a = Template::new("{a}_}bc");
        assert!(a.is_err());
    }

    #[test]
    fn render_test() {
        let a = Template::new("a_{b}_{c}_").unwrap();
        let b = a
            .render(
                serde_json::json!({
                    "a": 1,
                    "b": "kkk",
                    "c": 2.4
                })
                .as_object()
                .unwrap(),
            )
            .unwrap();
        assert_eq!(b, "a_kkk_2.4_")
    }
}
