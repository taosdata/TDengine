use std::collections::HashMap;

use anyhow::Context;
use serde::Serialize;
use taos::Itertools;
use tinytemplate::TinyTemplate;

use crate::plugins::transform::TableOptions;

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SModel {
    name: String,
    columns: Vec<Column>,
    tags: Vec<Tag>,
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Tag {
    name: String,
    r#type: String,
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    name: String,
    r#type: String,
    encode: Option<String>,
    compress: Option<String>,
    level: Option<String>,
}

impl SModel {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 每行一个 json map
    pub fn apply<I>(&self, ctx: I, opts: &TableOptions) -> anyhow::Result<HashMap<String, Self>>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        let mut template = tinytemplate::TinyTemplate::new();
        let name = self.name.replace("${", "{");
        template
            .add_template("name", &name)
            .context("add name template error")?;

        let col_templates = self.column_templates();
        for (name, text) in &col_templates {
            template
                .add_template(name, text)
                .context("add template error")?;
        }

        let tag_templates = self.tag_templates();
        for (name, text) in &tag_templates {
            template
                .add_template(name, text)
                .context("add template error")?;
        }

        let mut res = HashMap::new();
        for ctx in ctx.into_iter() {
            let name = template
                .render("name", &ctx)
                .context("render stable name error")?;
            let name = opts.canonical_table_name(&name).to_string();
            if res.contains_key(&name) {
                continue;
            }
            res.insert(
                name.clone(),
                SModel {
                    name,
                    columns: self.render_column(&template, &ctx)?,
                    tags: self.render_tags(&template, &ctx)?,
                },
            );
        }
        Ok(res)
    }

    fn column_templates(&self) -> Vec<(String, String)> {
        self.columns
            .iter()
            .enumerate()
            .flat_map(|(idx, col)| {
                let mut cols = vec![
                    (format!("col_{idx}_name"), col.name.replace("${", "{")),
                    (format!("col_{idx}_type"), col.r#type.replace("${", "{")),
                ];
                if let Some(encode) = col.encode.as_ref() {
                    cols.push((format!("col_{idx}_encode"), encode.replace("${", "{")));
                }
                if let Some(encode) = col.compress.as_ref() {
                    cols.push((format!("col_{idx}_compress"), encode.replace("${", "{")));
                }
                if let Some(encode) = col.level.as_ref() {
                    cols.push((format!("col_{idx}_level"), encode.replace("${", "{")));
                }
                cols
            })
            .collect()
    }

    fn render_column<C>(&self, template: &TinyTemplate, ctx: C) -> anyhow::Result<Vec<Column>>
    where
        C: Serialize,
    {
        self.columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                Ok(Column {
                    name: template
                        .render(&format!("col_{idx}_name"), &ctx)
                        .context("render stable column name error")?,
                    r#type: template
                        .render(&format!("col_{idx}_type"), &ctx)
                        .context("render stable column type error")?,
                    encode: col
                        .encode
                        .as_ref()
                        .map(|_| {
                            template
                                .render(&format!("col_{idx}_encode"), &ctx)
                                .context("render stable column encode error")
                        })
                        .transpose()?,
                    compress: col
                        .compress
                        .as_ref()
                        .map(|_| {
                            template
                                .render(&format!("col_{idx}_compress"), &ctx)
                                .context("render stable column compress error")
                        })
                        .transpose()?,
                    level: col
                        .level
                        .as_ref()
                        .map(|_| {
                            template
                                .render(&format!("col_{idx}_level"), &ctx)
                                .context("render stable column level error")
                        })
                        .transpose()?,
                })
            })
            .collect::<anyhow::Result<_>>()
    }

    fn tag_templates(&self) -> Vec<(String, String)> {
        self.tags
            .iter()
            .enumerate()
            .flat_map(|(idx, tag)| {
                vec![
                    (format!("tag_{idx}_name"), tag.name.replace("${", "{")),
                    (format!("tag_{idx}_type"), tag.r#type.replace("${", "{")),
                ]
            })
            .collect()
    }

    fn render_tags<C>(&self, template: &TinyTemplate, ctx: C) -> anyhow::Result<Vec<Tag>>
    where
        C: Serialize,
    {
        (0..self.tags.len())
            .map(|idx| {
                Ok(Tag {
                    name: template
                        .render(&format!("tag_{idx}_name"), &ctx)
                        .context("render stable tag name error")?,
                    r#type: template
                        .render(&format!("tag_{idx}_type"), &ctx)
                        .context("render stable tag type error")?,
                })
            })
            .collect::<anyhow::Result<_>>()
    }

    pub fn create_stable_sql(&self) -> String {
        let name = self.name();
        let columns = self
            .columns
            .iter()
            .map(|col| {
                let mut res = format!("{} {}", col.name, col.r#type);
                if let Some(encode) = col
                    .encode
                    .as_ref()
                    .map(|encode| format!(" ENCODE '{encode}'"))
                {
                    res.push_str(&encode);
                }
                if let Some(compress) = col
                    .compress
                    .as_ref()
                    .map(|compress| format!(" COMPRESS '{compress}'"))
                {
                    res.push_str(&compress);
                }
                if let Some(level) = col.level.as_ref().map(|level| format!(" LEVEL '{level}'")) {
                    res.push_str(&level);
                }
                res
            })
            .join(", ");
        let tags = self
            .tags
            .iter()
            .map(|tag| format!("{} {}", tag.name, tag.r#type))
            .join(", ");
        format!("CREATE STABLE IF NOT EXISTS `{name}` ({columns}) TAGS ({tags});")
    }
}

#[cfg(test)]
mod tests {
    use serde_json as json;

    use super::*;

    #[test]
    fn create_stable_sql_test() -> anyhow::Result<()> {
        let model = SModel {
            name: "abc".to_string(),
            columns: vec![
                Column {
                    name: "col1".to_string(),
                    r#type: "TIMESTAMP".to_string(),
                    encode: Some("delta-i".to_string()),
                    compress: Some("lz4".to_string()),
                    level: Some("medium".to_string()),
                },
                Column {
                    name: "col2".to_string(),
                    r#type: "VARCHAR(128)".to_string(),
                    compress: Some("lz4".to_string()),
                    ..Default::default()
                },
            ],
            tags: vec![
                Tag {
                    name: "tag1".to_string(),
                    r#type: "INT".to_string(),
                },
                Tag {
                    name: "tag2".to_string(),
                    r#type: "VARCHAR(128)".to_string(),
                },
            ],
        };
        assert_eq!(
            model.create_stable_sql(),
            "CREATE STABLE IF NOT EXISTS `abc` (col1 TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', col2 VARCHAR(128) COMPRESS 'lz4') TAGS (tag1 INT, tag2 VARCHAR(128));"
        );
        Ok(())
    }

    #[test]
    fn apply_test() -> anyhow::Result<()> {
        let model = SModel {
            name: "${abc_x}".to_string(),
            columns: vec![
                Column {
                    name: "${col1_x}".to_string(),
                    r#type: "${TIMESTAMP_x}".to_string(),
                    encode: Some("${delta-i_x}".to_string()),
                    compress: Some("${lz4_x}".to_string()),
                    level: Some("${medium_x}".to_string()),
                },
                Column {
                    name: "${col2_x}".to_string(),
                    r#type: "${VARCHAR(128)_x}".to_string(),
                    compress: Some("${lz4_x}".to_string()),
                    ..Default::default()
                },
            ],
            tags: vec![
                Tag {
                    name: "${tag1_x}".to_string(),
                    r#type: "${INT_x}".to_string(),
                },
                Tag {
                    name: "${tag2_x}".to_string(),
                    r#type: "${VARCHAR(128)_x}".to_string(),
                },
            ],
        };
        let models = model.apply(
            [json::json!({
                "abc_x": "abc",
                "col1_x": "col1",
                "TIMESTAMP_x": "TIMESTAMP",
                "delta-i_x": "delta-i",
                "lz4_x": "lz4",
                "medium_x": "medium",
                "col2_x": "col2",
                "VARCHAR(128)_x": "VARCHAR(128)",
                "tag1_x": "tag1",
                "INT_x": "INT",
                "tag2_x": "tag2",
                "VARCHAR(128)_x": "VARCHAR(128)"
            })],
            &TableOptions::default(),
        )?;
        assert_eq!(models.get("abc").context("model not found")?.create_stable_sql(), "CREATE STABLE IF NOT EXISTS `abc` (col1 TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', col2 VARCHAR(128) COMPRESS 'lz4') TAGS (tag1 INT, tag2 VARCHAR(128));");
        Ok(())
    }
}
