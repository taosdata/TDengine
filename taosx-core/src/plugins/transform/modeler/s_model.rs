use std::collections::{HashMap, HashSet};

use anyhow::Context;
use serde_json::Value;
use taos::Itertools;
use tinytemplate::TinyTemplate;

use crate::plugins::transform::TableOptions;

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SModel {
    pub name: String,
    pub columns: Vec<Column>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Tag {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: String,
    pub encode: Option<String>,
    pub compress: Option<String>,
    pub level: Option<String>,
}

impl SModel {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    /// 每行一个 json map
    pub fn apply(
        &self,
        ctx: &[Value],
        opts: &TableOptions,
    ) -> anyhow::Result<HashMap<String, Self>> {
        let mut template = tinytemplate::TinyTemplate::new();
        let name = self.name.replace("${", "{");
        if self.name.contains("${") {
            template
                .add_template("name", &name)
                .context("add name template error")?;
        }

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

        let mut models = HashMap::new();
        let mut curr_columns = HashSet::new();
        let mut curr_tags = HashSet::new();
        for ctx in ctx.iter() {
            let name = if self.name.contains("${") {
                template
                    .render_value("name", ctx)
                    .context("render stable name error")?
            } else {
                self.name.clone()
            };
            let name = opts.canonical_table_name(&name).to_string();
            let columns = self.render_column(&template, ctx)?;
            let tags = self.render_tags(&template, ctx)?;
            models
                .entry(name.clone())
                .and_modify(|model: &mut SModel| {
                    for col in &columns {
                        if !curr_columns.contains(&col.name) {
                            curr_columns.insert(col.name.clone());
                            model.columns.push(col.clone());
                        }
                    }
                    for tag in &tags {
                        if !curr_tags.contains(&tag.name) {
                            curr_tags.insert(tag.name.clone());
                            model.tags.push(tag.clone());
                        }
                    }
                })
                .or_insert_with(|| {
                    curr_columns.extend(columns.iter().map(|v| v.name.clone()));
                    curr_tags.extend(tags.iter().map(|v| v.name.clone()));
                    SModel {
                        name,
                        columns,
                        tags,
                    }
                });
        }

        Ok(models)
    }

    fn column_templates(&self) -> Vec<(String, String)> {
        self.columns
            .iter()
            .enumerate()
            .flat_map(|(idx, col)| {
                let mut templates = Vec::new();
                if col.name.contains("${") {
                    templates.push((format!("col_{idx}_name"), col.name.replace("${", "{")));
                }
                if col.r#type.contains("${") {
                    templates.push((format!("col_{idx}_type"), col.r#type.replace("${", "{")))
                }
                if let Some(encode) = col.encode.as_ref().filter(|s| s.contains("${")) {
                    templates.push((format!("col_{idx}_encode"), encode.replace("${", "{")));
                }
                if let Some(encode) = col.compress.as_ref().filter(|s| s.contains("${")) {
                    templates.push((format!("col_{idx}_compress"), encode.replace("${", "{")));
                }
                if let Some(encode) = col.level.as_ref().filter(|s| s.contains("${")) {
                    templates.push((format!("col_{idx}_level"), encode.replace("${", "{")));
                }
                templates
            })
            .collect()
    }

    fn render_column(&self, template: &TinyTemplate, ctx: &Value) -> anyhow::Result<Vec<Column>> {
        self.columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                Ok(Column {
                    name: if col.name.contains("${") {
                        template
                            .render_value(&format!("col_{idx}_name"), ctx)
                            .context("render stable column name error")?
                    } else {
                        col.name.clone()
                    },
                    r#type: if col.r#type.contains("${") {
                        template
                            .render_value(&format!("col_{idx}_type"), ctx)
                            .context("render stable column type error")?
                    } else {
                        col.r#type.clone()
                    },
                    encode: col
                        .encode
                        .as_ref()
                        .filter(|v| v.contains("${"))
                        .map(|_| {
                            template
                                .render_value(&format!("col_{idx}_encode"), ctx)
                                .context("render stable column encode error")
                        })
                        .transpose()?,
                    compress: col
                        .compress
                        .as_ref()
                        .filter(|v| v.contains("${"))
                        .map(|_| {
                            template
                                .render_value(&format!("col_{idx}_compress"), ctx)
                                .context("render stable column compress error")
                        })
                        .transpose()?,
                    level: col
                        .level
                        .as_ref()
                        .filter(|v| v.contains("${"))
                        .map(|_| {
                            template
                                .render_value(&format!("col_{idx}_level"), ctx)
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
                let mut templates = Vec::new();
                if tag.name.contains("${") {
                    templates.push((format!("tag_{idx}_name"), tag.name.replace("${", "{")));
                }
                if tag.r#type.contains("${") {
                    templates.push((format!("tag_{idx}_type"), tag.r#type.replace("${", "{")));
                }
                templates
            })
            .collect()
    }

    fn render_tags(&self, template: &TinyTemplate, ctx: &Value) -> anyhow::Result<Vec<Tag>> {
        self.tags
            .iter()
            .enumerate()
            .map(|(idx, tag)| {
                Ok(Tag {
                    name: if tag.name.contains("${") {
                        template
                            .render_value(&format!("tag_{idx}_name"), ctx)
                            .context("render stable tag name error")?
                    } else {
                        tag.name.clone()
                    },
                    r#type: if tag.r#type.contains("${") {
                        template
                            .render_value(&format!("tag_{idx}_type"), ctx)
                            .context("render stable tag type error")?
                    } else {
                        tag.r#type.clone()
                    },
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
                let mut res = format!("`{}` {}", col.name, col.r#type);
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
            .map(|tag| format!("`{}` {}", tag.name, tag.r#type))
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
            "CREATE STABLE IF NOT EXISTS `abc` \
            (`col1` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', \
            `col2` VARCHAR(128) COMPRESS 'lz4') \
            TAGS (`tag1` INT, `tag2` VARCHAR(128));"
        );
        Ok(())
    }

    #[test]
    fn apply_single_column_test() -> anyhow::Result<()> {
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
            &[json::json!({
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
        assert_eq!(
            models
                .get("abc")
                .context("model not found")?
                .create_stable_sql(),
            "CREATE STABLE IF NOT EXISTS `abc` \
            (`col1` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', \
            `col2` VARCHAR(128) COMPRESS 'lz4') \
            TAGS (`tag1` INT, `tag2` VARCHAR(128));"
        );
        Ok(())
    }

    #[test]
    fn apply_multi_column_test() -> anyhow::Result<()> {
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
            &[
                json::json!({
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
                }),
                json::json!({
                    "abc_x": "abc",
                    "col1_x": "col11",
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
                }),
            ],
            &TableOptions::default(),
        )?;
        let sql = models
            .get("abc")
            .context("model not found")?
            .create_stable_sql();
        assert_eq!(
            sql,
            "CREATE STABLE IF NOT EXISTS `abc` \
            (`col1` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', \
            `col2` VARCHAR(128) COMPRESS 'lz4', \
            `col11` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium') \
            TAGS (`tag1` INT, `tag2` VARCHAR(128));"
        );
        Ok(())
    }
}
