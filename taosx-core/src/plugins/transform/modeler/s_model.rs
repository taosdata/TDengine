use std::collections::HashMap;

use anyhow::Context;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct SModeler {
    name: String,
    columns: Vec<Column>,
    tags: Vec<Tag>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Tag {
    name: String,
    r#type: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Column {
    name: String,
    r#type: String,
    encode: Option<String>,
    compress: Option<String>,
    level: Option<String>,
}

impl SModeler {
    pub fn name(&self) -> &str {
        &self.name
    }
    /// 每行一个 json map
    pub fn apply(
        &self,
        batch: &[serde_json::Map<String, serde_json::Value>],
    ) -> anyhow::Result<HashMap<String, Self>> {
        let mut template = tinytemplate::TinyTemplate::new();
        let name = self.name.replace("${", "{");
        template
            .add_template("name", &name)
            .context("add name template error")?;

        let col_templates = self
            .columns
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
            .collect::<Vec<(String, String)>>();
        for (name, text) in &col_templates {
            template
                .add_template(name, text)
                .context("add template error")?;
        }

        let tag_templates = self
            .tags
            .iter()
            .enumerate()
            .flat_map(|(idx, tag)| {
                vec![
                    (format!("tag_{idx}_name"), tag.name.replace("${", "{")),
                    (format!("tag_{idx}_type"), tag.r#type.replace("${", "{")),
                ]
            })
            .collect::<Vec<(String, String)>>();
        for (name, text) in &tag_templates {
            template
                .add_template(name, text)
                .context("add template error")?;
        }

        let mut res = HashMap::new();
        for map in batch.iter() {
            let name = template.render("name", map).context("render error")?;
            let mut columns = Vec::new();
            for (idx, col) in self.columns.iter().enumerate() {
                columns.push(Column {
                    name: template
                        .render(&format!("col_{idx}_name"), map)
                        .context("render error")?,
                    r#type: template
                        .render(&format!("col_{idx}_type"), map)
                        .context("render error")?,
                    encode: col
                        .encode
                        .as_ref()
                        .map(|_| {
                            template
                                .render(&format!("col_{idx}_encode"), map)
                                .context("render error")
                        })
                        .transpose()?,
                    compress: col
                        .compress
                        .as_ref()
                        .map(|_| {
                            template
                                .render(&format!("col_{idx}_compress"), map)
                                .context("render error")
                        })
                        .transpose()?,
                    level: col
                        .level
                        .as_ref()
                        .map(|_| {
                            template
                                .render(&format!("col_{idx}_level"), map)
                                .context("render error")
                        })
                        .transpose()?,
                })
            }
            let mut tags = Vec::new();
            for idx in 0..self.tags.len() {
                tags.push(Tag {
                    name: template
                        .render(&format!("tag_{idx}_name"), map)
                        .context("render error")?,
                    r#type: template
                        .render(&format!("tag_{idx}_type"), map)
                        .context("render error")?,
                });
            }
            res.insert(
                name.clone(),
                SModeler {
                    name,
                    columns,
                    tags,
                },
            );
        }
        Ok(res)
    }

    pub fn create_stable_sql(&self) -> String {
        todo!()
    }
}
