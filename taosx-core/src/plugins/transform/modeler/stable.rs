use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
};

use anyhow::{anyhow, Context};
use arrow::array::RecordBatch;
use faststr::FastStr;
use taos::Itertools;

use crate::{
    expr::Expr,
    plugins::transform::{modeler::template_to_expr, TableOptions},
};

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(from = "String", into = "String")]
pub struct FastStrExpr {
    template: FastStr,
    #[serde(skip)]
    expr: OnceLock<Expr>,
}

impl FastStrExpr {
    fn new(name: FastStr) -> Self {
        Self {
            template: name,
            expr: OnceLock::new(),
        }
    }

    fn eval(&self, records: &RecordBatch, row: usize) -> anyhow::Result<FastStr> {
        if !self.is_expr() {
            return Ok(self.template.clone());
        }

        let expr = match self.expr.get() {
            Some(expr) => expr,
            None => {
                let expr =
                    template_to_expr(&self.template).context("build stable name expr error")?;
                self.expr.get_or_init(|| expr)
            }
        };
        eval(expr, records, row).map(|s| s.into())
    }

    fn is_expr(&self) -> bool {
        self.template.contains("${")
    }
}

impl From<FastStr> for FastStrExpr {
    fn from(value: FastStr) -> Self {
        Self::new(value)
    }
}

impl From<&'static str> for FastStrExpr {
    fn from(value: &'static str) -> Self {
        Self::new(FastStr::from_static_str(value))
    }
}

impl From<String> for FastStrExpr {
    fn from(value: String) -> Self {
        Self {
            template: FastStr::from_string(value),
            expr: OnceLock::new(),
        }
    }
}

impl From<FastStrExpr> for String {
    fn from(value: FastStrExpr) -> Self {
        value.template.into()
    }
}

impl std::fmt::Display for FastStrExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.template.fmt(f)
    }
}

impl std::ops::Deref for FastStrExpr {
    type Target = FastStr;

    fn deref(&self) -> &Self::Target {
        &self.template
    }
}

fn eval(expr: &Expr, records: &RecordBatch, row: usize) -> anyhow::Result<String> {
    expr.eval_batch_row(records, row)
        .with_context(|| format!("eval expr {} error", expr.expr))?
        .into_string()
        .map_err(|e| anyhow!("expr {e} not string"))
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct STableModel {
    pub name: FastStrExpr,
    pub columns: Vec<Column>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Tag {
    pub name: FastStrExpr,
    pub r#type: FastStrExpr,
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: FastStrExpr,
    pub r#type: FastStrExpr,
    pub encode: Option<FastStrExpr>,
    pub compress: Option<FastStrExpr>,
    pub level: Option<FastStrExpr>,
}

impl STableModel {
    pub fn name(&self) -> &str {
        &self.name.template
    }

    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    /// 每行一个 json map
    pub fn apply(
        &self,
        records: &RecordBatch,
        opts: &TableOptions,
    ) -> anyhow::Result<HashMap<FastStr, Self>> {
        let mut models = HashMap::with_capacity(records.num_rows());
        let mut curr_columns = HashSet::with_capacity(records.num_rows());
        let mut curr_tags = HashSet::new();
        for row in 0..records.num_rows() {
            let name = self.name.eval(records, row)?;
            let name: FastStr = opts.canonical_table_name(&name).to_string().into();
            let columns = self.render_column(records, row)?;
            let tags = self.render_tags(records, row)?;
            models
                .entry(name.clone())
                .and_modify(|model: &mut STableModel| {
                    for col in &columns {
                        if !curr_columns.contains(&col.name.template) {
                            curr_columns.insert(col.name.template.clone());
                            model.columns.push(col.clone());
                        }
                    }
                    for tag in &tags {
                        if !curr_tags.contains(&tag.name.template) {
                            curr_tags.insert(tag.name.template.clone());
                            model.tags.push(tag.clone());
                        }
                    }
                })
                .or_insert_with(|| {
                    curr_columns.extend(columns.iter().map(|v| v.name.template.clone()));
                    curr_tags.extend(tags.iter().map(|v| v.name.template.clone()));
                    STableModel {
                        name: FastStrExpr::new(name.clone()),
                        columns,
                        tags,
                    }
                });
        }

        Ok(models)
    }

    fn render_column(&self, records: &RecordBatch, row: usize) -> anyhow::Result<Vec<Column>> {
        self.columns
            .iter()
            .map(|col| {
                Ok(Column {
                    name: col.name.eval(records, row)?.into(),
                    r#type: col.r#type.eval(records, row)?.into(),
                    encode: col
                        .encode
                        .as_ref()
                        .map(|encode| encode.eval(records, row))
                        .transpose()?
                        .map(|s| s.into()),
                    compress: col
                        .compress
                        .as_ref()
                        .map(|compress| compress.eval(records, row))
                        .transpose()?
                        .map(|s| s.into()),
                    level: col
                        .level
                        .as_ref()
                        .map(|level| level.eval(records, row))
                        .transpose()?
                        .map(|s| s.into()),
                })
            })
            .collect::<anyhow::Result<_>>()
    }

    fn render_tags(&self, records: &RecordBatch, row: usize) -> anyhow::Result<Vec<Tag>> {
        self.tags
            .iter()
            .map(|tag| {
                Ok(Tag {
                    name: tag.name.eval(records, row)?.into(),
                    r#type: tag.r#type.eval(records, row)?.into(),
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
                let mut res = format!("`{}` {}", col.name.template, col.r#type.template);
                if let Some(encode) = col
                    .encode
                    .as_ref()
                    .map(|encode| format!(" ENCODE '{}'", encode.template))
                {
                    res.push_str(&encode);
                }
                if let Some(compress) = col
                    .compress
                    .as_ref()
                    .map(|compress| format!(" COMPRESS '{}'", compress.template))
                {
                    res.push_str(&compress);
                }
                if let Some(level) = col
                    .level
                    .as_ref()
                    .map(|level| format!(" LEVEL '{}'", level.template))
                {
                    res.push_str(&level);
                }
                res
            })
            .join(", ");
        let tags = self
            .tags
            .iter()
            .map(|tag| format!("`{}` {}", tag.name.template, tag.r#type.template))
            .join(", ");
        format!("CREATE STABLE IF NOT EXISTS `{name}` ({columns}) TAGS ({tags});")
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_stable_sql_test() -> anyhow::Result<()> {
        let model = STableModel {
            name: "abc".into(),
            columns: vec![
                Column {
                    name: "col1".into(),
                    r#type: "TIMESTAMP".into(),
                    encode: Some("delta-i".into()),
                    compress: Some("lz4".into()),
                    level: Some("medium".into()),
                },
                Column {
                    name: "col2".into(),
                    r#type: "VARCHAR(128)".into(),
                    compress: Some("lz4".into()),
                    ..Default::default()
                },
            ],
            tags: vec![
                Tag {
                    name: "tag1".into(),
                    r#type: "INT".into(),
                },
                Tag {
                    name: "tag2".into(),
                    r#type: "VARCHAR(128)".into(),
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
        let model = STableModel {
            name: "${abc_x}".into(),
            columns: vec![
                Column {
                    name: "${col1_x}".into(),
                    r#type: "${TIMESTAMP_x}".into(),
                    encode: Some("${delta_x}".into()),
                    compress: Some("${lz4_x}".into()),
                    level: Some("${medium_x}".into()),
                },
                Column {
                    name: "${col2_x}".into(),
                    r#type: "${varchar_x}".into(),
                    compress: Some("${lz4_x}".into()),
                    ..Default::default()
                },
            ],
            tags: vec![
                Tag {
                    name: "${tag1_x}".into(),
                    r#type: "${INT_x}".into(),
                },
                Tag {
                    name: "${tag2_x}".into(),
                    r#type: "${varchar_x}".into(),
                },
            ],
        };
        let records = arrow::array::record_batch!(
            ("abc_x", Utf8, ["abc"]),
            ("col1_x", Utf8, ["col1"]),
            ("TIMESTAMP_x", Utf8, ["TIMESTAMP"]),
            ("delta_x", Utf8, ["delta-i"]),
            ("lz4_x", Utf8, ["lz4"]),
            ("medium_x", Utf8, ["medium"]),
            ("col2_x", Utf8, ["col2"]),
            ("varchar_x", Utf8, ["VARCHAR(128)"]),
            ("tag1_x", Utf8, ["tag1"]),
            ("INT_x", Utf8, ["INT"]),
            ("tag2_x", Utf8, ["tag2"])
        )
        .unwrap();
        let models = model.apply(&records, &TableOptions::default())?;
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
        let model = STableModel {
            name: "${abc_x}".into(),
            columns: vec![
                Column {
                    name: "${col1_x}".into(),
                    r#type: "${TIMESTAMP_x}".into(),
                    encode: Some("${delta_x}".into()),
                    compress: Some("${lz4_x}".into()),
                    level: Some("${medium_x}".into()),
                },
                Column {
                    name: "${col2_x}".into(),
                    r#type: "${VARCHAR_x}".into(),
                    compress: Some("${lz4_x}".into()),
                    ..Default::default()
                },
            ],
            tags: vec![
                Tag {
                    name: "${tag1_x}".into(),
                    r#type: "${INT_x}".into(),
                },
                Tag {
                    name: "${tag2_x}".into(),
                    r#type: "${VARCHAR_x}".into(),
                },
            ],
        };
        let records = arrow::array::record_batch!(
            ("abc_x", Utf8, ["abc", "abc"]),
            ("col1_x", Utf8, ["col1", "col11"]),
            ("TIMESTAMP_x", Utf8, ["TIMESTAMP", "TIMESTAMP"]),
            ("delta_x", Utf8, ["delta-i", "delta-i"]),
            ("lz4_x", Utf8, ["lz4", "lz4"]),
            ("medium_x", Utf8, ["medium", "medium"]),
            ("col2_x", Utf8, ["col2", "col2"]),
            ("VARCHAR_x", Utf8, ["VARCHAR(128)", "VARCHAR(128)"]),
            ("tag1_x", Utf8, ["tag1", "tag1"]),
            ("INT_x", Utf8, ["INT", "INT"]),
            ("tag2_x", Utf8, ["tag2", "tag2"])
        )
        .unwrap();
        let models = model.apply(&records, &TableOptions::default())?;
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
