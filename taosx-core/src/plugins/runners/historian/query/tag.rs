use tiberius::Row;

#[derive(Debug)]
pub struct TagMeta {
    pub name: String,
    pub description: String,
}

impl TagMeta {
    pub fn from_row(row: &Row) -> anyhow::Result<Self> {
        let tag_name = (row.try_get("TagName")? as Option<&str>)
            .ok_or(anyhow::anyhow!("TagName not exists in the row"))?
            .to_string();
        let description = (row.try_get("Description")? as Option<&str>)
            .unwrap_or("")
            .to_string();

        Ok(TagMeta {
            name: tag_name,
            description,
        })
    }
}