use taos::MetaUnit;

#[derive(Debug, serde::Serialize)]
pub struct Message {
    #[serde(flatten)]
    pub inner: MessageInner,
    pub offset: MessageOffset,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip)]
    pub vars: serde_json::Map<String, serde_json::Value>,
}

impl Message {
    pub fn new_data(
        data: serde_json::Map<String, serde_json::Value>,
        offset: MessageOffset,
        table_name: Option<&str>,
    ) -> Self {
        let json = serde_json::Value::from(data);
        let mut vars = serde_json::Map::from_iter([
            ("database".into(), offset.database().into()),
            ("tmq_topic".into(), offset.topic().into()),
            ("vgroup_id".into(), offset.vgroup_id().into()),
        ]);
        if let Some(table) = table_name {
            vars.insert("table".into(), table.into());
        }

        Self {
            inner: MessageInner::Data(json),
            offset,
            sql: None,
            vars,
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageInner {
    Meta(serde_json::Value),
    Data(serde_json::Value),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MessageOffset {
    pub database: String,
    pub topic: String,
    #[serde(rename = "vgroupId")]
    pub vgroup_id: i32,
    pub offset: i64,
}

impl MessageOffset {
    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn vgroup_id(&self) -> i32 {
        self.vgroup_id
    }
}

impl From<&taos::Offset> for MessageOffset {
    fn from(offset: &taos::Offset) -> Self {
        use taos::IsOffset;
        Self {
            database: offset.database().to_string(),
            topic: offset.topic().to_string(),
            vgroup_id: offset.vgroup_id(),
            offset: offset.offset(),
        }
    }
}

impl From<(MetaUnit, MessageOffset)> for Message {
    fn from((value, offset): (MetaUnit, MessageOffset)) -> Self {
        let sql = value.to_string();
        let mut vars = serde_json::Map::from_iter([
            ("database".into(), offset.database().into()),
            ("tmq_topic".into(), offset.topic().into()),
            ("vgroup_id".into(), offset.vgroup_id().into()),
        ]);
        match &value {
            MetaUnit::Create(create) => match create {
                taos::MetaCreate::Super { table_name, .. } => {
                    vars.insert("stable".into(), table_name.clone().into());
                }
                taos::MetaCreate::Child {
                    table_name,
                    using,
                    tags,
                    ..
                } => {
                    vars.insert("table".into(), table_name.clone().into());
                    vars.insert("stable".into(), using.clone().into());
                    for tag in tags {
                        vars.insert(tag.field.name().into(), tag.value.clone());
                    }
                }
                taos::MetaCreate::Normal { table_name, .. } => {
                    vars.insert("table".into(), table_name.clone().into());
                }
            },
            MetaUnit::Alter(alter) => {
                vars.insert("table".into(), alter.table_name.clone().into());
            }
            MetaUnit::Drop(drop) => match drop {
                taos::MetaDrop::Super { table_name } => {
                    vars.insert("stable".into(), table_name.clone().into());
                }
                taos::MetaDrop::Other { table_name_list } => {
                    vars.insert("table".into(), table_name_list.join("_").into());
                }
            },
            MetaUnit::Delete(_) => {}
        }
        let json = serde_json::to_value(value).expect("json meta must impl serialize trait");

        Self {
            inner: MessageInner::Meta(json),
            offset,
            sql: Some(sql),
            vars,
        }
    }
}
