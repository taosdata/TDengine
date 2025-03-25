use anyhow::bail;
use csv_async::StringRecord;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;

use crate::plugins::runners::opc::csv::column::CsvColumn;
use crate::runners::opc::OpcType;

pub const UA_HEADER: [&str; 16] = [
    "No.",
    "point_id",
    "enabled",
    "stable",
    "tbname",
    "value_col",
    "value_transform",
    "type",
    "quality_col",
    "ts_col",
    "ts_transform",
    "request_ts_col",
    "request_ts_transform",
    "received_ts_col",
    "received_ts_transform",
    "tag::VARCHAR(200)::name",
];
pub const UA_ROW: [&str; 16] = [
    "",
    "",
    "",
    "opc_{type}",
    "t_{ns}_{id}",
    "val",
    "",
    "",
    "quality",
    "ts",
    "",
    "qts",
    "",
    "rts",
    "",
    "",
];

pub const DA_HEADER: [&str; 16] = [
    "No.",
    "tag_name",
    "enabled",
    "stable",
    "tbname",
    "value_col",
    "value_transform",
    "type",
    "quality_col",
    "ts_col",
    "ts_transform",
    "request_ts_col",
    "request_ts_transform",
    "received_ts_col",
    "received_ts_transform",
    "tag::VARCHAR(200)::name",
];

pub const DA_ROW: [&str; 16] = [
    "",
    "",
    "",
    "opc_{type}",
    "t_{tag_name}",
    "val",
    "",
    "",
    "quality",
    "ts",
    "",
    "qts",
    "",
    "rts",
    "",
    "",
];

pub fn get_template(opc_type: OpcType, with_demo: bool) -> String {
    match opc_type {
        OpcType::OPCUA => ua_template(with_demo),
        OpcType::OPCDA => da_template(with_demo),
        OpcType::FAKE => unimplemented!("invalid opc type"),
    }
}

fn ua_template(with_demo: bool) -> String {
    let mut template = UA_HEADER.iter().join(",");
    template.push('\n');
    if with_demo {
        template.push_str("1,ns=3;i=1010,1,opc_{type},t_{ns}_{id},val,val*1.8+32,double,quality,ts,,qts,,rts,,temperature\n");
        template.push_str("2,ns=3;i=1011,1,opc_{type},t_{ns}_{id},val,val + 10,int,quality,ts,ts+8*3600*1000,qts,qts+8*3600*1000,rts,rts+8*3600*1000,pressure\n");
        template.push_str("3,ns=5;s=abcd,1,opc_{type},t_{ns}_{id},val,,,quality,ts,ts-6*1000,qts,qts-6*1000,rts,rts-6*1000,current\n");
    }
    template
}

fn da_template(with_demo: bool) -> String {
    let mut template = DA_HEADER.iter().join(",");
    template.push('\n');
    if with_demo {
        template.push_str("1,root.parent.temperature,1,opc_{type},t_{tag_name},val,val*1.8+32,float,quality,ts,,qts,,rts,,temperature\n");
        template.push_str("2,root.parent.pressure,1,opc_{type},t_{tag_name},val,val+10,,quality,ts,ts+8*3600*1000,qts,qts+8*3600*1000,rts,rts+8*3600*1000,pressure\n");
        template.push_str("3,root.parent.current,1,opc_{type},t_{tag_name},val,,,quality,ts,ts-6*1000,qts,qts-6*1000,rts,rts-6*1000,current\n");
    }
    template
}

#[derive(Debug)]
pub struct CsvHeader {
    opc_type: OpcType,
    columns: Vec<CsvColumn>,
    column_map: LinkedHashMap<String, usize>,
    primary_timestamp_index: Option<usize>,
    point_id_index: usize,
    enabled_index: usize,
}

impl CsvHeader {
    /// create csv header
    pub fn try_new(opc_type: OpcType, header: &StringRecord) -> anyhow::Result<Self> {
        let mut columns = Vec::new();
        let mut column_map = LinkedHashMap::new();
        let mut primary_ts = None;
        let mut point_id_index = None;
        let mut enabled_index = None;

        for (index, name) in header.iter().enumerate() {
            if opc_type == OpcType::OPCUA && name == "point_id" {
                point_id_index = Some(index);
            }
            if opc_type == OpcType::OPCDA && name == "tag_name" {
                point_id_index = Some(index);
            }
            if name == "enabled" {
                enabled_index = Some(index);
            }

            let mut col = CsvColumn::try_new(name, index)?;
            // if the header contains ts_col and received_ts_col, use the first one as primary key
            // if neither ts_col nor received_ts_col is found, the primary_ts will be None
            if col.is_timestamp && primary_ts.is_none() {
                col.is_primary_key = true;
                primary_ts = Some(index);
            }
            let col_name = col.name.clone();
            let is_duplicated = column_map.insert(col_name.clone(), index);
            // check if the column name is duplicated
            if is_duplicated.is_some() {
                bail!("duplicated column name: {}", col_name);
            }
            columns.push(col);
        }

        Ok(Self {
            opc_type,
            columns,
            column_map,
            primary_timestamp_index: primary_ts,
            point_id_index: point_id_index.ok_or(anyhow::anyhow!("point_id is required"))?,
            enabled_index: enabled_index.unwrap_or(1),
        })
    }

    pub fn check_required_columns(&self) -> anyhow::Result<()> {
        match self.opc_type {
            OpcType::OPCUA => {
                if !self.column_map.contains_key("point_id") {
                    bail!("point_id is required");
                }
            }
            OpcType::OPCDA => {
                if !self.column_map.contains_key("tag_name") {
                    bail!("tag_name is required");
                }
            }
            OpcType::FAKE => {}
        }

        if !self.column_map.contains_key("stable") {
            bail!("stable is required");
        }

        if !self.column_map.contains_key("tbname") {
            bail!("tbname is required");
        }

        Ok(())
    }

    pub fn id_index(&self) -> usize {
        self.point_id_index
    }

    pub fn enabled_index(&self) -> usize {
        self.enabled_index
    }

    pub fn get_opc_type(&self) -> OpcType {
        self.opc_type
    }

    pub fn get_columns(&self) -> Vec<&CsvColumn> {
        self.columns.iter().collect()
    }

    pub fn get_column(&self, col_name: &str) -> Option<&CsvColumn> {
        self.column_map
            .get(col_name)
            .and_then(|index| self.columns.get(*index))
    }

    pub fn get_primary_timestamp(&self) -> Option<&CsvColumn> {
        self.primary_timestamp_index
            .and_then(|index| self.columns.get(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taosx_ipc::prelude::IpcDataType;

    #[test]
    fn test_get_template() {
        let template = get_template(OpcType::OPCUA, true);
        let lines = template.trim().split("\n").collect_vec();
        assert_eq!(lines.len(), 4);
        let header = StringRecord::from(lines[0].split(",").collect_vec());
        let header = CsvHeader::try_new(OpcType::OPCUA, &header).unwrap();
        assert_eq!(header.opc_type, OpcType::OPCUA);
        assert_eq!(header.columns.len(), 16);
        assert_eq!(header.get_column("point_id").unwrap().index, 1);
        assert_eq!(header.get_primary_timestamp().unwrap().name, "ts_col");
        assert_eq!(header.get_column("ts_transform").unwrap().index, 10);
        assert_eq!(header.get_column("request_ts_col").unwrap().index, 11);
        assert_eq!(header.get_column("request_ts_transform").unwrap().index, 12);
        assert_eq!(header.get_column("received_ts_col").unwrap().index, 13);
        assert_eq!(
            header.get_column("received_ts_transform").unwrap().index,
            14
        );

        let template = get_template(OpcType::OPCDA, true);
        let lines = template.trim().split("\n").collect_vec();
        assert_eq!(lines.len(), 4);
        let header = StringRecord::from(lines[0].split(",").collect_vec());
        let header = CsvHeader::try_new(OpcType::OPCDA, &header).unwrap();
        assert_eq!(header.opc_type, OpcType::OPCDA);
        assert_eq!(header.columns.len(), 16);
        assert_eq!(header.get_column("tag_name").unwrap().index, 1);
        assert_eq!(header.get_primary_timestamp().unwrap().name, "ts_col");
        assert_eq!(header.get_column("ts_transform").unwrap().index, 10);
        assert_eq!(header.get_column("request_ts_col").unwrap().index, 11);
        assert_eq!(header.get_column("request_ts_transform").unwrap().index, 12);
        assert_eq!(header.get_column("received_ts_col").unwrap().index, 13);
        assert_eq!(
            header.get_column("received_ts_transform").unwrap().index,
            14
        );
    }

    #[tokio::test]
    async fn test_try_new() {
        let header = csv_async::StringRecord::from(vec![
            "0",
            "point_id",
            "enabled",
            "stable",
            "tbname",
            "value_col",
            "value_transform",
            "type",
            "quality_col",
            "ts_col",
            "received_ts_col",
            "ts_transform",
            "received_ts_transform",
            "tag::VARCHAR(200)::name",
        ]);

        let csv_header = CsvHeader::try_new(OpcType::OPCUA, &header).unwrap();

        let primary_ts = csv_header.get_primary_timestamp().unwrap();
        assert_eq!(primary_ts.index, 9);
        assert_eq!(primary_ts.name, "ts_col");

        assert_eq!(csv_header.column_map.len(), 14);

        let col = csv_header.get_column("point_id").unwrap();
        assert_eq!(col.index, 1);

        let col = csv_header.get_column("enabled").unwrap();
        assert_eq!(col.index, 2);

        let col = csv_header.get_column("name").unwrap();
        assert_eq!(col.index, 13);
        assert!(col.is_tag);
        assert_eq!(col.tag_type, Some(IpcDataType::VarChar(200)));
    }

    #[tokio::test]
    async fn test_check_required_columns() {
        let header = csv_async::StringRecord::from(vec!["point_id", "stable", "tbname"]);
        let csv_header = CsvHeader::try_new(OpcType::OPCUA, &header).unwrap();

        let res = csv_header.check_required_columns();
        assert!(res.is_ok());
    }
}
