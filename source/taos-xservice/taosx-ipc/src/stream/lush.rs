use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteMessage {
    // 子表 ID
    pub table: String,
    // delete table 语句 where 后面的条件
    pub condition: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterMessage {
    // 子表 ID
    pub table: String,
    // alter table 的子句，比如： set tag, add column.
    pub alter_table_clause: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropMessage {
    // 子表 ID
    pub table: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertMessage {
    // 子表 ID
    pub table: String,
    // 列名和值部分
    pub column_values: String,
}

impl AlterMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }
}

impl DropMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }
}

impl DeleteMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }
}

impl InsertMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }

    pub fn column_values(&self) -> &str {
        self.column_values.as_str()
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Serialize, Deserialize)]
pub enum LushMessageControl {
    DELETE(DeleteMessage),
    ALTER(AlterMessage),
    DROP(DropMessage),
    INSERT(InsertMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    // DeleteMessage tests
    #[test]
    fn test_delete_message_creation() {
        let msg = DeleteMessage {
            table: "sensor_001".to_string(),
            condition: "ts < '2024-01-01'".to_string(),
        };
        assert_eq!(msg.table, "sensor_001");
        assert_eq!(msg.condition, "ts < '2024-01-01'");
    }

    #[test]
    fn test_delete_message_table_id() {
        let msg = DeleteMessage {
            table: "test_table".to_string(),
            condition: "id = 123".to_string(),
        };
        assert_eq!(msg.table_id(), "test_table");
    }

    #[test]
    fn test_delete_message_debug() {
        let msg = DeleteMessage {
            table: "table1".to_string(),
            condition: "value > 100".to_string(),
        };
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("DeleteMessage"));
        assert!(debug_str.contains("table1"));
    }

    #[test]
    fn test_delete_message_serialize() {
        let msg = DeleteMessage {
            table: "sensor".to_string(),
            condition: "ts < NOW()".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("sensor"));
        assert!(json.contains("ts < NOW()"));
    }

    #[test]
    fn test_delete_message_deserialize() {
        let json = r#"{"table":"sensor_001","condition":"ts < '2024-01-01'"}"#;
        let msg: DeleteMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.table, "sensor_001");
        assert_eq!(msg.condition, "ts < '2024-01-01'");
    }

    // AlterMessage tests
    #[test]
    fn test_alter_message_creation() {
        let msg = AlterMessage {
            table: "metrics".to_string(),
            alter_table_clause: "ADD COLUMN temperature DOUBLE".to_string(),
        };
        assert_eq!(msg.table, "metrics");
        assert_eq!(msg.alter_table_clause, "ADD COLUMN temperature DOUBLE");
    }

    #[test]
    fn test_alter_message_table_id() {
        let msg = AlterMessage {
            table: "test_table".to_string(),
            alter_table_clause: "SET TAG location = 'NYC'".to_string(),
        };
        assert_eq!(msg.table_id(), "test_table");
    }

    #[test]
    fn test_alter_message_debug() {
        let msg = AlterMessage {
            table: "sensors".to_string(),
            alter_table_clause: "DROP COLUMN old_col".to_string(),
        };
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("AlterMessage"));
    }

    #[test]
    fn test_alter_message_serialize_deserialize() {
        let msg = AlterMessage {
            table: "device_data".to_string(),
            alter_table_clause: "RENAME COLUMN old TO new".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: AlterMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.table, msg.table);
        assert_eq!(deserialized.alter_table_clause, msg.alter_table_clause);
    }

    // DropMessage tests
    #[test]
    fn test_drop_message_creation() {
        let msg = DropMessage {
            table: "old_sensor".to_string(),
        };
        assert_eq!(msg.table, "old_sensor");
    }

    #[test]
    fn test_drop_message_table_id() {
        let msg = DropMessage {
            table: "to_be_dropped".to_string(),
        };
        assert_eq!(msg.table_id(), "to_be_dropped");
    }

    #[test]
    fn test_drop_message_debug() {
        let msg = DropMessage {
            table: "temp_table".to_string(),
        };
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("DropMessage"));
        assert!(debug_str.contains("temp_table"));
    }

    #[test]
    fn test_drop_message_serialize_deserialize() {
        let msg = DropMessage {
            table: "sensor_123".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: DropMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.table, msg.table);
    }

    // InsertMessage tests
    #[test]
    fn test_insert_message_creation() {
        let msg = InsertMessage {
            table: "sensor_data".to_string(),
            column_values: "(ts, temperature, humidity) VALUES (NOW(), 25.5, 60.0)".to_string(),
        };
        assert_eq!(msg.table, "sensor_data");
        assert!(msg.column_values.contains("25.5"));
    }

    #[test]
    fn test_insert_message_table_id() {
        let msg = InsertMessage {
            table: "metrics".to_string(),
            column_values: "(value) VALUES (100)".to_string(),
        };
        assert_eq!(msg.table_id(), "metrics");
    }

    #[test]
    fn test_insert_message_column_values() {
        let msg = InsertMessage {
            table: "data".to_string(),
            column_values: "(col1, col2) VALUES (1, 2)".to_string(),
        };
        assert_eq!(msg.column_values(), "(col1, col2) VALUES (1, 2)");
    }

    #[test]
    fn test_insert_message_debug() {
        let msg = InsertMessage {
            table: "test".to_string(),
            column_values: "(x) VALUES (42)".to_string(),
        };
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("InsertMessage"));
    }

    #[test]
    fn test_insert_message_serialize_deserialize() {
        let msg = InsertMessage {
            table: "sensor_001".to_string(),
            column_values: "(ts, val) VALUES ('2024-01-01', 123)".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: InsertMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.table, msg.table);
        assert_eq!(deserialized.column_values, msg.column_values);
    }

    // LushMessageControl enum tests
    #[test]
    fn test_lush_message_control_delete_variant() {
        let delete_msg = DeleteMessage {
            table: "test".to_string(),
            condition: "id = 1".to_string(),
        };
        let control = LushMessageControl::DELETE(delete_msg);

        match control {
            LushMessageControl::DELETE(msg) => {
                assert_eq!(msg.table, "test");
            }
            _ => panic!("Expected DELETE variant"),
        }
    }

    #[test]
    fn test_lush_message_control_alter_variant() {
        let alter_msg = AlterMessage {
            table: "test".to_string(),
            alter_table_clause: "ADD COLUMN x INT".to_string(),
        };
        let control = LushMessageControl::ALTER(alter_msg);

        match control {
            LushMessageControl::ALTER(msg) => {
                assert_eq!(msg.table, "test");
            }
            _ => panic!("Expected ALTER variant"),
        }
    }

    #[test]
    fn test_lush_message_control_drop_variant() {
        let drop_msg = DropMessage {
            table: "old_table".to_string(),
        };
        let control = LushMessageControl::DROP(drop_msg);

        match control {
            LushMessageControl::DROP(msg) => {
                assert_eq!(msg.table, "old_table");
            }
            _ => panic!("Expected DROP variant"),
        }
    }

    #[test]
    fn test_lush_message_control_insert_variant() {
        let insert_msg = InsertMessage {
            table: "data".to_string(),
            column_values: "(x) VALUES (1)".to_string(),
        };
        let control = LushMessageControl::INSERT(insert_msg);

        match control {
            LushMessageControl::INSERT(msg) => {
                assert_eq!(msg.table, "data");
            }
            _ => panic!("Expected INSERT variant"),
        }
    }

    #[test]
    fn test_lush_message_control_debug() {
        let delete_msg = DeleteMessage {
            table: "test".to_string(),
            condition: "id = 1".to_string(),
        };
        let control = LushMessageControl::DELETE(delete_msg);
        let debug_str = format!("{:?}", control);
        assert!(debug_str.contains("DELETE"));
    }

    #[test]
    fn test_lush_message_control_serialize_delete() {
        let delete_msg = DeleteMessage {
            table: "sensor".to_string(),
            condition: "ts < NOW()".to_string(),
        };
        let control = LushMessageControl::DELETE(delete_msg);
        let json = serde_json::to_string(&control).unwrap();
        assert!(json.contains("DELETE"));
        assert!(json.contains("sensor"));
    }

    #[test]
    fn test_lush_message_control_deserialize() {
        let json = r#"{"DELETE":{"table":"test","condition":"id=1"}}"#;
        let control: LushMessageControl = serde_json::from_str(json).unwrap();

        match control {
            LushMessageControl::DELETE(msg) => {
                assert_eq!(msg.table, "test");
                assert_eq!(msg.condition, "id=1");
            }
            _ => panic!("Expected DELETE variant"),
        }
    }

    #[test]
    fn test_empty_strings() {
        let msg = DeleteMessage {
            table: "".to_string(),
            condition: "".to_string(),
        };
        assert_eq!(msg.table_id(), "");
    }

    #[test]
    fn test_special_characters_in_table_names() {
        let msg = InsertMessage {
            table: "table_123$test".to_string(),
            column_values: "(x) VALUES (1)".to_string(),
        };
        assert_eq!(msg.table_id(), "table_123$test");
    }

    #[test]
    fn test_complex_sql_conditions() {
        let msg = DeleteMessage {
            table: "sensor".to_string(),
            condition: "ts BETWEEN '2024-01-01' AND '2024-12-31' AND value > 100".to_string(),
        };
        assert!(msg.condition.contains("BETWEEN"));
        assert!(msg.condition.contains("AND"));
    }

    #[test]
    fn test_multiline_alter_clause() {
        let msg = AlterMessage {
            table: "metrics".to_string(),
            alter_table_clause: "ADD COLUMN col1 INT,\nADD COLUMN col2 DOUBLE".to_string(),
        };
        assert!(msg.alter_table_clause.contains("\n"));
    }
}
