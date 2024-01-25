use std::fmt;
use std::fmt::Formatter;

pub struct Live {
    pub datetime: i64,
    pub tag_name: String,
    pub value: Option<f64>,
    pub v_value: Option<String>,
    pub quality: u8,
    pub quality_detail: Option<i32>,
    pub opc_quality: Option<i32>,
    pub ww_tag_key: i32,
    pub ww_retrieval_mode: Option<String>,
    pub ww_time_dead_band: Option<i32>,
    pub ww_value_dead_band: Option<f64>,
    pub ww_time_zone: Option<String>,
    pub ww_parameters: Option<String>,
    pub source_tag: Option<String>,
    pub source_server: Option<String>,
    pub ww_value_selector: String,
    pub ww_expression: Option<String>,
    pub ww_unit: Option<String>,
}

impl fmt::Display for Live {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.datetime,
            self.tag_name,
            self.value
                .map(|v| { format!("{}", v) })
                .unwrap_or("NULL".to_string()),
            self.v_value
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
            self.quality,
            self.quality_detail
                .map(|v| { format!("{}", v) })
                .unwrap_or("NULL".to_string()),
            self.opc_quality
                .map(|v| { format!("{}", v) })
                .unwrap_or("NULL".to_string()),
            self.ww_tag_key,
            self.ww_retrieval_mode
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
            self.ww_time_dead_band
                .map(|v| { format!("{}", v) })
                .unwrap_or("NULL".to_string()),
            self.ww_value_dead_band
                .map(|v| { format!("{}", v) })
                .unwrap_or("NULL".to_string()),
            self.ww_time_zone
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
            self.ww_parameters
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
            self.source_tag
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
            self.source_server
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
            self.ww_value_selector,
            self.ww_expression
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
            self.ww_unit
                .clone()
                .map(|v| { format!("\"{}\"", v) })
                .unwrap_or("NULL".to_string()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_live() {
        let live = Live {
            datetime: 123,
            tag_name: "tag1".to_string(),
            value: Some(3.14),
            v_value: Some("3.14".to_string()),
            quality: 133,
            quality_detail: Some(192),
            opc_quality: Some(192),
            ww_tag_key: 123,
            ww_retrieval_mode: None,
            ww_time_dead_band: None,
            ww_value_dead_band: None,
            ww_time_zone: None,
            ww_parameters: None,
            source_tag: Some("source_tag".to_string()),
            source_server: Some("source_server".to_string()),
            ww_value_selector: "".to_string(),
            ww_expression: None,
            ww_unit: None,
        };

        // println!("{}", live.to_string());
        assert_eq!(
            "123,tag1,3.14,\"3.14\",133,192,192,123,NULL,NULL,NULL,NULL,NULL,\"source_tag\",\"source_server\",,NULL,NULL",
            live.to_string()
        );
    }
}
