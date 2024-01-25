use std::fmt;
use std::fmt::Formatter;

pub struct History {
    pub datetime: i64,
    pub tag_name: String,
    pub value: Option<f64>,
    pub v_value: Option<String>,
    pub quality: u8,
    pub quality_detail: Option<i32>,
    pub opc_quality: Option<i32>,
    pub ww_tag_key: i32,
    pub ww_row_count: Option<i32>,
    pub ww_resolution: Option<i32>,
    pub ww_edge_detection: Option<String>,
    pub ww_retrieval_mode: Option<String>,
    pub ww_time_dead_band: Option<i32>,
    pub ww_value_dead_band: Option<f64>,
    pub ww_time_zone: Option<String>,
    pub ww_version: Option<String>,
    pub ww_cycle_count: Option<i32>,
    pub ww_time_stamp_rule: Option<String>,
    pub ww_interpolation_type: Option<String>,
    pub ww_quality_rule: Option<String>,
    pub ww_state_calc: Option<String>,
    pub state_time: Option<f64>,
    pub percent_good: Option<f64>,
    pub ww_parameters: Option<String>,
    pub start_datetime: i64,
    pub source_tag: Option<String>,
    pub source_server: Option<String>,
    pub ww_filter: Option<String>,
    pub ww_value_selector: String,
    pub ww_max_states: Option<i32>,
    pub ww_option: Option<String>,
    pub ww_expression: Option<String>,
    pub ww_unit: Option<String>,
}

impl fmt::Display for History {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.datetime,
            self.tag_name,
            self.value
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.v_value
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.quality,
            self.quality_detail
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.opc_quality
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_tag_key,
            self.ww_row_count
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_resolution
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_edge_detection
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_retrieval_mode
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_time_dead_band
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_value_dead_band
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_time_zone
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_version
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_cycle_count
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_time_stamp_rule
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_interpolation_type
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_quality_rule
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_state_calc
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.state_time
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.percent_good
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_parameters
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.start_datetime,
            self.source_tag
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.source_server
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_filter
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            format!("\"{}\"",self.ww_value_selector),
            self.ww_max_states
                .map(|v| format!("{}", v))
                .unwrap_or("NULL".to_string()),
            self.ww_option
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_expression
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
            self.ww_unit
                .clone()
                .map(|v| format!("\"{}\"", v))
                .unwrap_or("NULL".to_string()),
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_history() {
        let history = History {
            datetime: 123,
            tag_name: "tag1".to_string(),
            value: Some(3.14),
            v_value: Some("3.14".to_string()),
            quality: 133,
            quality_detail: Some(192),
            opc_quality: None,
            ww_tag_key: 1,
            ww_row_count: None,
            ww_resolution: Some(123),
            ww_edge_detection: None,
            ww_retrieval_mode: None,
            ww_time_dead_band: None,
            ww_value_dead_band: None,
            ww_time_zone: None,
            ww_version: None,
            ww_cycle_count: None,
            ww_time_stamp_rule: None,
            ww_interpolation_type: None,
            ww_quality_rule: None,
            ww_state_calc: None,
            state_time: None,
            percent_good: None,
            ww_parameters: None,
            start_datetime: 123,
            source_tag: Some("source_tag".to_string()),
            source_server: Some("source_server".to_string()),
            ww_filter: None,
            ww_value_selector: "".to_string(),
            ww_max_states: None,
            ww_option: None,
            ww_expression: None,
            ww_unit: None,
        };

        // println!("{}", history.to_string());
        assert_eq!(
            "123,tag1,3.14,\"3.14\",133,192,NULL,1,NULL,123,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,123,\"source_tag\",\"source_server\",NULL,\"\",NULL,NULL,NULL,NULL",
            history.to_string()
        );
    }
}
