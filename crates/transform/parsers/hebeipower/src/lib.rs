use regex::Regex;
use serde_json::json;
use serde_json::Map;
use serde_json::Value as JsonValue;
use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::os::raw::c_void;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[no_mangle]
pub extern "C" fn parser_name() -> *mut c_char {
    let name = CString::new("hebeipower").unwrap();
    name.into_raw()
}

#[no_mangle]
pub extern "C" fn parser_version() -> *mut c_char {
    let version = CString::new("0.1.0").unwrap();
    version.into_raw()
}

#[repr(C)]
pub struct ParserResponse {
    e: i32,
    p: *mut c_void,
}

struct ParserConfig {
    value_key_pattern: Regex,
    value_type_key: Option<String>,
    white_type_key: Option<String>,
}

impl ParserConfig {
    fn new(config_param: &str) -> Result<Self, String> {
        tracing::info!("hebeipower config_param: {:?}", config_param);
        let ctx_parts = config_param.split(",").collect::<Vec<&str>>();
        if ctx_parts.len() < 1 || ctx_parts[0] == "" {
            return Err("Invalid config".to_string());
        }

        let regex = format!(r#"{}\d{{4}}"#, ctx_parts[0]);
        let regex = Regex::new(&regex).map_err(|err| format!("{:?}", err))?;

        Ok(ParserConfig {
            value_key_pattern: regex,
            value_type_key: ctx_parts.get(1).map(|s| s.to_string()),
            white_type_key: ctx_parts.get(2).map(|s| s.to_string()),
        })
    }

    fn parse_object(&self, object: Map<String, JsonValue>) -> Vec<Map<String, JsonValue>> {
        // let data_type = self.value_type_key.as_deref().unwrap_or("");
        let the_flag = self
            .value_type_key
            .as_ref()
            .map(|s| {
                for (k, v) in object.iter() {
                    if k == s {
                        return v.as_str().unwrap_or("");
                    }
                }
                ""
            })
            .unwrap_or("");

        let mut arr_data = Vec::new();
        let date_date = object.get("DATA_DATE").unwrap().as_str().unwrap();
        let mut share_object = Map::new();

        for (k, v) in object.iter() {
            if self.value_key_pattern.is_match(k) {
                let mut new_obj = Map::new();
                new_obj.insert(format!("_val{}", the_flag), v.clone());

                let dt = format!(
                    "{}T{}:{}:00+08:00",
                    date_date,
                    k[1..3].to_string(),
                    k[3..].to_string()
                );
                new_obj.insert("_ts".to_string(), json!(dt));
                arr_data.push(new_obj);
            } else if k != "DATA_DATE" {
                share_object.insert(k.clone(), v.clone());
            }
        }

        for obj in arr_data.iter_mut() {
            for (k, v) in share_object.iter() {
                obj.insert(k.clone(), v.clone());
            }
        }

        arr_data
    }
}

#[no_mangle]
pub extern "C" fn parser_new(ctx: *const c_char, len: i32) -> ParserResponse {
    let ctx = unsafe { std::slice::from_raw_parts(ctx as *const u8, len as usize) };
    let parser_config = std::str::from_utf8(ctx).map(|s| ParserConfig::new(s));
    if parser_config.is_err() {
        return ParserResponse {
            e: 1,
            p: std::ptr::null_mut(),
        };
    }

    let parser_config = Box::into_raw(Box::new(parser_config.unwrap()));
    ParserResponse {
        e: 0,
        p: parser_config as *mut c_void,
    }
}

fn set_output(output_string: String, output_p: *mut *mut u8, output_l: *mut u32) {
    let mut output = ManuallyDrop::new(output_string);
    output.shrink_to_fit();
    unsafe {
        *output_p = output.as_ptr() as *mut u8;
        *output_l = output.len() as u32;
    }
}

#[no_mangle]
pub unsafe extern "C" fn parser_mutate(
    p: *mut c_void,
    input_p: *const u8,
    input_l: u32,
    output_p: *mut *mut u8,
    output_l: *mut u32,
) -> *const c_char {
    /*
     * U,DATA_TYPE,1
     * value_key_prefix
     * value_type_key
     * white_type_key
     */
    let parser_config = (p as *mut ParserConfig).as_mut().unwrap();

    let input_len = input_l as usize;
    let output_string = std::str::from_utf8(std::slice::from_raw_parts(input_p, input_len))
        .map(|s| serde_json::from_str::<serde_json::Value>(s))
        .map(|value| match value {
            Ok(JsonValue::Object(object)) => {
                let parsed_data = parser_config.parse_object(object);
                let r = serde_json::to_string(&parsed_data);
                if r.is_err() {
                    tracing::error!("Failed to serialize parsed data: {:?}", r.err());
                    return "[]".to_string();
                }
                r.unwrap()
            }
            Ok(JsonValue::Array(objs)) => {
                let mut result_array = Vec::new();

                for obj in objs.into_iter() {
                    if let JsonValue::Object(object) = obj {
                        let parsed_data = parser_config.parse_object(object);
                        result_array.extend(parsed_data);
                    }
                }

                let r = serde_json::to_string(&result_array);
                if r.is_err() {
                    tracing::error!("Failed to serialize parsed data: {:?}", r.err());
                    return "[]".to_string();
                }
                r.unwrap()
            }
            Ok(_) => {
                tracing::error!("raw data is not a json object");
                "[]".to_string()
            }
            Err(err) => {
                tracing::error!("raw data can't be parsed as json: {:?}", err);
                "[]".to_string()
            }
        });

    match output_string {
        Ok(s) => {
            set_output(s, output_p, output_l);
        }
        Err(err) => {
            tracing::error!("Failed to parse input data: {:?}", err);
            set_output("[]".to_string(), output_p, output_l);
        }
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn parser_free(p: *mut c_void) {
    let parser_config = Box::from_raw(p as *mut ParserConfig);
    drop(parser_config);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_config_error() {
        let parser_config = ParserConfig::new("");
        assert!(parser_config.is_err());
    }

    #[test]
    fn test_parse_object_with_data_type() {
        let parser_config = ParserConfig::new("U,DATA_TYPE,1").unwrap();
        let object = json!({
            "DATA_DATE": "2021-01-01",
            "DATA_TYPE": "1",
            "U0001": 1.0,
            "U0002": 2.0,
            "U0003": 3.0,
            "DEV_ID": "8100000888",
        });
        let object = object.as_object().unwrap();

        let parsed_data = parser_config.parse_object(object.clone());
        assert_eq!(parsed_data.len(), 3);
        assert_eq!(parsed_data[0].get("_val1").unwrap().as_f64().unwrap(), 1.0);
        assert_eq!(
            parsed_data[0].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:01:00+08:00"
        );
        assert_eq!(parsed_data[1].get("_val1").unwrap().as_f64().unwrap(), 2.0);
        assert_eq!(
            parsed_data[1].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:02:00+08:00"
        );
        assert_eq!(parsed_data[2].get("_val1").unwrap().as_f64().unwrap(), 3.0);
        assert_eq!(
            parsed_data[2].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:03:00+08:00"
        );
    }

    #[test]
    fn test_parse_object_without_data_type() {
        let parser_config = ParserConfig::new("U").unwrap();
        let object = json!({
            "DATA_DATE": "2021-01-01",
            "DATA_TYPE": "1",
            "U0001": 1.0,
            "U0002": 2.0,
            "U0003": 3.0,
            "DEV_ID": "8100000888",
        });
        let object = object.as_object().unwrap();

        let parsed_data = parser_config.parse_object(object.clone());
        assert_eq!(parsed_data.len(), 3);
        assert_eq!(parsed_data[0].get("_val").unwrap().as_f64().unwrap(), 1.0);
        assert_eq!(
            parsed_data[0].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:01:00+08:00"
        );
        assert_eq!(parsed_data[1].get("_val").unwrap().as_f64().unwrap(), 2.0);
        assert_eq!(
            parsed_data[1].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:02:00+08:00"
        );
        assert_eq!(parsed_data[2].get("_val").unwrap().as_f64().unwrap(), 3.0);
        assert_eq!(
            parsed_data[2].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:03:00+08:00"
        );
    }

    #[test]
    fn test_parse_object_exception_missing_data_type() {
        let parser_config = ParserConfig::new("U,DATA_TYPE").unwrap();
        let object = json!({
            "DATA_DATE": "2021-01-01",
            "U0001": 1.0,
            "U0002": 2.0,
            "U0003": 3.0,
            "DEV_ID": "8100000888",
        });
        let object = object.as_object().unwrap();

        let parsed_data = parser_config.parse_object(object.clone());
        assert_eq!(parsed_data.len(), 3);
        assert_eq!(parsed_data[0].get("_val").unwrap().as_f64().unwrap(), 1.0);
        assert_eq!(
            parsed_data[0].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:01:00+08:00"
        );
        assert_eq!(parsed_data[1].get("_val").unwrap().as_f64().unwrap(), 2.0);
        assert_eq!(
            parsed_data[1].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:02:00+08:00"
        );
        assert_eq!(parsed_data[2].get("_val").unwrap().as_f64().unwrap(), 3.0);
        assert_eq!(
            parsed_data[2].get("_ts").unwrap().as_str().unwrap(),
            "2021-01-01T00:03:00+08:00"
        );
    }
}
