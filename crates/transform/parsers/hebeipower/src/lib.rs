use regex::Regex;
use serde_json::Map;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::HashSet;
use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::os::raw::c_void;

use std::sync::LazyLock;

static DATE_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"20[2-9]\d-[01]\d-[0-3]\d").unwrap());

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[unsafe(no_mangle)]
pub extern "C" fn parser_name() -> *mut c_char {
    let name = CString::new("hebeipower").unwrap();
    name.into_raw()
}

#[unsafe(no_mangle)]
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
    value_type_range: HashSet<String>,
}

impl ParserConfig {
    fn new(config_param: &str) -> Result<Self, String> {
        tracing::info!("hebeipower config_param: {:?}", config_param);
        let ctx_parts = config_param.split(",").collect::<Vec<&str>>();
        if ctx_parts.is_empty() || ctx_parts[0].is_empty() {
            return Err("Invalid config".to_string());
        }

        let value_key_pattern = Regex::new(ctx_parts[0]).map_err(|err| format!("{:?}", err))?;

        let value_type_range = ctx_parts.get(2).map_or(HashSet::new(), |s| {
            s.split("|").map(|s| s.to_string()).collect()
        });

        Ok(ParserConfig {
            value_key_pattern,
            value_type_key: ctx_parts.get(1).map(|s| s.to_string()),
            value_type_range,
        })
    }

    fn parse_object(&self, object: Map<String, JsonValue>) -> Vec<Map<String, JsonValue>> {
        let mut arr_data = Vec::new();

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
        if !self.value_type_range.is_empty() && !self.value_type_range.contains(the_flag) {
            println!("not in value type range: {}", the_flag);
            return arr_data;
        }

        let data_date = object
            .get("DATA_DATE")
            .map(|v| v.as_str().unwrap_or(""))
            .unwrap_or("");
        if !DATE_PATTERN.is_match(data_date) {
            println!("not match DATE_PATTERN: {}", data_date);
            return arr_data;
        }

        let mut share_object = Map::new();
        for (k, v) in object.iter() {
            if self.value_key_pattern.is_match(k) {
                let dt = format!("{}T{}:{}:00+08:00", data_date, &k[1..3], &k[3..]);

                let mut new_obj = Map::new();
                new_obj.insert(format!("_val{}", the_flag), v.clone());
                if !the_flag.is_empty() {
                    new_obj.insert("_val".to_string(), v.clone());
                }

                new_obj.insert("_ts".to_string(), json!(dt));
                arr_data.push(new_obj);
            } else if k != "DATA_DATE" {
                share_object.insert(k.clone(), v.clone());
            }
        }

        for obj in arr_data.iter_mut() {
            for (k, v) in share_object.iter() {
                if k == "DEV_ID" {
                    match v {
                        JsonValue::String(dev_id_value) => {
                            if dev_id_value.len() > 16 {
                                return Vec::new();
                            }
                        }
                        _ => {
                            return Vec::new();
                        }
                    }
                }
                obj.insert(k.clone(), v.clone());
            }
        }

        arr_data
    }
}

/// # Safety
///
/// - `ctx` must be a non-null pointer to at least `len` bytes of initialized memory.
/// - The memory region referenced by `ctx` must remain valid and not be freed, moved,
///   or concurrently mutated for the entire duration of this function call.
/// - This function must not be called concurrently with the same `ctx` pointer from
///   multiple threads or call sites.
/// - `len` must accurately represent the size in bytes of the buffer at `ctx`; it must
///   not be larger than the allocated region and must not truncate the intended data.
/// - The `len` bytes at `ctx` must contain a valid UTF-8 string representing the
///   parser configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn parser_new(ctx: *const c_char, len: i32) -> ParserResponse {
    let ctx = std::slice::from_raw_parts(ctx as *const _, len as usize);
    let parser_config = std::str::from_utf8(ctx).map(ParserConfig::new);
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

/// # Safety
///
/// - `p` must be a valid pointer returned from [`parser_new`] and must not have been freed yet.
/// - `input_p` must be a valid pointer to `input_l` bytes of initialized memory.
/// - `output_p` and `output_l` must be valid, non-null pointers to writable memory where this
///   function can store the output buffer pointer and its length, respectively.
/// - This function must not be called concurrently with `parser_free` on the same `p`.
/// - The caller is responsible for managing the lifetime and eventual deallocation of the
///   output buffer written via `output_p`.
#[unsafe(no_mangle)]
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
     * value_type_range
     */
    let parser_config = (p as *mut ParserConfig).as_mut().unwrap();

    let input_len = input_l as usize;
    let slice = std::slice::from_raw_parts(input_p, input_len);
    let output_string = std::str::from_utf8(slice)
        .map(serde_json::from_str::<serde_json::Value>)
        .map(|value| match value {
            Ok(JsonValue::Object(object)) => {
                let parsed_data = parser_config.parse_object(object);
                let r = serde_json::to_string(&parsed_data);
                r.unwrap_or_else(|err| {
                    tracing::error!("Failed to serialize parsed data: {:?}", err);
                    "[]".to_string()
                })
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
                r.unwrap_or_else(|err| {
                    tracing::error!("Failed to serialize parsed data: {:?}", err);
                    "[]".to_string()
                })
            }
            Ok(_) => {
                tracing::error!("raw data is not a json object or array");
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

/// # Safety
///
/// - `p` must be a valid pointer previously returned from `parser_new`.
/// - `p` must not be null.
/// - `p` must not have been previously freed.
/// - After this function returns, `p` is no longer valid and must not be used.
/// - This function must not be called concurrently with any other operation on the same `p`.
///
/// This function releases the parser configuration associated with `p`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn parser_free(p: *mut c_void) {
    let parser_config = Box::from_raw(p as *mut ParserConfig);
    drop(parser_config);
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, FixedOffset, Local};

    use super::*;

    fn get_yesterday() -> String {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let yesterday = Local::now().with_timezone(&offset) - Duration::days(1);
        yesterday.format("%Y-%m-%d").to_string()
    }

    #[test]
    fn test_parser_config_error() {
        let parser_config = ParserConfig::new("");
        assert!(parser_config.is_err());
    }

    #[test]
    fn test_parse_object_with_data_type() {
        let yesterday = get_yesterday();

        let parser_config = ParserConfig::new(r"^U\d{2}00$,DATA_TYPE").unwrap();
        let object = json!({
            "DATA_DATE": yesterday.clone(),
            "DATA_TYPE": "1",
            "U0000": 1.0,
            "U0001": 2.0,
            "U0002": 3.0,
            "DEV_ID": "8100000888",
        });
        let object = object.as_object().unwrap();

        let parsed_data = parser_config.parse_object(object.clone());
        assert_eq!(parsed_data.len(), 1);
        assert_eq!(parsed_data[0].get("_val1").unwrap().as_f64().unwrap(), 1.0);
        assert_eq!(
            parsed_data[0].get("_ts").unwrap().as_str().unwrap(),
            format!("{yesterday}T00:00:00+08:00")
        );
    }

    #[test]
    fn test_parse_object_with_dev_id_too_long() {
        let yesterday = get_yesterday();

        let parser_config = ParserConfig::new(r"^U\d{2}00$,DATA_TYPE").unwrap();
        let object = json!({
            "DATA_DATE": yesterday,
            "DATA_TYPE": "1",
            "U0000": 1.0,
            "U0001": 2.0,
            "U0002": 3.0,
            "DEV_ID": "812345678910111213141516",
        });
        let object = object.as_object().unwrap();

        let parsed_data = parser_config.parse_object(object.clone());
        assert_eq!(parsed_data.len(), 0);
    }

    #[test]
    fn test_parse_object_with_multiple_time() {
        let yesterday = get_yesterday();

        let parser_config = ParserConfig::new(r"^U\d{2}(00|15|30|45)$,DATA_TYPE").unwrap();
        let object = json!({
            "DATA_DATE": yesterday.clone(),
            "DATA_TYPE": "1",
            "U0000": 1.0,
            "U0006": 2.0,
            "U0015": 3.0,
            "U0030": 4.0,
            "U0040": 5.0,
            "U0045": 6.0,
            "DEV_ID": "8100000888",
        });
        let object = object.as_object().unwrap();

        let parsed_data = parser_config.parse_object(object.clone());
        assert_eq!(parsed_data.len(), 4);
        assert_eq!(parsed_data[0].get("_val1").unwrap().as_f64().unwrap(), 1.0);
        assert_eq!(
            parsed_data[0].get("_ts").unwrap().as_str().unwrap(),
            format!("{yesterday}T00:00:00+08:00")
        );
        assert_eq!(
            parsed_data[1].get("_ts").unwrap().as_str().unwrap(),
            format!("{yesterday}T00:15:00+08:00")
        );
        assert_eq!(
            parsed_data[2].get("_ts").unwrap().as_str().unwrap(),
            format!("{yesterday}T00:30:00+08:00")
        );
        assert_eq!(
            parsed_data[3].get("_ts").unwrap().as_str().unwrap(),
            format!("{yesterday}T00:45:00+08:00")
        );
    }

    #[test]
    fn test_parse_object_with_data_type_range() {
        let yesterday = get_yesterday();

        let parser_config = ParserConfig::new(r"^U\d{4}$,DATA_TYPE,2").unwrap();
        let object = json!({
            "DATA_DATE": yesterday.clone(),
            "DATA_TYPE": "1",
            "U0001": 1.0,
            "DEV_ID": "8100000888",
        });
        let object = object.as_object().unwrap();

        let parsed_data = parser_config.parse_object(object.clone());
        assert_eq!(parsed_data.len(), 0);
    }

    #[test]
    fn test_parse_object_exception_missing_data_type() {
        let yesterday = get_yesterday();
        let parser_config = ParserConfig::new(r"^U\d{4}$,DATA_TYPE").unwrap();
        let object = json!({
            "DATA_DATE": yesterday.clone(),
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
            format!("{yesterday}T00:01:00+08:00")
        );
        assert_eq!(parsed_data[1].get("_val").unwrap().as_f64().unwrap(), 2.0);
        assert_eq!(
            parsed_data[1].get("_ts").unwrap().as_str().unwrap(),
            format!("{yesterday}T00:02:00+08:00")
        );
        assert_eq!(parsed_data[2].get("_val").unwrap().as_f64().unwrap(), 3.0);
        assert_eq!(
            parsed_data[2].get("_ts").unwrap().as_str().unwrap(),
            format!("{yesterday}T00:03:00+08:00")
        );
    }
}
