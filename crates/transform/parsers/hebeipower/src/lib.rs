use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::os::raw::c_void;
use regex::Regex;
use serde_json::json;
use serde_json::Map;
use serde_json::Value as JsonValue;


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
    value_type_key: CString,
    white_type_key: CString,
}

// static mut PARSER: *mut Parser = std::ptr::null_mut();
#[no_mangle]
pub extern "C" fn parser_new(ctx: *const c_char, len: i32) -> ParserResponse {
    let ctx = unsafe { std::slice::from_raw_parts(ctx, len as usize) };
    let ctx = unsafe { std::mem::transmute(ctx) };

    let ctx = std::str::from_utf8(ctx).unwrap();
    let ctx_parts = ctx.split(",").collect::<Vec<&str>>();
    let ctx_parts_len = ctx_parts.len();

    let regstr = format!(r#"{}\d{{4}}"#,  ctx_parts[0]);

    let parser_config = ParserConfig {
        value_key_pattern: Regex::new(&regstr).unwrap(),
        value_type_key: if ctx_parts_len > 1 { CString::new(ctx_parts[1]).unwrap() } else { CString::new("").unwrap() },
        white_type_key: if ctx_parts_len > 2 { CString::new(ctx_parts[2]).unwrap() } else { CString::new("").unwrap() },
    };
    
    let parser_config = Box::into_raw(Box::new(parser_config));
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

fn parse_data(
    object: Map<String, JsonValue>,
    value_key_pattern: &Regex,
    data_type: &str,
) -> Vec<Map<String, JsonValue>> {
    
    println!("value_key_pattern: {:?}", value_key_pattern);

    let mut the_flag = "";
    for (k, v) in object.iter() {
        if k == data_type {
            the_flag = v.as_str().unwrap_or("");
            break;
        }
    }

    let mut arr_data = Vec::new();
    let date_date = object.get("DATA_DATE").unwrap().as_str().unwrap();
    let mut share_object = Map::new();

    for (k, v) in object.iter() {
        if value_key_pattern.is_match(k) {
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

#[no_mangle]
pub unsafe extern "C" fn parser_mutate(p: *mut c_void, input_p: *const u8, input_l: u32, output_p: *mut *mut u8, output_l: *mut u32) 
-> *const c_char {
    /*
     * U,DATA_TYPE,1
     * value_key_prefix
     * value_type_key
     * white_type_key
     */
    let parser_config = (p as *mut ParserConfig).as_mut().unwrap();

    let input_len = input_l as usize;
    let input_string = std::str::from_utf8(std::slice::from_raw_parts(input_p, input_len)).unwrap();

    println!("input_string: {}", input_string);
    
    let value = serde_json::from_str::<serde_json::Value>(input_string).unwrap();
    
    let output_string = match value {
        JsonValue::Object(object) => {
            let parsed_data = parse_data(object, &parser_config.value_key_pattern, parser_config.value_type_key.to_str().unwrap());
            serde_json::to_string(&parsed_data).unwrap()
        },
        _ => {"".to_string()},
    };
    println!("output_string: {}", output_string);

    set_output(output_string, output_p, output_l);
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn parser_free(p: *mut c_void) {
    let parser_config = Box::from_raw(p as *mut ParserConfig);
    drop(parser_config);
}
