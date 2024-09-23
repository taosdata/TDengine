use captcha::filters::{Dots, Noise, Wave};
use captcha::{Captcha, Geometry};
use lazy_static::lazy_static;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use taoslog::utils::{QidMetadataGetter, Span};
use taoslog::QidManager;

use crate::qid::{headers_with_qid, Qid};

pub fn sign_string(input: &str) -> String {
    let mut hasher = Sha1::new();
    let string_to_sign = format!("{}fuYV7zMZzQ", input);
    hasher.update(string_to_sign.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}

fn verify_string(input: &str, expected: &str) -> bool {
    let result = sign_string(input);
    result == expected
}

pub fn record_binding_phone_email(server: &str, phone_email: &str, file: &PathBuf) {
    let sign = sign_string(phone_email);
    let binding_record = format!("{}|{}|{}\n", server, phone_email, sign);
    // 打开文件，追加写入，如果文件不存在，则创建
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(file)
        .unwrap();
    file.write_all(binding_record.as_bytes()).unwrap();
    file.flush().unwrap();
}

pub fn check_phone_email_verified(filename: &PathBuf, current_server: &str) -> io::Result<()> {
    let file = File::open(filename)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if line.starts_with(current_server) {
            let mut items = line.split("|");
            let server = items.nth(0).unwrap();
            let phone_email = items.nth(0);
            let sign = items.nth(0);
            // println!("server: {}, phone_email: {:?}, sign: {:?}", server, phone_email, sign);

            if phone_email.is_some() && sign.is_some() && server == current_server {
                let phone_email = phone_email.unwrap();
                let sign = sign.unwrap();
                if verify_string(phone_email, sign) {
                    return Ok(());
                }
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::NotFound, "not find any "))
}

#[derive(Debug)]
struct VerificationCode {
    code: String,
    expire_time: u64,
}

lazy_static! {
    static ref VERIFICATION_CODES: Mutex<HashMap<String, VerificationCode>> =
        Mutex::new(HashMap::new());
}

pub fn generate_verification_code(key: String) -> String {
    let mut code = String::new();
    for _ in 0..4 {
        let digit = rand::random::<u8>() % 10;
        code.push_str(&digit.to_string());
    }
    save_to_cache(key, &code);

    code
}

#[derive(Serialize, Debug)]
struct RequestBodySendVerificationCode {
    phone: String,
    email: String,
    code: String,
    duration: u8,
    language: String,
}

#[derive(Deserialize, Debug)]
struct ResponseCloudOpenApi {
    code: u32,
}

// 如果配置了url，则使用配置的url，否则根据语言选择默认的url
fn get_url_prefix(url_config: Option<String>, lang: &str) -> String {
    url_config.unwrap_or_else(|| match lang {
        "zh_CN" => "https://cloud.taosdata.com/openapi".to_string(),
        _ => "https://cloud.tdengine.com/openapi".to_string(),
    })
}

pub async fn send_verification_code_with_cloud_open_api(
    url_config: Option<String>,
    phone_email: &str,
    lang: &str,
) -> anyhow::Result<u32> {
    let mut phone = "";
    let mut email = "";
    match phone_email.find("@") {
        Some(_) => email = phone_email,
        None => phone = phone_email,
    }

    let mut url = get_url_prefix(url_config, lang);
    url.push_str("/trial/verification-code");

    let duration = 10_u8;

    let code = generate_verification_code(phone_email.to_string());
    let string_to_sign = format!(
        "code={}&duration={}&email={}&language={}&phone={}",
        code, duration, email, lang, phone
    );

    let body = RequestBodySendVerificationCode {
        phone: phone.to_string(),
        email: email.to_string(),
        code,
        duration: 10,
        language: lang.to_string(),
    };
    let json_body = serde_json::to_string(&body)?;
    log::debug!("json_body: {}", json_body);

    let response = request_cloud(url, Method::POST, json_body, string_to_sign)
        .await?
        .json::<ResponseCloudOpenApi>()
        .await?;

    Ok(response.code)
}

async fn request_cloud(
    url: String,
    method: Method,
    json_body: String,
    params_to_sign: String,
) -> anyhow::Result<reqwest::Response> {
    let nonce = rand::random::<u64>() % 1000000000;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let string_to_sign = format!("{}&nonce={}&ts={}", params_to_sign, nonce, ts);
    let sign = sign_string(&string_to_sign);

    let mut qid: Qid = Span.get_qid().unwrap_or_else(Qid::init);
    qid.add_sequence_id();

    log::info!("post url: {}, request body:{}", url, json_body);

    // 连接超时时间为30秒，请求超时时间为60秒
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(60))
        .build()?;

    let response = http_client
        .request(method, &url)
        .headers(headers_with_qid(&qid))
        .header(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        )
        .header("Server-Key", "UVf6b2fA")
        .header("Nonce", nonce)
        .header("Time-Stamp", ts.to_string())
        .header("Sign", sign)
        .body(json_body)
        .send()
        .await?;
    log::info!("response: {:?}", response);

    Ok(response)
}

fn get_explore_version() -> String {
    // let mut version = "unknown".to_string();
    // let metadata = MetadataCommand::new().exec().unwrap();
    // for package in metadata.packages {
    //     if package.name == "taos-explorer" {
    //         version = package.version.to_string();
    //         break;
    //     }
    // }
    // version
    "1.7.0".to_string()
}

#[derive(Serialize, Debug)]
struct RequestBodyReportVerificationStatus {
    phone: String,
    email: String,
    code: String,
    name: String,
    #[serde(rename = "taosdVersion")]
    taosd_version: String,
    #[serde(rename = "explorerVersion")]
    explorer_version: String,
}

// 上报验证状态到云端
pub async fn report_verification_status_to_cloud(
    url_config: Option<String>,
    phone_email: &str,
    code: &str,
    lang: &str,
    name: &str,
) -> anyhow::Result<u32> {
    let mut phone = "";
    let mut email = "";
    match phone_email.find("@") {
        Some(_) => email = phone_email,
        None => phone = phone_email,
    }

    let mut url = get_url_prefix(url_config, lang);
    url.push_str("/trial/verification-result");

    let explorer_version = get_explore_version();
    let string_to_sign = format!(
        "code={}&email={}&explorerVersion={}&name={}&phone={}&taosdVersion=",
        code, email, explorer_version, name, phone
    );
    log::debug!("string_to_sign: {}", string_to_sign);

    let body = RequestBodyReportVerificationStatus {
        phone: phone.to_string(),
        email: email.to_string(),
        code: code.to_string(),
        name: name.to_string(),
        taosd_version: "".to_string(),
        explorer_version,
    };
    let json_body = serde_json::to_string(&body)?;

    let response = request_cloud(url, Method::POST, json_body, string_to_sign)
        .await?
        .json::<ResponseCloudOpenApi>()
        .await?;

    Ok(response.code)
}

#[derive(Serialize, Debug)]
struct RequestBodyTaosdInfo {
    phone: String,
    email: String,
    #[serde(rename = "taosdVersion")]
    taosd_version: String,
    #[serde(rename = "instanceId")]
    cluster_id: String,
}

// 上报连接的 taosd 信息到云端
pub async fn report_taosd_info_to_cloud(
    url_config: Option<String>,
    phone_email: &str,
    lang: &str,
    cluster_id: &str,
    taosd_version: &str,
) -> anyhow::Result<u32> {
    let mut phone = "";
    let mut email = "";
    match phone_email.find("@") {
        Some(_) => email = phone_email,
        None => phone = phone_email,
    }

    let mut url = get_url_prefix(url_config, lang);
    url.push_str("/trial/verification-result");

    let string_to_sign = format!(
        "email={}&instanceId={}&phone={}&taosdVersion={}",
        email, cluster_id, phone, taosd_version
    );
    log::debug!("string_to_sign: {}", string_to_sign);

    let body = RequestBodyTaosdInfo {
        phone: phone.to_string(),
        email: email.to_string(),
        taosd_version: taosd_version.to_string(),
        cluster_id: cluster_id.to_string(),
    };
    let json_body = serde_json::to_string(&body)?;
    log::debug!("json_body: {}", json_body);

    let response = request_cloud(url, Method::PUT, json_body, string_to_sign)
        .await?
        .json::<ResponseCloudOpenApi>()
        .await?;
    log::debug!(
        "report_verification_status_to_cloud success: {}",
        response.code
    );

    Ok(response.code)
}

fn save_to_cache(key: String, code: &String) {
    let expire_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60 * 5;
    log::debug!(
        "save_to_cache: key: {}, code: {}, expire_time: {}",
        key,
        code,
        expire_time
    );

    VERIFICATION_CODES.lock().unwrap().insert(
        key,
        VerificationCode {
            code: code.clone(),
            expire_time,
        },
    );
}

pub fn check_security_code(key: &str, code: &str) -> String {
    let mut codes = VERIFICATION_CODES.lock().unwrap();
    log::debug!("check_security_code: key: {}, code: {}", key, code);
    if let Some(verification_code) = codes.get(key) {
        log::debug!("find the code in cache: {:?}", verification_code);
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if verification_code.expire_time < current_time {
            codes.remove(key);
            return "none".to_string();
        }

        if verification_code.code == code {
            codes.remove(key);
            return "pass".to_string();
        } else {
            return "error".to_string();
        }
    }
    log::debug!("not find the code in cache");

    "none".to_string()
}

pub fn generate_captcha(key: String) -> Option<Vec<u8>> {
    let captcha_chars = "123456789".chars().collect::<Vec<char>>();

    let mut captcha = Captcha::new();
    captcha
        .set_chars(&captcha_chars)
        .add_chars(4)
        .apply_filter(Noise::new(0.1))
        .apply_filter(Wave::new(2.0, 20.0).horizontal())
        .apply_filter(Wave::new(2.0, 20.0).vertical())
        .apply_filter(Dots::new(10))
        .extract(Geometry::new(40, 280, 130, 190));

    save_to_cache(key, &captcha.chars_as_string());

    captcha.as_png()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_captcha() {
        generate_captcha("dddss".to_string());
    }

    #[test]
    fn test_sign_string() {
        // test phone number
        let input = "code=1145&duration=10&email=37376532@qq.com&language=en_US&nonce=111714236&phone=&ts=1713607706255";
        let expected = "48421beeb8c659286f609d7a242cec4b472c21bd";
        let result = sign_string(input);
        // println!("============result: {}", result);
        assert_eq!(result, expected);
    }

    fn prepare_test_file(filename: &PathBuf) {
        let content = "localhost:8080|15801381212|eac0aedab66250c36d517fa9401a8c415efc26afe8f95b2b70619918dbcf4d6c\nlocalhost:6060|15801381212@163.com|bc16f1f1a1157efa3c2c0481adeabef083eeb3f8626c39d575fc7256ef8edabb\n";
        std::fs::write(filename, content).unwrap();
    }

    // #[test]
    // fn test_report_verification_status_to_cloud() {
    //     let phone_email = "15801381212";
    //     let code = "1234";
    //     let lang = "zh_CN";
    //     let taosd_version = "2.0.0";
    //     println!("========report_verification_status_to_cloud");
    //     report_verification_status_to_cloud(Some("https://pre.ali.cloud.taosdata.com/openapi".to_string()), phone_email, code.to_string(), lang, taosd_version.to_string());
    // }

    // #[tokio::test]
    // async fn test_send_verification_code_with_cloud_open_api() {
    //     let phone = "13466397075";
    //     let email = "37376532@qq.com";
    //     let lang = "zh_CN";
    //     send_verification_code_with_cloud_open_api(Some("https://pre.ali.cloud.taosdata.com/openapi".to_string()), phone, lang).await.unwrap();
    //     send_verification_code_with_cloud_open_api(Some("https://pre.ali.cloud.taosdata.com/openapi".to_string()), email, lang).await.unwrap();
    // }

    #[test]
    fn test_check_phone_email_verified() {
        let filename = PathBuf::from("phone_email_verified.txt");
        prepare_test_file(&filename);

        let result = check_phone_email_verified(&filename, "localhost:8080");
        assert!(result.is_ok());

        let result = check_phone_email_verified(&filename, "localhost:6060");
        assert!(result.is_ok());

        let result = check_phone_email_verified(&filename, "localhost:5050");
        assert!(result.is_err());

        let empty_filename = PathBuf::from("empty_phone_email_verified.txt");
        let result = check_phone_email_verified(&empty_filename, "localhost:8080");
        assert!(result.is_err());
    }

    #[test]
    fn test_record_binding_phone_email() {
        let filename = PathBuf::from("phone_email_verified.txt");
        prepare_test_file(&filename);

        let server = "localhost:8080";
        let phone_email = "15801381212";
        record_binding_phone_email(server, phone_email, &filename);

        let result = check_phone_email_verified(&filename, server);
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_record_binding_phone_email() {
        let filename = PathBuf::from("empty_phone_email_verified.txt");
        let server = "localhost:8080";
        let phone_email = "15801381212";
        record_binding_phone_email(server, phone_email, &filename);

        let result = check_phone_email_verified(&filename, server);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_verification_code() {
        let code = generate_verification_code("15801381212".to_string());
        VERIFICATION_CODES
            .lock()
            .unwrap()
            .iter()
            .for_each(|(k, v)| {
                println!("k: {}, v: {:?}", k, v);
            });

        println!("code: {}", code);
        assert_eq!(code.len(), 4);
    }

    #[test]
    fn test_check_verification_code() {
        let phone_email = "15801381212";
        let code = generate_verification_code(phone_email.to_string());
        print!("code: {}\n", code);

        assert_eq!(check_security_code(phone_email, "1234"), "error");
        assert_eq!(check_security_code(phone_email, &code), "pass");

        // 再次验证时，已经失效了，不能重复使用
        assert_eq!(check_security_code(phone_email, &code), "none");
    }
}
