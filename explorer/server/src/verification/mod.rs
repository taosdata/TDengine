use captcha::filters::{Dots, Noise, Wave};
use captcha::{Captcha, Geometry};
use lazy_static::lazy_static;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::Path;
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
    dbg!(sign_string(input)) == dbg!(expected)
}

pub fn record_binding_phone_email(server: &str, phone_email: &str, file: &Path) -> io::Result<()> {
    let sign = sign_string(phone_email);
    let binding_record = format!("{}|{}|{}\n", server, phone_email, sign);
    // 打开文件，追加写入，如果文件不存在，则创建
    let mut file = OpenOptions::new().append(true).create(true).open(file)?;
    file.write_all(binding_record.as_bytes())?;
    file.flush()?;
    tracing::debug!(server, phone_email, sign, "record_binding_phone_email",);
    Ok(())
}

pub fn check_phone_email_verified(filename: &Path, current_server: &str) -> io::Result<()> {
    let file = File::open(filename)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if line.starts_with(current_server) {
            let mut items = line.split("|");
            let server = items.next().unwrap();
            let phone_email = items.next();
            let sign = items.next();
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

// Use owned key type since it will be persisted in global cache.
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
struct RequestBodySendVerificationCode<'a> {
    phone: &'a str,
    email: &'a str,
    code: &'a str,
    duration: u8,
    language: &'a str,
}

#[derive(Deserialize, Debug)]
struct ResponseCloudOpenApi {
    code: u32,
}

const CLOUD_OPENAPI_URL_CN: &str = "https://cloud.taosdata.com/openapi";
const CLOUD_OPENAPI_URL_EN: &str = "https://cloud.tdengine.com/openapi";
// 如果配置了url，则使用配置的url，否则根据语言选择默认的url
fn get_url_prefix<'a>(url_config: Option<&'a str>, lang: &str) -> &'a str {
    url_config.unwrap_or(match lang {
        "zh_CN" => CLOUD_OPENAPI_URL_CN,
        _ => CLOUD_OPENAPI_URL_EN,
    })
}

pub async fn send_verification_code_with_cloud_open_api(
    url_config: Option<&str>,
    phone_email: &str,
    lang: &str,
) -> anyhow::Result<u32> {
    let mut phone = "";
    let mut email = "";
    match phone_email.find("@") {
        Some(_) => email = phone_email,
        None => phone = phone_email,
    }

    let url = get_url_prefix(url_config, lang);
    let url = format!("{url}/trial/verification-code");

    let duration = 10_u8;

    let code = generate_verification_code(phone_email.to_string());
    let string_to_sign = format!(
        "code={}&duration={}&email={}&language={}&phone={}",
        code, duration, email, lang, phone
    );

    let body = RequestBodySendVerificationCode {
        phone,
        email,
        code: &code,
        duration: 10,
        language: lang,
    };
    let json_body = serde_json::to_string(&body)?;
    tracing::debug!("json_body: {}", json_body);

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

    tracing::info!("post url: {}, request body:{}", url, json_body);

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
    tracing::info!("response: {:?}", response);

    Ok(response)
}

fn get_explore_version() -> &'static str {
    crate::build::PKG_VERSION
}

#[derive(Serialize, Debug)]
struct RequestBodyReportVerificationStatus<'a> {
    phone: &'a str,
    email: &'a str,
    code: &'a str,
    firstname: &'a str,
    lastname: &'a str,
    #[serde(rename = "taosdVersion")]
    taosd_version: &'a str,
    #[serde(rename = "explorerVersion")]
    explorer_version: &'a str,
}

// 上报验证状态到云端
pub async fn report_verification_status_to_cloud(
    url_config: Option<&str>,
    phone_email: &str,
    code: &str,
    lang: &str,
    firstname: &str,
    lastname: &str,
) -> anyhow::Result<u32> {
    let mut phone = "";
    let mut email = "";
    match phone_email.find("@") {
        Some(_) => email = phone_email,
        None => phone = phone_email,
    }

    let url = format!(
        "{}/trial/verification-result",
        get_url_prefix(url_config, lang)
    );

    let explorer_version = get_explore_version();
    let string_to_sign = format!(
        "code={}&email={}&explorerVersion={}&firstname={}&lastname={}&phone={}&taosdVersion=",
        code, email, explorer_version, firstname, lastname, phone
    );
    tracing::debug!("string_to_sign: {}", string_to_sign);

    let body = RequestBodyReportVerificationStatus {
        phone,
        email,
        code,
        firstname,
        lastname,
        taosd_version: "",
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
struct RequestBodyTaosdInfo<'a> {
    phone: &'a str,
    email: &'a str,
    #[serde(rename = "taosdVersion")]
    taosd_version: &'a str,
    #[serde(rename = "instanceId")]
    cluster_id: &'a str,
}

// 上报连接的 taosd 信息到云端
pub async fn report_taosd_info_to_cloud(
    url_config: Option<&str>,
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

    let url = format!(
        "{}/trial/verification-result",
        get_url_prefix(url_config, lang)
    );

    let string_to_sign = format!(
        "email={}&instanceId={}&phone={}&taosdVersion={}",
        email, cluster_id, phone, taosd_version
    );
    tracing::debug!("string_to_sign: {}", string_to_sign);

    let body = RequestBodyTaosdInfo {
        phone,
        email,
        taosd_version,
        cluster_id,
    };
    let json_body = serde_json::to_string(&body)?;
    tracing::debug!("json_body: {}", json_body);

    let response = request_cloud(url, Method::PUT, json_body, string_to_sign)
        .await?
        .json::<ResponseCloudOpenApi>()
        .await?;
    tracing::debug!(
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
    tracing::debug!(
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
    tracing::debug!("check_security_code: key: {}, code: {}", key, code);
    if let Some(verification_code) = codes.get(key) {
        tracing::debug!("find the code in cache: {:?}", verification_code);
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
    tracing::debug!("not find the code in cache");

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

    fn prepare_test_file(filename: &Path) {
        let content = b"localhost:8080|15801381212|0729f126a96de7b5f6e14a934034cd76beefa6f0\nlocalhost:6060|15801381212@163.com|167c7598ad08de1019a031c61cde2d822242dc66\n";
        std::fs::write(filename, content).unwrap();
    }

    #[test]
    fn test_check_phone_email_verified() {
        let filename = assert_fs::NamedTempFile::new("phone_email_verified.txt").unwrap();

        prepare_test_file(&filename);

        check_phone_email_verified(&filename, "localhost:8080").expect("should be ok");

        let result = check_phone_email_verified(&filename, "localhost:6060");
        assert!(result.is_ok());

        let result = check_phone_email_verified(&filename, "localhost:5050");
        assert!(result.is_err());

        let empty_filename =
            assert_fs::NamedTempFile::new("empty_phone_email_verified.txt").unwrap();
        let result = check_phone_email_verified(&empty_filename, "localhost:8080");
        assert!(result.is_err());
    }

    #[test]
    fn test_record_binding_phone_email() {
        let filename = assert_fs::NamedTempFile::new("phone_email_verified.txt").unwrap();
        prepare_test_file(&filename);

        let server = "localhost:8080";
        let phone_email = "15801381212";
        record_binding_phone_email(server, phone_email, &filename).unwrap();

        let result = check_phone_email_verified(&filename, server);
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_record_binding_phone_email() {
        let filename = assert_fs::NamedTempFile::new("empty_phone_email_verified.txt").unwrap();
        let server = "localhost:8080";
        let phone_email = "15801381212";
        record_binding_phone_email(server, phone_email, &filename).unwrap();

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
        println!("code: {}", code);

        assert_eq!(check_security_code(phone_email, "1234"), "error");
        assert_eq!(check_security_code(phone_email, &code), "pass");

        // 再次验证时，已经失效了，不能重复使用
        assert_eq!(check_security_code(phone_email, &code), "none");
    }

    #[test]
    fn test_url_config() {
        assert_eq!(get_url_prefix(None, "zh_CN"), CLOUD_OPENAPI_URL_CN);
        assert_eq!(get_url_prefix(None, "_"), CLOUD_OPENAPI_URL_EN);
        assert_eq!(get_url_prefix(None, "AnyOther"), CLOUD_OPENAPI_URL_EN);
    }

    #[tokio::test]
    async fn test_verification_online() {
        let _ = tracing_subscriber::fmt().try_init();
        let mut server = mockito::Server::new_async().await;
        let url = server.url();
        let openapi = format!("{url}/openapi");

        // Case 1: URL error
        let res =
            send_verification_code_with_cloud_open_api(Some(&openapi), "test@example.com", "zh_CN")
                .await;
        assert!(res.is_err(), "{res:?}");

        let verify = "/openapi/trial/verification-code";

        // Case 2: Mock success
        let m1 = server
            .mock("POST", verify)
            .match_body(mockito::Matcher::Regex(
                r#""email":"test@example.com""#.to_string(),
            ))
            .with_body(r#"{"code":200}"#)
            .create_async()
            .await;

        let res =
            send_verification_code_with_cloud_open_api(Some(&openapi), "test@example.com", "zh_CN")
                .await;
        assert_eq!(res.unwrap(), 200);
        m1.assert_async().await;

        // Case 3: Mock failed.
        let m1 = server
            .mock("POST", verify)
            .match_body(mockito::Matcher::Regex(
                r#""phone":"13500000000""#.to_string(),
            ))
            .with_body(r#"{"code":500}"#)
            .create_async()
            .await;

        let res =
            send_verification_code_with_cloud_open_api(Some(&openapi), "13500000000", "zh_CN")
                .await;
        assert_eq!(res.unwrap(), 500);
        m1.assert_async().await;
    }

    #[tokio::test]
    async fn test_report_verification() {
        let _ = tracing_subscriber::fmt().try_init();
        let mut server = mockito::Server::new_async().await;
        let url = server.url();
        let openapi = format!("{url}/openapi");
        let res = report_verification_status_to_cloud(
            Some(&openapi),
            "test@example.com",
            "200",
            "zh_CN",
            "First Name",
            "Last Name",
        )
        .await;
        assert!(res.is_err(), "{res:?}");
        let verify = "/openapi/trial/verification-result";
        let m1 = server
            .mock("POST", verify)
            .match_body(mockito::Matcher::Regex(
                r#""email":"test@example.com""#.to_string(),
            ))
            .with_body(r#"{"code":200}"#)
            .create_async()
            .await;

        let res = report_verification_status_to_cloud(
            Some(&openapi),
            "test@example.com",
            "200",
            "zh_CN",
            "First Name",
            "Last Name",
        )
        .await;
        assert_eq!(res.unwrap(), 200);
        m1.assert_async().await;

        let m2 = server
            .mock("POST", verify)
            .match_body(mockito::Matcher::Regex(
                r#""phone":"13500000000""#.to_string(),
            ))
            .with_body(r#"{"code":500}"#)
            .create_async()
            .await;

        let res = report_verification_status_to_cloud(
            Some(&openapi),
            "13500000000",
            "200",
            "zh_CN",
            "First Name",
            "Last Name",
        )
        .await;
        assert_eq!(res.unwrap(), 500);
        m2.assert_async().await;
    }

    #[tokio::test]
    async fn test_report_taosd_info_to_cloud() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
            .try_init();
        let mut server = mockito::Server::new_async().await;
        let url = server.url();
        let openapi = format!("{url}/openapi");
        let res = report_taosd_info_to_cloud(
            Some(&openapi),
            "test@example.com",
            "200",
            "zh_CN",
            "Test Name",
        )
        .await;
        assert!(res.is_err(), "{res:?}");
        let verify = "/openapi/trial/verification-result";
        let m1 = server
            .mock("PUT", verify)
            .match_body(mockito::Matcher::Regex(
                r#""email":"test@example.com""#.to_string(),
            ))
            .with_body(r#"{"code":200}"#)
            .create_async()
            .await;

        let res = report_taosd_info_to_cloud(
            Some(&openapi),
            "test@example.com",
            "zh_CN",
            "ID",
            "version",
        )
        .await;
        assert_eq!(res.unwrap(), 200);
        m1.assert_async().await;

        let m2 = server
            .mock("PUT", verify)
            .match_body(mockito::Matcher::Regex(r#""instanceId":"ID""#.to_string()))
            .with_body(r#"{"code":500}"#)
            .create_async()
            .await;

        let res =
            report_taosd_info_to_cloud(Some(&openapi), "13500000000", "zh_CN", "ID", "version")
                .await;
        assert_eq!(res.unwrap(), 500);
        m2.assert_async().await;
    }
}
