use captcha::filters::{Dots, Noise, Wave};
use captcha::{Captcha, Geometry};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Mutex;

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
struct ResponseSendVerificationCode {
    code: u32,
    msg: Option<String>,
}

pub async fn send_verification_code_with_cloud_open_api(
    url: &str,
    phone_email: &str,
    lang: Option<&str>,
) -> anyhow::Result<u32> {
    let mut phone = "";
    let mut email = "";
    match phone_email.find("@") {
        Some(_) => email = phone_email,
        None => phone = phone_email,
    }
    let lang_code = match lang {
        Some("zh") => "zh_CN",
        _ => "en_US",
    };

    let duration = 10_u8;
    let nonce = rand::random::<u64>() % 1000000000;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let code = generate_verification_code(phone_email.to_string());
    let string_to_sign = format!(
        "code={}&duration={}&email={}&language={}&phone={}&nonce={}&ts={}",
        code, duration, email, lang_code, phone, nonce, ts
    );
    let sign = sign_string(&string_to_sign);
    // log::debug!("send_verification_code_with_cloud_open_api: [{}] sign: [{}]", string_to_sign, sign);

    let body = RequestBodySendVerificationCode {
        phone: phone.to_string(),
        email: email.to_string(),
        code,
        duration: 10,
        language: lang_code.to_string(),
    };
    let json_body = serde_json::to_string(&body)?;
    // log::debug!("send_verification_code === url: {}", url);
    log::debug!("send_verification_code json_body: {}", json_body);

    let http_client = reqwest::Client::new();
    let response = http_client
        .post(url)
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
        .await?
        .json::<ResponseSendVerificationCode>()
        .await?;

    log::debug!("response: {:?}", response);
    Ok(response.code)
}

fn save_to_cache(key: String, code: &String) {
    let expire_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60 * 5;
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
    if let Some(verification_code) = codes.get(key) {
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

        // test email
        // let input = "15801381212@163.com";
        // let expected = "bc16f1f1a1157efa3c2c0481adeabef083eeb3f8626c39d575fc7256ef8edabb";
        // let result = sign_string(input);
        // assert_eq!(result, expected);
    }

    fn prepare_test_file(filename: &PathBuf) {
        let content = "localhost:8080|15801381212|eac0aedab66250c36d517fa9401a8c415efc26afe8f95b2b70619918dbcf4d6c\nlocalhost:6060|15801381212@163.com|bc16f1f1a1157efa3c2c0481adeabef083eeb3f8626c39d575fc7256ef8edabb\n";
        std::fs::write(filename, content).unwrap();
    }

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
