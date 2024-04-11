use captcha::filters::{Dots, Noise, Wave};
use captcha::Captcha;
use lazy_static::lazy_static;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::sync::Mutex;

pub fn sign_string(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.update("taosdata@2017-2027");
    let result = hasher.finalize();
    format!("{:x}", result)
}

fn verify_string(input: &str, expected: &str) -> bool {
    let result = sign_string(input);
    result == expected
}

pub fn record_binding_phone_email(server: &str, phone_email: &str, file: &str) {
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

pub fn check_phone_email_verified(filename: &str, current_server: &str) -> io::Result<()> {
    let file = File::open(filename)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        println!("line: {}", line);
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
    // 存储一份到缓存中
    // let expire_time = std::time::SystemTime::now()
    //     .duration_since(std::time::UNIX_EPOCH)
    //     .unwrap()
    //     .as_secs()
    //     + 60 * 5;
    // VERIFICATION_CODES.lock().unwrap().insert(
    //     key,
    //     VerificationCode {
    //         code: code.clone(),
    //         expire_time,
    //     },
    // );
    save_to_cache(key, &code);

    code
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
        .view(200, 80)
        .apply_filter(Dots::new(10));

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
        let input = "15801381212";
        let expected = "eac0aedab66250c36d517fa9401a8c415efc26afe8f95b2b70619918dbcf4d6c";
        let result = sign_string(input);
        assert_eq!(result, expected);

        // test email
        let input = "15801381212@163.com";
        let expected = "bc16f1f1a1157efa3c2c0481adeabef083eeb3f8626c39d575fc7256ef8edabb";
        let result = sign_string(input);
        assert_eq!(result, expected);
    }

    fn prepare_test_file(filename: &str) {
        let content = "localhost:8080|15801381212|eac0aedab66250c36d517fa9401a8c415efc26afe8f95b2b70619918dbcf4d6c\nlocalhost:6060|15801381212@163.com|bc16f1f1a1157efa3c2c0481adeabef083eeb3f8626c39d575fc7256ef8edabb\n";
        std::fs::write(filename, content).unwrap();
    }

    #[test]
    fn test_check_phone_email_verified() {
        let filename = "phone_email_verified.txt";
        prepare_test_file(filename);

        let result = check_phone_email_verified(filename, "localhost:8080");
        assert!(result.is_ok());

        let result = check_phone_email_verified(filename, "localhost:6060");
        assert!(result.is_ok());

        let result = check_phone_email_verified(filename, "localhost:5050");
        assert!(result.is_err());

        let empty_filename = "empty_phone_email_verified.txt";
        let result = check_phone_email_verified(empty_filename, "localhost:8080");
        assert!(result.is_err());
    }

    #[test]
    fn test_record_binding_phone_email() {
        let filename = "phone_email_verified.txt";
        prepare_test_file(filename);

        let server = "localhost:8080";
        let phone_email = "15801381212";
        record_binding_phone_email(server, phone_email, filename);

        let result = check_phone_email_verified(filename, server);
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_record_binding_phone_email() {
        let filename = "empty_phone_email_verified.txt";
        let server = "localhost:8080";
        let phone_email = "15801381212";
        record_binding_phone_email(server, phone_email, filename);

        let result = check_phone_email_verified(filename, server);
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
