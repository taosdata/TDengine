use std::env;

pub fn test_username() -> String {
    env::var("TEST_USERNAME").unwrap_or_else(|_| "root".to_string())
}

pub fn test_password() -> String {
    env::var("TEST_PASSWORD").unwrap_or_else(|_| "taosdata".to_string())
}
