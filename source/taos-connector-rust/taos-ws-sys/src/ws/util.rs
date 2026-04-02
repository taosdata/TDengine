use std::ffi::{c_char, CStr};

use libc::{setlocale, LC_CTYPE};

use crate::ws::{self, config, TaosResult};

pub fn get_system_locale() -> String {
    #[cfg(target_os = "windows")]
    {
        let locale_ptr = unsafe { setlocale(LC_CTYPE, c"en_US.UTF-8".as_ptr()) };
        if !locale_ptr.is_null() {
            return "en_US.UTF-8".to_string();
        }
        String::new()
    }

    #[cfg(not(target_os = "windows"))]
    {
        let locale_ptr = unsafe { setlocale(LC_CTYPE, c"".as_ptr()) };
        if !locale_ptr.is_null() {
            if let Ok(locale) = unsafe { CStr::from_ptr(locale_ptr).to_str() } {
                return locale.to_string();
            }
        }
        "en_US.UTF-8".to_string()
    }
}

pub fn is_cloud_host(host: &str) -> bool {
    host.contains("cloud.tdengine") || host.contains("cloud.taosdata")
}

pub fn resolve_port(host: &str, port: u16) -> u16 {
    if port != 0 {
        port
    } else if is_cloud_host(host) {
        ws::DEFAULT_CLOUD_PORT
    } else {
        ws::DEFAULT_PORT
    }
}

pub fn camel_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for ch in s.chars() {
        if ch.is_ascii_uppercase() {
            out.push('_');
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[derive(Default)]
pub struct DsnBuilder<'a> {
    addr: Option<&'a str>,
    user: Option<&'a str>,
    pass: Option<&'a str>,
    totp_code: Option<&'a str>,
    bearer_token: Option<&'a str>,
    db: Option<&'a str>,
    ws_tls_mode: Option<config::WsTlsMode>,
    ws_tls_version: Option<&'a str>,
    ws_tls_ca: Option<&'a str>,
}

impl<'a> DsnBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn addr(mut self, addr: Option<&'a str>) -> Self {
        self.addr = addr;
        self
    }

    pub fn user(mut self, user: Option<&'a str>) -> Self {
        self.user = user;
        self
    }

    pub fn pass(mut self, pass: Option<&'a str>) -> Self {
        self.pass = pass;
        self
    }

    pub fn totp_code(mut self, totp_code: Option<&'a str>) -> Self {
        self.totp_code = totp_code;
        self
    }

    pub fn bearer_token(mut self, bearer_token: Option<&'a str>) -> Self {
        self.bearer_token = bearer_token;
        self
    }

    pub fn db(mut self, db: Option<&'a str>) -> Self {
        self.db = db;
        self
    }

    pub fn ws_tls_mode(mut self, mode: Option<config::WsTlsMode>) -> Self {
        self.ws_tls_mode = mode;
        self
    }

    pub fn ws_tls_version(mut self, version: Option<&'a str>) -> Self {
        self.ws_tls_version = version;
        self
    }

    pub fn ws_tls_ca(mut self, ca: Option<&'a str>) -> Self {
        self.ws_tls_ca = ca;
        self
    }

    pub fn build(self) -> String {
        let addr = match self.addr {
            Some(addr) => addr.to_string(),
            None => match config::adapter_list() {
                Some(addr) => addr.to_string(),
                None => format!("{}:{}", ws::DEFAULT_HOST, ws::DEFAULT_PORT),
            },
        };

        let user = self.user.unwrap_or(ws::DEFAULT_USER);
        let pass = self.pass.unwrap_or(ws::DEFAULT_PASS);
        let db = self.db.unwrap_or(ws::DEFAULT_DB);

        let ws_tls_mode = self.ws_tls_mode.unwrap_or_else(config::ws_tls_mode);

        let ws_tls_version = self
            .ws_tls_version
            .map_or_else(|| config::ws_tls_version().to_string(), |s| s.to_string());

        let ws_tls_ca = self
            .ws_tls_ca
            .map(|s| s.to_string())
            .or_else(|| config::ws_tls_ca().map(|s| s.to_string()));

        let compression = config::compression();
        let conn_retries = config::conn_retries();
        let retry_backoff_ms = config::retry_backoff_ms();
        let retry_backoff_max_ms = config::retry_backoff_max_ms();

        let protocol = match ws_tls_mode {
            config::WsTlsMode::Disabled => "ws",
            _ => "wss",
        };

        let ws_tls_mode = match ws_tls_mode {
            config::WsTlsMode::VerifyCa => Some("verify_ca"),
            config::WsTlsMode::VerifyIdentity => Some("verify_identity"),
            _ => None,
        };

        let tls_mode = match ws_tls_mode {
            Some(mode) => format!("&tls_mode={mode}"),
            None => String::new(),
        };

        let tls_ca = match ws_tls_ca {
            Some(ca) => format!("&tls_ca={ca}"),
            None => String::new(),
        };

        let totp_or_token = match (self.bearer_token, self.totp_code) {
            (Some(token), _) => format!("&bearer_token={token}"),
            (_, Some(totp)) => format!("&totp_code={totp}"),
            _ => String::new(),
        };

        let other_params =
            format!("&tls_version={ws_tls_version}{tls_mode}{tls_ca}{totp_or_token}");

        let params = format!(
            "compression={compression}\
            &conn_retries={conn_retries}\
            &retry_backoff_ms={retry_backoff_ms}\
            &retry_backoff_max_ms={retry_backoff_max_ms}"
        );

        if is_cloud_host(&addr) && user == "token" {
            format!("wss://{addr}/{db}?token={pass}&{params}")
        } else {
            format!("{protocol}://{user}:{pass}@{addr}/{db}?{params}{other_params}")
        }
    }
}

pub unsafe fn c_char_to_str<'a>(ptr: *const c_char) -> TaosResult<Option<&'a str>> {
    if !ptr.is_null() {
        CStr::from_ptr(ptr).to_str().map(Some).map_err(Into::into)
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_system_locale() {
        let locale = get_system_locale();
        println!("system locale: {}", locale);
        #[cfg(not(target_os = "windows"))]
        assert!(!locale.is_empty());
    }

    #[test]
    fn test_camel_to_snake() {
        assert_eq!(camel_to_snake("CamelCase"), "_camel_case");
        assert_eq!(camel_to_snake("camelCaseTest"), "camel_case_test");
        assert_eq!(camel_to_snake("lowercase"), "lowercase");
        assert_eq!(camel_to_snake("UPPERCASE"), "_u_p_p_e_r_c_a_s_e");
    }
}
