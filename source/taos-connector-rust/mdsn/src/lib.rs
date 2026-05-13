//! M-DSN: A Multi-address DSN(Data Source Name) parser.
//!
//! M-DSN support two kind of DSN format:
//!
//! 1. `<driver>[+<protocol>]://<username>:<password>@<addresses>/<database>?<params>`
//! 2. `<driver>[+<protocol>]://<username>:<password>@<fragment>?<params>`
//! 3. `<driver>://<username>:<password>@<protocol>(<addresses>)/<database>?<params>`
//!
//! All the items will be parsed into struct [Dsn](crate::Dsn).
//!
//! ## Parser
//!
//! ```rust
//! use mdsn::Dsn;
//! use std::str::FromStr;
//!
//! # fn main() -> Result<(), mdsn::DsnError> {
//! // The two styles are equivalent.
//! let dsn = Dsn::from_str("taos://root:taosdata@host1:6030,host2:6030/db")?;
//! let dsn: Dsn = "taos://root:taosdata@host1:6030,host2:6030/db".parse()?;
//!
//! assert_eq!(dsn.driver, "taos");
//! assert_eq!(dsn.username.unwrap(), "root");
//! assert_eq!(dsn.password.unwrap(), "taosdata");
//! assert_eq!(dsn.subject.unwrap(), "db");
//! assert_eq!(dsn.addresses.len(), 2);
//! assert_eq!(dsn.addresses, vec![
//!     mdsn::Address::new("host1", 6030),
//!     mdsn::Address::new("host2", 6030),
//! ]);
//! # Ok(())
//! # }
//! ```
//!
//! ## DSN Examples
//!
//! A DSN for [TDengine](https://taosdata.com) driver [taos](https://docs.rs/taos).
//!
//! ```dsn
//! taos://root:taosdata@localhost:6030/db?timezone=Asia/Shanghai&asyncLog=1
//! ```
//!
//! With multi-address:
//!
//! ```dsn
//! taos://root:taosdata@host1:6030,host2:6030/db?timezone=Asia/Shanghai
//! ```
//!
//! A DSN for unix socket:
//!
//! ```dsn
//! unix:///path/to/unix.sock?param1=value
//! ```
//!
//! A DSN for postgresql with url-encoded socket directory path.
//!
//! ```dsn
//! postgresql://%2Fvar%2Flib%2Fpostgresql/db
//! ```
//!
//! A DSN for sqlite db file, note that you must use prefix `./` for a relative path file.
//!
//! ```dsn
//! sqlite://./file.db
//! ```
//!
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;

use itertools::Itertools;
#[cfg(feature = "pest")]
use pest::Parser;
#[cfg(feature = "pest")]
use pest_derive::Parser;
use regex::Regex;
use thiserror::Error;
use urlencoding::{decode, encode};

#[cfg(feature = "pest")]
#[derive(Parser)]
#[grammar = "dsn.pest"]
struct DsnParser;

/// Error caused by [pest](https://docs.rs/pest) DSN parser.
#[derive(Debug, Error)]
pub enum DsnError {
    #[cfg(feature = "pest")]
    #[error("{0}")]
    ParseErr(#[from] pest::error::Error<Rule>),
    #[error("unable to parse port from {0}")]
    PortErr(#[from] ParseIntError),
    #[error("invalid dsn format: {0}, use either path:/path or driver://user:pass@host:port?k=v")]
    InvalidFormat(String),
    #[error("No `:` found in {0}, use either path:/path or driver://user:pass@host:port?k=v")]
    NoColonFound(String),
    #[error("dsn contains '@' character({0}), please use full DSN format: <driver>://<username>:<password>@<addresses>/<database>?<params>")]
    InvalidSpecialCharacterFormat(String),
    #[error("invalid driver {0}")]
    InvalidDriver(String),
    #[error("invalid protocol {0}")]
    InvalidProtocol(String),
    #[error("invalid password {0}: {1}")]
    InvalidPassword(String, FromUtf8Error),
    #[error("invalid connection {0}")]
    InvalidConnection(String),
    #[error("invalid addresses: {0}, error: {1}")]
    InvalidAddresses(String, String),
    #[error("requires database: {0}")]
    RequireDatabase(String),
    #[error("requires parameter: {0}")]
    RequireParam(String),
    #[error("invalid parameter for {0}: {1}")]
    InvalidParam(String, String),
    #[error("non utf8 character: {0}")]
    NonUtf8Character(#[from] FromUtf8Error),
}

/// A simple struct to represent a server address, with host:port or socket path.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Address {
    /// Host or ip address of the server.
    pub host: Option<String>,
    /// Port to connect to the server.
    pub port: Option<u16>,
    /// Use unix socket path to connect.
    pub path: Option<String>,
}

impl Address {
    /// Construct server address with host and port.
    #[inline]
    pub fn new<T: Into<String>>(host: T, port: u16) -> Self {
        let host = host.into();
        let host = if host.is_empty() { None } else { Some(host) };
        Self {
            host,
            port: Some(port),
            ..Default::default()
        }
    }

    /// Construct server address with host or ip address only.
    #[inline]
    pub fn from_host<T: Into<String>>(host: T) -> Self {
        let host = host.into();
        let host = if host.is_empty() { None } else { Some(host) };
        Self {
            host,
            ..Default::default()
        }
    }

    /// Construct server address with unix socket path.
    #[inline]
    pub fn from_path<T: Into<String>>(path: T) -> Self {
        Self {
            path: Some(path.into()),
            ..Default::default()
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.host.is_none() && self.port.is_none() && self.path.is_none()
    }
}

impl FromStr for Address {
    type Err = DsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(feature = "pest")]
        {
            let mut addr = Self::default();
            if let Some(dsn) = DsnParser::parse(Rule::address, &s)?.next() {
                for inner in dsn.into_inner() {
                    match inner.as_rule() {
                        Rule::host => addr.host = Some(inner.as_str().to_string()),
                        Rule::port => addr.port = Some(inner.as_str().parse()?),
                        Rule::path => {
                            addr.path = Some(
                                urlencoding::decode(inner.as_str())
                                    .expect("UTF-8")
                                    .to_string(),
                            )
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Ok(addr)
        }
        #[cfg(not(feature = "pest"))]
        {
            if s.is_empty() {
                Ok(Self::default())
            } else if let Some((host, port)) = s.split_once(':') {
                Ok(Address::new(host, port.parse().unwrap()))
            } else if s.contains('%') {
                Ok(Address::from_path(urlencoding::decode(s).unwrap()))
            } else {
                Ok(Address::from_host(s))
            }
        }
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.host, self.port, &self.path) {
            (Some(host), None, None) => write!(f, "{host}"),
            (Some(host), Some(port), None) => write!(f, "{host}:{port}"),
            (None, Some(port), None) => write!(f, ":{port}"),
            (None, None, Some(path)) => write!(f, "{}", urlencoding::encode(path)),
            (None, None, None) => Ok(()),
            _ => unreachable!("path will be conflict with host/port"),
        }
    }
}

#[test]
#[cfg(feature = "pest")]
fn addr_parse() {
    let s = "taosdata:6030";
    let addr = Address::from_str(s).unwrap();
    assert_eq!(addr.to_string(), s);
    assert!(!addr.is_empty());

    let s = "/var/lib/taos";
    let addr = Address::from_str(&urlencoding::encode(s)).unwrap();
    assert_eq!(addr.path.as_ref().unwrap(), s);
    assert_eq!(addr.to_string(), urlencoding::encode(s));

    assert_eq!(
        Address::new("localhost", 6030).to_string(),
        "localhost:6030"
    );
    assert_eq!(Address::from_host("localhost").to_string(), "localhost");
    assert_eq!(
        Address::from_path("/path/unix.sock").to_string(),
        "%2Fpath%2Funix.sock"
    );

    let none = Address {
        host: None,
        port: None,
        path: None,
    };
    assert!(none.is_empty());
    assert_eq!(none.to_string(), "");
}

/// A DSN(**Data Source Name**) parser.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Dsn {
    pub driver: String,
    pub protocol: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub addresses: Vec<Address>,
    pub path: Option<String>,
    pub subject: Option<String>,
    pub params: BTreeMap<String, String>,
}

impl Dsn {
    pub fn from_regex(input: &str) -> Result<Self, DsnError> {
        // lazy_static::lazy_static! {
        //     static ref RE: Regex = Regex::new(r"(?x)
        //         (?P<driver>[\w.-]+)(\+(?P<protocol>[^@/?\#]+))?: # abc
        //         (
        //             //((?P<username>[\w\s\-_%.]+)?(:(?P<password>[^@/?\#]+))?@)? # for authorization
        //                 (((?P<protocol2>[\w\s.-]+)\()?
        //                     (?P<addr>[\w\-_%.:]*(:\d{0,5})?(,[\w\-:_.]*(:\d{0,5})?)*)  # for addresses
        //                 \)?)?
        //                 (/(?P<subject>[\w\s%$@.,/-]+)?)?                             # for subject
        //             |
        //             # url-like dsn
        //             //((?P<username>[\w\s\-_%.]+)?(:(?P<password>[^@/?\#]+))?@)? # for authorization
        //                 (((?P<protocol2>[\w\s.-]+)\()?
        //                     (?P<addr>[\w\-_%.:]*(:\d{0,5})?(,[\w\-:_.]*(:\d{0,5})?)*)?  # for addresses
        //                 \)?)?
        //                 (/(?P<subject>[\w\s%$@.,/-]+)?)?                             # for subject
        //             | # or
        //             # path-like dsn
        //             (?P<path>([\\/.~]$|/\s*\w+[\w\s %$@*:,.\\\-/ _\(\)\[\]{}（）【】｛｝]*|[\.~\w\s]?[\w\s %$@*:,.\\\-/ _\(\)\[\]{}（）【】｛｝]+))
        //         ) # abc
        //         (\?(?P<params>(?s:.)*))?").unwrap();
        // }

        if !input.contains(':') {
            return Err(DsnError::NoColonFound(input.to_string()));
        }

        let (driver, conf) = input.split_once(':').unwrap();

        let (driver, protocol) = if let Some((driver, protocol)) = driver.split_once('+') {
            (driver.to_string(), Some(protocol.to_string()))
        } else {
            (driver.to_string(), None)
        };

        #[inline]
        fn percent_decode(encoded: String) -> String {
            decode(&encoded).map(|s| s.to_string()).unwrap_or(encoded)
        }
        type ParsedAddr = (Option<String>, Option<String>, Option<String>, Vec<Address>);
        fn parse_single_addr(addr: &str) -> Result<Option<Address>, DsnError> {
            lazy_static::lazy_static! {
                static ref AT_SINGLE_ADDR_WITH_PORT_REGEX: Regex = Regex::new(r"^(\[[0-9a-fA-F:]+\]|[\w\-_%.]*)(:(\d{0,5}))?$").unwrap();
            }
            if addr.is_empty() {
                return Ok(None);
            }
            if let Some(cap) = AT_SINGLE_ADDR_WITH_PORT_REGEX.captures(addr) {
                let host = cap
                    .get(1)
                    .and_then(|s| {
                        let s = s.as_str();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s.to_string())
                        }
                    })
                    .map(percent_decode);
                let port = cap.get(3).and_then(|m| m.as_str().parse::<u16>().ok());

                match (host, port) {
                    (Some(host), Some(port)) => Ok(Some(Address::new(host, port))),
                    (Some(host), None) => {
                        if host.contains('/') {
                            Ok(Some(Address::from_path(host)))
                        } else {
                            Ok(Some(Address::from_host(host)))
                        }
                    }
                    (host, port) => Ok(Some(Address {
                        host,
                        port,
                        ..Default::default()
                    })),
                }
            } else {
                Err(DsnError::InvalidAddresses(
                    addr.to_string(),
                    "incorrect host:port format".to_string(),
                ))
            }
        }
        fn parse_addr(main: &str) -> Result<ParsedAddr, DsnError> {
            if main.trim().is_empty() {
                return Ok((None, None, None, vec![]));
            }
            let mut protocol = None;
            if let Some(at) = main
                .bytes()
                .enumerate()
                .rev()
                .find(|(_, v)| *v == b'@')
                .map(|(at, _)| at)
            {
                let (user_pass, addrs) = main.split_at(at);
                let mut addrs = addrs.strip_prefix('@').expect("strip prefix @");

                let addrs = if addrs.is_empty() {
                    vec![]
                } else {
                    lazy_static::lazy_static! {
                        static ref AT_SINGLE_ADDR_REQUIRE_PORT_REGEX: Regex = Regex::new(r"^(.+):(\d{1,5})$").unwrap();
                        static ref AT_MULTI_ADDR_REQUIRE_PORT_REGEX: Regex = Regex::new(r"^(?P<host>.+):(?P<port>\d{1,5})(,(([\w\-_%.]+)(:\d{0,5})?))*$").unwrap();
                        static ref PROTOCOL2_MULTI_ADDR_REQUIRE_PORT_REGEX: Regex = Regex::new(r"^((?P<protocol>.*)\((?P<addrs>.*)\))$").unwrap();
                    }
                    if !addrs.contains(':') {
                        vec![Address::from_host(percent_decode(addrs.to_string()))]
                    } else {
                        if let Some(cap) = PROTOCOL2_MULTI_ADDR_REQUIRE_PORT_REGEX.captures(addrs) {
                            protocol = cap
                                .name("protocol")
                                .map(|m: regex::Match<'_>| m.as_str().to_string());
                            addrs = cap.name("addrs").map(|m| m.as_str()).unwrap();
                        }
                        addrs
                            .split(',')
                            .filter_map(|addr| {
                                if addr.is_empty() {
                                    return None;
                                }
                                if let Some(port) =
                                    addr.strip_prefix(':').map(|port| port.parse::<u16>())
                                {
                                    return port
                                        .map_err(|e| {
                                            DsnError::InvalidAddresses(
                                                addr.to_string(),
                                                e.to_string(),
                                            )
                                        })
                                        .map(|port| Address {
                                            port: Some(port),
                                            ..Default::default()
                                        })
                                        .map(Some)
                                        .transpose();
                                }
                                if let Some(host) = addr.strip_suffix(':') {
                                    return Some(Ok(Address::from_host(percent_decode(
                                        host.to_string(),
                                    ))));
                                }
                                AT_SINGLE_ADDR_REQUIRE_PORT_REGEX
                                    .captures(addr)
                                    .ok_or_else(|| {
                                        DsnError::InvalidAddresses(
                                            addr.to_string(),
                                            "format error, use host:port syntax".to_string(),
                                        )
                                    })
                                    .and_then(|cap| {
                                        let host = cap
                                            .get(1)
                                            .map(|m| m.as_str().to_string())
                                            .map(percent_decode);
                                        let port = cap
                                            .get(2)
                                            .map(|m| {
                                                m.as_str().parse::<u16>().map_err(|e| {
                                                    DsnError::InvalidAddresses(
                                                        addr.to_string(),
                                                        e.to_string(),
                                                    )
                                                })
                                            })
                                            .transpose()?;
                                        Ok(Some(Address {
                                            host,
                                            port,
                                            ..Default::default()
                                        }))
                                    })
                                    .transpose()
                            })
                            .try_collect()?
                    }
                };

                let (username, password) = if let Some((user, pass)) = user_pass.split_once(':') {
                    (Some(user.to_string()), Some(pass.to_string()))
                } else {
                    (Some(user_pass.to_string()), None)
                };

                Ok((protocol, username, password, addrs))
            } else {
                lazy_static::lazy_static! {
                    static ref AT_MULTI_ADDR_WITH_PORT_REGEX: Regex = Regex::new(r"^(?P<host>\[[0-9a-fA-F:]+\]|[\w\-_%.]*)(:(?P<port>\d{0,5}))?[,(?P<addr2>(?P<host2>\[[0-9a-fA-F:]+\]|[\w\-_%.]*)(:(?P<port2>\d{0,5}))?)]*$").unwrap();
                }
                if main == ":" {
                    return Ok((protocol, None, None, vec![]));
                }
                match parse_single_addr(main) {
                    Ok(Some(addr)) => Ok((protocol, None, None, vec![addr])),
                    Ok(None) => Ok((protocol, None, None, Vec::new())),
                    Err(err) => {
                        if AT_MULTI_ADDR_WITH_PORT_REGEX.is_match(main) {
                            Ok((
                                protocol,
                                None,
                                None,
                                main.split(',')
                                    .map(|s| s.trim())
                                    .map(parse_single_addr)
                                    // .filter(|v| !v.is_ok_and(|s| s.is_none()))
                                    .filter_map_ok(|v| v)
                                    .try_collect()?,
                            ))
                        } else {
                            Err(err)
                        }
                    }
                }
            }
        }
        if conf.starts_with("//") {
            let main = conf.strip_prefix("//").expect("strip prefix");

            lazy_static::lazy_static! {
                static ref ADDR_PREFIX_REGEX: Regex = Regex::new(r"^(?P<addr>[\w\-_%.:]*(:\d{0,5})?(,[\w\-:_.]*(:\d{0,5})?)*)").unwrap();
                static ref URL_PARAMS_REGEX: Regex = Regex::new(r"^(?P<main>.*)(\?(?P<params>[^?]+))$").unwrap();
                static ref ENDS_WITH_ADDR_REGEX: Regex = Regex::new(r"@(\[[0-9a-fA-F:]+\]|[\w\-_%.])+(:\d{1,5})?(,(\[[0-9a-fA-F:]+\]|[\w\-_%.])+(:\d{1,5})?)*$").unwrap();
                static ref PARAM_WITH_AT_REGEX: Regex = Regex::new(r"\?.*=\S+$").unwrap();
            }
            let dsn = Dsn {
                driver,
                protocol,
                path: None,
                ..Default::default()
            };
            fn try_parse_url(dsn: &Dsn, main: &str, at: Option<usize>) -> Result<Dsn, DsnError> {
                let mut dsn = dsn.clone();
                let (main, params) = if let Some(at) = at {
                    let (main, params) = main.split_at(at);
                    let params = params
                        .strip_prefix('?')
                        .expect("split_at.1 must contain ?")
                        .trim();
                    if params.is_empty() {
                        (main, None)
                    } else {
                        (main, Some(params))
                    }
                } else {
                    (main, None)
                };

                if let Some(p) = params {
                    for p in p.split('&') {
                        if p.contains('=') {
                            if let Some((k, v)) = p.split_once('=') {
                                let k = urlencoding::decode(k)?;
                                let v = urlencoding::decode(v)?;
                                dsn.params.insert(k.to_string(), v.to_string());
                            }
                        } else {
                            let p = urlencoding::decode(p)?;
                            dsn.params.insert(p.to_string(), String::new());
                        }
                    }
                }

                if let Some((addr, subject)) = main.split_once('/') {
                    let (addr, subject) = (addr.trim(), subject.trim());
                    let (protocol, username, password, addresses) = parse_addr(addr)?;
                    let subject = if subject.is_empty() {
                        None
                    } else {
                        if subject.contains('?') {
                            return Err(DsnError::InvalidSpecialCharacterFormat(
                                "Subject contains '?' mark".to_string(),
                            ));
                        }
                        Some(subject.to_string())
                    };

                    if protocol.is_some() {
                        dsn.protocol = protocol;
                    }
                    dsn.subject = subject;
                    dsn.username = username.map(percent_decode);
                    dsn.password = password.map(percent_decode);
                    dsn.addresses = addresses;
                } else {
                    let (protocol, username, password, addresses) = parse_addr(main)?;

                    if protocol.is_some() {
                        dsn.protocol = protocol;
                    }
                    dsn.username = username.map(percent_decode);
                    dsn.password = password.map(percent_decode);
                    dsn.addresses = addresses;
                }
                Ok(dsn)
            }

            fn get_question_position(main: &str) -> Option<Vec<usize>> {
                let question_pos = main
                    .bytes()
                    .enumerate()
                    .rev()
                    .filter(|(_, v)| *v == b'?')
                    .map(|(at, _v)| at)
                    .collect_vec();

                if question_pos.is_empty() {
                    None
                } else {
                    Some(question_pos)
                }
            }
            let at = if !main.starts_with('?') && ENDS_WITH_ADDR_REGEX.is_match(main) {
                let res = try_parse_url(&dsn, main, None);
                if res.is_err() {
                    get_question_position(main)
                } else {
                    None
                }
            } else {
                get_question_position(main)
            };
            // let (main, params, sep_at) = if ENDS_WITH_ADDR_REGEX.is_match(main) {
            //     (main, None, 0)
            // } else if let Some(at) = main
            //     .bytes()
            //     .enumerate()
            //     .rev()
            //     .find(|(_, v)| *v == b'?')
            //     .map(|(at, _v)| at)
            // {
            //     let (main, params) = main.split_at(at);
            //     let params = params
            //         .strip_prefix('?')
            //         .expect("split_at.1 must contain ?")
            //         .trim();
            //     if params.is_empty() {
            //         (main, None, at)
            //     } else {
            //         (main, Some(params), at)
            //     }
            // } else {
            //     (main, None, 0)
            // };

            if let Some(pos) = at {
                for (i, at) in pos.iter().enumerate() {
                    let res = try_parse_url(&dsn, main, Some(*at));
                    if res.is_ok() {
                        return res;
                    }
                    if res.is_err() && i == pos.len() - 1 {
                        return res;
                    }
                }
                Err(DsnError::InvalidFormat(input.to_string()))
            } else {
                try_parse_url(&dsn, main, None)
            }
        } else {
            // path-like dsn
            lazy_static::lazy_static! {
                static ref PATH_REGEX: Regex = Regex::new(r"(?P<path>[^?]*)\?(?P<params>.*)").unwrap();
            }
            let mut dsn = Dsn {
                driver,
                protocol,
                path: None,
                ..Default::default()
            };
            if let Some(cap) = PATH_REGEX.captures(conf) {
                let path = cap.name("path").map(|m| m.as_str().to_string());
                let params = cap.name("params").map(|m| m.as_str().to_string());
                dsn.path = path;
                dsn.params = params
                    .map(|s| {
                        s.split('&')
                            .map(|p| {
                                if let Some((k, v)) = p.split_once('=') {
                                    (percent_decode(k.to_string()), percent_decode(v.to_string()))
                                } else {
                                    (percent_decode(p.to_string()), percent_decode(String::new()))
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
            } else {
                dsn.path = Some(percent_decode(conf.to_string()));
            }
            Ok(dsn)
        }
    }

    #[cfg(feature = "pest")]
    pub fn from_pest(input: &str) -> Result<Self, DsnError> {
        let dsn = DsnParser::parse(Rule::dsn, input)?.next().unwrap();

        let mut to = Dsn::default();
        for pair in dsn.into_inner() {
            match pair.as_rule() {
                Rule::scheme => {
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::driver => to.driver = inner.as_str().to_string(),
                            Rule::protocol => to.protocol = Some(inner.as_str().to_string()),
                            _ => unreachable!(),
                        }
                    }
                }
                Rule::SCHEME_IDENT => (),
                Rule::username_with_password => {
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::username => to.username = Some(inner.as_str().to_string()),
                            Rule::password => to.password = Some(inner.as_str().to_string()),
                            _ => unreachable!(),
                        }
                    }
                }
                Rule::protocol_with_addresses => {
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::addresses => {
                                for inner in inner.into_inner() {
                                    match inner.as_rule() {
                                        Rule::address => {
                                            let mut addr = Address::default();
                                            for inner in inner.into_inner() {
                                                match inner.as_rule() {
                                                    Rule::host => {
                                                        addr.host = Some(inner.as_str().to_string())
                                                    }
                                                    Rule::port => {
                                                        addr.port = Some(inner.as_str().parse()?)
                                                    }
                                                    Rule::path => {
                                                        addr.path = Some(
                                                            urlencoding::decode(inner.as_str())
                                                                .expect("UTF-8")
                                                                .to_string(),
                                                        )
                                                    }
                                                    _ => unreachable!(),
                                                }
                                            }
                                            to.addresses.push(addr);
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            }
                            Rule::protocol => to.protocol = Some(inner.as_str().to_string()),
                            _ => unreachable!(),
                        }
                    }
                }
                Rule::database => {
                    to.subject = Some(pair.as_str().to_string());
                }
                Rule::fragment => {
                    to.path = Some(pair.as_str().to_string());
                }
                Rule::path_like => {
                    to.path = Some(pair.as_str().to_string());
                }
                Rule::param => {
                    let (mut name, mut value) = (String::new(), String::new());
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::name => name = inner.as_str().to_string(),
                            Rule::value => value = inner.as_str().to_string(),
                            _ => unreachable!(),
                        }
                    }
                    to.params.insert(name, value);
                }
                Rule::EOI => {}
                _ => unreachable!(),
            }
        }
        Ok(to)
    }

    #[inline]
    pub fn drain_params(&mut self) -> BTreeMap<String, String> {
        let drained = self
            .params
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.params.clear();
        drained
    }

    #[inline]
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) -> Option<String> {
        self.params.insert(key.into(), value.into())
    }

    #[inline]
    pub fn get<T: AsRef<str>>(&self, key: T) -> Option<&String> {
        self.params.get(key.as_ref())
    }

    #[inline]
    pub fn remove<T: AsRef<str>>(&mut self, key: T) -> Option<String> {
        self.params.remove(key.as_ref())
    }

    fn is_path_like(&self) -> bool {
        self.username.is_none()
            && self.password.is_none()
            && self.addresses.is_empty()
            && self.path.is_some()
    }

    /// Display with sensitive fields redacted.
    pub fn redacted(&self) -> RedactedDsn<'_> {
        RedactedDsn(self)
    }
}

pub trait IntoDsn: Send {
    fn into_dsn(self) -> Result<Dsn, DsnError>;
}

impl IntoDsn for &str {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        self.parse()
    }
}

impl IntoDsn for String {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        self.as_str().into_dsn()
    }
}

impl IntoDsn for &String {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        self.as_str().into_dsn()
    }
}

impl IntoDsn for &Dsn {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        Ok(self.clone())
    }
}

impl IntoDsn for Dsn {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        Ok(self)
    }
}

impl Display for Dsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.driver)?;
        if let Some(protocol) = &self.protocol {
            write!(f, "+{protocol}")?;
        }

        if self.is_path_like() {
            write!(f, ":")?;
        } else {
            write!(f, "://")?;
        }

        match (&self.username, &self.password) {
            (Some(username), Some(password)) => {
                write!(f, "{}:{}@", encode(username), encode(password))?;
            }
            (Some(username), None) => write!(f, "{}@", encode(username))?,
            (None, Some(password)) => write!(f, ":{}@", encode(password))?,
            (None, None) => {}
        }

        if !self.addresses.is_empty() {
            write!(
                f,
                "{}",
                self.addresses.iter().map(ToString::to_string).join(",")
            )?;
        }
        if let Some(database) = &self.subject {
            write!(f, "/{database}")?;
        } else if let Some(path) = &self.path {
            write!(f, "{path}")?;
        }

        if !self.params.is_empty() {
            write!(
                f,
                "?{}",
                self.params
                    .iter()
                    .map(|(k, v)| format!(
                        "{}={}",
                        percent_encode_or_not(k),
                        percent_encode_or_not(v)
                    ))
                    .join("&")
            )?;
        }
        Ok(())
    }
}

/// A wrapper around `&Dsn` that implements `Display` with sensitive fields redacted.
///
/// Use this type when you need to log or display a DSN without exposing passwords
/// or sensitive params (totp_code, totpCode, bearer_token, bearerToken, token).
pub struct RedactedDsn<'a>(pub &'a Dsn);

impl<'a> Display for RedactedDsn<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dsn = self.0;
        write!(f, "{}", dsn.driver)?;
        if let Some(protocol) = &dsn.protocol {
            write!(f, "+{protocol}")?;
        }

        if dsn.is_path_like() {
            write!(f, ":")?;
        } else {
            write!(f, "://")?;
        }

        match (&dsn.username, &dsn.password) {
            (Some(username), Some(_)) => {
                write!(f, "{}:[REDACTED]@", encode(username))?;
            }
            (Some(username), None) => write!(f, "{}@", encode(username))?,
            (None, Some(_)) => write!(f, ":[REDACTED]@")?,
            (None, None) => {}
        }

        if !dsn.addresses.is_empty() {
            write!(
                f,
                "{}",
                dsn.addresses.iter().map(ToString::to_string).join(",")
            )?;
        }

        if let Some(database) = &dsn.subject {
            write!(f, "/{database}")?;
        } else if let Some(path) = &dsn.path {
            write!(f, "{path}")?;
        }

        if !dsn.params.is_empty() {
            write!(
                f,
                "?{}",
                dsn.params
                    .iter()
                    .map(|(k, v)| {
                        const SENSITIVE_PARAMS: &[&str] = &[
                            "totp_code",
                            "totpCode",
                            "bearer_token",
                            "bearerToken",
                            "token",
                            "td.connect.token",
                        ];
                        let value = if SENSITIVE_PARAMS.contains(&k.as_str()) {
                            "[REDACTED]"
                        } else {
                            v.as_str()
                        };

                        format!(
                            "{}={}",
                            percent_encode_or_not(k),
                            percent_encode_or_not(value)
                        )
                    })
                    .join("&")
            )?;
        }
        Ok(())
    }
}

/// Convenience function to redact a DSN string.
///
/// Parses the input and returns a redacted string representation.
/// Returns `"<invalid dsn>"` if the input cannot be parsed.
pub fn redact_dsn(dsn: &str) -> String {
    match Dsn::from_str(dsn) {
        Ok(dsn) => format!("{}", dsn.redacted()),
        Err(_) => "<invalid dsn>".to_string(),
    }
}

fn percent_encode_or_not(v: &str) -> Cow<'_, str> {
    if v.contains(['=', '&', '#', '@']) {
        urlencoding::encode(v)
    } else {
        v.into()
    }
}

impl TryFrom<&str> for Dsn {
    type Error = DsnError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Dsn::from_str(value)
    }
}

impl TryFrom<&String> for Dsn {
    type Error = DsnError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Dsn::from_str(value)
    }
}

impl TryFrom<String> for Dsn {
    type Error = DsnError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Dsn::from_str(&value)
    }
}

impl TryFrom<&Dsn> for Dsn {
    type Error = DsnError;

    fn try_from(value: &Dsn) -> Result<Self, Self::Error> {
        Ok(value.clone())
    }
}

impl FromStr for Dsn {
    type Err = DsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(feature = "pest")]
        return Self::from_pest(s);
        #[cfg(not(feature = "pest"))]
        return Self::from_regex(s);
    }
}

/// Returns if a dsn param value is true or false.
///
/// - Empty means true.
/// - 1/t/true/yes/on/enable/enabled are true.
/// - All others are false.
///
/// # Examples
///
/// ```rust
/// use mdsn::value_is_true;
///
/// for s in ["", "1", "true", "t", "yes", "y", "on", "enable", "enabled"] {
///     assert_eq!(value_is_true(s), true);
/// }
/// for s in ["0", "false", "f", "no", "n", "disable", "disabled", "any-other-str"] {
///     assert_eq!(value_is_true(s), false);
/// }
/// ```
pub fn value_is_true<T: AsRef<str>>(s: T) -> bool {
    matches!(
        s.as_ref(),
        "" | "1"
            | "true"
            | "t"
            | "yes"
            | "y"
            | "on"
            | "enable"
            | "enabled"
            | "T"
            | "YES"
            | "TRUE"
            | "ON"
            | "ENABLE"
            | "ENABLED"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_dsn() {
        let _ = "taos://".into_dsn().unwrap();
        let _ = "taos://".to_string().into_dsn().unwrap();
        let _ = (&"taos://".to_string()).into_dsn().unwrap();

        let a: Dsn = "taos://".parse().unwrap();
        let b = a.into_dsn().unwrap();
        let c = (&b).into_dsn().unwrap();

        let d: Dsn = (&c).try_into().unwrap();
        let _: Dsn = (d).try_into().unwrap();
        let _: Dsn = ("taos://").try_into().unwrap();
        let _: Dsn = ("taos://".to_string()).try_into().unwrap();
        let _: Dsn = (&"taos://:password@".parse::<Dsn>().unwrap().to_string())
            .try_into()
            .unwrap();
    }

    #[test]
    fn test_methods() {
        let mut dsn: Dsn = "taos://localhost:6030/test?debugFlag=135".parse().unwrap();

        let flag = dsn.get("debugFlag").unwrap();
        assert_eq!(flag, "135");

        dsn.set("configDir", "/tmp/taos");
        assert_eq!(dsn.get("configDir").unwrap(), "/tmp/taos");
        let config = dsn.remove("configDir").unwrap();
        assert_eq!(config, "/tmp/taos");

        let params = dsn.drain_params();
        assert!(dsn.params.is_empty());
        assert!(params.contains_key("debugFlag"));
        assert!(params.len() == 1);
    }

    #[test]
    fn username_with_password() {
        let s = "taos://";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos:///";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos://");

        let s = "taos://root@";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root:taosdata@";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn host_port_mix() {
        let s = "taos://localhost";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    port: Some(6030),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    port: Some(6030),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@localhost:1234port";
        let e = Dsn::from_str(s).expect_err("port error");
        assert_eq!(
            e.to_string(),
            "invalid addresses: localhost:1234port, error: format error, use host:port syntax"
        );

        let s = "taos://root@localhost:";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    port: None,
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn username_with_host() {
        let s = "taos://root@localhost";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    port: Some(6030),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address::new("localhost", 6030)],
                ..Default::default()
            }
        );

        let s = "taos://root:taosdata@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                addresses: vec![Address::new("localhost", 6030)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn username_with_multi_addresses() {
        let s = "taos://root@host1:6030,,,,/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                addresses: vec![Address::new("host1", 6030)],
                ..Default::default()
            }
        );

        let s = "taos://root@host1.domain:6030,host2.domain:6031";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![
                    Address::new("host1.domain", 6030),
                    Address::new("host2.domain", 6031)
                ],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root:taosdata@host1:6030,host2:6031";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn db_only() {
        let s = "taos:///db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                subject: Some("db1".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos:///db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                subject: Some("db1".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn username_with_multi_addresses_database() {
        let s = "taos://root@host1:6030,host2:6031/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root:taosdata@host1:6030,host2:6031/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                subject: Some("db1".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn protocol() {
        let s = "taos://root@tcp(host1:6030,host2:6031)/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                protocol: Some("tcp".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos+tcp://root@host1:6030,host2:6031/db1");

        let s = "taos+tcp://root@host1:6030,host2:6031/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                protocol: Some("tcp".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos+tcp://root@host1:6030,host2:6031/db1");
    }

    #[test]
    fn fragment() {
        let s = "postgresql://%2Fvar%2Flib%2Fpostgresql/dbname";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "postgresql".to_string(),
                subject: Some("dbname".to_string()),
                addresses: vec![Address {
                    path: Some("/var/lib/postgresql".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "unix:/path/to/unix.sock";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "unix".to_string(),
                path: Some("/path/to/unix.sock".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "sqlite:/c:/full/windows/path/to/file.db";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "sqlite".to_string(),
                path: Some("/c:/full/windows/path/to/file.db".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "sqlite:./file.db";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "sqlite".to_string(),
                path: Some("./file.db".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "sqlite://root:pass@//full/unix/path/to/file.db?mode=0666&readonly=true";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "sqlite".to_string(),
                username: Some("root".to_string()),
                password: Some("pass".to_string()),
                subject: Some("/full/unix/path/to/file.db".to_string()),
                params: (BTreeMap::from_iter(vec![
                    ("mode".to_string(), "0666".to_string()),
                    ("readonly".to_string(), "true".to_string())
                ])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn path_special_chars() {
        // support `Chinese characters`, `uppercase and lowercase letters`, `digits`, `spaces`, `%`, `$`, `@`, `.`, `-`, `_`, `(`, `)`, `[`, `]`, `{`, `}`, `（`, `）`, `【`, `】`, `｛`, `｝`
        let s = "csv:./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝.csv";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "csv".to_string(),
                path: Some(
                    "./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝.csv".to_string()
                ),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        // do not support other characters which are not in the list above(such as `&` .etc)
        // Edited by @zitsen on 2024-12-16: `&` is supported now.
        let s = "csv:./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝&.csv";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "csv".to_string(),
                path: Some(
                    "./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝&.csv".to_string()
                ),
                ..Default::default()
            }
        );
    }

    #[test]
    fn params() {
        let s = r#"taos://?abc=abc"#;
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                params: (BTreeMap::from_iter(vec![("abc".to_string(), "abc".to_string())])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = r#"taos://root@localhost?abc=abc"#;
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address::from_host("localhost")],
                params: (BTreeMap::from_iter(vec![("abc".to_string(), "abc".to_string())])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = r#"taos://root@localhost?a%20b"#;
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address::from_host("localhost")],
                params: (BTreeMap::from_iter(vec![("a b".to_string(), String::new())])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos://root@localhost?a b=");
    }

    #[test]
    fn parse_taos_tmq() {
        let s = "taos://root:taosdata@localhost/aa23d04011eca42cf7d8c1dd05a37985?topics=aa23d04011eca42cf7d8c1dd05a37985&group.id=tg2";
        let _ = Dsn::from_str(s).unwrap();
    }

    #[test]
    fn tmq_ws_driver() {
        let dsn = Dsn::from_str("tmq+ws:///abc1?group.id=abc3&timeout=50ms").unwrap();
        assert_eq!(dsn.driver, "tmq");
        assert_eq!(dsn.to_string(), "tmq+ws:///abc1?group.id=abc3&timeout=50ms");
    }

    #[test]
    fn tmq_offset() {
        let dsn = Dsn::from_str("tmq+ws:///abc1?offset=10:20,11:40").unwrap();
        let offset = dsn.get("offset").unwrap();
        dbg!(&dsn);
        dbg!(&offset);
    }

    #[test]
    fn password_special_chars() {
        let p = "!@#$%^&*()";
        let e = urlencoding::encode(p);
        dbg!(&e);

        let dsn = Dsn::from_str(&format!("taos://root:{e}@localhost:6030/")).unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.password.as_deref().unwrap(), p);
        assert_eq!(dsn.to_string(), format!("taos://root:{e}@localhost:6030"));
    }

    #[test]
    fn param_special_chars() {
        let p = "!@#$%^&*()";
        let e = urlencoding::encode(p);
        dbg!(&e);

        let dsn = Dsn::from_str(&format!("taos://root:{e}@localhost:6030?code1={e}")).unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.password.as_deref().unwrap(), p);
        assert_eq!(dsn.get("code1").unwrap(), p);
        assert_eq!(
            dsn.to_string(),
            format!("taos://root:{e}@localhost:6030?code1={e}")
        );
    }

    #[test]
    fn param_special_chars_all() {
        let u = "a";
        let p = "!@#$%^&*()";
        let e = urlencoding::encode(p);
        dbg!(&e);

        let dsn = Dsn::from_str(&format!("taos://{u}:{p}@localhost:6030?{e}={e}")).unwrap();
        dbg!(&dsn);
        let dsn2 = Dsn::from_str(&format!("taos://{u}:{e}@localhost:6030?{e}={e}")).unwrap();
        assert_eq!(dsn, dsn2);
        assert_eq!(dsn.password.as_deref().unwrap(), p);
        assert_eq!(dsn.get(p).unwrap(), p);
        assert_eq!(
            dsn.to_string(),
            format!("taos://{u}:{e}@localhost:6030?{e}={e}")
        );
    }

    #[test]
    fn unix_path_with_glob() {
        let dsn = Dsn::from_str("csv:./**.csv?param=1").unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");

        let dsn = Dsn::from_str("csv:./**.csv?param=1").unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");

        let dsn = Dsn::from_str("csv:.\\**.csv?param=1").unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");
    }

    #[test]
    fn unix_path_with_space() {
        let dsn = Dsn::from_str("csv:./a b.csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./a b.csv");
    }

    #[test]
    fn unix_multiple_path() {
        let dsn = Dsn::from_str("csv:./a b.csv,c d .csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./a b.csv,c d .csv");
    }

    #[test]
    fn unit_path_with_utf8() {
        let dsn = Dsn::from_str("csv:./文件 Aa1.csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./文件 Aa1.csv");
    }

    #[test]
    fn with_space() {
        let dsn = Dsn::from_str("pi:///Met1 ABC").unwrap();
        assert_eq!(dsn.subject, Some("Met1 ABC".to_string()));

        let dsn = Dsn::from_str("pi://u ser:pa ss@host:80/Met1 ABC").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.subject, Some("Met1 ABC".to_string()));
        assert_eq!(dsn.username, Some("u ser".to_string()));
        assert_eq!(dsn.password, Some("pa ss".to_string()));
    }

    #[test]
    fn test_address() {
        let addr = Address::from_str("").unwrap();
        assert!(addr.is_empty());
        assert_eq!(addr.to_string(), "");
        let addr = Address::from_str("192.168.1.32").unwrap();
        assert!(!addr.is_empty());
        assert_eq!(addr.host.expect("host set").as_str(), "192.168.1.32");

        let addr = Address::from_str("192.168.1.32:0").unwrap();
        assert!(!addr.is_empty());
        assert_eq!(addr.host.expect("host set").as_str(), "192.168.1.32");
        assert_eq!(addr.port.expect("port set"), 0);

        let addr = Address::from_str("/path/to/file%20name").unwrap();
        assert_eq!(addr.path.as_ref().expect("path set"), "/path/to/file name");
        assert!(addr.host.is_none());
        assert!(addr.port.is_none());
        assert!(!addr.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_address_panic() {
        let addr = Address {
            host: Some("localhost".to_string()),
            port: Some(6030),
            path: Some("/path/to/file".to_string()),
        };
        addr.to_string();
    }

    #[test]
    fn test_value_is_true() {
        for s in ["", "1", "true", "t", "yes", "y", "on", "enable", "enabled"] {
            assert_eq!(value_is_true(s), true);
        }
        for s in [
            "0",
            "false",
            "f",
            "no",
            "n",
            "disable",
            "disabled",
            "any-other-str",
        ] {
            assert_eq!(value_is_true(s), false);
        }
    }

    #[test]
    fn url_single_addr_special_chars() {
        let s = "taos://root:!@#$%^&*()-_+=[]{}:;><?|~,@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.password.unwrap(), "!@#$%^&*()-_+=[]{}:;><?|~,");
        let s = "taos://root:Ab1!#$%^&*()-_+=[]{}:;><?|~,@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.password.unwrap(), "Ab1!#$%^&*()-_+=[]{}:;><?|~,");
        let s = "taos://root:Ab1@#$%^&*()_+@localhost:6030/test?mode=all";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.password.unwrap(), "Ab1@#$%^&*()_+");
    }
    #[test]
    fn path_with_special_chars() {
        let s = "taos:/!@#$%^&*()-_+=[]{}:;><|~,?params=1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.path.unwrap(), "/!@#$%^&*()-_+=[]{}:;><|~,");
        assert_eq!(dsn.params.get("params").unwrap(), "1");
    }
    #[test]
    fn at_question_position_mix() {
        let s =
            "taos:/!@#$%^&*()-_+=[]{}:;><|~,?params=./@abc.txt&b=!@#$%^*()-_+=[]{}:;><中文汉字£¢§®";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.path.unwrap(), "/!@#$%^&*()-_+=[]{}:;><|~,");
        assert_eq!(dsn.params.get("params").unwrap(), "./@abc.txt");
        assert_eq!(
            dsn.params.get("b").unwrap(),
            "!@#$%^*()-_+=[]{}:;><中文汉字£¢§®"
        );
    }

    #[test]
    fn taosx_dsn_opc() {
        let dsn = format!(
            "opcua://{}?node_id_pattern={}&browse_name_pattern={}",
            "192.168.2.16:53530/OPCUA/SimulationServer", "^(?!.*_Error).+$", "^(?!.*_Error).+$"
        )
        .into_dsn()
        .unwrap();
        assert_eq!(dsn.driver, "opcua");
        assert_eq!(dsn.addresses.len(), 1);
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "192.168.2.16");
        assert_eq!(dsn.addresses[0].port.unwrap(), 53530);
        assert_eq!(dsn.subject.as_deref().unwrap(), "OPCUA/SimulationServer");
        assert_eq!(
            dsn.params.get("node_id_pattern").unwrap(),
            "^(?!.*_Error).+$"
        );
        assert_eq!(
            dsn.params.get("browse_name_pattern").unwrap(),
            "^(?!.*_Error).+$"
        );

        let dsn = "opcua://?certificate=@./tests/opc/certificate.crt"
            .into_dsn()
            .unwrap();
        assert!(dsn.subject.is_none());
        assert_eq!(
            dsn.get("certificate").unwrap(),
            "@./tests/opc/certificate.crt"
        );

        let dsn = "opcua://?certificate=@abc".into_dsn().unwrap();
        dbg!(&dsn);
        assert!(dsn.subject.is_none());
        assert!(dsn.addresses.is_empty());
        assert_eq!(dsn.get("certificate").unwrap(), "@abc");

        let dsn = "opcua://192.168.1.13:9092/abc/path?certificate=@abc"
            .into_dsn()
            .unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "192.168.1.13");
        assert_eq!(dsn.addresses[0].port.unwrap(), 9092);
        assert_eq!(dsn.subject.as_deref().unwrap(), "abc/path");
        assert_eq!(dsn.get("certificate").unwrap(), "@abc");
    }

    #[test]
    fn taosx_dsn_kafka() {
        let dsn = Dsn::from_str("kafka://localhost:9092").unwrap();
        assert_eq!(dsn.addresses.len(), 1);
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "localhost");
        let dsn = Dsn::from_str("kafka://localhost:9092,192.168.1.92:9092").unwrap();
        assert_eq!(dsn.addresses.len(), 2);
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "localhost");
        assert_eq!(dsn.addresses[1].host.as_deref().unwrap(), "192.168.1.92");
        let dsn = Dsn::from_str("kafka://:9092,:9092").unwrap();
        assert_eq!(dsn.addresses.len(), 2);
        assert_eq!(dsn.addresses[0].port.unwrap(), 9092);
        assert_eq!(dsn.addresses[1].port.unwrap(), 9092);
        let dsn = Dsn::from_str("kafka://:9092").unwrap();
        assert_eq!(dsn.addresses.len(), 1);
        assert_eq!(dsn.addresses[0].port.unwrap(), 9092);

        let dsn = Dsn::from_str("kafka://:?topics=tp1,tp2").unwrap();
        assert_eq!(dsn.get("topics").unwrap(), "tp1,tp2");
        assert!(dsn.addresses.is_empty());
        assert!(dsn.subject.is_none());
        let dsn = Dsn::from_str("kafka://").unwrap();
        assert!(dsn.addresses.is_empty());
        assert!(dsn.subject.is_none());
    }
    #[test]
    fn taosx_dsn_mqtt() {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1884").unwrap();
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "127.0.0.1");
        assert_eq!(dsn.addresses[0].port.unwrap(), 1884);

        let dsn = Dsn::from_str("mqtt://127.0.0.1:").unwrap();
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "127.0.0.1");
        assert!(dsn.addresses[0].port.is_none());

        let dsn = Dsn::from_str("mqtt://:1883").unwrap();
        assert!(dsn.addresses[0].host.is_none());
        assert_eq!(dsn.addresses[0].port.unwrap(), 1883);

        let dsn = Dsn::from_str("mqtt://:").unwrap();
        assert!(dsn.addresses.is_empty());
    }

    #[test]
    fn taosx_csv_dsn() {
        let dsn = r#"csv:/data/test-csv?skip=0&has_header=true&new_file_notify=false&file_pattern=^\\?\\-\\*\\-\\[\\-\\]\\-[ab]\\-[^ef]\\-.\\-.*\\.csv$"#;
        let dsn = Dsn::from_str(dsn).unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.driver, "csv");
        assert_eq!(dsn.path.as_deref().unwrap(), "/data/test-csv");
        assert_eq!(dsn.get("skip").unwrap(), "0");
        assert_eq!(dsn.get("has_header").unwrap(), "true");
        assert_eq!(dsn.get("new_file_notify").unwrap(), "false");
        assert_eq!(
            dsn.get("file_pattern").unwrap(),
            r#"^\\?\\-\\*\\-\\[\\-\\]\\-[ab]\\-[^ef]\\-.\\-.*\\.csv$"#,
        );
    }

    #[test]
    fn test_ipv6_single_addr() {
        {
            let dsn = Dsn::from_str("ws://[::1]:6041").unwrap();
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);

            let dsn = Dsn::from_str("ws://[::1]").unwrap();
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert!(dsn.addresses[0].port.is_none());

            let dsn = Dsn::from_str("ws://[::1]:").unwrap();
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert!(dsn.addresses[0].port.is_none());
        }

        {
            let dsn =
                Dsn::from_str("ws://[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]:16041").unwrap();
            assert_eq!(
                dsn.addresses[0].host.as_deref().unwrap(),
                "[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]"
            );
            assert_eq!(dsn.addresses[0].port.unwrap(), 16041);

            let dsn = Dsn::from_str("ws://[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]").unwrap();
            assert_eq!(
                dsn.addresses[0].host.as_deref().unwrap(),
                "[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]"
            );
            assert!(dsn.addresses[0].port.is_none());

            let dsn = Dsn::from_str("ws://[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]:").unwrap();
            assert_eq!(
                dsn.addresses[0].host.as_deref().unwrap(),
                "[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]"
            );
            assert!(dsn.addresses[0].port.is_none());
        }

        {
            let dsn = Dsn::from_str("ws://[2002:9ba:b4e:6:be24:11ff:fe9c:3dd]:6041").unwrap();
            assert_eq!(
                dsn.addresses[0].host.as_deref().unwrap(),
                "[2002:9ba:b4e:6:be24:11ff:fe9c:3dd]"
            );
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);

            let dsn = Dsn::from_str("ws://[2001:db8::1]:6041").unwrap();
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[2001:db8::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
        }
    }

    #[test]
    fn test_ipv6_multi_addrs() {
        let dsn = Dsn::from_str(
            "ws://[::1]:6041,[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]:16041,[2001:db8::1],[2001:db8::1]:",
        )
        .unwrap();

        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
        assert_eq!(dsn.addresses[0].port.unwrap(), 6041);

        assert_eq!(
            dsn.addresses[1].host.as_deref().unwrap(),
            "[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]"
        );
        assert_eq!(dsn.addresses[1].port.unwrap(), 16041);

        assert_eq!(dsn.addresses[2].host.as_deref().unwrap(), "[2001:db8::1]");
        assert!(dsn.addresses[2].port.is_none());

        assert_eq!(dsn.addresses[3].host.as_deref().unwrap(), "[2001:db8::1]");
        assert!(dsn.addresses[3].port.is_none());
    }

    #[test]
    fn test_ipv6() {
        {
            let dsn = Dsn::from_str("tmq+ws://root:taosdata@[::1]:6041,[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]:6042,[::1]:/db?a=a&b=b&c=c").unwrap();
            assert_eq!(dsn.driver, "tmq");
            assert_eq!(dsn.protocol.as_deref().unwrap(), "ws");
            assert_eq!(dsn.username.as_deref().unwrap(), "root");
            assert_eq!(dsn.password.as_deref().unwrap(), "taosdata");

            assert_eq!(dsn.addresses.len(), 3);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(
                dsn.addresses[1].host.as_deref().unwrap(),
                "[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]"
            );
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);
            assert_eq!(dsn.addresses[2].host.as_deref().unwrap(), "[::1]");
            assert!(dsn.addresses[2].port.is_none());

            assert_eq!(dsn.subject.as_deref().unwrap(), "db");

            assert_eq!(dsn.get("a").unwrap(), "a");
            assert_eq!(dsn.get("b").unwrap(), "b");
            assert_eq!(dsn.get("c").unwrap(), "c");
        }

        {
            let dsn = Dsn::from_str("tmq+ws://root:taosdata@[::1]:6041,[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]:6042,[::1]:/db?a=a&b=b&c=c").unwrap();
            assert_eq!(dsn.driver, "tmq");
            assert_eq!(dsn.protocol.as_deref().unwrap(), "ws");
            assert_eq!(dsn.username.as_deref().unwrap(), "root");
            assert_eq!(dsn.password.as_deref().unwrap(), "taosdata");

            assert_eq!(dsn.addresses.len(), 3);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(
                dsn.addresses[1].host.as_deref().unwrap(),
                "[2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd]"
            );
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);
            assert_eq!(dsn.addresses[2].host.as_deref().unwrap(), "[::1]");
            assert!(dsn.addresses[2].port.is_none());

            assert_eq!(dsn.subject.as_deref().unwrap(), "db");

            assert_eq!(dsn.get("a").unwrap(), "a");
            assert_eq!(dsn.get("b").unwrap(), "b");
            assert_eq!(dsn.get("c").unwrap(), "c");
        }

        {
            let dsn =
                Dsn::from_str("ws://root:taosdata@[::1]:6041,[::1]:6042/?key1=value1&key2=value2")
                    .unwrap();
            assert_eq!(dsn.driver, "ws");
            assert_eq!(dsn.username.as_deref().unwrap(), "root");
            assert_eq!(dsn.password.as_deref().unwrap(), "taosdata");

            assert_eq!(dsn.addresses.len(), 2);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(dsn.addresses[1].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);

            assert_eq!(dsn.get("key1").unwrap(), "value1");
            assert_eq!(dsn.get("key2").unwrap(), "value2");
        }

        {
            let dsn =
                Dsn::from_str("ws://[::1]:6041,[::1]:6042/db_1748596516?key1=value1&key2=value2")
                    .unwrap();
            assert_eq!(dsn.driver, "ws");

            assert_eq!(dsn.addresses.len(), 2);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(dsn.addresses[1].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);

            assert_eq!(dsn.subject.as_deref().unwrap(), "db_1748596516");

            assert_eq!(dsn.get("key1").unwrap(), "value1");
            assert_eq!(dsn.get("key2").unwrap(), "value2");
        }

        {
            let dsn = Dsn::from_str("ws://[::1]:6041,[::1]:6042/db_1748596516").unwrap();
            assert_eq!(dsn.driver, "ws");

            assert_eq!(dsn.addresses.len(), 2);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(dsn.addresses[1].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);

            assert_eq!(dsn.subject.as_deref().unwrap(), "db_1748596516");
        }

        {
            let dsn = Dsn::from_str("ws://[::1]:6041,[::1]:6042?key1=value1&key2=value2").unwrap();
            assert_eq!(dsn.driver, "ws");

            assert_eq!(dsn.addresses.len(), 2);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(dsn.addresses[1].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);

            assert_eq!(dsn.get("key1").unwrap(), "value1");
            assert_eq!(dsn.get("key2").unwrap(), "value2");
        }

        {
            let dsn = Dsn::from_str("ws://root:taosdata@[::1]:6041,[::1]:6042/?").unwrap();
            assert_eq!(dsn.driver, "ws");
            assert_eq!(dsn.username.as_deref().unwrap(), "root");
            assert_eq!(dsn.password.as_deref().unwrap(), "taosdata");

            assert_eq!(dsn.addresses.len(), 2);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(dsn.addresses[1].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);
        }

        {
            let dsn = Dsn::from_str("ws://@[::1]:6041,[::1]:6042/").unwrap();
            assert_eq!(dsn.driver, "ws");
            assert_eq!(dsn.addresses.len(), 2);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
            assert_eq!(dsn.addresses[1].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[1].port.unwrap(), 6042);
        }

        {
            let dsn = Dsn::from_str("ws://@[::1]:6041").unwrap();
            assert_eq!(dsn.driver, "ws");
            assert_eq!(dsn.addresses.len(), 1);
            assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
            assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
        }

        {
            let dsn = Dsn::from_str("ws:///db_1748596516").unwrap();
            assert_eq!(dsn.driver, "ws");
            assert_eq!(dsn.subject.as_deref().unwrap(), "db_1748596516");
        }
    }

    #[test]
    fn test_ipv6_err() {
        let res = Dsn::from_str("ws://[::ffff:192.0.2.128]:6041");
        assert!(res.is_err(), "incorrect host:port format");

        let res = Dsn::from_str("ws://2001:db8::1:6041");
        assert!(res.is_err(), "incorrect host:port format");

        let res = Dsn::from_str("ws://[2001:db8::1:6041");
        assert!(res.is_err(), "incorrect host:port format");

        let res = Dsn::from_str("ws://2001:db8::1]:6041");
        assert!(res.is_err(), "incorrect host:port format");

        let res = Dsn::from_str("ws://[ghij:klmn::1]:6041");
        assert!(res.is_err(), "incorrect host:port format");

        let res = Dsn::from_str("ws://:::6041");
        assert!(res.is_err(), "incorrect host:port format");
    }

    #[test]
    fn test_tls() {
        const TEST_CERT: &str = r"-----BEGIN CERTIFICATE-----
MIIDvTCCAqWgAwIBAgIUfQjwmIKn4dhLRrmlfu7lmVTVUQkwDQYJKoZIhvcNAQEL
BQAwbjELMAkGA1UEBhMCQ04xEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwI
WW91ckNpdHkxEDAOBgNVBAoMB1lvdXJPcmcxETAPBgNVBAsMCFlvdXJVbml0MRMw
EQYDVQQDDApZb3VyUm9vdENBMB4XDTI1MTExNDA4MDk1N1oXDTM1MTExMjA4MDk1
N1owbjELMAkGA1UEBhMCQ04xEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwI
WW91ckNpdHkxEDAOBgNVBAoMB1lvdXJPcmcxETAPBgNVBAsMCFlvdXJVbml0MRMw
EQYDVQQDDApZb3VyUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEA1LFgg8JWCzFPlYWcMl7J3HsX1uzoURCFG4//KOpAgTb4zTw5+FmOV2fk8uj9
quD91IFZz0hGgSW3FstEdfsQh/4v2IHMvMjL6+TFu5E46jg5SlIMSaqaW1/cWKyB
vyWe7pyHJtQxo2btFioZFqYBw2Xv4q9fXBbMSI0iD/UG7ssgi1xP7e0AAXqOK1BV
QmEA7kaxvKY1ZXYVHrGZM7+f1PFzHkClU8CnpdRY2vMTmYoc2deNnHUWaa1tEWvC
8cyLLcXdElM0hn3ZI7nrb3/SCiBOHvXZo8hlundKVVigChpGhiG21WRrXDohhKnu
cjUoa5bAdEEfsNJc7fnHfRG0WQIDAQABo1MwUTAdBgNVHQ4EFgQUJIS7MbM5ZV+U
IxW9a7hjSsb6O3IwHwYDVR0jBBgwFoAUJIS7MbM5ZV+UIxW9a7hjSsb6O3IwDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAjs2RZ898QnJgwVYRFO/t
pimfluY6xIMs7NfePQyuJdrVgJ6mEUKjrrGTVAUrTtDBKm5AvImhywX7CV4s9c/L
YBDdS6+aAmPJW1TQsRpQxSoFUxd/bzbiisaUmXcFDsqrM9K2YTj1KjgpLG0H7ENJ
5oIMZzoyF9DkYqVaDzaotpJJqTcqAd8pYxT9TzKLLCjWWgVjfRfthIZaNZkfC+Hr
bdPwUC/7TgGTiEVXLm/jAGKhN0kRIrbIzro0MZqJuDW5tmr570lcE4PMUq2/4g2u
j/p1+4zmwB7F4u64uwBzwcZN5qCcAkfYYxjPbGARED4pA8YVpMx2DqmeYdR5pTj8
8Q==
-----END CERTIFICATE-----";

        let dsn = Dsn::from_str(
            &format!("wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3,TLSv1.2&tls_ca={TEST_CERT}"),
        )
        .unwrap();
        assert_eq!(dsn.driver, "wss");
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "localhost");
        assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
        assert_eq!(dsn.get("tls_mode").unwrap(), "verify_ca");
        assert_eq!(dsn.get("tls_version").unwrap(), "TLSv1.3,TLSv1.2");
        assert_eq!(dsn.get("tls_ca").unwrap(), TEST_CERT);

        let dsn = Dsn::from_str(&format!(
            "wss://127.0.0.1:6041?tls_ca={TEST_CERT}&tls_mode=verify_identity&tls_version=tlsv1.3"
        ))
        .unwrap();
        assert_eq!(dsn.driver, "wss");
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "127.0.0.1");
        assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
        assert_eq!(dsn.get("tls_mode").unwrap(), "verify_identity");
        assert_eq!(dsn.get("tls_version").unwrap(), "tlsv1.3");
        assert_eq!(dsn.get("tls_ca").unwrap(), TEST_CERT);

        let dsn = Dsn::from_str(&format!(
            "wss://[::1]:6041?tls_mode=verify_ca&tls_ca={TEST_CERT}&tls_version=tlsv1.3,tlsv1.2"
        ))
        .unwrap();
        assert_eq!(dsn.driver, "wss");
        assert_eq!(dsn.addresses[0].host.as_deref().unwrap(), "[::1]");
        assert_eq!(dsn.addresses[0].port.unwrap(), 6041);
        assert_eq!(dsn.get("tls_mode").unwrap(), "verify_ca");
        assert_eq!(dsn.get("tls_version").unwrap(), "tlsv1.3,tlsv1.2");
        assert_eq!(dsn.get("tls_ca").unwrap(), TEST_CERT);
    }

    #[test]
    fn test_redacted_display() {
        let dsn = Dsn::from_str("ws://root:secret@localhost:6041?param1=value1").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "ws://root:[REDACTED]@localhost:6041?param1=value1");

        let dsn = Dsn::from_str("ws://root@localhost:6041?totp_code=123456").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "ws://root@localhost:6041?totp_code=[REDACTED]");

        let dsn = Dsn::from_str("ws://root:secret@localhost:6041?totpCode=123456").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(
            dsn_str,
            "ws://root:[REDACTED]@localhost:6041?totpCode=[REDACTED]"
        );

        let dsn = Dsn::from_str("ws://:secret@localhost:6041?bearer_token=my_token").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(
            dsn_str,
            "ws://:[REDACTED]@localhost:6041?bearer_token=[REDACTED]"
        );

        let dsn = Dsn::from_str("ws://root:secret@localhost:6041?bearerToken=my_token").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(
            dsn_str,
            "ws://root:[REDACTED]@localhost:6041?bearerToken=[REDACTED]"
        );

        let dsn = Dsn::from_str("ws://localhost:6041?token=my_cloud_token").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "ws://localhost:6041?token=[REDACTED]");

        let dsn = Dsn::from_str("ws://localhost:6041?td.connect.token=my_token").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "ws://localhost:6041?td.connect.token=[REDACTED]");
    }

    #[test]
    fn test_redacted_dsn() {
        let dsn = Dsn::from_str("taos+ws://root:secret@localhost:6041/db").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "taos+ws://root:[REDACTED]@localhost:6041/db");

        let dsn = Dsn::from_str("file:/path/to/file?param=value").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "file:/path/to/file?param=value");

        let dsn = Dsn::from_str("ws://:secret@localhost:6041").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "ws://:[REDACTED]@localhost:6041");

        let dsn = Dsn::from_str("taos://root:secret@localhost:6030/test_db").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "taos://root:[REDACTED]@localhost:6030/test_db");

        let dsn = Dsn::from_str("taos:/path/to/file").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "taos:/path/to/file");

        let dsn = Dsn::from_str("file:/path/to/file?token=secret").unwrap();
        let dsn_str = format!("{}", dsn.redacted());
        assert_eq!(dsn_str, "file:/path/to/file?token=[REDACTED]");
    }

    #[test]
    fn test_redact_dsn() {
        assert_eq!(
            redact_dsn("ws://root:secret@localhost:6041/db"),
            "ws://root:[REDACTED]@localhost:6041/db",
        );
        assert_eq!(redact_dsn("invalid_dsn"), "<invalid dsn>");
    }
}
