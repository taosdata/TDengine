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
//!
//! # fn main() -> Result<(), mdsn::DsnError> {
//! // The two styles are equivalent.
//! let dsn = Dsn::parse("taos://root:taosdata@host1:6030,host2:6030/db")?;
//! let dsn: Dsn = "taos://root:taosdata@host1:6030,host2:6030/db".parse()?;
//!
//! assert_eq!(dsn.driver, "taos");
//! assert_eq!(dsn.username.unwrap(), "root");
//! assert_eq!(dsn.password.unwrap(), "taosdata");
//! assert_eq!(dsn.database.unwrap(), "db");
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
use std::collections::BTreeMap;
use std::fmt::Display;
use std::num::ParseIntError;
use std::str::FromStr;

use itertools::Itertools;
use pest;
use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "dsn.pest"]
struct DsnParser;

/// Error caused by [pest] DSN parser.
#[derive(Debug, Error)]
pub enum DsnError {
    #[error("{0}")]
    ParseErr(#[from] pest::error::Error<Rule>),
    #[error("unable to parse port from {0}")]
    PortErr(#[from] ParseIntError),
    #[error("invalid driver {0}")]
    InvalidDriver(String),
    #[error("invalid protocol {0}")]
    InvalidProtocol(String),
    #[error("invalid connection {0}")]
    InvalidConnection(String),
    #[error("invalid addresses {0:?}")]
    InvalidAddresses(Vec<Address>),
    #[error("requires database: {0}")]
    RequireDatabase(String),
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
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: Some(host.into()),
            port: Some(port),
            ..Default::default()
        }
    }
    /// Construct server address with host or ip address only.
    #[inline]
    pub fn from_host(host: impl Into<String>) -> Self {
        Self {
            host: Some(host.into()),
            ..Default::default()
        }
    }

    /// Construct server address with unix socket path.
    #[inline]
    pub fn from_path(path: impl Into<String>) -> Self {
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
fn addr_parse() {
    let s = "taosdata:6030";
    let addr = Address::from_str(s).unwrap();
    assert_eq!(addr.to_string(), s);

    let s = "/var/lib/taos";
    let addr = Address::from_str(&urlencoding::encode(s)).unwrap();
    assert_eq!(addr.path.as_ref().unwrap(), s);
    assert_eq!(addr.to_string(), urlencoding::encode(s));
}

/// A DSN(**Data Source Name**) parser.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Dsn {
    pub driver: String,
    pub protocol: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub addresses: Vec<Address>,
    pub fragment: Option<String>,
    pub database: Option<String>,
    pub params: BTreeMap<String, String>,
}

pub trait IntoDsn {
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
        write!(f, "://")?;
        match (&self.username, &self.password) {
            (Some(username), Some(password)) => write!(f, "{username}:{password}@")?,
            (Some(username), None) => write!(f, "{username}@")?,
            (None, Some(password)) => write!(f, ":{password}@")?,
            (None, None) => {}
        }
        if !self.addresses.is_empty() {
            write!(
                f,
                "{}",
                self.addresses.iter().map(ToString::to_string).join(",")
            )?;
        }
        if let Some(database) = &self.database {
            write!(f, "/{database}")?;
        }
        if let Some(fragment) = &self.fragment {
            write!(f, "{fragment}")?;
        }
        if !self.params.is_empty() {
            write!(
                f,
                "?{}",
                self.params
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .join("&")
            )?;
        }
        Ok(())
    }
}

impl Dsn {
    /// Parse from a DSN string.
    #[inline]
    pub fn parse(dsn: impl AsRef<str>) -> Result<Self, DsnError> {
        dsn.as_ref().parse()
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
        let dsn = DsnParser::parse(Rule::dsn, s)?.next().unwrap();

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
                    to.database = Some(pair.as_str().to_string());
                }
                Rule::fragment => {
                    to.fragment = Some(pair.as_str().to_string());
                }
                Rule::param => {
                    let (mut name, mut value) = ("".to_string(), "".to_string());
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
}

#[test]
fn username_with_password() {
    let s = "taos://";

    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = "taos:///";

    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), "taos://");

    let s = "taos://root@";

    let dsn = Dsn::from_str(&s).unwrap();
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

    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
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
}
#[test]
fn username_with_host() {
    let s = "taos://root@localhost";
    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
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
    let s = "taos://root@host1.domain:6030,host2.domain:6031";
    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
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
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            database: Some("db1".to_string()),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = "taos:///db1";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            database: Some("db1".to_string()),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);
}

#[test]
fn username_with_multi_addresses_database() {
    let s = "taos://root@host1:6030,host2:6031/db1";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            username: Some("root".to_string()),
            database: Some("db1".to_string()),
            addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = "taos://root:taosdata@host1:6030,host2:6031/db1";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            username: Some("root".to_string()),
            password: Some("taosdata".to_string()),
            database: Some("db1".to_string()),
            addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);
}

#[test]
fn protocol() {
    let s = "taos://root@tcp(host1:6030,host2:6031)/db1";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            username: Some("root".to_string()),
            database: Some("db1".to_string()),
            protocol: Some("tcp".to_string()),
            addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), "taos+tcp://root@host1:6030,host2:6031/db1");

    let s = "taos+tcp://root@host1:6030,host2:6031/db1";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            username: Some("root".to_string()),
            database: Some("db1".to_string()),
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
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "postgresql".to_string(),
            database: Some("dbname".to_string()),
            addresses: vec![Address {
                path: Some("/var/lib/postgresql".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = "unix:///path/to/unix.sock";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "unix".to_string(),
            fragment: Some("/path/to/unix.sock".to_string()),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = "sqlite:///c:/full/windows/path/to/file.db";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "sqlite".to_string(),
            fragment: Some("/c:/full/windows/path/to/file.db".to_string()),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = "sqlite://./file.db";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "sqlite".to_string(),
            fragment: Some("./file.db".to_string()),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = "sqlite://root:pass@/full/unix/path/to/file.db?mode=0666&readonly=true";
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "sqlite".to_string(),
            username: Some("root".to_string()),
            password: Some("pass".to_string()),
            fragment: Some("/full/unix/path/to/file.db".to_string()),
            params: BTreeMap::from_iter(vec![
                ("mode".to_string(), "0666".to_string()),
                ("readonly".to_string(), "true".to_string())
            ]),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);
}

#[test]
fn params() {
    let s = r#"taos://?abc=abc"#;
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            params: BTreeMap::from_iter(vec![("abc".to_string(), "abc".to_string())]),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);

    let s = r#"taos://root@localhost?abc=abc"#;
    let dsn = Dsn::from_str(&s).unwrap();
    assert_eq!(
        dsn,
        Dsn {
            driver: "taos".to_string(),
            username: Some("root".to_string()),
            addresses: vec![Address::from_host("localhost")],
            params: BTreeMap::from_iter(vec![("abc".to_string(), "abc".to_string())]),
            ..Default::default()
        }
    );
    assert_eq!(dsn.to_string(), s);
}

#[test]
fn parse_taos_tmq() {
    let s = "taos://root:taosdata@localhost/aa23d04011eca42cf7d8c1dd05a37985?topics=aa23d04011eca42cf7d8c1dd05a37985&group.id=tg2";
    let _ = Dsn::from_str(&s).unwrap();
}
