use std::fmt::Display;
use std::str::FromStr;

use chrono::NaiveDateTime;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::common::Precision;

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseProperties {
    pub vgroups: Option<u64>,
    pub replica: Option<u16>,
    pub quorum: Option<u16>,
    pub days: Option<u16>,
    pub keep: Option<String>,
    #[serde(rename = "cache(MB)")]
    pub cache: Option<u32>,
    pub blocks: Option<u32>,
    pub minrows: Option<u32>,
    pub maxrows: Option<u32>,
    #[serde(rename = "wallevel")]
    pub wal: Option<u8>,
    pub fsync: Option<u32>,
    pub comp: Option<u8>,
    pub cachelast: Option<u8>,
    pub precision: Option<Precision>,
    pub update: Option<u8>,
}

macro_rules! _prop_builder {
    ($($f:ident)*, $ty:ty) => {
        $(pub fn $f(mut self, $f: $ty) -> Self {
            self.$f = Some($f);
            self
        })*
    };
}
impl DatabaseProperties {
    pub fn new() -> Self {
        Self::default()
    }

    _prop_builder!(vgroups, u64);
    _prop_builder!(cache blocks minrows maxrows fsync, u32);
    _prop_builder!(replica quorum days, u16);
    _prop_builder!(wal comp cachelast update, u8);
    _prop_builder!(precision, Precision);
    _prop_builder!(keep, String);
}

impl Display for DatabaseProperties {
    #[inline]
    #[allow(unused_assignments)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut has_wrote = false;

        macro_rules! _write_if {
            ($($f:ident) *) => {
                $(if let Some($f) = &self.$f {
                    if has_wrote {
                        write!(f, " {} {}", paste!(stringify!([<$f:upper>])), $f)?;
                    } else {
                        write!(f, "{} {}", paste!(stringify!([<$f:upper>])), $f)?;
                        has_wrote = true;
                    }
                })*
            };
            ('str $($s:ident) *) => {
                $(if let Some($s) = &self.$s {
                    if has_wrote {
                        write!(f, " {} '{}'", paste!(stringify!([<$s:upper>])), $s)?;
                    } else {
                        write!(f, "{} '{}'", paste!(stringify!([<$s:upper>])), $s)?;
                        has_wrote = true;
                    }
                })*
            };
            ($($f:ident) *; 'str $($s:ident) *) => {
                _write_if!($($f) *);
                _write_if!('str $($s) *)
            };
            ($($f:ident) *; 'str $($s:ident) *; $($f2:ident) *) => {
                _write_if!($($f) *; 'str $($s) *);
                _write_if!($($f2) *)
            };
        }

        // todo: keep now may fail
        _write_if!(vgroups replica quorum days keep cache blocks minrows
                   maxrows wal fsync comp cachelast; 'str precision; update);
        Ok(())
    }
}

impl FromStr for DatabaseProperties {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // CREATE DATABASE log REPLICA 1 QUORUM 1 DAYS 10 KEEP 30 CACHE 1 BLOCKS 3 MINROWS 100 MAXROWS 4096 WAL 1 FSYNC 3000 COMP 2 CACHELAST 0 PRECISION 'us' UPDATE 0
        use nom::branch::alt;
        use nom::bytes::complete::tag;
        use nom::character::complete::*;
        use nom::character::streaming;
        use nom::multi::many0;
        use nom::sequence::*;
        use nom::IResult;

        let mut repr = Self::new();

        fn parse_name(s: &str) -> IResult<&str, &str> {
            preceded(
                tuple((multispace0, tag("CREATE DATABASE"), multispace1)),
                alphanumeric1,
            )(s)
        }

        let s = parse_name(s).map(|s| s.0).unwrap_or(s);

        fn parse_props(s: &str) -> IResult<&str, Vec<(&str, &str)>> {
            many0(separated_pair(
                preceded(multispace0, alphanumeric1),
                streaming::char(' '),
                alt((
                    alphanumeric1,
                    delimited(char('\''), alphanumeric1, char('\'')),
                )),
            ))(s)
        }

        if let Ok((_s, props)) = parse_props(s) {
            for (prop, value) in props {
                macro_rules! _parse {
                    ($($($f:ident) +, $t:ident);*) => {
                        paste::paste! {
                            match prop.to_lowercase() {
                                $($(s if s == stringify!($f) => {
                                    repr = repr.$f($t::from_str(value)?);
                                },)*)*
                                _ => (),
                            }
                        }
                    }
                }
                _parse!(vgroups, u64;
                        cache blocks minrows maxrows fsync, u32;
                        replica quorum days, u16;
                        wal comp cachelast  update, u8;
                        keep, String;
                        precision, Precision);
            }
        }

        Ok(repr)
    }
}

#[test]
fn db_prop_from_str() {
    let s = "REPLICA 1 QUORUM 1 DAYS 10 KEEP 30 CACHE 1 BLOCKS 3 MINROWS 100 MAXROWS 4096 WAL 1 FSYNC 3000 COMP 2 CACHELAST 0 PRECISION 'us' UPDATE 0";

    let db = DatabaseProperties::from_str(s).unwrap();

    let t = db.to_string();

    dbg!(db);

    assert_eq!(s, t);
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseRepr {
    pub name: String,
    #[serde(flatten)]
    pub props: DatabaseProperties,
}

/// A show database representation struct.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShowDatabase {
    pub name: String,
    pub created_time: Option<NaiveDateTime>,
    pub ntables: Option<usize>,
    #[serde(flatten)]
    pub props: DatabaseProperties,
    pub status: Option<String>,
}

unsafe impl Send for ShowDatabase {}
