use std::{io::Write, ops::Deref, str::FromStr, sync::Once};

use log::Level;
use pretty_env_logger::env_logger::fmt::{Color, StyledValue};

use crate::{helpers::Precision, Result, Taos, TaosOptions};

/// Used in [test(naming = "uuid-v1")] macro to choose database naming strategy
#[derive(Debug)]
pub enum NamingStrategy {
    Random,
    UuidV1,
    Named(String),
}

impl NamingStrategy {
    fn name(&self) -> String {
        use NamingStrategy::*;
        match self {
            Random => {
                // randomly generate
                use faker_rand::lorem::Word;
                use rand::rngs::ThreadRng;
                use rand::Rng;
                static mut RNG: Option<ThreadRng> = None;
                const ONCE: Once = Once::new();
                ONCE.call_once(|| unsafe {
                    RNG = Some(rand::thread_rng());
                });
                let rng = unsafe { RNG.as_mut().unwrap() };
                String::from_iter([rng.gen::<Word>().to_string(), rng.gen::<Word>().to_string()])
            }
            UuidV1 => {
                // time-based uuid generator
                use uuid::v1::{Context, Timestamp};
                use uuid::Uuid;

                use rand::rngs::ThreadRng;
                use rand::Rng;
                static mut RNG: Option<ThreadRng> = None;
                const ONCE: Once = Once::new();
                ONCE.call_once(|| unsafe {
                    RNG = Some(rand::thread_rng());
                });
                let context = Context::new(unsafe { RNG.as_mut().unwrap().gen() });
                let dur = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .expect("");
                let ts = Timestamp::from_unix(&context, dur.as_secs(), dur.subsec_nanos());
                let node: Vec<u8> = (0..6)
                    .map(|_| unsafe { RNG.as_mut().unwrap() }.gen())
                    .collect();
                let mut uuid: Vec<_> = Uuid::new_v1(ts, &node)
                    .expect("failed to generate UUID")
                    .to_hyphenated()
                    .to_string()
                    .replace("-", "")
                    .chars()
                    .collect();
                for _ in 0..uuid.len() {
                    log::error!("uuid: {}", String::from_iter(uuid.clone()));
                    if uuid[0].is_alphabetic() {
                        break;
                    } else {
                        uuid.rotate_left(1);
                    }
                }
                if uuid[0].is_alphabetic() {
                    String::from_iter(uuid)
                } else {
                    self.name()
                }
            }
            Named(name) => name.clone(),
            _ => unimplemented!(),
        }
    }
}

impl Iterator for NamingStrategy {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.name())
    }
}

impl Default for NamingStrategy {
    fn default() -> Self {
        NamingStrategy::UuidV1
    }
}

impl From<()> for NamingStrategy {
    fn from(_: ()) -> Self {
        NamingStrategy::default()
    }
}

impl From<&str> for NamingStrategy {
    fn from(s: &str) -> Self {
        match s {
            "random" => NamingStrategy::Random,
            "uuid-v1" | "uuidv1" | "UuidV1" => NamingStrategy::UuidV1,
            _ => NamingStrategy::Named(s.to_string()),
        }
    }
}
impl From<String> for NamingStrategy {
    fn from(s: String) -> Self {
        match s.as_str() {
            "random" => NamingStrategy::Random,
            "uuid-v1" | "uuidv1" | "UuidV1" => NamingStrategy::UuidV1,
            _ => NamingStrategy::Named(s),
        }
    }
}

#[derive(Debug)]
/// Used in [test(drop = "<strategy>")] macro to make sure to drop before/after test.
pub enum DroppingStrategy {
    None,
    Before,
    After,
    Always,
}

impl DroppingStrategy {
    fn drop_after(&self) -> bool {
        use DroppingStrategy::*;
        matches!(self, After | Always)
    }
    fn drop_before(&self) -> bool {
        use DroppingStrategy::*;
        matches!(self, Before | Always)
    }
}

impl From<&str> for DroppingStrategy {
    fn from(s: &str) -> Self {
        match s.as_ref() {
            "none" => DroppingStrategy::None,
            "before" => DroppingStrategy::Before,
            "after" => DroppingStrategy::After,
            "always" => DroppingStrategy::Always,
            _ => unreachable!("invalid drop strategy used in [test] macro"),
        }
    }
}

impl From<String> for DroppingStrategy {
    fn from(s: String) -> Self {
        Self::from(s.as_str())
    }
}

impl From<()> for DroppingStrategy {
    fn from(_: ()) -> Self {
        DroppingStrategy::default()
    }
}

impl Default for DroppingStrategy {
    fn default() -> Self {
        DroppingStrategy::Always
    }
}

#[derive(Debug)]
pub enum PrecisionStrategy {
    Preset(Precision),
    Random,
    Cyclic,
}

impl PrecisionStrategy {
    fn precision(&self) -> Precision {
        use PrecisionStrategy::*;
        match self {
            Preset(p) => p.clone(),
            Random => {
                //
                use rand::rngs::ThreadRng;
                use rand::Rng;
                static mut RNG: Option<ThreadRng> = None;
                const ONCE: Once = Once::new();
                ONCE.call_once(|| unsafe {
                    RNG = Some(rand::thread_rng());
                });
                let rng = unsafe { RNG.as_mut().unwrap() };
                rng.gen::<u8>().into()
            }
            Cyclic => {
                static mut ITER: u64 = 0;
                let p = match unsafe { ITER % 3 } {
                    0 => Precision::Milliseconds,
                    1 => Precision::Microseconds,
                    2 => Precision::Nanoseconds,
                    _ => unreachable!(),
                };
                unsafe { ITER += 1 };
                p
            }
        }
    }
}

impl Iterator for PrecisionStrategy {
    type Item = Precision;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.precision())
    }
}

impl From<()> for PrecisionStrategy {
    fn from(_: ()) -> Self {
        PrecisionStrategy::default()
    }
}

impl Default for PrecisionStrategy {
    fn default() -> Self {
        PrecisionStrategy::Preset(Precision::Milliseconds)
    }
}

impl From<&str> for PrecisionStrategy {
    fn from(s: &str) -> Self {
        PrecisionStrategy::from_str(s).expect(&format!("invalid precision strategy: {}", s))
    }
}

impl FromStr for PrecisionStrategy {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self> {
        use PrecisionStrategy::*;
        match Precision::from_str(s) {
            Ok(p) => Ok(Preset(p)),
            Err(_) => match s {
                "random" => Ok(Random),
                "cyclic" => Ok(Cyclic),
                _ => Err(err!('str "unsupported strategy {}")),
            },
        }
    }
}
#[derive(Debug, Default)]

pub struct Builder {
    naming: NamingStrategy,
    precision: PrecisionStrategy,
    dropping: DroppingStrategy,
    databases: usize,
}

impl Builder {
    pub fn new(
        naming: impl Into<NamingStrategy>,
        drop: impl Into<DroppingStrategy>,
        precision: impl Into<PrecisionStrategy>,
        databases: usize,
    ) -> Self {
        Self {
            naming: naming.into(),
            precision: precision.into(),
            dropping: drop.into(),
            databases,
        }
    }

    pub fn naming(mut self, naming: impl Into<NamingStrategy>) -> Self {
        self.naming = naming.into();
        self
    }
    pub fn dropping(mut self, drop: impl Into<DroppingStrategy>) -> Self {
        self.dropping = drop.into();
        self
    }
    pub fn precision(mut self, precision: impl Into<PrecisionStrategy>) -> Self {
        self.precision = precision.into();
        self
    }
    pub fn databases(mut self, databases: usize) -> Self {
        self.databases = databases.into();
        self
    }

    pub fn build(self) -> Result<TaosWrapper> {
        static LOGGER_INIT: Once = Once::new();
        LOGGER_INIT.call_once(|| {
            pretty_env_logger::formatted_timed_builder()
                .format_module_path(true)
                .filter_level(log::LevelFilter::Trace)
                .format(|buf, record| -> std::result::Result<(), std::io::Error> {
                    fn colored_level<'a>(
                        style: &'a mut pretty_env_logger::env_logger::fmt::Style,
                        level: Level,
                    ) -> StyledValue<'a, &'static str> {
                        match level {
                            Level::Trace => style.set_color(Color::Cyan).value("TRACE"),
                            Level::Debug => style.set_color(Color::Blue).value("DEBUG"),
                            Level::Info => style.set_color(Color::Green).value("INFO"),
                            Level::Warn => style.set_color(Color::Yellow).value("WARN "),
                            Level::Error => style.set_color(Color::Red).value("ERROR"),
                        }
                    }
                    let mut style = buf.style();
                    writeln!(
                        buf,
                        "[{}:{}] {} {} - {}",
                        record.file().unwrap_or("unknown"),
                        record.line().unwrap_or(0),
                        chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.%f"),
                        colored_level(&mut style, record.level()),
                        record.args()
                    )
                })
                .is_test(true)
                .init();
        });
        let taos = TaosOptions::new().build()?;

        let db: Vec<_> = self
            .naming
            .into_iter()
            .zip(self.precision.into_iter())
            .take(self.databases)
            .collect();
        let mut used = false;
        for (name, precision) in &db {
            if self.dropping.drop_before() {
                taos.exec_sync(format!("drop database if exists {}", name))?;
            }
            taos.exec_sync(format!(
                "create database if not exists {} precision '{}'",
                name, precision
            ))?;
            if !used {
                taos.exec_sync(format!("use {}", name))?;
                used = true;
            }
        }
        Ok(TaosWrapper {
            taos,
            db,
            drop: self.dropping,
        })
    }
}

pub struct TaosWrapper {
    taos: Taos,
    db: Vec<(String, Precision)>,
    drop: DroppingStrategy,
}

impl Deref for TaosWrapper {
    type Target = Taos;

    fn deref(&self) -> &Self::Target {
        &self.taos
    }
}

impl Drop for TaosWrapper {
    fn drop(&mut self) {
        self.clean().unwrap();
    }
}

impl TaosWrapper {
    pub fn taos(&self) -> &Taos {
        &self.taos
    }

    pub fn databases_with_precision(&self) -> &[(String, Precision)] {
        &self.db
    }

    pub fn databases(&self) -> Vec<&str> {
        self.db.iter().map(|v| v.0.as_str()).collect()
    }

    pub fn default_database(&self) -> &str {
        &self.db[0].0
    }

    fn clean(&self) -> Result<()> {
        if self.drop.drop_after() {
            for (name, _) in &self.db {
                log::debug!("drop database: {}", name);
                self.taos
                    .exec_sync(format!("drop database if exists {}", name))?;
                log::debug!("dropped database: {}", name);
            }
        }
        Ok(())
    }
}
