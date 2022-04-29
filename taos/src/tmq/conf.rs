use crate::{Error, IntoCStr, Result, TaosOptions};

use std::iter::Iterator;
use taos_sys::*;

use super::Consumer;
/* tmq conf */
pub struct TmqConf(*mut tmq_conf_t);

impl TmqConf {
    pub(crate) fn as_ptr(&self) -> *mut tmq_conf_t {
        self.0
    }
    pub fn new() -> Self {
        Self(unsafe { tmq_conf_new() })
    }

    pub fn from_opts(opts: &TaosOptions) -> Result<Self> {
        let mut conf = Self::new();

        macro_rules! _set_opt {
            ($f:ident, $c:literal) => {
                if let Some($f) = &opts.$f {
                    conf.set(format!("td.connect.{}", $c), format!("{}", $f))?;
                }
            };
            ($f:ident) => {
                if let Some($f) = &opts.$f {
                    conf.set(format!("td.connect.{}", stringify!($c)), format!("{}", $f))?;
                }
            };
        }

        _set_opt!(host, "ip");
        _set_opt!(username, "user");
        _set_opt!(password, "pass");
        _set_opt!(port, "port");
        _set_opt!(database, "db");

        conf.with(opts.params.iter().filter(|(k, _)| k.contains(".")))
    }

    pub fn group_id(mut self, id: &str) -> Self {
        self.set("group.id", id)
            .expect("set group.id should always be ok");
        self
    }
    pub fn client_id(mut self, id: &str) -> Self {
        self.set("client.id", id)
            .expect("set group.id should always be ok");
        self
    }

    pub fn enable_auto_commit(mut self, enabled: bool) -> Self {
        self.set("enable.auto.commit", if enabled { "true" } else { "false" })
            .expect("set group.id should always be ok");
        self
    }

    pub fn with<K: AsRef<str>, V: AsRef<str>>(
        mut self,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Result<Self> {
        for (k, v) in iter {
            self.set(k, v)?;
        }
        Ok(self)
    }

    pub fn set<K: AsRef<str>, V: AsRef<str>>(&mut self, key: K, value: V) -> Result<&mut Self> {
        let ret = unsafe {
            tmq_conf_set(
                self.0,
                key.as_ref().into_c_str().as_ptr(),
                value.as_ref().into_c_str().as_ptr(),
            )
        };
        match ret {
            tmq_conf_res_t::Ok => Ok(self),
            tmq_conf_res_t::Invalid => Err(Error::from_string("invalid key value set for tmq")),
            tmq_conf_res_t::Unknown => Err(Error::from_string("unknown key for tmq conf")),
        }
    }

    pub fn set_offset_commit_cb(&mut self, cb: tmq_commit_cb) -> () {
        unsafe {
            tmq_conf_set_offset_commit_cb(self.0, cb);
        }
    }

    pub fn consumer(&self) -> Result<Consumer> {
        unsafe {
            let mut err = [0; 256];
            let tmq = tmq_consumer_new(self.0, err.as_mut_ptr() as _, 255);
            if err[0] != 0 {
                Err(Error::from_string(
                    String::from_utf8_lossy(&err).to_string(),
                ))
            } else {
                Ok(Consumer::new(tmq))
            }
        }
    }
}

impl Drop for TmqConf {
    fn drop(&mut self) {
        unsafe { tmq_conf_destroy(self.0) }
    }
}
