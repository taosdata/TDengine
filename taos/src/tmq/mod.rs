use std::{
    ffi::{c_void, CStr},
    fmt::Debug,
    mem::ManuallyDrop,
    slice,
    sync::{Arc, Weak},
};

use crate::{Code, Error, IntoCStr, Result, Taos};
use taos_query::Dsn;
use taos_sys::*;

#[derive(Debug)]
pub struct TmqList(*mut tmq_list_t);

impl TmqList {
    fn new() -> Self {
        Self(unsafe { tmq_list_new() })
    }
    fn as_ptr(&self) -> *mut tmq_list_t {
        self.0
    }
    fn append<'a>(&mut self, c_str: impl IntoCStr<'a>) -> Result<()> {
        let ret = unsafe { tmq_list_append(self.0, c_str.into_c_str().as_ptr()) };
        if ret == 0 {
            Ok(())
        } else {
            Err(Error::new(Code::Failed, "append tmq list error"))
        }
    }

    fn from_topics<'a, T: IntoCStr<'a>>(topics: impl IntoIterator<Item = T>) -> Result<Self> {
        let mut list = Self::new();
        for topic in topics {
            list.append(topic)?;
        }
        Ok(list)
    }

    pub fn iter(&self) -> std::vec::IntoIter<&str> {
        self.to_str_vec().into_iter()
    }

    pub fn to_str_vec(&self) -> Vec<&str> {
        unsafe {
            let ptr = self.as_ptr();
            let len = tmq_list_get_size(ptr);
            if len == 0 {
                return vec![];
            }
            let arr = tmq_list_to_c_array(ptr);
            slice::from_raw_parts(arr, len as usize)
                .into_iter()
                .map(|ptr| {
                    CStr::from_ptr(*ptr)
                        .to_str()
                        .expect("topic should always be utf-8 valid")
                })
                .collect()
        }
    }
}

impl<'a> IntoIterator for &'a TmqList {
    type Item = &'a str;

    type IntoIter = std::vec::IntoIter<&'a str>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// todo: tmq_list_destroy cause double free error.
impl Drop for TmqList {
    fn drop(&mut self) {
        unsafe {
            log::trace!("list destroy");
            tmq_list_destroy(self.0);
        }
    }
}

mod conf;
pub use conf::*;

mod consumer;
pub use consumer::Consumer;

mod offset;
pub use offset::*;

use self::consumer::ConsumerRef;

pub struct TmqBuilder {
    conf: TmqConf,
    wait: i64,
    topics: TmqList,
    on_commit: Option<Arc<fn(ConsumerRef, Result<Offsets>)>>,
}

impl TmqBuilder {
    pub fn from_dsn<T: TryInto<Dsn>>(dsn: T) -> Result<Self>
    where
        T::Error: Debug,
    {
        let dsn = dsn.try_into().unwrap();
        log::debug!("build from {dsn}");
        let mut conf = TmqConf::new();
        macro_rules! _set_opt {
            ($f:ident, $c:literal) => {
                if let Some($f) = &dsn.$f {
                    conf.set(format!("td.connect.{}", $c), format!("{}", $f))?;
                }
            };
            ($f:ident) => {
                if let Some($f) = &dsn.$f {
                    conf.set(format!("td.connect.{}", stringify!($c)), format!("{}", $f))?;
                }
            };
        }

        // todo: host port?
        _set_opt!(username, "user");
        _set_opt!(password, "pass");
        _set_opt!(database, "db");

        // let tmq_params = dsn.params.iter_mut().filter(|(k, _)| k.contains(".")).collect();
        let mut conf = conf.with(dsn.params.iter().filter(|(k, _)| k.contains(".")))?;
        // conf.set("msg.with.table.name".to_string(), "true".to_string()).unwrap();

        let wait = dsn
            .params
            .get("wait")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        log::debug!("with wait time {wait}ms");
        let mut topics = TmqList::new();
        if let Some(t) = dsn.params.get("topics") {
            for s in t.split(",") {
                topics.append(s)?;
            }
        }
        Ok(Self {
            conf,
            wait,
            topics,
            on_commit: None,
        })
    }

    pub fn on_commit(&mut self, callback: fn(ConsumerRef, Result<Offsets>)) -> &mut Self {
        unsafe extern "C" fn tmq_commit_callback(
            _tmq: *mut tmq_t,
            resp: tmq_resp_err_t,
            _topic: *mut tmq_topic_vgroup_list_t,
            param: *mut c_void,
        ) {
            log::info!("commit {resp:?}");
            let cons = ConsumerRef::from_ptr(_tmq);
            let topic = resp.ok_or("commit failed").map(|_| Offsets(_topic));
            let cb: &Weak<fn(ConsumerRef, Result<Offsets>)> = std::mem::transmute(param);
            (*cb.as_ptr())(cons, topic);
        }
        let on_commit = Arc::new(callback);
        let cb = Arc::downgrade(&on_commit);

        self.on_commit = Some(on_commit);
        // todo: callback pointer should be freed in Drop.
        self.conf
            .set_offset_commit_cb(tmq_commit_callback, Box::into_raw(Box::new(cb)) as _);
        self
    }

    pub fn build(&self) -> Result<Consumer> {
        unsafe {
            let mut err = [0; 256];
            let tmq = tmq_consumer_new(self.conf.as_ptr(), err.as_mut_ptr() as _, 255);
            if err[0] != 0 {
                return Err(Error::from_string(
                    String::from_utf8_lossy(&err).to_string(),
                ));
            } else {
                let cons = Consumer::new(tmq, self.wait);
                cons.subscribe(&self.topics)?;
                Ok(cons)
            }
        }
    }

    pub fn subscribe<'a, T: IntoCStr<'a>>(
        &self,
        topics: impl IntoIterator<Item = T>,
    ) -> Result<Consumer> {
        unsafe {
            let mut err = [0; 256];
            let tmq = tmq_consumer_new(self.conf.as_ptr(), err.as_mut_ptr() as _, 255);
            if err[0] != 0 {
                return Err(Error::from_string(
                    String::from_utf8_lossy(&err).to_string(),
                ));
            } else {
                let cons = Consumer::new(tmq, self.wait);
                let topics = TmqList::from_topics(topics)?;
                cons.subscribe(&topics)?;
                Ok(cons)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::prelude::sync::*;
    use crate::tmq::*;

    use anyhow::Result;

    fn drop_topic(taos: &Taos, topic: &str) -> Result<()> {
        taos.exec(format!("drop topic if exists {topic}"))?;
        Ok(())
    }

    fn _build_consumer(taos: &Taos) -> Result<Consumer> {
        println!("consumer config");
        let mut conf = TmqConf::new();
        conf.set("group.id", "tg2")?;
        unsafe extern "C" fn tmq_commit_callback(
            _tmq: *mut tmq_t,
            resp: tmq_resp_err_t,
            _topic: *mut tmq_topic_vgroup_list_t,
            param: *mut c_void,
        ) {
            log::info!("commit {resp:?}");
        }
        conf.set_offset_commit_cb(tmq_commit_callback, std::ptr::null_mut());
        println!("build consumer");
        Ok(conf.consumer()?)
    }

    fn process_message(msg: &mut ResultSet) {
        let rows = msg.to_rows_vec();

        for row in rows {
            println!("{row:?}");
        }
    }

    fn insert(database: &str, max_inserts: usize) -> Result<()> {
        println!("connect taos in a spawned thread");
        let taos = Taos::new((), "root", "taosdata", database, 0)?;
        println!("start to insert 10 rows");
        for i in 0..max_inserts {
            use crate::prelude::sync::*;
            taos.exec(&format!("insert into tu1 values (now, {i})"))?;
            println!("- {i} rows inserted");
        }
        drop(taos);
        println!("write data thread finish");
        Ok(())
    }
    fn sync_consume_loop(database: &str, consumer: &Consumer) -> Result<()> {
        println!("consume loop");
        let running = Arc::new(atomic::AtomicBool::new(true));
        let msg_count = atomic::AtomicUsize::new(0);
        let running2 = running.clone();

        let database = database.to_string();
        thread::spawn(move || match insert(&database, 10) {
            Ok(_) => {
                running2.store(false, atomic::Ordering::SeqCst);
            }
            Err(err) => {
                running2.store(false, atomic::Ordering::SeqCst);
                eprintln!("{}", err.to_string());
            }
        });

        println!("inserting thread spawned.");
        while running.load(atomic::Ordering::SeqCst)
            || msg_count.load(atomic::Ordering::SeqCst) < 10
        {
            println!("looping...");
            if let Some(Ok(mut msg)) = consumer.poll_wait(1000) {
                println!("msg: {}", msg_count.load(atomic::Ordering::SeqCst));
                process_message(&mut msg);
                msg_count.fetch_add(1, atomic::Ordering::SeqCst);

                consumer.commit(None, 0)?;
                println!("msg summary: {:?}", msg.summary());
            }
        }
        println!("loop done");
        Ok(())
    }

    // todo: drop after consume will cause segmentation fault, use specific db name and no dropping.
    // #[crate::test(log_level = "trace", naming = "tmq_consume_test", dropping = "none")]
    #[crate::test(log_level = "trace")]
    fn tmq_consume(taos: &Taos, database: &str) -> Result<()> {
        let version = crate::client_info();
        println!("version: {}", version);
        if !version.starts_with("3") {
            return Ok(());
        }
        println!("connected");
        taos.exec_many([
            "create stable if not exists st1 (ts timestamp, k int) tags(a int)",
            "create table if not exists tu1 using st1 tags(1)",
            "create table if not exists tu2 using st1 tags(2)",
        ])?;
        taos.create_topic(database, database)?;

        let topic = database;
        let gid = database;

        let dsn = format!("taos:///{database}?topics={topic}&group.id={gid}&wait=1000");
        log::info!("subscribe with dsn: {dsn}");
        let consumer = TmqBuilder::from_dsn(&dsn)?
            .on_commit(
                |_: ConsumerRef, _: std::result::Result<Offsets, taos_error::Error>| {
                    log::info!("rust callback");
                },
            )
            .build()?;
        println!("topics created");
        sync_consume_loop(database, &consumer)?;
        dbg!(consumer.subscription()?);

        drop_topic(&taos, &topic)?;
        println!("finished");
        Ok(())
    }

    /// Consume from one database and write to another.
    // #[crate::test(log_level = "trace")]
    #[crate::test(log_level = "trace", naming = "tmq1", dropping = "none")]
    async fn tmq_stream(taos: &Taos, database: &str) -> Result<()> {
        let version = crate::client_info();
        println!("version: {}", version);
        if !version.starts_with("3") {
            return Ok(());
        }
        println!("connected");
        taos.exec_many([
            "create stable if not exists st1 (ts timestamp, k int) tags(a int)",
            "create table if not exists tu1 using st1 tags(1)",
            "create table if not exists tu2 using st1 tags(2)",
        ])?;
        taos.create_topic(database, database)?;

        let topic = database;
        let gid = database;

        let dsn = format!(
            "taos:///{database}?topics={topic}&group.id={gid}&wait=2000&msg.with.table.name=true"
        );
        log::info!("subscribe with dsn: {dsn}");
        let consumer = TmqBuilder::from_dsn(&dsn)?
            .on_commit(
                |_: ConsumerRef, _: std::result::Result<Offsets, taos_error::Error>| {
                    log::info!("rust callback");
                },
            )
            .build()?;
        println!("topics created");

        const MAX_INSERTS: usize = 10;
        let database = database.to_string();
        thread::spawn(move || insert(&database, MAX_INSERTS));

        taos.exec_many([
            "create database if not exists db2",
            "create stable if not exists db2.st1 (ts timestamp, k int) tags(a int)",
            "create table if not exists db2.tu1 using db2.st1 tags(1)",
            "create table if not exists db2.tu2 using db2.st1 tags(2)",
        ])?;
        let unfold = futures::sink::unfold(0, |mut sum, mut rs: ResultSet| async move {
            for block in rs.blocks_iter() {
                let bind: Vec<MultiBind> = block.columns_iter().map(|col| col.into()).collect();
                let table = block.tmq_table_name().unwrap();
                let mut stmt = taos.stmt(format!("insert into db2.{table} values(?,?)"))?;
                stmt.multi_bind(&bind)?;
                stmt.execute()?;
                let inserted = stmt.affected_rows();
                log::info!("inserted {inserted} rows");
            }
            let (blocks, rows) = rs.summary();
            assert!(blocks == 1, "tmq response blocks always should be 1");
            sum += rows;
            eprintln!("sum: {sum}, rows in block = {rows}");
            Ok::<_, taos_error::Error>(sum)
        });
        futures::pin_mut!(unfold);
        use futures::prelude::*;
        consumer.forward(unfold).await?;

        let db2_rows: usize = taos.query_one("select count(*) from db2.tu1")?.unwrap_or(0);

        drop_topic(&taos, &topic)?;
        taos.exec("drop database db2")?;
        if db2_rows != MAX_INSERTS {
            anyhow::bail!("inserted rows not match");
        }
        println!("finished");
        Ok(())
    }
}
