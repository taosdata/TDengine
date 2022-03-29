use std::{ffi::CStr, os::raw::c_char};

use crate::{Result, Taos, TaosCode, TaosError, TaosResult, ToCString};
use taos_sys::*;

pub struct TmqList(*mut tmq_list_t);

impl TmqList {
    pub fn new() -> Self {
        Self(unsafe { tmq_list_new() })
    }
    pub fn append(&mut self, c_str: impl AsRef<CStr>) -> Result<()> {
        let ret = unsafe { tmq_list_append(self.0, c_str.as_ref().as_ptr()) };
        if ret == 0 {
            Ok(())
        } else {
            Err(TaosError::new(TaosCode::Unknown, "append tmq list error"))
        }
    }
}

impl Taos {
    pub fn create_topic<'a>(&self, name: &str, sql: &str) -> Result<TaosResult<'a>> {
        // let name = name.to_c_string().as_ptr();
        // let sql_len = sql.len();
        // let sql = sql.to_c_string().as_ptr();
        let name = b"test_stb_topic_1\x00" as *const u8 as *const std::os::raw::c_char;
        let sql = b"select * from tu1\x00";
        let sql_ptr = b"select * from tu1\x00" as *const u8 as *const c_char;
        let res = unsafe { tmq_create_topic(self.0, name, sql_ptr, (sql.len() - 1) as _) };

        TaosResult::try_from_ptr(res)
    }

    pub fn consumer(&self, conf: &TmqConf) -> Result<Consumer> {
        let cons = unsafe { tmq_consumer_new(self.0, conf.as_ptr(), std::ptr::null_mut(), 0) };
        Ok(Consumer::new(cons))
    }
}
mod conf;
pub use conf::*;

mod consumer;
pub use consumer::Consumer;

mod message;
pub use message::*;

mod offset;
pub use offset::*;

#[cfg(test)]
mod test {
    use std::ffi::c_void;
    use std::sync::atomic;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::tmq::*;
    use crate::Result;
    use crate::TaosOptions;
    fn init_env(taos: &Taos) -> Result<()> {
        taos.query_sync("create database if not exists abc1 vgroups 1")?;
        println!("ues abc1");
        taos.query_sync("use abc1")?;
        taos.query_sync("create stable if not exists st1 (ts timestamp, k int) tags(a int)")?;
        taos.query_sync("create table if not exists tu1 using st1 tags(1)")?;
        taos.query_sync("create table if not exists tu2 using st1 tags(2)")?;
        Ok(())
    }

    fn create_topic(taos: &Taos) -> Result<()> {
        println!("use abc1");
        taos.query_sync("use abc1")?;
        println!("create topic");
        taos.create_topic("test_stb_topic_1", "select * from tu1")?;
        println!("create topic ok");
        Ok(())
    }

    fn build_consumer(taos: &Taos) -> Result<Consumer> {
        let res = taos.query_sync("use abc1")?;
        drop(res);
        println!("consumer config");
        let mut conf = TmqConf::new();
        conf.set("group.id", "tg2")?;
        unsafe extern "C" fn tmq_commit_callback(
            tmq: *mut tmq_t,
            resp: tmq_resp_err_t,
            topic: *mut tmq_topic_vgroup_list_t,
            param: *mut c_void,
        ) {
            println!("commit {resp:?}");
        }
        conf.set_offset_commit_cb(tmq_commit_callback);
        println!("build consumer");
        taos.consumer(&conf)
    }

    fn build_topic_list() -> Result<TmqList> {
        println!("build topic list");
        let mut topic = TmqList::new();
        topic.append("test_stb_topic_1".to_c_string())?;
        Ok(topic)
    }

    fn process_message(msg: &Message) {
        unsafe {
            tmqShowMsg(msg.0);
        }
    }
    fn sync_consume_loop(consumer: &Consumer, topics: &TmqList) -> Result<()> {
        println!("consume loop");
        const MIN_COMMIT_COUNT: usize = 1;
        // start subscription
        consumer.subscribe(topics)?;
        let running = Arc::new(atomic::AtomicBool::new(true));
        let msg_count = atomic::AtomicUsize::new(0);
        let running2 = running.clone();

        let thread = thread::spawn(move || {
            let taos = Taos::new("", "root", "taosdata", "", 0).unwrap();
            taos.query_sync("use abc1").expect("");
            for i in 0..10 {
                taos.query_sync(&format!("insert into tu1 values (now, {i})"))
                    .unwrap();
                thread::sleep(Duration::from_millis(1));
            }
            running2.store(false, atomic::Ordering::SeqCst);
            drop(taos);
            println!("write data thread finish");
        });

        while running.load(atomic::Ordering::SeqCst) {
            if let Some(msg) = consumer.poll(1000) {
                println!("msg: {}", msg_count.load(atomic::Ordering::SeqCst));
                process_message(&msg);
                let count = msg_count.fetch_add(1, atomic::Ordering::SeqCst);
                if count % MIN_COMMIT_COUNT == 0 {
                    consumer.commit(None, 0)?;
                }
            }
        }
        // consumer.unsubscribe()?; // todo: call unsubscribe when it be safe.
        thread.join().unwrap();
        Ok(())
    }

    #[test]
    fn tmq_consume() -> Result<()> {
        unsafe { taos_sys::taos_init() };
        println!("version: {}", crate::client_info());
        TaosOptions::new().config_dir("/home/huolinhe/Projects/taosdata/taosx/TDengine/demo");
        let taos = Taos::new("", "root", "taosdata", "", 0).unwrap();
        init_env(&taos)?;
        create_topic(&taos)?;
        let consumer = build_consumer(&taos)?;
        let topics = build_topic_list()?;
        sync_consume_loop(&consumer, &topics)?;
        Ok(())
    }
}
