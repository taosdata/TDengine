use crate::*;

pub type taos_async_fetch_cb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, rows: c_int);

pub type taos_async_query_cb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, code: c_int);

extern "C" {
    pub fn taos_fetch_rows_a(res: *mut TAOS_RES, fp: taos_async_fetch_cb, param: *mut c_void);

    pub fn taos_query_a(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: taos_async_query_cb,
        param: *mut c_void,
    );
}

#[test]
fn test_query_a() {
    use crate::*;

    use std::sync::mpsc::channel;
    use std::sync::mpsc::Sender;
    pub struct CallbackArg {
        pub sender: Sender<i32>,
    }
    pub unsafe extern "C" fn async_query_callback(
        param: *mut c_void,
        res: *mut TAOS_RES,
        code: c_int,
    ) {
        assert!(code == 0);
        taos_free_result(res);
        let param = param as *mut CallbackArg;
        let args = Box::from_raw(param);

        let CallbackArg { sender } = *args;

        sender.send(12).unwrap();
    }
    unsafe {
        let taos = taos_connect(
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            0,
        );
        let (sender, receiver) = channel();
        let args = CallbackArg { sender };
        let args = Box::new(args);
        taos_query_a(
            taos,
            b"show databases\0" as *const u8 as _,
            async_query_callback,
            Box::into_raw(args) as *mut c_void,
        );
        let msg = receiver.recv().unwrap();
        println!("received: {msg}");
        taos_close(taos);
    }
}
