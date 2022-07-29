#[test]
#[cfg(taos_v2)]
fn test_query_a() {
    use taos_sys::*;

    use std::sync::mpsc::channel;
    use std::sync::mpsc::Sender;
    pub struct CallbackArg {
        pub sender: Sender<i32>,
    }
    pub unsafe extern "C" fn async_query_callback(
        param: *mut c_void,
        res: *mut c_void,
        code: c_int,
    ) {
        assert!(code == 0);
        let _ = RawRes::from_ptr(res);
        let param = param as *mut CallbackArg;
        let args = Box::from_raw(param);

        let CallbackArg { sender } = *args;

        sender.send(12).unwrap();
    }

    let taos = RawTaos::connect(
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        0,
    )
    .expect("connect");
    let (sender, receiver) = channel();
    let args = CallbackArg { sender };
    let args = Box::new(args);
    taos.query_a(
        b"show databases\0" as *const u8 as _,
        async_query_callback,
        Box::into_raw(args) as *mut c_void,
    );
    let msg = receiver.recv().unwrap();
    println!("received: {msg}");
}
