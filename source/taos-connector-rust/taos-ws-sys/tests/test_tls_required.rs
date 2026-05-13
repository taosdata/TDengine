#[cfg(feature = "test-tls")]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use taosws::ws::error::{taos_errno, taos_errstr};
    use taosws::ws::tmq::{
        tmq_conf_destroy, tmq_conf_new, tmq_conf_res_t, tmq_conf_set, tmq_consumer_close,
        tmq_consumer_new,
    };
    use taosws::ws::{
        taos_close, taos_connect, taos_connect_with, taos_init, taos_options, taos_set_option,
        OPTIONS, TSDB_OPTION,
    };

    #[test]
    fn test_taos_connect() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                6445,
            );
            assert!(!taos.is_null());
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_connect_failed() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                6041,
            );
            assert!(taos.is_null());

            let code = taos_errno(taos);
            assert_eq!(code, 0x8000000Bu32 as i32);

            let err = taos_errstr(taos);
            let errstr = CStr::from_ptr(err).to_str().unwrap();
            assert_eq!(errstr, "Unable to establish connection");
        }
    }

    #[test]
    fn test_taos_connect_with() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let mut opts = OPTIONS {
                keys: [ptr::null(); 256],
                values: [ptr::null(); 256],
                count: 0,
            };
            taos_set_option(&mut opts, c"port".as_ptr(), c"6445".as_ptr());

            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_connect_with_tls_disabled() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let mut opts = OPTIONS {
                keys: [ptr::null(); 256],
                values: [ptr::null(); 256],
                count: 0,
            };
            taos_set_option(&mut opts, c"wsTlsMode".as_ptr(), c"0".as_ptr());
            taos_set_option(&mut opts, c"port".as_ptr(), c"6041".as_ptr());

            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_connect_with_tls_required() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let mut opts = OPTIONS {
                keys: [ptr::null(); 256],
                values: [ptr::null(); 256],
                count: 0,
            };
            taos_set_option(&mut opts, c"wsTlsMode".as_ptr(), c"1".as_ptr());
            taos_set_option(&mut opts, c"wsTlsVersion".as_ptr(), c"TLSv1.2".as_ptr());
            taos_set_option(&mut opts, c"port".as_ptr(), c"6445".as_ptr());

            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_connect() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let code = taos_init();
            assert_eq!(code, 0);

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.port".as_ptr();
            let value = c"6445".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"group.id".as_ptr();
            let value = c"1001".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            tmq_conf_destroy(conf);
            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);
        }
    }

    #[test]
    fn test_tmq_connect_tls_disabled() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let code = taos_init();
            assert_eq!(code, 0);

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.port".as_ptr();
            let value = c"6041".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"ws.tls.mode".as_ptr();
            let value = c"0".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"group.id".as_ptr();
            let value = c"1001".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            tmq_conf_destroy(conf);
            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);
        }
    }

    #[test]
    fn test_tmq_connect_tls_required() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_required.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let code = taos_init();
            assert_eq!(code, 0);

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.port".as_ptr();
            let value = c"6445".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"ws.tls.mode".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"ws.tls.version".as_ptr();
            let value = c"TLSv1.2".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"group.id".as_ptr();
            let value = c"1001".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            tmq_conf_destroy(conf);
            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);
        }
    }
}
