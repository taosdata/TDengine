#[cfg(feature = "test-tls")]
mod tests {
    use std::ffi::{CStr, CString};
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
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                6446,
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
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
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
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let mut opts = OPTIONS {
                keys: [ptr::null(); 256],
                values: [ptr::null(); 256],
                count: 0,
            };
            taos_set_option(&mut opts, c"port".as_ptr(), c"6446".as_ptr());

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
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
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
    fn test_taos_connect_with_tls_verify_ca() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let ws_tls_ca = CString::new(
                "-----BEGIN CERTIFICATE-----
MIIDvTCCAqWgAwIBAgIUbyCO/TtsFG8IWRDLWohhOpFKVS0wDQYJKoZIhvcNAQEL
BQAwbjELMAkGA1UEBhMCQ04xEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwI
WW91ckNpdHkxEDAOBgNVBAoMB1lvdXJPcmcxETAPBgNVBAsMCFlvdXJVbml0MRMw
EQYDVQQDDApZb3VyUm9vdENBMB4XDTI1MTIxMTA5MzUxOFoXDTM1MTIwOTA5MzUx
OFowbjELMAkGA1UEBhMCQ04xEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwI
WW91ckNpdHkxEDAOBgNVBAoMB1lvdXJPcmcxETAPBgNVBAsMCFlvdXJVbml0MRMw
EQYDVQQDDApZb3VyUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEArAC426sQDcJwYQsaA7h0XgSN/yDM3nH0awO3m7/TNT4n8nKQcCRmEnkTI/QF
a2FBIlMERBQB55ZlTcSYsuglMrKSHUmYj9A+B9Uhs4uAC6bGGtLPnWDBCXFPU9nH
HeXRtid7NgTWxKh8YLbXIcuuKWcOZfadw+pmLiIh9fK72vKqX9LOvoMxIwr3ehs+
yjW4fToEDmmUDuTGsNJ2WmMTF1JEkUmLCdoMW2k2e0U/3hYgOadYTTfVypnZAWt0
7H3v/iC0e17K1tKc4J7TcdUPOO0aOTVcTvYoqA3yljKhFlDB50ljrhJzKoC5KLys
9SlsXCZOSgdvj9t2U5Op6Ot07QIDAQABo1MwUTAdBgNVHQ4EFgQULQTxA3wb5eEX
FId2DD0uLma0jd4wHwYDVR0jBBgwFoAULQTxA3wb5eEXFId2DD0uLma0jd4wDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAJfy18SdA9jR+O/BHHfkL
QFzm71vrqSWjLtvu6Wl2wXj5vJ51cxXoHGBMSB9B05PU+34oSuiyxu2AEur/1jZ3
K+S9wXUe5u5d4tJwfebwYxNSbAQkKsxMYCEx8yyO7oz+b0U8u/r3DUFxC4Hy7yT6
LXNzGfczOPDOln3QeXwzLv4Cse2g2Ner6JzIwUjf3XPVj5ousjhehq1Q7Lvk+GE7
YyI5MJFFR1XWSUR2023PAF4Ut+LZEHpnNPtmc0JZ2hBPq9IkOiM4jwnL2Da/4F+p
Us2YT2Jxi5fG7xfLGOwopV1LGR3oOuRMkjkVAV7LWcks6Kyi2ZmJrADFTFXso0s5
Sg==
-----END CERTIFICATE-----",
            )
            .unwrap();

            let mut opts = OPTIONS {
                keys: [ptr::null(); 256],
                values: [ptr::null(); 256],
                count: 0,
            };
            taos_set_option(&mut opts, c"wsTlsMode".as_ptr(), c"2".as_ptr());
            taos_set_option(&mut opts, c"wsTlsVersion".as_ptr(), c"TLSv1.2".as_ptr());
            taos_set_option(&mut opts, c"wsTlsCa".as_ptr(), ws_tls_ca.as_ptr());
            taos_set_option(&mut opts, c"port".as_ptr(), c"6446".as_ptr());

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
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let code = taos_init();
            assert_eq!(code, 0);

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.port".as_ptr();
            let value = c"6446".as_ptr();
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
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
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
    fn test_tmq_connect_tls_verify_ca() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls_verify_ca_pem.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let code = taos_init();
            assert_eq!(code, 0);

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.port".as_ptr();
            let value = c"6446".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"ws.tls.mode".as_ptr();
            let value = c"2".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"ws.tls.version".as_ptr();
            let value = c"TLSv1.2".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let ws_tls_ca = CString::new(
                "-----BEGIN CERTIFICATE-----
MIIDvTCCAqWgAwIBAgIUbyCO/TtsFG8IWRDLWohhOpFKVS0wDQYJKoZIhvcNAQEL
BQAwbjELMAkGA1UEBhMCQ04xEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwI
WW91ckNpdHkxEDAOBgNVBAoMB1lvdXJPcmcxETAPBgNVBAsMCFlvdXJVbml0MRMw
EQYDVQQDDApZb3VyUm9vdENBMB4XDTI1MTIxMTA5MzUxOFoXDTM1MTIwOTA5MzUx
OFowbjELMAkGA1UEBhMCQ04xEjAQBgNVBAgMCVlvdXJTdGF0ZTERMA8GA1UEBwwI
WW91ckNpdHkxEDAOBgNVBAoMB1lvdXJPcmcxETAPBgNVBAsMCFlvdXJVbml0MRMw
EQYDVQQDDApZb3VyUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEArAC426sQDcJwYQsaA7h0XgSN/yDM3nH0awO3m7/TNT4n8nKQcCRmEnkTI/QF
a2FBIlMERBQB55ZlTcSYsuglMrKSHUmYj9A+B9Uhs4uAC6bGGtLPnWDBCXFPU9nH
HeXRtid7NgTWxKh8YLbXIcuuKWcOZfadw+pmLiIh9fK72vKqX9LOvoMxIwr3ehs+
yjW4fToEDmmUDuTGsNJ2WmMTF1JEkUmLCdoMW2k2e0U/3hYgOadYTTfVypnZAWt0
7H3v/iC0e17K1tKc4J7TcdUPOO0aOTVcTvYoqA3yljKhFlDB50ljrhJzKoC5KLys
9SlsXCZOSgdvj9t2U5Op6Ot07QIDAQABo1MwUTAdBgNVHQ4EFgQULQTxA3wb5eEX
FId2DD0uLma0jd4wHwYDVR0jBBgwFoAULQTxA3wb5eEXFId2DD0uLma0jd4wDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAJfy18SdA9jR+O/BHHfkL
QFzm71vrqSWjLtvu6Wl2wXj5vJ51cxXoHGBMSB9B05PU+34oSuiyxu2AEur/1jZ3
K+S9wXUe5u5d4tJwfebwYxNSbAQkKsxMYCEx8yyO7oz+b0U8u/r3DUFxC4Hy7yT6
LXNzGfczOPDOln3QeXwzLv4Cse2g2Ner6JzIwUjf3XPVj5ousjhehq1Q7Lvk+GE7
YyI5MJFFR1XWSUR2023PAF4Ut+LZEHpnNPtmc0JZ2hBPq9IkOiM4jwnL2Da/4F+p
Us2YT2Jxi5fG7xfLGOwopV1LGR3oOuRMkjkVAV7LWcks6Kyi2ZmJrADFTFXso0s5
Sg==
-----END CERTIFICATE-----",
            )
            .unwrap();

            let key = c"ws.tls.ca".as_ptr();
            let res = tmq_conf_set(conf, key, ws_tls_ca.as_ptr());
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
