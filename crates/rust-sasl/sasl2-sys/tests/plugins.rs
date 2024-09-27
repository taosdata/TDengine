// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::ffi::{CStr, CString};
use std::ptr;

use libc::c_void;
use sasl2_sys::sasl::{sasl_client_init, sasl_server_init, SASL_OK};
use sasl2_sys::saslplug::{
    client_sasl_mechanism_t, sasl_client_plugin_info, sasl_info_callback_stage_t,
    sasl_server_plugin_info, server_sasl_mechanism_t, SASL_INFO_LIST_END, SASL_INFO_LIST_START,
};

#[no_mangle]
unsafe extern "C" fn client_plugin_info_callback(
    m: *mut client_sasl_mechanism_t,
    stage: sasl_info_callback_stage_t,
    out: *mut c_void,
) {
    if stage != SASL_INFO_LIST_START && stage != SASL_INFO_LIST_END && m != ptr::null_mut() {
        let mech_name = CStr::from_ptr((*(*m).plug).mech_name);
        let out = out as *mut HashSet<String>;
        (*out).insert(mech_name.to_str().unwrap().into());
    }
}

#[no_mangle]
unsafe extern "C" fn server_plugin_info_callback(
    m: *mut server_sasl_mechanism_t,
    stage: sasl_info_callback_stage_t,
    out: *mut c_void,
) {
    if stage != SASL_INFO_LIST_START && stage != SASL_INFO_LIST_END && m != ptr::null_mut() {
        let mech_name = CStr::from_ptr((*(*m).plug).mech_name);
        let out = out as *mut HashSet<String>;
        (*out).insert(mech_name.to_str().unwrap().into());
    }
}

#[test]
fn test_plugin_info() {
    let mut client_mechs = HashSet::new();
    unsafe {
        let res = sasl_client_init(ptr::null());
        if res != SASL_OK {
            panic!("failed to initialize sasl client");
        }
        sasl_client_plugin_info(
            ptr::null(), // indicates interest in all plugins
            Some(client_plugin_info_callback),
            &mut client_mechs as *mut HashSet<String> as *mut c_void,
        );
    }
    assert!(client_mechs.contains("EXTERNAL"));
    #[cfg(feature = "gssapi-vendored")]
    assert!(client_mechs.contains("GSSAPI"));
    #[cfg(feature = "plain")]
    assert!(client_mechs.contains("PLAIN"));
    #[cfg(feature = "scram")]
    {
        assert!(client_mechs.contains("SCRAM-SHA-1"));
        assert!(client_mechs.contains("SCRAM-SHA-256"));
    }

    let mut server_mechs = HashSet::new();
    unsafe {
        let res = sasl_server_init(ptr::null(), CString::new("test_plugins").unwrap().as_ptr());
        if res != SASL_OK {
            panic!("failed to initialize sasl server");
        }
        sasl_server_plugin_info(
            ptr::null(), // indicates interest in all plugins
            Some(server_plugin_info_callback),
            &mut server_mechs as *mut HashSet<String> as *mut c_void,
        );
    }
    assert!(server_mechs.contains("EXTERNAL"));
    #[cfg(feature = "gssapi-vendored")]
    assert!(server_mechs.contains("GSSAPI"));
    #[cfg(feature = "plain")]
    assert!(server_mechs.contains("PLAIN"));
    #[cfg(feature = "scram")]
    {
        assert!(server_mechs.contains("SCRAM-SHA-1"));
        assert!(server_mechs.contains("SCRAM-SHA-256"));
    }
}
