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

//! SASL plugin API.

use libc::{c_char, c_int, c_uchar, c_uint, c_ulong, c_void};

use super::hmac_md5::{HMAC_MD5_CTX, HMAC_MD5_STATE};
use super::md5::MD5_CTX;
use super::prop::{propctx, propval};
use super::sasl::{
    iovec, sasl_callback_t, sasl_calloc_t, sasl_channel_binding_t, sasl_conn_t, sasl_free_t,
    sasl_getopt_t, sasl_http_request_t, sasl_interact_t, sasl_malloc_t, sasl_mutex_alloc_t,
    sasl_mutex_free_t, sasl_mutex_lock_t, sasl_mutex_unlock_t, sasl_rand_t, sasl_realloc_t,
    sasl_security_properties_t, sasl_ssf_t,
};

pub type sasl_callback_ft = Option<unsafe extern "C" fn() -> c_int>;

pub type sasl_getcallback_t = Option<
    unsafe extern "C" fn(
        conn: *mut sasl_conn_t,
        callbackid: c_ulong,
        pproc: *mut sasl_callback_ft,
        pcontext: *mut *mut c_void,
    ) -> c_int,
>;

// Utility functions for plugins.

pub const SASL_UTILS_VERSION: c_uint = 4;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct sasl_utils {
    pub version: c_int,

    pub conn: *mut sasl_conn_t,
    pub rpool: *mut sasl_rand_t,
    pub getopt_context: *mut c_void,

    pub getopt: sasl_getopt_t,

    pub malloc: sasl_malloc_t,
    pub calloc: sasl_calloc_t,
    pub realloc: sasl_realloc_t,
    pub free: sasl_free_t,

    pub mutex_alloc: sasl_mutex_alloc_t,
    pub mutex_lock: sasl_mutex_lock_t,
    pub mutex_unlock: sasl_mutex_unlock_t,
    pub mutex_free: sasl_mutex_free_t,

    pub MD5Init: Option<unsafe extern "C" fn(arg1: *mut MD5_CTX)>,
    pub MD5Update:
        Option<unsafe extern "C" fn(arg1: *mut MD5_CTX, text: *const c_uchar, len: c_uint)>,
    pub MD5Final: Option<unsafe extern "C" fn(arg1: *mut c_uchar, arg2: *mut MD5_CTX)>,
    pub hmac_md5: Option<
        unsafe extern "C" fn(
            text: *const c_uchar,
            text_len: c_int,
            key: *const c_uchar,
            key_len: c_int,
            arg1: *mut c_uchar,
        ),
    >,
    pub hmac_md5_init:
        Option<unsafe extern "C" fn(arg1: *mut HMAC_MD5_CTX, key: *const c_uchar, len: c_int)>,
    pub hmac_md5_final: Option<unsafe extern "C" fn(arg1: *mut c_uchar, arg2: *mut HMAC_MD5_CTX)>,
    pub hmac_md5_precalc:
        Option<unsafe extern "C" fn(arg1: *mut HMAC_MD5_STATE, key: *const c_uchar, len: c_int)>,
    pub hmac_md5_import:
        Option<unsafe extern "C" fn(arg1: *mut HMAC_MD5_CTX, arg2: *mut HMAC_MD5_STATE)>,

    pub mkchal: Option<
        unsafe extern "C" fn(
            conn: *mut sasl_conn_t,
            buf: *mut c_char,
            maxlen: c_uint,
            hostflag: c_uint,
        ) -> c_int,
    >,
    pub utf8verify: Option<unsafe extern "C" fn(str: *const c_char, len: c_uint) -> c_int>,
    pub rand: Option<unsafe extern "C" fn(rpool: *mut sasl_rand_t, buf: *mut c_char, len: c_uint)>,
    pub churn:
        Option<unsafe extern "C" fn(rpool: *mut sasl_rand_t, data: *const c_char, len: c_uint)>,

    pub checkpass: Option<
        unsafe extern "C" fn(
            conn: *mut sasl_conn_t,
            user: *const c_char,
            userlen: c_uint,
            pass: *const c_char,
            passlen: c_uint,
        ) -> c_int,
    >,

    pub decode64: Option<
        unsafe extern "C" fn(
            in_: *const c_char,
            inlen: c_uint,
            out: *mut c_char,
            outmax: c_uint,
            outlen: *mut c_uint,
        ) -> c_int,
    >,
    pub encode64: Option<
        unsafe extern "C" fn(
            in_: *const c_char,
            inlen: c_uint,
            out: *mut c_char,
            outmax: c_uint,
            outlen: *mut c_uint,
        ) -> c_int,
    >,

    pub erasebuffer: Option<unsafe extern "C" fn(buf: *mut c_char, len: c_uint)>,

    pub getprop: Option<
        unsafe extern "C" fn(
            conn: *mut sasl_conn_t,
            propnum: c_int,
            pvalue: *mut *const c_void,
        ) -> c_int,
    >,
    pub setprop: Option<
        unsafe extern "C" fn(conn: *mut sasl_conn_t, propnum: c_int, value: *const c_void) -> c_int,
    >,

    pub getcallback: sasl_getcallback_t,

    pub log:
        Option<unsafe extern "C" fn(conn: *mut sasl_conn_t, level: c_int, fmt: *const c_char, ...)>,

    pub seterror: Option<
        unsafe extern "C" fn(conn: *mut sasl_conn_t, flags: c_uint, fmt: *const c_char, ...),
    >,

    pub spare_fptr: Option<unsafe extern "C" fn() -> *mut c_int>,

    pub prop_new: Option<unsafe extern "C" fn(estimate: c_uint) -> *mut propctx>,
    pub prop_dup:
        Option<unsafe extern "C" fn(src_ctx: *mut propctx, dst_ctx: *mut *mut propctx) -> c_int>,
    pub prop_request:
        Option<unsafe extern "C" fn(ctx: *mut propctx, names: *mut *const c_char) -> c_int>,
    pub prop_get: Option<unsafe extern "C" fn(ctx: *mut propctx) -> *const propval>,
    pub prop_getnames: Option<
        unsafe extern "C" fn(
            ctx: *mut propctx,
            names: *mut *const c_char,
            vals: *mut propval,
        ) -> c_int,
    >,
    pub prop_clear: Option<unsafe extern "C" fn(ctx: *mut propctx, requests: c_int)>,
    pub prop_dispose: Option<unsafe extern "C" fn(ctx: *mut *mut propctx)>,
    pub prop_format: Option<
        unsafe extern "C" fn(
            ctx: *mut propctx,
            sep: *const c_char,
            seplen: c_int,
            outbuf: *mut c_char,
            outmax: c_uint,
            outlen: *mut c_uint,
        ) -> c_int,
    >,
    pub prop_set: Option<
        unsafe extern "C" fn(
            ctx: *mut propctx,
            name: *const c_char,
            value: *const c_char,
            vallen: c_int,
        ) -> c_int,
    >,
    pub prop_setvals: Option<
        unsafe extern "C" fn(
            ctx: *mut propctx,
            name: *const c_char,
            values: *mut *const c_char,
        ) -> c_int,
    >,
    pub prop_erase: Option<unsafe extern "C" fn(ctx: *mut propctx, name: *const c_char)>,

    pub auxprop_store: Option<
        unsafe extern "C" fn(
            conn: *mut sasl_conn_t,
            ctx: *mut propctx,
            user: *const c_char,
        ) -> c_int,
    >,

    pub spare_fptr1: Option<unsafe extern "C" fn() -> c_int>,
    pub spare_fptr2: Option<unsafe extern "C" fn() -> c_int>,
}

pub type sasl_utils_t = sasl_utils;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct sasl_out_params {
    pub doneflag: c_uint,

    pub user: *const c_char,
    pub authid: *const c_char,

    pub ulen: c_uint,
    pub alen: c_uint,

    pub maxoutbuf: c_uint,
    pub mech_ssf: sasl_ssf_t,
    pub encode_context: *mut c_void,
    pub encode: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            invec: *const iovec,
            numiov: c_uint,
            output: *mut *const c_char,
            outputlen: *mut c_uint,
        ) -> c_int,
    >,
    pub decode_context: *mut c_void,
    pub decode: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            input: *const c_char,
            inputlen: c_uint,
            output: *mut *const c_char,
            outputlen: *mut c_uint,
        ) -> c_int,
    >,

    pub client_creds: *mut c_void,

    pub gss_peer_name: *const c_void,
    pub gss_local_name: *const c_void,
    pub cbindingname: *const c_char,
    pub spare_fptr1: Option<unsafe extern "C" fn() -> c_int>,
    pub spare_fptr2: Option<unsafe extern "C" fn() -> c_int>,
    pub cbindingdisp: c_uint,
    pub spare_int2: c_int,
    pub spare_int3: c_int,
    pub spare_int4: c_int,

    pub param_version: c_int,
}

pub type sasl_out_params_t = sasl_out_params;

pub type sasl_info_callback_stage_t = c_uint;

pub const SASL_INFO_LIST_START: sasl_info_callback_stage_t = 0;
pub const SASL_INFO_LIST_MECH: sasl_info_callback_stage_t = 1;
pub const SASL_INFO_LIST_END: sasl_info_callback_stage_t = 2;

// Channel binding.

pub type sasl_cbinding_disp_t = c_uint;

pub const SASL_CB_DISP_NONE: sasl_cbinding_disp_t = 0;
pub const SASL_CB_DISP_WANT: sasl_cbinding_disp_t = 1;
pub const SASL_CB_DISP_USED: sasl_cbinding_disp_t = 2;

// Client mechanism functions.

#[repr(C)]
#[derive(Copy, Clone)]
pub struct sasl_client_params {
    pub service: *const c_char,
    pub serverFQDN: *const c_char,
    pub clientFQDN: *const c_char,
    pub utils: *const sasl_utils_t,
    pub prompt_supp: *const sasl_callback_t,
    pub iplocalport: *const c_char,
    pub ipremoteport: *const c_char,

    pub servicelen: c_uint,
    pub slen: c_uint,
    pub clen: c_uint,
    pub iploclen: c_uint,
    pub ipremlen: c_uint,

    pub props: sasl_security_properties_t,
    pub external_ssf: sasl_ssf_t,

    pub gss_creds: *const c_void,
    pub cbinding: *const sasl_channel_binding_t,
    pub http_request: *const sasl_http_request_t,
    pub spare_ptr4: *mut c_void,

    pub canon_user: Option<
        unsafe extern "C" fn(
            conn: *mut sasl_conn_t,
            in_: *const c_char,
            len: c_uint,
            flags: c_uint,
            oparams: *mut sasl_out_params_t,
        ) -> c_int,
    >,

    pub spare_fptr1: Option<unsafe extern "C" fn() -> c_int>,

    pub cbindingdisp: c_uint,
    pub spare_int2: c_int,
    pub spare_int3: c_int,

    pub flags: c_uint,

    pub param_version: c_int,
}

pub type sasl_client_params_t = sasl_client_params;

pub const SASL_FEAT_WANT_CLIENT_FIRST: c_uint = 2;
pub const SASL_FEAT_SERVER_FIRST: c_uint = 16;
pub const SASL_FEAT_ALLOWS_PROXY: c_uint = 32;
pub const SASL_FEAT_DONTUSE_USERPASSWD: c_uint = 128;
pub const SASL_FEAT_GSS_FRAMING: c_uint = 256;
pub const SASL_FEAT_CHANNEL_BINDING: c_uint = 2048;
pub const SASL_FEAT_SUPPORTS_HTTP: c_uint = 4096;
pub const SASL_FEAT_NEEDSERVERFQDN: c_uint = 1;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct sasl_client_plug {
    pub mech_name: *const c_char,
    pub max_ssf: sasl_ssf_t,
    pub security_flags: c_uint,
    pub features: c_uint,
    pub required_prompts: *const c_ulong,
    pub glob_context: *mut c_void,
    pub mech_new: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            cparams: *mut sasl_client_params_t,
            conn_context: *mut *mut c_void,
        ) -> c_int,
    >,
    pub mech_step: Option<
        unsafe extern "C" fn(
            conn_context: *mut c_void,
            cparams: *mut sasl_client_params_t,
            serverin: *const c_char,
            serverinlen: c_uint,
            prompt_need: *mut *mut sasl_interact_t,
            clientout: *mut *const c_char,
            clientoutlen: *mut c_uint,
            oparams: *mut sasl_out_params_t,
        ) -> c_int,
    >,
    pub mech_dispose:
        Option<unsafe extern "C" fn(conn_context: *mut c_void, utils: *const sasl_utils_t)>,
    pub mech_free:
        Option<unsafe extern "C" fn(glob_context: *mut c_void, utils: *const sasl_utils_t)>,
    pub idle: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            conn_context: *mut c_void,
            cparams: *mut sasl_client_params_t,
        ) -> c_int,
    >,
    pub spare_fptr1: Option<unsafe extern "C" fn() -> c_int>,
    pub spare_fptr2: Option<unsafe extern "C" fn() -> c_int>,
}

pub type sasl_client_plug_t = sasl_client_plug;

pub const SASL_CLIENT_PLUG_VERSION: c_uint = 4;

pub type sasl_client_plug_init_t = Option<
    unsafe extern "C" fn(
        utils: *const sasl_utils_t,
        max_version: c_int,
        out_version: *mut c_int,
        pluglist: *mut *mut sasl_client_plug_t,
        plugcount: *mut c_int,
    ) -> c_int,
>;

extern "C" {
    pub fn sasl_client_add_plugin(
        plugname: *const c_char,
        cplugfunc: sasl_client_plug_init_t,
    ) -> c_int;
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct client_sasl_mechanism {
    pub version: c_int,
    pub plugname: *mut c_char,
    pub plug: *const sasl_client_plug_t,
}

pub type client_sasl_mechanism_t = client_sasl_mechanism;

pub type sasl_client_info_callback_t = Option<
    unsafe extern "C" fn(
        m: *mut client_sasl_mechanism_t,
        stage: sasl_info_callback_stage_t,
        rock: *mut c_void,
    ),
>;

extern "C" {
    pub fn sasl_client_plugin_info(
        mech_list: *const c_char,
        info_cb: sasl_client_info_callback_t,
        info_cb_rock: *mut c_void,
    ) -> c_int;
}

// Server functions.

pub type sasl_logmsg_p =
    Option<unsafe extern "C" fn(conn: *mut sasl_conn_t, fmt: *const c_char, ...)>;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct sasl_server_params {
    pub service: *const c_char,
    pub appname: *const c_char,
    pub serverFQDN: *const c_char,
    pub user_realm: *const c_char,
    pub iplocalport: *const c_char,
    pub ipremoteport: *const c_char,

    pub servicelen: c_uint,
    pub applen: c_uint,
    pub slen: c_uint,
    pub urlen: c_uint,
    pub iploclen: c_uint,
    pub ipremlen: c_uint,

    pub log_level: c_int,

    pub utils: *const sasl_utils_t,
    pub callbacks: *const sasl_callback_t,

    pub props: sasl_security_properties_t,
    pub external_ssf: sasl_ssf_t,

    pub transition: Option<
        unsafe extern "C" fn(conn: *mut sasl_conn_t, pass: *const c_char, passlen: c_uint) -> c_int,
    >,

    pub canon_user: Option<
        unsafe extern "C" fn(
            conn: *mut sasl_conn_t,
            user: *const c_char,
            ulen: c_uint,
            flags: c_uint,
            oparams: *mut sasl_out_params_t,
        ) -> c_int,
    >,

    pub propctx: *mut propctx,

    pub gss_creds: *const c_void,
    pub cbinding: *const sasl_channel_binding_t,
    pub http_request: *const sasl_http_request_t,
    pub spare_ptr4: *mut c_void,
    pub spare_fptr1: Option<unsafe extern "C" fn() -> c_int>,
    pub spare_fptr2: Option<unsafe extern "C" fn() -> c_int>,
    pub spare_int1: c_int,
    pub spare_int2: c_int,
    pub spare_int3: c_int,

    pub flags: c_uint,

    pub param_version: c_int,
}

pub type sasl_server_params_t = sasl_server_params;

pub const SASL_SET_REMOVE: c_uint = 1;

pub const SASL_FEAT_SERVICE: c_uint = 512;
pub const SASL_FEAT_GETSECRET: c_uint = 1024;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct sasl_server_plug {
    pub mech_name: *const c_char,
    pub max_ssf: sasl_ssf_t,
    pub security_flags: c_uint,
    pub features: c_uint,
    pub glob_context: *mut c_void,
    pub mech_new: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            challenge: *const c_char,
            challen: c_uint,
            conn_context: *mut *mut c_void,
        ) -> c_int,
    >,
    pub mech_step: Option<
        unsafe extern "C" fn(
            conn_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            clientin: *const c_char,
            clientinlen: c_uint,
            serverout: *mut *const c_char,
            serveroutlen: *mut c_uint,
            oparams: *mut sasl_out_params_t,
        ) -> c_int,
    >,
    pub mech_dispose:
        Option<unsafe extern "C" fn(conn_context: *mut c_void, utils: *const sasl_utils_t)>,
    pub mech_free:
        Option<unsafe extern "C" fn(glob_context: *mut c_void, utils: *const sasl_utils_t)>,
    pub setpass: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            user: *const c_char,
            pass: *const c_char,
            passlen: c_uint,
            oldpass: *const c_char,
            oldpasslen: c_uint,
            flags: c_uint,
        ) -> c_int,
    >,
    pub user_query: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            user: *const c_char,
            maxmech: c_int,
            mechlist: *mut *const c_char,
        ) -> c_int,
    >,
    pub idle: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            conn_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
        ) -> c_int,
    >,
    pub mech_avail: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            conn_context: *mut *mut c_void,
        ) -> c_int,
    >,
    pub spare_fptr2: Option<unsafe extern "C" fn() -> c_int>,
}

pub type sasl_server_plug_t = sasl_server_plug;

pub const SASL_SERVER_PLUG_VERSION: c_uint = 4;

pub type sasl_server_plug_init_t = Option<
    unsafe extern "C" fn(
        utils: *const sasl_utils_t,
        max_version: c_int,
        out_version: *mut c_int,
        pluglist: *mut *mut sasl_server_plug_t,
        plugcount: *mut c_int,
    ) -> c_int,
>;

extern "C" {
    pub fn sasl_server_add_plugin(
        plugname: *const c_char,
        splugfunc: sasl_server_plug_init_t,
    ) -> c_int;
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct server_sasl_mechanism {
    pub version: c_int,
    pub condition: c_int,
    pub plugname: *mut c_char,
    pub plug: *const sasl_server_plug_t,
    pub f: *mut c_char,
}

pub type server_sasl_mechanism_t = server_sasl_mechanism;

pub type sasl_server_info_callback_t = Option<
    unsafe extern "C" fn(
        m: *mut server_sasl_mechanism_t,
        stage: sasl_info_callback_stage_t,
        rock: *mut c_void,
    ),
>;

extern "C" {
    pub fn sasl_server_plugin_info(
        mech_list: *const c_char,
        info_cb: sasl_server_info_callback_t,
        info_cb_rock: *mut c_void,
    ) -> c_int;
}

// User canonicalization plugin.

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct sasl_canonuser {
    pub features: c_int,

    pub spare_int1: c_int,

    pub glob_context: *mut c_void,

    pub name: *mut c_char,

    pub canon_user_free:
        Option<unsafe extern "C" fn(glob_context: *mut c_void, utils: *const sasl_utils_t)>,

    pub canon_user_server: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            user: *const c_char,
            len: c_uint,
            flags: c_uint,
            out: *mut c_char,
            out_umax: c_uint,
            out_ulen: *mut c_uint,
        ) -> c_int,
    >,

    pub canon_user_client: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            cparams: *mut sasl_client_params_t,
            user: *const c_char,
            len: c_uint,
            flags: c_uint,
            out: *mut c_char,
            out_max: c_uint,
            out_len: *mut c_uint,
        ) -> c_int,
    >,

    pub spare_fptr1: Option<unsafe extern "C" fn() -> c_int>,
    pub spare_fptr2: Option<unsafe extern "C" fn() -> c_int>,
    pub spare_fptr3: Option<unsafe extern "C" fn() -> c_int>,
}

pub type sasl_canonuser_plug_t = sasl_canonuser;

pub const SASL_CANONUSER_PLUG_VERSION: c_uint = 5;

pub type sasl_canonuser_init_t = Option<
    unsafe extern "C" fn(
        utils: *const sasl_utils_t,
        max_version: c_int,
        out_version: *mut c_int,
        plug: *mut *mut sasl_canonuser_plug_t,
        plugname: *const c_char,
    ) -> c_int,
>;

extern "C" {
    pub fn sasl_canonuser_add_plugin(
        plugname: *const c_char,
        canonuserfunc: sasl_canonuser_init_t,
    ) -> c_int;
}

// Auxiliary property plugin.

#[repr(C)]
#[derive(Copy, Clone)]
pub struct sasl_auxprop_plug {
    pub features: c_int,

    pub spare_int1: c_int,

    pub glob_context: *mut c_void,

    pub auxprop_free:
        Option<unsafe extern "C" fn(glob_context: *mut c_void, utils: *const sasl_utils_t)>,

    pub auxprop_lookup: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            flags: c_uint,
            user: *const c_char,
            ulen: c_uint,
        ) -> c_int,
    >,

    pub name: *mut c_char,

    pub auxprop_store: Option<
        unsafe extern "C" fn(
            glob_context: *mut c_void,
            sparams: *mut sasl_server_params_t,
            ctx: *mut propctx,
            user: *const c_char,
            ulen: c_uint,
        ) -> c_int,
    >,
}

pub type sasl_auxprop_plug_t = sasl_auxprop_plug;

pub const SASL_AUXPROP_OVERRIDE: c_uint = 1;
pub const SASL_AUXPROP_AUTHZID: c_uint = 2;

pub const SASL_AUXPROP_VERIFY_AGAINST_HASH: c_uint = 16;

pub const SASL_AUXPROP_PLUG_VERSION: c_uint = 8;

pub type sasl_auxprop_init_t = Option<
    unsafe extern "C" fn(
        utils: *const sasl_utils_t,
        max_version: c_int,
        out_version: *mut c_int,
        plug: *mut *mut sasl_auxprop_plug_t,
        plugname: *const c_char,
    ) -> c_int,
>;

extern "C" {
    pub fn sasl_auxprop_add_plugin(
        plugname: *const c_char,
        auxpropfunc: sasl_auxprop_init_t,
    ) -> c_int;
}

pub type auxprop_info_callback_t = Option<
    unsafe extern "C" fn(
        m: *mut sasl_auxprop_plug_t,
        stage: sasl_info_callback_stage_t,
        rock: *mut c_void,
    ),
>;

extern "C" {
    // Apparently missing in the libsasl2 shipped with macOS .
    #[cfg(not(all(target_os = "macos", not(feature = "vendored"))))]
    pub fn auxprop_plugin_info(
        mech_list: *const c_char,
        info_cb: auxprop_info_callback_t,
        info_cb_rock: *mut c_void,
    ) -> c_int;
}
