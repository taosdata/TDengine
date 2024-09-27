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

//! SASL utility functions.

use libc::{c_char, c_int, c_uint};

use super::sasl::{sasl_conn_t, sasl_rand_t};

extern "C" {
    pub fn sasl_decode64(
        in_: *const c_char,
        inlen: c_uint,
        out: *mut c_char,
        outmax: c_uint,
        outlen: *mut c_uint,
    ) -> c_int;

    pub fn sasl_encode64(
        in_: *const c_char,
        inlen: c_uint,
        out: *mut c_char,
        outmax: c_uint,
        outlen: *mut c_uint,
    ) -> c_int;

    pub fn sasl_mkchal(
        conn: *mut sasl_conn_t,
        buf: *mut c_char,
        maxlen: c_uint,
        hostflag: c_uint,
    ) -> c_int;

    pub fn sasl_utf8verify(str: *const c_char, len: c_uint) -> c_int;

    pub fn sasl_randcreate(rpool: *mut *mut sasl_rand_t) -> c_int;

    pub fn sasl_randfree(rpool: *mut *mut sasl_rand_t);

    pub fn sasl_randseed(rpool: *mut sasl_rand_t, seed: *const c_char, len: c_uint);

    pub fn sasl_rand(rpool: *mut sasl_rand_t, buf: *mut c_char, len: c_uint);

    pub fn sasl_churn(rpool: *mut sasl_rand_t, data: *const c_char, len: c_uint);

    pub fn sasl_erasebuffer(pass: *mut c_char, len: c_uint);

    // Apparently missing in the libsasl2 shipped with macOS .
    #[cfg(not(all(target_os = "macos", not(feature = "vendored"))))]
    pub fn sasl_strlower(val: *mut c_char) -> *mut c_char;

    pub fn sasl_config_init(filename: *const c_char) -> c_int;

    pub fn sasl_config_done();
}
