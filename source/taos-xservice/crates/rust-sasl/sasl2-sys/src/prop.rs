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

//! Property request/response management routines.

use libc::{c_char, c_uint};

pub const PROP_DEFAULT: c_uint = 4;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct propval {
    pub name: *const c_char,
    pub values: *mut *const c_char,
    pub nvalues: c_uint,
    pub valsize: c_uint,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct propctx {
    _unused: [u8; 0],
}

extern "C" {
    pub fn prop_new(estimate: c_uint) -> *mut propctx;

    pub fn prop_dup(src_ctx: *mut propctx, dst_ctx: *mut *mut propctx) -> ::std::os::raw::c_int;

    pub fn prop_request(ctx: *mut propctx, names: *mut *const c_char) -> ::std::os::raw::c_int;

    pub fn prop_get(ctx: *mut propctx) -> *const propval;

    pub fn prop_getnames(
        ctx: *mut propctx,
        names: *mut *const c_char,
        vals: *mut propval,
    ) -> ::std::os::raw::c_int;

    pub fn prop_clear(ctx: *mut propctx, requests: ::std::os::raw::c_int);

    pub fn prop_erase(ctx: *mut propctx, name: *const c_char);

    pub fn prop_dispose(ctx: *mut *mut propctx);

    pub fn prop_format(
        ctx: *mut propctx,
        sep: *const c_char,
        seplen: ::std::os::raw::c_int,
        outbuf: *mut c_char,
        outmax: c_uint,
        outlen: *mut c_uint,
    ) -> ::std::os::raw::c_int;

    pub fn prop_set(
        ctx: *mut propctx,
        name: *const c_char,
        value: *const c_char,
        vallen: ::std::os::raw::c_int,
    ) -> ::std::os::raw::c_int;

    pub fn prop_setvals(
        ctx: *mut propctx,
        name: *const c_char,
        values: *mut *const c_char,
    ) -> ::std::os::raw::c_int;
}
