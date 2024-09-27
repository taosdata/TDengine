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

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use libc::c_int;

#[test]
fn test_version() {
    let mut implementation: *const c_char = ptr::null();
    let mut version_string: *const c_char = ptr::null();
    let mut version_major: c_int = 0;
    let mut version_minor: c_int = 0;
    let mut version_step: c_int = 0;
    let mut version_patch: c_int = 0;
    unsafe {
        sasl2_sys::sasl::sasl_version_info(
            &mut implementation,
            &mut version_string,
            &mut version_major,
            &mut version_minor,
            &mut version_step,
            &mut version_patch,
        );
        println!(
            "implementation={:?} version_string={:?} version={}.{}.{}-{}",
            CStr::from_ptr(implementation),
            CStr::from_ptr(version_string),
            version_major,
            version_minor,
            version_step,
            version_patch
        );
    }
}

#[test]
fn test_readme_deps() {
    version_sync::assert_markdown_deps_updated!("../README.md");
}

#[test]
fn test_html_root_url() {
    version_sync::assert_html_root_url_updated!("src/lib.rs");
}
