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

//! HMAC MD5 utilities.

use super::md5::MD5_CTX;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct HMAC_MD5_CTX_s {
    pub ictx: MD5_CTX,
    pub octx: MD5_CTX,
}

pub type HMAC_MD5_CTX = HMAC_MD5_CTX_s;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct HMAC_MD5_STATE_s {
    pub istate: [u32; 4],
    pub ostate: [u32; 4],
}

pub type HMAC_MD5_STATE = HMAC_MD5_STATE_s;
