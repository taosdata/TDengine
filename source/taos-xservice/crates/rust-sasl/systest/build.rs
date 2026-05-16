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

use std::env;
use std::path::PathBuf;

fn main() {
    let mut cfg = ctest::TestGenerator::new();
    if let Ok(root) = env::var("DEP_SASL2_ROOT") {
        cfg.include(PathBuf::from(root).join("include"));
    }

    cfg.header("sasl/prop.h")
        .header("sasl/sasl.h")
        .header("sasl/saslplug.h")
        .header("sasl/saslutil.h")
        .type_name(|ty, is_struct, is_union| match ty {
            "MD5_CTX" => ty.to_string(),
            "iovec" => "struct iovec".into(),
            _ if is_struct => format!("struct {}", ty),
            _ if is_union => format!("union {}", ty),
            _ => ty.to_string(),
        })
        .field_name(|s, f| match (s, f) {
            ("sasl_callback", "proc_") => "proc".into(),
            _ => f.to_string(),
        })
        .skip_struct(|s| match s {
            "propctx" | "sasl_conn" | "sasl_rand_s" => true,
            _ => false,
        })
        .skip_type(|t| match t {
            "sasl_conn_t"
            | "sasl_rand_t"
            | "sasl_malloc_t"
            | "sasl_calloc_t"
            | "sasl_realloc_t"
            | "sasl_free_t"
            | "sasl_mutex_alloc_t"
            | "sasl_mutex_lock_t"
            | "sasl_mutex_unlock_t"
            | "sasl_mutex_free_t"
            | "sasl_getopt_t"
            | "sasl_log_t"
            | "sasl_getpath_t"
            | "sasl_verifyfile_t"
            | "sasl_getconfpath_t"
            | "sasl_getsimple_t"
            | "sasl_getsecret_t"
            | "sasl_chalprompt_t"
            | "sasl_getrealm_t"
            | "sasl_authorize_t"
            | "sasl_server_userdb_checkpass_t"
            | "sasl_server_userdb_setpass_t"
            | "sasl_canon_user_t"
            | "sasl_getcallback_t"
            | "sasl_client_plug_init_t"
            | "sasl_client_info_callback_t"
            | "sasl_logmsg_p"
            | "sasl_server_plug_init_t"
            | "sasl_server_info_callback_t"
            | "sasl_canonuser_init_t"
            | "sasl_auxprop_init_t"
            | "auxprop_info_callback_t"
            // TODO(benesch): sasl_secret_t might be legitimately broken. Not
            // clear how to handle structs with variable-length arrays.
            | "sasl_secret_t" => true,
            _ => false,
        })
        .skip_field_type(|s, f| match (s, f) {
            ("sasl_utils", "getopt")
            | ("sasl_utils", "malloc")
            | ("sasl_utils", "calloc")
            | ("sasl_utils", "realloc")
            | ("sasl_utils", "free")
            | ("sasl_utils", "mutex_alloc")
            | ("sasl_utils", "mutex_lock")
            | ("sasl_utils", "mutex_unlock")
            | ("sasl_utils", "mutex_free")
            | ("sasl_utils", "getcallback") => true,
            _ => false,
        })
        .generate("../sasl2-sys/src/lib.rs", "all.rs");
}
