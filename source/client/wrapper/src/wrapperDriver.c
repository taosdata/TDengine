/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "wrapper.h"
#include "wrapperHint.h"

#ifdef WINDOWS
#define DRIVER_NATIVE_NAME    "taosnative.dll"
#define DRIVER_WSBSOCKET_NAME "taosws.dll"
#elif defined(DARWIN)
#define DRIVER_NATIVE_NAME    "libtaosnative.dylib"
#define DRIVER_WSBSOCKET_NAME "libtaosws.dylib"
#else
#define DRIVER_NATIVE_NAME    "libtaosnative.so"
#define DRIVER_WSBSOCKET_NAME "libtaosws.so"
#endif

#define LOAD_FUNC(fptr, fname)                \
  funcName = fname;                           \
  fptr = taosLoadDllFunc(tsDriver, funcName); \
  if (fptr == NULL) goto _OVER;

#ifdef WEBSOCKET
EDriverType tsDriverType = DRIVER_NATIVE;  // todo simon
#else
EDriverType tsDriverType = DRIVER_NATIVE;
#endif

void *tsDriver = NULL;

static int32_t taosGetDevelopPath(char *driverPath, const char *driverName) {
  char    appPath[PATH_MAX] = {0};
  int32_t ret = taosAppPath(appPath, PATH_MAX);
  if (ret == 0) {
    snprintf(driverPath, PATH_MAX, "%s%s..%slib%s%s", appPath, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP, driverName);
    ret = taosRealPath(driverPath, NULL, PATH_MAX);
  }

  return ret;
}

int32_t taosDriverInit(EDriverType driverType) {
  int32_t     code = -1;
  char        driverPath[PATH_MAX + 32] = {0};
  const char *driverName = NULL;
  const char *funcName = NULL;

  if (driverType == DRIVER_NATIVE) {
    driverName = DRIVER_NATIVE_NAME;
  } else {
    driverName = DRIVER_WSBSOCKET_NAME;
  }

  // load from develop build path
  if (tsDriver == NULL && taosGetDevelopPath(driverPath, driverName) == 0) {
    tsDriver = taosLoadDll(driverPath);
  }  

  // load from system path
  if (tsDriver == NULL) {
    tsDriver = taosLoadDll(driverName);
  }

  // load from install path on mac
#if defined(DARWIN)
  if (tsDriver == NULL) {
    snprintf(driverPath, PATH_MAX, "/usr/local/lib/%s", driverName);
    tsDriver = taosLoadDll(driverPath);
  }
#endif

  if (tsDriver == NULL) {
    printf("failed to load %s since %s [0x%X]\r\n", driverName, terrstr(), terrno);
    showWrapperHint(terrno);
    return code;
  }

  // printf("load driver from %s\r\n", driverPath);
  LOAD_FUNC(fp_taos_set_config, "taos_set_config");

  LOAD_FUNC(fp_taos_init, "taos_init");
  LOAD_FUNC(fp_taos_cleanup, "taos_cleanup");
  LOAD_FUNC(fp_taos_options, "taos_options");
  LOAD_FUNC(fp_taos_options_connection, "taos_options_connection");
  LOAD_FUNC(fp_taos_connect, "taos_connect");
  LOAD_FUNC(fp_taos_connect_auth, "taos_connect_auth");
  LOAD_FUNC(fp_taos_close, "taos_close");

  LOAD_FUNC(fp_taos_data_type, "taos_data_type");

  LOAD_FUNC(fp_taos_stmt_init, "taos_stmt_init");
  LOAD_FUNC(fp_taos_stmt_init_with_reqid, "taos_stmt_init_with_reqid");
  LOAD_FUNC(fp_taos_stmt_init_with_options, "taos_stmt_init_with_options");
  LOAD_FUNC(fp_taos_stmt_prepare, "taos_stmt_prepare");
  LOAD_FUNC(fp_taos_stmt_set_tbname_tags, "taos_stmt_set_tbname_tags");
  LOAD_FUNC(fp_taos_stmt_set_tbname, "taos_stmt_set_tbname");
  LOAD_FUNC(fp_taos_stmt_set_tags, "taos_stmt_set_tags");
  LOAD_FUNC(fp_taos_stmt_set_sub_tbname, "taos_stmt_set_sub_tbname");
  LOAD_FUNC(fp_taos_stmt_get_tag_fields, "taos_stmt_get_tag_fields");
  LOAD_FUNC(fp_taos_stmt_get_col_fields, "taos_stmt_get_col_fields");
  LOAD_FUNC(fp_taos_stmt_reclaim_fields, "taos_stmt_reclaim_fields");

  LOAD_FUNC(fp_taos_stmt_is_insert, "taos_stmt_is_insert");
  LOAD_FUNC(fp_taos_stmt_num_params, "taos_stmt_num_params");
  LOAD_FUNC(fp_taos_stmt_get_param, "taos_stmt_get_param");
  LOAD_FUNC(fp_taos_stmt_bind_param, "taos_stmt_bind_param");
  LOAD_FUNC(fp_taos_stmt_bind_param_batch, "taos_stmt_bind_param_batch");
  LOAD_FUNC(fp_taos_stmt_bind_single_param_batch, "taos_stmt_bind_single_param_batch");
  LOAD_FUNC(fp_taos_stmt_add_batch, "taos_stmt_add_batch");
  LOAD_FUNC(fp_taos_stmt_execute, "taos_stmt_execute");
  LOAD_FUNC(fp_taos_stmt_use_result, "taos_stmt_use_result");
  LOAD_FUNC(fp_taos_stmt_close, "taos_stmt_close");
  LOAD_FUNC(fp_taos_stmt_errstr, "taos_stmt_errstr");
  LOAD_FUNC(fp_taos_stmt_affected_rows, "taos_stmt_affected_rows");
  LOAD_FUNC(fp_taos_stmt_affected_rows_once, "taos_stmt_affected_rows_once");

  LOAD_FUNC(fp_taos_stmt2_init, "taos_stmt2_init");
  LOAD_FUNC(fp_taos_stmt2_prepare, "taos_stmt2_prepare");
  LOAD_FUNC(fp_taos_stmt2_bind_param, "taos_stmt2_bind_param");
  LOAD_FUNC(fp_taos_stmt2_bind_param_a, "taos_stmt2_bind_param_a");
  LOAD_FUNC(fp_taos_stmt2_exec, "taos_stmt2_exec");
  LOAD_FUNC(fp_taos_stmt2_close, "taos_stmt2_close");
  LOAD_FUNC(fp_taos_stmt2_is_insert, "taos_stmt2_is_insert");
  LOAD_FUNC(fp_taos_stmt2_get_fields, "taos_stmt2_get_fields");
  LOAD_FUNC(fp_taos_stmt2_free_fields, "taos_stmt2_free_fields");
  LOAD_FUNC(fp_taos_stmt2_result, "taos_stmt2_result");
  LOAD_FUNC(fp_taos_stmt2_error, "taos_stmt2_error");

  LOAD_FUNC(fp_taos_query, "taos_query");
  LOAD_FUNC(fp_taos_query_with_reqid, "taos_query_with_reqid");

  LOAD_FUNC(fp_taos_fetch_row, "taos_fetch_row");
  LOAD_FUNC(fp_taos_result_precision, "taos_result_precision");
  LOAD_FUNC(fp_taos_free_result, "taos_free_result");
  LOAD_FUNC(fp_taos_kill_query, "taos_kill_query");
  LOAD_FUNC(fp_taos_field_count, "taos_field_count");
  LOAD_FUNC(fp_taos_num_fields, "taos_num_fields");
  LOAD_FUNC(fp_taos_affected_rows, "taos_affected_rows");
  LOAD_FUNC(fp_taos_affected_rows64, "taos_affected_rows64");

  LOAD_FUNC(fp_taos_fetch_fields, "taos_fetch_fields");
  LOAD_FUNC(fp_taos_fetch_fields_e, "taos_fetch_fields_e");
  LOAD_FUNC(fp_taos_select_db, "taos_select_db");
  LOAD_FUNC(fp_taos_print_row, "taos_print_row");
  LOAD_FUNC(fp_taos_print_row_with_size, "taos_print_row_with_size");
  LOAD_FUNC(fp_taos_stop_query, "taos_stop_query");
  LOAD_FUNC(fp_taos_is_null, "taos_is_null");
  LOAD_FUNC(fp_taos_is_null_by_column, "taos_is_null_by_column");
  LOAD_FUNC(fp_taos_is_update_query, "taos_is_update_query");
  LOAD_FUNC(fp_taos_fetch_block, "taos_fetch_block");
  LOAD_FUNC(fp_taos_fetch_block_s, "taos_fetch_block_s");
  LOAD_FUNC(fp_taos_fetch_raw_block, "taos_fetch_raw_block");
  LOAD_FUNC(fp_taos_get_column_data_offset, "taos_get_column_data_offset");
  LOAD_FUNC(fp_taos_validate_sql, "taos_validate_sql");
  LOAD_FUNC(fp_taos_reset_current_db, "taos_reset_current_db");

  LOAD_FUNC(fp_taos_fetch_lengths, "taos_fetch_lengths");
  LOAD_FUNC(fp_taos_result_block, "taos_result_block");

  LOAD_FUNC(fp_taos_get_server_info, "taos_get_server_info");
  LOAD_FUNC(fp_taos_get_client_info, "taos_get_client_info");
  LOAD_FUNC(fp_taos_get_current_db, "taos_get_current_db");

  LOAD_FUNC(fp_taos_errstr, "taos_errstr");
  LOAD_FUNC(fp_taos_errno, "taos_errno");

  LOAD_FUNC(fp_taos_query_a, "taos_query_a");
  LOAD_FUNC(fp_taos_query_a_with_reqid, "taos_query_a_with_reqid");
  LOAD_FUNC(fp_taos_fetch_rows_a, "taos_fetch_rows_a");
  LOAD_FUNC(fp_taos_fetch_raw_block_a, "taos_fetch_raw_block_a");
  LOAD_FUNC(fp_taos_get_raw_block, "taos_get_raw_block");

  LOAD_FUNC(fp_taos_get_db_route_info, "taos_get_db_route_info");
  LOAD_FUNC(fp_taos_get_table_vgId, "taos_get_table_vgId");
  LOAD_FUNC(fp_taos_get_tables_vgId, "taos_get_tables_vgId");

  LOAD_FUNC(fp_taos_load_table_info, "taos_load_table_info");

  LOAD_FUNC(fp_taos_set_hb_quit, "taos_set_hb_quit");

  LOAD_FUNC(fp_taos_set_notify_cb, "taos_set_notify_cb");

  LOAD_FUNC(fp_taos_fetch_whitelist_a, "taos_fetch_whitelist_a");

  LOAD_FUNC(fp_taos_fetch_whitelist_dual_stack_a, "taos_fetch_whitelist_dual_stack_a");

  LOAD_FUNC(fp_taos_set_conn_mode, "taos_set_conn_mode");

  LOAD_FUNC(fp_taos_schemaless_insert, "taos_schemaless_insert");
  LOAD_FUNC(fp_taos_schemaless_insert_with_reqid, "taos_schemaless_insert_with_reqid");
  LOAD_FUNC(fp_taos_schemaless_insert_raw, "taos_schemaless_insert_raw");
  LOAD_FUNC(fp_taos_schemaless_insert_raw_with_reqid, "taos_schemaless_insert_raw_with_reqid");
  LOAD_FUNC(fp_taos_schemaless_insert_ttl, "taos_schemaless_insert_ttl");
  LOAD_FUNC(fp_taos_schemaless_insert_ttl_with_reqid, "taos_schemaless_insert_ttl_with_reqid");
  LOAD_FUNC(fp_taos_schemaless_insert_raw_ttl, "taos_schemaless_insert_raw_ttl");
  LOAD_FUNC(fp_taos_schemaless_insert_raw_ttl_with_reqid, "taos_schemaless_insert_raw_ttl_with_reqid");
  LOAD_FUNC(fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key,
            "taos_schemaless_insert_raw_ttl_with_reqid_tbname_key");
  LOAD_FUNC(fp_taos_schemaless_insert_ttl_with_reqid_tbname_key, "taos_schemaless_insert_ttl_with_reqid_tbname_key");

  LOAD_FUNC(fp_tmq_conf_new, "tmq_conf_new");
  LOAD_FUNC(fp_tmq_conf_set, "tmq_conf_set");
  LOAD_FUNC(fp_tmq_conf_destroy, "tmq_conf_destroy");
  LOAD_FUNC(fp_tmq_conf_set_auto_commit_cb, "tmq_conf_set_auto_commit_cb");

  LOAD_FUNC(fp_tmq_list_new, "tmq_list_new");
  LOAD_FUNC(fp_tmq_list_append, "tmq_list_append");
  LOAD_FUNC(fp_tmq_list_destroy, "tmq_list_destroy");
  LOAD_FUNC(fp_tmq_list_get_size, "tmq_list_get_size");
  LOAD_FUNC(fp_tmq_list_to_c_array, "tmq_list_to_c_array");

  LOAD_FUNC(fp_tmq_consumer_new, "tmq_consumer_new");
  LOAD_FUNC(fp_tmq_subscribe, "tmq_subscribe");
  LOAD_FUNC(fp_tmq_unsubscribe, "tmq_unsubscribe");
  LOAD_FUNC(fp_tmq_subscription, "tmq_subscription");
  LOAD_FUNC(fp_tmq_consumer_poll, "tmq_consumer_poll");
  LOAD_FUNC(fp_tmq_consumer_close, "tmq_consumer_close");
  LOAD_FUNC(fp_tmq_commit_sync, "tmq_commit_sync");
  LOAD_FUNC(fp_tmq_commit_async, "tmq_commit_async");
  LOAD_FUNC(fp_tmq_commit_offset_sync, "tmq_commit_offset_sync");
  LOAD_FUNC(fp_tmq_commit_offset_async, "tmq_commit_offset_async");
  LOAD_FUNC(fp_tmq_get_topic_assignment, "tmq_get_topic_assignment");
  LOAD_FUNC(fp_tmq_free_assignment, "tmq_free_assignment");
  LOAD_FUNC(fp_tmq_offset_seek, "tmq_offset_seek");
  LOAD_FUNC(fp_tmq_position, "tmq_position");
  LOAD_FUNC(fp_tmq_committed, "tmq_committed");

  LOAD_FUNC(fp_tmq_get_connect, "tmq_get_connect");
  LOAD_FUNC(fp_tmq_get_table_name, "tmq_get_table_name");
  LOAD_FUNC(fp_tmq_get_res_type, "tmq_get_res_type");
  LOAD_FUNC(fp_tmq_get_topic_name, "tmq_get_topic_name");
  LOAD_FUNC(fp_tmq_get_db_name, "tmq_get_db_name");
  LOAD_FUNC(fp_tmq_get_vgroup_id, "tmq_get_vgroup_id");
  LOAD_FUNC(fp_tmq_get_vgroup_offset, "tmq_get_vgroup_offset");
  LOAD_FUNC(fp_tmq_err2str, "tmq_err2str");

  LOAD_FUNC(fp_tmq_get_raw, "tmq_get_raw");
  LOAD_FUNC(fp_tmq_write_raw, "tmq_write_raw");
  LOAD_FUNC(fp_taos_write_raw_block, "taos_write_raw_block");
  LOAD_FUNC(fp_taos_write_raw_block_with_reqid, "taos_write_raw_block_with_reqid");
  LOAD_FUNC(fp_taos_write_raw_block_with_fields, "taos_write_raw_block_with_fields");
  LOAD_FUNC(fp_taos_write_raw_block_with_fields_with_reqid, "taos_write_raw_block_with_fields_with_reqid");
  LOAD_FUNC(fp_tmq_free_raw, "tmq_free_raw");

  LOAD_FUNC(fp_tmq_get_json_meta, "tmq_get_json_meta");
  LOAD_FUNC(fp_tmq_free_json_meta, "tmq_free_json_meta");

  LOAD_FUNC(fp_taos_check_server_status, "taos_check_server_status");
  LOAD_FUNC(fp_taos_write_crashinfo, "taos_write_crashinfo");
  LOAD_FUNC(fp_getBuildInfo, "getBuildInfo");

  code = 0;

_OVER:
  if (code != 0) {
    printf("failed to load function %s from %s since %s [0x%X]\r\n", funcName, driverPath, terrstr(), terrno);
    showWrapperHint(terrno);
    taosDriverCleanup();
  }

  return code;
}

void taosDriverCleanup() {
  if (tsDriver != NULL) {
    taosCloseDll(tsDriver);
    tsDriver = NULL;
  }
}
