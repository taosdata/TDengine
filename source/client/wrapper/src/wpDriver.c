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

#ifdef WINDOWS
#include <windows.h>
#else
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#endif

#define DRIVER_NATIVE_NAME    "libtaosinternal.so"
#define DRIVER_WSBSOCKET_NAME "libtaosws.so"
#define MAX_PATH_LEN          4096

#if defined(_WIN32)
#define DIRSEP "\\"
#else
#define DIRSEP "/"
#endif

#define LOAD_FUNC(fptr, fname, driverName)  \
  funcName = fname;                         \
  fptr = wpLoadDllFunc(wpHandle, funcName); \
  if (fptr == NULL) goto _OVER;

static int32_t wpDriverInit(EDriverType driverType);
static void    wpDriverCleanup();
static void   *wpLoadDll(const char *fileName);
static void   *wpLoadDllFunc(void *handle, const char *funcName);
static void    wpCloseDll(void *handle);
void           wpDirName(char *name);
static int32_t wpRealPath(char *dirname, char *realPath, int32_t maxlen);
static int32_t wpAppPath(char *path, int32_t maxLen);

EDriverType wpType = DRIVER_NATIVE;
void       *wpHandle = NULL;
int32_t     wpErrorNo = 0;
int32_t     wpErrorStr[64] = {0};

static void *wpLoadDll(const char *fileName) {
#if defined(WINDOWS)
  return NULL;
#elif defined(_TD_DARWIN_64)
  return NULL;
#else
  return dlopen(fileName, RTLD_LAZY);
#endif
}

static void wpCloseDll(void *handle) {
#if defined(WINDOWS)
  return;
#elif defined(_TD_DARWIN_64)
  return;
#else
  dlclose(handle);
#endif
}

static void *wpLoadDllFunc(void *handle, const char *funcName) {
#if defined(WINDOWS)
  return NULL;
#elif defined(_TD_DARWIN_64)
  return NULL;
#else
  return dlsym(handle, funcName);
#endif
}

void wpDirName(char *name) {
#ifdef WINDOWS
  char Drive1[MAX_PATH], Dir1[MAX_PATH];
  _splitpath(name, Drive1, Dir1, NULL, NULL);
  size_t dirNameLen = strlen(Drive1) + strlen(Dir1);
  if (dirNameLen > 0) {
    if (name[dirNameLen - 1] == '/' || name[dirNameLen - 1] == '\\') {
      name[dirNameLen - 1] = 0;
    } else {
      name[dirNameLen] = 0;
    }
  } else {
    name[0] = 0;
  }
#else
  char *end = strrchr(name, '/');
  if (end != NULL) {
    *end = '\0';
  } else {
    name[0] = 0;
  }
#endif
}

static int32_t wpRealPath(char *dirname, char *realPath, int32_t maxlen) {
  char tmp[MAX_PATH_LEN] = {0};
#ifdef WINDOWS
  if (_fullpath(tmp, dirname, maxlen) != NULL) {
#else
  if (realpath(dirname, tmp) != NULL) {
#endif
    if (strlen(tmp) < maxlen) {
      if (realPath == NULL) {
        tstrncpy(dirname, tmp, maxlen);
      } else {
        tstrncpy(realPath, tmp, maxlen);
      }
      return 0;
    }
  }

  return -1;
}

static int32_t wpAppPath(char *path, int32_t maxLen) {
#if !defined(WINDOWS)
  int32_t ret = readlink("/proc/self/exe", path, maxLen - 1);
#else
  int32_t ret = GetModuleFileName(NULL, path, maxLen - 1);
#endif

  if (ret >= 0) {
    wpDirName(path);
    return 0;
  }
  { return ret; }
}

static int32_t wpGetDevelopPath(char *driverPath, const char *driverName) {
#ifdef WINDOWS
  int32_t ret = -1;
#else
  char    appPath[MAX_PATH_LEN] = {0};
  int32_t ret = wpAppPath(appPath, MAX_PATH_LEN);
  if (ret == 0) {
    snprintf(driverPath, MAX_PATH_LEN, "%s%s..%slib%s%s", appPath, DIRSEP, DIRSEP, DIRSEP, driverName);
    ret = wpRealPath(driverPath, NULL, MAX_PATH_LEN);
  }
#endif

  return ret;
}

static int32_t wpGetInstallPath(char *driverPath, const char *driverName) {
#ifdef WINDOWS
  (void)tstrncpy(driverPath, "C:\\Windows\\System32", MAX_PATH_LEN);
#else
  (void)snprintf(driverPath, MAX_PATH_LEN, "/usr/local/taos/driver/%s", driverName);
#endif
  return 0;
}

int32_t wpDriverInit(EDriverType driverType) {
  int32_t     code = 0;
  char        driverPath[MAX_PATH_LEN + 20] = {0};
  const char *driverName = NULL;
  char       *funcName = NULL;

  if (driverType == DRIVER_NATIVE) {
    driverName = DRIVER_NATIVE_NAME;
  } else {
    driverName = DRIVER_WSBSOCKET_NAME;
  }

  if (wpHandle == NULL && wpGetDevelopPath(driverPath, driverName) == 0) {
    wpHandle = taosLoadDll(driverPath);
  }

  if (wpHandle == NULL && wpGetInstallPath(driverPath, driverName) == 0) {
    wpHandle = taosLoadDll(driverPath);
  }

  if (wpHandle == NULL) {
    printf("failed to load driverName at %s since %s\r\n", driverPath, terrstr());
    if (errno != 0) {
      wpErrorNo = errno;
      strncpy(wpErrorStr, strerror(errno), sizeof(wpErrorStr));
    } else {
      // TSDB_CODE_DLL_NOT_FOUND
      wpErrorNo =  0x010B;
      strncpy(wpErrorStr, "dynamic linked library not found", sizeof(wpErrorStr));
    }
    return -1;
  }

  printf("load client driver from %s\r\n", driverPath);

  LOAD_FUNC(fp_taos_cleanup, "fp_taos_cleanup", driverPath);
  LOAD_FUNC(fp_taos_options, "fp_taos_options", driverPath);
  LOAD_FUNC(fp_taos_set_config, "fp_taos_set_config", driverPath);
  LOAD_FUNC(fp_taos_init, "fp_taos_init", driverPath);
  LOAD_FUNC(fp_taos_connect, "fp_taos_connect", driverPath);
  LOAD_FUNC(fp_taos_connect_auth, "fp_taos_connect_auth", driverPath);
  LOAD_FUNC(fp_taos_close, "fp_taos_close", driverPath);
  LOAD_FUNC(fp_taos_data_type, "fp_taos_data_type", driverPath);
  LOAD_FUNC(fp_taos_stmt_init, "fp_taos_stmt_init", driverPath);
  LOAD_FUNC(fp_taos_stmt_init_with_reqid, "fp_taos_stmt_init_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_stmt_init_with_options, "fp_taos_stmt_init_with_options", driverPath);
  LOAD_FUNC(fp_taos_stmt_prepare, "fp_taos_stmt_prepare", driverPath);
  LOAD_FUNC(fp_taos_stmt_set_tbname_tags, "fp_taos_stmt_set_tbname_tags", driverPath);
  LOAD_FUNC(fp_taos_stmt_set_tbname, "fp_taos_stmt_set_tbname", driverPath);
  LOAD_FUNC(fp_taos_stmt_set_tags, "fp_taos_stmt_set_tags", driverPath);
  LOAD_FUNC(fp_taos_stmt_set_sub_tbname, "fp_taos_stmt_set_sub_tbname", driverPath);
  LOAD_FUNC(fp_taos_stmt_get_tag_fields, "fp_taos_stmt_get_tag_fields", driverPath);
  LOAD_FUNC(fp_taos_stmt_get_col_fields, "fp_taos_stmt_get_col_fields", driverPath);
  LOAD_FUNC(fp_taos_stmt_reclaim_fields, "fp_taos_stmt_reclaim_fields", driverPath);
  LOAD_FUNC(fp_taos_stmt_is_insert, "fp_taos_stmt_is_insert", driverPath);
  LOAD_FUNC(fp_taos_stmt_num_params, "fp_taos_stmt_num_params", driverPath);
  LOAD_FUNC(fp_taos_stmt_get_param, "fp_taos_stmt_get_param", driverPath);
  LOAD_FUNC(fp_taos_stmt_bind_param, "fp_taos_stmt_bind_param", driverPath);
  LOAD_FUNC(fp_taos_stmt_bind_param_batch, "fp_taos_stmt_bind_param_batch", driverPath);
  LOAD_FUNC(fp_taos_stmt_bind_single_param_batch, "fp_taos_stmt_bind_single_param_batch", driverPath);
  LOAD_FUNC(fp_taos_stmt_add_batch, "fp_taos_stmt_add_batch", driverPath);
  LOAD_FUNC(fp_taos_stmt_execute, "fp_taos_stmt_execute", driverPath);
  LOAD_FUNC(fp_taos_stmt_use_result, "fp_taos_stmt_use_result", driverPath);
  LOAD_FUNC(fp_taos_stmt_close, "fp_taos_stmt_close", driverPath);
  LOAD_FUNC(fp_taos_stmt_errstr, "fp_taos_stmt_errstr", driverPath);
  LOAD_FUNC(fp_taos_stmt_affected_rows, "fp_taos_stmt_affected_rows", driverPath);
  LOAD_FUNC(fp_taos_stmt_affected_rows_once, "fp_taos_stmt_affected_rows_once", driverPath);
  LOAD_FUNC(fp_taos_stmt2_init, "fp_taos_stmt2_init", driverPath);
  LOAD_FUNC(fp_taos_stmt2_prepare, "fp_taos_stmt2_prepare", driverPath);
  LOAD_FUNC(fp_taos_stmt2_bind_param, "fp_taos_stmt2_bind_param", driverPath);
  LOAD_FUNC(fp_taos_stmt2_exec, "fp_taos_stmt2_exec", driverPath);
  LOAD_FUNC(fp_taos_stmt2_close, "fp_taos_stmt2_close", driverPath);
  LOAD_FUNC(fp_taos_stmt2_is_insert, "fp_taos_stmt2_is_insert", driverPath);
  LOAD_FUNC(fp_taos_stmt2_get_fields, "fp_taos_stmt2_get_fields", driverPath);
  LOAD_FUNC(fp_taos_stmt2_get_stb_fields, "fp_taos_stmt2_get_stb_fields", driverPath);
  LOAD_FUNC(fp_taos_stmt2_free_fields, "fp_taos_stmt2_free_fields", driverPath);
  LOAD_FUNC(fp_taos_stmt2_free_stb_fields, "fp_taos_stmt2_free_stb_fields", driverPath);
  LOAD_FUNC(fp_taos_stmt2_result, "fp_taos_stmt2_result", driverPath);
  LOAD_FUNC(fp_taos_stmt2_error, "fp_taos_stmt2_error", driverPath);
  LOAD_FUNC(fp_taos_query, "fp_taos_query", driverPath);
  LOAD_FUNC(fp_taos_query_with_reqid, "fp_taos_query_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_fetch_row, "fp_taos_fetch_row", driverPath);
  LOAD_FUNC(fp_taos_result_precision, "fp_taos_result_precision", driverPath);
  LOAD_FUNC(fp_taos_free_result, "fp_taos_free_result", driverPath);
  LOAD_FUNC(fp_taos_kill_query, "fp_taos_kill_query", driverPath);
  LOAD_FUNC(fp_taos_field_count, "fp_taos_field_count", driverPath);
  LOAD_FUNC(fp_taos_num_fields, "fp_taos_num_fields", driverPath);
  LOAD_FUNC(fp_taos_affected_rows, "fp_taos_affected_rows", driverPath);
  LOAD_FUNC(fp_taos_affected_rows64, "fp_taos_affected_rows64", driverPath);
  LOAD_FUNC(fp_taos_fetch_fields, "fp_taos_fetch_fields", driverPath);
  LOAD_FUNC(fp_taos_select_db, "fp_taos_select_db", driverPath);
  LOAD_FUNC(fp_taos_print_row, "fp_taos_print_row", driverPath);
  LOAD_FUNC(fp_taos_print_row_with_size, "fp_taos_print_row_with_size", driverPath);
  LOAD_FUNC(fp_taos_stop_query, "fp_taos_stop_query", driverPath);
  LOAD_FUNC(fp_taos_is_null, "fp_taos_is_null", driverPath);
  LOAD_FUNC(fp_taos_is_null_by_column, "fp_taos_is_null_by_column", driverPath);
  LOAD_FUNC(fp_taos_is_update_query, "fp_taos_is_update_query", driverPath);
  LOAD_FUNC(fp_taos_fetch_block, "fp_taos_fetch_block", driverPath);
  LOAD_FUNC(fp_taos_fetch_block_s, "fp_taos_fetch_block_s", driverPath);
  LOAD_FUNC(fp_taos_fetch_raw_block, "fp_taos_fetch_raw_block", driverPath);
  LOAD_FUNC(fp_taos_get_column_data_offset, "fp_taos_get_column_data_offset", driverPath);
  LOAD_FUNC(fp_taos_validate_sql, "fp_taos_validate_sql", driverPath);
  LOAD_FUNC(fp_taos_reset_current_db, "fp_taos_reset_current_db", driverPath);
  LOAD_FUNC(fp_taos_fetch_lengths, "fp_taos_fetch_lengths", driverPath);
  LOAD_FUNC(fp_taos_result_block, "fp_taos_result_block", driverPath);
  LOAD_FUNC(fp_taos_get_server_info, "fp_taos_get_server_info", driverPath);
  LOAD_FUNC(fp_taos_get_client_info, "fp_taos_get_client_info", driverPath);
  LOAD_FUNC(fp_taos_get_current_db, "fp_taos_get_current_db", driverPath);
  LOAD_FUNC(fp_taos_errstr, "fp_taos_errstr", driverPath);
  LOAD_FUNC(fp_taos_errno, "fp_taos_errno", driverPath);
  LOAD_FUNC(fp_taos_query_a, "fp_taos_query_a", driverPath);
  LOAD_FUNC(fp_taos_query_a_with_reqid, "fp_taos_query_a_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_fetch_rows_a, "fp_taos_fetch_rows_a", driverPath);
  LOAD_FUNC(fp_taos_fetch_raw_block_a, "fp_taos_fetch_raw_block_a", driverPath);
  LOAD_FUNC(fp_taos_get_raw_block, "fp_taos_get_raw_block", driverPath);
  LOAD_FUNC(fp_taos_get_db_route_info, "fp_taos_get_db_route_info", driverPath);
  LOAD_FUNC(fp_taos_get_table_vgId, "fp_taos_get_table_vgId", driverPath);
  LOAD_FUNC(fp_taos_get_tables_vgId, "fp_taos_get_tables_vgId", driverPath);
  LOAD_FUNC(fp_taos_load_table_info, "fp_taos_load_table_info", driverPath);
  LOAD_FUNC(fp_taos_set_hb_quit, "fp_taos_set_hb_quit", driverPath);
  LOAD_FUNC(fp_taos_set_notify_cb, "fp_taos_set_notify_cb", driverPath);
  LOAD_FUNC(fp_taos_fetch_whitelist_a, "fp_taos_fetch_whitelist_a", driverPath);
  LOAD_FUNC(fp_taos_set_conn_mode, "fp_taos_set_conn_mode", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert, "fp_taos_schemaless_insert", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_with_reqid, "fp_taos_schemaless_insert_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_raw, "fp_taos_schemaless_insert_raw", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_raw_with_reqid, "fp_taos_schemaless_insert_raw_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_ttl, "fp_taos_schemaless_insert_ttl", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_ttl_with_reqid, "fp_taos_schemaless_insert_ttl_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_raw_ttl, "fp_taos_schemaless_insert_raw_ttl", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_raw_ttl_with_reqid, "fp_taos_schemaless_insert_raw_ttl_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key,
            "fp_taos_schemaless_insert_raw_ttl_with_reqid_tbname_key", driverPath);
  LOAD_FUNC(fp_taos_schemaless_insert_ttl_with_reqid_tbname_key, "fp_taos_schemaless_insert_ttl_with_reqid_tbname_key",
            driverPath);
  LOAD_FUNC(fp_tmq_conf_new, "fp_tmq_conf_new", driverPath);
  LOAD_FUNC(fp_tmq_conf_set, "fp_tmq_conf_set", driverPath);
  LOAD_FUNC(fp_tmq_conf_destroy, "fp_tmq_conf_destroy", driverPath);
  LOAD_FUNC(fp_tmq_conf_set_auto_commit_cb, "fp_tmq_conf_set_auto_commit_cb", driverPath);
  LOAD_FUNC(fp_tmq_list_new, "fp_tmq_list_new", driverPath);
  LOAD_FUNC(fp_tmq_list_append, "fp_tmq_list_append", driverPath);
  LOAD_FUNC(fp_tmq_list_destroy, "fp_tmq_list_destroy", driverPath);
  LOAD_FUNC(fp_tmq_list_get_size, "fp_tmq_list_get_size", driverPath);
  LOAD_FUNC(fp_tmq_list_to_c_array, "fp_tmq_list_to_c_array", driverPath);
  LOAD_FUNC(fp_tmq_consumer_new, "fp_tmq_consumer_new", driverPath);
  LOAD_FUNC(fp_tmq_subscribe, "fp_tmq_subscribe", driverPath);
  LOAD_FUNC(fp_tmq_unsubscribe, "fp_tmq_unsubscribe", driverPath);
  LOAD_FUNC(fp_tmq_subscription, "fp_tmq_subscription", driverPath);
  LOAD_FUNC(fp_tmq_consumer_poll, "fp_tmq_consumer_poll", driverPath);
  LOAD_FUNC(fp_tmq_consumer_close, "fp_tmq_consumer_close", driverPath);
  LOAD_FUNC(fp_tmq_commit_sync, "fp_tmq_commit_sync", driverPath);
  LOAD_FUNC(fp_tmq_commit_async, "fp_tmq_commit_async", driverPath);
  LOAD_FUNC(fp_tmq_commit_offset_sync, "fp_tmq_commit_offset_sync", driverPath);
  LOAD_FUNC(fp_tmq_commit_offset_async, "fp_tmq_commit_offset_async", driverPath);
  LOAD_FUNC(fp_tmq_get_topic_assignment, "fp_tmq_get_topic_assignment", driverPath);
  LOAD_FUNC(fp_tmq_free_assignment, "fp_tmq_free_assignment", driverPath);
  LOAD_FUNC(fp_tmq_offset_seek, "fp_tmq_offset_seek", driverPath);
  LOAD_FUNC(fp_tmq_position, "fp_tmq_position", driverPath);
  LOAD_FUNC(fp_tmq_committed, "fp_tmq_committed", driverPath);
  LOAD_FUNC(fp_tmq_get_connect, "fp_tmq_get_connect", driverPath);
  LOAD_FUNC(fp_tmq_get_table_name, "fp_tmq_get_table_name", driverPath);
  LOAD_FUNC(fp_tmq_get_res_type, "fp_tmq_get_res_type", driverPath);
  LOAD_FUNC(fp_tmq_get_topic_name, "fp_tmq_get_topic_name", driverPath);
  LOAD_FUNC(fp_tmq_get_db_name, "fp_tmq_get_db_name", driverPath);
  LOAD_FUNC(fp_tmq_get_vgroup_id, "fp_tmq_get_vgroup_id", driverPath);
  LOAD_FUNC(fp_tmq_get_vgroup_offset, "fp_tmq_get_vgroup_offset", driverPath);
  LOAD_FUNC(fp_tmq_err2str, "fp_tmq_err2str", driverPath);
  LOAD_FUNC(fp_tmq_get_raw, "fp_tmq_get_raw", driverPath);
  LOAD_FUNC(fp_tmq_write_raw, "fp_tmq_write_raw", driverPath);
  LOAD_FUNC(fp_taos_write_raw_block, "fp_taos_write_raw_block", driverPath);
  LOAD_FUNC(fp_taos_write_raw_block_with_reqid, "fp_taos_write_raw_block_with_reqid", driverPath);
  LOAD_FUNC(fp_taos_write_raw_block_with_fields, "fp_taos_write_raw_block_with_fields", driverPath);
  LOAD_FUNC(fp_taos_write_raw_block_with_fields_with_reqid, "fp_taos_write_raw_block_with_fields_with_reqid",
            driverPath);
  LOAD_FUNC(fp_tmq_free_raw, "fp_tmq_free_raw", driverPath);
  LOAD_FUNC(fp_tmq_get_json_meta, "fp_tmq_get_json_meta", driverPath);
  LOAD_FUNC(fp_tmq_free_json_meta, "fp_tmq_free_json_meta", driverPath);
  LOAD_FUNC(fp_taos_check_server_status, "fp_taos_check_server_status", driverPath);
  LOAD_FUNC(fp_getBuildInfo, "fp_getBuildInfo", driverPath);

  code = 0;

_OVER:
  if (code != 0) {
    printf("failed to load func:%s from %s\r\n", funcName, driverPath);
    if (errno != 0) {
      wpErrorNo = errno;
      strncpy(wpErrorStr, strerror(errno), sizeof(wpErrorStr));
    } else {
      // TSDB_CODE_DLL_FUNC_NOT_FOUND
      strncpy(wpErrorStr, "function in dynamic linked library not found", sizeof(wpErrorStr));
      wpErrorNo = 0x010C;
    }
    wpDriverCleanup(wpHandle);
    return -1;
  }

  return code;
}

void wpDriverCleanup() {
  if (wpHandle != NULL) {
    taosCloseDll(wpHandle);
  }
}
