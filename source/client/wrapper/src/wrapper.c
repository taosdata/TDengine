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
#include "os.h"

static void *tsDriver = NULL;
static int (*fptr_taos_init)(void) = NULL;
static void (*fptr_taos_cleanup)(void) = NULL;
static TAOS *(*fptr_taos_connect)(const char *ip, const char *user, const char *pass, const char *db,
                                  uint16_t port) = NULL;
static TAOS *(*fptr_taos_connect_auth)(const char *ip, const char *user, const char *auth, const char *db,
                                       uint16_t port) = NULL;
static void (*fptr_taos_close)(TAOS *taos) = NULL;
static TAOS_RES *(*fptr_taos_query)(TAOS *taos, const char *sql) = NULL;
static TAOS_ROW (*fptr_taos_fetch_row)(TAOS_RES *res) = NULL;
static int (*fptr_taos_result_precision)(TAOS_RES *res) = NULL;
static void (*fptr_taos_free_result)(TAOS_RES *res) = NULL;
static void (*fptr_taos_kill_query)(TAOS *taos) = NULL;
static int (*fptr_taos_field_count)(TAOS_RES *res) = NULL;
static int (*fptr_taos_num_fields)(TAOS_RES *res) = NULL;
static int (*fptr_taos_affected_rows)(TAOS_RES *res) = NULL;
static int64_t (*fptr_taos_affected_rows64)(TAOS_RES *res) = NULL;
static TAOS_FIELD *(*fptr_taos_fetch_fields)(TAOS_RES *res) = NULL;
static int (*fptr_taos_select_db)(TAOS *taos, const char *db) = NULL;
static int (*fptr_taos_print_row)(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) = NULL;
static int *(*fptr_taos_fetch_lengths)(TAOS_RES *res) = NULL;
static void (*fptr_taos_fetch_rows_a)(TAOS_RES *res, __taos_async_fn_t fp, void *param) = NULL;
static int (*fptr_taos_set_conn_mode)(TAOS *taos, int mode, int value) = NULL;
static void (*fptr_taos_set_hb_quit)(int8_t quitByKill) = NULL;

static const char *(*fptr_taos_get_server_info)(TAOS *taos) = NULL;
static const char *(*fptr_taos_get_client_info)() = NULL;
static int (*fptr_taos_get_current_db)(TAOS *taos, char *database, int len, int *required) = NULL;
static const char *(*fptr_taos_errstr)(TAOS_RES *res) = NULL;
static int (*fptr_taos_errno)(TAOS_RES *res) = NULL;
static TSDB_SERVER_STATUS (*fptr_taos_check_server_status)(const char *fqdn, int port, char *details,
                                                           int maxlen) = NULL;

int taos_init(void) { return (*fptr_taos_init)(); }

void taos_cleanup(void) { (*fptr_taos_cleanup)(); }

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  return (*fptr_taos_connect)(ip, user, pass, db, port);
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  return (*fptr_taos_connect_auth)(ip, user, auth, db, port);
}

void taos_close(TAOS *taos) { (*fptr_taos_close)(taos); }

TAOS_RES *taos_query(TAOS *taos, const char *sql) { return (*fptr_taos_query)(taos, sql); }

TAOS_ROW taos_fetch_row(TAOS_RES *res) { return (*fptr_taos_fetch_row)(res); }

int taos_result_precision(TAOS_RES *res) { return (*fptr_taos_result_precision)(res); }

void taos_free_result(TAOS_RES *res) { (*fptr_taos_free_result)(res); }

void taos_kill_query(TAOS *taos) { (*fptr_taos_kill_query)(taos); }

int taos_field_count(TAOS_RES *res) { return (*fptr_taos_field_count)(res); }

int taos_num_fields(TAOS_RES *res) { return (*fptr_taos_num_fields)(res); }

int taos_affected_rows(TAOS_RES *res) { return (*fptr_taos_affected_rows)(res); }

int64_t taos_affected_rows64(TAOS_RES *res) { return (*fptr_taos_affected_rows64)(res); }

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) { return (*fptr_taos_fetch_fields)(res); }

int taos_select_db(TAOS *taos, const char *db) { return (*fptr_taos_select_db)(taos, db); }

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  return (*fptr_taos_print_row)(str, row, fields, num_fields);
}

int *taos_fetch_lengths(TAOS_RES *res) { return (*fptr_taos_fetch_lengths)(res); }

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) { (*fptr_taos_fetch_rows_a)(res, fp, param); }

int taos_set_conn_mode(TAOS *taos, int mode, int value) { return (*fptr_taos_set_conn_mode)(taos, mode, value); }

void taos_set_hb_quit(int8_t quitByKill) { (*fptr_taos_set_hb_quit)(quitByKill); }

const char *taos_get_server_info(TAOS *taos) { return (*fptr_taos_get_server_info)(taos); }

const char *taos_get_client_info() { return (*fptr_taos_get_client_info)(); }

int taos_get_current_db(TAOS *taos, char *database, int len, int *required) {
  return (*fptr_taos_get_current_db)(taos, database, len, required);
}

const char *taos_errstr(TAOS_RES *res) { return (*fptr_taos_errstr)(res); }

int taos_errno(TAOS_RES *res) { return (*fptr_taos_errno)(res); }

TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen) {
  return (*fptr_taos_check_server_status)(fqdn, port, details, maxlen);
}

#define TAOS_DRIVER_NATIVE_NAME    "libtaosinternal.so"
#define TAOS_DRIVER_WSBSOCKET_NAME "libtaos.so"

#define LOAD_FUNC(fptr, fname, driverName)    \
  funcName = fname;                           \
  fptr = taosLoadDllFunc(tsDriver, funcName); \
  if (fptr == NULL) goto _OVER;

static int32_t taosGetDebugDrive(char *driverPath, const char *driverName) {
  int32_t ret = 0;

#ifdef WINDOWS
  tstrncpy(tmp, "./", PATH_MAX);
#else
  char appPath[PATH_MAX] = {0};
  ret = taosGetAppPath(appPath, PATH_MAX);
  if (ret == 0) {
    snprintf(driverPath, PATH_MAX + 1, "%s%s..%slib%s%s", appPath, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP, driverName);
    ret = taosRealPath(driverPath, NULL, PATH_MAX);
  }
#endif

  return ret;
}

static int32_t taosGetInstallDriver(char *driverPath, const char *driverName) {
#ifdef WINDOWS
  (void)tstrncpy(tmp, "./", PATH_MAX);
#else
  (void)snprintf(driverPath, PATH_MAX, "/usr/local/taos/driver/%s", driverName);
#endif
  return 0;
}

int32_t taosDriverInit(ETaosDriverType driverType) {
  int32_t code = 0;
  char   *funcName = NULL;

  char        driverPath[PATH_MAX + 20] = {0};
  const char *driverName = TAOS_DRIVER_WSBSOCKET_NAME;
  if (driverType == TAOS_DRIVER_NATIVE) {
    driverName = TAOS_DRIVER_NATIVE_NAME;
  }

  if (tsDriver == NULL && taosGetDebugDrive(driverPath, driverName) == 0) {
    tsDriver = taosLoadDll(driverPath);
  }

  if (tsDriver == NULL && taosGetInstallDriver(driverPath, driverName) == 0) {
    tsDriver = taosLoadDll(driverPath);
  }

  if (tsDriver == NULL) {
    if (terrno == 0) {
      terrno = TSDB_CODE_DLL_NOT_FOUND;
    }
    printf("failed to load driverName at %s since %s\r\n", driverPath, terrstr());
    return -1;
  }

  printf("Load client driver from %s\r\n", driverPath);

  LOAD_FUNC(fptr_taos_init, "taos_init", driverPath);
  LOAD_FUNC(fptr_taos_cleanup, "taos_cleanup", driverPath);
  LOAD_FUNC(fptr_taos_connect, "taos_connect", driverPath);
  LOAD_FUNC(fptr_taos_connect_auth, "taos_connect_auth", driverPath);
  LOAD_FUNC(fptr_taos_close, "taos_close", driverPath);
  LOAD_FUNC(fptr_taos_query, "taos_query", driverPath);
  LOAD_FUNC(fptr_taos_fetch_row, "taos_fetch_row", driverPath);
  LOAD_FUNC(fptr_taos_result_precision, "taos_result_precision", driverPath);
  LOAD_FUNC(fptr_taos_free_result, "taos_free_result", driverPath);
  LOAD_FUNC(fptr_taos_kill_query, "taos_kill_query", driverPath);
  LOAD_FUNC(fptr_taos_field_count, "taos_field_count", driverPath);
  LOAD_FUNC(fptr_taos_num_fields, "taos_num_fields", driverPath);
  LOAD_FUNC(fptr_taos_affected_rows, "taos_affected_rows", driverPath);
  LOAD_FUNC(fptr_taos_affected_rows64, "taos_affected_rows64", driverPath);
  LOAD_FUNC(fptr_taos_fetch_fields, "taos_fetch_fields", driverPath);
  LOAD_FUNC(fptr_taos_select_db, "taos_select_db", driverPath);
  LOAD_FUNC(fptr_taos_print_row, "taos_print_row", driverPath);
  LOAD_FUNC(fptr_taos_fetch_lengths, "taos_fetch_lengths", driverPath);
  LOAD_FUNC(fptr_taos_fetch_rows_a, "taos_fetch_rows_a", driverPath);
  LOAD_FUNC(fptr_taos_set_conn_mode, "taos_set_conn_mode", driverPath);
  LOAD_FUNC(fptr_taos_set_hb_quit, "taos_set_hb_quit", driverPath);
  LOAD_FUNC(fptr_taos_get_server_info, "taos_get_server_info", driverPath);
  LOAD_FUNC(fptr_taos_get_client_info, "taos_get_client_info", driverPath);
  LOAD_FUNC(fptr_taos_get_current_db, "taos_get_current_db", driverPath);
  LOAD_FUNC(fptr_taos_errstr, "taos_errstr", driverPath);
  LOAD_FUNC(fptr_taos_errno, "taos_errno", driverPath);
  LOAD_FUNC(fptr_taos_check_server_status, "taos_check_server_status", driverPath);

  code = 0;

_OVER:
  if (code != 0) {
    printf("failed to load func:%s from %s\r\n", funcName, driverPath);
    if (terrno == 0) {
      terrno = TSDB_CODE_DLL_FUNC_NOT_FOUND;
    }
    taosDriverCleanup(tsDriver);
  }

  return code;
}

void taosDriverCleanup() {
  if (tsDriver != NULL) {
    taosCloseDll(tsDriver);
  }
}