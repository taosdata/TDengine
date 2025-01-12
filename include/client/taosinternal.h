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

#ifndef TDENGINE_TAOS_INTERNAL_H
#define TDENGINE_TAOS_INTERNAL_H

#include "taos.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------ JNI INTERFACE -----------------------------------*/
#define RET_MSG_LENGTH 1024
typedef struct setConfRet {
  SET_CONF_RET_CODE retCode;
  char              retMsg[RET_MSG_LENGTH];
} setConfRet;

DLL_EXPORT setConfRet taos_set_config(const char *config);

/* ------------------------------ STMT2 INTERFACE ---------------------------------*/
typedef void TAOS_STMT2;

typedef struct TAOS_STMT2_OPTION {
  int64_t           reqid;
  bool              singleStbInsert;
  bool              singleTableBindOnce;
  __taos_async_fn_t asyncExecFn;
  void             *userdata;
} TAOS_STMT2_OPTION;

typedef struct TAOS_STMT2_BIND {
  int      buffer_type;
  void    *buffer;
  int32_t *length;
  char    *is_null;
  int      num;
} TAOS_STMT2_BIND;

typedef struct TAOS_STMT2_BINDV {
  int               count;
  char            **tbnames;
  TAOS_STMT2_BIND **tags;
  TAOS_STMT2_BIND **bind_cols;
} TAOS_STMT2_BINDV;

DLL_EXPORT TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option);
DLL_EXPORT int         taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length);
DLL_EXPORT int         taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx);
DLL_EXPORT int         taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows);
DLL_EXPORT int         taos_stmt2_close(TAOS_STMT2 *stmt);
DLL_EXPORT int         taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert);
DLL_EXPORT int         taos_stmt2_get_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields);
DLL_EXPORT void        taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields);
DLL_EXPORT TAOS_RES   *taos_stmt2_result(TAOS_STMT2 *stmt);
DLL_EXPORT char       *taos_stmt2_error(TAOS_STMT2 *stmt);

/* ------------------------------ WHITELIST INTERFACE -------------------------------*/
typedef void (*__taos_async_whitelist_fn_t)(void *param, int code, TAOS *taos, int numOfWhiteLists,
                                            uint64_t *pWhiteLists);
DLL_EXPORT void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param);

/* ------------------------------ TAOSX INTERFACE -----------------------------------*/
typedef struct tmq_raw_data {
  void    *raw;
  uint32_t raw_len;
  uint16_t raw_type;
} tmq_raw_data;

DLL_EXPORT int32_t tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw);
DLL_EXPORT int32_t tmq_write_raw(TAOS *taos, tmq_raw_data raw);
DLL_EXPORT int     taos_write_raw_block(TAOS *taos, int numOfRows, char *pData, const char *tbname);
DLL_EXPORT int     taos_write_raw_block_with_reqid(TAOS *taos, int numOfRows, char *pData, const char *tbname,
                                                   int64_t reqid);
DLL_EXPORT int     taos_write_raw_block_with_fields(TAOS *taos, int rows, char *pData, const char *tbname,
                                                    TAOS_FIELD *fields, int numFields);
DLL_EXPORT int     taos_write_raw_block_with_fields_with_reqid(TAOS *taos, int rows, char *pData, const char *tbname,
                                                               TAOS_FIELD *fields, int numFields, int64_t reqid);
DLL_EXPORT void    tmq_free_raw(tmq_raw_data raw);

// Returning null means error. Returned result need to be freed by tmq_free_json_meta
DLL_EXPORT char *tmq_get_json_meta(TAOS_RES *res);
DLL_EXPORT void  tmq_free_json_meta(char *jsonMeta);
/* ---------------------------- TAOSX END -------------------------------- */

/* ---------------------------- INTERNAL INTERFACE ------------------------ */
typedef enum {
  TSDB_SRV_STATUS_UNAVAILABLE = 0,
  TSDB_SRV_STATUS_NETWORK_OK = 1,
  TSDB_SRV_STATUS_SERVICE_OK = 2,
  TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
  TSDB_SRV_STATUS_EXTING = 4,
} TSDB_SERVER_STATUS;

DLL_EXPORT TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen);
DLL_EXPORT char              *getBuildInfo();

#ifdef __cplusplus
}
#endif

#endif
