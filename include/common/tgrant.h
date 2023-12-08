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

#ifndef _TD_COMMON_GRANT_H_
#define _TD_COMMON_GRANT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taoserror.h"
#include "tdef.h"

#ifndef GRANTS_COL_MAX_LEN
#define GRANTS_COL_MAX_LEN 196
#endif

#define GRANT_HEART_BEAT_MIN 2
#define GRANT_ACTIVE_CODE    "activeCode"
#define GRANT_C_ACTIVE_CODE  "cActiveCode"

typedef enum {
  TSDB_GRANT_ALL,
  TSDB_GRANT_TIME,
  TSDB_GRANT_USER,
  TSDB_GRANT_DB,
  TSDB_GRANT_TIMESERIES,
  TSDB_GRANT_DNODE,
  TSDB_GRANT_ACCT,
  TSDB_GRANT_STORAGE,
  TSDB_GRANT_SPEED,
  TSDB_GRANT_QUERY_TIME,
  TSDB_GRANT_CONNS,
  TSDB_GRANT_STREAM,
  TSDB_GRANT_CPU_CORES,
  TSDB_GRANT_STABLE,
  TSDB_GRANT_TABLE,
  TSDB_GRANT_TOPIC,
  TSDB_GRANT_STREAM_EXPIRE,
  TSDB_GRANT_TOPIC_EXPIRE,
  TSDB_GRANT_AUDIT_EXPIRE,
  TSDB_GRANT_MULTI_TIER_EXPIRE,
  TSDB_GRANT_BACKUP_RESTORE_EXPIRE,
  TSDB_GRANT_REPLICATION_EXPIRE,
} EGrantType;

typedef struct {
  int64_t grantedTime;
} SGrantedInfo;

int32_t grantCheck(EGrantType grant);
char*   grantGetMachineId();

#ifndef GRANTS_CFG
#ifdef TD_ENTERPRISE
#define GRANTS_SCHEMA                                                                                            \
  static const SSysDbTableSchema grantsSchema[] = {                                                              \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                      \
      {.name = "expire_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                 \
      {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                      \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                  \
      {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                      \
      {.name = "streams", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                      \
      {.name = "topics", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                       \
      {.name = "cpu_cores", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                    \
      {.name = "multi_tier_storage_expire", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},   \
      {.name = "streams_expire", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},              \
      {.name = "topics_expire", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},               \
      {.name = "audit_log_expire", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},            \
      {.name = "backup_restore_expire", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},       \
      {.name = "data_replication_expire", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},     \
      {.name = "opc_da", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},      \
      {.name = "opc_ua", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},      \
      {.name = "pi", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},          \
      {.name = "kafka", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},       \
      {.name = "influxdb", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},    \
      {.name = "mqtt", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},        \
      {.name = "opentsdb", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},    \
      {.name = "tdengine2.6", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR}, \
      {.name = "tdengine3.0", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR}, \
  }
#else
#define GRANTS_SCHEMA                                                                            \
  static const SSysDbTableSchema grantsSchema[] = {                                              \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},      \
      {.name = "expire_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR}, \
      {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},      \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},  \
      {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},      \
      {.name = "streams", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},      \
      {.name = "topics", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},       \
      {.name = "cpu_cores", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},    \
  }
#endif
#define GRANT_CFG_ADD
#define GRANT_CFG_SET
#define GRANT_CFG_GET
#define GRANT_CFG_CHECK
#define GRANT_CFG_SKIP
#define GRANT_CFG_DECLARE
#define GRANT_CFG_EXTERN
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GRANT_H_*/
