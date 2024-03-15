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
#define GRANT_FLAG_ALL       (0x01)
#define GRANT_FLAG_AUDIT     (0x02)
#define GRANT_FLAG_VIEW      (0x04)

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
  TSDB_GRANT_STREAMS,
  TSDB_GRANT_CPU_CORES,
  TSDB_GRANT_STABLE,
  TSDB_GRANT_TABLE,
  TSDB_GRANT_SUBSCRIPTION,
  TSDB_GRANT_AUDIT,
  TSDB_GRANT_CSV,
  TSDB_GRANT_VIEW,
  TSDB_GRANT_MULTI_TIER,
  TSDB_GRANT_BACKUP_RESTORE,
} EGrantType;

int32_t grantCheck(EGrantType grant);
int32_t grantCheckExpire(EGrantType grant);
char*   tGetMachineId();
#ifdef TD_UNIQ_GRANT
int32_t grantCheckLE(EGrantType grant);
#endif

// #ifndef GRANTS_CFG
#ifdef TD_ENTERPRISE
#define GRANTS_SCHEMA                                                                                              \
  static const SSysDbTableSchema grantsSchema[] = {                                                                \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "expire_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},  \
      {.name = "service_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true}, \
      {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "state", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},         \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},   \
      {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "cpu_cores", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},    \
  }
#else
#define GRANTS_SCHEMA                                                                                              \
  static const SSysDbTableSchema grantsSchema[] = {                                                                \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "expire_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},  \
      {.name = "service_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true}, \
      {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "state", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},         \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},   \
      {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "cpu_cores", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},    \
  }
#endif
// #define GRANT_CFG_ADD
// #define GRANT_CFG_SET
// #define GRANT_CFG_GET
// #define GRANT_CFG_CHECK
// #define GRANT_CFG_SKIP
// #define GRANT_CFG_DECLARE
// #define GRANT_CFG_EXTERN
// #endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GRANT_H_*/
