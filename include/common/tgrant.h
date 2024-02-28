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
#ifdef GRANTS_CFG
#include "tgrantCfg.h"
#endif

#ifndef GRANTS_COL_MAX_LEN
#define GRANTS_COL_MAX_LEN 196
#endif

#define GRANT_HEART_BEAT_MIN 2

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
} EGrantType;

int32_t grantCheck(EGrantType grant);
int32_t grantAlterActiveCode(int32_t did, const char* old, const char* new, char* out, int8_t type);

#ifndef GRANTS_CFG
#ifdef TD_ENTERPRISE
#ifndef TD_GRANT_WWHISTORIAN
#define GRANTS_SCHEMA                                                                                                                \
  static const SSysDbTableSchema grantsSchema[] = {                                                                                  \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "expire_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                    \
      {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "storage", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                        \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                     \
      {.name = "databases", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                      \
      {.name = "users", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                          \
      {.name = "accounts", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                       \
      {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "connections", .bytes = 11 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                    \
      {.name = "streams", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "cpu_cores", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                       \
      {.name = "speed", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                           \
      {.name = "querytime", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                       \
      {.name = "opc_da", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},         \
      {.name = "opc_ua", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},         \
      {.name = "pi", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},             \
      {.name = "kafka", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},          \
      {.name = "influxdb", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "mqtt", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},           \
      {.name = "avevahistorian", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true}, \
      {.name = "opentsdb", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
  }
#else
#define GRANTS_SCHEMA                                                                                                                \
  static const SSysDbTableSchema grantsSchema[] = {                                                                                  \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "expire_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                    \
      {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "storage", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                        \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                     \
      {.name = "databases", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                      \
      {.name = "users", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                          \
      {.name = "accounts", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                       \
      {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "connections", .bytes = 11 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                    \
      {.name = "streams", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                         \
      {.name = "cpu_cores", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                       \
      {.name = "speed", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                           \
      {.name = "querytime", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                       \
      {.name = "opc_da", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},         \
      {.name = "opc_ua", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},         \
      {.name = "pi", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},             \
      {.name = "kafka", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},          \
      {.name = "influxdb", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},       \
      {.name = "mqtt", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},           \
      {.name = "avevahistorian", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true}, \
  }
#endif
#else
#define GRANTS_SCHEMA                                                                                                          \
  static const SSysDbTableSchema grantsSchema[] = {                                                                            \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                   \
      {.name = "expire_time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},              \
      {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                   \
      {.name = "storage", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                  \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},               \
      {.name = "databases", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                \
      {.name = "users", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                    \
      {.name = "accounts", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                 \
      {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                   \
      {.name = "connections", .bytes = 11 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},              \
      {.name = "streams", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                   \
      {.name = "cpu_cores", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                 \
      {.name = "speed", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                     \
      {.name = "querytime", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},                 \
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