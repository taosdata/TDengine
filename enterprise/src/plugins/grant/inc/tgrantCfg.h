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

#ifndef _TD_GRANTS_H_
#define _TD_GRANTS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <machine.h>

#define GRANTS_COL_MAX_LEN 196

#define GRANTS_SCHEMA                                                                                         \
  static const SSysDbTableSchema grantsSchema[] = {                                                           \
      {.name = "version", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                   \
      {.name = "expire_time", .bytes = 11 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},              \
      {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},               \
      {.name = "databases", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                \
      {.name = "stables", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                  \
      {.name = "tables", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},                   \
      {.name = "opc_da", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},   \
      {.name = "opc_ua", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},   \
      {.name = "pi", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},       \
      {.name = "kafka", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},    \
      {.name = "influxdb", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR}, \
      {.name = "mqtt", .bytes = GRANTS_COL_MAX_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},     \
  }

#define GRANT_CFG_DECLARE uint64_t tsGrantLimitTimeSeries = GRANT_TIME_SERIES_LIMITS;  \
    uint32_t tsGrantLimitDbs = GRANT_DATABASE_LIMITS;                                  \
    uint32_t tsGrantLimitSTables = GRANT_STABLE_LIMITS;                                \
    uint32_t tsGrantLimitTables = GRANT_TABLE_LIMITS;                                  \
    bool     tsGrantUpdateForced = false;

#define GRANT_CFG_EXTERN extern int64_t tsGrantLimitTimeSeries;  \
    extern int32_t tsGrantLimitDbs;                              \
    extern int32_t tsGrantLimitSTables;                          \
    extern int32_t tsGrantLimitTables;                           \
    extern bool    tsGrantUpdateForced;

#define GRANT_CFG_ADD                                                                        \
  do {                                                                                       \
    if (cfgAddString(pCfg, "grant", "", false) != 0) return -1;                              \
    SConfigItem *pItemGrant = cfgGetItem(pCfg, "grant");                                     \
    pItemGrant->array = taosArrayInit_s(sizeof(SConfigGrantItem), 5);                        \
    if (pItemGrant->array == NULL) {                                                         \
        terrno = TSDB_CODE_OUT_OF_MEMORY;                                                    \
        return -1;                                                                           \
    }                                                                                        \
    ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 0))->u64 = GRANT_TIME_SERIES_LIMITS; \
    ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 1))->u32 = GRANT_DATABASE_LIMITS;    \
    ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 2))->u32 = GRANT_STABLE_LIMITS;      \
    ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 3))->u32 = GRANT_TABLE_LIMITS;       \
    ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 4))->bval = false;                   \
  } while(0)

typedef struct SConfigGrantItem {
  union {
    bool     bval;
    uint32_t u32;
    uint64_t u64;
  };
} SConfigGrantItem;

#define GRANT_CFG_SET                                                                                                   \
  do {                                                                                                                  \
    char *grantCfgNameList[4] = { "grantLimitTimeSeries", "grantLimitDbs", "grantLimitSTables", "grantLimitTables" };   \
    for (int grantCfgNameIndex = 0; grantCfgNameIndex < 4; grantCfgNameIndex++) {                                       \
        if (strcmp(name, grantCfgNameList[grantCfgNameIndex]) == 0) {                                                   \
            SConfigItem *pItemGrant = cfgGetItem(pCfg, "grant");                                                        \
            if (pItemGrant == NULL || pItemGrant->array == NULL) return -1;                                             \
            SConfigGrantItem *pConfigGrantItem = (SConfigGrantItem*)taosArrayGet(pItemGrant->array, grantCfgNameIndex); \
            if (grantCfgNameIndex == 0) {                                                                               \
                pConfigGrantItem->u64 = taosStr2UInt64(value, NULL, 10);                                                \
            } else {                                                                                                    \
                pConfigGrantItem->u32 = taosStr2UInt32(value, NULL, 10);                                                \
            }                                                                                                           \
            ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 4))->bval = true;                                       \
            return 0;                                                                                                   \
        }                                                                                                               \
    }                                                                                                                   \
  } while(0)

#define GRANT_CFG_GET                                                                      \
  do {                                                                                     \
    SConfigItem *pItemGrant = cfgGetItem(pCfg, "grant");                                   \
    tsGrantLimitTimeSeries = ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 0))->u64; \
    tsGrantLimitDbs = ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 1))->u32;        \
    tsGrantLimitSTables = ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 2))->u32;    \
    tsGrantLimitTables = ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 3))->u32;     \
    tsGrantUpdateForced = ((SConfigGrantItem*)taosArrayGet(pItemGrant->array, 4))->bval;   \
  } while(0)

#define GRANT_CFG_CHECK                    \
  do {                                     \
    if (strcmp(name, "grant") == 0) {      \
            *forbidden = true;             \
            return;                        \
        }                                  \
  } while(0)

#define GRANT_CFG_SKIP                     \
    if (strcmp(pItem->name, "grant") == 0) \
        continue

#ifdef __cplusplus

}
#endif

#endif /*_TD_COMMON_GRANT_H_*/
