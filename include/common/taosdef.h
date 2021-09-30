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

#ifndef _TD_COMMON_TAOS_DEF_H
#define _TD_COMMON_TAOS_DEF_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tdef.h"
#include "taos.h"

#define TSWINDOW_INITIALIZER ((STimeWindow) {INT64_MIN, INT64_MAX})
#define TSWINDOW_DESC_INITIALIZER ((STimeWindow) {INT64_MAX, INT64_MIN})
#define IS_TSWINDOW_SPECIFIED(win) (((win).skey != INT64_MIN) || ((win).ekey != INT64_MAX))

typedef enum {
  TAOS_QTYPE_RPC   = 0,
  TAOS_QTYPE_FWD   = 1,
  TAOS_QTYPE_WAL   = 2,
  TAOS_QTYPE_CQ    = 3,
  TAOS_QTYPE_QUERY = 4
} EQType;

typedef enum {
  TSDB_SUPER_TABLE  = 0,   // super table
  TSDB_CHILD_TABLE  = 1,   // table created from super table
  TSDB_NORMAL_TABLE = 2,   // ordinary table
  TSDB_STREAM_TABLE = 3,   // table created from stream computing
  TSDB_TEMP_TABLE   = 4,   // temp table created by nest query
  TSDB_TABLE_MAX    = 5
} ETableType;

typedef enum {
  TSDB_MOD_MNODE   = 0,
  TSDB_MOD_HTTP    = 1,
  TSDB_MOD_MONITOR = 2,
  TSDB_MOD_MQTT    = 3,
  TSDB_MOD_MAX     = 4
} EModuleType;

typedef enum {
  TSDB_CHECK_ITEM_NETWORK,
  TSDB_CHECK_ITEM_MEM,
  TSDB_CHECK_ITEM_CPU,
  TSDB_CHECK_ITEM_DISK,
  TSDB_CHECK_ITEM_OS,
  TSDB_CHECK_ITEM_ACCESS,
  TSDB_CHECK_ITEM_VERSION,
  TSDB_CHECK_ITEM_DATAFILE,
  TSDB_CHECK_ITEM_MAX
} ECheckItemType;

typedef enum {
  TD_ROW_DISCARD_UPDATE   = 0,
  TD_ROW_OVERWRITE_UPDATE = 1,
  TD_ROW_PARTIAL_UPDATE   = 2
} TDUpdateConfig;

extern char *qtypeStr[];

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TAOS_DEF_H*/
