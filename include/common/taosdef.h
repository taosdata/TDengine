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

#ifndef _TD_COMMON_TAOS_DEF_H_
#define _TD_COMMON_TAOS_DEF_H_

#include "taos.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int64_t tb_uid_t;

#define TSWINDOW_INITIALIZER       ((STimeWindow){INT64_MIN, INT64_MAX})
#define TSWINDOW_DESC_INITIALIZER  ((STimeWindow){INT64_MAX, INT64_MIN})
#define IS_TSWINDOW_SPECIFIED(win) (((win).skey != INT64_MIN) || ((win).ekey != INT64_MAX))
#define TSWINDOW_IS_EQUAL(t1, t2)  (((t1).skey == (t2).skey) && ((t1).ekey == (t2).ekey))

//define show cluster alive and show db.alive
#define SHOW_STATUS_NOT_AVAILABLE  0
#define SHOW_STATUS_AVAILABLE      1
#define SHOW_STATUS_HALF_AVAILABLE 2

typedef enum {
  TSDB_SUPER_TABLE = 1,   // super table
  TSDB_CHILD_TABLE = 2,   // table created from super table
  TSDB_NORMAL_TABLE = 3,  // ordinary table
  TSDB_TEMP_TABLE = 4,    // temp table created by nest query
  TSDB_SYSTEM_TABLE = 5,
  TSDB_TSMA_TABLE = 6,  // time-range-wise sma
  TSDB_VIEW_TABLE = 7,
  TSDB_TABLE_MAX = 8
} ETableType;

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
  TD_ROW_DISCARD_UPDATE = 0,
  TD_ROW_OVERWRITE_UPDATE = 1,
  TD_ROW_PARTIAL_UPDATE = 2,
} TDUpdateConfig;

#define TD_SUPPORT_UPDATE(u) ((u) > 0)

typedef enum {
  TSDB_STATIS_OK = 0,    // statis part exist and load successfully
  TSDB_STATIS_NONE = 1,  // statis part not exist
} ETsdbStatisStatus;

typedef enum {
  TSDB_SMA_TYPE_BLOCK = 0,       // Block-wise SMA
  TSDB_SMA_TYPE_TIME_RANGE = 1,  // Time-range-wise SMA
  TSDB_SMA_TYPE_ROLLUP = 2,      // Rollup SMA
} ETsdbSmaType;

typedef enum {
  TSDB_RETENTION_L0 = 0,
  TSDB_RETENTION_L1 = 1,
  TSDB_RETENTION_L2 = 2,
  TSDB_RETENTION_MAX = 3
} ERetentionLevel;

typedef enum {
  TSDB_BITMODE_DEFAULT = 0,  // 2 bits
  TSDB_BITMODE_ONE_BIT = 1,  // 1 bit
} EBitmapMode;

extern char *qtypeStr[];

#define TSDB_PORT_HTTP 11

#undef TD_DEBUG_PRINT_ROW
#undef TD_DEBUG_PRINT_TSDB_LOAD_DCOLS
#undef TD_DEBUG_PRINT_TAG

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TAOS_DEF_H_*/
