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

#ifndef _TD_TDATA_H_
#define _TD_TDATA_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

typedef struct STaosData TDATA, tdata_t;

typedef enum {
  TAOS_META_STABLE_DATA = 0,  // super table meta
  TAOS_META_TABLE_DATA,       // non-super table meta
  TAOS_TS_ROW_DATA,           // row time-series data
  TAOS_TS_COL_DATA,           // col time-series data
  TAOS_DATA_MAX
} ETaosDataT;

struct STaosData {
  ETaosDataT type;
  uint32_t   nPayload;
  uint8_t   *pPayload;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDATA_H_*/