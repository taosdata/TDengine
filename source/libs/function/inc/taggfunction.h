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

#ifndef TDENGINE_TAGGFUNCTION_H
#define TDENGINE_TAGGFUNCTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "tname.h"
#include "taosdef.h"
#include "tvariant.h"
#include "function.h"
#include "tudf.h"

#define AVG_FUNCTION_INTER_BUFFER_SIZE 50

#define DATA_SET_FLAG ','  // to denote the output area has data, not null value
#define DATA_SET_FLAG_SIZE sizeof(DATA_SET_FLAG)

typedef struct SInterpInfoDetail {
  TSKEY  ts;  // interp specified timestamp
  int8_t type;
  int8_t primaryCol;
} SInterpInfoDetail;

bool topbot_datablock_filter(SqlFunctionCtx *pCtx, const char *minval, const char *maxval);

/**
 * the numOfRes should be kept, since it may be used later
 * and allow the ResultInfo to be re initialized
 */
static FORCE_INLINE void initResultRowEntry(SResultRowEntryInfo *pResInfo, int32_t bufLen) {
  pResInfo->initialized = true;  // the this struct has been initialized flag

  pResInfo->complete  = false;
  pResInfo->numOfRes  = 0;
  memset(GET_ROWCELL_INTERBUF(pResInfo), 0, bufLen);
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TAGGFUNCTION_H
