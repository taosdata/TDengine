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


extern SAggFunctionInfo aggFunc[35];

#define FUNCSTATE_SO           0x0u
#define FUNCSTATE_MO           0x1u    // dynamic number of output, not multinumber of output e.g., TOP/BOTTOM
#define FUNCSTATE_STREAM       0x2u    // function avail for stream
#define FUNCSTATE_STABLE       0x4u    // function avail for super table
#define FUNCSTATE_NEED_TS      0x8u    // timestamp is required during query processing
#define FUNCSTATE_SELECTIVITY  0x10u   // selectivity functions, can exists along with tag columns

#define BASIC_FUNC_SO FUNCSTATE_SO | FUNCSTATE_STREAM | FUNCSTATE_STABLE
#define BASIC_FUNC_MO FUNCSTATE_MO | FUNCSTATE_STREAM | FUNCSTATE_STABLE

#define AVG_FUNCTION_INTER_BUFFER_SIZE 50

#define DATA_SET_FLAG ','  // to denote the output area has data, not null value
#define DATA_SET_FLAG_SIZE sizeof(DATA_SET_FLAG)

typedef struct SInterpInfoDetail {
  TSKEY  ts;  // interp specified timestamp
  int8_t type;
  int8_t primaryCol;
} SInterpInfoDetail;

#define GET_ROWCELL_INTERBUF(_c) ((void*) ((char*)(_c) + sizeof(SResultRowEntryInfo)))

typedef struct STwaInfo {
  int8_t      hasResult;  // flag to denote has value
  double      dOutput;
  SPoint1     p;
  STimeWindow win;
} STwaInfo;

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
