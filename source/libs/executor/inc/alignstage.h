/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
 * for more details.
 */

#ifndef _TD_ALIGN_STAGE_H_
#define _TD_ALIGN_STAGE_H_

#include "tdatablock.h"

#ifdef __cplusplus
extern "C" {
#endif

struct SExecTaskInfo;

// One TIME_ALIGN output column.  srcSlotId is the slot of the source column in
// the input block; outSlotId is the slot the filled value is written to.
// hasSpan/spanValue/spanUnit carry the optional time-range parameter;
// spanUnit is stored as a char and q is already folded into months.
typedef struct SAlignStageCol {
  int16_t  srcSlotId;
  int16_t  outSlotId;
  int8_t   method;  // EAlignMethod: 1 = PREV, 2 = NEXT
  SDataType type;
  char     name[TSDB_COL_NAME_LEN];
  bool     hasSpan;
  int64_t  spanValue;
  int8_t   spanUnit;
} SAlignStageCol;

typedef struct SAlignStage SAlignStage;

// Build the alignment stage.  Function contexts are created per series on
// demand, so one stage instance can serve interleaved series.
// tsSlotId is the block slot of the primary timestamp (used to refresh the ts
// window); spanTsSlotId is the time-axis slot for time-range checks; tzName is
// the session timezone (calendar units only), mono tells fill_next whether the
// input timestamps are monotonic in the delivery direction, and order is the
// delivery order (TSDB_ORDER_ASC/DESC).  A descending stream maps PREV to
// fill_next and NEXT to fill_forward so the values stay time-based.
int32_t alignStageCreate(SAlignStage** ppStage, const SAlignStageCol* pCols, int32_t numOfCols,
                         int32_t tsSlotId, int32_t spanTsSlotId, const char* tzName, bool mono,
                         int8_t order, struct SExecTaskInfo* pTaskInfo);
void    alignStageSetMaxHeldRows(SAlignStage* pStage, int64_t maxHeldRows);
bool    alignStageHasNext(const SAlignStage* pStage);

// Feed one input block of the given series/phase.  The input block already
// carries empty slots for every align output.  *ppReady receives the held block
// once no NEXT column has an unresolved position, or NULL while rows are still
// pending.  A returned block stays valid until the next feed/publish of the
// same series; callers consume it before continuing.
int32_t alignStageFeed(SAlignStage* pStage, uint64_t uid, int8_t phase, SSDataBlock* pIn,
                       SSDataBlock** ppReady);

// Publish the next held block whose series has ended (end of input or a scan
// phase switch).  *ppBlock is NULL when nothing is left to publish.
int32_t alignStageFlushNext(SAlignStage* pStage, SSDataBlock** ppBlock);

// Publish the held block of one series whose input has ended.  Used by hosts
// that merge several independent per-table streams (parallel table sort), where
// one series can end while the others are still running.  *ppBlock is NULL when
// that series has nothing left to publish.
int32_t alignStageFlushSeries(SAlignStage* pStage, uint64_t uid, SSDataBlock** ppBlock);

// Release all per-series state but keep the stage usable (operator reset).
int32_t alignStageReset(SAlignStage* pStage);
void    alignStageDestroy(SAlignStage* pStage);

#ifdef __cplusplus
}
#endif

#endif  // _TD_ALIGN_STAGE_H_
