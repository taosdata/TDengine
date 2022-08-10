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

#ifndef TDENGINE_TFILL_H
#define TDENGINE_TFILL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taosdef.h"
#include "tcommon.h"

struct SSDataBlock;

typedef struct SFillColInfo {
  SExprInfo *pExpr;
  int16_t    flag;            // column flag: TAG COLUMN|NORMAL COLUMN
  int16_t    tagIndex;        // index of current tag in SFillTagColInfo array list
  SVariant   fillVal;
} SFillColInfo;

typedef struct SFillLinearInfo {
  SPoint  start;
  SPoint  end;
  bool    hasNull;
  bool    fillLastPoint;
  int16_t type;
  int32_t bytes;
} SFillLinearInfo;

typedef struct {
  SSchema col;
  char*   tagVal;
} SFillTagColInfo;

typedef struct SFillInfo {
  TSKEY     start;                // start timestamp
  TSKEY     end;                  // endKey for fill
  TSKEY     currentKey;           // current active timestamp, the value may be changed during the fill procedure.
  int32_t   tsSlotId;             // primary time stamp slot id
  int32_t   order;                // order [TSDB_ORDER_ASC|TSDB_ORDER_DESC]
  int32_t   type;                 // fill type
  int32_t   numOfRows;            // number of rows in the input data block
  int32_t   index;                // active row index
  int32_t   numOfTotal;           // number of filled rows in one round
  int32_t   numOfCurrent;         // number of filled rows in current results

  int32_t   numOfTags;            // number of tags
  int32_t   numOfCols;            // number of columns, including the tags columns
  int32_t   rowSize;              // size of each row
  SInterval interval;

  SArray   *prev;
  SArray   *next;
  SSDataBlock *pSrcBlock;
  int32_t   alloc;                // data buffer size in rows

  SFillColInfo* pFillCol;         // column info for fill operations
  SFillTagColInfo* pTags;         // tags value for filling gap
  const char* id;
} SFillInfo;

int64_t getNumOfResultsAfterFillGap(SFillInfo* pFillInfo, int64_t ekey, int32_t maxNumOfRows);


void taosFillSetStartInfo(struct SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey);
void taosResetFillInfo(struct SFillInfo* pFillInfo, TSKEY startTimestamp);
void taosFillSetInputDataBlock(struct SFillInfo* pFillInfo, const struct SSDataBlock* pInput);
struct SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfOutput, const struct SNodeListNode* val);
bool taosFillHasMoreResults(struct SFillInfo* pFillInfo);

SFillInfo* taosCreateFillInfo(TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
                              SInterval* pInterval, int32_t fillType, struct SFillColInfo* pCol, int32_t slotId,
                              int32_t order, const char* id);

void* taosDestroyFillInfo(struct SFillInfo *pFillInfo);
int64_t taosFillResultDataBlock(struct SFillInfo* pFillInfo, SSDataBlock* p, int32_t capacity);
int64_t getFillInfoStart(struct SFillInfo *pFillInfo);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TFILL_H
