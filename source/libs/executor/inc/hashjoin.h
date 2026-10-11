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
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_HASHJOIN_H
#define TDENGINE_HASHJOIN_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"
#include "operator.h"

#define HASH_JOIN_DEFAULT_PAGE_SIZE 10485760
#define HJOIN_DEFAULT_BLK_ROWS_NUM 4096
#define HJOIN_BLK_SIZE_LIMIT 10485760
#define HJOIN_ROW_BITMAP_SIZE (2 * 1048576)
#define HJOIN_NMATCH_BATCH_SIZE 64
#define HJOIN_BLK_THRESHOLD_RATIO 0.9

typedef struct SHJoinOperatorInfo SHJoinOperatorInfo;
typedef struct SGroupData SGroupData;

typedef int32_t (*hJoinImplFp)(SOperatorInfo*);
typedef int32_t (*hJoinBuildFp)(SOperatorInfo* pOperator, bool* pQueryDone);
// Probe-end callbacks set at most one control flag; needBreak takes precedence.
typedef int32_t (*hJoinProbeEndFp)(SHJoinOperatorInfo* pJoin, bool* needBreak, bool* needContinue);

#pragma pack(push, 1)
typedef struct SBufRowInfo {
  void*    next;
  uint16_t pageId;
  int32_t  offset;
  uint16_t keyPageId;
  int32_t  keyOffset;
  uint32_t grpRowIdx;
} SBufRowInfo;
#pragma pack(pop)

typedef enum EHJoinPhase {
  E_JOIN_PHASE_PRE = 1,
  E_JOIN_PHASE_CUR,
  E_JOIN_PHASE_POST
} EHJoinPhase;

typedef enum EHJoinMultiGroupPhase {
  HJOIN_MULTI_GROUP_BUILD = 0,
  HJOIN_MULTI_GROUP_PROBE,
  HJOIN_MULTI_GROUP_EMIT_EOG,
  HJOIN_MULTI_GROUP_DONE,
} EHJoinMultiGroupPhase;

typedef struct SHJoinCtx {
  bool         rowRemains;
  bool         midRemains;
  int64_t      limit;
  SBufRowInfo* pBuildRow;
  SSDataBlock* pProbeData;
  EHJoinPhase  probePhase;
  int32_t      probePreIdx;
  int32_t      probeStartIdx;
  int32_t      probeEndIdx;
  int32_t      probePostIdx;
  bool         readMatch;

  // FULL JOIN unmatched-build iteration state; reset between groups.
  bool         buildNMatchDone;
  bool         buildNMatchInited;
  SGroupData*  pBuildNMatchGrp;  // Borrowed from pKeyHash.
  int32_t      buildNMatchIter;
  SBufRowInfo* pBuildNMatchRow;   // Borrowed from pBuildNMatchGrp->rows; valid for the current group only.
  SBufRowInfo* pBuildNullRows;    // Owned linked list.
} SHJoinCtx;

typedef struct SHJoinColInfo {
  int32_t          srcSlot;
  int32_t          dstSlot;
  int32_t          keyColIdx;
  int8_t           type;
  uint8_t          precision;        // Normalized key precision shared by both inputs.
  uint8_t          sourcePrecision;  // Original input expression precision.
  bool             vardata;
  bool             isExpr;
  int32_t*         offset;
  int32_t          bytes;
  int32_t          bufOffset;
  char*            data;
  char*            bitMap;
  SColumnInfoData* colData;
  SScalarParam     converted;
} SHJoinColInfo;

typedef struct SBufPageInfo {
  int32_t pageSize;
  int32_t offset;
  char*   data;
} SBufPageInfo;


struct SGroupData {
  SBufRowInfo* rows;
  char*        bitmap;
  uint32_t     rowsNum;
  uint32_t     rowsMatchNum;
  int32_t*     keyOffsets;
  int32_t      keyOffsetNum;
};


typedef struct SHJoinColMap {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     vardata;
  int32_t  bytes;
} SHJoinColMap;

typedef enum EHJoinPrimExprType {
  HJ_PRIM_NONE = 0,
  HJ_PRIM_TIMETRUNCATE = 1,
  HJ_PRIM_TIMESTAMP_SCALE,
  HJ_PRIM_VALUE,
} EHJoinPrimExprType;

typedef struct SHJoinPrimExprCtx {
  EHJoinPrimExprType type;
  // TIMETRUNCATE parameters; VALUE also uses precision.
  int64_t truncateUnit;
  int64_t timezoneUnit;
  timezone_t tz;  /* DST-aware IANA handle, NULL  ->  fixed offset */
  int8_t   fdow;  /* firstDayOfWeek 0-6, for week trunc alignment */
  int32_t  precision;
  int32_t targetSlotId;
  uint8_t targetPrec;  // TIMESTAMP_SCALE output precision.
  int64_t constTs;     // VALUE timestamp literal.
} SHJoinPrimExprCtx;

typedef struct SHJoinTableCtx {
  int32_t        downStreamIdx;
  SOperatorInfo* downStream;
  int64_t        blkId;
  SQueryStat     inputStat;
  bool           hasTimeRange;
  SExprSupp      keyExprSupp;
  SExecTaskInfo* taskInfo;
  bool           multiGroupJoin;
  uint64_t       baseGId;

  SHJoinColMap*      primCol;
  SNode*             primExpr;
  SHJoinPrimExprCtx  primCtx;
  
  int32_t        keyNum;
  SHJoinColInfo* keyCols;
  char*          keyBuf;
  size_t         keyBufCap;
  char*          keyData;
  
  int32_t        valNum;
  SHJoinColInfo* valCols;
  char*          valData;
  int32_t        valBitMapSize;
  int32_t        valBufSize;
  SArray*        valVarCols;
  bool           valColExist;
} SHJoinTableCtx;

typedef struct SHJoinExecInfo {
  int64_t  buildBlkNum;
  int64_t  buildBlkRows;
  int64_t  probeBlkNum;
  int64_t  probeBlkRows;
  int64_t  resRows;
  int64_t  expectRows;
  uint64_t hashMemPeakBytes;
} SHJoinExecInfo;

typedef struct SHJoinOperatorInfo {
  EJoinType        joinType;
  EJoinSubType     subType;
  SHJoinTableCtx   tbs[2];
  SHJoinTableCtx*  pBuild;
  SHJoinTableCtx*  pProbe;
  SFilterInfo*     pPreFilter;
  SFilterInfo*     pFinFilter;  
  SSDataBlock*     finBlk;
  SSDataBlock*     midBlk;
  int64_t          outBlkId;
  STimeWindow      tblTimeRange;
  int32_t          pResColNum;
  int8_t*          pResColMap;
  int32_t          resPrimSlotId;
  SArray*          pRowBufs;
  SSHashObj*       pKeyHash;
  bool             keyHashBuilt;
  bool                  multiGroupJoin;
  SBlockID              probeId;
  bool                  probeIdSet;
  SSDataBlock*          pPendingProbe;
  uint64_t              hashOwnedBytes;
  EHJoinMultiGroupPhase multiGroupPhase;
  int32_t               groupOrderIdx;
  uint64_t              currentBaseGId;
  bool                  buildEogSeen;
  bool                  probeEogSeen;
  SArray*               pProbeWinIdx;
  SArray*               pOutputWinIdx;
  SHJoinCtx        ctx;
  SHJoinExecInfo   execInfo;
  int32_t          blkThreshold;
  hJoinImplFp      joinFp;
  hJoinBuildFp     buildFp;
  hJoinProbeEndFp  probeEndFp;
} SHJoinOperatorInfo;

#define IS_HASH_JOIN_KEY_COL(_keyIdx) ((_keyIdx) >= 0)

#define HJ_ERR_RET(c)                 \
  do {                                \
    int32_t _code = (c);              \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)

#define HJ_ERR_JRET(c)               \
  do {                               \
    code = (c);                      \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

int32_t hInnerJoinDo(struct SOperatorInfo* pOperator);
int32_t hLeftJoinDo(struct SOperatorInfo* pOperator);
int32_t hFullJoinDo(struct SOperatorInfo* pOperator);
int32_t hSemiJoinDo(struct SOperatorInfo* pOperator);
int32_t hAntiJoinDo(struct SOperatorInfo* pOperator);
int32_t hJoinBuildHash(struct SOperatorInfo* pOperator, bool* queryDone);
int32_t hFullJoinBuildHash(struct SOperatorInfo* pOperator, bool* queryDone);
int32_t hJoinEmitBuildNMatchRows(SHJoinOperatorInfo* pJoin);
int32_t hJoinAppendResRowsToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, int32_t maxRows,
                                  SBufRowInfo** ppStart, bool* allFetched);
void hJoinSetDone(struct SOperatorInfo* pOperator);
int32_t hJoinAppendResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, bool* allFetched);
int32_t hJoinCopyKeyColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx, size_t* pBufLen, bool* pNullKey);
int32_t hJoinCopyMergeMidBlk(SHJoinOperatorInfo* pJoin, SSDataBlock** ppMid, SSDataBlock** ppFin);
int32_t hJoinHandleMidRemains(SHJoinOperatorInfo* pJoin, SHJoinCtx* pCtx);
bool    hJoinBlkReachThreshold(SHJoinOperatorInfo* pInfo, int64_t blkRows);
int32_t hJoinCopyNMatchRowsToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, int32_t startIdx, int32_t rows);

#if defined(BUILD_TEST)
void hJoinTestFailHashInitOnce(void);
void hJoinTestFailPagePushOnce(void);
void hJoinTestFailAfterPageGrowthOnce(void);
void hJoinTestResetFailureHooks(void);
#endif

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASHJOIN_H
