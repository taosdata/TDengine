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
#define HJOIN_BLK_THRESHOLD_RATIO 0.9

typedef int32_t (*hJoinImplFp)(SOperatorInfo*);

#pragma pack(push, 1)
typedef struct SBufRowInfo {
  void*    next;
  uint16_t pageId;
  int32_t  offset;
} SBufRowInfo;
#pragma pack(pop)

typedef enum EHJoinPhase {
  E_JOIN_PHASE_PRE = 1,
  E_JOIN_PHASE_CUR,
  E_JOIN_PHASE_POST
} EHJoinPhase;

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
} SHJoinCtx;

typedef struct SHJoinColInfo {
  int32_t          srcSlot;
  int32_t          dstSlot;
  bool             keyCol;
  bool             vardata;
  int32_t*         offset;
  int32_t          bytes;
  char*            data;
  char*            bitMap;
  SColumnInfoData* colData;
} SHJoinColInfo;

typedef struct SBufPageInfo {
  int32_t pageSize;
  int32_t offset;
  char*   data;
} SBufPageInfo;


typedef struct SGroupData {
  SBufRowInfo* rows;
} SGroupData;


typedef struct SHJoinColMap {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     vardata;
  int32_t  bytes;
} SHJoinColMap;

// for now timetruncate only
typedef struct SHJoinPrimExprCtx {
  int64_t truncateUnit;
  int64_t timezoneUnit;
  int32_t targetSlotId;
} SHJoinPrimExprCtx;

typedef struct SHJoinTableCtx {
  int32_t        downStreamIdx;
  SOperatorInfo* downStream;
  int32_t        blkId;
  SQueryStat     inputStat;
  bool           hasTimeRange;

  SHJoinColMap*      primCol;
  SNode*             primExpr;
  SHJoinPrimExprCtx  primCtx;
  
  int32_t        keyNum;
  SHJoinColInfo* keyCols;
  char*          keyBuf;
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
  int64_t buildBlkNum;
  int64_t buildBlkRows;
  int64_t probeBlkNum;
  int64_t probeBlkRows;
  int64_t resRows;
  int64_t expectRows;
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
  STimeWindow      tblTimeRange;
  int32_t          pResColNum;
  int8_t*          pResColMap;
  SArray*          pRowBufs;
  SSHashObj*       pKeyHash;
  bool             keyHashBuilt;
  SHJoinCtx        ctx;
  SHJoinExecInfo   execInfo;
  int32_t          blkThreshold;
  hJoinImplFp      joinFp;  
} SHJoinOperatorInfo;


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
void hJoinSetDone(struct SOperatorInfo* pOperator);
void hJoinAppendResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, bool* allFetched);
bool hJoinCopyKeyColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen);
int32_t hJoinCopyMergeMidBlk(SHJoinCtx* pCtx, SSDataBlock** ppMid, SSDataBlock** ppFin);
int32_t hJoinHandleMidRemains(SHJoinOperatorInfo* pJoin, SHJoinCtx* pCtx);
bool hJoinBlkReachThreshold(SHJoinOperatorInfo* pInfo, int64_t blkRows);
int32_t hJoinCopyNMatchRowsToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, int32_t startIdx, int32_t rows);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASHJOIN_H
