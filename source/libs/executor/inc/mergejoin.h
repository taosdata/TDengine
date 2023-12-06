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
#ifndef TDENGINE_MERGEJOIN_H
#define TDENGINE_MERGEJOIN_H

#ifdef __cplusplus
extern "C" {
#endif

typedef SSDataBlock* (*joinImplFp)(SOperatorInfo*);

typedef enum EJoinTableType {
  E_JOIN_TB_BUILD = 1,
  E_JOIN_TB_PROBE
} EJoinTableType;

typedef enum EJoinPhase {
  E_JOIN_PHASE_RETRIEVE,
  E_JOIN_PHASE_SPLIT,
  E_JOIN_PHASE_OUTPUT,
  E_JOIN_PHASE_DONE
} EJoinPhase;

typedef struct SMJoinColInfo {
  int32_t  srcSlot;
  int32_t  dstSlot;
} SMJoinColInfo;

typedef struct SMJoinCartCtx {
  SSDataBlock*   pResBlk;

  int32_t        firstColNum;
  SMJoinColInfo* pFirstCols;
  SSDataBlock*   pFirstBlk;
  int32_t        firstRowIdx;
  int32_t        firstRowNum;

  int32_t        secondColNum;
  SMJoinColInfo* pSecondCols;
  SSDataBlock*   pSecondBlk;
  int32_t        secondRowIdx;
  int32_t        secondRowNum;
} SMJoinCartCtx;

typedef struct SMJoinBlkInfo {
  bool         cloned;
  SSDataBlock* pBlk;
  void*        pNext;
} SMJoinBlkInfo;

typedef struct SMJoinRowInfo {
  int64_t blkId;
  int32_t rowIdx;
  int64_t rowGIdx;
} SMJoinRowInfo;

typedef struct SMJoinTableInfo {
  int32_t        downStreamIdx;
  SOperatorInfo* downStream;
  int32_t        blkId;
  SQueryStat     inputStat;

  SMJoinColInfo* primCol;
  char*          primData;

  int32_t        finNum;
  SMJoinColInfo* finCols;

  
  int32_t        keyNum;
  SMJoinColInfo* keyCols;
  char*          keyBuf;
  char*          keyData;
  
  int32_t        valNum;
  SMJoinColInfo* valCols;
  char*          valData;
  int32_t        valBitMapSize;
  int32_t        valBufSize;
  SArray*        valVarCols;
  bool           valColExist;
} SMJoinTableInfo;

typedef struct SMJoinTsJoinCtx {
  SMJoinTableCtx* pProbeCtx;
  SMJoinTableCtx* pBuildCtx;
  int64_t         probeRowNum;
  int64_t         buildRowNum;
  int64_t*        probeTs;
  int64_t*        buildTs;
  int64_t*        probeEndTs;
  int64_t*        buildEndTs;
  bool            inSameTsGrp;
  bool            inDiffTsGrp;
  bool            nextProbeRow;
  SGrpPairRes*    pLastGrpPair;
  SGrpPairRes     currGrpPair;
} SMJoinTsJoinCtx;

typedef struct SBuildGrpResIn {
  bool             multiBlkGrp;
  SMJoinBlkInfo*   grpBeginBlk;
  int32_t          grpRowBeginIdx;
  int32_t          grpRowNum;
} SBuildGrpResIn;

typedef struct SBuildGrpResOut {
  bool             hashJoin;
  SSHashObj*       pGrpHash;
  int32_t          grpRowReadIdx;
  int32_t          grpRowGReadIdx;
} SBuildGrpResOut;

typedef struct SProbeGrpResIn {
  bool             allRowsGrp;
  SMJoinBlkInfo*   grpBeginBlk;
  int32_t          grpRowBeginIdx;
  int32_t          grpRowNum;
  int64_t          grpLastTs;
} SProbeGrpResIn;

typedef struct SProbeGrpResOut {
  int32_t          grpRowReadIdx;
} SProbeGrpResOut;

typedef struct SGrpPairRes {
  bool            sameTsGrp;
  bool            finishGrp;
  SProbeGrpResIn  probeIn;
  SBuildGrpResIn  buildIn;

  /* KEEP THIS PART AT THE END */
  bool            outBegin;
  SBuildGrpResOut buildOut;
  SProbeGrpResOut probeOut;
  /* KEEP THIS PART AT THE END */
} SGrpPairRes;

#define GRP_PAIR_INIT_SIZE (sizeof(SGrpPairRes) - sizeof(bool) - sizeof(SBuildGrpResOut) - sizeof(SProbeGrpResOut))

typedef struct SMJoinOutputCtx {
  int32_t       grpReadIdx;
  SMJoinCartCtx cartCtx;
  SArray*       pGrpResList;
} SMJoinOutputCtx;

typedef struct SMJoinTableCtx {
  EJoinTableType   type;
  void*            blkFetchedFp;
  SMJoinTableInfo* pTbInfo;
  bool             dsInitDone;
  bool             dsFetchDone;
  int64_t          blkCurTs;
  int32_t          blkRowIdx;
  int64_t          blkIdx;
  int64_t          blkNum;
  SMJoinBlkInfo*   pCurrBlk;
  SMJoinBlkInfo*   pHeadBlk;
  SMJoinBlkInfo*   pTailBlk;
} SMJoinTableCtx;

typedef struct SMJoinMergeCtx {
  bool             hashJoin;
  EJoinPhase       joinPhase;
  int64_t          grpCurTs;
  SMJoinOutputCtx  outputCtx;
  SMJoinTsJoinCtx  tsJoinCtx;  
  SMJoinTableCtx   buildTbCtx;
  SMJoinTableCtx   probeTbCtx;
} SMJoinMergeCtx;

typedef struct SMJoinWinCtx {

} SMJoinWinCtx;


typedef struct SMJoinFlowFlags {
  bool mergeJoin;
  bool windowJoin;
  bool preFilter;
  bool retrieveAfterBuildDone;
} SMJoinFlowFlags;

typedef struct SMJoinCtx {
  SMJoinFlowFlags* pFlags;
  union {
    SMJoinMergeCtx mergeCtx;
    SMJoinWinCtx   winCtx;
  };

} SMJoinCtx;

typedef struct SMJoinExecInfo {
  int64_t buildBlkNum;
  int64_t buildBlkRows;
  int64_t probeBlkNum;
  int64_t probeBlkRows;
  int64_t resRows;
  int64_t expectRows;
} SMJoinExecInfo;


typedef struct SMJoinOperatorInfo {
  SOperatorInfo*   pOperator;
  int32_t          joinType;
  int32_t          subType;
  int32_t          inputTsOrder;  
  SMJoinTableInfo  tbs[2];
  SMJoinTableInfo* pBuild;
  SMJoinTableInfo* pProbe;
  SSDataBlock*     pRes;
  int32_t          pResColNum;
  int8_t*          pResColMap;
  SFilterInfo*     pPreFilter;
  SFilterInfo*     pFinFilter;
  SMJoinFuncs      joinFps;
  SMJoinCtx        ctx;
  SMJoinExecInfo   execInfo;
} SMJoinOperatorInfo;

#define MJOIN_DS_REQ_INIT(_pOp) ((_pOp)->pOperatorGetParam && ((SSortMergeJoinOperatorParam*)(_pOp)->pOperatorGetParam->value)->initDownstream)
#define MJOIN_DS_NEED_INIT(_pOp, _tbctx) (MJOIN_DS_REQ_INIT(_pOp) && (!(_tbctx)->dsInitDone))
#define MJOIN_TB_LOW_BLK(_tbctx) ((_tbctx)->blkNum <= 0 || ((_tbctx)->blkNum == 1 && (_tbctx)->pHeadBlk->cloned))

#define START_NEW_GRP(_ctx) memset(&(_ctx)->currGrpPair, 0, GRP_PAIR_INIT_SIZE)

#define FIN_SAME_TS_GRP(_ctx, _octx, _done) do {                                             \
    if ((_ctx)->inSameTsGrp) {                                                               \
      (_ctx)->currGrpPair.sameTsGrp = true;                                                  \
      (_ctx)->currGrpPair.finishGrp = (_done);                                               \
      (_ctx)->inSameTsGrp = false;                                                           \
      (_ctx)->pLastGrpPair = taosArrayPush((_octx)->pGrpResList, &(_ctx)->currGrpPair);      \
    }                                                                                        \
  } while (0)

#define FIN_DIFF_TS_GRP(_ctx, _octx, _done) do {                                             \
    if ((_ctx)->inDiffTsGrp) {                                                               \
      (_ctx)->currGrpPair.sameTsGrp = false;                                                 \
      (_ctx)->currGrpPair.finishGrp = true;                                                  \
      (_ctx)->currGrpPair.probeIn.allRowsGrp= (_done);                                       \
      (_ctx)->inDiffTsGrp = false;                                                           \
      (_ctx)->pLastGrpPair = taosArrayPush((_octx)->pGrpResList, &(_ctx)->currGrpPair);      \
    } else if (_done) {                                                                      \
      (_ctx)->currGrpPair.sameTsGrp = false;                                                 \
      (_ctx)->currGrpPair.finishGrp = true;                                                  \
      (_ctx)->currGrpPair.probeIn.grpRowBeginIdx = (_ctx)->pProbeCtx->blkRowIdx;             \
      (_ctx)->currGrpPair.probeIn.allRowsGrp = true;                                         \
    }                                                                                        \
  } while (0)


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_MERGEJOIN_H
