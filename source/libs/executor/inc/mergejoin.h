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

#if 0
#define MJOIN_DEFAULT_BLK_ROWS_NUM 3 //4096
#define MJOIN_HJOIN_CART_THRESHOLD 10
#define MJOIN_BLK_SIZE_LIMIT 0 //10485760
#define MJOIN_ROW_BITMAP_SIZE (2 * 1048576)
#else
#define MJOIN_DEFAULT_BLK_ROWS_NUM 4096
#define MJOIN_HJOIN_CART_THRESHOLD 16
#define MJOIN_BLK_SIZE_LIMIT 10485760
#define MJOIN_ROW_BITMAP_SIZE (2 * 1048576)
#endif
#define MJOIN_SEMI_ANTI_BLK_ROWS_NUM 64
#define MJOIN_BLK_THRESHOLD_RATIO 0.9

struct SMJoinOperatorInfo;

typedef enum EJoinTableType {
  E_JOIN_TB_BUILD = 1,
  E_JOIN_TB_PROBE
} EJoinTableType;


#define MJOIN_TBTYPE(_type) (E_JOIN_TB_BUILD == (_type) ? "BUILD" : "PROBE")
#define IS_FULL_OUTER_JOIN(_jtype, _stype) ((_jtype) == JOIN_TYPE_FULL && (_stype) == JOIN_STYPE_OUTER)

typedef struct SMJoinRowPos {
  SSDataBlock* pBlk;
  int32_t      pos;
} SMJoinRowPos;

typedef struct SMJoinColMap {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     vardata;
  int32_t  bytes;
} SMJoinColMap;

typedef struct SMJoinColInfo {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     jsonData;
  bool     vardata;
  int32_t* offset;
  int32_t  bytes;
  char*    data;
  char*    bitMap;
  SColumnInfoData* colData;  
} SMJoinColInfo;

typedef struct SMJoinHashGrpRows {
  int32_t      rowBitmapOffset;
  int32_t      rowMatchNum;
  bool         allRowsMatch;
  bool         allRowsNMatch;
  SArray*      pRows;
} SMJoinHashGrpRows;


typedef struct SMJoinNMatchCtx {
  void*   pGrp;
  int32_t iter;
  int32_t bitIdx;
  int32_t rowIdx;
  int32_t grpIdx;
} SMJoinNMatchCtx;

// for now timetruncate only
typedef struct SMJoinPrimExprCtx {
  int64_t truncateUnit;
  int64_t timezoneUnit;
  int32_t targetSlotId;
} SMJoinPrimExprCtx;

typedef struct SMJoinTableCtx {
  EJoinTableType     type;
  int32_t            downStreamIdx;
  SOperatorInfo*     downStream;
  bool               dsInitDone;
  bool               dsFetchDone;
  SNode*             primExpr;
  SMJoinPrimExprCtx  primCtx;

  int32_t            blkId;
  SQueryStat         inputStat;

  uint64_t           lastInGid;
  SSDataBlock*       remainInBlk;

  SMJoinColMap*      primCol;
  char*              primData;

  int32_t            finNum;
  SMJoinColMap*      finCols;
  
  int32_t            keyNum;
  int32_t            keyNullSize;
  SMJoinColInfo*     keyCols;
  char*              keyBuf;
  char*              keyData;

  bool               newBlk;
  SSDataBlock*       blk;
  int32_t            blkRowIdx;

  // merge join
  
  int64_t            grpTotalRows;
  int32_t            grpIdx;
  bool               noKeepEqGrpRows;
  bool               multiEqGrpRows;
  int64_t            eqRowLimit;
  int64_t            eqRowNum;
  SArray*            eqGrps;
  SArray*            createdBlks;

  // hash join

  int32_t            grpArrayIdx;
  SArray*            pGrpArrays;

  bool               multiRowsGrp;
  int32_t            grpRowIdx;
  SArray*            pHashCurGrp;
  SMJoinHashGrpRows* pHashGrpRows;
  SSHashObj*         pGrpHash;  

  int64_t            rowBitmapSize;
  int64_t            rowBitmapOffset;
  char*              pRowBitmap;
  
  SMJoinNMatchCtx    nMatchCtx;
} SMJoinTableCtx;

typedef struct SMJoinMatchInfo {
  int32_t      rowBitmapOffset;
  int32_t      rowMatchNum;
  bool         allRowsNMatch;
  bool         allRowsMatch;
} SMJoinMatchInfo;

typedef struct SMJoinGrpRows {
  SSDataBlock* blk;
  int32_t      beginIdx;
  int32_t      endIdx;
  int32_t      readIdx;
  int32_t      rowBitmapOffset;
  int32_t      rowMatchNum;
  bool         allRowsNMatch;
  bool         allRowsMatch;
  bool         readMatch;
  bool         clonedBlk;
} SMJoinGrpRows;

#define MJOIN_COMMON_CTX \
  struct SMJoinOperatorInfo* pJoin; \
  bool                       ascTs; \
  bool                  grpRemains; \
  SSDataBlock*              finBlk; \
  bool                   lastEqGrp; \
  bool                lastProbeGrp; \
  bool                   seqWinGrp; \
  bool                   groupJoin; \
  int32_t             blkThreshold; \
  int64_t                    limit; \
  int64_t                   jLimit

typedef struct SMJoinCommonCtx {
  MJOIN_COMMON_CTX;
} SMJoinCommonCtx;

typedef int32_t (*joinCartFp)(void*);

typedef struct SMJoinMergeCtx {
  // KEEP IT FIRST
  struct SMJoinOperatorInfo* pJoin;
  bool                       ascTs;
  bool                  grpRemains;
  SSDataBlock*              finBlk;
  bool                   lastEqGrp;
  bool                lastProbeGrp;
  bool                   seqWinGrp;
  bool                   groupJoin;
  int32_t             blkThreshold;
  int64_t                    limit;
  int64_t                   jLimit;
  // KEEP IT FIRST
  
  bool                hashCan;
  bool                midRemains;
  bool                nmatchRemains;
  SSDataBlock*        midBlk;
  int64_t             lastEqTs;
  SMJoinGrpRows       probeNEqGrp;
  SMJoinGrpRows       buildNEqGrp;
  bool                hashJoin;
  joinCartFp          hashCartFp;
  joinCartFp          mergeCartFp;
} SMJoinMergeCtx;

typedef enum {
  E_CACHE_NONE = 0,
  E_CACHE_OUTBLK,
  E_CACHE_INBLK
} SMJoinCacheMode;

typedef struct SMJoinWinCache {
  int32_t                    pageLimit;
  int32_t                    outRowIdx;
  int32_t                    colNum;
  int32_t                    rowNum;
  int8_t                     grpIdx;
  SArray*                    grps;
  SArray*                    grpsQueue;
  SSDataBlock*               outBlk;
} SMJoinWinCache;

typedef int32_t (*joinMoveWin)(void*);

typedef struct SMJoinWindowCtx {
  // KEEP IT FIRST
  struct SMJoinOperatorInfo* pJoin;
  bool                       ascTs;
  bool                  grpRemains;
  SSDataBlock*              finBlk;
  bool                   lastEqGrp;
  bool                lastProbeGrp;
  bool                   seqWinGrp;
  bool                   groupJoin;
  int32_t             blkThreshold;
  int64_t                    limit;
  int64_t                   jLimit;
  // KEEP IT FIRST
  
  int32_t                    asofOpType;
  int64_t                    winBeginOffset;
  int64_t                    winEndOffset;
  bool                       lowerRowsAcq;
  bool                       eqRowsAcq;
  bool                       greaterRowsAcq;
  bool                       forwardRowsAcq;

  int64_t                    winBeginTs;
  int64_t                    winEndTs;
  joinMoveWin                moveWinBeginFp;
  joinMoveWin                moveWinEndFp;
  bool                       eqPostDone;
  int64_t                    lastTs;
  SMJoinGrpRows              probeGrp;
  SMJoinGrpRows              buildGrp;
  SMJoinWinCache             cache;
} SMJoinWindowCtx;


typedef struct SMJoinFlowFlags {
  bool mergeJoin;
  bool windowJoin;
  bool preFilter;
  bool retrieveAfterBuildDone;
} SMJoinFlowFlags;

typedef struct SMJoinCtx {
  SMJoinFlowFlags* pFlags;
  bool             mergeCtxInUse;
  union {
    SMJoinMergeCtx  mergeCtx;
    SMJoinWindowCtx windowCtx;
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


typedef SSDataBlock* (*joinImplFp)(SOperatorInfo*);
typedef SSDataBlock* (*joinRetrieveFp)(struct SMJoinOperatorInfo*, SMJoinTableCtx*);
typedef void (*joinResetFp)(struct SMJoinOperatorInfo*);


typedef struct SMJoinOperatorInfo {
  SOperatorInfo*    pOperator;
  int32_t           joinType;
  int32_t           subType;
  int32_t           inputTsOrder;
  int32_t           errCode;  
  int64_t           outGrpId;
  uint64_t          outBlkId;
  SMJoinTableCtx    tbs[2];
  SMJoinTableCtx*   build;
  SMJoinTableCtx*   probe;
  SFilterInfo*      pFPreFilter;
  SFilterInfo*      pPreFilter;
  SFilterInfo*      pFinFilter;
  joinImplFp        joinFp;
  joinRetrieveFp    retrieveFp;
  joinResetFp       grpResetFp;
  SMJoinCtx         ctx;
  SMJoinExecInfo    execInfo;
} SMJoinOperatorInfo;

#define MJOIN_DS_REQ_INIT(_pOp) ((_pOp)->pOperatorGetParam && ((SSortMergeJoinOperatorParam*)(_pOp)->pOperatorGetParam->value)->initDownstream)
#define MJOIN_DS_NEED_INIT(_pOp, _tbctx) (MJOIN_DS_REQ_INIT(_pOp) && (!(_tbctx)->dsInitDone))
#define MJOIN_TB_LOW_BLK(_tbctx) ((_tbctx)->blkNum <= 0 || ((_tbctx)->blkNum == 1 && (_tbctx)->pHeadBlk->cloned))

#define REACH_HJOIN_THRESHOLD(_prb, _bld) ((_bld)->grpTotalRows >= MJOIN_HJOIN_CART_THRESHOLD)

#define SET_SAME_TS_GRP_HJOIN(_pair, _octx) ((_pair)->hashJoin = (_octx)->hashCan && REACH_HJOIN_THRESHOLD(_pair))

#define PROBE_TS_NMATCH(_asc, _pts, _bts) (((_asc) && (_pts) < (_bts)) || (!(_asc) && (_pts) > (_bts)))
#define PROBE_TS_NREACH(_asc, _pts, _bts) (((_asc) && (_pts) > (_bts)) || (!(_asc) && (_pts) < (_bts)))
#define MJOIN_BUILD_BLK_OOR(_asc, _pts, _pidx, _bts, _bnum) (((_asc) && (*((int64_t*)(_pts) + (_pidx)) > *((int64_t*)(_bts) + (_bnum) - 1))) || ((!(_asc)) && (*((int64_t*)(_pts) + (_pidx)) < *((int64_t*)(_bts) + (_bnum) - 1))))

#define GRP_REMAIN_ROWS(_grp) ((_grp)->endIdx - (_grp)->readIdx + 1)
#define GRP_DONE(_grp) ((_grp)->readIdx > (_grp)->endIdx)

#define MJOIN_PROBE_TB_ROWS_DONE(_tb) ((_tb)->blkRowIdx >= (_tb)->blk->info.rows)
#define FJOIN_PROBE_TB_ROWS_DONE(_tb) ((NULL == (_tb)->blk) || ((_tb)->blkRowIdx >= (_tb)->blk->info.rows))
#define MJOIN_BUILD_TB_ROWS_DONE(_tb) ((NULL == (_tb)->blk) || ((_tb)->blkRowIdx >= (_tb)->blk->info.rows))
#define MJOIN_TB_GRP_ROWS_DONE(_tb, _grpJoin) ((_tb)->dsFetchDone || ((_grpJoin) && NULL == (_tb)->blk && NULL != (_tb)->remainInBlk))

#define BLK_IS_FULL(_blk) ((_blk)->info.rows == (_blk)->info.capacity)

#define MJOIN_ROW_BITMAP_SET(_b, _base, _idx) (!colDataIsNull_f((_b + _base), _idx))
#define MJOIN_SET_ROW_BITMAP(_b, _base, _idx) colDataClearNull_f((_b + _base), _idx)

#define ASOF_EQ_ROW_INCLUDED(_op) (OP_TYPE_GREATER_EQUAL == (_op) || OP_TYPE_LOWER_EQUAL == (_op) || OP_TYPE_EQUAL == (_op))
#define ASOF_LOWER_ROW_INCLUDED(_op) (OP_TYPE_GREATER_EQUAL == (_op) || OP_TYPE_GREATER_THAN == (_op))
#define ASOF_GREATER_ROW_INCLUDED(_op) (OP_TYPE_LOWER_EQUAL == (_op) || OP_TYPE_LOWER_THAN == (_op))
#define WIN_ONLY_EQ_ROW_INCLUDED(_soff, _eoff) (0 == ((SValueNode*)(_soff))->datum.i && 0 == ((SValueNode*)(_eoff))->datum.i)

#define MJOIN_PUSH_BLK_TO_CACHE(_cache, _blk)                                   \
  do {                                                                          \
    SMJoinGrpRows* pGrp = (SMJoinGrpRows*)taosArrayReserve((_cache)->grps, 1);   \
    (_cache)->rowNum += (_blk)->info.rows;                                       \
    pGrp->blk = (_blk);                                                         \
    pGrp->beginIdx = 0;                                        \
    pGrp->endIdx = (_blk)->info.rows - 1;                       \
  } while (0)

#define MJOIN_RESTORE_TB_BLK(_cache, _tb)                                         \
  do {                                                                          \
    SMJoinGrpRows* pGrp = taosArrayGet((_cache)->grps, 0);              \
    if (NULL != pGrp) {         \
      (_tb)->blk = pGrp->blk;    \
      (_tb)->blkRowIdx = pGrp->beginIdx;                      \
    } else {                                                                    \
      (_tb)->blk = NULL;    \
      (_tb)->blkRowIdx = 0;                    \
    }                                                                           \
  } while (0)

#define MJOIN_SAVE_TB_BLK(_cache, _tb)                                         \
  do {                                                                          \
    SMJoinGrpRows* pGrp = taosArrayGet((_cache)->grps, 0);              \
    if (NULL != pGrp) {                                               \
      pGrp->beginIdx = (_tb)->blkRowIdx;                              \
      pGrp->readIdx = pGrp->beginIdx;                                       \
    }                                                                   \
  } while (0)  

#define MJOIN_POP_TB_BLK(_cache)                                           \
  do {                                                                          \
    SMJoinGrpRows* pGrp = taosArrayGet((_cache)->grps, 0);              \
    if (NULL != pGrp) {         \
      if (pGrp->blk == (_cache)->outBlk) {                                              \
        blockDataCleanup(pGrp->blk);                                 \
      }                                                               \
      taosArrayPopFrontBatch((_cache)->grps, 1);                    \
    }                                                               \
  } while (0)

#define MJOIN_GET_TB_COL_TS(_col, _ts, _tb)                                     \
  do {                                                                          \
    if (NULL != (_tb)->blk && (_tb)->blkRowIdx < (_tb)->blk->info.rows) {       \
      (_col) = taosArrayGet((_tb)->blk->pDataBlock, (_tb)->primCtx.targetSlotId);   \
      (_ts) = *((int64_t*)(_col)->pData + (_tb)->blkRowIdx);                    \
    } else {                                                                    \
      (_ts) = INT64_MAX;                                                        \
    }                                                                           \
  } while (0)

#define MJOIN_GET_TB_CUR_TS(_col, _ts, _tb)                                     \
    do {                                                                        \
      (_ts) = *((int64_t*)(_col)->pData + (_tb)->blkRowIdx);                    \
    } while (0)


#define MJ_ERR_RET(c)                 \
  do {                                \
    int32_t _code = (c);              \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)

#define MJ_ERR_JRET(c)               \
  do {                               \
    code = (c);                      \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

#define MJ_RET(c)                     \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)



void mJoinDestroyMergeCtx(SMJoinOperatorInfo* pJoin);
void mJoinDestroyWindowCtx(SMJoinOperatorInfo* pJoin);
int32_t mJoinInitWindowCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode);
int32_t mJoinInitMergeCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode);
void mWinJoinResetWindowCache(SMJoinWindowCtx* pCtx, SMJoinWinCache* pCache);
SSDataBlock* mInnerJoinDo(struct SOperatorInfo* pOperator);
SSDataBlock* mLeftJoinDo(struct SOperatorInfo* pOperator);
SSDataBlock* mFullJoinDo(struct SOperatorInfo* pOperator);
SSDataBlock* mSemiJoinDo(struct SOperatorInfo* pOperator);
SSDataBlock* mAntiJoinDo(struct SOperatorInfo* pOperator);
SSDataBlock* mWinJoinDo(struct SOperatorInfo* pOperator);
void mJoinResetGroupTableCtx(SMJoinTableCtx* pCtx);
void mJoinResetTableCtx(SMJoinTableCtx* pCtx);
void mLeftJoinGroupReset(SMJoinOperatorInfo* pJoin);
void mWinJoinGroupReset(SMJoinOperatorInfo* pJoin);
bool mJoinRetrieveBlk(SMJoinOperatorInfo* pJoin, int32_t* pIdx, SSDataBlock** ppBlk, SMJoinTableCtx* pTb);
void mJoinSetDone(SOperatorInfo* pOperator);
bool mJoinIsDone(SOperatorInfo* pOperator);
bool mJoinCopyKeyColsDataToBuf(SMJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen);
int32_t mJoinBuildEqGroups(SMJoinTableCtx* pTable, int64_t timestamp, bool* wholeBlk, bool restart);
int32_t mJoinRetrieveEqGrpRows(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTable, int64_t timestamp);
int32_t mJoinCreateFullBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTable);
int32_t mJoinCreateBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTable);
int32_t mJoinSetKeyColsData(SSDataBlock* pBlock, SMJoinTableCtx* pTable);
int32_t mJoinProcessEqualGrp(SMJoinMergeCtx* pCtx, int64_t timestamp, bool lastBuildGrp);
int32_t mJoinHashGrpCart(SSDataBlock* pBlk, SMJoinGrpRows* probeGrp, bool append, SMJoinTableCtx* probe, SMJoinTableCtx* build, bool* cont);
int32_t mJoinMergeGrpCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pFirst, SMJoinGrpRows* pSecond);
int32_t mJoinHandleMidRemains(SMJoinMergeCtx* pCtx);
int32_t mJoinNonEqGrpCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pGrp, bool probeGrp);
int32_t mJoinNonEqCart(SMJoinCommonCtx* pCtx, SMJoinGrpRows* pGrp, bool probeGrp, bool singleProbeRow);
int32_t mJoinCopyMergeMidBlk(SMJoinMergeCtx* pCtx, SSDataBlock** ppMid, SSDataBlock** ppFin);
int32_t mJoinFilterAndMarkRows(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SMJoinTableCtx* build, int32_t startGrpIdx, int32_t startRowIdx);
int32_t mJoinFilterAndMarkHashRows(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SMJoinTableCtx* build, int32_t startRowIdx);
int32_t mJoinGetRowBitmapOffset(SMJoinTableCtx* pTable, int32_t rowNum, int32_t *rowBitmapOffset);
int32_t mJoinProcessLowerGrp(SMJoinMergeCtx* pCtx, SMJoinTableCtx* pTb, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs);
int32_t mJoinProcessGreaterGrp(SMJoinMergeCtx* pCtx, SMJoinTableCtx* pTb, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs);
int32_t mJoinFilterAndKeepSingleRow(SSDataBlock* pBlock, SFilterInfo* pFilterInfo);
int32_t mJoinFilterAndNoKeepRows(SSDataBlock* pBlock, SFilterInfo* pFilterInfo);
int32_t mJoinBuildEqGrp(SMJoinTableCtx* pTable, int64_t timestamp, bool* wholeBlk, SMJoinGrpRows* pGrp);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_MERGEJOIN_H
