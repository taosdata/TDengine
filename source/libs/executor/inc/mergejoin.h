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

#define MJOIN_DEFAULT_BLK_ROWS_NUM 4
#define MJOIN_HJOIN_CART_THRESHOLD 16
#define MJOIN_BLK_SIZE_LIMIT 20

struct SMJoinOperatorInfo;

typedef SSDataBlock* (*joinImplFp)(SOperatorInfo*);

typedef enum EJoinTableType {
  E_JOIN_TB_BUILD = 1,
  E_JOIN_TB_PROBE
} EJoinTableType;

#define MJOIN_TBTYPE(_type) (E_JOIN_TB_BUILD == (_type) ? "BUILD" : "PROBE")

typedef struct SMJoinRowPos {
  SSDataBlock* pBlk;
  int32_t      pos;
} SMJoinRowPos;

typedef struct SMJoinColMap {
  int32_t  srcSlot;
  int32_t  dstSlot;
} SMJoinColMap;

typedef struct SMJoinColInfo {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     keyCol;
  bool     vardata;
  int32_t* offset;
  int32_t  bytes;
  char*    data;
  char*    bitMap;
  SColumnInfoData* colData;  
} SMJoinColInfo;


typedef struct SMJoinTableCtx {
  EJoinTableType type;
  int32_t        downStreamIdx;
  SOperatorInfo* downStream;
  bool           dsInitDone;
  bool           dsFetchDone;

  int32_t        blkId;
  SQueryStat     inputStat;

  SMJoinColMap*  primCol;
  char*          primData;

  int32_t        finNum;
  SMJoinColMap*  finCols;
  
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

  bool           newBlk;
  SSDataBlock*   blk;
  int32_t        blkRowIdx;

  // merge join
  
  int64_t        grpTotalRows;
  int32_t        grpIdx;
  SArray*        eqGrps;
  SArray*        createdBlks;

  // hash join

  int32_t        grpArrayIdx;
  SArray*        pGrpArrays;

  int32_t        grpRowIdx;
  SArray*        pHashCurGrp;
  SSHashObj*     pGrpHash;  
} SMJoinTableCtx;

typedef struct SMJoinGrpRows {
  SSDataBlock* blk;
  int32_t      beginIdx;
  int32_t      endIdx;
  int32_t      readIdx;
  bool         readMatch;
} SMJoinGrpRows;

typedef struct SMJoinMergeCtx {
  struct SMJoinOperatorInfo* pJoin;
  bool                hashCan;
  bool                keepOrder;
  bool                grpRemains;
  bool                midRemains;
  bool                lastEqGrp;
  int32_t             blkThreshold;
  SSDataBlock*        midBlk;
  SSDataBlock*        finBlk;
  int64_t             lastEqTs;
  SMJoinGrpRows       probeNEqGrp;
  bool                hashJoin;
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
  int32_t          errCode;
  SMJoinTableCtx   tbs[2];
  SMJoinTableCtx*  build;
  SMJoinTableCtx*  probe;
  SFilterInfo*     pFPreFilter;
  SFilterInfo*     pPreFilter;
  SFilterInfo*     pFinFilter;
//  SMJoinFuncs*     joinFps;
  SMJoinCtx        ctx;
  SMJoinExecInfo   execInfo;
} SMJoinOperatorInfo;

#define MJOIN_DS_REQ_INIT(_pOp) ((_pOp)->pOperatorGetParam && ((SSortMergeJoinOperatorParam*)(_pOp)->pOperatorGetParam->value)->initDownstream)
#define MJOIN_DS_NEED_INIT(_pOp, _tbctx) (MJOIN_DS_REQ_INIT(_pOp) && (!(_tbctx)->dsInitDone))
#define MJOIN_TB_LOW_BLK(_tbctx) ((_tbctx)->blkNum <= 0 || ((_tbctx)->blkNum == 1 && (_tbctx)->pHeadBlk->cloned))

#define REACH_HJOIN_THRESHOLD(_prb, _bld) ((_prb)->grpTotalRows * (_bld)->grpTotalRows >= MJOIN_HJOIN_CART_THRESHOLD)

#define SET_SAME_TS_GRP_HJOIN(_pair, _octx) ((_pair)->hashJoin = (_octx)->hashCan && REACH_HJOIN_THRESHOLD(_pair))

#define LEFT_JOIN_NO_EQUAL(_order, _pts, _bts) ((_order) && (_pts) < (_bts)) || (!(_order) && (_pts) > (_bts))
#define LEFT_JOIN_DISCRAD(_order, _pts, _bts) ((_order) && (_pts) > (_bts)) || (!(_order) && (_pts) < (_bts))

#define GRP_REMAIN_ROWS(_grp) ((_grp)->endIdx - (_grp)->readIdx + 1)
#define GRP_DONE(_grp) ((_grp)->readIdx > (_grp)->endIdx)

#define MJOIN_PROBE_TB_ROWS_DONE(_tb) ((_tb)->blkRowIdx >= (_tb)->blk->info.rows)
#define MJOIN_BUILD_TB_ROWS_DONE(_tb) ((NULL == (_tb)->blk) || ((_tb)->blkRowIdx >= (_tb)->blk->info.rows))

#define BLK_IS_FULL(_blk) ((_blk)->info.rows == (_blk)->info.capacity)


#define MJOIN_GET_TB_COL_TS(_col, _ts, _tb)                                     \
  do {                                                                          \
    if (NULL != (_tb)->blk && (_tb)->blkRowIdx < (_tb)->blk->info.rows) {                                                   \
      (_col) = taosArrayGet((_tb)->blk->pDataBlock, (_tb)->primCol->srcSlot);   \
      (_ts) = *((int64_t*)(_col)->pData + (_tb)->blkRowIdx);                    \
    } else {                                                                    \
      (_ts) = INT64_MIN;                                                        \
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




int32_t mJoinInitMergeCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode);
SSDataBlock* mLeftJoinDo(struct SOperatorInfo* pOperator);
bool mJoinRetrieveImpl(SMJoinOperatorInfo* pJoin, int32_t* pIdx, SSDataBlock** ppBlk, SMJoinTableCtx* pTb);
void mJoinSetDone(SOperatorInfo* pOperator);
bool mJoinCopyKeyColsDataToBuf(SMJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen);
void mJoinBuildEqGroups(SMJoinTableCtx* pTable, int64_t timestamp, bool* wholeBlk, bool restart);
int32_t mJoinRetrieveEqGrpRows(SOperatorInfo* pOperator, SMJoinTableCtx* pTable, int64_t timestamp);
int32_t mJoinMakeBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTable);
int32_t mJoinSetKeyColsData(SSDataBlock* pBlock, SMJoinTableCtx* pTable);

  

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_MERGEJOIN_H
