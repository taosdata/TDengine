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

#define MJOIN_DEFAULT_BLK_ROWS_NUM 4096
#define MJOIN_HJOIN_CART_THRESHOLD 16
#define MJOIN_BLK_SIZE_LIMIT 10485760

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


typedef struct SMJoinTableInfo {
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

  SSDataBlock*   blk;
  int32_t        blkRowIdx;

  int64_t        grpRowsNum;
  int64_t        grpRemainRows;
  int32_t        grpIdx;
  SArray*        eqGrps;
  SArray*        createdBlks;

  int32_t        grpArrayIdx;
  SArray*        pGrpArrays;

  // hash join
  int32_t        grpRowIdx;
  SArray*        pHashCurGrp;
  SSHashObj*     pGrpHash;  
} SMJoinTableInfo;

typedef struct SMJoinGrpRows {
  SSDataBlock* blk;
  int32_t      beginIdx;
  int32_t      endIdx;
  int32_t      readIdx;
  bool         readMatch;
} SMJoinGrpRows;

typedef struct SMJoinMergeCtx {
  bool          hashCan;
  bool          keepOrder;
  bool          grpRemains;
  bool          midRemains;
  bool          eqCart;
  bool          noColCond;
  int32_t       blksCapacity;
  SSDataBlock*  midBlk;
  SSDataBlock*  finBlk;
  SSDataBlock*  resBlk;
  int64_t       lastEqTs;
  SMJoinGrpRows probeNEqGrp;
  bool          hashJoin;
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
  SMJoinTableInfo* build;
  SMJoinTableInfo* probe;
  SSDataBlock*     pResBlk;
  int32_t          pResColNum;
  int8_t*          pResColMap;
  SFilterInfo*     pFPreFilter;
  SFilterInfo*     pPreFilter;
  SFilterInfo*     pFinFilter;
  SMJoinFuncs*     joinFps;
  SMJoinCtx        ctx;
  SMJoinExecInfo   execInfo;
} SMJoinOperatorInfo;

#define MJOIN_DS_REQ_INIT(_pOp) ((_pOp)->pOperatorGetParam && ((SSortMergeJoinOperatorParam*)(_pOp)->pOperatorGetParam->value)->initDownstream)
#define MJOIN_DS_NEED_INIT(_pOp, _tbctx) (MJOIN_DS_REQ_INIT(_pOp) && (!(_tbctx)->dsInitDone))
#define MJOIN_TB_LOW_BLK(_tbctx) ((_tbctx)->blkNum <= 0 || ((_tbctx)->blkNum == 1 && (_tbctx)->pHeadBlk->cloned))

#define REACH_HJOIN_THRESHOLD(_prb, _bld) ((_prb)->grpRowsNum * (_bld)->grpRowsNum > MJOIN_HJOIN_CART_THRESHOLD)

#define SET_SAME_TS_GRP_HJOIN(_pair, _octx) ((_pair)->hashJoin = (_octx)->hashCan && REACH_HJOIN_THRESHOLD(_pair))

#define LEFT_JOIN_NO_EQUAL(_order, _pts, _bts) ((_order) && (_pts) < (_bts)) || (!(_order) && (_pts) > (_bts))
#define LEFT_JOIN_DISCRAD(_order, _pts, _bts) ((_order) && (_pts) > (_bts)) || (!(_order) && (_pts) < (_bts))

#define GRP_REMAIN_ROWS(_grp) ((_grp)->endIdx - (_grp)->readIdx + 1)
#define GRP_DONE(_grp) ((_grp)->readIdx > (_grp)->endIdx)

#define BLK_IS_FULL(_blk) ((_blk)->info.rows == (_blk)->info.capacity)


#define SET_TABLE_CUR_TS(_col, _ts, _tb)                                      \
  do {                                                                        \
    (_col) = taosArrayGet((_tb)->blk->pDataBlock, (_tb)->primCol->srcSlot);   \
    (_ts) = *((int64_t*)(_col)->pData + (_tb)->blkRowIdx);                    \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_MERGEJOIN_H
