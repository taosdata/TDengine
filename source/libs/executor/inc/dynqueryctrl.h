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
#ifndef TDENGINE_DYNQUERYCTRL_H
#define TDENGINE_DYNQUERYCTRL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"
typedef struct SDynQueryCtrlExecInfo {
  int64_t prevBlkNum;
  int64_t prevBlkRows;
  int64_t postBlkNum;
  int64_t postBlkRows;
  int64_t leftCacheNum;
  int64_t rightCacheNum;
} SDynQueryCtrlExecInfo;

typedef struct SStbJoinTableList {
  void    *pNext;
  int64_t  uidNum;
  int64_t  readIdx;
  int32_t *pLeftVg;
  int64_t *pLeftUid;
  int32_t *pRightVg;
  int64_t *pRightUid;
} SStbJoinTableList;

typedef struct SStbJoinPrevJoinCtx {
  bool               joinBuild;
  SSHashObj*         leftHash;
  SSHashObj*         rightHash;
  SSHashObj*         leftCache;
  SSHashObj*         rightCache;
  SSHashObj*         onceTable;
  int64_t            tableNum;
  SStbJoinTableList* pListHead;
  SStbJoinTableList* pListTail;
} SStbJoinPrevJoinCtx;

typedef struct SStbJoinPostJoinCtx {
  bool    isStarted;
  bool    leftNeedCache;
  bool    rightNeedCache;
  int32_t leftVgId;
  int32_t rightVgId;
  int64_t leftCurrUid;
  int64_t rightCurrUid;
  int64_t rightNextUid;
} SStbJoinPostJoinCtx;

typedef struct SStbJoinDynCtrlCtx {
  SStbJoinPrevJoinCtx prev;
  SStbJoinPostJoinCtx post;
} SStbJoinDynCtrlCtx;

typedef struct SStbJoinDynCtrlInfo {
  SDynQueryCtrlExecInfo execInfo;
  SStbJoinDynCtrlBasic  basic;
  SStbJoinDynCtrlCtx    ctx;
  SDataBlockDescNode*   pOutputDataBlockDesc;
} SStbJoinDynCtrlInfo;

typedef struct SVtbScanDynCtrlInfo {
  bool             scanAllCols;
  char*            dbName;
  char*            stbName;
  tsem_t           ready;
  SEpSet           epSet;
  SUseDbRsp*       pRsp;
  uint64_t         suid;
  int32_t          acctId;
  int32_t          curTableIdx;
  int32_t          lastTableIdx;
  SArray*          readColList;
  SArray*          childTableList;
  SHashObj*        childTableMap;
  SHashObj*        dbVgInfoMap;
  SHashObj*        orgTbVgColMap;
  SMsgCb*          pMsgCb;
  SOperatorParam*  vtbScanParam;
} SVtbScanDynCtrlInfo;

typedef struct SDynQueryCtrlOperatorInfo {
  EDynQueryType         qType;
  union {
    SStbJoinDynCtrlInfo stbJoin;
    SVtbScanDynCtrlInfo vtbScan;
  };
} SDynQueryCtrlOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_DYNQUERYCTRL_H
