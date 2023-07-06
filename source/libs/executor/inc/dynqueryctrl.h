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

typedef struct SDynQueryCtrlExecInfo {
  int64_t prevBlkNum;
  int64_t prevBlkRows;
  int64_t postBlkNum;
  int64_t postBlkRows;
} SDynQueryCtrlExecInfo;

typedef struct SStbJoinPrevJoinCtx {
  SSDataBlock* pLastBlk;
  int32_t      lastRow;
} SStbJoinPrevJoinCtx;

typedef struct SStbJoinPostJoinCtx {
  bool isStarted;
} SStbJoinPostJoinCtx;

typedef struct SStbJoinDynCtrlCtx {
  SStbJoinPrevJoinCtx prev;
  SStbJoinPostJoinCtx post;
} SStbJoinDynCtrlCtx;

typedef struct SStbJoinDynCtrlInfo {
  SStbJoinDynCtrlBasic basic;
  SStbJoinDynCtrlCtx   ctx;
} SStbJoinDynCtrlInfo;

typedef struct SDynQueryCtrlOperatorInfo {
  EDynQueryType         qType;
  SDynQueryCtrlExecInfo execInfo;
  union {
    SStbJoinDynCtrlInfo stbJoin;
  };
} SDynQueryCtrlOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_DYNQUERYCTRL_H
