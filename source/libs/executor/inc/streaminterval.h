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
#ifndef STREAM_NONBLOCKINTERVAL_H
#define STREAM_NONBLOCKINTERVAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"

int32_t doStreamIntervalNonblockAggImpl(struct SOperatorInfo* pOperator, SSDataBlock* pBlock);
STimeWindow getFinalTimeWindow(int64_t ts, SInterval* pInterval);
int32_t getNextQualifiedFinalWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                                    TSKEY* primaryKeys, int32_t prevPosition);
int32_t createFinalIntervalSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                             SReadHandle* pHandle, SOperatorInfo** ppOptInfo);
int32_t createSemiIntervalSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                             SReadHandle* pHandle, SOperatorInfo** ppOptInfo);
int32_t getHistoryRemainResultInfo(SStreamAggSupporter* pAggSup, SArray* pUpdated, int32_t capacity);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_NONBLOCKINTERVAL_H
