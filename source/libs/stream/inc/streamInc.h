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

#ifndef _STREAM_INC_H_
#define _STREAM_INC_H_

#include "executor.h"
#include "query.h"
#include "tstream.h"

#include "trpc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int8_t inited;
  void*  timer;
} SStreamGlobalEnv;

static SStreamGlobalEnv streamEnv;

int32_t streamDispatchStreamBlock(SStreamTask* pTask);

SStreamDataBlock* createStreamDataFromDispatchMsg(const SStreamDispatchReq* pReq, int32_t blockType, int32_t srcVg);
SStreamDataBlock* createStreamBlockFromResults(SStreamQueueItem* pItem, SStreamTask* pTask, int64_t resultSize, SArray* pRes);
void    destroyStreamDataBlock(SStreamDataBlock* pBlock);

int32_t streamRetrieveReqToData(const SStreamRetrieveReq* pReq, SStreamDataBlock* pData);
int32_t streamDispatchAllBlocks(SStreamTask* pTask, const SStreamDataBlock* data);

int32_t streamBroadcastToChildren(SStreamTask* pTask, const SSDataBlock* pBlock);

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq);

int32_t streamDispatchCheckMsg(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet);

int32_t streamDispatchOneRecoverFinishReq(SStreamTask* pTask, const SStreamRecoverFinishReq* pReq, int32_t vgId,
                                          SEpSet* pEpSet);

SStreamQueueItem* streamMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* pElem);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_INC_H_ */
