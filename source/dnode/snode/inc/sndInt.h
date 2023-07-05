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

#ifndef _TD_SNODE_INT_H_
#define _TD_SNODE_INT_H_

#include "os.h"

#include "tlog.h"
#include "tmsg.h"
#include "tqueue.h"
#include "trpc.h"
#include "tstream.h"

#include "snode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSnode {
  char*        path;
  SStreamMeta* pMeta;
  SMsgCb       msgCb;
} SSnode;

#if 0
typedef struct {
  SHashObj* pHash;  // taskId -> SStreamTask
} SStreamMeta;

SStreamMeta* sndMetaNew();
void         sndMetaDelete(SStreamMeta* pMeta);

int32_t      sndMetaDeployTask(SStreamMeta* pMeta, SStreamTask* pTask);
SStreamTask* sndMetaGetTask(SStreamMeta* pMeta, int32_t taskId);
int32_t      sndMetaRemoveTask(SStreamMeta* pMeta, int32_t taskId);

int32_t sndDropTaskOfStream(SStreamMeta* pMeta, int64_t streamId);
int32_t sndStopTaskOfStream(SStreamMeta* pMeta, int64_t streamId);
int32_t sndResumeTaskOfStream(SStreamMeta* pMeta, int64_t streamId);
#endif

void initStreamStateAPI(SStorageAPI* pAPI);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SNODE_INT_H_*/
