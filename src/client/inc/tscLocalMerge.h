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

#ifndef TDENGINE_TSCLOCALMERGE_H
#define TDENGINE_TSCLOCALMERGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "qExtbuffer.h"
#include "qFill.h"
#include "taosmsg.h"
#include "tlosertree.h"
#include "tsclient.h"

#define MAX_NUM_OF_SUBQUERY_RETRY 3
  
struct SQLFunctionCtx;

typedef struct SLocalDataSource {
  tExtMemBuffer *pMemBuffer;
  int32_t        flushoutIdx;
  int32_t        pageId;
  int32_t        rowIdx;
  tFilePage      filePage;
} SLocalDataSource;

typedef struct SLocalMerger {
  SLocalDataSource     **pLocalDataSrc;
  int32_t                numOfBuffer;
  int32_t                numOfCompleted;
  int32_t                numOfVnode;
  SLoserTreeInfo        *pLoserTree;
  int32_t                rowSize;          // size of each intermediate result.
  tOrderDescriptor      *pDesc;
  tExtMemBuffer        **pExtMemBuffer;    // disk-based buffer
  char                  *buf;              // temp buffer
} SLocalMerger;

typedef struct SRetrieveSupport {
  tExtMemBuffer **  pExtMemBuffer;     // for build loser tree
  tOrderDescriptor *pOrderDescriptor;
  int32_t           subqueryIndex;     // index of current vnode in vnode list
  SSqlObj *         pParentSql;
  tFilePage *       localBuffer;       // temp buffer, there is a buffer for each vnode to
  uint32_t          numOfRetry;        // record the number of retry times
} SRetrieveSupport;

int32_t tscLocalReducerEnvCreate(SQueryInfo* pQueryInfo, tExtMemBuffer ***pMemBuffer, int32_t numOfSub, tOrderDescriptor **pDesc, uint32_t nBufferSize, int64_t id);

void tscLocalReducerEnvDestroy(tExtMemBuffer **pMemBuffer, tOrderDescriptor *pDesc, int32_t numOfVnodes);

int32_t saveToBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, void *data,
                     int32_t numOfRows, int32_t orderType);

int32_t tscFlushTmpBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, int32_t orderType);

/*
 * create local reducer to launch the second-stage reduce process at client site
 */
int32_t tscCreateLocalMerger(tExtMemBuffer **pMemBuffer, int32_t numOfBuffer, tOrderDescriptor *pDesc,
                          SQueryInfo *pQueryInfo, SLocalMerger **pMerger, int64_t id);

void tscDestroyLocalMerger(SLocalMerger* pLocalMerger);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCLOCALMERGE_H
