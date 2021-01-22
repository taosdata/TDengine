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
  SLocalDataSource **    pLocalDataSrc;
  int32_t                numOfBuffer;
  int32_t                numOfCompleted;
  int32_t                numOfVnode;
  SLoserTreeInfo *       pLoserTree;
  char *                 prevRowOfInput;
  tFilePage *            pResultBuf;
  int32_t                nResultBufSize;
  tFilePage *            pTempBuffer;
  struct SQLFunctionCtx *pCtx;
  int32_t                rowSize;      // size of each intermediate result.
  bool                   hasPrevRow;   // cannot be released
  bool                   hasUnprocessedRow;
  tOrderDescriptor *     pDesc;
  SColumnModel *         resColModel;
  SColumnModel*          finalModel;
  tExtMemBuffer **       pExtMemBuffer;      // disk-based buffer
  SFillInfo*             pFillInfo;          // interpolation support structure
  char*                  pFinalRes;          // result data after interpo
  tFilePage*             discardData;
  bool                   discard;
  int32_t                offset;             // limit offset value
  bool                   orderPrjOnSTable;   // projection query on stable
} SLocalMerger;

typedef struct SRetrieveSupport {
  tExtMemBuffer **  pExtMemBuffer;     // for build loser tree
  tOrderDescriptor *pOrderDescriptor;
  SColumnModel*     pFinalColModel;    // colModel for final result
  SColumnModel*     pFFColModel;
  int32_t           subqueryIndex;     // index of current vnode in vnode list
  SSqlObj *         pParentSql;
  tFilePage *       localBuffer;       // temp buffer, there is a buffer for each vnode to
  uint32_t          numOfRetry;        // record the number of retry times
} SRetrieveSupport;

int32_t tscLocalReducerEnvCreate(SSqlObj *pSql, tExtMemBuffer ***pMemBuffer, tOrderDescriptor **pDesc,
                                 SColumnModel **pFinalModel, SColumnModel** pFFModel, uint32_t nBufferSize);

void tscLocalReducerEnvDestroy(tExtMemBuffer **pMemBuffer, tOrderDescriptor *pDesc, SColumnModel *pFinalModel, SColumnModel* pFFModel,
                               int32_t numOfVnodes);

int32_t saveToBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, void *data,
                     int32_t numOfRows, int32_t orderType);

int32_t tscFlushTmpBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, int32_t orderType);

/*
 * create local reducer to launch the second-stage reduce process at client site
 */
void tscCreateLocalMerger(tExtMemBuffer **pMemBuffer, int32_t numOfBuffer, tOrderDescriptor *pDesc,
                           SColumnModel *finalModel, SColumnModel *pFFModel, SSqlObj* pSql);

void tscDestroyLocalMerger(SSqlObj *pSql);

int32_t tscDoLocalMerge(SSqlObj *pSql);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCLOCALMERGE_H
