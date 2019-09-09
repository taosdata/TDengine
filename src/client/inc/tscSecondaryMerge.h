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

#ifndef TDENGINE_TSCSECONARYMERGE_H
#define TDENGINE_TSCSECONARYMERGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosmsg.h"
#include "textbuffer.h"
#include "tinterpolation.h"
#include "tlosertree.h"
#include "tsclient.h"

#define MAX_NUM_OF_SUBQUERY_RETRY 3

/*
 * @version 0.1
 * @date   2018/01/05
 * @author liaohj
 * management of client-side reducer for metric query
 */

struct SQLFunctionCtx;

typedef struct SLocalDataSource {
  tExtMemBuffer *pMemBuffer;
  int32_t        flushoutIdx;
  int32_t        pageId;
  int32_t        rowIdx;
  tFilePage      filePage;
} SLocalDataSource;

enum {
  TSC_LOCALREDUCE_READY = 0x0,
  TSC_LOCALREDUCE_IN_PROGRESS = 0x1,
  TSC_LOCALREDUCE_TOBE_FREED = 0x2,
};

typedef struct SLocalReducer {
  SLocalDataSource **pLocalDataSrc;
  int32_t         numOfBuffer;
  int32_t         numOfCompleted;

  int32_t numOfVnode;

  SLoserTreeInfo *pLoserTree;
  char *          prevRowOfInput;

  tFilePage *pResultBuf;
  int32_t    nResultBufSize;

  char *pBufForInterpo;  // intermediate buffer for interpolation

  tFilePage *pTempBuffer;

  struct SQLFunctionCtx *pCtx;

  int32_t rowSize;  // size of each intermediate result.
  int32_t status;   // denote it is in reduce process, in reduce process, it
  // cannot be released
  bool hasPrevRow;
  bool hasUnprocessedRow;

  tOrderDescriptor *pDesc;
  tColModel *       resColModel;

  tExtMemBuffer **   pExtMemBuffer;      // disk-based buffer
  SInterpolationInfo interpolationInfo;  // interpolation support structure

  char *pFinalRes;  // result data after interpo

  tFilePage *discardData;
  bool       discard;

  int32_t offset;
} SLocalReducer;

typedef struct SRetrieveSupport {
  tExtMemBuffer **  pExtMemBuffer;  // for build loser tree
  tOrderDescriptor *pOrderDescriptor;
  tColModel *       pFinalColModel;  // colModel for final result

  /*
   * shared by all subqueries
   * It is the number of completed retrieval subquery.
   * once this value equals to numOfVnodes, all retrieval are completed.
   * Local merge is launched.
   */
  int32_t *numOfFinished;
  int32_t  numOfVnodes;  // total number of vnode
  int32_t  vnodeIdx;     // index of current vnode in vnode list

  /*
   * shared by all subqueries
   * denote the status of query on vnode, if code!=0, all following
   * retrieval on vnode are aborted.
   */
  int32_t *code;

  SSqlObj *  pParentSqlObj;
  tFilePage *localBuffer;  // temp buffer, there is a buffer for each vnode to
  // save data
  uint64_t *numOfTotalRetrievedPoints;  // total number of points in this query
  // retrieved from server

  uint32_t        numOfRetry;  // record the number of retry times
  pthread_mutex_t queryMutex;
} SRetrieveSupport;

int32_t tscLocalReducerEnvCreate(SSqlObj *pSql, tExtMemBuffer ***pMemBuffer, tOrderDescriptor **pDesc,
                                 tColModel **pFinalModel, uint32_t nBufferSize);

void tscLocalReducerEnvDestroy(tExtMemBuffer **pMemBuffer, tOrderDescriptor *pDesc, tColModel *pFinalModel,
                               int32_t numOfVnodes);

int32_t saveToBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, void *data,
                     int32_t numOfRows, int32_t orderType);

int32_t tscFlushTmpBuffer(tExtMemBuffer *pMemoryBuf, tOrderDescriptor *pDesc, tFilePage *pPage, int32_t orderType);

/*
 * create local reducer to launch the second-stage reduce process at client site
 */
void tscCreateLocalReducer(tExtMemBuffer **pMemBuffer, int32_t numOfBuffer, tOrderDescriptor *pDesc,
                           tColModel *finalModel, SSqlCmd *pSqlCmd, SSqlRes *pRes);

void tscDestroyLocalReducer(SSqlObj *pSql);

int32_t tscLocalDoReduce(SSqlObj *pSql);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCSECONARYMERGE_H
