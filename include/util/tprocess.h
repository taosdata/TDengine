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

#ifndef _TD_UTIL_PROCESS_H_
#define _TD_UTIL_PROCESS_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int32_t contLen;
  char    pCont[];
} SBlockItem;

typedef void *(*ProcFp)(void *parent, SBlockItem *pItem);

typedef struct SProcQueue SProcQueue;

typedef struct {
  int32_t childQueueSize;
  int32_t parentQueueSize;
  ProcFp  childFp;
  ProcFp  parentFp;
} SProcCfg;

typedef struct {
  int32_t     pid;
  SProcCfg    cfg;
  SProcQueue *pChildQueue;
  SProcQueue *pParentQueue;
  pthread_t   childThread;
  pthread_t   parentThread;
  void       *pParent;
  bool        stopFlag;
  bool        testFlag;
} SProcObj;

SProcObj *taosProcInit(const SProcCfg *pCfg);
int32_t   taosProcStart(SProcObj *pProc);
void      taosProcStop(SProcObj *pProc);
void      taosProcCleanup(SProcObj *pProc);
int32_t   taosProcPushChild(SProcObj *pProc, void *pCont, int32_t contLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_PROCESS_H_*/
