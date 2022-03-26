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

typedef struct SProcQueue SProcQueue;
typedef struct SProcObj   SProcObj;
typedef void *(*ProcMallocFp)(int32_t contLen);
typedef void *(*ProcFreeFp)(void *pCont);
typedef void *(*ProcConsumeFp)(void *pParent, void *pHead, int32_t headLen, void *pBody, int32_t bodyLen);

typedef struct {
  int32_t       childQueueSize;
  ProcConsumeFp childConsumeFp;
  ProcMallocFp  childMallocHeadFp;
  ProcFreeFp    childFreeHeadFp;
  ProcMallocFp  childMallocBodyFp;
  ProcFreeFp    childFreeBodyFp;
  int32_t       parentQueueSize;
  ProcConsumeFp parentConsumeFp;
  ProcMallocFp  parentdMallocHeadFp;
  ProcFreeFp    parentFreeHeadFp;
  ProcMallocFp  parentMallocBodyFp;
  ProcFreeFp    parentFreeBodyFp;
  bool          testFlag;
  void         *pParent;
  const char   *name;
} SProcCfg;

SProcObj *taosProcInit(const SProcCfg *pCfg);
void      taosProcCleanup(SProcObj *pProc);
int32_t   taosProcRun(SProcObj *pProc);
void      taosProcStop(SProcObj *pProc);
bool      taosProcIsChild(SProcObj *pProc);

int32_t taosProcPutToChildQueue(SProcObj *pProc, void *pHead, int32_t headLen, void *pBody, int32_t bodyLen);
int32_t taosProcPutToParentQueue(SProcObj *pProc, void *pHead, int32_t headLen, void *pBody, int32_t bodyLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_PROCESS_H_*/
