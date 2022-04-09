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

#ifndef _TD_DND_SNODE_INT_H_
#define _TD_DND_SNODE_INT_H_

#include "dndInt.h"
#include "snode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSnodeMgmt {
  SSnode       *pSnode;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  const char   *path;
  SRWLatch      latch;
  int8_t        uniqueWorkerInUse;
  SArray       *uniqueWorkers;  // SArray<SMultiWorker*>
  SSingleWorker sharedWorker;
  SSingleWorker monitorWorker;
} SSnodeMgmt;

// smInt.c
int32_t smOpen(SMgmtWrapper *pWrapper);
int32_t smDrop(SMgmtWrapper *pWrapper);

// smHandle.c
void    smInitMsgHandle(SMgmtWrapper *pWrapper);
int32_t smProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t smProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t smProcessGetMonSmInfoReq(SMgmtWrapper *pWrapper, SNodeMsg *pReq);

// smWorker.c
int32_t smStartWorker(SSnodeMgmt *pMgmt);
void    smStopWorker(SSnodeMgmt *pMgmt);
int32_t smProcessMgmtMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t smProcessUniqueMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t smProcessSharedMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t smProcessExecMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t smProcessMonitorMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_INT_H_*/