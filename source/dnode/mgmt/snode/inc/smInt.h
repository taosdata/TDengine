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

#include "sm.h"
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
} SSnodeMgmt;

// smInt.c
int32_t smOpen(SMgmtWrapper *pWrapper);
int32_t smDrop(SMgmtWrapper *pWrapper);

// smMsg.c
void    smInitMsgHandles(SMgmtWrapper *pWrapper);
int32_t smProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t smProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

// smWorker.c
int32_t smStartWorker(SSnodeMgmt *pMgmt);
void    smStopWorker(SSnodeMgmt *pMgmt);
int32_t smProcessMgmtMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t smProcessUniqueMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t smProcessSharedMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t smProcessExecMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_INT_H_*/