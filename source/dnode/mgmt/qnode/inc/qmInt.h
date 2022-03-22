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

#ifndef _TD_DND_QNODE_INT_H_
#define _TD_DND_QNODE_INT_H_

#include "qm.h"
#include "qnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SQnodeMgmt {
  SQnode       *pQnode;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  const char   *path;
  SSingleWorker queryWorker;
  SSingleWorker fetchWorker;
} SQnodeMgmt;

// qmInt.c
int32_t qmOpen(SMgmtWrapper *pWrapper);
int32_t qmDrop(SMgmtWrapper *pWrapper);

// qmMsg.c
void    qmInitMsgHandles(SMgmtWrapper *pWrapper);
int32_t qmProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t qmProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

// qmWorker.c
int32_t qmPutMsgToQueryQueue(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);
int32_t qmPutMsgToFetchQueue(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);
int32_t qmGetQueueSize(SMgmtWrapper *pWrapper, int32_t vgId, EQueueType qtype);

int32_t qmStartWorker(SQnodeMgmt *pMgmt);
void    qmStopWorker(SQnodeMgmt *pMgmt);
int32_t qmProcessQueryMsg(SQnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t qmProcessFetchMsg(SQnodeMgmt *pMgmt, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_QNODE_INT_H_*/