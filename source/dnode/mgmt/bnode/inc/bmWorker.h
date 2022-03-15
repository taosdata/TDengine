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

#ifndef _TD_DND_BNODE_WORKER_H_
#define _TD_DND_BNODE_WORKER_H_

#include "bmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t bmStartWorker(SDnode *pDnode);
void    bmStopWorker(SDnode *pDnode);
void    bmInitMsgFp(SMnodeMgmt *pMgmt);
void    bmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
int32_t bmPutMsgToWriteQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t bmPutMsgToReadQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
void    bmConsumeChildQueue(SDnode *pDnode, SNodeMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);
void    bmConsumeParentQueue(SDnode *pDnode, SRpcMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);

void bmProcessWriteMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
void bmProcessSyncMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
void bmProcessReadMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_BNODE_WORKER_H_*/