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

#ifndef _TD_DND_SNODE_WORKER_H_
#define _TD_DND_SNODE_WORKER_H_

#include "smInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t smStartWorker(SDnode *pDnode);
void    smStopWorker(SDnode *pDnode);
void    smInitMsgFp(SMnodeMgmt *pMgmt);
void    smProcessRpcMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
int32_t smPutMsgToWriteQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t smPutMsgToReadQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
void    smConsumeChildQueue(SDnode *pDnode, SNodeMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);
void    smConsumeParentQueue(SDnode *pDnode, SRpcMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);

void smProcessWriteMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
void smProcessSyncMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
void smProcessReadMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_WORKER_H_*/