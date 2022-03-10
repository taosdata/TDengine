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

#ifndef _TD_DND_MNODE_MGMT_H_
#define _TD_DND_MNODE_MGMT_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dndEnv.h"

// interface
int32_t mmInit(SDnode *pDnode);
void    mmCleanup(SDnode *pDnode);

// internal
void mmInitMsgFp(SMnodeMgmt *pMgmt);

SMnode *mmAcquire(SDnode *pDnode);
void    mmRelease(SDnode *pDnode, SMnode *pMnode);

// mmFile
int32_t mmReadFile(SDnode *pDnode);
int32_t mmWriteFile(SDnode *pDnode);

// mmMsg
void mmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);

// mmQueue
int32_t mmWriteToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SMnodeMsg *pMnodeMsg);
////////////

int32_t dndGetUserAuthFromMnode(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey);
int32_t dndProcessCreateMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t dndProcessAlterMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t dndProcessDropMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

int32_t mmGetMonitorInfo(SDnode *pDnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                         SMonGrantInfo *pGrantInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_MGMT_H_*/