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
int32_t mmProcessCreateMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t mmProcessAlterMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t mmProcessDropMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

// mmFile
int32_t mmReadFile(SDnode *pDnode);
int32_t mmWriteFile(SDnode *pDnode);

// mmHandle
int32_t dndGetUserAuthFromMnode(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey);
int32_t mmGetMonitorInfo(SDnode *pDnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                         SMonGrantInfo *pGrantInfo);

// mmMgmt
SMnode *mmAcquire(SDnode *pDnode);
void    mmRelease(SDnode *pDnode, SMnode *pMnode);
int32_t mmOpen(SDnode *pDnode, SMnodeOpt *pOption);
int32_t mmAlter(SDnode *pDnode, SMnodeOpt *pOption);
int32_t mmDrop(SDnode *pDnode);
int32_t mmBuildOptionFromReq(SDnode *pDnode, SMnodeOpt *pOption, SDCreateMnodeReq *pCreate);

// mmWorker
int32_t mmStartWorker(SDnode *pDnode);
void    mmStopWorker(SDnode *pDnode);
void    mmInitMsgFp(SMndMgmt *pMgmt);
void    mmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
int32_t mmPutMsgToWriteQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t mmPutMsgToReadQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
void    mmConsumeChildQueue(SDnode *pDnode, SMndMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);
void    mmConsumeParentQueue(SDnode *pDnode, SMndMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_MGMT_H_*/