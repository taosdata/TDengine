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

#ifndef _TD_DND_MNODE_H_
#define _TD_DND_MNODE_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dndEnv.h"

int32_t dndInitMnode(SDnode *pDnode);
void    dndCleanupMnode(SDnode *pDnode);

int32_t dndGetUserAuthFromMnode(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey);
void    dndProcessMnodeReadMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessMnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessMnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
int32_t dndProcessCreateMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t dndProcessAlterMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t dndProcessDropMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

int32_t dndGetMnodeMonitorInfo(SDnode *pDnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                               SMonGrantInfo *pGrantInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_H_*/