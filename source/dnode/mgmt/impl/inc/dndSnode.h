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

#ifndef _TD_DND_SNODE_H_
#define _TD_DND_SNODE_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dndEnv.h"

int32_t dndInitSnode(SDnode *pDnode);
void    dndCleanupSnode(SDnode *pDnode);

// void    dndProcessSnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
int32_t dndProcessCreateSnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t dndProcessDropSnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

void dndProcessSnodeMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void dndProcessSnodeUniqueMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void dndProcessSnodeSharedMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void dndProcessSnodeExecMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_H_*/
