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

#ifndef _TD_DND_DNODE_H_
#define _TD_DND_DNODE_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dndInt.h"

int32_t dndInitDnode(SDnode *pDnode);
void    dndCleanupDnode(SDnode *pDnode);

int32_t dndGetDnodeId(SDnode *pDnode);
int64_t dndGetClusterId(SDnode *pDnode);
void    dndGetDnodeEp(SDnode *pDnode, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort);
void    dndGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet);

void dndSendRedirectMsg(SDnode *pDnode, SRpcMsg *pMsg);
void dndSendStatusMsg(SDnode *pDnode);
void dndProcessMgmtMsg(SDnode *pDnode, SRpcMsg *pRpcMsg, SEpSet *pEpSet);
void dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DNODE_H_*/