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

int32_t dndInitDnode(SDnode *pDnd);
void    dndCleanupDnode(SDnode *pDnd);
void    dndProcessDnodeReq(SDnode *pDnd, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessDnodeRsp(SDnode *pDnd, SRpcMsg *pMsg, SEpSet *pEpSet);

int32_t dndGetDnodeId(SDnode *pDnd);
int64_t dndGetClusterId(SDnode *pDnd);
void    dndGetDnodeEp(SDnode *pDnd, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort);
void    dndGetMnodeEpSet(SDnode *pDnd, SEpSet *pEpSet);
void    dndSendRedirectMsg(SDnode *pDnd, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DNODE_H_*/