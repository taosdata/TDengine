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

#ifndef _TD_DND_DNODE_MGMT_H_
#define _TD_DND_DNODE_MGMT_H_

#include "dmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t dmInit(SMgmtWrapper *pWrapper);
void    dmCleanup(SMgmtWrapper *pWrapper);
bool    dmRequire(SMgmtWrapper *pWrapper);

void    dmGetDnodeEp(SDnode *pDnode, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort);
void    dmGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet);

void dmSendRedirectRsp(SDnode *pDnode, SRpcMsg *pMsg);
void dmProcessMgmtMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg) ;

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DNODE_MGMT_H_*/