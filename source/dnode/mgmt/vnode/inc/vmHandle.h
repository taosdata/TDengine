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

#ifndef _TD_DND_VNODE_HANDLE_H_
#define _TD_DND_VNODE_HANDLE_H_

#include "vmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

void    vmInitMsgHandles(SMgmtWrapper *pWrapper);
int32_t vmProcessCreateVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessAlterVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessDropVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t dndProcessAuthVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessSyncVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessCompactVnodeReq(SDnode *pDnode, SRpcMsg *pReq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODE_HANDLE_H_*/