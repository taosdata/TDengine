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

#ifndef _TD_DND_VNODE_WORKER_H_
#define _TD_DND_VNODE_WORKER_H_

#include "vmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t vmStartWorker(SVnodesMgmt *pMgmt);
void    vmStopWorker(SVnodesMgmt *pMgmt);
int32_t vmAllocQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);
void    vmFreeQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);

int32_t vmPutMsgToQueryQueue(SVnodesMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToApplyQueue(SVnodesMgmt *pMgmt, int32_t vgId, SRpcMsg *pMsg);

int32_t vmProcessWriteMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t vmProcessSyncMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t vmProcessQueryMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t vmProcessFetchMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODE_WORKER_H_*/