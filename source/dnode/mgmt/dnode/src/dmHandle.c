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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "dmHandle.h"
#include "dndWorker.h"
#include "dmMgmt.h"

static void dndSetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgType, NodeMsgFp nodeMsgFp) {
  SDnodeMgmt *pMgmt = pWrapper->pMgmt;
  SMsgHandle *pHandle = &pMgmt->msgHandles[TMSG_INDEX(msgType)];

  pHandle->pWrapper = pWrapper;
  pHandle->nodeMsgFp = nodeMsgFp;
  pHandle->rpcMsgFp = dndProcessRpcMsg;
}

void dndInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_VNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_NETWORK_TEST, dndProcessMgmtMsg);

  // Requests handled by MNODE
  dndSetMsgHandle(pWrapper, TDMT_MND_STATUS_RSP, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_GRANT_RSP, dndProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_AUTH_RSP, dndProcessMgmtMsg);
}

SMsgHandle dmGetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgIndex) {
  SDnodeMgmt *pMgmt = pWrapper->pMgmt;
  return pMgmt->msgHandles[msgIndex];
}
