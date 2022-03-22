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
#include "mmInt.h"

int32_t mmProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDCreateMnodeReq createReq = {0};
  if (tDeserializeSDCreateMnodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.replica <= 1 || createReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to create mnode since %s", terrstr());
    return -1;
  } else {
    return mmOpenFromMsg(pWrapper, &createReq);
  }
}

int32_t mmProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnode  *pDnode = pWrapper->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDDropMnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropMnodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  } else {
    return mmDrop(pWrapper);
  }
}

int32_t mmProcessAlterReq(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SDnode  *pDnode = pMgmt->pDnode;
  SRpcMsg *pReq = &pMsg->rpcMsg;

  SDAlterMnodeReq alterReq = {0};
  if (tDeserializeSDCreateMnodeReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (alterReq.dnodeId != pDnode->dnodeId) {
    terrno = TSDB_CODE_NODE_INVALID_OPTION;
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  } else {
    return mmAlter(pMgmt, &alterReq);
  }
}

void mmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_VNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);

  // Requests handled by MNODE
  dndSetMsgHandle(pWrapper, TDMT_MND_CONNECT, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_ACCT, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_ACCT, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_ACCT, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_USER, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_USER, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_USER, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_GET_USER_AUTH, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_DNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CONFIG_DNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_DNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_MNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_MNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_QNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_QNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_SNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_SNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_BNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_BNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_DB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_DB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_USE_DB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_DB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_SYNC_DB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_COMPACT_DB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_FUNC, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_RETRIEVE_FUNC, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_FUNC, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_STB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_STB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_STB, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_TABLE_META, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_VGROUP_LIST, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_QUERY, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_CONN, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_HEARTBEAT, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_SHOW, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_SHOW_RETRIEVE, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_STATUS, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_TRANS, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_GRANT, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_AUTH, (NodeMsgFp)mmProcessReadMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_TOPIC, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_TOPIC, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_TOPIC, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_SUBSCRIBE, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_MQ_COMMIT_OFFSET, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_GET_SUB_EP, (NodeMsgFp)mmProcessReadMsg, 0);

  // Requests handled by VNODE
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CONN_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_REB_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_STB_RSP, (NodeMsgFp)mmProcessWriteMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_VND_TASK_DEPLOY, (NodeMsgFp)mmProcessWriteMsg, 0);
}
