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

void mmInitMsgHandle(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_VNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);

  // Requests handled by MNODE
  dndSetMsgHandle(pWrapper, TDMT_MND_CONNECT, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_ACCT, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_ACCT, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_ACCT, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_USER, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_USER, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_USER, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_GET_USER_AUTH, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_DNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CONFIG_DNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_DNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_MNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_MNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_QNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_QNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_SNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_SNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_BNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_BNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_DB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_DB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_USE_DB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_DB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_SYNC_DB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_COMPACT_DB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_FUNC, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_RETRIEVE_FUNC, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_FUNC, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_STB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_STB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_STB, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_SMA, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_SMA, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_TABLE_META, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_VGROUP_LIST, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_QUERY, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_CONN, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_HEARTBEAT, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_SHOW, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_SHOW_RETRIEVE, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_SYSTABLE_RETRIEVE, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_STATUS, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_TRANS, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_GRANT, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_AUTH, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_TOPIC, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_TOPIC, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_TOPIC, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_SUBSCRIBE, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_MQ_COMMIT_OFFSET, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_GET_SUB_EP, mmProcessReadMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_STREAM, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_TASK_DEPLOY_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_GET_DB_CFG, mmProcessReadMsg, DEFAULT_HANDLE);

  // Requests handled by VNODE
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CONN_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_REB_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_CANCEL_CONN_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_STB_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_SMA_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_SMA_RSP, mmProcessWriteMsg, DEFAULT_HANDLE);

  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY, mmProcessQueryMsg, MNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_CONTINUE, mmProcessQueryMsg, MNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH, mmProcessQueryMsg, MNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_TASK, mmProcessQueryMsg, MNODE_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_HEARTBEAT, mmProcessQueryMsg, MNODE_HANDLE);

}
