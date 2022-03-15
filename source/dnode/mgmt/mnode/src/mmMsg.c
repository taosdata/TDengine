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
#include "mmMsg.h"
#include "dmInt.h"
#include "mmWorker.h"

int32_t mmProcessCreateReq(SDnode *pDnode, SRpcMsg *pReq) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, MNODE);
  SMnodeMgmt   *pMgmt = pWrapper->pMgmt;

  SDCreateMnodeReq createReq = {0};
  if (tDeserializeSDCreateMnodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.replica <= 1 || createReq.dnodeId != dmGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_MNODE_INVALID_OPTION;
    dError("failed to create mnode since %s", terrstr());
    return -1;
  }

  SMnodeOpt option = {0};
  if (mmBuildOptionFromReq(pMgmt, &option, &createReq) != 0) {
    terrno = TSDB_CODE_DND_MNODE_INVALID_OPTION;
    dError("failed to create mnode since %s", terrstr());
    return -1;
  }

  SMnode *pMnode = mmAcquire(pMgmt);
  if (pMnode != NULL) {
    mmRelease(pMgmt, pMnode);
    terrno = TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED;
    dError("failed to create mnode since %s", terrstr());
    return -1;
  }

  dDebug("start to create mnode");
  return mmOpen(pMgmt, &option);
}

int32_t mmProcessAlterReq(SDnode *pDnode, SRpcMsg *pReq) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, MNODE);
  SMnodeMgmt   *pMgmt = pWrapper->pMgmt;

  SDAlterMnodeReq alterReq = {0};
  if (tDeserializeSDCreateMnodeReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (alterReq.dnodeId != dmGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_MNODE_INVALID_OPTION;
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  SMnodeOpt option = {0};
  if (mmBuildOptionFromReq(pMgmt, &option, &alterReq) != 0) {
    terrno = TSDB_CODE_DND_MNODE_INVALID_OPTION;
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  SMnode *pMnode = mmAcquire(pMgmt);
  if (pMnode == NULL) {
    terrno = TSDB_CODE_DND_MNODE_NOT_DEPLOYED;
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  dDebug("start to alter mnode");
  int32_t code = mmAlter(pMgmt, &option);
  mmRelease(pMgmt, pMnode);

  return code;
}

int32_t mmProcessDropReq(SDnode *pDnode, SRpcMsg *pReq) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, MNODE);
  SMnodeMgmt   *pMgmt = pWrapper->pMgmt;

  SDDropMnodeReq dropReq = {0};
  if (tDeserializeSMCreateDropMnodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (dropReq.dnodeId != dmGetDnodeId(pDnode)) {
    terrno = TSDB_CODE_DND_MNODE_INVALID_OPTION;
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  SMnode *pMnode = mmAcquire(pMgmt);
  if (pMnode == NULL) {
    terrno = TSDB_CODE_DND_MNODE_NOT_DEPLOYED;
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  dDebug("start to drop mnode");
  int32_t code = mmDrop(pMgmt);
  mmRelease(pMgmt, pMnode);

  return code;
}

void mmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_VNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE_RSP, mmProcessWriteMsg);

  // Requests handled by MNODE
  dndSetMsgHandle(pWrapper, TDMT_MND_CONNECT, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_ACCT, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_ACCT, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_ACCT, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_USER, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_USER, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_USER, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_GET_USER_AUTH, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_DNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CONFIG_DNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_DNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_MNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_MNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_QNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_QNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_SNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_SNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_BNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_BNODE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_DB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_DB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_USE_DB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_DB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_SYNC_DB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_COMPACT_DB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_FUNC, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_RETRIEVE_FUNC, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_FUNC, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_STB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_STB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_STB, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_TABLE_META, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_VGROUP_LIST, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_QUERY, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_CONN, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_HEARTBEAT, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_SHOW, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_SHOW_RETRIEVE, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_STATUS, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_KILL_TRANS, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_GRANT, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_AUTH, mmProcessReadMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_CREATE_TOPIC, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_ALTER_TOPIC, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_DROP_TOPIC, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_SUBSCRIBE, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_MQ_COMMIT_OFFSET, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_GET_SUB_EP, mmProcessReadMsg);

  // Requests handled by VNODE
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CONN_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_REB_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB_RSP, mmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_STB_RSP, mmProcessWriteMsg);
}
