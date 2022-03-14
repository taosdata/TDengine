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
#include "mmHandle.h"
#include "mmWorker.h"

#if 0
#include "dmMgmt.h"

int32_t mmProcessCreateMnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
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
  if (mmBuildOptionFromReq(pDnode, &option, &createReq) != 0) {
    terrno = TSDB_CODE_DND_MNODE_INVALID_OPTION;
    dError("failed to create mnode since %s", terrstr());
    return -1;
  }

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode != NULL) {
    mmRelease(pDnode, pMnode);
    terrno = TSDB_CODE_DND_MNODE_ALREADY_DEPLOYED;
    dError("failed to create mnode since %s", terrstr());
    return -1;
  }

  dDebug("start to create mnode");
  return mmOpen(pDnode, &option);
}

int32_t mmProcessAlterMnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
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
  if (mmBuildOptionFromReq(pDnode, &option, &alterReq) != 0) {
    terrno = TSDB_CODE_DND_MNODE_INVALID_OPTION;
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) {
    terrno = TSDB_CODE_DND_MNODE_NOT_DEPLOYED;
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  dDebug("start to alter mnode");
  int32_t code = mmAlter(pDnode, &option);
  mmRelease(pDnode, pMnode);

  return code;
}

int32_t mmProcessDropMnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
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

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) {
    terrno = TSDB_CODE_DND_MNODE_NOT_DEPLOYED;
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  dDebug("start to drop mnode");
  int32_t code = mmDrop(pDnode);
  mmRelease(pDnode, pMnode);

  return code;
}

int32_t mmGetMonitorInfo(SDnode *pDnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                         SMonGrantInfo *pGrantInfo) {
  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) return -1;

  int32_t code = mndGetMonitorInfo(pMnode, pClusterInfo, pVgroupInfo, pGrantInfo);
  mmRelease(pDnode, pMnode);
  return code;
}

int32_t mmGetUserAuth(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) {
    terrno = TSDB_CODE_APP_NOT_READY;
    dTrace("failed to get user auth since %s", terrstr());
    return -1;
  }

  int32_t code = mndRetriveAuth(pMnode, user, spi, encrypt, secret, ckey);
  mmRelease(pDnode, pMnode);

  dTrace("user:%s, retrieve auth spi:%d encrypt:%d", user, *spi, *encrypt);
  return code;
}

#endif

static void mmSetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgType, NodeMsgFp nodeMsgFp) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  SMsgHandle *pHandle = &pMgmt->msgHandles[TMSG_INDEX(msgType)];

  pHandle->pWrapper = pWrapper;
  pHandle->nodeMsgFp = nodeMsgFp;
  pHandle->rpcMsgFp = dndProcessRpcMsg;
}

void mmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  mmSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_ALTER_VNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE_RSP, mmProcessWriteMsg);

  // Requests handled by MNODE
  mmSetMsgHandle(pWrapper, TDMT_MND_CONNECT, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_ACCT, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_ALTER_ACCT, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_ACCT, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_USER, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_ALTER_USER, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_USER, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_GET_USER_AUTH, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_DNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CONFIG_DNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_DNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_MNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_MNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_QNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_QNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_SNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_SNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_BNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_BNODE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_DB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_DB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_USE_DB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_ALTER_DB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_SYNC_DB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_COMPACT_DB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_FUNC, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_RETRIEVE_FUNC, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_FUNC, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_STB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_ALTER_STB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_STB, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_TABLE_META, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_VGROUP_LIST, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_KILL_QUERY, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_KILL_CONN, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_HEARTBEAT, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_SHOW, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_SHOW_RETRIEVE, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_STATUS, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_KILL_TRANS, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_GRANT, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_AUTH, mmProcessReadMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_CREATE_TOPIC, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_ALTER_TOPIC, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_DROP_TOPIC, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_SUBSCRIBE, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_MQ_COMMIT_OFFSET, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_MND_GET_SUB_EP, mmProcessReadMsg);

  // Requests handled by VNODE
  mmSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CONN_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_VND_MQ_REB_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB_RSP, mmProcessWriteMsg);
  mmSetMsgHandle(pWrapper, TDMT_VND_DROP_STB_RSP, mmProcessWriteMsg);
}

SMsgHandle mmGetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgIndex) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return pMgmt->msgHandles[msgIndex];
}
