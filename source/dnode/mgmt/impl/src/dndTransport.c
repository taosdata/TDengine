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

/* this file is mainly responsible for the communication between DNODEs. Each
 * dnode works as both server and client. Dnode may send status, grant, config
 * messages to mnode, mnode may send create/alter/drop table/vnode messages
 * to dnode. All theses messages are handled from here
 */

#define _DEFAULT_SOURCE
#include "dndTransport.h"
#include "dndMgmt.h"
#include "dndMnode.h"
#include "dndSnode.h"
#include "dndVnodes.h"

#define INTERNAL_USER   "_dnd"
#define INTERNAL_CKEY   "_key"
#define INTERNAL_SECRET "_pwd"

static void dndInitMsgFp(STransMgmt *pMgmt) {
  // Requests handled by DNODE
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_MNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_MNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_ALTER_MNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_ALTER_MNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_MNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_MNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_QNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_QNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_QNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_QNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_SNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_SNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_SNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_SNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_BNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_BNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_BNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_BNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_VNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_VNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_ALTER_VNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_ALTER_VNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_VNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_VNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_SYNC_VNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_SYNC_VNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_COMPACT_VNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_COMPACT_VNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CONFIG_DNODE)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CONFIG_DNODE_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_NETWORK_TEST)] = dndProcessMgmtMsg;

  // Requests handled by MNODE
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CONNECT)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_ACCT)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_ACCT)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_ACCT)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_USER)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_USER)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_USER)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_GET_USER_AUTH)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_DNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CONFIG_DNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_DNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_MNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_MNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_QNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_QNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_SNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_SNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_BNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_BNODE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_DB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_DB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_USE_DB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_DB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SYNC_DB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_COMPACT_DB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_FUNC)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_RETRIEVE_FUNC)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_FUNC)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_STB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_STB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_STB)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_TABLE_META)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_VGROUP_LIST)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_KILL_QUERY)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_KILL_CONN)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_HEARTBEAT)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SHOW)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SHOW_RETRIEVE)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_STATUS)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_STATUS_RSP)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_KILL_TRANS)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_GRANT)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_GRANT_RSP)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_AUTH)] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_AUTH_RSP)] = dndProcessMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_TOPIC)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_TOPIC)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_TOPIC)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SUBSCRIBE)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_MQ_COMMIT_OFFSET)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_SET_CONN_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_REB_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_GET_SUB_EP)] = dndProcessMnodeReadMsg;

  // Requests handled by VNODE
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_SUBMIT)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_QUERY)] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_QUERY_CONTINUE)] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_FETCH)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_FETCH_RSP)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_ALTER_TABLE)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_UPDATE_TAG_VAL)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_TABLE_META)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_TABLES_META)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_CONSUME)] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_QUERY)] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_CONNECT)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_DISCONNECT)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_SET_CUR)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_RES_READY)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_TASKS_STATUS)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_CANCEL_TASK)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_DROP_TASK)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_CREATE_STB)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_CREATE_STB_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_ALTER_STB)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_ALTER_STB_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_DROP_STB)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_DROP_STB_RSP)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_CREATE_TABLE)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_ALTER_TABLE)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_DROP_TABLE)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_SHOW_TABLES)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_SHOW_TABLES_FETCH)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_SET_CONN)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_REB)] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_SET_CUR)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_CONSUME)] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_QUERY_HEARTBEAT)] = dndProcessVnodeFetchMsg;

  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_TASK_DEPLOY)] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_TASK_EXEC)] = dndProcessVnodeFetchMsg;

  // Requests handled by SNODE
  pMgmt->msgFp[TMSG_INDEX(TDMT_SND_TASK_DEPLOY)] = dndProcessSnodeMgmtMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_SND_TASK_EXEC)] = dndProcessSnodeExecMsg;
}

static void dndProcessResponse(void *parent, SRpcMsg *pRsp, SEpSet *pEpSet) {
  SDnode     *pDnode = parent;
  STransMgmt *pMgmt = &pDnode->tmgmt;

  tmsg_t msgType = pRsp->msgType;

  if (dndGetStat(pDnode) == DND_STAT_STOPPED) {
    if (pRsp == NULL || pRsp->pCont == NULL) return;
    dTrace("RPC %p, rsp:%s ignored since dnode exiting, app:%p", pRsp->handle, TMSG_INFO(msgType), pRsp->ahandle);
    rpcFreeCont(pRsp->pCont);
    return;
  }

  DndMsgFp fp = pMgmt->msgFp[TMSG_INDEX(msgType)];
  if (fp != NULL) {
    dTrace("RPC %p, rsp:%s will be processed, code:0x%x app:%p", pRsp->handle, TMSG_INFO(msgType), pRsp->code & 0XFFFF,
           pRsp->ahandle);
    (*fp)(pDnode, pRsp, pEpSet);
  } else {
    dError("RPC %p, rsp:%s not processed, app:%p", pRsp->handle, TMSG_INFO(msgType), pRsp->ahandle);
    rpcFreeCont(pRsp->pCont);
  }
}

static int32_t dndInitClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->tmgmt;

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = "D-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = dndProcessResponse;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.user = INTERNAL_USER;
  rpcInit.ckey = INTERNAL_CKEY;
  rpcInit.spi = 1;
  rpcInit.parent = pDnode;

  char pass[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t *)(INTERNAL_SECRET), strlen(INTERNAL_SECRET), pass);
  rpcInit.secret = pass;

  pMgmt->clientRpc = rpcOpen(&rpcInit);
  if (pMgmt->clientRpc == NULL) {
    dError("failed to init dnode rpc client");
    return -1;
  }

  dDebug("dnode rpc client is initialized");
  return 0;
}

static void dndCleanupClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->tmgmt;
  if (pMgmt->clientRpc) {
    rpcClose(pMgmt->clientRpc);
    pMgmt->clientRpc = NULL;
    dDebug("dnode rpc client is closed");
  }
}

static void dndProcessRequest(void *param, SRpcMsg *pReq, SEpSet *pEpSet) {
  SDnode     *pDnode = param;
  STransMgmt *pMgmt = &pDnode->tmgmt;

  tmsg_t msgType = pReq->msgType;
  if (msgType == TDMT_DND_NETWORK_TEST) {
    dTrace("RPC %p, network test req will be processed, app:%p", pReq->handle, pReq->ahandle);
    dndProcessStartupReq(pDnode, pReq);
    return;
  }

  if (dndGetStat(pDnode) == DND_STAT_STOPPED) {
    dError("RPC %p, req:%s ignored since dnode exiting, app:%p", pReq->handle, TMSG_INFO(msgType), pReq->ahandle);
    SRpcMsg rspMsg = {.handle = pReq->handle, .code = TSDB_CODE_DND_OFFLINE, .ahandle = pReq->ahandle};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pReq->pCont);
    return;
  } else if (dndGetStat(pDnode) != DND_STAT_RUNNING) {
    dError("RPC %p, req:%s ignored since dnode not running, app:%p", pReq->handle, TMSG_INFO(msgType), pReq->ahandle);
    SRpcMsg rspMsg = {.handle = pReq->handle, .code = TSDB_CODE_APP_NOT_READY, .ahandle = pReq->ahandle};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pReq->pCont);
    return;
  }

  if (pReq->pCont == NULL) {
    dTrace("RPC %p, req:%s not processed since its empty, app:%p", pReq->handle, TMSG_INFO(msgType), pReq->ahandle);
    SRpcMsg rspMsg = {.handle = pReq->handle, .code = TSDB_CODE_DND_INVALID_MSG_LEN, .ahandle = pReq->ahandle};
    rpcSendResponse(&rspMsg);
    return;
  }

  DndMsgFp fp = pMgmt->msgFp[TMSG_INDEX(msgType)];
  if (fp != NULL) {
    dTrace("RPC %p, req:%s will be processed, app:%p", pReq->handle, TMSG_INFO(msgType), pReq->ahandle);
    (*fp)(pDnode, pReq, pEpSet);
  } else {
    dError("RPC %p, req:%s not processed since no handle, app:%p", pReq->handle, TMSG_INFO(msgType), pReq->ahandle);
    SRpcMsg rspMsg = {.handle = pReq->handle, .code = TSDB_CODE_MSG_NOT_PROCESSED, .ahandle = pReq->ahandle};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pReq->pCont);
  }
}

static void dndSendMsgToMnodeRecv(SDnode *pDnode, SRpcMsg *pRpcMsg, SRpcMsg *pRpcRsp) {
  STransMgmt *pMgmt = &pDnode->tmgmt;

  SEpSet epSet = {0};
  dndGetMnodeEpSet(pDnode, &epSet);
  rpcSendRecv(pMgmt->clientRpc, &epSet, pRpcMsg, pRpcRsp);
}

static int32_t dndAuthInternalReq(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (strcmp(user, INTERNAL_USER) == 0) {
    char pass[TSDB_PASSWORD_LEN + 1] = {0};
    taosEncryptPass_c((uint8_t *)(INTERNAL_SECRET), strlen(INTERNAL_SECRET), pass);
    memcpy(secret, pass, TSDB_PASSWORD_LEN);
    *spi = 1;
    *encrypt = 0;
    *ckey = 0;
    return 0;
  } else if (strcmp(user, TSDB_NETTEST_USER) == 0) {
    char pass[TSDB_PASSWORD_LEN + 1] = {0};
    taosEncryptPass_c((uint8_t *)(TSDB_NETTEST_USER), strlen(TSDB_NETTEST_USER), pass);
    memcpy(secret, pass, TSDB_PASSWORD_LEN);
    *spi = 1;
    *encrypt = 0;
    *ckey = 0;
    return 0;
  } else {
    return -1;
  }
}

static int32_t dndRetrieveUserAuthInfo(void *parent, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  SDnode *pDnode = parent;

  if (dndAuthInternalReq(parent, user, spi, encrypt, secret, ckey) == 0) {
    dTrace("user:%s, get auth from mnode, spi:%d encrypt:%d", user, *spi, *encrypt);
    return 0;
  }

  if (dndGetUserAuthFromMnode(pDnode, user, spi, encrypt, secret, ckey) == 0) {
    dTrace("user:%s, get auth from mnode, spi:%d encrypt:%d", user, *spi, *encrypt);
    return 0;
  }

  if (terrno != TSDB_CODE_APP_NOT_READY) {
    dTrace("failed to get user auth from mnode since %s", terrstr());
    return -1;
  }

  SAuthReq authReq = {0};
  tstrncpy(authReq.user, user, TSDB_USER_LEN);
  int32_t contLen = tSerializeSAuthReq(NULL, 0, &authReq);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSAuthReq(pReq, contLen, &authReq);

  SRpcMsg rpcMsg = {.pCont = pReq, .contLen = contLen, .msgType = TDMT_MND_AUTH, .ahandle = (void *)9528};
  SRpcMsg rpcRsp = {0};
  dTrace("user:%s, send user auth req to other mnodes, spi:%d encrypt:%d", user, authReq.spi, authReq.encrypt);
  dndSendMsgToMnodeRecv(pDnode, &rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0) {
    terrno = rpcRsp.code;
    dError("user:%s, failed to get user auth from other mnodes since %s", user, terrstr());
  } else {
    SAuthRsp authRsp = {0};
    tDeserializeSAuthReq(rpcRsp.pCont, rpcRsp.contLen, &authRsp);
    memcpy(secret, authRsp.secret, TSDB_PASSWORD_LEN);
    memcpy(ckey, authRsp.ckey, TSDB_PASSWORD_LEN);
    *spi = authRsp.spi;
    *encrypt = authRsp.encrypt;
    dTrace("user:%s, success to get user auth from other mnodes, spi:%d encrypt:%d", user, authRsp.spi,
           authRsp.encrypt);
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

static int32_t dndInitServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->tmgmt;
  dndInitMsgFp(pMgmt);

  int32_t numOfThreads = (int32_t)((tsNumOfCores * tsNumOfThreadsPerCore) / 2.0);
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = pDnode->cfg.serverPort;
  rpcInit.label = "D-S";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp = dndProcessRequest;
  rpcInit.sessions = tsMaxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.afp = dndRetrieveUserAuthInfo;
  rpcInit.parent = pDnode;

  pMgmt->serverRpc = rpcOpen(&rpcInit);
  if (pMgmt->serverRpc == NULL) {
    dError("failed to init dnode rpc server");
    return -1;
  }

  dDebug("dnode rpc server is initialized");
  return 0;
}

static void dndCleanupServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->tmgmt;
  if (pMgmt->serverRpc) {
    rpcClose(pMgmt->serverRpc);
    pMgmt->serverRpc = NULL;
    dDebug("dnode rpc server is closed");
  }
}

int32_t dndInitTrans(SDnode *pDnode) {
  if (dndInitClient(pDnode) != 0) {
    return -1;
  }

  if (dndInitServer(pDnode) != 0) {
    return -1;
  }

  dInfo("dnode-transport is initialized");
  return 0;
}

void dndCleanupTrans(SDnode *pDnode) {
  dInfo("dnode-transport start to clean up");
  dndCleanupServer(pDnode);
  dndCleanupClient(pDnode);
  dInfo("dnode-transport is cleaned up");
}

int32_t dndSendReqToDnode(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pReq) {
  STransMgmt *pMgmt = &pDnode->tmgmt;
  if (pMgmt->clientRpc == NULL) {
    terrno = TSDB_CODE_DND_OFFLINE;
    return -1;
  }

  rpcSendRequest(pMgmt->clientRpc, pEpSet, pReq, NULL);
  return 0;
}

int32_t dndSendReqToMnode(SDnode *pDnode, SRpcMsg *pReq) {
  SEpSet epSet = {0};
  dndGetMnodeEpSet(pDnode, &epSet);
  return dndSendReqToDnode(pDnode, &epSet, pReq);
}
