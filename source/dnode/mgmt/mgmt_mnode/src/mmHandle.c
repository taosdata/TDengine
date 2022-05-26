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

void mmGetMonitorInfo(SMnodeMgmt *pMgmt, SMonMmInfo *pInfo) {
  mndGetMonitorInfo(pMgmt->pMnode, &pInfo->cluster, &pInfo->vgroup, &pInfo->grant);
}

void mmGetMnodeLoads(SMnodeMgmt *pMgmt, SMonMloadInfo *pInfo) {
  pInfo->isMnode = 1;
  mndGetLoad(pMgmt->pMnode, &pInfo->load);
}

int32_t mmProcessGetMonitorInfoReq(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMonMmInfo mmInfo = {0};
  mmGetMonitorInfo(pMgmt, &mmInfo);
  dmGetMonitorSystemInfo(&mmInfo.sys);
  monGetLogs(&mmInfo.log);

  int32_t rspLen = tSerializeSMonMmInfo(NULL, 0, &mmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonMmInfo(pRsp, rspLen, &mmInfo);
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  tFreeSMonMmInfo(&mmInfo);
  return 0;
}

int32_t mmProcessGetLoadsReq(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMonMloadInfo mloads = {0};
  mmGetMnodeLoads(pMgmt, &mloads);

  int32_t rspLen = tSerializeSMonMloadInfo(NULL, 0, &mloads);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonMloadInfo(pRsp, rspLen, &mloads);
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  return 0;
}

int32_t mmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDCreateMnodeReq createReq = {0};
  if (tDeserializeSDCreateMnodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.replica != 1) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create mnode since %s", terrstr());
    return -1;
  }

  bool deployed = true;

  SMnodeMgmt mgmt = {0};
  mgmt.path = pInput->path;
  mgmt.name = pInput->name;
  if (mmWriteFile(&mgmt, &createReq, deployed) != 0) {
    dError("failed to write mnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

int32_t mmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SDDropMnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  bool deployed = false;

  SMnodeMgmt mgmt = {0};
  mgmt.path = pInput->path;
  mgmt.name = pInput->name;
  if (mmWriteFile(&mgmt, NULL, deployed) != 0) {
    dError("failed to write mnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

SArray *mmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(64, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MON_MM_INFO, mmPutNodeMsgToMonitorQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MON_MM_LOAD, mmPutNodeMsgToMonitorQueue, 0) == NULL) goto _OVER;

  // Requests handled by DNODE
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_MNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_MNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_MNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_QNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_QNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_SNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_SNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_BNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_BNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_VNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_VNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CONFIG_DNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;

  // Requests handled by MNODE
  if (dmSetMgmtHandle(pArray, TDMT_MND_CONNECT, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_ACCT, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_ACCT, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_ACCT, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_USER, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_USER, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_USER, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_USER_AUTH, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_DNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CONFIG_DNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_DNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_MNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_MNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_QNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_QNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_QNODE_LIST, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_SNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_SNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_BNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_BNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_DB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_DB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_USE_DB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_DB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_COMPACT_DB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_FUNC, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_RETRIEVE_FUNC, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_FUNC, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_STB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_STB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_STB, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_SMA, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_SMA, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TABLE_META, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_VGROUP_LIST, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_QUERY, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_CONN, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_HEARTBEAT, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SYSTABLE_RETRIEVE, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STATUS, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_TRANS, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GRANT, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_AUTH, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_MNODE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_TOPIC, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_TOPIC, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_TOPIC, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SUBSCRIBE, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MQ_COMMIT_OFFSET, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MQ_ASK_EP, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_VG_CHANGE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_VG_DELETE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_STREAM, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TASK_DEPLOY_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_DB_CFG, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_INDEX, mmPutNodeMsgToReadQueue, 0) == NULL) goto _OVER;

  // Requests handled by VNODE
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_STB_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_STB_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_STB_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_SMA_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_SMA_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY, mmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_CONTINUE, mmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_FETCH, mmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TASK, mmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_HEARTBEAT, mmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_VNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_VNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_COMPACT_VNODE_RSP, mmPutNodeMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_TIMEOUT, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_PING, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_PING_REPLY, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_CLIENT_REQUEST, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_CLIENT_REQUEST_REPLY, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_REQUEST_VOTE, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_REQUEST_VOTE_REPLY, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_APPEND_ENTRIES, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_SYNC_APPEND_ENTRIES_REPLY, mmPutNodeMsgToSyncQueue, 1) == NULL) goto _OVER;

  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
