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
  mndGetMonitorInfo(pMgmt->pMnode, &pInfo->cluster, &pInfo->vgroup, &pInfo->stb, &pInfo->grant);
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

void mmGetMnodeLoads(SMnodeMgmt *pMgmt, SMonMloadInfo *pInfo) {
  pInfo->isMnode = 1;
  mndGetLoad(pMgmt->pMnode, &pInfo->load);
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
  const STraceId  *trace = &pMsg->info.traceId;
  SDCreateMnodeReq createReq = {0};
  if (tDeserializeSDCreateMnodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (createReq.replica != 1) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dGError("failed to create mnode since %s", terrstr());
    return -1;
  }

  bool deployed = true;

  SMnodeMgmt mgmt = {0};
  mgmt.path = pInput->path;
  mgmt.name = pInput->name;
  if (mmWriteFile(&mgmt, &createReq.replicas[0], deployed) != 0) {
    dGError("failed to write mnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

int32_t mmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;
  SDDropMnodeReq  dropReq = {0};
  if (tDeserializeSCreateDropMQSBNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dGError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  bool deployed = false;

  SMnodeMgmt mgmt = {0};
  mgmt.path = pInput->path;
  mgmt.name = pInput->name;
  if (mmWriteFile(&mgmt, NULL, deployed) != 0) {
    dGError("failed to write mnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

SArray *mmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(128, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_MNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_MNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_QNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_QNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_SNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_SNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_BNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_BNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_VNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_VNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CONFIG_DNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MND_CONNECT, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_ACCT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_ACCT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_ACCT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_USER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_USER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_USER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_USER_AUTH, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_DNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CONFIG_DNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_DNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_MNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_MNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_MNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_MNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_QNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_QNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_QNODE_LIST, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DNODE_LIST, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_SNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_SNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_BNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_BNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_USE_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_COMPACT_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TRIM_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_DB_CFG, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_VGROUP_LIST, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_REDISTRIBUTE_VGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MERGE_VGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_BALANCE_VGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_FUNC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_RETRIEVE_FUNC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_FUNC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_STB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_STB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_STB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TABLE_META, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_BATCH_META, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TABLE_CFG, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_SMA, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_SMA, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_STREAM, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_STREAM, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_INDEX, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_TABLE_INDEX, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_TOPIC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_TOPIC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_TOPIC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SUBSCRIBE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MQ_ASK_EP, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MQ_HB, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MQ_DROP_CGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MQ_DROP_CGROUP_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MQ_COMMIT_OFFSET, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_TRANS, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_QUERY, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_CONN, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_HEARTBEAT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STATUS, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SYSTABLE_RETRIEVE, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  // if (dmSetMgmtHandle(pArray, TDMT_MND_GRANT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_AUTH, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SHOW_VARIABLES, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SERVER_VERSION, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY, mmPutMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_QUERY, mmPutMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_CONTINUE, mmPutMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_HEARTBEAT, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_FETCH, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_STB_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_STB_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_STB_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_SMA_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_SMA_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_VG_CHANGE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_MQ_VG_DELETE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CHECK_ALTER_INFO_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_DROP_TASK, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DEPLOY_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DROP_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIG_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_REPLICA_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIRM_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_HASHRANGE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_COMPACT_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MON_MM_INFO, mmPutMsgToMonitorQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MON_MM_LOAD, mmPutMsgToMonitorQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SYNC_TIMEOUT, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_PING, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_PING_REPLY, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST_BATCH, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST_REPLY, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_REQUEST_VOTE, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_REQUEST_VOTE_REPLY, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES_BATCH, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES_REPLY, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SNAPSHOT_SEND, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SNAPSHOT_RSP, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SET_MNODE_STANDBY, mmPutMsgToSyncQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SET_MNODE_STANDBY_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SET_VNODE_STANDBY_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
