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

void mmGetMnodeLoads(SMnodeMgmt *pMgmt, SMonMloadInfo *pInfo) {
  pInfo->isMnode = 1;
  mndGetLoad(pMgmt->pMnode, &pInfo->load);
}

int32_t mmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  const STraceId  *trace = &pMsg->info.traceId;
  SDCreateMnodeReq createReq = {0};
  if (tDeserializeSDCreateMnodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMnodeOpt option = {.deploy = true,
                      .numOfReplicas = createReq.replica,
                      .numOfTotalReplicas = createReq.replica + createReq.learnerReplica,
                      .selfIndex = -1,
                      .lastIndex = createReq.lastIndex};

  memcpy(option.replicas, createReq.replicas, sizeof(createReq.replicas));
  for (int32_t i = 0; i < createReq.replica; ++i) {
    if (createReq.replicas[i].id == pInput->pData->dnodeId) {
      option.selfIndex = i;
    }
    option.nodeRoles[i] = TAOS_SYNC_ROLE_VOTER;
  }

  memcpy(&(option.replicas[createReq.replica]), createReq.learnerReplicas, sizeof(createReq.learnerReplicas));
  for (int32_t i = 0; i < createReq.learnerReplica; ++i) {
    if (createReq.learnerReplicas[i].id == pInput->pData->dnodeId) {
      option.selfIndex = createReq.replica + i;
    }
    option.nodeRoles[createReq.replica + i] = TAOS_SYNC_ROLE_LEARNER;
  }

  if (option.selfIndex == -1) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dGError("failed to create mnode since %s, selfIndex is -1", terrstr());
    return -1;
  }

  if (mmWriteFile(pInput->path, &option) != 0) {
    dGError("failed to write mnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

int32_t mmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;
  SDDropMnodeReq  dropReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dGError("failed to drop mnode since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  SMnodeOpt option = {.deploy = false};
  if (mmWriteFile(pInput->path, &option) != 0) {
    dGError("failed to write mnode file since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  tFreeSMCreateQnodeReq(&dropReq);
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
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_VNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_VNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CONFIG_DNODE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_MNODE_TYPE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_VNODE_TYPE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CHECK_VNODE_LEARNER_CATCHUP_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CONFIG_CHANGE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MND_CONNECT, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_ACCT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_ACCT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_ACCT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_USER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_USER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_USER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_USER_AUTH, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_DNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CONFIG_DNODE, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
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
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_USE_DB, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_COMPACT_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TRIM_DB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_DB_CFG, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_VGROUP_LIST, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_REDISTRIBUTE_VGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_MERGE_VGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SPLIT_VGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_BALANCE_VGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_BALANCE_VGROUP_LEADER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_FUNC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_RETRIEVE_FUNC, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
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
  if (dmSetMgmtHandle(pArray, TDMT_MND_PAUSE_STREAM, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_RESUME_STREAM, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_MND_RETRIEVE_IP_WHITE, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_USER_WHITELIST, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_INDEX, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_TABLE_INDEX, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TMQ_CREATE_TOPIC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TMQ_DROP_TOPIC, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TMQ_SUBSCRIBE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TMQ_ASK_EP, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TMQ_HB, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TMQ_DROP_CGROUP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_TMQ_DROP_CGROUP_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_TRANS, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_QUERY, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_CONN, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_HEARTBEAT, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STATUS, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_NOTIFY, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SYSTABLE_RETRIEVE, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_AUTH, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SHOW_VARIABLES, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_SERVER_VERSION, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_INDEX, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_INDEX, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_RESTORE_DNODE, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CREATE_VIEW, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_DROP_VIEW, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_VIEW_META, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STATIS, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_KILL_COMPACT, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_CONFIG_CLUSTER, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_QUERY_COMPACT_PROGRESS_RSP, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY, mmPutMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_QUERY, mmPutMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_CONTINUE, mmPutMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_HEARTBEAT, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_FETCH, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_TASK_NOTIFY, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_STB_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_STB_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_STB_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_TTL_TABLE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TRIM_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_SMA_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_SMA_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_SUBSCRIBE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_DELETE_SUB_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_ADD_CHECKINFO_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_TMQ_DEL_CHECKINFO_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_DROP_TASK, mmPutMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DEPLOY_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_DROP_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_PAUSE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_RESUME_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TASK_STOP_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_CHECK_POINT_SOURCE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_UPDATE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_STREAM_TASK_RESET_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_HEARTBEAT, mmPutMsgToReadQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_REQ_CHKPT, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_KILL_COMPACT_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIG_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_REPLICA_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_CONFIRM_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ALTER_HASHRANGE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_COMPACT_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_CREATE_INDEX_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DROP_INDEX_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_DISABLE_WRITE_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SYNC_TIMEOUT_ELECTION, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST_BATCH, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_CLIENT_REQUEST_REPLY, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_REQUEST_VOTE, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_REQUEST_VOTE_REPLY, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES_BATCH, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_APPEND_ENTRIES_REPLY, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SNAPSHOT_SEND, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_PREP_SNAPSHOT, mmPutMsgToSyncQueue, 1) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SYNC_FORCE_FOLLOWER_RSP, mmPutMsgToWriteQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SYNC_TIMEOUT, mmPutMsgToSyncRdQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_HEARTBEAT, mmPutMsgToSyncRdQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_HEARTBEAT_REPLY, mmPutMsgToSyncRdQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_SNAPSHOT_RSP, mmPutMsgToSyncRdQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SYNC_PREP_SNAPSHOT_REPLY, mmPutMsgToSyncRdQueue, 1) == NULL) goto _OVER;

  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
