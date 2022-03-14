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
#include "vmHandle.h"
#include "vmWorker.h"

static void vmSetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgType, NodeMsgFp nodeMsgFp) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;
  SMsgHandle  *pHandle = &pMgmt->msgHandles[TMSG_INDEX(msgType)];

  pHandle->pWrapper = pWrapper;
  pHandle->nodeMsgFp = nodeMsgFp;
  pHandle->rpcMsgFp = dndProcessRpcMsg;
}

void vmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by VNODE
  vmSetMsgHandle(pWrapper, TDMT_VND_SUBMIT, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_QUERY, vmProcessQueryMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_QUERY_CONTINUE, vmProcessQueryMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_FETCH, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_FETCH_RSP, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_ALTER_TABLE, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_UPDATE_TAG_VAL, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_TABLE_META, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_TABLES_META, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_CONSUME, vmProcessQueryMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_QUERY, vmProcessQueryMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_CONNECT, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_DISCONNECT, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CUR, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_RES_READY, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_TASKS_STATUS, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_CANCEL_TASK, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_DROP_TASK, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_DROP_STB, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_CREATE_TABLE, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_ALTER_TABLE, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_DROP_TABLE, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES_FETCH, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CONN, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_REB, vmProcessWriteMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CUR, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_CONSUME, vmProcessFetchMsg);
  vmSetMsgHandle(pWrapper, TDMT_VND_QUERY_HEARTBEAT, vmProcessFetchMsg);
}

SMsgHandle vmGetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgIndex) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;
  return pMgmt->msgHandles[msgIndex];
}
