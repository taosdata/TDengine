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
#include "vmMsg.h"
#include "vmWorker.h"


int32_t vmProcessCreateVnodeReq(SDnode *pDnode, SRpcMsg *pReq){return 0;}
int32_t vmProcessAlterVnodeReq(SDnode *pDnode, SRpcMsg *pReq){return 0;}
int32_t vmProcessDropVnodeReq(SDnode *pDnode, SRpcMsg *pReq){return 0;}
int32_t dndProcessAuthVnodeReq(SDnode *pDnode, SRpcMsg *pReq){return 0;}
int32_t vmProcessSyncVnodeReq(SDnode *pDnode, SRpcMsg *pReq){return 0;}
int32_t vmProcessCompactVnodeReq(SDnode *pDnode, SRpcMsg *pReq){return 0;}

void vmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by VNODE
  dndSetMsgHandle(pWrapper, TDMT_VND_SUBMIT, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_CONTINUE, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_FETCH_RSP, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_UPDATE_TAG_VAL, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_TABLE_META, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_TABLES_META, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_CONSUME, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_QUERY, vmProcessQueryMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_CONNECT, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_DISCONNECT, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CUR, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_RES_READY, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_TASKS_STATUS, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CANCEL_TASK, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_TASK, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_STB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_STB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_STB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CREATE_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_ALTER_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_DROP_TABLE, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_SHOW_TABLES_FETCH, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CONN, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_REB, vmProcessWriteMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_MQ_SET_CUR, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_CONSUME, vmProcessFetchMsg);
  dndSetMsgHandle(pWrapper, TDMT_VND_QUERY_HEARTBEAT, vmProcessFetchMsg);
}
