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

#include "vnodeQuery.h"
#include "vnodeDef.h"

int vnodeQueryOpen(SVnode *pVnode) { return qWorkerInit(NULL, &pVnode->pQuery); }

int vnodeProcessQueryReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  vInfo("query message is processed");
  return qWorkerProcessQueryMsg(pVnode, pVnode->pQuery, pMsg);
}

int vnodeProcessFetchReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  vInfo("fetch message is processed");
  switch (pMsg->msgType) {
    case TDMT_VND_FETCH:
      return qWorkerProcessFetchMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_RES_READY:
      return qWorkerProcessReadyMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_TASKS_STATUS:
      return qWorkerProcessStatusMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_CANCEL_TASK:
      return qWorkerProcessCancelMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_DROP_TASK:
      return qWorkerProcessDropMsg(pVnode, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
      break;
  }
  return 0;
}

static int vnodeGetTableMeta(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
#if 0
  STableInfoMsg *pReq = (STableInfoMsg *)(pMsg->pCont);
  STableMetaMsg *pRspMsg;
  int            ret;

  if (metaGetTableInfo(pVnode->pMeta, pReq->tableFname, &pRspMsg) < 0) {
    return -1;
  }

  *pRsp = malloc(sizeof(SRpcMsg));
  if (TD_IS_NULL(*pRsp)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    free(pMsg);
    return -1;
  }

  // TODO
  (*pRsp)->pCont = pRspMsg;

#endif
  return 0;
}