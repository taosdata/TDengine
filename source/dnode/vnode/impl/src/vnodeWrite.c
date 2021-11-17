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

#include "vnodeDef.h"

int vnodeProcessWMsgs(SVnode *pVnode, SArray *pMsgs) {
  SRpcMsg *pReq;
  SRpcMsg *pRsp;

  for (size_t i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pReq = taosArrayGet(pMsgs, i);

    vnodeApplyWMsg(pVnode, pReq, pRsp);
  }

  return 0;
}

int vnodeApplyWMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  // TODO
  int code = 0;

  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_CREATE_TABLE:
      if (metaCreateTable(pVnode->pMeta, pMsg->pCont) < 0) {
        /* TODO */
        return -1;
      }
      break;
    case TSDB_MSG_TYPE_DROP_TABLE:
      if (metaDropTable(pVnode->pMeta, pMsg->pCont) < 0) {
        /* TODO */
        return -1;
      }
      break;
    case TSDB_MSG_TYPE_SUBMIT:
      if (tsdbInsertData(pVnode->pTsdb, pMsg->pCont) < 0) {
        /* TODO */
        return -1;
      }
      break;
    default:
      break;
  }

  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */