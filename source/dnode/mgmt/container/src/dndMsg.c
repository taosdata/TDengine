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

#define _DEFAULT_SOURCE
#include "dndInt.h"

static SMgmtWrapper *dndGetWrapperFromMsg(SDnode *pDnode, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = NULL;
  switch (pMsg->rpcMsg.msgType) {
    case TDMT_DND_CREATE_MNODE:
      return dndGetWrapper(pDnode, MNODE);
    case TDMT_DND_CREATE_QNODE:
      return dndGetWrapper(pDnode, QNODE);
    case TDMT_DND_CREATE_SNODE:
      return dndGetWrapper(pDnode, SNODE);
    case TDMT_DND_CREATE_BNODE:
      return dndGetWrapper(pDnode, BNODE);
    default:
      return NULL;
  }
}

int32_t dndProcessCreateNodeMsg(SDnode *pDnode, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dndGetWrapperFromMsg(pDnode, pMsg);
  if (pWrapper->procType == PROC_SINGLE) {
    switch (pMsg->rpcMsg.msgType) {
      case TDMT_DND_CREATE_MNODE:
        return mmProcessCreateReq(pWrapper->pMgmt, pMsg);
      case TDMT_DND_CREATE_QNODE:
        return qmProcessCreateReq(pWrapper->pMgmt, pMsg);
      case TDMT_DND_CREATE_SNODE:
        return smProcessCreateReq(pWrapper->pMgmt, pMsg);
      case TDMT_DND_CREATE_BNODE:
        return bmProcessCreateReq(pWrapper->pMgmt, pMsg);
      default:
        terrno = TSDB_CODE_MSG_NOT_PROCESSED;
        return -1;
    }
  } else {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    return -1;
  }
}

int32_t dndProcessDropNodeMsg(SDnode *pDnode, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dndGetWrapperFromMsg(pDnode, pMsg);
  switch (pMsg->rpcMsg.msgType) {
    case TDMT_DND_DROP_MNODE:
      return mmProcessDropReq(pWrapper->pMgmt, pMsg);
    case TDMT_DND_DROP_QNODE:
      return qmProcessDropReq(pWrapper->pMgmt, pMsg);
    case TDMT_DND_DROP_SNODE:
      return smProcessDropReq(pWrapper->pMgmt, pMsg);
    case TDMT_DND_DROP_BNODE:
      return bmProcessDropReq(pWrapper->pMgmt, pMsg);
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      return -1;
  }
}