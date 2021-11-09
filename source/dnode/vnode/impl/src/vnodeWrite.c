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

int vnodeProcessWriteReqs(SVnode *pVnode, SReqBatch *pReqBatch) {
  /* TODO */
  return 0;
}

int vnodeApplyWriteRequest(SVnode *pVnode, const SRequest *pRequest) {
  int    reqType; /* TODO */
  size_t reqSize; /* TODO */
  int    code = 0;

  // Copy the request to vnode buffer
  SRequest *pReq = mMalloc(pVnode->inuse, reqSize);
  if (pReq == NULL) {
    // TODO: handle error
  }

  memcpy(pReq, pRequest, reqSize);

  // Push the request to TQ so consumers can consume
  tqPushMsg(pVnode->pTq, pReq, 0);

  // Process the request
  switch (reqType) {
    case TSDB_MSG_TYPE_CREATE_TABLE:
      code = metaCreateTable(pVnode->pMeta, NULL /* TODO */);
      break;
    case TSDB_MSG_TYPE_DROP_TABLE:
      code = metaDropTable(pVnode->pMeta, 0 /* TODO */);
      break;
      /* TODO */
    default:
      break;
  }

  if (vnodeShouldCommit(pVnode)) {
    if (vnodeAsyncCommit(pVnode) < 0) {
      // TODO: handle error
    }
  }

  return code;
}

/* ------------------------ STATIC METHODS ------------------------ */