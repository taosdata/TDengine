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

#include "vnodeInt.h"

int vnodeProcessSubmitReq(SVnode *pVnode, SSubmitReq *pReq, SSubmitRsp *pRsp) {
  // TODO: Check inputs

#if 1
  void *pMem = NULL;
  if ((pMem = aMalloc(pVnode->allocator, REQ_SIZE(pReq))) == NULL) {
    // No more memory to allocate, schedule an async commit
    // and continue
    vnodeAsyncCommit(pVnode);

    // Reset allocator and allocat more
    vnodeResetAllocator(pVnode);
    pMem = aMalloc(pVnode->allocator, REQ_SIZE(pReq));
    if (pMem == NULL) {
      // TODO: handle the error
    }
  }

  // TODO: if SSubmitReq is compressed or encoded, we need to decode the request
  memcpy(pMem, pReq, REQ_SIZE(pReq));

  if (tqPushMsg((SSubmitReq *)pReq) < 0) {
    // TODO: handle error
  }

  SSubmitReqReader reader;
  taosInitSubmitReqReader(&reader, (SSubmitReq *)pMem);

  if (tsdbInsertData((SSubmitReq *)pMem) < 0) {
    // TODO: handler error
  }

#endif

  return 0;
}

int vnodeProcessCreateTableReq(SVnode *pVnode, SCreateTableReq *pReq, SCreateTableRsp *pRsp) {
  // TODO
  return 0;
}

int vnodeProcessDropTableReq(SVnode *pVnode, SDropTableReq *pReq, SDropTableRsp *pRsp) {
  // TODO
  return 0;
}

int vnodeProcessAlterTableReq(SVnode *pVnode, SDropTableReq *pReq, SDropTableRsp *pRsp) {
  // TODO
  return 0;
}
