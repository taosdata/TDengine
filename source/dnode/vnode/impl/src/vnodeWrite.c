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

static int vnodeApplyWriteRequest(SVnode *pVnode, const SRequest *pRequest);

int vnodeProcessWriteReqs(SVnode *pVnode, SReqBatch *pReqBatch) {
  /* TODO */
  return 0;
}

int vnodeApplyWriteReqs(SVnode *pVnode, SReqBatch *pReqBatch) {
  SReqBatchIter rbIter;

  tdInitRBIter(&rbIter, pReqBatch);

  for (;;) {
    const SRequest *pReq = tdRBIterNext(&rbIter);
    if (pReq == NULL) {
      break;
    }

    if (vnodeApplyWriteRequest(pVnode, pReq) < 0) {
      // TODO
    }
  }

  tdClearRBIter(&rbIter);

  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */
static int vnodeApplyWriteRequest(SVnode *pVnode, const SRequest *pRequest) {
  /* TODO */
  return 0;
}