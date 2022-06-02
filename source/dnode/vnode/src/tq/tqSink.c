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

#include "tq.h"

void tqTableSink(SStreamTask* pTask, void* vnode, int64_t ver, void* data) {
  const SArray* pRes = (const SArray*)data;
  SVnode*       pVnode = (SVnode*)vnode;

  ASSERT(pTask->tbSink.pTSchema);
  SSubmitReq* pReq = tdBlockToSubmit(pRes, pTask->tbSink.pTSchema, true, pTask->tbSink.stbUid,
                                     pTask->tbSink.stbFullName, pVnode->config.vgId);
  /*tPrintFixedSchemaSubmitReq(pReq, pTask->tbSink.pTSchema);*/
  // build write msg
  SRpcMsg msg = {
      .msgType = TDMT_VND_SUBMIT,
      .pCont = pReq,
      .contLen = ntohl(pReq->length),
  };

  ASSERT(tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) == 0);
}
