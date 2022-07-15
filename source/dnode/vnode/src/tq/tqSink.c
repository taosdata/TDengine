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

SSubmitReq* tdBlockToSubmit(const SArray* pBlocks, const STSchema* pTSchema, bool createTb, int64_t suid,
                            const char* stbFullName, int32_t vgId) {
  SSubmitReq* ret = NULL;
  SArray*     schemaReqs = NULL;
  SArray*     schemaReqSz = NULL;
  SArray*     tagArray = taosArrayInit(1, sizeof(STagVal));
  if (!tagArray) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t sz = taosArrayGetSize(pBlocks);

  if (createTb) {
    schemaReqs = taosArrayInit(sz, sizeof(void*));
    schemaReqSz = taosArrayInit(sz, sizeof(int32_t));
    for (int32_t i = 0; i < sz; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
      STagVal      tagVal = {
               .cid = taosArrayGetSize(pDataBlock->pDataBlock) + 1,
               .type = TSDB_DATA_TYPE_UBIGINT,
               .i64 = (int64_t)pDataBlock->info.groupId,
      };
      STag* pTag = NULL;
      taosArrayClear(tagArray);
      taosArrayPush(tagArray, &tagVal);
      tTagNew(tagArray, 1, false, &pTag);
      if (pTag == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        taosArrayDestroy(tagArray);
        return NULL;
      }

      SVCreateTbReq createTbReq = {0};
      createTbReq.name = buildCtbNameByGroupId(stbFullName, pDataBlock->info.groupId);
      createTbReq.ctb.name = strdup(stbFullName);
      createTbReq.flags = 0;
      createTbReq.type = TSDB_CHILD_TABLE;
      createTbReq.ctb.suid = suid;
      createTbReq.ctb.pTag = (uint8_t*)pTag;

      int32_t code;
      int32_t schemaLen;
      tEncodeSize(tEncodeSVCreateTbReq, &createTbReq, schemaLen, code);
      if (code < 0) {
        tdDestroySVCreateTbReq(&createTbReq);
        taosArrayDestroy(tagArray);
        taosMemoryFreeClear(ret);
        return NULL;
      }

      void* schemaStr = taosMemoryMalloc(schemaLen);
      if (schemaStr == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return NULL;
      }
      taosArrayPush(schemaReqs, &schemaStr);
      taosArrayPush(schemaReqSz, &schemaLen);

      SEncoder encoder = {0};
      tEncoderInit(&encoder, schemaStr, schemaLen);
      code = tEncodeSVCreateTbReq(&encoder, &createTbReq);
      if (code < 0) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return NULL;
      }
      tEncoderClear(&encoder);
      tdDestroySVCreateTbReq(&createTbReq);
    }
  }
  taosArrayDestroy(tagArray);

  // cal size
  int32_t cap = sizeof(SSubmitReq);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
    int32_t      rows = pDataBlock->info.rows;
    // TODO min
    int32_t rowSize = pDataBlock->info.rowSize;
    int32_t maxLen = TD_ROW_MAX_BYTES_FROM_SCHEMA(pTSchema);

    int32_t schemaLen = 0;
    if (createTb) {
      schemaLen = *(int32_t*)taosArrayGet(schemaReqSz, i);
    }
    cap += sizeof(SSubmitBlk) + schemaLen + rows * maxLen;
  }

  // assign data
  // TODO
  ret = rpcMallocCont(cap);
  ret->header.vgId = vgId;
  ret->version = htonl(1);
  ret->length = sizeof(SSubmitReq);
  ret->numOfBlocks = htonl(sz);

  SSubmitBlk* blkHead = POINTER_SHIFT(ret, sizeof(SSubmitReq));
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);

    blkHead->numOfRows = htons(pDataBlock->info.rows);
    blkHead->sversion = htonl(pTSchema->version);
    // TODO
    blkHead->suid = htobe64(suid);
    // uid is assigned by vnode
    blkHead->uid = 0;

    int32_t rows = pDataBlock->info.rows;

    int32_t dataLen = 0;

    void* blkSchema = POINTER_SHIFT(blkHead, sizeof(SSubmitBlk));

    int32_t schemaLen = 0;
    if (createTb) {
      schemaLen = *(int32_t*)taosArrayGet(schemaReqSz, i);
      void* schemaStr = taosArrayGetP(schemaReqs, i);
      memcpy(blkSchema, schemaStr, schemaLen);
    }
    blkHead->schemaLen = htonl(schemaLen);

    STSRow* rowData = POINTER_SHIFT(blkSchema, schemaLen);
    for (int32_t j = 0; j < rows; j++) {
      SRowBuilder rb = {0};
      tdSRowInit(&rb, pTSchema->version);
      tdSRowSetTpInfo(&rb, pTSchema->numOfCols, pTSchema->flen);
      tdSRowResetBuf(&rb, rowData);

      for (int32_t k = 0; k < pTSchema->numOfCols; k++) {
        const STColumn*  pColumn = &pTSchema->columns[k];
        SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, k);
        if (colDataIsNull_s(pColData, j)) {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, pColumn->offset, k);
        } else {
          void* data = colDataGetData(pColData, j);
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, pColumn->offset, k);
        }
      }
      int32_t rowLen = TD_ROW_LEN(rowData);
      rowData = POINTER_SHIFT(rowData, rowLen);
      dataLen += rowLen;
    }
    blkHead->dataLen = htonl(dataLen);

    ret->length += sizeof(SSubmitBlk) + schemaLen + dataLen;
    blkHead = POINTER_SHIFT(blkHead, sizeof(SSubmitBlk) + schemaLen + dataLen);
  }

  ret->length = htonl(ret->length);

  if (schemaReqs) taosArrayDestroyP(schemaReqs, taosMemoryFree);
  taosArrayDestroy(schemaReqSz);

  return ret;
}

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

  if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
    tqDebug("failed to put into write-queue since %s", terrstr());
  }
}
