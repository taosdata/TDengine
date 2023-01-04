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

#include "tcommon.h"
#include "tmsg.h"
#include "tq.h"

int32_t tqBuildDeleteReq(SVnode* pVnode, const char* stbFullName, const SSDataBlock* pDataBlock,
                         SBatchDeleteReq* deleteReq) {
  ASSERT(pDataBlock->info.type == STREAM_DELETE_RESULT);
  int32_t          totRow = pDataBlock->info.rows;
  SColumnInfoData* pStartTsCol = taosArrayGet(pDataBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pDataBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pGidCol = taosArrayGet(pDataBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pTbNameCol = taosArrayGet(pDataBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);

  tqDebug("stream delete msg: row %d", totRow);

  for (int32_t row = 0; row < totRow; row++) {
    int64_t startTs = *(int64_t*)colDataGetData(pStartTsCol, row);
    int64_t endTs = *(int64_t*)colDataGetData(pEndTsCol, row);
    int64_t groupId = *(int64_t*)colDataGetData(pGidCol, row);
    char*   name;
    void*   varTbName = NULL;
    if (!colDataIsNull(pTbNameCol, totRow, row, NULL)) {
      varTbName = colDataGetVarData(pTbNameCol, row);
    }

    if (varTbName != NULL && varTbName != (void*)-1) {
      name = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN);
      memcpy(name, varDataVal(varTbName), varDataLen(varTbName));
    } else {
      name = buildCtbNameByGroupId(stbFullName, groupId);
    }
    tqDebug("stream delete msg: vgId:%d, groupId :%" PRId64 ", name: %s, start ts:%" PRId64 "end ts:%" PRId64,
            pVnode->config.vgId, groupId, name, startTs, endTs);
#if 0
    SMetaReader mr = {0};
    metaReaderInit(&mr, pVnode->pMeta, 0);
    if (metaGetTableEntryByName(&mr, name) < 0) {
      metaReaderClear(&mr);
      tqDebug("stream delete msg, skip vgId:%d since no table: %s", pVnode->config.vgId, name);
      taosMemoryFree(name);
      continue;
    }

    int64_t uid = mr.me.uid;
    metaReaderClear(&mr);
    taosMemoryFree(name);
#endif
    SSingleDeleteReq req = {
        .startTs = startTs,
        .endTs = endTs,
    };
    strncpy(req.tbname, name, TSDB_TABLE_NAME_LEN - 1);
    taosMemoryFree(name);
    /*tqDebug("stream delete msg, active: vgId:%d, ts:%" PRId64 " name:%s", pVnode->config.vgId, ts, name);*/
    taosArrayPush(deleteReq->deleteReqs, &req);
  }
  return 0;
}

SSubmitReq* tqBlockToSubmit(SVnode* pVnode, const SArray* pBlocks, const STSchema* pTSchema,
                            SSchemaWrapper* pTagSchemaWrapper, bool createTb, int64_t suid, const char* stbFullName,
                            SBatchDeleteReq* pDeleteReq) {
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
      if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
        int32_t padding1 = 0;
        void*   padding2 = NULL;
        taosArrayPush(schemaReqSz, &padding1);
        taosArrayPush(schemaReqs, &padding2);
        continue;
      }

      //      STag* pTag = NULL;
      //      taosArrayClear(tagArray);
      //      SArray *tagName = taosArrayInit(1, TSDB_COL_NAME_LEN);
      //      for(int j = 0; j < pTagSchemaWrapper->nCols; j++){
      //        STagVal tagVal = {
      //            .cid = pTagSchemaWrapper->pSchema[j].colId,
      //            .type = pTagSchemaWrapper->pSchema[j].type,
      //            .i64 = (int64_t)pDataBlock->info.id.groupId,
      //        };
      //        taosArrayPush(tagArray, &tagVal);
      //        taosArrayPush(tagName, pTagSchemaWrapper->pSchema[j].name);
      //      }
      //
      //      tTagNew(tagArray, 1, false, &pTag);
      //      if (pTag == NULL) {
      //        terrno = TSDB_CODE_OUT_OF_MEMORY;
      //        taosArrayDestroy(tagArray);
      //        taosArrayDestroy(tagName);
      //        return NULL;
      //      }

      SVCreateTbReq createTbReq = {0};

      // set const
      createTbReq.flags = 0;
      createTbReq.type = TSDB_CHILD_TABLE;
      createTbReq.ctb.suid = suid;

      // set super table name
      SName name = {0};
      tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
      createTbReq.ctb.stbName = strdup((char*)tNameGetTableName(&name));  // strdup(stbFullName);

      // set tag content
      taosArrayClear(tagArray);
      STagVal tagVal = {
          .cid = taosArrayGetSize(pDataBlock->pDataBlock) + 1,
          .type = TSDB_DATA_TYPE_UBIGINT,
          .i64 = (int64_t)pDataBlock->info.id.groupId,
      };
      taosArrayPush(tagArray, &tagVal);
      createTbReq.ctb.tagNum = taosArrayGetSize(tagArray);

      STag* pTag = NULL;
      tTagNew(tagArray, 1, false, &pTag);
      if (pTag == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        taosArrayDestroy(tagArray);
        taosArrayDestroyP(schemaReqs, taosMemoryFree);
        taosArrayDestroy(schemaReqSz);
        return NULL;
      }
      createTbReq.ctb.pTag = (uint8_t*)pTag;

      // set tag name
      SArray* tagName = taosArrayInit(1, TSDB_COL_NAME_LEN);
      char    tagNameStr[TSDB_COL_NAME_LEN] = {0};
      strcpy(tagNameStr, "group_id");
      taosArrayPush(tagName, tagNameStr);
      createTbReq.ctb.tagName = tagName;

      // set table name
      if (pDataBlock->info.parTbName[0]) {
        createTbReq.name = strdup(pDataBlock->info.parTbName);
      } else {
        createTbReq.name = buildCtbNameByGroupId(stbFullName, pDataBlock->info.id.groupId);
      }

      // save schema len
      int32_t code;
      int32_t schemaLen;
      tEncodeSize(tEncodeSVCreateTbReq, &createTbReq, schemaLen, code);
      if (code < 0) {
        tdDestroySVCreateTbReq(&createTbReq);
        taosArrayDestroy(tagArray);
        taosArrayDestroyP(schemaReqs, taosMemoryFree);
        taosArrayDestroy(schemaReqSz);
        return NULL;
      }
      taosArrayPush(schemaReqSz, &schemaLen);

      // save schema str
      void* schemaStr = taosMemoryMalloc(schemaLen);
      if (schemaStr == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        tdDestroySVCreateTbReq(&createTbReq);
        taosArrayDestroy(tagArray);
        taosArrayDestroyP(schemaReqs, taosMemoryFree);
        taosArrayDestroy(schemaReqSz);
        return NULL;
      }
      taosArrayPush(schemaReqs, &schemaStr);

      SEncoder encoder = {0};
      tEncoderInit(&encoder, schemaStr, schemaLen);
      code = tEncodeSVCreateTbReq(&encoder, &createTbReq);
      if (code < 0) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        tdDestroySVCreateTbReq(&createTbReq);
        taosArrayDestroy(tagArray);
        taosArrayDestroyP(schemaReqs, taosMemoryFree);
        taosArrayDestroy(schemaReqSz);
        tEncoderClear(&encoder);
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
    if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
      continue;
    }
    int32_t rows = pDataBlock->info.rows;
    /*int32_t rowSize = pDataBlock->info.rowSize;*/
    int32_t maxLen = TD_ROW_MAX_BYTES_FROM_SCHEMA(pTSchema);

    int32_t schemaLen = 0;
    if (createTb) {
      schemaLen = *(int32_t*)taosArrayGet(schemaReqSz, i);
    }
    cap += sizeof(SSubmitBlk) + schemaLen + rows * maxLen;
  }

  // assign data
  ret = rpcMallocCont(cap);
  ret->header.vgId = pVnode->config.vgId;
  ret->length = sizeof(SSubmitReq);
  ret->numOfBlocks = htonl(sz);

  SSubmitBlk* blkHead = POINTER_SHIFT(ret, sizeof(SSubmitReq));
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
    if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
      pDeleteReq->suid = suid;
      pDeleteReq->deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
      tqBuildDeleteReq(pVnode, stbFullName, pDataBlock, pDeleteReq);
      continue;
    }

    blkHead->numOfRows = htonl(pDataBlock->info.rows);
    blkHead->sversion = htonl(pTSchema->version);
    blkHead->suid = htobe64(suid);
    // uid is assigned by vnode
    blkHead->uid = 0;

    int32_t rows = pDataBlock->info.rows;

    tqDebug("tq sink, convert block1 %d, rows: %d", i, rows);

    int32_t dataLen = 0;
    int32_t schemaLen = 0;
    void*   blkSchema = POINTER_SHIFT(blkHead, sizeof(SSubmitBlk));
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
      tdSRowEnd(&rb);
      int32_t rowLen = TD_ROW_LEN(rowData);
      rowData = POINTER_SHIFT(rowData, rowLen);
      dataLen += rowLen;
    }
    blkHead->dataLen = htonl(dataLen);

    ret->length += sizeof(SSubmitBlk) + schemaLen + dataLen;
    blkHead = POINTER_SHIFT(blkHead, sizeof(SSubmitBlk) + schemaLen + dataLen);
  }

  ret->length = htonl(ret->length);

  taosArrayDestroyP(schemaReqs, taosMemoryFree);
  taosArrayDestroy(schemaReqSz);

  return ret;
}

void tqSinkToTablePipeline(SStreamTask* pTask, void* vnode, int64_t ver, void* data) {
  const SArray*   pBlocks = (const SArray*)data;
  SVnode*         pVnode = (SVnode*)vnode;
  int64_t         suid = pTask->tbSink.stbUid;
  char*           stbFullName = pTask->tbSink.stbFullName;
  STSchema*       pTSchema = pTask->tbSink.pTSchema;
  SSchemaWrapper* pSchemaWrapper = pTask->tbSink.pSchemaWrapper;

  int32_t blockSz = taosArrayGetSize(pBlocks);

  SArray* tagArray = taosArrayInit(1, sizeof(STagVal));
  if (!tagArray) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return;
  }

  tqDebug("vgId:%d, task %d write into table, block num: %d", TD_VID(pVnode), pTask->taskId, blockSz);
  for (int32_t i = 0; i < blockSz; i++) {
    bool         createTb = true;
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
    if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
      SBatchDeleteReq deleteReq = {0};
      deleteReq.deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
      deleteReq.suid = suid;
      tqBuildDeleteReq(pVnode, stbFullName, pDataBlock, &deleteReq);
      if (taosArrayGetSize(deleteReq.deleteReqs) == 0) {
        taosArrayDestroy(deleteReq.deleteReqs);
        continue;
      }

      int32_t len;
      int32_t code;
      tEncodeSize(tEncodeSBatchDeleteReq, &deleteReq, len, code);
      if (code < 0) {
        //
        ASSERT(0);
      }
      SEncoder encoder;
      void*    serializedDeleteReq = rpcMallocCont(len + sizeof(SMsgHead));
      void*    abuf = POINTER_SHIFT(serializedDeleteReq, sizeof(SMsgHead));
      tEncoderInit(&encoder, abuf, len);
      tEncodeSBatchDeleteReq(&encoder, &deleteReq);
      tEncoderClear(&encoder);
      taosArrayDestroy(deleteReq.deleteReqs);

      ((SMsgHead*)serializedDeleteReq)->vgId = pVnode->config.vgId;

      SRpcMsg msg = {
          .msgType = TDMT_VND_BATCH_DEL,
          .pCont = serializedDeleteReq,
          .contLen = len + sizeof(SMsgHead),
      };
      if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
        tqDebug("failed to put delete req into write-queue since %s", terrstr());
      }
    } else {
      char* ctbName = NULL;
      // set child table name
      if (pDataBlock->info.parTbName[0]) {
        ctbName = strdup(pDataBlock->info.parTbName);
      } else {
        ctbName = buildCtbNameByGroupId(stbFullName, pDataBlock->info.id.groupId);
      }

      int32_t schemaLen = 0;
      void*   schemaStr = NULL;

      int64_t     uid = 0;
      SMetaReader mr = {0};
      metaReaderInit(&mr, pVnode->pMeta, 0);
      if (metaGetTableEntryByName(&mr, ctbName) < 0) {
        metaReaderClear(&mr);
        tqDebug("vgId:%d, stream write into %s, table auto created", TD_VID(pVnode), ctbName);

        SVCreateTbReq createTbReq = {0};

        // set const
        createTbReq.flags = 0;
        createTbReq.type = TSDB_CHILD_TABLE;
        createTbReq.ctb.suid = suid;

        // set super table name
        SName name = {0};
        tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
        createTbReq.ctb.stbName = strdup((char*)tNameGetTableName(&name));  // strdup(stbFullName);
        createTbReq.name = ctbName;
        ctbName = NULL;

        // set tag content
        taosArrayClear(tagArray);
        STagVal tagVal = {
            .cid = taosArrayGetSize(pDataBlock->pDataBlock) + 1,
            .type = TSDB_DATA_TYPE_UBIGINT,
            .i64 = (int64_t)pDataBlock->info.id.groupId,
        };
        taosArrayPush(tagArray, &tagVal);
        createTbReq.ctb.tagNum = taosArrayGetSize(tagArray);

        STag* pTag = NULL;
        tTagNew(tagArray, 1, false, &pTag);
        if (pTag == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          taosArrayDestroy(tagArray);
          tdDestroySVCreateTbReq(&createTbReq);
          return;
        }
        createTbReq.ctb.pTag = (uint8_t*)pTag;

        // set tag name
        SArray* tagName = taosArrayInit(1, TSDB_COL_NAME_LEN);
        char    tagNameStr[TSDB_COL_NAME_LEN] = {0};
        strcpy(tagNameStr, "group_id");
        taosArrayPush(tagName, tagNameStr);
        createTbReq.ctb.tagName = tagName;

        int32_t code;
        tEncodeSize(tEncodeSVCreateTbReq, &createTbReq, schemaLen, code);
        if (code < 0) {
          tdDestroySVCreateTbReq(&createTbReq);
          taosArrayDestroy(tagArray);
          return;
        }

        // set schema str
        schemaStr = taosMemoryMalloc(schemaLen);
        if (schemaStr == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          tdDestroySVCreateTbReq(&createTbReq);
          taosArrayDestroy(tagArray);
          return;
        }

        SEncoder encoder = {0};
        tEncoderInit(&encoder, schemaStr, schemaLen);
        code = tEncodeSVCreateTbReq(&encoder, &createTbReq);
        if (code < 0) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          tdDestroySVCreateTbReq(&createTbReq);
          taosArrayDestroy(tagArray);
          tEncoderClear(&encoder);
          taosMemoryFree(schemaStr);
          return;
        }
        tEncoderClear(&encoder);
        tdDestroySVCreateTbReq(&createTbReq);
      } else {
        if (mr.me.type != TSDB_CHILD_TABLE) {
          tqError("vgId:%d, failed to write into %s, since table type incorrect, type %d", TD_VID(pVnode), ctbName,
                  mr.me.type);
          metaReaderClear(&mr);
          taosMemoryFree(ctbName);
          continue;
        }
        if (mr.me.ctbEntry.suid != suid) {
          tqError("vgId:%d, failed to write into %s, since suid mismatch, expect suid: %" PRId64
                  ", actual suid %" PRId64 "",
                  TD_VID(pVnode), ctbName, suid, mr.me.ctbEntry.suid);
          metaReaderClear(&mr);
          taosMemoryFree(ctbName);
          continue;
        }

        createTb = false;
        uid = mr.me.uid;
        metaReaderClear(&mr);

        tqDebug("vgId:%d, stream write, table %s, uid %" PRId64 " already exist, skip create", TD_VID(pVnode), ctbName,
                uid);

        taosMemoryFreeClear(ctbName);
      }

      int32_t cap = sizeof(SSubmitReq);

      int32_t rows = pDataBlock->info.rows;
      int32_t maxLen = TD_ROW_MAX_BYTES_FROM_SCHEMA(pTSchema);

      cap += sizeof(SSubmitBlk) + schemaLen + rows * maxLen;

      SSubmitReq* pSubmit = rpcMallocCont(cap);
      pSubmit->header.vgId = pVnode->config.vgId;
      pSubmit->length = sizeof(SSubmitReq);
      pSubmit->numOfBlocks = htonl(1);

      SSubmitBlk* blkHead = POINTER_SHIFT(pSubmit, sizeof(SSubmitReq));

      blkHead->numOfRows = htonl(pDataBlock->info.rows);
      blkHead->sversion = htonl(pTSchema->version);
      blkHead->suid = htobe64(suid);
      // uid is assigned by vnode
      blkHead->uid = 0;
      blkHead->schemaLen = 0;

      tqDebug("tq sink, convert block2 %d, rows: %d", i, rows);

      int32_t dataLen = 0;
      void*   blkSchema = POINTER_SHIFT(blkHead, sizeof(SSubmitBlk));
      STSRow* rowData = blkSchema;
      if (createTb) {
        memcpy(blkSchema, schemaStr, schemaLen);
        blkHead->schemaLen = htonl(schemaLen);
        rowData = POINTER_SHIFT(blkSchema, schemaLen);
      } else {
        blkHead->uid = htobe64(uid);
      }

      taosMemoryFreeClear(schemaStr);

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
            void* colData = colDataGetData(pColData, j);
            if (k == 0) {
              tqDebug("tq sink, row %d ts %" PRId64, j, *(int64_t*)colData);
            }
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, colData, true, pColumn->offset, k);
          }
        }
        tdSRowEnd(&rb);
        int32_t rowLen = TD_ROW_LEN(rowData);
        rowData = POINTER_SHIFT(rowData, rowLen);
        dataLen += rowLen;
      }
      blkHead->dataLen = htonl(dataLen);

      pSubmit->length += sizeof(SSubmitBlk) + schemaLen + dataLen;
      pSubmit->length = htonl(pSubmit->length);

      SRpcMsg msg = {
          .msgType = TDMT_VND_SUBMIT,
          .pCont = pSubmit,
          .contLen = ntohl(pSubmit->length),
      };

      if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
        tqDebug("failed to put into write-queue since %s", terrstr());
      }
    }
  }
  taosArrayDestroy(tagArray);
}

#if 0
void tqSinkToTableMerge(SStreamTask* pTask, void* vnode, int64_t ver, void* data) {
  const SArray*   pRes = (const SArray*)data;
  SVnode*         pVnode = (SVnode*)vnode;
  SBatchDeleteReq deleteReq = {0};

  tqDebug("vgId:%d, task %d write into table, block num: %d", TD_VID(pVnode), pTask->taskId, (int32_t)pRes->size);

  ASSERT(pTask->tbSink.pTSchema);
  deleteReq.deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
  SSubmitReq* submitReq = tqBlockToSubmit(pVnode, pRes, pTask->tbSink.pTSchema, pTask->tbSink.pSchemaWrapper, true,
                                          pTask->tbSink.stbUid, pTask->tbSink.stbFullName, &deleteReq);

  tqDebug("vgId:%d, task %d convert blocks over, put into write-queue", TD_VID(pVnode), pTask->taskId);

  if (taosArrayGetSize(deleteReq.deleteReqs) != 0) {
    int32_t code;
    int32_t len;
    tEncodeSize(tEncodeSBatchDeleteReq, &deleteReq, len, code);
    if (code < 0) {
      //
      ASSERT(0);
    }
    SEncoder encoder;
    void*    serializedDeleteReq = rpcMallocCont(len + sizeof(SMsgHead));
    void*    abuf = POINTER_SHIFT(serializedDeleteReq, sizeof(SMsgHead));
    tEncoderInit(&encoder, abuf, len);
    tEncodeSBatchDeleteReq(&encoder, &deleteReq);
    tEncoderClear(&encoder);

    ((SMsgHead*)serializedDeleteReq)->vgId = pVnode->config.vgId;

    SRpcMsg msg = {
        .msgType = TDMT_VND_BATCH_DEL,
        .pCont = serializedDeleteReq,
        .contLen = len + sizeof(SMsgHead),
    };
    if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
      rpcFreeCont(serializedDeleteReq);
      tqDebug("failed to put into write-queue since %s", terrstr());
    }
  }
  taosArrayDestroy(deleteReq.deleteReqs);

  /*tPrintFixedSchemaSubmitReq(pReq, pTask->tbSink.pTSchema);*/
  // build write msg
  SRpcMsg msg = {
      .msgType = TDMT_VND_SUBMIT,
      .pCont = submitReq,
      .contLen = ntohl(submitReq->length),
  };

  if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
    tqDebug("failed to put into write-queue since %s", terrstr());
  }
}
#endif
