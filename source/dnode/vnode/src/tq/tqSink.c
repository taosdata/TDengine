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
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return;
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
        ctbName = taosStrdup(pDataBlock->info.parTbName);
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
        createTbReq.ctb.stbName = taosStrdup((char*)tNameGetTableName(&name));  // taosStrdup(stbFullName);
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

      tqDebug("tq sink pipe1, convert block2 %d, rows: %d", i, rows);

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
              tqDebug("tq sink pipe1, row %d ts %" PRId64, j, *(int64_t*)colData);
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

static int32_t encodeCreateChildTableForRPC(SVCreateTbBatchReq* pReqs, int32_t vgId, void** pBuf, int32_t* contLen) {
  int32_t ret = 0;

  tEncodeSize(tEncodeSVCreateTbBatchReq, pReqs, *contLen, ret);
  if (ret < 0) {
    ret = -1;
    goto end;
  }
  *contLen += sizeof(SMsgHead);
  *pBuf = rpcMallocCont(*contLen);
  if (NULL == *pBuf) {
    ret = -1;
    goto end;
  }
  ((SMsgHead*)(*pBuf))->vgId = vgId;
  ((SMsgHead*)(*pBuf))->contLen = htonl(*contLen);
  SEncoder coder = {0};
  tEncoderInit(&coder, POINTER_SHIFT(*pBuf, sizeof(SMsgHead)), (*contLen) - sizeof(SMsgHead));
  if (tEncodeSVCreateTbBatchReq(&coder, pReqs) < 0) {
    rpcFreeCont(*pBuf);
    *pBuf = NULL;
    *contLen = 0;
    tEncoderClear(&coder);
    ret = -1;
    goto end;
  }
  tEncoderClear(&coder);

end:
  return ret;
}

int32_t tqPutReqToQueue(SVnode* pVnode, SVCreateTbBatchReq* pReqs) {
  void*   buf = NULL;
  int32_t tlen = 0;
  encodeCreateChildTableForRPC(pReqs, TD_VID(pVnode), &buf, &tlen);

  SRpcMsg msg = {
      .msgType = TDMT_VND_CREATE_TABLE,
      .pCont = buf,
      .contLen = tlen,
  };

  if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
    tqError("failed to put into write-queue since %s", terrstr());
  }

  return TSDB_CODE_SUCCESS;
}

void tqSinkToTablePipeline2(SStreamTask* pTask, void* vnode, int64_t ver, void* data) {
  const SArray* pBlocks = (const SArray*)data;
  SVnode*       pVnode = (SVnode*)vnode;
  int64_t       suid = pTask->tbSink.stbUid;
  char*         stbFullName = pTask->tbSink.stbFullName;
  STSchema*     pTSchema = pTask->tbSink.pTSchema;
  /*SSchemaWrapper* pSchemaWrapper = pTask->tbSink.pSchemaWrapper;*/

  int32_t blockSz = taosArrayGetSize(pBlocks);

  tqDebug("vgId:%d, task %d write into table, block num: %d", TD_VID(pVnode), pTask->taskId, blockSz);

  void*   pBuf = NULL;
  SArray* tagArray = NULL;
  SArray* pVals = NULL;
  SArray* crTblArray = NULL;

  for (int32_t i = 0; i < blockSz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
    int32_t      rows = pDataBlock->info.rows;
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
    } else if (pDataBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      SVCreateTbBatchReq reqs = {0};
      crTblArray = reqs.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
      if (NULL == reqs.pArray) {
        goto _end;
      }
      for (int32_t rowId = 0; rowId < rows; rowId++) {
        SVCreateTbReq  createTbReq = {0};
        SVCreateTbReq* pCreateTbReq = &createTbReq;

        // set const
        pCreateTbReq->flags = 0;
        pCreateTbReq->type = TSDB_CHILD_TABLE;
        pCreateTbReq->ctb.suid = suid;

        // set super table name
        SName name = {0};
        tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
        pCreateTbReq->ctb.stbName = taosStrdup((char*)tNameGetTableName(&name));  // taosStrdup(stbFullName);

        // set tag content
        int32_t size = taosArrayGetSize(pDataBlock->pDataBlock);
        if (size == 2) {
          tagArray = taosArrayInit(1, sizeof(STagVal));
          if (!tagArray) {
            tdDestroySVCreateTbReq(pCreateTbReq);
            goto _end;
          }
          STagVal tagVal = {
              .cid = pTSchema->numOfCols + 1,
              .type = TSDB_DATA_TYPE_UBIGINT,
              .i64 = (int64_t)pDataBlock->info.id.groupId,
          };
          taosArrayPush(tagArray, &tagVal);

          // set tag name
          SArray* tagName = taosArrayInit(1, TSDB_COL_NAME_LEN);
          char    tagNameStr[TSDB_COL_NAME_LEN] = "group_id";
          taosArrayPush(tagName, tagNameStr);
          pCreateTbReq->ctb.tagName = tagName;
        } else {
          tagArray = taosArrayInit(size - 1, sizeof(STagVal));
          if (!tagArray) {
            tdDestroySVCreateTbReq(pCreateTbReq);
            goto _end;
          }
          for (int32_t tagId = UD_TAG_COLUMN_INDEX, step = 1; tagId < size; tagId++, step++) {
            SColumnInfoData* pTagData = taosArrayGet(pDataBlock->pDataBlock, tagId);
            STagVal          tagVal = {
                         .cid = pTSchema->numOfCols + step,
                         .type = pTagData->info.type,
            };
            void* pData = colDataGetData(pTagData, rowId);
            if (colDataIsNull_s(pTagData, rowId)) {
              continue;
            } else if (IS_VAR_DATA_TYPE(pTagData->info.type)) {
              tagVal.nData = varDataLen(pData);
              tagVal.pData = varDataVal(pData);
            } else {
              memcpy(&tagVal.i64, pData, pTagData->info.bytes);
            }
            taosArrayPush(tagArray, &tagVal);
          }
        }
        pCreateTbReq->ctb.tagNum = TMAX(size - UD_TAG_COLUMN_INDEX, 1);

        STag* pTag = NULL;
        tTagNew(tagArray, 1, false, &pTag);
        tagArray = taosArrayDestroy(tagArray);
        if (pTag == NULL) {
          tdDestroySVCreateTbReq(pCreateTbReq);
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _end;
        }


        pCreateTbReq->ctb.pTag = (uint8_t*)pTag;

        // set table name
        if (!pDataBlock->info.parTbName[0]) {
          SColumnInfoData* pGpIdColInfo = taosArrayGet(pDataBlock->pDataBlock, UD_GROUPID_COLUMN_INDEX);
          void*            pGpIdData = colDataGetData(pGpIdColInfo, rowId);
          pCreateTbReq->name = buildCtbNameByGroupId(stbFullName, *(uint64_t*)pGpIdData);
        } else {
          pCreateTbReq->name = taosStrdup(pDataBlock->info.parTbName);
        }
        taosArrayPush(reqs.pArray, pCreateTbReq);
      }
      reqs.nReqs = taosArrayGetSize(reqs.pArray);
      if (tqPutReqToQueue(pVnode, &reqs) != TSDB_CODE_SUCCESS) {
        goto _end;
      }
      tagArray = taosArrayDestroy(tagArray);
      taosArrayDestroyEx(crTblArray, (FDelete)tdDestroySVCreateTbReq);
      crTblArray = NULL;
    } else {
      SSubmitTbData tbData = {0};
      tqDebug("tq sink pipe2, convert block1 %d, rows: %d", i, rows);

      if (!(tbData.aRowP = taosArrayInit(rows, sizeof(SRow*)))) {
        goto _end;
      }

      tbData.suid = suid;
      tbData.uid = 0;  // uid is assigned by vnode
      tbData.sver = pTSchema->version;

      char* ctbName = NULL;
      tqDebug("vgId:%d, stream write into %s, table auto created", TD_VID(pVnode), pDataBlock->info.parTbName);
      if (pDataBlock->info.parTbName[0]) {
        ctbName = taosStrdup(pDataBlock->info.parTbName);
      } else {
        ctbName = buildCtbNameByGroupId(stbFullName, pDataBlock->info.id.groupId);
      }

      SMetaReader mr = {0};
      metaReaderInit(&mr, pVnode->pMeta, 0);
      if (metaGetTableEntryByName(&mr, ctbName) < 0) {
        metaReaderClear(&mr);
        tqDebug("vgId:%d, stream write into %s, table auto created", TD_VID(pVnode), ctbName);

        SVCreateTbReq* pCreateTbReq = NULL;

        if (!(pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateStbReq)))) {
          taosMemoryFree(ctbName);
          goto _end;
        };

        // set const
        pCreateTbReq->flags = 0;
        pCreateTbReq->type = TSDB_CHILD_TABLE;
        pCreateTbReq->ctb.suid = suid;

        // set super table name
        SName name = {0};
        tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
        pCreateTbReq->ctb.stbName = taosStrdup((char*)tNameGetTableName(&name));  // taosStrdup(stbFullName);

        // set tag content
        tagArray = taosArrayInit(1, sizeof(STagVal));
        if (!tagArray) {
          taosMemoryFree(ctbName);
          tdDestroySVCreateTbReq(pCreateTbReq);
          goto _end;
        }
        STagVal tagVal = {
            .cid = pTSchema->numOfCols + 1,
            .type = TSDB_DATA_TYPE_UBIGINT,
            .i64 = (int64_t)pDataBlock->info.id.groupId,
        };
        taosArrayPush(tagArray, &tagVal);
        pCreateTbReq->ctb.tagNum = taosArrayGetSize(tagArray);

        STag* pTag = NULL;
        tTagNew(tagArray, 1, false, &pTag);
        tagArray = taosArrayDestroy(tagArray);
        if (pTag == NULL) {
          taosMemoryFree(ctbName);
          tdDestroySVCreateTbReq(pCreateTbReq);
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _end;
        }
        pCreateTbReq->ctb.pTag = (uint8_t*)pTag;

        // set tag name
        SArray* tagName = taosArrayInit(1, TSDB_COL_NAME_LEN);
        char    tagNameStr[TSDB_COL_NAME_LEN] = {0};
        strcpy(tagNameStr, "group_id");
        taosArrayPush(tagName, tagNameStr);
        pCreateTbReq->ctb.tagName = tagName;

        // set table name
        pCreateTbReq->name = ctbName;
        ctbName = NULL;

        tbData.pCreateTbReq = pCreateTbReq;
        tbData.flags = SUBMIT_REQ_AUTO_CREATE_TABLE;
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

        tbData.uid = mr.me.uid;
        metaReaderClear(&mr);
        taosMemoryFreeClear(ctbName);
      }

      // rows
      if (!pVals && !(pVals = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal)))) {
        taosArrayDestroy(tbData.aRowP);
        tdDestroySVCreateTbReq(tbData.pCreateTbReq);
        goto _end;
      }

      for (int32_t j = 0; j < rows; j++) {
        taosArrayClear(pVals);
        int32_t dataIndex = 0;
        for (int32_t k = 0; k < pTSchema->numOfCols; k++) {
          const STColumn* pCol = &pTSchema->columns[k];
          if (k == 0) {
            SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, dataIndex);
            void*            colData = colDataGetData(pColData, j);
            tqDebug("tq sink pipe2, row %d, col %d ts %" PRId64, j, k, *(int64_t*)colData);
          }
          if (IS_SET_NULL(pCol)) {
            SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
            taosArrayPush(pVals, &cv);
          } else {
            SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, dataIndex);
            if (colDataIsNull_s(pColData, j)) {
              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
              taosArrayPush(pVals, &cv);
              dataIndex++;
            } else {
              void* colData = colDataGetData(pColData, j);
              if (IS_STR_DATA_TYPE(pCol->type)) {
                SValue sv =
                    (SValue){.nData = varDataLen(colData), .pData = varDataVal(colData)};  // address copy, no value
                SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, sv);
                taosArrayPush(pVals, &cv);
              } else {
                SValue sv;
                memcpy(&sv.val, colData, tDataTypes[pCol->type].bytes);
                SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, sv);
                taosArrayPush(pVals, &cv);
              }
              dataIndex++;
            }
          }
        }
        SRow* pRow = NULL;
        if ((terrno = tRowBuild(pVals, (STSchema*)pTSchema, &pRow)) < 0) {
          tDestroySSubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
          goto _end;
        }
        ASSERT(pRow);
        taosArrayPush(tbData.aRowP, &pRow);
      }

      SSubmitReq2 submitReq = {0};
      if (!(submitReq.aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
        tDestroySSubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }

      taosArrayPush(submitReq.aSubmitTbData, &tbData);

      // encode
      int32_t len;
      int32_t code;
      tEncodeSize(tEncodeSSubmitReq2, &submitReq, len, code);
      SEncoder encoder;
      len += sizeof(SSubmitReq2Msg);
      pBuf = rpcMallocCont(len);
      if (NULL == pBuf) {
        tDestroySSubmitReq2(&submitReq, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }
      ((SSubmitReq2Msg*)pBuf)->header.vgId = TD_VID(pVnode);
      ((SSubmitReq2Msg*)pBuf)->header.contLen = htonl(len);
      ((SSubmitReq2Msg*)pBuf)->version = htobe64(1);
      tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SSubmitReq2Msg)), len - sizeof(SSubmitReq2Msg));
      if (tEncodeSSubmitReq2(&encoder, &submitReq) < 0) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        tqError("failed to encode submit req since %s", terrstr());
        tEncoderClear(&encoder);
        rpcFreeCont(pBuf);
        tDestroySSubmitReq2(&submitReq, TSDB_MSG_FLG_ENCODE);
        continue;
      }
      tEncoderClear(&encoder);
      tDestroySSubmitReq2(&submitReq, TSDB_MSG_FLG_ENCODE);

      SRpcMsg msg = {
          .msgType = TDMT_VND_SUBMIT,
          .pCont = pBuf,
          .contLen = len,
      };

      if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
        tqDebug("failed to put into write-queue since %s", terrstr());
      }
    }
  }
_end:
  taosArrayDestroy(tagArray);
  taosArrayDestroy(pVals);
  taosArrayDestroyEx(crTblArray, (FDelete)tdDestroySVCreateTbReq);
  // TODO: change
}
