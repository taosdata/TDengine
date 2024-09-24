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

#include <common/tmsg.h>
#include "tcommon.h"
#include "tmsg.h"
#include "tq.h"

typedef struct STableSinkInfo {
  uint64_t uid;
  tstr     name;
} STableSinkInfo;

static bool    hasOnlySubmitData(const SArray* pBlocks, int32_t numOfBlocks);
static int32_t tsAscendingSortFn(const void* p1, const void* p2);
static int32_t setDstTableDataUid(SVnode* pVnode, SStreamTask* pTask, SSDataBlock* pDataBlock, char* stbFullName,
                                  SSubmitTbData* pTableData);
static int32_t doBuildAndSendDeleteMsg(SVnode* pVnode, char* stbFullName, SSDataBlock* pDataBlock, SStreamTask* pTask,
                                       int64_t suid);
static int32_t doBuildAndSendSubmitMsg(SVnode* pVnode, SStreamTask* pTask, SSubmitReq2* pReq, int32_t numOfBlocks);
static int32_t buildSubmitMsgImpl(SSubmitReq2* pSubmitReq, int32_t vgId, void** pMsg, int32_t* msgLen);
static int32_t doConvertRows(SSubmitTbData* pTableData, const STSchema* pTSchema, SSDataBlock* pDataBlock,
                             int64_t earlyTs, const char* id);
static int32_t doWaitForDstTableCreated(SVnode* pVnode, SStreamTask* pTask, STableSinkInfo* pTableSinkInfo,
                                        const char* dstTableName, int64_t* uid);
static int32_t doPutIntoCache(SSHashObj* pSinkTableMap, STableSinkInfo* pTableSinkInfo, uint64_t groupId,
                              const char* id);
static int32_t doRemoveFromCache(SSHashObj* pSinkTableMap, uint64_t groupId, const char* id);
static bool    isValidDstChildTable(SMetaReader* pReader, int32_t vgId, const char* ctbName, int64_t suid);
static int32_t initCreateTableMsg(SVCreateTbReq* pCreateTableReq, uint64_t suid, const char* stbFullName,
                                  int32_t numOfTags);
static int32_t createDefaultTagColName(SArray** pColNameList);
static int32_t setCreateTableMsgTableName(SVCreateTbReq* pCreateTableReq, SSDataBlock* pDataBlock, const char* stbFullName,
                                       int64_t gid, bool newSubTableRule);
static int32_t doCreateSinkInfo(const char* pDstTableName, STableSinkInfo** pInfo);

int32_t tqBuildDeleteReq(STQ* pTq, const char* stbFullName, const SSDataBlock* pDataBlock, SBatchDeleteReq* deleteReq,
                         const char* pIdStr, bool newSubTableRule) {
  int32_t          totalRows = pDataBlock->info.rows;
  SColumnInfoData* pStartTsCol = taosArrayGet(pDataBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pDataBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pGidCol = taosArrayGet(pDataBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pTbNameCol = taosArrayGet(pDataBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);

  if (pStartTsCol == NULL || pEndTsCol == NULL || pGidCol == NULL || pTbNameCol == NULL) {
    return terrno;
  }

  tqDebug("s-task:%s build %d rows delete msg for table:%s", pIdStr, totalRows, stbFullName);

  for (int32_t row = 0; row < totalRows; row++) {
    int64_t skey = *(int64_t*)colDataGetData(pStartTsCol, row);
    int64_t ekey = *(int64_t*)colDataGetData(pEndTsCol, row);
    int64_t groupId = *(int64_t*)colDataGetData(pGidCol, row);

    char* name = NULL;
    char* originName = NULL;
    void* varTbName = NULL;
    if (!colDataIsNull(pTbNameCol, totalRows, row, NULL)) {
      varTbName = colDataGetVarData(pTbNameCol, row);
    }

    if (varTbName != NULL && varTbName != (void*)-1) {
      name = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN);
      if (name == NULL) {
        return terrno;
      }

      memcpy(name, varDataVal(varTbName), varDataLen(varTbName));
      if (newSubTableRule && !isAutoTableName(name) && !alreadyAddGroupId(name, groupId) && groupId != 0 && stbFullName) {
        buildCtbNameAddGroupId(stbFullName, name, groupId);
      }
    } else if (stbFullName) {
      int32_t code = buildCtbNameByGroupId(stbFullName, groupId, &name);
      if (code) {
        return code;
      }
    } else {
      originName = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE);
      if (originName == NULL) {
        return terrno;
      }

      if (metaGetTableNameByUid(pTq->pVnode, groupId, originName) == 0) {
        name = varDataVal(originName);
      }
    }

    if (!name || *name == '\0') {
      tqWarn("s-task:%s failed to build delete msg groupId:%" PRId64 ", skey:%" PRId64 " ekey:%" PRId64
             " since invalid tbname:%s",
             pIdStr, groupId, skey, ekey, name ? name : "NULL");
    } else {
      tqDebug("s-task:%s build delete msg groupId:%" PRId64 ", name:%s, skey:%" PRId64 " ekey:%" PRId64, pIdStr,
              groupId, name, skey, ekey);

      SSingleDeleteReq req = {.startTs = skey, .endTs = ekey};
      strncpy(req.tbname, name, TSDB_TABLE_NAME_LEN - 1);
      void* p = taosArrayPush(deleteReq->deleteReqs, &req);
      if (p == NULL) {
        return terrno;
      }
    }

    if (originName) {
      name = originName;
    }

    taosMemoryFreeClear(name);
  }

  return 0;
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

static bool tqGetTableInfo(SSHashObj* pTableInfoMap, uint64_t groupId, STableSinkInfo** pInfo) {
  void* pVal = tSimpleHashGet(pTableInfoMap, &groupId, sizeof(uint64_t));
  if (pVal) {
    *pInfo = *(STableSinkInfo**)pVal;
    return true;
  }

  return false;
}

static int32_t tqPutReqToQueue(SVnode* pVnode, SVCreateTbBatchReq* pReqs) {
  void*   buf = NULL;
  int32_t tlen = 0;

  int32_t code = encodeCreateChildTableForRPC(pReqs, TD_VID(pVnode), &buf, &tlen);
  if (code) {
    tqError("vgId:%d failed to encode create table msg, create table failed, code:%s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  SRpcMsg msg = {.msgType = TDMT_VND_CREATE_TABLE, .pCont = buf, .contLen = tlen};
  code = tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg);
  if (code) {
    tqError("failed to put into write-queue since %s", terrstr());
  }

  return code;
}

int32_t initCreateTableMsg(SVCreateTbReq* pCreateTableReq, uint64_t suid, const char* stbFullName, int32_t numOfTags) {
  pCreateTableReq->flags = 0;
  pCreateTableReq->type = TSDB_CHILD_TABLE;
  pCreateTableReq->ctb.suid = suid;

  // set super table name
  SName name = {0};

  int32_t code = tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (code == 0) {
    pCreateTableReq->ctb.stbName = taosStrdup((char*)tNameGetTableName(&name));
    if (pCreateTableReq->ctb.stbName == NULL) { // ignore this error code
      tqError("failed to duplicate the stb name:%s, failed to init create-table msg and create req table", stbFullName);
      code = terrno;
    }
  }

  pCreateTableReq->ctb.tagNum = numOfTags;
  return code;
}

int32_t createDefaultTagColName(SArray** pColNameList) {
  *pColNameList = NULL;

  SArray* pTagColNameList = taosArrayInit(1, TSDB_COL_NAME_LEN);
  if (pTagColNameList == NULL) {
    return terrno;
  }

  char  tagNameStr[TSDB_COL_NAME_LEN] = "group_id";
  void* p = taosArrayPush(pTagColNameList, tagNameStr);
  if (p == NULL) {
    taosArrayDestroy(pTagColNameList);
    return terrno;
  }

  *pColNameList = pTagColNameList;
  return TSDB_CODE_SUCCESS;
}

int32_t setCreateTableMsgTableName(SVCreateTbReq* pCreateTableReq, SSDataBlock* pDataBlock, const char* stbFullName,
                                int64_t gid, bool newSubTableRule) {
  if (pDataBlock->info.parTbName[0]) {
    if (newSubTableRule && !isAutoTableName(pDataBlock->info.parTbName) &&
        !alreadyAddGroupId(pDataBlock->info.parTbName, gid) && gid != 0 && stbFullName) {
      pCreateTableReq->name = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN);
      if (pCreateTableReq->name == NULL) {
        return terrno;
      }

      strcpy(pCreateTableReq->name, pDataBlock->info.parTbName);
      buildCtbNameAddGroupId(stbFullName, pCreateTableReq->name, gid);
//      tqDebug("gen name from:%s", pDataBlock->info.parTbName);
    } else {
      pCreateTableReq->name = taosStrdup(pDataBlock->info.parTbName);
      if (pCreateTableReq->name == NULL) {
        return terrno;
      }
//      tqDebug("copy name:%s", pDataBlock->info.parTbName);
    }
  } else {
    int32_t code = buildCtbNameByGroupId(stbFullName, gid, &pCreateTableReq->name);
    return code;
//    tqDebug("gen name from stbFullName:%s gid:%"PRId64, stbFullName, gid);
  }

  return 0;
}

static int32_t doBuildAndSendCreateTableMsg(SVnode* pVnode, char* stbFullName, SSDataBlock* pDataBlock,
                                            SStreamTask* pTask, int64_t suid) {
  STSchema*   pTSchema = pTask->outputInfo.tbSink.pTSchema;
  int32_t     rows = pDataBlock->info.rows;
  SArray*     tagArray = taosArrayInit(4, sizeof(STagVal));
  const char* id = pTask->id.idStr;
  int32_t     vgId = pTask->pMeta->vgId;
  int32_t     code = 0;

  tqDebug("s-task:%s build create %d table(s) msg", id, rows);
  SVCreateTbBatchReq reqs = {0};
  SArray*            crTblArray = reqs.pArray = taosArrayInit(1, sizeof(SVCreateTbReq));
  if ((NULL == reqs.pArray) || (tagArray == NULL)) {
    tqError("s-task:%s failed to init create table msg, code:%s", id, tstrerror(terrno));
    code = terrno;
    goto _end;
  }

  for (int32_t rowId = 0; rowId < rows; rowId++) {
    SVCreateTbReq* pCreateTbReq = &((SVCreateTbReq){0});

    int32_t size = taosArrayGetSize(pDataBlock->pDataBlock);
    int32_t numOfTags = TMAX(size - UD_TAG_COLUMN_INDEX, 1);

    code = initCreateTableMsg(pCreateTbReq, suid, stbFullName, numOfTags);
    if (code) {
      tqError("s-task:%s vgId:%d failed to init create table msg", id, vgId);
      continue;
    }
    taosArrayClear(tagArray);

    if (size == 2) {
      STagVal tagVal = {
          .cid = pTSchema->numOfCols + 1, .type = TSDB_DATA_TYPE_UBIGINT, .i64 = pDataBlock->info.id.groupId};

      void* p = taosArrayPush(tagArray, &tagVal);
      if (p == NULL) {
        return terrno;
      }

      code = createDefaultTagColName(&pCreateTbReq->ctb.tagName);
      if (code) {
        return code;
      }
    } else {
      for (int32_t tagId = UD_TAG_COLUMN_INDEX, step = 1; tagId < size; tagId++, step++) {
        SColumnInfoData* pTagData = taosArrayGet(pDataBlock->pDataBlock, tagId);
        if (pTagData == NULL) {
          continue;
        }

        STagVal tagVal = {.cid = pTSchema->numOfCols + step, .type = pTagData->info.type};
        void*   pData = colDataGetData(pTagData, rowId);
        if (colDataIsNull_s(pTagData, rowId)) {
          continue;
        } else if (IS_VAR_DATA_TYPE(pTagData->info.type)) {
          tagVal.nData = varDataLen(pData);
          tagVal.pData = (uint8_t*)varDataVal(pData);
        } else {
          memcpy(&tagVal.i64, pData, pTagData->info.bytes);
        }
        void* p = taosArrayPush(tagArray, &tagVal);
        if (p == NULL) {
          code = terrno;
          goto _end;
        }
      }
    }

    code = tTagNew(tagArray, 1, false, (STag**)&pCreateTbReq->ctb.pTag);
    taosArrayDestroy(tagArray);
    tagArray = NULL;

    if (pCreateTbReq->ctb.pTag == NULL || (code != 0)) {
      tdDestroySVCreateTbReq(pCreateTbReq);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _end;
    }

    uint64_t gid = pDataBlock->info.id.groupId;
    if (taosArrayGetSize(pDataBlock->pDataBlock) > UD_GROUPID_COLUMN_INDEX) {
      SColumnInfoData* pGpIdColInfo = taosArrayGet(pDataBlock->pDataBlock, UD_GROUPID_COLUMN_INDEX);
      if (pGpIdColInfo == NULL) {
        continue;
      }

      // todo remove this
      void* pGpIdData = colDataGetData(pGpIdColInfo, rowId);
      if (gid != *(int64_t*)pGpIdData) {
        tqError("s-task:%s vgId:%d invalid groupId:%" PRId64 " actual:%" PRId64 " in sink task", id, vgId, gid,
                *(int64_t*)pGpIdData);
      }
    }

    code = setCreateTableMsgTableName(pCreateTbReq, pDataBlock, stbFullName, gid,
                               pTask->ver >= SSTREAM_TASK_SUBTABLE_CHANGED_VER && pTask->subtableWithoutMd5 != 1);
    if (code) {
      goto _end;
    }

    void* p = taosArrayPush(reqs.pArray, pCreateTbReq);
    if (p == NULL) {
      code = terrno;
      goto _end;
    }

    STableSinkInfo* pInfo = NULL;
    bool alreadyCached = tqGetTableInfo(pTask->outputInfo.tbSink.pTblInfo, gid, &pInfo);
    if (!alreadyCached) {
      code = doCreateSinkInfo(pCreateTbReq->name, &pInfo);
      if (code) {
        tqError("vgId:%d failed to create sink tableInfo for table:%s, s-task:%s", vgId, pCreateTbReq->name, id);
        continue;
      }

      code = doPutIntoCache(pTask->outputInfo.tbSink.pTblInfo, pInfo, gid, id);
      if (code) {
        tqError("vgId:%d failed to put sink tableInfo:%s into cache, s-task:%s", vgId, pCreateTbReq->name, id);
      }
    }

    tqDebug("s-task:%s build create table:%s msg complete", id, pCreateTbReq->name);
  }

  reqs.nReqs = taosArrayGetSize(reqs.pArray);
  code = tqPutReqToQueue(pVnode, &reqs);
  if (code != TSDB_CODE_SUCCESS) {
    tqError("s-task:%s failed to send create table msg", id);
  }

_end:
  taosArrayDestroy(tagArray);
  taosArrayDestroyEx(crTblArray, (FDelete)tdDestroySVCreateTbReq);
  return code;
}

int32_t doBuildAndSendSubmitMsg(SVnode* pVnode, SStreamTask* pTask, SSubmitReq2* pReq, int32_t numOfBlocks) {
  const char* id = pTask->id.idStr;
  int32_t     vgId = TD_VID(pVnode);
  int32_t     len = 0;
  void*       pBuf = NULL;
  int32_t     numOfFinalBlocks = taosArrayGetSize(pReq->aSubmitTbData);

  int32_t code = buildSubmitMsgImpl(pReq, vgId, &pBuf, &len);
  if (code != TSDB_CODE_SUCCESS) {
    tqError("s-task:%s build submit msg failed, vgId:%d, code:%s", id, vgId, tstrerror(code));
    return code;
  }

  SRpcMsg msg = {.msgType = TDMT_VND_SUBMIT, .pCont = pBuf, .contLen = len};
  code = tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg);
  if (code == TSDB_CODE_SUCCESS) {
    tqDebug("s-task:%s vgId:%d comp %d blocks into %d and send to dstTable(s) completed", id, vgId, numOfBlocks,
            numOfFinalBlocks);
  } else {
    tqError("s-task:%s failed to put into write-queue since %s", id, terrstr());
  }

  SSinkRecorder* pRec = &pTask->execInfo.sink;

  pRec->numOfSubmit += 1;
  if ((pRec->numOfSubmit % 1000) == 0) {
    double el = (taosGetTimestampMs() - pTask->execInfo.readyTs) / 1000.0;
    tqInfo("s-task:%s vgId:%d write %" PRId64 " blocks (%" PRId64 " rows) in %" PRId64
           " submit into dst table, %.2fMiB duration:%.2f Sec.",
           id, vgId, pRec->numOfBlocks, pRec->numOfRows, pRec->numOfSubmit, SIZE_IN_MiB(pRec->dataSize), el);
  }

  return TSDB_CODE_SUCCESS;
}

// merge the new submit table block with the existed blocks
// if ts in the new data block overlap with existed one, replace it
int32_t doMergeExistedRows(SSubmitTbData* pExisted, const SSubmitTbData* pNew, const char* id) {
  int32_t oldLen = taosArrayGetSize(pExisted->aRowP);
  int32_t newLen = taosArrayGetSize(pNew->aRowP);
  int32_t numOfPk = 0;

  int32_t j = 0, k = 0;
  SArray* pFinal = taosArrayInit(oldLen + newLen, POINTER_BYTES);
  if (pFinal == NULL) {
    tqError("s-task:%s failed to prepare merge result datablock, code:%s", id, tstrerror(terrno));
    return terrno;
  }

  while (j < newLen && k < oldLen) {
    SRow* pNewRow = *(SRow**)TARRAY_GET_ELEM(pNew->aRowP, j);
    SRow* pOldRow = *(SRow**)TARRAY_GET_ELEM(pExisted->aRowP, k);

    if (pNewRow->ts < pOldRow->ts) {
      void* p = taosArrayPush(pFinal, &pNewRow);
      if (p == NULL) {
        return terrno;
      }
      j += 1;
    } else if (pNewRow->ts > pOldRow->ts) {
      void* p = taosArrayPush(pFinal, &pOldRow);
      if (p == NULL) {
        return terrno;
      }

      k += 1;
    } else {
        // check for the existance of primary key
        if (pNewRow->numOfPKs == 0) {
          void* p = taosArrayPush(pFinal, &pNewRow);
          if (p == NULL) {
            return terrno;
          }

          k += 1;
          j += 1;
          tRowDestroy(pOldRow);
        } else {
          numOfPk = pNewRow->numOfPKs;

          SRowKey kNew, kOld;
          tRowGetKey(pNewRow, &kNew);
          tRowGetKey(pOldRow, &kOld);

          int32_t ret = tRowKeyCompare(&kNew, &kOld);
          if (ret <= 0) {
            void* p = taosArrayPush(pFinal, &pNewRow);
            if (p == NULL) {
              return terrno;
            }

            j += 1;

            if (ret == 0) {
              k += 1;
              tRowDestroy(pOldRow);
            }
          } else {
            void* p = taosArrayPush(pFinal, &pOldRow);
            if (p == NULL) {
              return terrno;
            }

            k += 1;
          }
        }
    }
  }

  while (j < newLen) {
    SRow* pRow = *(SRow**)TARRAY_GET_ELEM(pNew->aRowP, j++);
    void* p = taosArrayPush(pFinal, &pRow);
    if (p == NULL) {
      return terrno;
    }
  }

  while (k < oldLen) {
    SRow* pRow = *(SRow**)TARRAY_GET_ELEM(pExisted->aRowP, k++);
    void* p = taosArrayPush(pFinal, &pRow);
    if (p == NULL) {
      return terrno;
    }
  }

  taosArrayDestroy(pNew->aRowP);
  taosArrayDestroy(pExisted->aRowP);
  pExisted->aRowP = pFinal;

  tqTrace("s-task:%s rows merged, final rows:%d, pk:%d uid:%" PRId64 ", existed auto-create table:%d, new-block:%d",
          id, (int32_t)taosArrayGetSize(pFinal), numOfPk, pExisted->uid, (pExisted->pCreateTbReq != NULL),
          (pNew->pCreateTbReq != NULL));

  tdDestroySVCreateTbReq(pNew->pCreateTbReq);
  taosMemoryFree(pNew->pCreateTbReq);
  return TSDB_CODE_SUCCESS;
}

bool isValidDstChildTable(SMetaReader* pReader, int32_t vgId, const char* ctbName, int64_t suid) {
  if (pReader->me.type != TSDB_CHILD_TABLE) {
    tqError("vgId:%d, failed to write into %s, since table type:%d incorrect", vgId, ctbName, pReader->me.type);
    terrno = TSDB_CODE_TDB_INVALID_TABLE_TYPE;
    return false;
  }

  if (pReader->me.ctbEntry.suid != suid) {
    tqError("vgId:%d, failed to write into %s, since suid mismatch, expect suid:%" PRId64 ", actual:%" PRId64, vgId,
            ctbName, suid, pReader->me.ctbEntry.suid);
    terrno = TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE;
    return false;
  }

  terrno = 0;
  return true;
}

int32_t buildAutoCreateTableReq(const char* stbFullName, int64_t suid, int32_t numOfCols, SSDataBlock* pDataBlock,
                                SArray* pTagArray, bool newSubTableRule, SVCreateTbReq** pReq) {
  *pReq = NULL;

  SVCreateTbReq* pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (pCreateTbReq == NULL) {
    return terrno;
  }

  taosArrayClear(pTagArray);
  int32_t code = initCreateTableMsg(pCreateTbReq, suid, stbFullName, 1);
  if (code != 0) {
    return code;
  }

  STagVal tagVal = {.cid = numOfCols, .type = TSDB_DATA_TYPE_UBIGINT, .i64 = pDataBlock->info.id.groupId};
  void* p = taosArrayPush(pTagArray, &tagVal);
  if (p == NULL) {
    return terrno;
  }

  code = tTagNew(pTagArray, 1, false, (STag**)&pCreateTbReq->ctb.pTag);
  if (pCreateTbReq->ctb.pTag == NULL || (code != 0)) {
    tdDestroySVCreateTbReq(pCreateTbReq);
    taosMemoryFreeClear(pCreateTbReq);
    return code;
  }

  code = createDefaultTagColName(&pCreateTbReq->ctb.tagName);
  if (code) {
    return code;
  }

  // set table name
  code = setCreateTableMsgTableName(pCreateTbReq, pDataBlock, stbFullName, pDataBlock->info.id.groupId, newSubTableRule);
  if (code) {
    return code;
  }

  *pReq = pCreateTbReq;
  return code;
}

int32_t buildSubmitMsgImpl(SSubmitReq2* pSubmitReq, int32_t vgId, void** pMsg, int32_t* msgLen) {
  int32_t code = 0;
  void*   pBuf = NULL;
  *msgLen = 0;

  // encode
  int32_t len = 0;
  tEncodeSize(tEncodeSubmitReq, pSubmitReq, len, code);

  SEncoder encoder = {0};
  len += sizeof(SSubmitReq2Msg);

  pBuf = rpcMallocCont(len);
  if (NULL == pBuf) {
    tDestroySubmitReq(pSubmitReq, TSDB_MSG_FLG_ENCODE);
    return terrno;
  }

  ((SSubmitReq2Msg*)pBuf)->header.vgId = vgId;
  ((SSubmitReq2Msg*)pBuf)->header.contLen = htonl(len);
  ((SSubmitReq2Msg*)pBuf)->version = htobe64(1);

  tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SSubmitReq2Msg)), len - sizeof(SSubmitReq2Msg));
  if (tEncodeSubmitReq(&encoder, pSubmitReq) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("failed to encode submit req, code:%s, ignore and continue", terrstr());
    tEncoderClear(&encoder);
    rpcFreeCont(pBuf);
    tDestroySubmitReq(pSubmitReq, TSDB_MSG_FLG_ENCODE);
    return code;
  }

  tEncoderClear(&encoder);
  tDestroySubmitReq(pSubmitReq, TSDB_MSG_FLG_ENCODE);

  *msgLen = len;
  *pMsg = pBuf;
  return TSDB_CODE_SUCCESS;
}

int32_t tsAscendingSortFn(const void* p1, const void* p2) {
  SRow* pRow1 = *(SRow**)p1;
  SRow* pRow2 = *(SRow**)p2;

  if (pRow1->ts == pRow2->ts) {
    return 0;
  } else {
    return pRow1->ts > pRow2->ts ? 1 : -1;
  }
}

int32_t doConvertRows(SSubmitTbData* pTableData, const STSchema* pTSchema, SSDataBlock* pDataBlock, int64_t earlyTs,
                      const char* id) {
  int32_t numOfRows = pDataBlock->info.rows;
  int32_t code = TSDB_CODE_SUCCESS;

  SArray* pVals = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  pTableData->aRowP = taosArrayInit(numOfRows, sizeof(SRow*));

  if (pTableData->aRowP == NULL || pVals == NULL) {
    taosArrayDestroy(pTableData->aRowP);
    pTableData->aRowP = NULL;
    taosArrayDestroy(pVals);
    code = terrno;
    tqError("s-task:%s failed to prepare write stream res blocks, code:%s", id, tstrerror(code));
    return code;
  }

  for (int32_t j = 0; j < numOfRows; j++) {
    taosArrayClear(pVals);

    int32_t dataIndex = 0;
    int64_t ts = 0;

    for (int32_t k = 0; k < pTSchema->numOfCols; k++) {
      const STColumn* pCol = &pTSchema->columns[k];

      // primary timestamp column, for debug purpose
      if (k == 0) {
        SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, dataIndex);
        if (pColData == NULL) {
          continue;
        }

        ts = *(int64_t*)colDataGetData(pColData, j);
        tqTrace("s-task:%s sink row %d, col %d ts %" PRId64, id, j, k, ts);

        if (ts < earlyTs) {
          tqError("s-task:%s ts:%" PRId64 " of generated results out of valid time range %" PRId64 " , discarded", id,
                  ts, earlyTs);
          taosArrayDestroy(pTableData->aRowP);
          pTableData->aRowP = NULL;
          taosArrayDestroy(pVals);
          return TSDB_CODE_SUCCESS;
        }
      }

      if (IS_SET_NULL(pCol)) {
        if (pCol->flags & COL_IS_KEY) {
          qError("ts:%" PRId64 " primary key column should not be null, colId:%" PRIi16 ", colType:%" PRIi8, ts,
                 pCol->colId, pCol->type);
          break;
        }
        SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
        void* p = taosArrayPush(pVals, &cv);
        if (p == NULL) {
          return terrno;
        }
      } else {
        SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, dataIndex);
        if (pColData == NULL) {
          continue;
        }

        if (colDataIsNull_s(pColData, j)) {
          if (pCol->flags & COL_IS_KEY) {
            qError("ts:%" PRId64 " primary key column should not be null, colId:%" PRIi16 ", colType:%" PRIi8,
                   ts, pCol->colId, pCol->type);
            break;
          }

          SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
          void* p = taosArrayPush(pVals, &cv);
          if (p == NULL) {
            return terrno;
          }

          dataIndex++;
        } else {
          void* colData = colDataGetData(pColData, j);
          if (IS_VAR_DATA_TYPE(pCol->type)) { // address copy, no value
            SValue sv =
                (SValue){.type = pCol->type, .nData = varDataLen(colData), .pData = (uint8_t*)varDataVal(colData)};
            SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
            void* p = taosArrayPush(pVals, &cv);
            if (p == NULL) {
              return terrno;
            }
          } else {
            SValue sv = {.type = pCol->type};
            memcpy(&sv.val, colData, tDataTypes[pCol->type].bytes);
            SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
            void* p = taosArrayPush(pVals, &cv);
            if (p == NULL) {
              return terrno;
            }
          }
          dataIndex++;
        }
      }
    }

    SRow* pRow = NULL;
    code = tRowBuild(pVals, (STSchema*)pTSchema, &pRow);
    if (code != TSDB_CODE_SUCCESS) {
      tDestroySubmitTbData(pTableData, TSDB_MSG_FLG_ENCODE);
      taosArrayDestroy(pVals);
      tqError("s-task:%s build rows for submit failed, ts:%"PRId64, id, ts);
      return code;
    }

    void* p = taosArrayPush(pTableData->aRowP, &pRow);
    if (p == NULL) {
      return terrno;
    }
  }

  taosArrayDestroy(pVals);
  return TSDB_CODE_SUCCESS;
}

int32_t doWaitForDstTableCreated(SVnode* pVnode, SStreamTask* pTask, STableSinkInfo* pTableSinkInfo,
                                 const char* dstTableName, int64_t* uid) {
  int32_t     vgId = TD_VID(pVnode);
  int64_t     suid = pTask->outputInfo.tbSink.stbUid;
  const char* id = pTask->id.idStr;

  while (pTableSinkInfo->uid == 0) {
    if (streamTaskShouldStop(pTask)) {
      tqDebug("s-task:%s task will stop, quit from waiting for table:%s create", id, dstTableName);
      return TSDB_CODE_STREAM_EXEC_CANCELLED;
    }

    // wait for the table to be created
    SMetaReader mr = {0};
    metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);

    int32_t code = metaGetTableEntryByName(&mr, dstTableName);
    if (code == 0) {  // table already exists, check its type and uid
      bool isValid = isValidDstChildTable(&mr, vgId, dstTableName, suid);
      if (isValid) {  // not valid table, ignore it
        tqDebug("s-task:%s set uid:%" PRIu64 " for dstTable:%s from meta", id, mr.me.uid, pTableSinkInfo->name.data);
        // set the destination table uid
        (*uid) = mr.me.uid;
        pTableSinkInfo->uid = mr.me.uid;
      }

      metaReaderClear(&mr);
      return terrno;
    } else {  // not exist, wait and retry
      metaReaderClear(&mr);
      taosMsleep(100);
      tqDebug("s-task:%s wait 100ms for the table:%s ready before insert data", id, dstTableName);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doCreateSinkInfo(const char* pDstTableName, STableSinkInfo** pInfo) {
  int32_t nameLen = strlen(pDstTableName);
  (*pInfo) = taosMemoryCalloc(1, sizeof(STableSinkInfo) + nameLen + 1);
  if (*pInfo == NULL) {
    return terrno;
  }

  (*pInfo)->name.len = nameLen;
  memcpy((*pInfo)->name.data, pDstTableName, nameLen);
  return TSDB_CODE_SUCCESS;
}

int32_t setDstTableDataUid(SVnode* pVnode, SStreamTask* pTask, SSDataBlock* pDataBlock, char* stbFullName,
                           SSubmitTbData* pTableData) {
  uint64_t        groupId = pDataBlock->info.id.groupId;
  char*           dstTableName = pDataBlock->info.parTbName;
  int32_t         numOfRows = pDataBlock->info.rows;
  const char*     id = pTask->id.idStr;
  int64_t         suid = pTask->outputInfo.tbSink.stbUid;
  STSchema*       pTSchema = pTask->outputInfo.tbSink.pTSchema;
  int32_t         vgId = TD_VID(pVnode);
  STableSinkInfo* pTableSinkInfo = NULL;
  int32_t         code = 0;

  bool alreadyCached = tqGetTableInfo(pTask->outputInfo.tbSink.pTblInfo, groupId, &pTableSinkInfo);

  if (alreadyCached) {
    if (dstTableName[0] == 0) {  // data block does not set the destination table name
      tstrncpy(dstTableName, pTableSinkInfo->name.data, pTableSinkInfo->name.len + 1);
      tqDebug("s-task:%s vgId:%d, gropuId:%" PRIu64 " datablock table name is null, set name:%s", id, vgId, groupId,
              dstTableName);
    } else {
      tstrncpy(dstTableName, pTableSinkInfo->name.data, pTableSinkInfo->name.len + 1);
      if (pTableSinkInfo->uid != 0) {
        tqDebug("s-task:%s write %d rows into groupId:%" PRIu64 " dstTable:%s(uid:%" PRIu64 ")", id, numOfRows, groupId,
                dstTableName, pTableSinkInfo->uid);
      } else {
        tqDebug("s-task:%s write %d rows into groupId:%" PRIu64 " dstTable:%s(not set uid yet for the secondary block)",
                id, numOfRows, groupId, dstTableName);
      }
    }
  } else {  // this groupId has not been kept in cache yet
    if (dstTableName[0] == 0) {
      memset(dstTableName, 0, TSDB_TABLE_NAME_LEN);
      code = buildCtbNameByGroupIdImpl(stbFullName, groupId, dstTableName);
      if (code) {
        tqDebug("s-task:%s failed to build auto create table-name:%s, groupId:0x%" PRId64, id, dstTableName, groupId);
        return code;
      }
    } else {
      if (pTask->subtableWithoutMd5 != 1 && !isAutoTableName(dstTableName) &&
          !alreadyAddGroupId(dstTableName, groupId) && groupId != 0) {
        tqDebug("s-task:%s append groupId:%" PRId64 " for generated dstTable:%s", id, groupId, dstTableName);
        if (pTask->ver == SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
          buildCtbNameAddGroupId(NULL, dstTableName, groupId);
        } else if (pTask->ver > SSTREAM_TASK_SUBTABLE_CHANGED_VER && stbFullName) {
          buildCtbNameAddGroupId(stbFullName, dstTableName, groupId);
        }
      }
    }

    code = doCreateSinkInfo(dstTableName, &pTableSinkInfo);
    if (code == 0) {
      tqDebug("s-task:%s build new sinkTableInfo to add cache, dstTable:%s", id, dstTableName);
    } else {
      tqDebug("s-task:%s failed to build new sinkTableInfo, dstTable:%s", id, dstTableName);
      return code;
    }
  }

  if (alreadyCached) {
    pTableData->uid = pTableSinkInfo->uid;

    if (pTableData->uid == 0) {
      tqTrace("s-task:%s cached tableInfo:%s uid is invalid, acquire it from meta", id, pTableSinkInfo->name.data);
      return doWaitForDstTableCreated(pVnode, pTask, pTableSinkInfo, dstTableName, &pTableData->uid);
    } else {
      tqTrace("s-task:%s set the dstTable uid from cache:%" PRId64, id, pTableData->uid);
    }
  } else {
    // The auto-create option will always set to be open for those submit messages, which arrives during the period
    // the creating of the destination table, due to the absence of the user-specified table in TSDB. When scanning
    // data from WAL, those submit messages, with auto-created table option, will be discarded expect the first, for
    // those mismatched table uids. Only the FIRST table has the correct table uid, and those remain all have
    // randomly generated, but false table uid in the WAL.
    SMetaReader mr = {0};
    metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);

    // table not in cache, let's try to extract it from tsdb meta
    if (metaGetTableEntryByName(&mr, dstTableName) < 0) {
      metaReaderClear(&mr);

      if (pTask->outputInfo.tbSink.autoCreateCtb) {
        tqDebug("s-task:%s stream write into table:%s, table auto created", id, dstTableName);

        SArray* pTagArray = taosArrayInit(pTSchema->numOfCols + 1, sizeof(STagVal));
        if (pTagArray == NULL) {
          return terrno;
        }

        pTableData->flags = SUBMIT_REQ_AUTO_CREATE_TABLE;
        code =
            buildAutoCreateTableReq(stbFullName, suid, pTSchema->numOfCols + 1, pDataBlock, pTagArray,
                                    (pTask->ver >= SSTREAM_TASK_SUBTABLE_CHANGED_VER && pTask->subtableWithoutMd5 != 1),
                                    &pTableData->pCreateTbReq);
        taosArrayDestroy(pTagArray);

        if (code) {
          tqError("s-task:%s failed to build auto create dst-table req:%s, code:%s", id, dstTableName, tstrerror(code));
          taosMemoryFree(pTableSinkInfo);
          return code;
        }

        pTableSinkInfo->uid = 0;
        code = doPutIntoCache(pTask->outputInfo.tbSink.pTblInfo, pTableSinkInfo, groupId, id);
      } else {
        metaReaderClear(&mr);

        tqError("s-task:%s vgId:%d dst-table:%s not auto-created, and not create in tsdb, discard data", id,
                vgId, dstTableName);
        return TSDB_CODE_TDB_TABLE_NOT_EXIST;
      }
    } else {
      bool isValid = isValidDstChildTable(&mr, vgId, dstTableName, suid);
      if (!isValid) {
        metaReaderClear(&mr);
        taosMemoryFree(pTableSinkInfo);
        tqError("s-task:%s vgId:%d table:%s already exists, but not child table, stream results is discarded", id, vgId,
                dstTableName);
        return terrno;
      } else {
        pTableData->uid = mr.me.uid;
        pTableSinkInfo->uid = mr.me.uid;

        metaReaderClear(&mr);
        code = doPutIntoCache(pTask->outputInfo.tbSink.pTblInfo, pTableSinkInfo, groupId, id);
      }
    }
  }

  return code;
}

int32_t tqSetDstTableDataPayload(uint64_t suid, const STSchema *pTSchema, int32_t blockIndex, SSDataBlock* pDataBlock,
                               SSubmitTbData* pTableData, int64_t earlyTs, const char* id) {
  int32_t numOfRows = pDataBlock->info.rows;
  char*   dstTableName = pDataBlock->info.parTbName;

  tqDebug("s-task:%s sink data pipeline, build submit msg from %dth resBlock, including %d rows, dst suid:%" PRId64, id,
          blockIndex + 1, numOfRows, suid);

  // convert all rows
  int32_t code = doConvertRows(pTableData, pTSchema, pDataBlock, earlyTs, id);
  if (code != TSDB_CODE_SUCCESS) {
    tqError("s-task:%s failed to convert rows from result block, code:%s", id, tstrerror(terrno));
    return code;
  }

  if (pTableData->aRowP != NULL) {
    taosArraySort(pTableData->aRowP, tsAscendingSortFn);
    tqTrace("s-task:%s build submit msg for dstTable:%s, numOfRows:%d", id, dstTableName, numOfRows);
  }

  return code;
}

void tqSinkDataIntoDstTable(SStreamTask* pTask, void* vnode, void* data) {
  const SArray*    pBlocks = (const SArray*)data;
  SVnode*          pVnode = (SVnode*)vnode;
  int64_t          suid = pTask->outputInfo.tbSink.stbUid;
  char*            stbFullName = pTask->outputInfo.tbSink.stbFullName;
  STSchema*        pTSchema = pTask->outputInfo.tbSink.pTSchema;
  int32_t          vgId = TD_VID(pVnode);
  int32_t          numOfBlocks = taosArrayGetSize(pBlocks);
  int32_t          code = TSDB_CODE_SUCCESS;
  const char*      id = pTask->id.idStr;
  int64_t          earlyTs = tsdbGetEarliestTs(pVnode->pTsdb);
  STaskOutputInfo* pOutputInfo = &pTask->outputInfo;

  if (pTask->outputInfo.tbSink.pTagSchema == NULL) {
    SMetaReader mer1 = {0};
    metaReaderDoInit(&mer1, pVnode->pMeta, META_READER_LOCK);

    code = metaReaderGetTableEntryByUid(&mer1, pOutputInfo->tbSink.stbUid);
    if (code != TSDB_CODE_SUCCESS) {
      tqError("s-task:%s vgId:%d failed to get the dst stable, failed to sink results", id, vgId);
      metaReaderClear(&mer1);
      return;
    }

    pOutputInfo->tbSink.pTagSchema = tCloneSSchemaWrapper(&mer1.me.stbEntry.schemaTag);
    metaReaderClear(&mer1);

    SSchemaWrapper* pTagSchema = pOutputInfo->tbSink.pTagSchema;
    SSchema*        pCol1 = &pTagSchema->pSchema[0];
    if (pTagSchema->nCols == 1 && pCol1->type == TSDB_DATA_TYPE_UBIGINT && strcmp(pCol1->name, "group_id") == 0) {
      pOutputInfo->tbSink.autoCreateCtb = true;
    } else {
      pOutputInfo->tbSink.autoCreateCtb = false;
    }
  }

  bool onlySubmitData = hasOnlySubmitData(pBlocks, numOfBlocks);
  if (!onlySubmitData) {
    tqDebug("vgId:%d, s-task:%s write %d stream resBlock(s) into table, has delete block, submit one-by-one", vgId, id,
            numOfBlocks);

    for (int32_t i = 0; i < numOfBlocks; ++i) {
      if (streamTaskShouldStop(pTask)) {
        return;
      }

      SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
      if (pDataBlock == NULL) {
        continue;
      }

      if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
        code = doBuildAndSendDeleteMsg(pVnode, stbFullName, pDataBlock, pTask, suid);
      } else if (pDataBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
        code = doBuildAndSendCreateTableMsg(pVnode, stbFullName, pDataBlock, pTask, suid);
      } else if (pDataBlock->info.type == STREAM_CHECKPOINT) {
        continue;
      } else {
        pTask->execInfo.sink.numOfBlocks += 1;

        SSubmitReq2 submitReq = {.aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData))};
        if (submitReq.aSubmitTbData == NULL) {
          code = terrno;
          tqError("s-task:%s vgId:%d failed to prepare submit msg in sink task, code:%s", id, vgId, tstrerror(code));
          return;
        }

        SSubmitTbData tbData = {.suid = suid, .uid = 0, .sver = pTSchema->version, .flags = TD_REQ_FROM_APP};
        code = setDstTableDataUid(pVnode, pTask, pDataBlock, stbFullName, &tbData);
        if (code != TSDB_CODE_SUCCESS) {
          tqError("vgId:%d s-task:%s dst-table not exist, stb:%s discard stream results", vgId, id, stbFullName);
          continue;
        }

        code = tqSetDstTableDataPayload(suid, pTSchema, i, pDataBlock, &tbData, earlyTs, id);
        if (code != TSDB_CODE_SUCCESS || tbData.aRowP == NULL) {
          if (tbData.pCreateTbReq != NULL) {
            tdDestroySVCreateTbReq(tbData.pCreateTbReq);
            (void) doRemoveFromCache(pTask->outputInfo.tbSink.pTblInfo, pDataBlock->info.id.groupId, id);
            tbData.pCreateTbReq = NULL;
          }
          continue;
        }

        void* p = taosArrayPush(submitReq.aSubmitTbData, &tbData);
        if (p == NULL) {
          tqDebug("vgId:%d, s-task:%s failed to build submit msg, data lost", vgId, id);
        }

        code = doBuildAndSendSubmitMsg(pVnode, pTask, &submitReq, 1);
        if (code) {  // failed and continue
          tqDebug("vgId:%d, s-task:%s submit msg failed, data lost", vgId, id);
        }
      }
    }
  } else {
    tqDebug("vgId:%d, s-task:%s write %d stream resBlock(s) into table, merge submit msg", vgId, id, numOfBlocks);
    SHashObj* pTableIndexMap =
        taosHashInit(numOfBlocks, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);

    SSubmitReq2 submitReq = {.aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData))};
    if (submitReq.aSubmitTbData == NULL) {
      code = terrno;
      tqError("s-task:%s vgId:%d failed to prepare submit msg in sink task, code:%s", id, vgId, tstrerror(code));
      taosHashCleanup(pTableIndexMap);
      return;
    }

    bool hasSubmit = false;
    for (int32_t i = 0; i < numOfBlocks; i++) {
      if (streamTaskShouldStop(pTask)) {
        taosHashCleanup(pTableIndexMap);
        tDestroySubmitReq(&submitReq, TSDB_MSG_FLG_ENCODE);
        return;
      }

      SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
      if (pDataBlock == NULL) {
        continue;
      }

      if (pDataBlock->info.type == STREAM_CHECKPOINT) {
        continue;
      }

      hasSubmit = true;
      pTask->execInfo.sink.numOfBlocks += 1;
      uint64_t groupId = pDataBlock->info.id.groupId;

      SSubmitTbData tbData = {.suid = suid, .uid = 0, .sver = pTSchema->version, .flags = TD_REQ_FROM_APP};

      int32_t* index = taosHashGet(pTableIndexMap, &groupId, sizeof(groupId));
      if (index == NULL) {  // no data yet, append it
        code = setDstTableDataUid(pVnode, pTask, pDataBlock, stbFullName, &tbData);
        if (code != TSDB_CODE_SUCCESS) {
          tqError("vgId:%d dst-table gid:%" PRId64 " not exist, discard stream results", vgId, groupId);
          continue;
        }

        code = tqSetDstTableDataPayload(suid, pTSchema, i, pDataBlock, &tbData, earlyTs, id);
        if (code != TSDB_CODE_SUCCESS || tbData.aRowP == NULL) {
          if (tbData.pCreateTbReq != NULL) {
            tdDestroySVCreateTbReq(tbData.pCreateTbReq);
            (void) doRemoveFromCache(pTask->outputInfo.tbSink.pTblInfo, groupId, id);
            tbData.pCreateTbReq = NULL;
          }
          continue;
        }

        void* p = taosArrayPush(submitReq.aSubmitTbData, &tbData);
        if (p == NULL) {
          tqError("vgId:%d, s-task:%s failed to build submit msg, data lost", vgId, id);
          continue;
        }

        int32_t size = (int32_t)taosArrayGetSize(submitReq.aSubmitTbData) - 1;
        code = taosHashPut(pTableIndexMap, &groupId, sizeof(groupId), &size, sizeof(size));
        if (code) {
          tqError("vgId:%d, s-task:%s failed to put group into index map, code:%s", vgId, id, tstrerror(code));
          continue;
        }
      } else {
        code = tqSetDstTableDataPayload(suid, pTSchema, i, pDataBlock, &tbData, earlyTs, id);
        if (code != TSDB_CODE_SUCCESS || tbData.aRowP == NULL) {
          if (tbData.pCreateTbReq != NULL) {
            tdDestroySVCreateTbReq(tbData.pCreateTbReq);
            tbData.pCreateTbReq = NULL;
          }
          continue;
        }

        SSubmitTbData* pExisted = taosArrayGet(submitReq.aSubmitTbData, *index);
        if (pExisted == NULL) {
          continue;
        }

        code = doMergeExistedRows(pExisted, &tbData, id);
        if (code != TSDB_CODE_SUCCESS) {
          continue;
        }
      }

      pTask->execInfo.sink.numOfRows += pDataBlock->info.rows;
    }

    taosHashCleanup(pTableIndexMap);

    if (hasSubmit) {
      code = doBuildAndSendSubmitMsg(pVnode, pTask, &submitReq, numOfBlocks);
      if (code) {  // failed and continue
        tqError("vgId:%d failed to build and send submit msg", vgId);
      }
    } else {
      tDestroySubmitReq(&submitReq, TSDB_MSG_FLG_ENCODE);
      tqDebug("vgId:%d, s-task:%s write results completed", vgId, id);
    }
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
bool hasOnlySubmitData(const SArray* pBlocks, int32_t numOfBlocks) {
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock* p = taosArrayGet(pBlocks, i);
    if (p == NULL) {
      continue;
    }

    if (p->info.type == STREAM_DELETE_RESULT || p->info.type == STREAM_CREATE_CHILD_TABLE) {
      return false;
    }
  }

  return true;
}

int32_t doPutIntoCache(SSHashObj* pSinkTableMap, STableSinkInfo* pTableSinkInfo, uint64_t groupId, const char* id) {
  int32_t code = tSimpleHashPut(pSinkTableMap, &groupId, sizeof(uint64_t), &pTableSinkInfo, POINTER_BYTES);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(pTableSinkInfo);
  } else {
    tqDebug("s-task:%s new dst table:%s(uid:%" PRIu64 ") added into cache, total:%d", id, pTableSinkInfo->name.data,
            pTableSinkInfo->uid, tSimpleHashGetSize(pSinkTableMap));
  }

  return code;
}

int32_t doRemoveFromCache(SSHashObj* pSinkTableMap, uint64_t groupId, const char* id) {
  if (tSimpleHashGetSize(pSinkTableMap) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = tSimpleHashRemove(pSinkTableMap, &groupId, sizeof(groupId));
  if (code == 0) {
    tqDebug("s-task:%s remove cached table meta for groupId:%" PRId64, id, groupId);
  } else {
    tqError("s-task:%s failed to remove table meta from hashmap, groupId:%" PRId64, id, groupId);
  }
  return code;
}

int32_t doBuildAndSendDeleteMsg(SVnode* pVnode, char* stbFullName, SSDataBlock* pDataBlock, SStreamTask* pTask,
                                int64_t suid) {
  SBatchDeleteReq deleteReq = {.suid = suid, .deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq))};
  if (deleteReq.deleteReqs == NULL) {
    return terrno;
  }

  int32_t code = tqBuildDeleteReq(pVnode->pTq, stbFullName, pDataBlock, &deleteReq, pTask->id.idStr,
                                  pTask->ver >= SSTREAM_TASK_SUBTABLE_CHANGED_VER && pTask->subtableWithoutMd5 != 1);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (taosArrayGetSize(deleteReq.deleteReqs) == 0) {
    taosArrayDestroy(deleteReq.deleteReqs);
    return TSDB_CODE_SUCCESS;
  }

  int32_t len;
  tEncodeSize(tEncodeSBatchDeleteReq, &deleteReq, len, code);
  if (code != TSDB_CODE_SUCCESS) {
    qError("s-task:%s failed to encode delete request", pTask->id.idStr);
    return code;
  }

  SEncoder encoder = {0};
  void*    serializedDeleteReq = rpcMallocCont(len + sizeof(SMsgHead));
  void*    abuf = POINTER_SHIFT(serializedDeleteReq, sizeof(SMsgHead));
  tEncoderInit(&encoder, abuf, len);
  code = tEncodeSBatchDeleteReq(&encoder, &deleteReq);
  tEncoderClear(&encoder);
  taosArrayDestroy(deleteReq.deleteReqs);

  if (code) {
    return code;
  }

  ((SMsgHead*)serializedDeleteReq)->vgId = TD_VID(pVnode);

  SRpcMsg msg = {.msgType = TDMT_VND_BATCH_DEL, .pCont = serializedDeleteReq, .contLen = len + sizeof(SMsgHead)};
  if (tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) != 0) {
    tqDebug("failed to put delete req into write-queue since %s", terrstr());
  }

  return TSDB_CODE_SUCCESS;
}