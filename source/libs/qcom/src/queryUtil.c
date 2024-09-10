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

#include "os.h"
#include "query.h"
#include "tglobal.h"
#include "tmsg.h"
#include "trpc.h"
#include "tsched.h"
#include "tworker.h"
// clang-format off
#include "cJSON.h"
#include "queryInt.h"

typedef struct STaskQueue {
  SQueryAutoQWorkerPool wrokrerPool;
  STaosQueue* pTaskQueue;
} STaskQueue;

int32_t getAsofJoinReverseOp(EOperatorType op) {
  switch (op) {
    case OP_TYPE_GREATER_THAN:
      return OP_TYPE_LOWER_THAN;
    case OP_TYPE_GREATER_EQUAL:
      return OP_TYPE_LOWER_EQUAL;
    case OP_TYPE_LOWER_THAN:
      return OP_TYPE_GREATER_THAN;
    case OP_TYPE_LOWER_EQUAL:
      return OP_TYPE_GREATER_EQUAL;
    case OP_TYPE_EQUAL:
      return OP_TYPE_EQUAL;
    default:
      break;
  }

  return -1;
}

const SSchema* tGetTbnameColumnSchema() { 
  static struct SSchema _s = {
      .colId = TSDB_TBNAME_COLUMN_INDEX,
      .type = TSDB_DATA_TYPE_BINARY,
      .bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE,
      .name = "tbname",
  };
  return &_s; 
}

static bool doValidateSchema(SSchema* pSchema, int32_t numOfCols, int32_t maxLen) {
  int32_t rowLen = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    // 1. valid types
    if (!isValidDataType(pSchema[i].type)) {
      return false;
    }

    // 2. valid length for each type
    if (pSchema[i].type == TSDB_DATA_TYPE_BINARY || pSchema[i].type == TSDB_DATA_TYPE_VARBINARY) {
      if (pSchema[i].bytes > TSDB_MAX_BINARY_LEN) {
        return false;
      }
    } else if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
      if (pSchema[i].bytes > TSDB_MAX_NCHAR_LEN) {
        return false;
      }
    } else if (pSchema[i].type == TSDB_DATA_TYPE_GEOMETRY) {
      if (pSchema[i].bytes > TSDB_MAX_GEOMETRY_LEN) {
        return false;
      }
    } else {
      if (pSchema[i].bytes != tDataTypes[pSchema[i].type].bytes) {
        return false;
      }
    }

    // 3. valid column names
    for (int32_t j = i + 1; j < numOfCols; ++j) {
      if (strncmp(pSchema[i].name, pSchema[j].name, sizeof(pSchema[i].name) - 1) == 0) {
        return false;
      }
    }

    rowLen += pSchema[i].bytes;
  }

  return rowLen <= maxLen;
}

bool tIsValidSchema(struct SSchema* pSchema, int32_t numOfCols, int32_t numOfTags) {
  if (!VALIDNUMOFCOLS(numOfCols)) {
    return false;
  }

  if (!VALIDNUMOFTAGS(numOfTags)) {
    return false;
  }

  /* first column must be the timestamp, which is a primary key */
  if (pSchema[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    return false;
  }

  if (!doValidateSchema(pSchema, numOfCols, TSDB_MAX_BYTES_PER_ROW)) {
    return false;
  }

  if (!doValidateSchema(&pSchema[numOfCols], numOfTags, TSDB_MAX_TAGS_LEN)) {
    return false;
  }

  return true;
}

static STaskQueue taskQueue = {0};

static void processTaskQueue(SQueueInfo *pInfo, SSchedMsg *pSchedMsg) {
  __async_exec_fn_t execFn = (__async_exec_fn_t)pSchedMsg->ahandle;
  (void)execFn(pSchedMsg->thandle);
  taosFreeQitem(pSchedMsg);
}

int32_t initTaskQueue() {
  taskQueue.wrokrerPool.name = "taskWorkPool";
  taskQueue.wrokrerPool.min = tsNumOfTaskQueueThreads;
  taskQueue.wrokrerPool.max = tsNumOfTaskQueueThreads;
  int32_t coce = tQueryAutoQWorkerInit(&taskQueue.wrokrerPool);
  if (TSDB_CODE_SUCCESS != coce) {
    qError("failed to init task thread pool");
    return -1;
  }

  taskQueue.pTaskQueue = tQueryAutoQWorkerAllocQueue(&taskQueue.wrokrerPool, NULL, (FItem)processTaskQueue);
  if (NULL == taskQueue.pTaskQueue) {
    qError("failed to init task queue");
    return -1;
  }

  qDebug("task queue is initialized, numOfThreads: %d", tsNumOfTaskQueueThreads);
  return 0;
}

int32_t cleanupTaskQueue() {
  tQueryAutoQWorkerCleanup(&taskQueue.wrokrerPool);
  return 0;
}

int32_t taosAsyncExec(__async_exec_fn_t execFn, void* execParam, int32_t* code) {
  SSchedMsg* pSchedMsg; 
  int32_t rc = taosAllocateQitem(sizeof(SSchedMsg), DEF_QITEM, 0, (void **)&pSchedMsg);
  if (rc) return rc;
  pSchedMsg->fp = NULL;
  pSchedMsg->ahandle = execFn;
  pSchedMsg->thandle = execParam;
  pSchedMsg->msg = code;

  return taosWriteQitem(taskQueue.pTaskQueue, pSchedMsg);
}

int32_t taosAsyncWait() {
  if (!taskQueue.wrokrerPool.pCb) {
    qError("query task thread pool callback function is null");
    return -1;
  }
  return taskQueue.wrokrerPool.pCb->beforeBlocking(&taskQueue.wrokrerPool);
}

int32_t taosAsyncRecover() {
  if (!taskQueue.wrokrerPool.pCb) {
    qError("query task thread pool callback function is null");
    return -1;
  }
  return taskQueue.wrokrerPool.pCb->afterRecoverFromBlocking(&taskQueue.wrokrerPool);
}

void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  if (NULL == pMsgBody) {
    return;
  }

  taosMemoryFreeClear(pMsgBody->target.dbFName);
  taosMemoryFreeClear(pMsgBody->msgInfo.pData);
  if (pMsgBody->paramFreeFp) {
    (*pMsgBody->paramFreeFp)(pMsgBody->param);
  }
  taosMemoryFreeClear(pMsgBody);
}
void destroyAhandle(void *ahandle) {
  SMsgSendInfo *pSendInfo = ahandle;
  if (pSendInfo == NULL) return;

  destroySendMsgInfo(pSendInfo);
}

int32_t asyncSendMsgToServerExt(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, SMsgSendInfo* pInfo,
                                bool persistHandle, void* rpcCtx) {
  char* pMsg = rpcMallocCont(pInfo->msgInfo.len);
  if (NULL == pMsg) {
    qError("0x%" PRIx64 " msg:%s malloc failed", pInfo->requestId, TMSG_INFO(pInfo->msgType));
    destroySendMsgInfo(pInfo);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  memcpy(pMsg, pInfo->msgInfo.pData, pInfo->msgInfo.len);
  SRpcMsg rpcMsg = {
    .msgType = pInfo->msgType,
    .pCont = pMsg,
    .contLen = pInfo->msgInfo.len,
    .info.ahandle = (void*)pInfo,
    .info.handle = pInfo->msgInfo.handle,
    .info.persistHandle = persistHandle,
    .code = 0
  };
  TRACE_SET_ROOTID(&rpcMsg.info.traceId, pInfo->requestId);

  int code = rpcSendRequestWithCtx(pTransporter, epSet, &rpcMsg, pTransporterId, rpcCtx);
  if (code) {
    destroySendMsgInfo(pInfo);
  }
  return code;
}

int32_t asyncSendMsgToServer(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, SMsgSendInfo* pInfo) {
  return asyncSendMsgToServerExt(pTransporter, epSet, pTransporterId, pInfo, false, NULL);
}
int32_t asyncFreeConnById(void* pTransporter, int64_t pid) {
  return rpcFreeConnById(pTransporter, pid);
}

char* jobTaskStatusStr(int32_t status) {
  switch (status) {
    case JOB_TASK_STATUS_NULL:
      return "NULL";
    case JOB_TASK_STATUS_INIT:
      return "INIT";
    case JOB_TASK_STATUS_EXEC:
      return "EXECUTING";
    case JOB_TASK_STATUS_PART_SUCC:
      return "PARTIAL_SUCCEED";
    case JOB_TASK_STATUS_FETCH:
      return "FETCHING";
    case JOB_TASK_STATUS_SUCC:
      return "SUCCEED";
    case JOB_TASK_STATUS_FAIL:
      return "FAILED";
    case JOB_TASK_STATUS_DROP:
      return "DROPPING";
    default:
      break;
  }

  return "UNKNOWN";
}

#if 0
SSchema createSchema(int8_t type, int32_t bytes, col_id_t colId, const char* name) {
  SSchema s = {0};
  s.type = type;
  s.bytes = bytes;
  s.colId = colId;

  tstrncpy(s.name, name, tListLen(s.name));
  return s;
}
#endif

void freeSTableMetaRspPointer(void *p) {
  tFreeSTableMetaRsp(*(void**)p);
  taosMemoryFreeClear(*(void**)p);
}

void destroyQueryExecRes(SExecResult* pRes) {
  if (NULL == pRes || NULL == pRes->res) {
    return;
  }

  switch (pRes->msgType) {
    case TDMT_VND_CREATE_TABLE: {
      taosArrayDestroyEx((SArray*)pRes->res, freeSTableMetaRspPointer);
      break;
    }
    case TDMT_MND_CREATE_STB:
    case TDMT_VND_ALTER_TABLE:
    case TDMT_MND_ALTER_STB: {
      tFreeSTableMetaRsp(pRes->res);
      taosMemoryFreeClear(pRes->res);
      break;
    }
    case TDMT_VND_SUBMIT: {
      tDestroySSubmitRsp2((SSubmitRsp2*)pRes->res, TSDB_MSG_FLG_DECODE);
      taosMemoryFreeClear(pRes->res);
      break;
    }
    case TDMT_SCH_QUERY:
    case TDMT_SCH_MERGE_QUERY: {
      taosArrayDestroy((SArray*)pRes->res);
      break;
    }
    default:
      qError("invalid exec result for request type %d", pRes->msgType);
  }
}
// clang-format on

int32_t dataConverToStr(char* str, int type, void* buf, int32_t bufSize, int32_t* len) {
  int32_t n = 0;

  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      n = sprintf(str, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      n = sprintf(str, (*(int8_t*)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = sprintf(str, "%d", *(int8_t*)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = sprintf(str, "%d", *(int16_t*)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = sprintf(str, "%d", *(int32_t*)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = sprintf(str, "%" PRId64, *(int64_t*)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = sprintf(str, "%e", GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = sprintf(str, "%e", GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_VARBINARY: {
      if (bufSize < 0) {
        //        tscError("invalid buf size");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      void*    data = NULL;
      uint32_t size = 0;
      if (taosAscii2Hex(buf, bufSize, &data, &size) < 0) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      *str = '"';
      memcpy(str + 1, data, size);
      *(str + size + 1) = '"';
      n = size + 2;
      taosMemoryFree(data);
      break;
    }
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      if (bufSize < 0) {
        //        tscError("invalid buf size");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      *str = '"';
      memcpy(str + 1, buf, bufSize);
      *(str + bufSize + 1) = '"';
      n = bufSize + 2;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (bufSize < 0) {
        //        tscError("invalid buf size");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      *str = '"';
      int32_t length = taosUcs4ToMbs((TdUcs4*)buf, bufSize, str + 1);
      if (length <= 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      *(str + length + 1) = '"';
      n = length + 2;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      n = sprintf(str, "%d", *(uint8_t*)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = sprintf(str, "%d", *(uint16_t*)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = sprintf(str, "%u", *(uint32_t*)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = sprintf(str, "%" PRIu64, *(uint64_t*)buf);
      break;

    default:
      //      tscError("unsupported type:%d", type);
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (len) *len = n;

  return TSDB_CODE_SUCCESS;
}

void parseTagDatatoJson(void* p, char** jsonStr) {
  char*   string = NULL;
  SArray* pTagVals = NULL;
  cJSON*  json = NULL;
  if (tTagToValArray((const STag*)p, &pTagVals) != 0) {
    goto end;
  }

  int16_t nCols = taosArrayGetSize(pTagVals);
  if (nCols == 0) {
    goto end;
  }
  char tagJsonKey[256] = {0};
  json = cJSON_CreateObject();
  if (json == NULL) {
    goto end;
  }
  for (int j = 0; j < nCols; ++j) {
    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, j);
    if (pTagVal == NULL) {
      continue;
    }
    // json key  encode by binary
    tstrncpy(tagJsonKey, pTagVal->pKey, sizeof(tagJsonKey));
    // json value
    char type = pTagVal->type;
    if (type == TSDB_DATA_TYPE_NULL) {
      cJSON* value = cJSON_CreateNull();
      if (value == NULL) {
        goto end;
      }
      if (!cJSON_AddItemToObject(json, tagJsonKey, value)) {
        goto end;
      }
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      cJSON* value = NULL;
      if (pTagVal->nData > 0) {
        char* tagJsonValue = taosMemoryCalloc(pTagVal->nData, 1);
        if (tagJsonValue == NULL) {
          goto end;
        }
        int32_t length = taosUcs4ToMbs((TdUcs4*)pTagVal->pData, pTagVal->nData, tagJsonValue);
        if (length < 0) {
          qError("charset:%s to %s. val:%s convert json value failed.", DEFAULT_UNICODE_ENCODEC, tsCharset,
                 pTagVal->pData);
          taosMemoryFree(tagJsonValue);
          goto end;
        }
        value = cJSON_CreateString(tagJsonValue);
        taosMemoryFree(tagJsonValue);
        if (value == NULL) {
          goto end;
        }
      } else if (pTagVal->nData == 0) {
        value = cJSON_CreateString("");
        if (value == NULL) {
          goto end;
        }
      } else {
        goto end;
      }

      if (!cJSON_AddItemToObject(json, tagJsonKey, value)) {
        goto end;
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double jsonVd = *(double*)(&pTagVal->i64);
      cJSON* value = cJSON_CreateNumber(jsonVd);
      if (value == NULL) {
        goto end;
      }
      if (!cJSON_AddItemToObject(json, tagJsonKey, value)) {
        goto end;
      }
    } else if (type == TSDB_DATA_TYPE_BOOL) {
      char   jsonVd = *(char*)(&pTagVal->i64);
      cJSON* value = cJSON_CreateBool(jsonVd);
      if (value == NULL) {
        goto end;
      }
      if (!cJSON_AddItemToObject(json, tagJsonKey, value)) {
        goto end;
      }
    } else {
      goto end;
    }
  }
  string = cJSON_PrintUnformatted(json);
end:
  cJSON_Delete(json);
  taosArrayDestroy(pTagVals);
  if (string == NULL) {
    string = taosStrdup(TSDB_DATA_NULL_STR_L);
  }
  *jsonStr = string;
}

int32_t cloneTableMeta(STableMeta* pSrc, STableMeta** pDst) {
  if (NULL == pSrc) {
    *pDst = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfField = pSrc->tableInfo.numOfColumns + pSrc->tableInfo.numOfTags;
  if (numOfField > TSDB_MAX_COL_TAG_NUM || numOfField < TSDB_MIN_COLUMNS) {
    *pDst = NULL;
    qError("too many column and tag num:%d,%d", pSrc->tableInfo.numOfColumns, pSrc->tableInfo.numOfTags);
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t metaSize = sizeof(STableMeta) + numOfField * sizeof(SSchema);
  int32_t schemaExtSize = 0;
  if (useCompress(pSrc->tableType) && pSrc->schemaExt) {
    schemaExtSize = pSrc->tableInfo.numOfColumns * sizeof(SSchemaExt);
  }
  *pDst = taosMemoryMalloc(metaSize + schemaExtSize);
  if (NULL == *pDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(*pDst, pSrc, metaSize);
  if (useCompress(pSrc->tableType) && pSrc->schemaExt) {
    (*pDst)->schemaExt = (SSchemaExt*)((char*)*pDst + metaSize);
    memcpy((*pDst)->schemaExt, pSrc->schemaExt, schemaExtSize);
  } else {
    (*pDst)->schemaExt = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

void getColumnTypeFromMeta(STableMeta* pMeta, char* pName, ETableColumnType* pType) {
  int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
  for (int32_t i = 0; i < nums; ++i) {
    if (0 == strcmp(pName, pMeta->schema[i].name)) {
      *pType = (i < pMeta->tableInfo.numOfColumns) ? TCOL_TYPE_COLUMN : TCOL_TYPE_TAG;
      return;
    }
  }

  *pType = TCOL_TYPE_NONE;
}

void freeVgInfo(SDBVgInfo* vgInfo) {
  if (NULL == vgInfo) {
    return;
  }

  taosHashCleanup(vgInfo->vgHash);
  taosArrayDestroy(vgInfo->vgArray);

  taosMemoryFreeClear(vgInfo);
}

int32_t cloneDbVgInfo(SDBVgInfo* pSrc, SDBVgInfo** pDst) {
  if (NULL == pSrc) {
    *pDst = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pDst = taosMemoryMalloc(sizeof(*pSrc));
  if (NULL == *pDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(*pDst, pSrc, sizeof(*pSrc));
  (*pDst)->vgArray = NULL;

  if (pSrc->vgHash) {
    (*pDst)->vgHash = taosHashInit(taosHashGetSize(pSrc->vgHash), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true,
                                   HASH_ENTRY_LOCK);
    if (NULL == (*pDst)->vgHash) {
      taosMemoryFreeClear(*pDst);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SVgroupInfo* vgInfo = NULL;
    void*        pIter = taosHashIterate(pSrc->vgHash, NULL);
    while (pIter) {
      vgInfo = pIter;
      int32_t* vgId = taosHashGetKey(pIter, NULL);

      if (0 != taosHashPut((*pDst)->vgHash, vgId, sizeof(*vgId), vgInfo, sizeof(*vgInfo))) {
        qError("taosHashPut failed, vgId:%d", vgInfo->vgId);
        taosHashCancelIterate(pSrc->vgHash, pIter);
        freeVgInfo(*pDst);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pIter = taosHashIterate(pSrc->vgHash, pIter);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t cloneSVreateTbReq(SVCreateTbReq* pSrc, SVCreateTbReq** pDst) {
  if (NULL == pSrc) {
    *pDst = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pDst = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (NULL == *pDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pDst)->flags = pSrc->flags;
  if (pSrc->name) {
    (*pDst)->name = taosStrdup(pSrc->name);
  }
  (*pDst)->uid = pSrc->uid;
  (*pDst)->btime = pSrc->btime;
  (*pDst)->ttl = pSrc->ttl;
  (*pDst)->commentLen = pSrc->commentLen;
  if (pSrc->comment) {
    (*pDst)->comment = taosStrdup(pSrc->comment);
  }
  (*pDst)->type = pSrc->type;

  if (pSrc->type == TSDB_CHILD_TABLE) {
    if (pSrc->ctb.stbName) {
      (*pDst)->ctb.stbName = taosStrdup(pSrc->ctb.stbName);
    }
    (*pDst)->ctb.tagNum = pSrc->ctb.tagNum;
    (*pDst)->ctb.suid = pSrc->ctb.suid;
    if (pSrc->ctb.tagName) {
      (*pDst)->ctb.tagName = taosArrayDup(pSrc->ctb.tagName, NULL);
    }
    STag* pTag = (STag*)pSrc->ctb.pTag;
    if (pTag) {
      (*pDst)->ctb.pTag = taosMemoryMalloc(pTag->len);
      memcpy((*pDst)->ctb.pTag, pTag, pTag->len);
    }
  } else {
    (*pDst)->ntb.schemaRow.nCols = pSrc->ntb.schemaRow.nCols;
    (*pDst)->ntb.schemaRow.version = pSrc->ntb.schemaRow.nCols;
    if (pSrc->ntb.schemaRow.nCols > 0 && pSrc->ntb.schemaRow.pSchema) {
      (*pDst)->ntb.schemaRow.pSchema = taosMemoryMalloc(pSrc->ntb.schemaRow.nCols * sizeof(SSchema));
      memcpy((*pDst)->ntb.schemaRow.pSchema, pSrc->ntb.schemaRow.pSchema, pSrc->ntb.schemaRow.nCols * sizeof(SSchema));
    }
  }

  return TSDB_CODE_SUCCESS;
}

void freeDbCfgInfo(SDbCfgInfo* pInfo) {
  if (pInfo) {
    taosArrayDestroy(pInfo->pRetensions);
  }
  taosMemoryFree(pInfo);
}

void* getTaskPoolWorkerCb() {
  return taskQueue.wrokrerPool.pCb;
}
