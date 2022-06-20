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

#define VALIDNUMOFCOLS(x) ((x) >= TSDB_MIN_COLUMNS && (x) <= TSDB_MAX_COLUMNS)
#define VALIDNUMOFTAGS(x) ((x) >= 0 && (x) <= TSDB_MAX_TAGS)

static struct SSchema _s = {
    .colId = TSDB_TBNAME_COLUMN_INDEX,
    .type = TSDB_DATA_TYPE_BINARY,
    .bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE,
    .name = "tbname",
};

const SSchema* tGetTbnameColumnSchema() { return &_s; }

static bool doValidateSchema(SSchema* pSchema, int32_t numOfCols, int32_t maxLen) {
  int32_t rowLen = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    // 1. valid types
    if (!isValidDataType(pSchema[i].type)) {
      return false;
    }

    // 2. valid length for each type
    if (pSchema[i].type == TSDB_DATA_TYPE_BINARY) {
      if (pSchema[i].bytes > TSDB_MAX_BINARY_LEN) {
        return false;
      }
    } else if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
      if (pSchema[i].bytes > TSDB_MAX_NCHAR_LEN) {
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

static void* pTaskQueue = NULL;

int32_t initTaskQueue() {
  int32_t queueSize = tsMaxShellConns * 2;
  pTaskQueue = taosInitScheduler(queueSize, tsNumOfTaskQueueThreads, "tsc");
  if (NULL == pTaskQueue) {
    qError("failed to init task queue");
    return -1;
  }

  qDebug("task queue is initialized, numOfThreads: %d", tsNumOfTaskQueueThreads);
  return 0;
}

int32_t cleanupTaskQueue() {
  taosCleanUpScheduler(pTaskQueue);
  return 0;
}

static void execHelper(struct SSchedMsg* pSchedMsg) {
  assert(pSchedMsg != NULL && pSchedMsg->ahandle != NULL);

  __async_exec_fn_t execFn = (__async_exec_fn_t)pSchedMsg->ahandle;
  int32_t           code = execFn(pSchedMsg->thandle);
  if (code != 0 && pSchedMsg->msg != NULL) {
    *(int32_t*)pSchedMsg->msg = code;
  }
}

int32_t taosAsyncExec(__async_exec_fn_t execFn, void* execParam, int32_t* code) {
  assert(execFn != NULL);

  SSchedMsg schedMsg = {0};
  schedMsg.fp = execHelper;
  schedMsg.ahandle = execFn;
  schedMsg.thandle = execParam;
  schedMsg.msg = code;

  taosScheduleTask(pTaskQueue, &schedMsg);
  return 0;
}

int32_t asyncSendMsgToServerExt(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, const SMsgSendInfo* pInfo,
                                bool persistHandle, void* rpcCtx) {
  char* pMsg = rpcMallocCont(pInfo->msgInfo.len);
  if (NULL == pMsg) {
    qError("0x%" PRIx64 " msg:%s malloc failed", pInfo->requestId, TMSG_INFO(pInfo->msgType));
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return terrno;
  }

  memcpy(pMsg, pInfo->msgInfo.pData, pInfo->msgInfo.len);
  SRpcMsg rpcMsg = {.msgType = pInfo->msgType,
                    .pCont = pMsg,
                    .contLen = pInfo->msgInfo.len,
                    .info.ahandle = (void*)pInfo,
                    .info.handle = pInfo->msgInfo.handle,
                    .info.persistHandle = persistHandle,
                    .code = 0};
  assert(pInfo->fp != NULL);
  TRACE_SET_ROOTID(&rpcMsg.info.traceId, pInfo->requestId);
  rpcSendRequestWithCtx(pTransporter, epSet, &rpcMsg, pTransporterId, rpcCtx);
  return TSDB_CODE_SUCCESS;
}

int32_t asyncSendMsgToServer(void* pTransporter, SEpSet* epSet, int64_t* pTransporterId, const SMsgSendInfo* pInfo) {
  return asyncSendMsgToServerExt(pTransporter, epSet, pTransporterId, pInfo, false, NULL);
}

char* jobTaskStatusStr(int32_t status) {
  switch (status) {
    case JOB_TASK_STATUS_NULL:
      return "NULL";
    case JOB_TASK_STATUS_NOT_START:
      return "NOT_START";
    case JOB_TASK_STATUS_EXECUTING:
      return "EXECUTING";
    case JOB_TASK_STATUS_PARTIAL_SUCCEED:
      return "PARTIAL_SUCCEED";
    case JOB_TASK_STATUS_SUCCEED:
      return "SUCCEED";
    case JOB_TASK_STATUS_FAILED:
      return "FAILED";
    case JOB_TASK_STATUS_DROPPING:
      return "DROPPING";
    default:
      break;
  }

  return "UNKNOWN";
}

SSchema createSchema(int8_t type, int32_t bytes, col_id_t colId, const char* name) {
  SSchema s = {0};
  s.type = type;
  s.bytes = bytes;
  s.colId = colId;

  tstrncpy(s.name, name, tListLen(s.name));
  return s;
}

void destroyQueryExecRes(SQueryExecRes* pRes) {
  if (NULL == pRes || NULL == pRes->res) {
    return;
  }

  switch (pRes->msgType) {
    case TDMT_VND_ALTER_TABLE:
    case TDMT_MND_ALTER_STB: {
      tFreeSTableMetaRsp((STableMetaRsp*)pRes->res);
      taosMemoryFreeClear(pRes->res);
      break;
    }
    case TDMT_VND_SUBMIT: {
      tFreeSSubmitRsp((SSubmitRsp*)pRes->res);
      break;
    }
    case TDMT_VND_QUERY: {
      taosArrayDestroy((SArray*)pRes->res);
      break;
    }
    default:
      qError("invalid exec result for request type %d", pRes->msgType);
  }
}
