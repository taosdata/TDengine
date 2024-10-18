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

#include "catalog.h"
#include "clientInt.h"
#include "clientLog.h"
#include "clientMonitor.h"
#include "clientStmt.h"
#include "clientStmt2.h"
#include "functionMgt.h"
#include "os.h"
#include "query.h"
#include "scheduler.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "version.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

static int32_t sentinel = TSC_VAR_NOT_RELEASE;
static int32_t createParseContext(const SRequestObj *pRequest, SParseContext **pCxt, SSqlCallbackWrapper *pWrapper);

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  static int32_t lock = 0;

  for (int i = 1; atomic_val_compare_exchange_32(&lock, 0, 1) != 0; ++i) {
    if (i % 1000 == 0) {
      tscInfo("haven't acquire lock after spin %d times.", i);
      (void)sched_yield();
    }
  }

  int ret = taos_options_imp(option, (const char *)arg);
  atomic_store_32(&lock, 0);
  return ret;
}
// this function may be called by user or system, or by both simultaneously.
void taos_cleanup(void) {
  tscDebug("start to cleanup client environment");
  if (atomic_val_compare_exchange_32(&sentinel, TSC_VAR_NOT_RELEASE, TSC_VAR_RELEASED) != TSC_VAR_NOT_RELEASE) {
    return;
  }

  monitorClose();
  tscStopCrashReport();

  hbMgrCleanUp();

  catalogDestroy();
  schedulerDestroy();

  fmFuncMgtDestroy();
  qCleanupKeywordsTable();

  if (TSDB_CODE_SUCCESS != cleanupTaskQueue()) {
    tscWarn("failed to cleanup task queue");
  }

  tmqMgmtClose();

  int32_t id = clientReqRefPool;
  clientReqRefPool = -1;
  taosCloseRef(id);

  id = clientConnRefPool;
  clientConnRefPool = -1;
  taosCloseRef(id);

  nodesDestroyAllocatorSet();
//  cleanupAppInfo();
  rpcCleanup();
  tscDebug("rpc cleanup");

  taosConvDestroy();
  DestroyRegexCache();

  tscInfo("all local resources released");
  taosCleanupCfg();
  taosCloseLog();
}

static setConfRet taos_set_config_imp(const char *config) {
  setConfRet ret = {SET_CONF_RET_SUCC, {0}};
  // TODO: need re-implementation
  return ret;
}

setConfRet taos_set_config(const char *config) {
  // TODO  pthread_mutex_lock(&setConfMutex);
  setConfRet ret = taos_set_config_imp(config);
  //  pthread_mutex_unlock(&setConfMutex);
  return ret;
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  tscDebug("try to connect to %s:%u, user:%s db:%s", ip, port, user, db);
  if (user == NULL) {
    user = TSDB_DEFAULT_USER;
  }

  if (pass == NULL) {
    pass = TSDB_DEFAULT_PASS;
  }

  STscObj *pObj = NULL;
  int32_t  code = taos_connect_internal(ip, user, pass, NULL, db, port, CONN_TYPE__QUERY, &pObj);
  if (TSDB_CODE_SUCCESS == code) {
    int64_t *rid = taosMemoryCalloc(1, sizeof(int64_t));
    if (NULL == rid) {
      tscError("out of memory when taos connect to %s:%u, user:%s db:%s", ip, port, user, db);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    *rid = pObj->id;
    return (TAOS *)rid;
  } else {
    terrno = code;
  }

  return NULL;
}

int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type) {
  if (taos == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("invalid parameter for %s", __func__);
    return terrno;
  }

  switch (type) {
    case TAOS_NOTIFY_PASSVER: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->passInfo.fp = fp;
      pObj->passInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    case TAOS_NOTIFY_WHITELIST_VER: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->whiteListInfo.fp = fp;
      pObj->whiteListInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    case TAOS_NOTIFY_USER_DROPPED: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->userDroppedInfo.fp = fp;
      pObj->userDroppedInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    default: {
      terrno = TSDB_CODE_INVALID_PARA;
      releaseTscObj(*(int64_t *)taos);
      return terrno;
    }
  }

  releaseTscObj(*(int64_t *)taos);
  return 0;
}

typedef struct SFetchWhiteListInfo {
  int64_t                     connId;
  __taos_async_whitelist_fn_t userCbFn;
  void                       *userParam;
} SFetchWhiteListInfo;

int32_t fetchWhiteListCallbackFn(void *param, SDataBuf *pMsg, int32_t code) {
  SFetchWhiteListInfo *pInfo = (SFetchWhiteListInfo *)param;
  TAOS                *taos = &pInfo->connId;
  if (code != TSDB_CODE_SUCCESS) {
    pInfo->userCbFn(pInfo->userParam, code, taos, 0, NULL);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pInfo);
    return code;
  }

  SGetUserWhiteListRsp wlRsp;
  if (TSDB_CODE_SUCCESS != tDeserializeSGetUserWhiteListRsp(pMsg->pData, pMsg->len, &wlRsp)) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pInfo);
    tFreeSGetUserWhiteListRsp(&wlRsp);
    return terrno;
  }

  uint64_t *pWhiteLists = taosMemoryMalloc(wlRsp.numWhiteLists * sizeof(uint64_t));
  if (pWhiteLists == NULL) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pInfo);
    tFreeSGetUserWhiteListRsp(&wlRsp);
    return terrno;
  }

  for (int i = 0; i < wlRsp.numWhiteLists; ++i) {
    pWhiteLists[i] = ((uint64_t)wlRsp.pWhiteLists[i].mask << 32) | wlRsp.pWhiteLists[i].ip;
  }

  pInfo->userCbFn(pInfo->userParam, code, taos, wlRsp.numWhiteLists, pWhiteLists);

  taosMemoryFree(pWhiteLists);
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pInfo);
  tFreeSGetUserWhiteListRsp(&wlRsp);
  return code;
}

void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param) {
  if (NULL == taos) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    return;
  }

  int64_t connId = *(int64_t *)taos;

  STscObj *pTsc = acquireTscObj(connId);
  if (NULL == pTsc) {
    fp(param, TSDB_CODE_TSC_DISCONNECTED, taos, 0, NULL);
    return;
  }

  SGetUserWhiteListReq req;
  (void)memcpy(req.user, pTsc->user, TSDB_USER_LEN);
  int32_t msgLen = tSerializeSGetUserWhiteListReq(NULL, 0, &req);
  if (msgLen < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  void *pReq = taosMemoryMalloc(msgLen);
  if (pReq == NULL) {
    fp(param, TSDB_CODE_OUT_OF_MEMORY, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  if (tSerializeSGetUserWhiteListReq(pReq, msgLen, &req) < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  SFetchWhiteListInfo *pParam = taosMemoryMalloc(sizeof(SFetchWhiteListInfo));
  if (pParam == NULL) {
    fp(param, TSDB_CODE_OUT_OF_MEMORY, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pParam->connId = connId;
  pParam->userCbFn = fp;
  pParam->userParam = param;
  SMsgSendInfo *pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) {
    fp(param, TSDB_CODE_OUT_OF_MEMORY, taos, 0, NULL);
    taosMemoryFree(pParam);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pSendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = msgLen, .handle = NULL};
  pSendInfo->requestId = generateRequestId();
  pSendInfo->requestObjRefId = 0;
  pSendInfo->param = pParam;
  pSendInfo->fp = fetchWhiteListCallbackFn;
  pSendInfo->msgType = TDMT_MND_GET_USER_WHITELIST;

  SEpSet epSet = getEpSet_s(&pTsc->pAppInfo->mgmtEp);
  if (TSDB_CODE_SUCCESS != asyncSendMsgToServer(pTsc->pAppInfo->pTransporter, &epSet, NULL, pSendInfo)) {
    tscWarn("failed to async send msg to server");
  }
  releaseTscObj(connId);
  return;
}

void taos_close_internal(void *taos) {
  if (taos == NULL) {
    return;
  }

  STscObj *pTscObj = (STscObj *)taos;
  tscDebug("0x%" PRIx64 " try to close connection, numOfReq:%d", pTscObj->id, pTscObj->numOfReqs);

  if (TSDB_CODE_SUCCESS != taosRemoveRef(clientConnRefPool, pTscObj->id)) {
    tscError("0x%" PRIx64 " failed to remove ref from conn pool", pTscObj->id);
  }
}

void taos_close(TAOS *taos) {
  if (taos == NULL) {
    return;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    taosMemoryFree(taos);
    return;
  }

  taos_close_internal(pObj);
  releaseTscObj(*(int64_t *)taos);
  taosMemoryFree(taos);
}

int taos_errno(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return terrno;
  }

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    return 0;
  }

  return ((SRequestObj *)res)->code;
}

const char *taos_errstr(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return (const char *)tstrerror(terrno);
  }

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    return "success";
  }

  SRequestObj *pRequest = (SRequestObj *)res;
  if (NULL != pRequest->msgBuf && (strlen(pRequest->msgBuf) > 0 || pRequest->code == TSDB_CODE_RPC_FQDN_ERROR)) {
    return pRequest->msgBuf;
  } else {
    return (const char *)tstrerror(pRequest->code);
  }
}

void taos_free_result(TAOS_RES *res) {
  if (NULL == res) {
    return;
  }

  tscDebug("taos free res %p", res);

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    tscDebug("0x%" PRIx64 " taos_free_result start to free query", pRequest->requestId);
    destroyRequest(pRequest);
    return;
  }
  SMqRspObj *pRsp = (SMqRspObj *)res;
  if (TD_RES_TMQ(res)) {
    tDeleteMqDataRsp(&pRsp->dataRsp);
    doFreeReqResultInfo(&pRsp->resInfo);
  } else if (TD_RES_TMQ_METADATA(res)) {
    tDeleteSTaosxRsp(&pRsp->dataRsp);
    doFreeReqResultInfo(&pRsp->resInfo);
  } else if (TD_RES_TMQ_META(res)) {
    tDeleteMqMetaRsp(&pRsp->metaRsp);
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    tDeleteMqBatchMetaRsp(&pRsp->batchMetaRsp);
  }
  taosMemoryFree(pRsp);

}

void taos_kill_query(TAOS *taos) {
  if (NULL == taos) {
    return;
  }

  int64_t  rid = *(int64_t *)taos;
  STscObj *pTscObj = acquireTscObj(rid);
  if (pTscObj) {
    stopAllRequests(pTscObj->pRequests);
  }
  releaseTscObj(rid);
}

int taos_field_count(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->numOfCols;
}

int taos_num_fields(TAOS_RES *res) { return taos_field_count(res); }

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  if (taos_num_fields(res) == 0 || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->userFields;
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) { return taosQueryImpl(taos, sql, false, TD_REQ_FROM_APP); }
TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqid) {
  return taosQueryImplWithReqid(taos, sql, false, reqid);
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  if (res == NULL) {
    return NULL;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
        pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0 || pRequest->killed) {
      return NULL;
    }

    if (pRequest->inCallback) {
      tscError("can not call taos_fetch_row before query callback ends.");
      terrno = TSDB_CODE_TSC_INVALID_OPERATION;
      return NULL;
    }

    return doAsyncFetchRows(pRequest, true, true);
  } else if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SMqRspObj      *msg = ((SMqRspObj *)res);
    SReqResultInfo *pResultInfo = NULL;
    if (msg->resIter == -1) {
      if (tmqGetNextResInfo(res, true, &pResultInfo) != 0) {
        return NULL;
      }
    } else {
      pResultInfo = tmqGetCurResInfo(res);
    }

    if (pResultInfo->current < pResultInfo->numOfRows) {
      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
      return pResultInfo->row;
    } else {
      if (tmqGetNextResInfo(res, true, &pResultInfo) != 0) {
        return NULL;
      }

      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
      return pResultInfo->row;
    }
  } else if (TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  } else {
    tscError("invalid result passed to taos_fetch_row");
    terrno = TSDB_CODE_TSC_INTERNAL_ERROR;
    return NULL;
  }
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  return taos_print_row_with_size(str, INT32_MAX, row, fields, num_fields);
}
int taos_print_row_with_size(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields){
  int32_t len = 0;
  for (int i = 0; i < num_fields; ++i) {
    if (i > 0 && len < size - 1) {
      str[len++] = ' ';
    }

    if (row[i] == NULL) {
      len += snprintf(str + len, size - len, "%s", TSDB_DATA_NULL_STR);
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += snprintf(str + len, size - len, "%d", *((int8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UTINYINT:
        len += snprintf(str + len, size - len, "%u", *((uint8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += snprintf(str + len, size - len, "%d", *((int16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_USMALLINT:
        len += snprintf(str + len, size - len, "%u", *((uint16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += snprintf(str + len, size - len, "%d", *((int32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UINT:
        len += snprintf(str + len, size - len, "%u", *((uint32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += snprintf(str + len, size - len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UBIGINT:
        len += snprintf(str + len, size - len, "%" PRIu64, *((uint64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += snprintf(str + len, size - len, "%f", fv);
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += snprintf(str + len, size - len, "%lf", dv);
      } break;

      case TSDB_DATA_TYPE_VARBINARY: {
        void    *data = NULL;
        uint32_t tmp = 0;
        int32_t  charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        if (taosAscii2Hex(row[i], charLen, &data, &tmp) < 0) {
          break;
        }
        uint32_t copyLen = TMIN(size - len - 1, tmp);
        (void)memcpy(str + len, data, copyLen);
        len += copyLen;
        taosMemoryFree(data);
      } break;
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_GEOMETRY: {
        int32_t charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        if (fields[i].type == TSDB_DATA_TYPE_BINARY || fields[i].type == TSDB_DATA_TYPE_VARBINARY ||
            fields[i].type == TSDB_DATA_TYPE_GEOMETRY) {
          if (charLen > fields[i].bytes || charLen < 0) {
            tscError("taos_print_row error binary. charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        } else {
          if (charLen > fields[i].bytes * TSDB_NCHAR_SIZE || charLen < 0) {
            tscError("taos_print_row error. charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        }

        uint32_t copyLen = TMIN(size - len - 1, charLen);
        (void)memcpy(str + len, row[i], copyLen);
        len += copyLen;
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += snprintf(str + len, size - len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += snprintf(str + len, size - len, "%d", *((int8_t *)row[i]));
      default:
        break;
    }

    if (len >= size - 1) {
      break;
    }
  }
  if (len < size){
    str[len] = 0;
  }

  return len;
}

int *taos_fetch_lengths(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->length;
}

TAOS_ROW *taos_result_block(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  if (taos_is_update_query(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return &pResInfo->row;
}

// todo intergrate with tDataTypes
const char *taos_data_type(int type) {
  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      return "TSDB_DATA_TYPE_NULL";
    case TSDB_DATA_TYPE_BOOL:
      return "TSDB_DATA_TYPE_BOOL";
    case TSDB_DATA_TYPE_TINYINT:
      return "TSDB_DATA_TYPE_TINYINT";
    case TSDB_DATA_TYPE_SMALLINT:
      return "TSDB_DATA_TYPE_SMALLINT";
    case TSDB_DATA_TYPE_INT:
      return "TSDB_DATA_TYPE_INT";
    case TSDB_DATA_TYPE_BIGINT:
      return "TSDB_DATA_TYPE_BIGINT";
    case TSDB_DATA_TYPE_FLOAT:
      return "TSDB_DATA_TYPE_FLOAT";
    case TSDB_DATA_TYPE_DOUBLE:
      return "TSDB_DATA_TYPE_DOUBLE";
    case TSDB_DATA_TYPE_VARCHAR:
      return "TSDB_DATA_TYPE_VARCHAR";
      //    case TSDB_DATA_TYPE_BINARY:          return "TSDB_DATA_TYPE_VARCHAR";
    case TSDB_DATA_TYPE_TIMESTAMP:
      return "TSDB_DATA_TYPE_TIMESTAMP";
    case TSDB_DATA_TYPE_NCHAR:
      return "TSDB_DATA_TYPE_NCHAR";
    case TSDB_DATA_TYPE_JSON:
      return "TSDB_DATA_TYPE_JSON";
    case TSDB_DATA_TYPE_GEOMETRY:
      return "TSDB_DATA_TYPE_GEOMETRY";
    case TSDB_DATA_TYPE_UTINYINT:
      return "TSDB_DATA_TYPE_UTINYINT";
    case TSDB_DATA_TYPE_USMALLINT:
      return "TSDB_DATA_TYPE_USMALLINT";
    case TSDB_DATA_TYPE_UINT:
      return "TSDB_DATA_TYPE_UINT";
    case TSDB_DATA_TYPE_UBIGINT:
      return "TSDB_DATA_TYPE_UBIGINT";
    case TSDB_DATA_TYPE_VARBINARY:
      return "TSDB_DATA_TYPE_VARBINARY";
    case TSDB_DATA_TYPE_DECIMAL:
      return "TSDB_DATA_TYPE_DECIMAL";
    case TSDB_DATA_TYPE_BLOB:
      return "TSDB_DATA_TYPE_BLOB";
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      return "TSDB_DATA_TYPE_MEDIUMBLOB";
    default:
      return "UNKNOWN";
  }
}

const char *taos_get_client_info() { return version; }

// return int32_t
int taos_affected_rows(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  SRequestObj    *pRequest = (SRequestObj *)res;
  SReqResultInfo *pResInfo = &pRequest->body.resInfo;
  return (int)pResInfo->numOfRows;
}

// return int64_t
int64_t taos_affected_rows64(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  SRequestObj    *pRequest = (SRequestObj *)res;
  SReqResultInfo *pResInfo = &pRequest->body.resInfo;
  return pResInfo->numOfRows;
}

int taos_result_precision(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return TSDB_TIME_PRECISION_MILLI;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    return pRequest->body.resInfo.precision;
  } else if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SReqResultInfo *info = tmqGetCurResInfo(res);
    return info->precision;
  }
  return TSDB_TIME_PRECISION_MILLI;
}

int taos_select_db(TAOS *taos, const char *db) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (pObj == NULL) {
    releaseTscObj(*(int64_t *)taos);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (db == NULL || strlen(db) == 0) {
    releaseTscObj(*(int64_t *)taos);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  char sql[256] = {0};
  (void)snprintf(sql, tListLen(sql), "use %s", db);

  TAOS_RES *pRequest = taos_query(taos, sql);
  int32_t   code = taos_errno(pRequest);

  taos_free_result(pRequest);
  releaseTscObj(*(int64_t *)taos);
  return code;
}

void taos_stop_query(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res)) {
    return;
  }

  stopAllQueries((SRequestObj *)res);
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return true;
  }
  SReqResultInfo *pResultInfo = tscGetCurResInfo(res);
  if (col >= pResultInfo->numOfCols || col < 0 || row >= pResultInfo->numOfRows || row < 0) {
    return true;
  }

  SResultColumn *pCol = &pResultInfo->pCol[col];
  if (IS_VAR_DATA_TYPE(pResultInfo->fields[col].type)) {
    return (pCol->offset[row] == -1);
  } else {
    return colDataIsNull_f(pCol->nullbitmap, row);
  }
}

bool taos_is_update_query(TAOS_RES *res) { return taos_num_fields(res) == 0; }

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  int32_t numOfRows = 0;
  /*int32_t code = */ terrno = taos_fetch_block_s(res, &numOfRows, rows);
  return numOfRows;
}

int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;

    (*rows) = NULL;
    (*numOfRows) = 0;

    if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
        pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
      return pRequest->code;
    }

    (void)doAsyncFetchRows(pRequest, false, true);

    // TODO refactor
    SReqResultInfo *pResultInfo = &pRequest->body.resInfo;
    pResultInfo->current = pResultInfo->numOfRows;

    (*rows) = pResultInfo->row;
    (*numOfRows) = pResultInfo->numOfRows;
    return pRequest->code;
  } else if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SReqResultInfo *pResultInfo = NULL;
    int32_t         code = tmqGetNextResInfo(res, true, &pResultInfo);
    if (code != 0) return code;

    pResultInfo->current = pResultInfo->numOfRows;
    (*rows) = pResultInfo->row;
    (*numOfRows) = pResultInfo->numOfRows;
    return 0;
  } else {
    tscError("taos_fetch_block_s invalid res type");
    return -1;
  }
}

int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData) {
  *numOfRows = 0;
  *pData = NULL;

  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SReqResultInfo *pResultInfo = NULL;
    int32_t         code = tmqGetNextResInfo(res, false, &pResultInfo);
    if (code != 0) {
      (*numOfRows) = 0;
      return 0;
    }

    pResultInfo->current = pResultInfo->numOfRows;
    (*numOfRows) = pResultInfo->numOfRows;
    (*pData) = (void *)pResultInfo->pData;
    return 0;
  }

  SRequestObj *pRequest = (SRequestObj *)res;

  if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
      pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
    return pRequest->code;
  }

  (void)doAsyncFetchRows(pRequest, false, false);

  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  pResultInfo->current = pResultInfo->numOfRows;
  (*numOfRows) = pResultInfo->numOfRows;
  (*pData) = (void *)pResultInfo->pData;

  return pRequest->code;
}

int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex) {
  if (res == NULL || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  int32_t numOfFields = taos_num_fields(res);
  if (columnIndex < 0 || columnIndex >= numOfFields || numOfFields == 0) {
    return 0;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  TAOS_FIELD     *pField = &pResInfo->userFields[columnIndex];
  if (!IS_VAR_DATA_TYPE(pField->type)) {
    return 0;
  }

  return pResInfo->pCol[columnIndex].offset;
}

int taos_is_null_by_column(TAOS_RES *res, int columnIndex, bool result[], int *rows) {
  if (res == NULL || result == NULL || rows == NULL || *rows <= 0 || columnIndex < 0 || TD_RES_TMQ_META(res) ||
      TD_RES_TMQ_BATCH_META(res)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t numOfFields = taos_num_fields(res);
  if (columnIndex >= numOfFields || numOfFields == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  TAOS_FIELD     *pField = &pResInfo->userFields[columnIndex];
  SResultColumn  *pCol = &pResInfo->pCol[columnIndex];

  if (*rows > pResInfo->numOfRows) {
    *rows = pResInfo->numOfRows;
  }
  if (IS_VAR_DATA_TYPE(pField->type)) {
    for (int i = 0; i < *rows; i++) {
      if (pCol->offset[i] == -1) {
        result[i] = true;
      } else {
        result[i] = false;
      }
    }
  } else {
    for (int i = 0; i < *rows; i++) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        result[i] = true;
      } else {
        result[i] = false;
      }
    }
  }
  return 0;
}

int taos_validate_sql(TAOS *taos, const char *sql) {
  TAOS_RES *pObj = taosQueryImpl(taos, sql, true, TD_REQ_FROM_APP);

  int code = taos_errno(pObj);

  taos_free_result(pObj);
  return code;
}

void taos_reset_current_db(TAOS *taos) {
  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (pTscObj == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return;
  }

  resetConnectDB(pTscObj);

  releaseTscObj(*(int64_t *)taos);
}

const char *taos_get_server_info(TAOS *taos) {
  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (pTscObj == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  releaseTscObj(*(int64_t *)taos);

  return pTscObj->sDetailVer;
}

int taos_get_current_db(TAOS *taos, char *database, int len, int *required) {
  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (pTscObj == NULL) {
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  int code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pTscObj->mutex);
  if (database == NULL || len <= 0) {
    if (required != NULL) *required = strlen(pTscObj->db) + 1;
    TSC_ERR_JRET(TSDB_CODE_INVALID_PARA);
  } else if (len < strlen(pTscObj->db) + 1) {
    tstrncpy(database, pTscObj->db, len);
    if (required) *required = strlen(pTscObj->db) + 1;
    TSC_ERR_JRET(TSDB_CODE_INVALID_PARA);
  } else {
    tstrncpy(database, pTscObj->db, len);
    code = 0;
  }
_return:
  (void)taosThreadMutexUnlock(&pTscObj->mutex);
  releaseTscObj(*(int64_t *)taos);
  return code;
}

void destorySqlCallbackWrapper(SSqlCallbackWrapper *pWrapper) {
  if (NULL == pWrapper) {
    return;
  }
  destoryCatalogReq(pWrapper->pCatalogReq);
  taosMemoryFree(pWrapper->pCatalogReq);
  qDestroyParseContext(pWrapper->pParseCtx);
  taosMemoryFree(pWrapper);
}

void destroyCtxInRequest(SRequestObj *pRequest) {
  schedulerFreeJob(&pRequest->body.queryJob, 0);
  qDestroyQuery(pRequest->pQuery);
  pRequest->pQuery = NULL;
  destorySqlCallbackWrapper(pRequest->pWrapper);
  pRequest->pWrapper = NULL;
}

static void doAsyncQueryFromAnalyse(SMetaData *pResultMeta, void *param, int32_t code) {
  SSqlCallbackWrapper *pWrapper = (SSqlCallbackWrapper *)param;
  SRequestObj         *pRequest = pWrapper->pRequest;
  SQuery              *pQuery = pRequest->pQuery;

  qDebug("0x%" PRIx64 " start to semantic analysis,QID:0x%" PRIx64, pRequest->self, pRequest->requestId);

  int64_t analyseStart = taosGetTimestampUs();
  pRequest->metric.ctgCostUs = analyseStart - pRequest->metric.ctgStart;
  pWrapper->pParseCtx->parseOnly = pRequest->parseOnly;

  if (TSDB_CODE_SUCCESS == code) {
    code = qAnalyseSqlSemantic(pWrapper->pParseCtx, pWrapper->pCatalogReq, pResultMeta, pQuery);
  }

  pRequest->metric.analyseCostUs += taosGetTimestampUs() - analyseStart;

  if (pRequest->parseOnly) {
    (void)memcpy(&pRequest->parseMeta, pResultMeta, sizeof(*pResultMeta));
    (void)memset(pResultMeta, 0, sizeof(*pResultMeta));
  }

  handleQueryAnslyseRes(pWrapper, pResultMeta, code);
}

int32_t cloneCatalogReq(SCatalogReq **ppTarget, SCatalogReq *pSrc) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SCatalogReq *pTarget = taosMemoryCalloc(1, sizeof(SCatalogReq));
  if (pTarget == NULL) {
    code = terrno;
  } else {
    pTarget->pDbVgroup = taosArrayDup(pSrc->pDbVgroup, NULL);
    pTarget->pDbCfg = taosArrayDup(pSrc->pDbCfg, NULL);
    pTarget->pDbInfo = taosArrayDup(pSrc->pDbInfo, NULL);
    pTarget->pTableMeta = taosArrayDup(pSrc->pTableMeta, NULL);
    pTarget->pTableHash = taosArrayDup(pSrc->pTableHash, NULL);
    pTarget->pUdf = taosArrayDup(pSrc->pUdf, NULL);
    pTarget->pIndex = taosArrayDup(pSrc->pIndex, NULL);
    pTarget->pUser = taosArrayDup(pSrc->pUser, NULL);
    pTarget->pTableIndex = taosArrayDup(pSrc->pTableIndex, NULL);
    pTarget->pTableCfg = taosArrayDup(pSrc->pTableCfg, NULL);
    pTarget->pTableTag = taosArrayDup(pSrc->pTableTag, NULL);
    pTarget->pView = taosArrayDup(pSrc->pView, NULL);
    pTarget->pTableTSMAs = taosArrayDup(pSrc->pTableTSMAs, NULL);
    pTarget->pTSMAs = taosArrayDup(pSrc->pTSMAs, NULL);
    pTarget->qNodeRequired = pSrc->qNodeRequired;
    pTarget->dNodeRequired = pSrc->dNodeRequired;
    pTarget->svrVerRequired = pSrc->svrVerRequired;
    pTarget->forceUpdate = pSrc->forceUpdate;
    pTarget->cloned = true;

    *ppTarget = pTarget;
  }

  return code;
}

void handleSubQueryFromAnalyse(SSqlCallbackWrapper *pWrapper, SMetaData *pResultMeta, SNode *pRoot) {
  SRequestObj         *pNewRequest = NULL;
  SSqlCallbackWrapper *pNewWrapper = NULL;
  int32_t              code = buildPreviousRequest(pWrapper->pRequest, pWrapper->pRequest->sqlstr, &pNewRequest);
  if (code) {
    handleQueryAnslyseRes(pWrapper, pResultMeta, code);
    return;
  }

  pNewRequest->pQuery = NULL;
  code = nodesMakeNode(QUERY_NODE_QUERY, (SNode **)&pNewRequest->pQuery);
  if (pNewRequest->pQuery) {
    pNewRequest->pQuery->pRoot = pRoot;
    pRoot = NULL;
    pNewRequest->pQuery->execStage = QUERY_EXEC_STAGE_ANALYSE;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = prepareAndParseSqlSyntax(&pNewWrapper, pNewRequest, false);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = cloneCatalogReq(&pNewWrapper->pCatalogReq, pWrapper->pCatalogReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    doAsyncQueryFromAnalyse(pResultMeta, pNewWrapper, code);
    nodesDestroyNode(pRoot);
  } else {
    handleQueryAnslyseRes(pWrapper, pResultMeta, code);
    return;
  }
}

void handleQueryAnslyseRes(SSqlCallbackWrapper *pWrapper, SMetaData *pResultMeta, int32_t code) {
  SRequestObj *pRequest = pWrapper->pRequest;
  SQuery      *pQuery = pRequest->pQuery;

  if (code == TSDB_CODE_SUCCESS && pQuery->pPrevRoot) {
    SNode *prevRoot = pQuery->pPrevRoot;
    pQuery->pPrevRoot = NULL;
    handleSubQueryFromAnalyse(pWrapper, pResultMeta, prevRoot);
    return;
  }

  if (code == TSDB_CODE_SUCCESS) {
    pRequest->stableQuery = pQuery->stableQuery;
    if (pQuery->pRoot) {
      pRequest->stmtType = pQuery->pRoot->type;
    }

    if (pQuery->haveResultSet) {
      code = setResSchemaInfo(&pRequest->body.resInfo, pQuery->pResSchema, pQuery->numOfResCols);
      setResPrecision(&pRequest->body.resInfo, pQuery->precision);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    TSWAP(pRequest->dbList, (pQuery)->pDbList);
    TSWAP(pRequest->tableList, (pQuery)->pTableList);
    TSWAP(pRequest->targetTableList, (pQuery)->pTargetTableList);

    launchAsyncQuery(pRequest, pQuery, pResultMeta, pWrapper);
  } else {
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    qDestroyQuery(pRequest->pQuery);
    pRequest->pQuery = NULL;

    if (NEED_CLIENT_HANDLE_ERROR(code)) {
      tscDebug("0x%" PRIx64 " client retry to handle the error, code:%d - %s, tryCount:%d,QID:0x%" PRIx64,
               pRequest->self, code, tstrerror(code), pRequest->retry, pRequest->requestId);
      restartAsyncQuery(pRequest, code);
      return;
    }

    // return to app directly
    tscError("0x%" PRIx64 " error occurs, code:%s, return to user app,QID:0x%" PRIx64, pRequest->self, tstrerror(code),
             pRequest->requestId);
    pRequest->code = code;
    returnToUser(pRequest);
  }
}

static int32_t getAllMetaAsync(SSqlCallbackWrapper *pWrapper, catalogCallback fp) {
  SRequestConnInfo conn = {.pTrans = pWrapper->pParseCtx->pTransporter,
                           .requestId = pWrapper->pParseCtx->requestId,
                           .requestObjRefId = pWrapper->pParseCtx->requestRid,
                           .mgmtEps = pWrapper->pParseCtx->mgmtEpSet};

  pWrapper->pRequest->metric.ctgStart = taosGetTimestampUs();

  return catalogAsyncGetAllMeta(pWrapper->pParseCtx->pCatalog, &conn, pWrapper->pCatalogReq, fp, pWrapper,
                                &pWrapper->pRequest->body.queryJob);
}

static void doAsyncQueryFromParse(SMetaData *pResultMeta, void *param, int32_t code);

static int32_t phaseAsyncQuery(SSqlCallbackWrapper *pWrapper) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (pWrapper->pRequest->pQuery->execStage) {
    case QUERY_EXEC_STAGE_PARSE: {
      // continue parse after get metadata
      code = getAllMetaAsync(pWrapper, doAsyncQueryFromParse);
      break;
    }
    case QUERY_EXEC_STAGE_ANALYSE: {
      // analysis after get metadata
      code = getAllMetaAsync(pWrapper, doAsyncQueryFromAnalyse);
      break;
    }
    case QUERY_EXEC_STAGE_SCHEDULE: {
      launchAsyncQuery(pWrapper->pRequest, pWrapper->pRequest->pQuery, NULL, pWrapper);
      break;
    }
    default:
      break;
  }
  return code;
}

static void doAsyncQueryFromParse(SMetaData *pResultMeta, void *param, int32_t code) {
  SSqlCallbackWrapper *pWrapper = (SSqlCallbackWrapper *)param;
  SRequestObj         *pRequest = pWrapper->pRequest;
  SQuery              *pQuery = pRequest->pQuery;

  pRequest->metric.ctgCostUs += taosGetTimestampUs() - pRequest->metric.ctgStart;
  qDebug("0x%" PRIx64 " start to continue parse,QID:0x%" PRIx64 ", code:%s", pRequest->self, pRequest->requestId,
         tstrerror(code));

  if (code == TSDB_CODE_SUCCESS) {
    // pWrapper->pCatalogReq->forceUpdate = false;
    code = qContinueParseSql(pWrapper->pParseCtx, pWrapper->pCatalogReq, pResultMeta, pQuery);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = phaseAsyncQuery(pWrapper);
  }

  if (TSDB_CODE_SUCCESS != code) {
    tscError("0x%" PRIx64 " error happens, code:%d - %s,QID:0x%" PRIx64, pWrapper->pRequest->self, code,
             tstrerror(code), pWrapper->pRequest->requestId);
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    terrno = code;
    pRequest->code = code;
    doRequestCallback(pRequest, code);
  }
}

void continueInsertFromCsv(SSqlCallbackWrapper *pWrapper, SRequestObj *pRequest) {
  int32_t code = qParseSqlSyntax(pWrapper->pParseCtx, &pRequest->pQuery, pWrapper->pCatalogReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = phaseAsyncQuery(pWrapper);
  }

  if (TSDB_CODE_SUCCESS != code) {
    tscError("0x%" PRIx64 " error happens, code:%d - %s,QID:0x%" PRIx64, pWrapper->pRequest->self, code,
             tstrerror(code), pWrapper->pRequest->requestId);
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    terrno = code;
    pRequest->code = code;
    doRequestCallback(pRequest, code);
  }
}

void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  int64_t connId = *(int64_t *)taos;
  tscDebug("taos_query_a start with sql:%s", sql);
  taosAsyncQueryImpl(connId, sql, fp, param, false, TD_REQ_FROM_APP);
  tscDebug("taos_query_a end with sql:%s", sql);
}

void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid) {
  int64_t connId = *(int64_t *)taos;
  taosAsyncQueryImplWithReqid(connId, sql, fp, param, false, reqid);
}

int32_t createParseContext(const SRequestObj *pRequest, SParseContext **pCxt, SSqlCallbackWrapper *pWrapper) {
  const STscObj *pTscObj = pRequest->pTscObj;

  *pCxt = taosMemoryCalloc(1, sizeof(SParseContext));
  if (*pCxt == NULL) {
    return terrno;
  }

  **pCxt = (SParseContext){.requestId = pRequest->requestId,
                           .requestRid = pRequest->self,
                           .acctId = pTscObj->acctId,
                           .db = pRequest->pDb,
                           .topicQuery = false,
                           .pSql = pRequest->sqlstr,
                           .sqlLen = pRequest->sqlLen,
                           .pMsg = pRequest->msgBuf,
                           .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                           .pTransporter = pTscObj->pAppInfo->pTransporter,
                           .pStmtCb = NULL,
                           .pUser = pTscObj->user,
                           .pEffectiveUser = pRequest->effectiveUser,
                           .isSuperUser = (0 == strcmp(pTscObj->user, TSDB_DEFAULT_USER)),
                           .enableSysInfo = pTscObj->sysInfo,
                           .async = true,
                           .svrVer = pTscObj->sVer,
                           .nodeOffline = (pTscObj->pAppInfo->onlineDnodes < pTscObj->pAppInfo->totalDnodes),
                           .allocatorId = pRequest->allocatorRefId,
                           .parseSqlFp = clientParseSql,
                           .parseSqlParam = pWrapper,
                           .setQueryFp = setQueryRequest};
  int8_t biMode = atomic_load_8(&((STscObj *)pTscObj)->biMode);
  (*pCxt)->biMode = biMode;
  return TSDB_CODE_SUCCESS;
}

int32_t prepareAndParseSqlSyntax(SSqlCallbackWrapper **ppWrapper, SRequestObj *pRequest, bool updateMetaForce) {
  int32_t              code = TSDB_CODE_SUCCESS;
  STscObj             *pTscObj = pRequest->pTscObj;
  SSqlCallbackWrapper *pWrapper = taosMemoryCalloc(1, sizeof(SSqlCallbackWrapper));
  if (pWrapper == NULL) {
    code = terrno;
  } else {
    pWrapper->pRequest = pRequest;
    pRequest->pWrapper = pWrapper;
    *ppWrapper = pWrapper;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createParseContext(pRequest, &pWrapper->pParseCtx, pWrapper);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pWrapper->pParseCtx->mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
    code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pWrapper->pParseCtx->pCatalog);
  }

  if (TSDB_CODE_SUCCESS == code && NULL == pRequest->pQuery) {
    int64_t syntaxStart = taosGetTimestampUs();

    pWrapper->pCatalogReq = taosMemoryCalloc(1, sizeof(SCatalogReq));
    if (pWrapper->pCatalogReq == NULL) {
      code = terrno;
    } else {
      pWrapper->pCatalogReq->forceUpdate = updateMetaForce;
      TSC_ERR_RET(qnodeRequired(pRequest, &pWrapper->pCatalogReq->qNodeRequired));
      code = qParseSqlSyntax(pWrapper->pParseCtx, &pRequest->pQuery, pWrapper->pCatalogReq);
    }

    pRequest->metric.parseCostUs += taosGetTimestampUs() - syntaxStart;
  }

  return code;
}

void doAsyncQuery(SRequestObj *pRequest, bool updateMetaForce) {
  SSqlCallbackWrapper *pWrapper = NULL;
  int32_t              code = TSDB_CODE_SUCCESS;

  if (pRequest->retry++ > REQUEST_TOTAL_EXEC_TIMES) {
    code = pRequest->prevCode;
    terrno = code;
    pRequest->code = code;
    tscDebug("call sync query cb with code: %s", tstrerror(code));
    doRequestCallback(pRequest, code);
    return;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = prepareAndParseSqlSyntax(&pWrapper, pRequest, updateMetaForce);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pRequest->stmtType = pRequest->pQuery->pRoot->type;
    code = phaseAsyncQuery(pWrapper);
  }

  if (TSDB_CODE_SUCCESS != code) {
    tscError("0x%" PRIx64 " error happens, code:%d - %s,QID:0x%" PRIx64, pRequest->self, code, tstrerror(code),
             pRequest->requestId);
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    qDestroyQuery(pRequest->pQuery);
    pRequest->pQuery = NULL;

    if (NEED_CLIENT_HANDLE_ERROR(code)) {
      tscDebug("0x%" PRIx64 " client retry to handle the error, code:%d - %s, tryCount:%d,QID:0x%" PRIx64,
               pRequest->self, code, tstrerror(code), pRequest->retry, pRequest->requestId);
      code = refreshMeta(pRequest->pTscObj, pRequest);
      if (code != 0) {
        tscWarn("0x%" PRIx64 " refresh meta failed, code:%d - %s,QID:0x%" PRIx64, pRequest->self, code, tstrerror(code),
                pRequest->requestId);
      }
      pRequest->prevCode = code;
      doAsyncQuery(pRequest, true);
      return;
    }

    terrno = code;
    pRequest->code = code;
    doRequestCallback(pRequest, code);
  }
}

void restartAsyncQuery(SRequestObj *pRequest, int32_t code) {
  tscInfo("restart request: %s p: %p", pRequest->sqlstr, pRequest);
  SRequestObj *pUserReq = pRequest;
  (void)acquireRequest(pRequest->self);
  while (pUserReq) {
    if (pUserReq->self == pUserReq->relation.userRefId || pUserReq->relation.userRefId == 0) {
      break;
    } else {
      int64_t nextRefId = pUserReq->relation.nextRefId;
      (void)releaseRequest(pUserReq->self);
      if (nextRefId) {
        pUserReq = acquireRequest(nextRefId);
      }
    }
  }
  bool hasSubRequest = pUserReq != pRequest || pRequest->relation.prevRefId != 0;
  if (pUserReq) {
    destroyCtxInRequest(pUserReq);
    pUserReq->prevCode = code;
    (void)memset(&pUserReq->relation, 0, sizeof(pUserReq->relation));
  } else {
    tscError("User req is missing");
    (void)removeFromMostPrevReq(pRequest);
    return;
  }
  if (hasSubRequest)
    (void)removeFromMostPrevReq(pRequest);
  else
    (void)releaseRequest(pUserReq->self);
  doAsyncQuery(pUserReq, true);
}

typedef struct SAsyncFetchParam {
  SRequestObj      *pReq;
  __taos_async_fn_t fp;
  void             *param;
} SAsyncFetchParam;

static int32_t doAsyncFetch(void *pParam) {
  SAsyncFetchParam *param = pParam;
  taosAsyncFetchImpl(param->pReq, param->fp, param->param);
  taosMemoryFree(param);
  return TSDB_CODE_SUCCESS;
}

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  if (res == NULL || fp == NULL) {
    tscError("taos_fetch_rows_a invalid paras");
    return;
  }
  if (!TD_RES_QUERY(res)) {
    tscError("taos_fetch_rows_a res is NULL");
    fp(param, res, TSDB_CODE_APP_ERROR);
    return;
  }

  SRequestObj *pRequest = res;
  if (TSDB_SQL_RETRIEVE_EMPTY_RESULT == pRequest->type) {
    fp(param, res, 0);
    return;
  }

  SAsyncFetchParam *pParam = taosMemoryCalloc(1, sizeof(SAsyncFetchParam));
  if (!pParam) {
    fp(param, res, terrno);
    return;
  }
  pParam->pReq = pRequest;
  pParam->fp = fp;
  pParam->param = param;
  int32_t code = taosAsyncExec(doAsyncFetch, pParam, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pParam);
    fp(param, res, code);
    return;
  }
}

void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  if (res == NULL || fp == NULL) {
    tscError("taos_fetch_raw_block_a invalid paras");
    return;
  }
  if (!TD_RES_QUERY(res)) {
    tscError("taos_fetch_raw_block_a res is NULL");
    return;
  }
  SRequestObj    *pRequest = res;
  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  // set the current block is all consumed
  pResultInfo->convertUcs4 = false;

  // it is a local executed query, no need to do async fetch
  taos_fetch_rows_a(pRequest, fp, param);
}

const void *taos_get_raw_block(TAOS_RES *res) {
  if (res == NULL) {
    tscError("taos_get_raw_block invalid paras");
    return NULL;
  }
  if (!TD_RES_QUERY(res)) {
    tscError("taos_get_raw_block res is NULL");
    return NULL;
  }
  SRequestObj *pRequest = res;

  return pRequest->body.resInfo.pData;
}

int taos_get_db_route_info(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  if (NULL == db || NULL == dbInfo) {
    tscError("invalid input param, db:%p, dbInfo:%p", db, dbInfo);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  int64_t      connId = *(int64_t *)taos;
  SRequestObj *pRequest = NULL;
  char        *sql = "taos_get_db_route_info";
  int32_t      code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return terrno;
  }

  STscObj  *pTscObj = pRequest->pTscObj;
  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)snprintf(dbFName, sizeof(dbFName), "%d.%s", pTscObj->acctId, db);

  code = catalogGetDBVgInfo(pCtg, &conn, dbFName, dbInfo);
  if (code) {
    goto _return;
  }

_return:

  terrno = code;

  destroyRequest(pRequest);
  return code;
}

int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  if (NULL == db || NULL == table || NULL == vgId) {
    tscError("invalid input param, db:%p, table:%p, vgId:%p", db, table, vgId);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  int64_t      connId = *(int64_t *)taos;
  SRequestObj *pRequest = NULL;
  char        *sql = "taos_get_table_vgId";
  int32_t      code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    return terrno;
  }

  pRequest->syncQuery = true;

  STscObj  *pTscObj = pRequest->pTscObj;
  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  SName tableName = {0};
  toName(pTscObj->acctId, db, table, &tableName);

  SVgroupInfo vgInfo;
  code = catalogGetTableHashVgroup(pCtg, &conn, &tableName, &vgInfo);
  if (code) {
    goto _return;
  }

  *vgId = vgInfo.vgId;

_return:

  terrno = code;

  destroyRequest(pRequest);
  return code;
}

int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  if (NULL == db || NULL == table || NULL == vgId || tableNum <= 0) {
    tscError("invalid input param, db:%p, table:%p, vgId:%p, tbNum:%d", db, table, vgId, tableNum);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  int64_t      connId = *(int64_t *)taos;
  SRequestObj *pRequest = NULL;
  char        *sql = "taos_get_table_vgId";
  int32_t      code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    return terrno;
  }

  pRequest->syncQuery = true;

  STscObj  *pTscObj = pRequest->pTscObj;
  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  code = catalogGetTablesHashVgId(pCtg, &conn, pTscObj->acctId, db, table, tableNum, vgId);
  if (code) {
    goto _return;
  }

_return:

  terrno = code;

  destroyRequest(pRequest);
  return code;
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  int64_t       connId = *(int64_t *)taos;
  const int32_t MAX_TABLE_NAME_LENGTH = 12 * 1024 * 1024;  // 12MB list
  int32_t       code = 0;
  SRequestObj  *pRequest = NULL;
  SCatalogReq   catalogReq = {0};

  if (NULL == tableNameList) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t length = (int32_t)strlen(tableNameList);
  if (0 == length) {
    return TSDB_CODE_SUCCESS;
  } else if (length > MAX_TABLE_NAME_LENGTH) {
    tscError("tableNameList too long, length:%d, maximum allowed:%d", length, MAX_TABLE_NAME_LENGTH);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  char *sql = "taos_load_table_info";
  code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto _return;
  }

  pRequest->syncQuery = true;

  STscObj *pTscObj = pRequest->pTscObj;
  code = transferTableNameList(tableNameList, pTscObj->acctId, pTscObj->db, &catalogReq.pTableMeta);
  if (code) {
    goto _return;
  }

  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  code = catalogAsyncGetAllMeta(pCtg, &conn, &catalogReq, syncCatalogFn, pRequest->body.interParam, NULL);
  if (code) {
    goto _return;
  }

  SSyncQueryParam *pParam = pRequest->body.interParam;
  code = tsem_wait(&pParam->sem);
  if (code) {
    tscError("tsem wait failed, code:%d - %s", code, tstrerror(code));
    goto _return;
  }
_return:
  destoryCatalogReq(&catalogReq);
  destroyRequest(pRequest);
  return code;
}

TAOS_STMT *taos_stmt_init(TAOS *taos) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT *pStmt = stmtInit(pObj, 0, NULL);
  if (NULL == pStmt) {
    tscError("stmt init failed, errcode:%s", terrstr());
  }
  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT *pStmt = stmtInit(pObj, reqid, NULL);
  if (NULL == pStmt) {
    tscError("stmt init failed, errcode:%s", terrstr());
  }
  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

TAOS_STMT *taos_stmt_init_with_options(TAOS *taos, TAOS_STMT_OPTIONS *options) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT *pStmt = stmtInit(pObj, options->reqId, options);
  if (NULL == pStmt) {
    tscError("stmt init failed, errcode:%s", terrstr());
  }
  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  if (stmt == NULL || sql == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtPrepare(stmt, sql, length);
}

int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags) {
  if (stmt == NULL || name == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t code = stmtSetTbName(stmt, name);
  if (code) {
    return code;
  }

  if (tags) {
    return stmtSetTbTags(stmt, tags);
  }

  return TSDB_CODE_SUCCESS;
}

int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name) {
  if (stmt == NULL || name == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtSetTbName(stmt, name);
}

int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) {
  if (stmt == NULL || tags == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtSetTbTags(stmt, tags);
}

int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name) { return taos_stmt_set_tbname(stmt, name); }

int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  if (stmt == NULL || NULL == fieldNum) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetTagFields(stmt, fieldNum, fields);
}

int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  if (stmt == NULL || NULL == fieldNum) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetColFields(stmt, fieldNum, fields);
}

// let stmt to reclaim TAOS_FIELD_E that was allocated by `taos_stmt_get_tag_fields`/`taos_stmt_get_col_fields`
void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields) {
  (void)stmt;
  if (!fields) return;
  taosMemoryFree(fields);
}

int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  if (stmt == NULL || bind == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (bind->num > 1) {
    tscError("invalid bind number %d for %s", bind->num, __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtBindBatch(stmt, bind, -1);
}

int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  if (stmt == NULL || bind == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (bind->num <= 0 || bind->num > INT16_MAX) {
    tscError("invalid bind num %d", bind->num);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t insert = 0;
  int32_t code = stmtIsInsert(stmt, &insert);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("stmt insert failed, errcode:%s", tstrerror(code));
    return code;
  }
  if (0 == insert && bind->num > 1) {
    tscError("only one row data allowed for query");
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtBindBatch(stmt, bind, -1);
}

int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx) {
  if (stmt == NULL || bind == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (colIdx < 0) {
    tscError("invalid bind column idx %d", colIdx);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t insert = 0;
  int32_t code = stmtIsInsert(stmt, &insert);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("stmt insert failed, errcode:%s", tstrerror(code));
    return code;
  }
  if (0 == insert && bind->num > 1) {
    tscError("only one row data allowed for query");
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtBindBatch(stmt, bind, colIdx);
}

int taos_stmt_add_batch(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtAddBatch(stmt);
}

int taos_stmt_execute(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtExec(stmt);
}

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  if (stmt == NULL || insert == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtIsInsert(stmt, insert);
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  if (stmt == NULL || nums == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetParamNum(stmt, nums);
}

int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  if (stmt == NULL || type == NULL || NULL == bytes || idx < 0) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetParam(stmt, idx, type, bytes);
}

TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  return stmtUseResult(stmt);
}

char *taos_stmt_errstr(TAOS_STMT *stmt) { return (char *)stmtErrstr(stmt); }

int taos_stmt_affected_rows(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }

  return stmtAffectedRows(stmt);
}

int taos_stmt_affected_rows_once(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }

  return stmtAffectedRowsOnce(stmt);
}

int taos_stmt_close(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtClose(stmt);
}

TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT2 *pStmt = stmtInit2(pObj, option);

  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

int taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length) {
  if (stmt == NULL || sql == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtPrepare2(stmt, sql, length);
}

int taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscStmt2 *pStmt = (STscStmt2 *)stmt;
  if (pStmt->options.asyncExecFn && !pStmt->semWaited) {
    if (tsem_wait(&pStmt->asyncQuerySem) != 0) {
      tscError("wait async query sem failed");
    }
    pStmt->semWaited = true;
  }

  int32_t code = 0;
  for (int i = 0; i < bindv->count; ++i) {
    if (bindv->tbnames && bindv->tbnames[i]) {
      code = stmtSetTbName2(stmt, bindv->tbnames[i]);
      if (code) {
        return code;
      }
    }

    if (bindv->tags && bindv->tags[i]) {
      code = stmtSetTbTags2(stmt, bindv->tags[i]);
      if (code) {
        return code;
      }
    }

    if (bindv->bind_cols && bindv->bind_cols[i]) {
      TAOS_STMT2_BIND *bind = bindv->bind_cols[i];

      if (bind->num <= 0 || bind->num > INT16_MAX) {
        tscError("invalid bind num %d", bind->num);
        terrno = TSDB_CODE_INVALID_PARA;
        return terrno;
      }

      int32_t insert = 0;
      (void)stmtIsInsert2(stmt, &insert);
      if (0 == insert && bind->num > 1) {
        tscError("only one row data allowed for query");
        terrno = TSDB_CODE_INVALID_PARA;
        return terrno;
      }

      code = stmtBindBatch2(stmt, bind, col_idx);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtExec2(stmt, affected_rows);
}

int taos_stmt2_close(TAOS_STMT2 *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtClose2(stmt);
}
/*
int taos_stmt2_param_count(TAOS_STMT2 *stmt, int *nums) {
  if (stmt == NULL || nums == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  return stmtGetParamNum2(stmt, nums);
}
*/
int taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert) {
  if (stmt == NULL || insert == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtIsInsert2(stmt, insert);
}

int taos_stmt2_get_fields(TAOS_STMT2 *stmt, TAOS_FIELD_T field_type, int *count, TAOS_FIELD_E **fields) {
  if (stmt == NULL || NULL == count) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (field_type == TAOS_FIELD_COL) {
    return stmtGetColFields2(stmt, count, fields);
  } else if (field_type == TAOS_FIELD_TAG) {
    return stmtGetTagFields2(stmt, count, fields);
  } else if (field_type == TAOS_FIELD_QUERY) {
    return stmtGetParamNum2(stmt, count);
  } else if (field_type == TAOS_FIELD_TBNAME) {
    return stmtGetParamTbName(stmt, count);
  } else {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
}

void taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_E *fields) {
  (void)stmt;
  if (!fields) return;
  taosMemoryFree(fields);
}

TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  return stmtUseResult2(stmt);
}

char *taos_stmt2_error(TAOS_STMT2 *stmt) { return (char *)stmtErrstr2(stmt); }

int taos_set_conn_mode(TAOS *taos, int mode, int value) {
  if (taos == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("invalid parameter for %s", __func__);
    return terrno;
  }
  switch (mode) {
    case TAOS_CONN_MODE_BI:
      atomic_store_8(&pObj->biMode, value);
      break;
    default:
      tscError("not supported mode.");
      return TSDB_CODE_INVALID_PARA;
  }
  return 0;
}

char *getBuildInfo() { return buildinfo; }
