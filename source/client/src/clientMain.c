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
#include "clientStmt.h"
#include "functionMgt.h"
#include "os.h"
#include "query.h"
#include "scheduler.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "version.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

static int32_t sentinel = TSC_VAR_NOT_RELEASE;
static int32_t createParseContext(const SRequestObj *pRequest, SParseContext **pCxt);

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  static int32_t lock = 0;

  for (int i = 1; atomic_val_compare_exchange_32(&lock, 0, 1) != 0; ++i) {
    if (i % 1000 == 0) {
      tscInfo("haven't acquire lock after spin %d times.", i);
      sched_yield();
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

  int32_t id = clientReqRefPool;
  clientReqRefPool = -1;
  taosCloseRef(id);

  hbMgrCleanUp();

  catalogDestroy();
  schedulerDestroy();

  fmFuncMgtDestroy();
  qCleanupKeywordsTable();

  id = clientConnRefPool;
  clientConnRefPool = -1;
  taosCloseRef(id);

  rpcCleanup();
  tscDebug("rpc cleanup");

  cleanupTaskQueue();

  taosConvDestroy();

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

  STscObj *pObj = taos_connect_internal(ip, user, pass, NULL, db, port, CONN_TYPE__QUERY);
  if (pObj) {
    int64_t *rid = taosMemoryCalloc(1, sizeof(int64_t));
    *rid = pObj->id;
    return (TAOS *)rid;
  }

  return NULL;
}

void taos_close_internal(void *taos) {
  if (taos == NULL) {
    return;
  }

  STscObj *pTscObj = (STscObj *)taos;
  tscDebug("0x%" PRIx64 " try to close connection, numOfReq:%d", pTscObj->id, pTscObj->numOfReqs);

  taosRemoveRef(clientConnRefPool, pTscObj->id);
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
  if (res == NULL || TD_RES_TMQ_META(res)) {
    if (terrno == TSDB_CODE_RPC_REDIRECT) terrno = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    return terrno;
  }

  if (TD_RES_TMQ(res)) {
    return 0;
  }

  return ((SRequestObj *)res)->code == TSDB_CODE_RPC_REDIRECT ? TSDB_CODE_RPC_NETWORK_UNAVAIL
                                                              : ((SRequestObj *)res)->code;
}

const char *taos_errstr(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
    if (terrno == TSDB_CODE_RPC_REDIRECT) terrno = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    return (const char *)tstrerror(terrno);
  }

  if (TD_RES_TMQ(res)) {
    return "success";
  }

  SRequestObj *pRequest = (SRequestObj *)res;
  if (NULL != pRequest->msgBuf && (strlen(pRequest->msgBuf) > 0 || pRequest->code == TSDB_CODE_RPC_FQDN_ERROR)) {
    return pRequest->msgBuf;
  } else {
    return pRequest->code == TSDB_CODE_RPC_REDIRECT ? (const char *)tstrerror(TSDB_CODE_RPC_NETWORK_UNAVAIL)
                                                    : (const char *)tstrerror(pRequest->code);
  }
}

void taos_free_result(TAOS_RES *res) {
  if (NULL == res) {
    return;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    tscDebug("0x%" PRIx64 " taos_free_result start to free query", pRequest->requestId);
    destroyRequest(pRequest);
  } else if (TD_RES_TMQ(res)) {
    SMqRspObj *pRsp = (SMqRspObj *)res;
    if (pRsp->rsp.blockData) taosArrayDestroyP(pRsp->rsp.blockData, taosMemoryFree);
    if (pRsp->rsp.blockDataLen) taosArrayDestroy(pRsp->rsp.blockDataLen);
    if (pRsp->rsp.withTbName) taosArrayDestroyP(pRsp->rsp.blockTbName, taosMemoryFree);
    if (pRsp->rsp.withSchema) taosArrayDestroyP(pRsp->rsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
    pRsp->resInfo.pRspMsg = NULL;
    doFreeReqResultInfo(&pRsp->resInfo);
  } else if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj *pRspObj = (SMqMetaRspObj *)res;
    taosMemoryFree(pRspObj->metaRsp.metaRsp);
    taosMemoryFree(pRspObj);
  }
}

void taos_kill_query(TAOS *taos) {
  if (NULL == taos) {
    return;
  }

  int64_t  rid = *(int64_t *)taos;
  STscObj *pTscObj = acquireTscObj(rid);
  stopAllRequests(pTscObj->pRequests);
  releaseTscObj(rid);
}

int taos_field_count(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
    return 0;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->numOfCols;
}

int taos_num_fields(TAOS_RES *res) { return taos_field_count(res); }

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  if (taos_num_fields(res) == 0 || TD_RES_TMQ_META(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->userFields;
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) { return taosQueryImpl(taos, sql, false); }

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

#if SYNC_ON_TOP_OF_ASYNC
    return doAsyncFetchRows(pRequest, true, true);
#else
    return doFetchRows(pRequest, true, true);
#endif

  } else if (TD_RES_TMQ(res)) {
    SMqRspObj      *msg = ((SMqRspObj *)res);
    SReqResultInfo *pResultInfo;
    if (msg->resIter == -1) {
      pResultInfo = tmqGetNextResInfo(res, true);
    } else {
      pResultInfo = tmqGetCurResInfo(res);
    }

    if (pResultInfo->current < pResultInfo->numOfRows) {
      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
      return pResultInfo->row;
    } else {
      pResultInfo = tmqGetNextResInfo(res, true);
      if (pResultInfo == NULL) return NULL;
      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
      return pResultInfo->row;
    }
  } else if (TD_RES_TMQ_META(res)) {
    return NULL;
  } else {
    // assert to avoid un-initialization error
    ASSERT(0);
  }
  return NULL;
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  int32_t len = 0;
  for (int i = 0; i < num_fields; ++i) {
    if (i > 0) {
      str[len++] = ' ';
    }

    if (row[i] == NULL) {
      len += sprintf(str + len, "%s", TSDB_DATA_NULL_STR);
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UTINYINT:
        len += sprintf(str + len, "%u", *((uint8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d", *((int16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_USMALLINT:
        len += sprintf(str + len, "%u", *((uint16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d", *((int32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UINT:
        len += sprintf(str + len, "%u", *((uint32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UBIGINT:
        len += sprintf(str + len, "%" PRIu64, *((uint64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += sprintf(str + len, "%f", fv);
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += sprintf(str + len, "%lf", dv);
      } break;

      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        if (fields[i].type == TSDB_DATA_TYPE_BINARY) {
          assert(charLen <= fields[i].bytes && charLen >= 0);
        } else {
          assert(charLen <= fields[i].bytes * TSDB_NCHAR_SIZE && charLen >= 0);
        }

        memcpy(str + len, row[i], charLen);
        len += charLen;
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
      default:
        break;
    }
  }
  str[len] = 0;

  return len;
}

int *taos_fetch_lengths(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->length;
}

TAOS_ROW *taos_result_block(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
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
    default:
      return "UNKNOWN";
  }
}

const char *taos_get_client_info() { return version; }

int taos_affected_rows(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ(res) || TD_RES_TMQ_META(res)) {
    return 0;
  }

  SRequestObj    *pRequest = (SRequestObj *)res;
  SReqResultInfo *pResInfo = &pRequest->body.resInfo;
  return pResInfo->numOfRows;
}

int taos_result_precision(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
    return TSDB_TIME_PRECISION_MILLI;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    return pRequest->body.resInfo.precision;
  } else if (TD_RES_TMQ(res)) {
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
  snprintf(sql, tListLen(sql), "use %s", db);

  TAOS_RES *pRequest = taos_query(taos, sql);
  int32_t   code = taos_errno(pRequest);

  taos_free_result(pRequest);
  releaseTscObj(*(int64_t *)taos);
  return code;
}

void taos_stop_query(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ(res) || TD_RES_TMQ_META(res)) {
    return;
  }

  SRequestObj *pRequest = (SRequestObj *)res;
  pRequest->killed = true;

  int32_t numOfFields = taos_num_fields(pRequest);
  // It is not a query, no need to stop.
  if (numOfFields == 0) {
    tscDebug("request 0x%" PRIx64 " no need to be killed since not query", pRequest->requestId);
    return;
  }

  schedulerFreeJob(&pRequest->body.queryJob, TSDB_CODE_TSC_QUERY_KILLED);
  tscDebug("request %" PRIx64 " killed", pRequest->requestId);
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
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
  /*int32_t code = */ taos_fetch_block_s(res, &numOfRows, rows);
  return numOfRows;
}

int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
    return 0;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;

    (*rows) = NULL;
    (*numOfRows) = 0;

    if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
        pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
      return 0;
    }

#if SYNC_ON_TOP_OF_ASYNC
    doAsyncFetchRows(pRequest, false, true);
#else
    doFetchRows(pRequest, true, true);
#endif

    // TODO refactor
    SReqResultInfo *pResultInfo = &pRequest->body.resInfo;
    pResultInfo->current = pResultInfo->numOfRows;

    (*rows) = pResultInfo->row;
    (*numOfRows) = pResultInfo->numOfRows;
    return pRequest->code;
  } else if (TD_RES_TMQ(res)) {
    SReqResultInfo *pResultInfo = tmqGetNextResInfo(res, true);
    if (pResultInfo == NULL) return -1;

    pResultInfo->current = pResultInfo->numOfRows;
    (*rows) = pResultInfo->row;
    (*numOfRows) = pResultInfo->numOfRows;
    return 0;
  } else {
    ASSERT(0);
    return -1;
  }
}

int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
    return 0;
  }

  if (TD_RES_TMQ(res)) {
    SReqResultInfo *pResultInfo = tmqGetNextResInfo(res, false);
    if (pResultInfo == NULL) {
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
    return 0;
  }

#if SYNC_ON_TOP_OF_ASYNC
  doAsyncFetchRows(pRequest, false, false);
#else
  doFetchRows(pRequest, false, false);
#endif

  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  pResultInfo->current = pResultInfo->numOfRows;
  (*numOfRows) = pResultInfo->numOfRows;
  (*pData) = (void *)pResultInfo->pData;

  return 0;
}

int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex) {
  if (res == NULL || TD_RES_TMQ_META(res)) {
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

int taos_validate_sql(TAOS *taos, const char *sql) {
  TAOS_RES *pObj = taosQueryImpl(taos, sql, true);

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

typedef struct SqlParseWrapper {
  SParseContext *pCtx;
  SCatalogReq    catalogReq;
  SRequestObj   *pRequest;
  SQuery        *pQuery;
} SqlParseWrapper;

static void destoryTablesReq(void *p) {
  STablesReq *pRes = (STablesReq *)p;
  taosArrayDestroy(pRes->pTables);
}

static void destorySqlParseWrapper(SqlParseWrapper *pWrapper) {
  taosArrayDestroy(pWrapper->catalogReq.pDbVgroup);
  taosArrayDestroy(pWrapper->catalogReq.pDbCfg);
  taosArrayDestroy(pWrapper->catalogReq.pDbInfo);
  taosArrayDestroyEx(pWrapper->catalogReq.pTableMeta, destoryTablesReq);
  taosArrayDestroyEx(pWrapper->catalogReq.pTableHash, destoryTablesReq);
  taosArrayDestroy(pWrapper->catalogReq.pUdf);
  taosArrayDestroy(pWrapper->catalogReq.pIndex);
  taosArrayDestroy(pWrapper->catalogReq.pUser);
  taosArrayDestroy(pWrapper->catalogReq.pTableIndex);
  taosMemoryFree(pWrapper->pCtx);
  taosMemoryFree(pWrapper);
}

void retrieveMetaCallback(SMetaData *pResultMeta, void *param, int32_t code) {
  SqlParseWrapper *pWrapper = (SqlParseWrapper *)param;
  SQuery          *pQuery = pWrapper->pQuery;
  SRequestObj     *pRequest = pWrapper->pRequest;

  if (code == TSDB_CODE_SUCCESS) {
    code = qAnalyseSqlSemantic(pWrapper->pCtx, &pWrapper->catalogReq, pResultMeta, pQuery);
    pRequest->stableQuery = pQuery->stableQuery;
    if (pQuery->pRoot) {
      pRequest->stmtType = pQuery->pRoot->type;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    if (pQuery->haveResultSet) {
      setResSchemaInfo(&pRequest->body.resInfo, pQuery->pResSchema, pQuery->numOfResCols);
      setResPrecision(&pRequest->body.resInfo, pQuery->precision);
    }

    TSWAP(pRequest->dbList, (pQuery)->pDbList);
    TSWAP(pRequest->tableList, (pQuery)->pTableList);
    TSWAP(pRequest->targetTableList, (pQuery)->pTargetTableList);

    destorySqlParseWrapper(pWrapper);

    tscDebug("0x%" PRIx64 " analysis semantics completed, start async query, reqId:0x%" PRIx64, pRequest->self,
             pRequest->requestId);
    launchAsyncQuery(pRequest, pQuery, pResultMeta);
    qDestroyQuery(pQuery);
  } else {
    destorySqlParseWrapper(pWrapper);
    qDestroyQuery(pQuery);
    if (NEED_CLIENT_HANDLE_ERROR(code)) {
      tscDebug("0x%" PRIx64 " client retry to handle the error, code:%d - %s, tryCount:%d, reqId:0x%" PRIx64,
               pRequest->self, code, tstrerror(code), pRequest->retry, pRequest->requestId);
      pRequest->prevCode = code;
      doAsyncQuery(pRequest, true);
      return;
    }

    // return to app directly
    tscError("0x%" PRIx64 " error occurs, code:%s, return to user app, reqId:0x%" PRIx64, pRequest->self,
             tstrerror(code), pRequest->requestId);
    pRequest->code = code;
    pRequest->body.queryFp(pRequest->body.param, pRequest, code);
  }
}

void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  int64_t connId = *(int64_t *)taos;
  taosAsyncQueryImpl(connId, sql, fp, param, false);
}

int32_t createParseContext(const SRequestObj *pRequest, SParseContext **pCxt) {
  const STscObj *pTscObj = pRequest->pTscObj;

  *pCxt = taosMemoryCalloc(1, sizeof(SParseContext));
  if (*pCxt == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
                           .schemalessType = pTscObj->schemalessType,
                           .isSuperUser = (0 == strcmp(pTscObj->user, TSDB_DEFAULT_USER)),
                           .async = true,
                           .svrVer = pTscObj->sVer,
                           .nodeOffline = (pTscObj->pAppInfo->onlineDnodes < pTscObj->pAppInfo->totalDnodes)};
  return TSDB_CODE_SUCCESS;
}

void doAsyncQuery(SRequestObj *pRequest, bool updateMetaForce) {
  SParseContext *pCxt = NULL;
  STscObj       *pTscObj = pRequest->pTscObj;
  int32_t        code = 0;

  if (pRequest->retry++ > REQUEST_TOTAL_EXEC_TIMES) {
    code = pRequest->prevCode;
    goto _error;
  }

  code = createParseContext(pRequest, &pCxt);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pCxt->mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCxt->pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SQuery *pQuery = NULL;

  SCatalogReq catalogReq = {.forceUpdate = updateMetaForce, .qNodeRequired = qnodeRequired(pRequest)};
  code = qParseSqlSyntax(pCxt, &pQuery, &catalogReq);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (!updateMetaForce) {
    STscObj            *pTscObj = pRequest->pTscObj;
    SAppClusterSummary *pActivity = &pTscObj->pAppInfo->summary;
    if (NULL == pQuery->pRoot) {
      atomic_add_fetch_64((int64_t *)&pActivity->numOfInsertsReq, 1);
    } else if (QUERY_NODE_SELECT_STMT == pQuery->pRoot->type) {
      atomic_add_fetch_64((int64_t *)&pActivity->numOfQueryReq, 1);
    }
  }

  SqlParseWrapper *pWrapper = taosMemoryCalloc(1, sizeof(SqlParseWrapper));
  if (pWrapper == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pWrapper->pCtx = pCxt;
  pWrapper->pQuery = pQuery;
  pWrapper->pRequest = pRequest;
  pWrapper->catalogReq = catalogReq;

  SRequestConnInfo conn = {.pTrans = pCxt->pTransporter,
                           .requestId = pCxt->requestId,
                           .requestObjRefId = pCxt->requestRid,
                           .mgmtEps = pCxt->mgmtEpSet};

  code = catalogAsyncGetAllMeta(pCxt->pCatalog, &conn, &catalogReq, retrieveMetaCallback, pWrapper,
                                &pRequest->body.queryJob);
  pCxt = NULL;
  if (code == TSDB_CODE_SUCCESS) {
    return;
  }

_error:
  tscError("0x%" PRIx64 " error happens, code:%d - %s, reqId:0x%" PRIx64, pRequest->self, code, tstrerror(code),
           pRequest->requestId);
  taosMemoryFree(pCxt);

  terrno = code;
  pRequest->code = code;
  pRequest->body.queryFp(pRequest->body.param, pRequest, code);
}

static void fetchCallback(void *pResult, void *param, int32_t code) {
  SRequestObj *pRequest = (SRequestObj *)param;

  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  tscDebug("0x%" PRIx64 " enter scheduler fetch cb, code:%d - %s, reqId:0x%" PRIx64, pRequest->self, code,
           tstrerror(code), pRequest->requestId);

  pResultInfo->pData = pResult;
  pResultInfo->numOfRows = 0;

  if (code != TSDB_CODE_SUCCESS) {
    pRequest->code = code;
    pRequest->body.fetchFp(pRequest->body.param, pRequest, 0);
    return;
  }

  if (pRequest->code != TSDB_CODE_SUCCESS) {
    pRequest->body.fetchFp(pRequest->body.param, pRequest, 0);
    return;
  }

  pRequest->code =
      setQueryResultFromRsp(pResultInfo, (SRetrieveTableRsp *)pResultInfo->pData, pResultInfo->convertUcs4, true);
  if (pRequest->code != TSDB_CODE_SUCCESS) {
    pResultInfo->numOfRows = 0;
    pRequest->code = code;
    tscError("0x%" PRIx64 " fetch results failed, code:%s, reqId:0x%" PRIx64, pRequest->self, tstrerror(code),
             pRequest->requestId);
  } else {
    tscDebug("0x%" PRIx64 " fetch results, numOfRows:%d total Rows:%" PRId64 ", complete:%d, reqId:0x%" PRIx64,
             pRequest->self, pResultInfo->numOfRows, pResultInfo->totalRows, pResultInfo->completed,
             pRequest->requestId);

    STscObj            *pTscObj = pRequest->pTscObj;
    SAppClusterSummary *pActivity = &pTscObj->pAppInfo->summary;
    atomic_add_fetch_64((int64_t *)&pActivity->fetchBytes, pRequest->body.resInfo.payloadLen);
  }

  pRequest->body.fetchFp(pRequest->body.param, pRequest, pResultInfo->numOfRows);
}

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  ASSERT(res != NULL && fp != NULL);
  ASSERT(TD_RES_QUERY(res));

  SRequestObj *pRequest = res;
  pRequest->body.fetchFp = fp;
  pRequest->body.param = param;

  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  // this query has no results or error exists, return directly
  if (taos_num_fields(pRequest) == 0 || pRequest->code != TSDB_CODE_SUCCESS) {
    pResultInfo->numOfRows = 0;
    pRequest->body.fetchFp(param, pRequest, pResultInfo->numOfRows);
    return;
  }

  // all data has returned to App already, no need to try again
  if (pResultInfo->completed) {
    // it is a local executed query, no need to do async fetch
    if (QUERY_EXEC_MODE_LOCAL == pRequest->body.execMode) {
      ASSERT(pResultInfo->numOfRows >= 0);
      if (pResultInfo->localResultFetched) {
        pResultInfo->numOfRows = 0;
        pResultInfo->current = 0;
      } else {
        pResultInfo->localResultFetched = true;
      }
    } else {
      pResultInfo->numOfRows = 0;
    }

    pRequest->body.fetchFp(param, pRequest, pResultInfo->numOfRows);
    return;
  }

  SSchedulerReq req = {
      .syncReq = false,
      .fetchFp = fetchCallback,
      .cbParam = pRequest,
  };

  schedulerFetchRows(pRequest->body.queryJob, &req);
}

void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  ASSERT(res != NULL && fp != NULL);
  ASSERT(TD_RES_QUERY(res));

  SRequestObj    *pRequest = res;
  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  // set the current block is all consumed
  pResultInfo->convertUcs4 = false;

  // it is a local executed query, no need to do async fetch
  taos_fetch_rows_a(pRequest, fp, param);
}

const void *taos_get_raw_block(TAOS_RES *res) {
  ASSERT(res != NULL);
  ASSERT(TD_RES_QUERY(res));
  SRequestObj *pRequest = res;

  return pRequest->body.resInfo.pData;
}

TAOS_SUB *taos_subscribe(TAOS *taos, int restart, const char *topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp,
                         void *param, int interval) {
  // TODO
  return NULL;
}

TAOS_RES *taos_consume(TAOS_SUB *tsub) {
  // TODO
  return NULL;
}

void taos_unsubscribe(TAOS_SUB *tsub, int keepProgress) {
  // TODO
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
  code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto _return;
  }

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

  code = catalogAsyncGetAllMeta(pCtg, &conn, &catalogReq, syncCatalogFn, pRequest->body.param, NULL);
  if (code) {
    goto _return;
  }

  SSyncQueryParam *pParam = pRequest->body.param;
  tsem_wait(&pParam->sem);

_return:
  taosArrayDestroy(catalogReq.pTableMeta);
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

  TAOS_STMT *pStmt = stmtInit(pObj);

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
  stmtIsInsert(stmt, &insert);
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
  stmtIsInsert(stmt, &insert);
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
