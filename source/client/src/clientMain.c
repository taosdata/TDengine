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
#include "os.h"
#include "query.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "version.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

static int32_t sentinel = TSC_VAR_NOT_RELEASE;

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
  tscInfo("start to cleanup client environment");

  if (atomic_val_compare_exchange_32(&sentinel, TSC_VAR_NOT_RELEASE, TSC_VAR_RELEASED) != TSC_VAR_NOT_RELEASE) {
    return;
  }

  int32_t id = clientReqRefPool;
  clientReqRefPool = -1;
  taosCloseRef(id);

  cleanupTaskQueue();

  id = clientConnRefPool;
  clientConnRefPool = -1;
  taosCloseRef(id);

  hbMgrCleanUp();

  rpcCleanup();
  catalogDestroy();
  taosCloseLog();

  tscInfo("all local resources released");
}

setConfRet taos_set_config(const char *config) {
  // TODO
  setConfRet ret = {SET_CONF_RET_SUCC, {0}};
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

  return taos_connect_internal(ip, user, pass, NULL, db, port);
}

void taos_close(TAOS *taos) {
  if (taos == NULL) {
    return;
  }

  STscObj *pTscObj = (STscObj *)taos;
  tscDebug("0x%" PRIx64 " try to close connection, numOfReq:%d", pTscObj->id, pTscObj->numOfReqs);

  /*taosRemoveRef(clientConnRefPool, pTscObj->id);*/
}

int taos_errno(TAOS_RES *tres) {
  if (tres == NULL) {
    return terrno;
  }

  return ((SRequestObj *)tres)->code;
}

const char *taos_errstr(TAOS_RES *res) {
  SRequestObj *pRequest = (SRequestObj *)res;

  if (pRequest == NULL) {
    return (const char *)tstrerror(terrno);
  }

  if (strlen(pRequest->msgBuf) > 0 || pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) {
    return pRequest->msgBuf;
  } else {
    return (const char *)tstrerror(pRequest->code);
  }
}

void taos_free_result(TAOS_RES *res) {
  SRequestObj *pRequest = (SRequestObj *)res;
  destroyRequest(pRequest);
}

int taos_field_count(TAOS_RES *res) {
  if (res == NULL) {
    return 0;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->numOfCols;
}

int taos_num_fields(TAOS_RES *res) { return taos_field_count(res); }

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  if (taos_num_fields(res) == 0) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->userFields;
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  if (taos == NULL || sql == NULL) {
    return NULL;
  }

  return taos_query_l(taos, sql, (int32_t)strlen(sql));
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  if (res == NULL) {
    return NULL;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
        pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
      return NULL;
    }

    return doFetchRow(pRequest, true, true);

  } else if (TD_RES_TMQ(res)) {
    SMqRspObj      *msg = ((SMqRspObj *)res);
    SReqResultInfo *pResultInfo = taosArrayGet(msg->res, msg->resIter);

    doSetOneRowPtr(pResultInfo);
    pResultInfo->current += 1;

    if (pResultInfo->row == NULL) {
      msg->resIter++;
      pResultInfo = taosArrayGet(msg->res, msg->resIter);
      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
    }

    return pResultInfo->row;

  } else {
    // assert to avoid uninitialization error
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

  return len;
}

int *taos_fetch_lengths(TAOS_RES *res) {
  if (res == NULL) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->length;
}

TAOS_ROW *taos_result_block(TAOS_RES *res) {
  if (res == NULL) {
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
  if (res == NULL || TD_RES_TMQ(res)) {
    return 0;
  }

  SRequestObj    *pRequest = (SRequestObj *)res;
  SReqResultInfo *pResInfo = &pRequest->body.resInfo;
  return pResInfo->numOfRows;
}

int taos_result_precision(TAOS_RES *res) {
  if (res == NULL) {
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
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (db == NULL || strlen(db) == 0) {
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  char sql[256] = {0};
  snprintf(sql, tListLen(sql), "use %s", db);

  TAOS_RES *pRequest = taos_query(taos, sql);
  int32_t   code = taos_errno(pRequest);

  taos_free_result(pRequest);
  return code;
}

void taos_stop_query(TAOS_RES *res) {
  if (res == NULL) {
    return;
  }

  SRequestObj *pRequest = (SRequestObj *)res;
  int32_t      numOfFields = taos_num_fields(pRequest);

  // It is not a query, no need to stop.
  if (numOfFields == 0) {
    return;
  }

  //  scheduleCancelJob(pRequest->body.pQueryJob);
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  SReqResultInfo *pResultInfo = tscGetCurResInfo(res);
  if (col >= pResultInfo->numOfCols || col < 0 || row >= pResultInfo->numOfRows || row < 0) {
    return true;
  }

  SResultColumn *pCol = &pResultInfo->pCol[col];
  return colDataIsNull_f(pCol->nullbitmap, row);
}

bool taos_is_update_query(TAOS_RES *res) { return taos_num_fields(res) == 0; }

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  int32_t numOfRows = 0;
  /*int32_t code = */ taos_fetch_block_s(res, &numOfRows, rows);
  return numOfRows;
}

int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  if (res == NULL) {
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

    doFetchRow(pRequest, false, true);

    // TODO refactor
    SReqResultInfo *pResultInfo = &pRequest->body.resInfo;
    pResultInfo->current = pResultInfo->numOfRows;

    (*rows) = pResultInfo->row;
    (*numOfRows) = pResultInfo->numOfRows;
    return pRequest->code;
  } else if (TD_RES_TMQ(res)) {
    SReqResultInfo *pResultInfo = tmqGetNextResInfo(res);
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
  if (res == NULL) {
    return 0;
  }
  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;

    if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
        pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
      return 0;
    }

    doFetchRow(pRequest, false, false);

    SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

    pResultInfo->current = pResultInfo->numOfRows;
    (*numOfRows) = pResultInfo->numOfRows;
    (*pData) = (void *)pResultInfo->pData;

    return 0;

  } else if (TD_RES_TMQ(res)) {
    SReqResultInfo *pResultInfo = tmqGetNextResInfo(res);
    if (pResultInfo == NULL) return -1;

    pResultInfo->current = pResultInfo->numOfRows;
    (*numOfRows) = pResultInfo->numOfRows;
    (*pData) = (void *)pResultInfo->pData;
    return 0;

  } else {
    ASSERT(0);
    return -1;
  }
}

int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex) {
  if (res == NULL) {
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

int taos_validate_sql(TAOS *taos, const char *sql) { return true; }

void taos_reset_current_db(TAOS *taos) {
  if (taos == NULL) {
    return;
  }

  resetConnectDB(taos);
}

const char *taos_get_server_info(TAOS *taos) {
  if (taos == NULL) {
    return NULL;
  }

  STscObj *pTscObj = (STscObj *)taos;
  return pTscObj->ver;
}

void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  // TODO
}

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  // TODO
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
  // TODO
  return -1;
}

TAOS_STMT *taos_stmt_init(TAOS *taos) {
  // TODO
  return NULL;
}

int taos_stmt_close(TAOS_STMT *stmt) {
  // TODO
  return -1;
}

int taos_stmt_execute(TAOS_STMT *stmt) {
  // TODO
  return -1;
}

char *taos_stmt_errstr(TAOS_STMT *stmt) {
  // TODO
  return NULL;
}

int taos_stmt_affected_rows(TAOS_STMT *stmt) {
  // TODO
  return -1;
}

TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision) {
  // TODO
  return NULL;
}

int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind) {
  // TODO
  return -1;
}

int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  // TODO
  return -1;
}

int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_BIND *tags) {
  // TODO
  return -1;
}

int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name) {
  // TODO
  return -1;
}

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  // TODO
  return -1;
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  // TODO
  return -1;
}

int taos_stmt_add_batch(TAOS_STMT *stmt) {
  // TODO
  return -1;
}

TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt) {
  // TODO
  return NULL;
}

int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  // TODO
  return -1;
}
