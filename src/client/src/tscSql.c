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
#include "tcache.h"
#include "tlog.h"
#include "tnote.h"
#include "trpc.h"
#include "tscJoinProcess.h"
#include "tscProfile.h"
#include "tscSQLParser.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tscompression.h"
#include "tsocket.h"
#include "ttimer.h"
#include "tutil.h"
#include "ihash.h"

TAOS *taos_connect_imp(const char *ip, const char *user, const char *pass, const char *db, uint16_t port,
                       void (*fp)(void *, TAOS_RES *, int), void *param, void **taos) {
  STscObj *pObj;

  taos_init();

  if (pTscMgmtConn == NULL || pVnodeConn == NULL) {
    globalCode = TSDB_CODE_APP_ERROR;
    return NULL;
  }

  if (user == NULL) {
    globalCode = TSDB_CODE_INVALID_ACCT;
    return NULL;
  } else {
    size_t len = strlen(user);
    if (len <= 0 || len > TSDB_USER_LEN) {
      globalCode = TSDB_CODE_INVALID_ACCT;
      return NULL;
    }
  }

  if (pass == NULL) {
    globalCode = TSDB_CODE_INVALID_PASS;
    return NULL;
  } else {
    size_t len = strlen(pass);
    if (len <= 0 || len > TSDB_KEY_LEN) {
      globalCode = TSDB_CODE_INVALID_PASS;
      return NULL;
    }
  }

#ifdef CLUSTER
  if (ip && ip[0]) {
    strcpy(tscMgmtIpList.ipstr[1], ip);
    tscMgmtIpList.ip[1] = inet_addr(ip);
  }
#else
  if (ip && ip[0]) {
    if (ip != tsMasterIp) {
      strcpy(tsMasterIp, ip);
    }
    tsServerIp = inet_addr(ip);
  }
#endif

  pObj = (STscObj *)malloc(sizeof(STscObj));
  if (NULL == pObj) {
    globalCode = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return NULL;
  }

  memset(pObj, 0, sizeof(STscObj));
  pObj->signature = pObj;

  strncpy(pObj->user, user, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), pObj->pass);
  pObj->mgmtPort = port ? port : tsMgmtShellPort;

  if (db) {
    int32_t len = strlen(db);
    /* db name is too long */
    if (len > TSDB_DB_NAME_LEN) {
      free(pObj);
      globalCode = TSDB_CODE_INVALID_DB;
      return NULL;
    }

    char tmp[TSDB_DB_NAME_LEN + 1] = {0};
    strcpy(tmp, db);

    strdequote(tmp);
    strtolower(pObj->db, tmp);
  }

  pthread_mutex_init(&pObj->mutex, NULL);

  SSqlObj *pSql = (SSqlObj *)malloc(sizeof(SSqlObj));
  if (NULL == pSql) {
    globalCode = TSDB_CODE_CLI_OUT_OF_MEMORY;
    free(pObj);
    return NULL;
  }

  memset(pSql, 0, sizeof(SSqlObj));
  pSql->pTscObj = pObj;
  pSql->signature = pSql;
  tsem_init(&pSql->rspSem, 0, 0);
  tsem_init(&pSql->emptyRspSem, 0, 1);
  pObj->pSql = pSql;
  pSql->fp = fp;
  pSql->param = param;
  if (taos != NULL) {
    *taos = pObj;
  }

  pSql->cmd.command = TSDB_SQL_CONNECT;
  int ret = tscAllocPayload(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE);
  if (TSDB_CODE_SUCCESS != ret) {
    globalCode = TSDB_CODE_CLI_OUT_OF_MEMORY;
    free(pSql);
    free(pObj);
    return NULL;
  }

  pSql->res.code = tscProcessSql(pSql);
  if (fp != NULL) {
    tscTrace("%p DB async connection is opening", pObj);
    return pObj;
  }

  if (pSql->res.code) {
    taos_close(pObj);
    return NULL;
  }

  tscTrace("%p DB connection is opened", pObj);
  return pObj;
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  if (ip == NULL || (ip != NULL && (strcmp("127.0.0.1", ip) == 0 || strcasecmp("localhost", ip) == 0))) {
    ip = tsMasterIp;
  }
  tscTrace("try to create a connection to %s", ip);

  void *taos = taos_connect_imp(ip, user, pass, db, port, NULL, NULL, NULL);
  if (taos != NULL) {
    STscObj *pObj = (STscObj *)taos;

    // version compare only requires the first 3 segments of the version string
    int code = taosCheckVersion(version, taos_get_server_info(taos), 3);
    if (code != 0) {
      pObj->pSql->res.code = code;
      taos_close(taos);
      return NULL;
    }
  }

  return taos;
}

TAOS *taos_connect_a(char *ip, char *user, char *pass, char *db, uint16_t port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, void **taos) {
#ifndef CLUSTER
  if (ip == NULL) {
    ip = tsMasterIp;
  }
#endif
  return taos_connect_imp(ip, user, pass, db, port, fp, param, taos);
}

void taos_close(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;

  if (pObj == NULL) return;
  if (pObj->signature != pObj) return;

  if (pObj->pHb != NULL) {
    tscSetFreeHeatBeat(pObj);
  } else {
    tscCloseTscObj(pObj);
  }
}

int taos_query_imp(STscObj *pObj, SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;

  pRes->numOfRows = 1;
  pRes->numOfTotal = 0;
  pSql->asyncTblPos = NULL;
  if (NULL != pSql->pTableHashList) {
    taosCleanUpIntHash(pSql->pTableHashList);
    pSql->pTableHashList = NULL;
  }
  
  tscDump("%p pObj:%p, SQL: %s", pSql, pObj, pSql->sqlstr);

  pRes->code = (uint8_t)tsParseSql(pSql, pObj->acctId, pObj->db, false);

  /*
   * set the qhandle to 0 before return in order to erase the qhandle value assigned in the previous successful query.
   * If qhandle is NOT set 0, the function of taos_free_result() will send message to server by calling tscProcessSql()
   * to free connection, which may cause segment fault, when the parse phrase is not even successfully executed.
   */
  pRes->qhandle = 0;
  pSql->thandle = NULL;

  if (pRes->code == TSDB_CODE_SUCCESS) {
    tscDoQuery(pSql);
  }

  if (pRes->code == TSDB_CODE_SUCCESS) {
    tscTrace("%p SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(pObj), pObj);
  } else {
    tscError("%p SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(pObj), pObj);
  }

  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscFreeSqlObjPartial(pSql);
  }

  return pRes->code;
}

int taos_query(TAOS *taos, const char *sqlstr) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlObj *pSql = pObj->pSql;
  SSqlRes *pRes = &pSql->res;

  size_t sqlLen = strlen(sqlstr);
  if (sqlLen > tsMaxSQLStringLen) {
    pRes->code =
        tscInvalidSQLErrMsg(pSql->cmd.payload, "sql too long", NULL);  // set the additional error msg for invalid sql
    tscError("%p SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);

    return pRes->code;
  }

  taosNotePrintTsc(sqlstr);

  void *sql = realloc(pSql->sqlstr, sqlLen + 1);
  if (sql == NULL) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("%p failed to malloc sql string buffer, reason:%s", pSql, strerror(errno));

    tscError("%p SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
    return pRes->code;
  }

  pSql->sqlstr = sql;
  strtolower(pSql->sqlstr, sqlstr);
  return taos_query_imp(pObj, pSql);
}

TAOS_RES *taos_use_result(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return NULL;
  }

  return pObj->pSql;
}

int taos_result_precision(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  return pSql->res.precision;
}

int taos_num_rows(TAOS_RES *res) { return 0; }

int taos_num_fields(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  SFieldInfo *pFieldsInfo = &pSql->cmd.fieldsInfo;

  return (pFieldsInfo->numOfOutputCols - pFieldsInfo->numOfHiddenCols);
}

int taos_field_count(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) return 0;

  return taos_num_fields(pObj->pSql);
}

int taos_affected_rows(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) return 0;

  return (pObj->pSql->res.numOfRows);
}

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) return 0;

  return pSql->cmd.fieldsInfo.pFields;
}

int taos_retrieve(TAOS_RES *res) {
  if (res == NULL) return 0;
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  if (pSql == NULL || pSql->signature != pSql) return 0;
  if (pRes->qhandle == 0) return 0;

  tscResetForNextRetrieve(pRes);

  if (pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }
  tscProcessSql(pSql);

  return pRes->numOfRows;
}

int taos_fetch_block_impl(TAOS_RES *res, TAOS_ROW *rows) {
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  STscObj *pObj = pSql->pTscObj;

  if (pRes->qhandle == 0 || pObj->pSql != pSql) {
    *rows = NULL;
    return 0;
  }

  // Retrieve new block
  tscResetForNextRetrieve(pRes);
  if (pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }

  tscProcessSql(pSql);
  if (pRes->numOfRows == 0) {
    *rows = NULL;
    return 0;
  }

  // secondary merge has handle this situation
  if (pCmd->command != TSDB_SQL_RETRIEVE_METRIC) {
    pRes->numOfTotal += pRes->numOfRows;
  }

  for (int i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    pRes->tsrow[i] = TSC_GET_RESPTR_BASE(pRes, pCmd, i, pCmd->order) +
                     pRes->bytes[i] * (1 - pCmd->order.order) * (pRes->numOfRows - 1);
  }

  *rows = pRes->tsrow;

  return (pCmd->order.order == TSQL_SO_DESC) ? pRes->numOfRows : -pRes->numOfRows;
}

static void **doSetResultRowData(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  int32_t num = 0;

  for (int i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    pRes->tsrow[i] = TSC_GET_RESPTR_BASE(pRes, pCmd, i, pCmd->order) + pRes->bytes[i] * pRes->row;

    // primary key column cannot be null in interval query, no need to check
    if (i == 0 && pCmd->nAggTimeInterval > 0) {
      continue;
    }

    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

    if (isNull(pRes->tsrow[i], pField->type)) {
      pRes->tsrow[i] = NULL;
    } else if (pField->type == TSDB_DATA_TYPE_NCHAR) {
      // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
      if (pRes->buffer[num] == NULL) {
        pRes->buffer[num] = malloc(pField->bytes + 1);
      } else {
        pRes->buffer[num] = realloc(pRes->buffer[num], pField->bytes + 1);
      }

      /* string terminated */
      memset(pRes->buffer[num], 0, pField->bytes + 1);

      if (taosUcs4ToMbs(pRes->tsrow[i], pField->bytes, pRes->buffer[num])) {
        pRes->tsrow[i] = pRes->buffer[num];
      } else {
        tscError("%p charset:%s to %s. val:%ls convert failed.", pSql, DEFAULT_UNICODE_ENCODEC, tsCharset, pRes->tsrow);
        pRes->tsrow[i] = NULL;
      }
      num++;
    }
  }

  assert(num <= pCmd->fieldsInfo.numOfOutputCols);

  return pRes->tsrow;
}

static void **getOneRowFromBuf(SSqlObj *pSql) {
  doSetResultRowData(pSql);

  SSqlRes *pRes = &pSql->res;
  pRes->row++;

  return pRes->tsrow;
}

static bool tscHashRemainDataInSubqueryResultSet(SSqlObj *pSql) {
  bool     hasData = true;
  SSqlCmd *pCmd = &pSql->cmd;

  if (tscProjectionQueryOnMetric(pCmd)) {
    bool allSubqueryExhausted = true;

    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      SSqlRes *pRes1 = &pSql->pSubs[i]->res;
      SSqlCmd *pCmd1 = &pSql->pSubs[i]->cmd;

      SMeterMetaInfo *pMetaInfo = tscGetMeterMetaInfo(pCmd1, 0);
      assert(pCmd1->numOfTables == 1);

      /*
       * if the global limitation is not reached, and current result has not exhausted, or next more vnodes are
       * available, go on
       */
      if (pMetaInfo->vnodeIndex < pMetaInfo->pMetricMeta->numOfVnodes && pRes1->row < pRes1->numOfRows &&
          (!tscHasReachLimitation(pSql->pSubs[i]))) {
        allSubqueryExhausted = false;
        break;
      }
    }

    hasData = !allSubqueryExhausted;
  } else {  // otherwise, in case inner join, if any subquery exhausted, query completed.
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      SSqlRes *pRes1 = &pSql->pSubs[i]->res;

      if ((pRes1->row >= pRes1->numOfRows && tscHasReachLimitation(pSql->pSubs[i]) &&
           tscProjectionQueryOnTable(&pSql->pSubs[i]->cmd)) ||
          (pRes1->numOfRows == 0)) {
        
        hasData = false;
        break;
      }
    }
  }

  return hasData;
}

static void **tscJoinResultsetFromBuf(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  while (1) {
    if (!tscHashRemainDataInSubqueryResultSet(pSql)) {  // free all sub sqlobj
      tscTrace("%p at least one subquery exhausted, free all other %d subqueries", pSql, pSql->numOfSubs - 1);

      SSubqueryState *pState = NULL;

      for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
        SSqlObj *               pChildObj = pSql->pSubs[i];
        SJoinSubquerySupporter *pSupporter = (SJoinSubquerySupporter *)pChildObj->param;
        pState = pSupporter->pState;

        tscDestroyJoinSupporter(pChildObj->param);
        taos_free_result(pChildObj);
      }

      free(pState);
      return NULL;
    }

    if (pRes->tsrow == NULL) {
      pRes->tsrow = malloc(POINTER_BYTES * pCmd->exprsInfo.numOfExprs);
    }

    bool success = false;
    if (pSql->numOfSubs >= 2) {  // do merge result
      SSqlRes *pRes1 = &pSql->pSubs[0]->res;
      SSqlRes *pRes2 = &pSql->pSubs[1]->res;

      if (pRes1->row < pRes1->numOfRows && pRes2->row < pRes2->numOfRows) {
        doSetResultRowData(pSql->pSubs[0]);
        doSetResultRowData(pSql->pSubs[1]);
        //        TSKEY key1 = *(TSKEY *)pRes1->tsrow[0];
        //        TSKEY key2 = *(TSKEY *)pRes2->tsrow[0];
        //        printf("first:%" PRId64 ", second:%" PRId64 "\n", key1, key2);
        success = true;
        pRes1->row++;
        pRes2->row++;
      }
    } else {  // only one subquery
      SSqlRes *pRes1 = &pSql->pSubs[0]->res;
      doSetResultRowData(pSql->pSubs[0]);

      success = (pRes1->row++ < pRes1->numOfRows);
    }

    if (success) {  // current row of final output has been built, return to app
      for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
        int32_t tableIndex = pRes->pColumnIndex[i].tableIndex;
        int32_t columnIndex = pRes->pColumnIndex[i].columnIndex;

        SSqlRes *pRes1 = &pSql->pSubs[tableIndex]->res;
        pRes->tsrow[i] = pRes1->tsrow[columnIndex];
      }

      break;
    } else {  // continue retrieve data from vnode
      tscFetchDatablockFromSubquery(pSql);
      if (pRes->code != TSDB_CODE_SUCCESS) {
        return NULL;
      }
    }
  }

  return pRes->tsrow;
}

TAOS_ROW taos_fetch_row_impl(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pRes->qhandle == 0 || pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    return NULL;
  }

  if (pCmd->command == TSDB_SQL_METRIC_JOIN_RETRIEVE) {
    tscFetchDatablockFromSubquery(pSql);

    if (pRes->code == TSDB_CODE_SUCCESS) {
      tscTrace("%p data from all subqueries have been retrieved to client", pSql);
      return tscJoinResultsetFromBuf(pSql);
    } else {
      tscTrace("%p retrieve data from subquery failed, code:%d", pSql, pRes->code);
      return NULL;
    }

  } else if (pRes->row >= pRes->numOfRows) {
    tscResetForNextRetrieve(pRes);

    if (pCmd->command < TSDB_SQL_LOCAL) {
      pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    }

    tscProcessSql(pSql);
    if (pRes->numOfRows == 0) {
      return NULL;
    }

    // local reducer has handle this situation
    if (pCmd->command != TSDB_SQL_RETRIEVE_METRIC) {
      pRes->numOfTotal += pRes->numOfRows;
    }
  }

  return getOneRowFromBuf(pSql);
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pSql == NULL || pSql->signature != pSql) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return NULL;
  }

  // projection query on metric, pipeline retrieve data from vnode list, instead of two-stage merge
  TAOS_ROW rows = taos_fetch_row_impl(res);
  while (rows == NULL && tscProjectionQueryOnMetric(pCmd)) {
    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

    // reach the maximum number of output rows, abort
    if (tscHasReachLimitation(pSql)) {
      return NULL;
    }

    /*
     * update the limit and offset value according to current retrieval results
     * Note: if pRes->offset > 0, pRes->numOfRows = 0, pRes->numOfTotal = 0;
     */
    pCmd->limit.limit = pCmd->globalLimit - pRes->numOfTotal;
    pCmd->limit.offset = pRes->offset;

    assert((pRes->offset >= 0 && pRes->numOfRows == 0) || (pRes->offset == 0 && pRes->numOfRows >= 0));

    /*
     * For project query with super table join, the numOfSub is equalled to the number of all subqueries, so
     * we need to reset the value of numOfSubs to be 0.
     *
     * For super table join with projection query, if anyone of the subquery is exhausted, the query completed.
     */
    pSql->numOfSubs = 0;

    if ((++pMeterMetaInfo->vnodeIndex) < pMeterMetaInfo->pMetricMeta->numOfVnodes) {
      pCmd->command = TSDB_SQL_SELECT;
      assert(pSql->fp == NULL);
      tscProcessSql(pSql);
      rows = taos_fetch_row_impl(res);
    }

    // check!!!
    if (rows != NULL || pMeterMetaInfo->vnodeIndex >= pMeterMetaInfo->pMetricMeta->numOfVnodes) {
      break;
    }
  }

  return rows;
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  int nRows = 0;

  if (pSql == NULL || pSql->signature != pSql) {
    globalCode = TSDB_CODE_DISCONNECTED;
    *rows = NULL;
    return 0;
  }

  // projection query on metric, pipeline retrieve data from vnode list,
  // instead of two-stage mergevnodeProcessMsgFromShell free qhandle
  nRows = taos_fetch_block_impl(res, rows);
  while (*rows == NULL && tscProjectionQueryOnMetric(pCmd)) {
    /* reach the maximum number of output rows, abort */
    if (tscHasReachLimitation(pSql)) {
      return 0;
    }

    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

    /* update the limit value according to current retrieval results */
    pCmd->limit.limit = pSql->cmd.globalLimit - pRes->numOfTotal;
    pCmd->limit.offset = pRes->offset;

    if ((++pMeterMetaInfo->vnodeIndex) < pMeterMetaInfo->pMetricMeta->numOfVnodes) {
      pSql->cmd.command = TSDB_SQL_SELECT;
      assert(pSql->fp == NULL);
      tscProcessSql(pSql);
      nRows = taos_fetch_block_impl(res, rows);
    }

    // check!!!
    if (*rows != NULL || pMeterMetaInfo->vnodeIndex >= pMeterMetaInfo->pMetricMeta->numOfVnodes) {
      break;
    }
  }

  return nRows;
}

int taos_select_db(TAOS *taos, const char *db) {
  char sql[64];

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  sprintf(sql, "use %s", db);

  return taos_query(taos, sql);
}

void taos_free_result(TAOS_RES *res) {
  if (res == NULL) return;

  SSqlObj *pSql = (SSqlObj *)res;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  tscTrace("%p start to free result", pSql);

  if (pSql->signature != pSql) return;

  if (pRes == NULL || pRes->qhandle == 0) {
    /* Query rsp is not received from vnode, so the qhandle is NULL */
    tscTrace("%p qhandle is null, abort free, fp:%p", pSql, pSql->fp);
    if (pSql->fp != NULL) {
      pSql->thandle = NULL;
      tscFreeSqlObj(pSql);
      tscTrace("%p Async SqlObj is freed by app", pSql);
    } else {
      tscFreeSqlObjPartial(pSql);
    }
    return;
  }

  // set freeFlag to 1 in retrieve message if there are un-retrieved results
  pCmd->type = TSDB_QUERY_TYPE_FREE_RESOURCE;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  /*
   * case 1. Partial data have been retrieved from vnodes, but not all data has been retrieved yet.
   *         We need to recycle the connection by noticing the vnode return 0 results.
   * case 2. When the query response is received from vnodes and the numOfRows is set to 0, the user calls
   *         taos_free_result before the taos_fetch_row is called in non-stream computing,
   *         we need to recycle the connection.
   * case 3. If the query process is cancelled by user in stable query, tscProcessSql should not be called
   *         for each subquery. Because the failure of execution tsProcessSql may trigger the callback function
   *         be executed, and the retry efforts may result in double free the resources, e.g.,SRetrieveSupport
   */
  if (pRes->code != TSDB_CODE_QUERY_CANCELLED &&
      ((pRes->numOfRows > 0 && pCmd->command < TSDB_SQL_LOCAL) ||
       (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows == 0 && pCmd->command == TSDB_SQL_SELECT &&
        pSql->pStream == NULL && pMeterMetaInfo->pMeterMeta != NULL))) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;

    void *fp = pSql->fp;
    if (fp != NULL) {
      pSql->freed = 1;
    }

    tscProcessSql(pSql);

    /*
     *  If release connection msg is sent to vnode, the corresponding SqlObj for async query can not be freed instantly,
     *  since its free operation is delegated to callback function, which is tscProcessMsgFromServer.
     */
    if (fp == NULL) {
      /*
       * fp may be released here, so we cannot use the pSql->fp
       *
       * In case of handle sync model query, the main SqlObj cannot be freed.
       * So, we only free part attributes, including allocated resources and references on metermeta/metricmeta
       * data in cache.
       *
       * Then this object will be reused and no free operation is required.
       */
      pSql->thandle = NULL;
      tscFreeSqlObjPartial(pSql);
      tscTrace("%p sql result is freed by app", pSql);
    }
  } else {
    // if no free resource msg is sent to vnode, we free this object immediately.
    pSql->thandle = NULL;

    if (pSql->fp) {
      assert(pRes->numOfRows == 0 || (pCmd->command > TSDB_SQL_LOCAL));
      tscFreeSqlObj(pSql);
      tscTrace("%p Async sql result is freed by app", pSql);
    } else {
      tscFreeSqlObjPartial(pSql);
      tscTrace("%p sql result is freed", pSql);
    }
  }
}

int taos_errno(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  int      code;

  if (pObj == NULL || pObj->signature != pObj) return globalCode;

  if ((int8_t)(pObj->pSql->res.code) == -1)
    code = TSDB_CODE_OTHERS;
  else
    code = pObj->pSql->res.code;

  return code;
}

char *taos_errstr(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  uint8_t  code;

  if (pObj == NULL || pObj->signature != pObj) return tsError[globalCode];

  if ((int8_t)(pObj->pSql->res.code) == -1)
    code = TSDB_CODE_OTHERS;
  else
    code = pObj->pSql->res.code;

  // for invalid sql, additional information is attached to explain why the sql is invalid
  if (code == TSDB_CODE_INVALID_SQL) {
    return pObj->pSql->cmd.payload;
  } else {
    if (code < 0 || code > TSDB_CODE_MAX_ERROR_CODE) {
      return tsError[TSDB_CODE_SUCCESS];
    } else {
      return tsError[code];
    }
  }
}

void taos_config(int debug, char *log_path) {
  uDebugFlag = debug;
  strcpy(logDir, log_path);
}

char *taos_get_server_info(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;

  if (pObj == NULL) return NULL;

  return pObj->sversion;
}

char *taos_get_client_info() { return version; }

void taos_stop_query(TAOS_RES *res) {
  if (res == NULL) return;

  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql->signature != pSql) return;
  tscTrace("%p start to cancel query", res);

  pSql->res.code = TSDB_CODE_QUERY_CANCELLED;

  if (tscIsTwoStageMergeMetricQuery(&pSql->cmd)) {
    tscKillMetricQuery(pSql);
    return;
  }

  if (pSql->cmd.command >= TSDB_SQL_LOCAL) {
    return;
  }

  if (pSql->thandle == NULL) {
    tscTrace("%p no connection, abort cancel", res);
    return;
  }

  taosStopRpcConn(pSql->thandle);
  tscTrace("%p query is cancelled", res);
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  int len = 0;
  for (int i = 0; i < num_fields; ++i) {
    if (row[i] == NULL) {
      len += sprintf(str + len, "%s ", TSDB_DATA_NULL_STR);
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += sprintf(str + len, "%d ", *((char *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d ", *((short *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d ", *((int *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64 " ", *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += sprintf(str + len, "%f ", fv);
      }
        break;

      case TSDB_DATA_TYPE_DOUBLE:{
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += sprintf(str + len, "%lf ", dv);
      }
        break;

      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR: {
        /* limit the max length of string to no greater than the maximum length,
         * in case of not null-terminated string */
        size_t xlen = strlen(row[i]);
        size_t trueLen = MIN(xlen, fields[i].bytes);

        memcpy(str + len, (char *)row[i], trueLen);

        str[len + trueLen] = ' ';
        len += (trueLen + 1);
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += sprintf(str + len, "%" PRId64 " ", *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += sprintf(str + len, "%d ", *((int8_t *)row[i]));
      default:
        break;
    }
  }

  return len;
}

int taos_validate_sql(TAOS *taos, const char *sql) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlObj *pSql = pObj->pSql;
  SSqlRes *pRes = &pSql->res;

  pRes->numOfRows = 1;
  pRes->numOfTotal = 0;

  tscTrace("%p Valid SQL: %s pObj:%p", pSql, sql, pObj);

  int32_t sqlLen = strlen(sql);
  if (sqlLen > tsMaxSQLStringLen) {
    tscError("%p sql too long", pSql);
    pRes->code = TSDB_CODE_INVALID_SQL;
    return pRes->code;
  }

  pSql->sqlstr = realloc(pSql->sqlstr, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("%p failed to malloc sql string buffer", pSql);
    tscTrace("%p Valid SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
    return pRes->code;
  }

  strtolower(pSql->sqlstr, sql);

  pSql->asyncTblPos = NULL;
  if (NULL != pSql->pTableHashList) {
    taosCleanUpIntHash(pSql->pTableHashList);
    pSql->pTableHashList = NULL;
  }

  pRes->code = (uint8_t)tsParseSql(pSql, pObj->acctId, pObj->db, false);
  int code = pRes->code;

  tscTrace("%p Valid SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
  taos_free_result(pSql);

  return code;
}

static int tscParseTblNameList(SSqlObj *pSql, const char *tblNameList, int32_t tblListLen) {
  // must before clean the sqlcmd object
  tscRemoveAllMeterMetaInfo(&pSql->cmd, false);
  tscCleanSqlCmd(&pSql->cmd);

  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->command = TSDB_SQL_MULTI_META;
  pCmd->count = 0;

  int   code = TSDB_CODE_INVALID_METER_ID;
  char *str = (char *)tblNameList;

  SMeterMetaInfo *pMeterMetaInfo = tscAddEmptyMeterMetaInfo(pCmd);

  if ((code = tscAllocPayload(pCmd, tblListLen + 16)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  char *nextStr;
  char  tblName[TSDB_METER_ID_LEN];
  int   payloadLen = 0;
  char *pMsg = pCmd->payload;
  while (1) {
    nextStr = strchr(str, ',');
    if (nextStr == NULL) {
      break;
    }

    memcpy(tblName, str, nextStr - str);
    int32_t len = nextStr - str;
    tblName[len] = '\0';

    str = nextStr + 1;

    strtrim(tblName);
    len = (uint32_t)strlen(tblName);

    SSQLToken sToken = {.n = len, .type = TK_ID, .z = tblName};
    tSQLGetToken(tblName, &sToken.type);

    // Check if the table name available or not
    if (tscValidateName(&sToken) != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_INVALID_METER_ID;
      sprintf(pCmd->payload, "table name is invalid");
      return code;
    }

    if ((code = setMeterID(pSql, &sToken, 0)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (++pCmd->count > TSDB_MULTI_METERMETA_MAX_NUM) {
      code = TSDB_CODE_INVALID_METER_ID;
      sprintf(pCmd->payload, "tables over the max number");
      return code;
    }

    if (payloadLen + strlen(pMeterMetaInfo->name) + 128 >= pCmd->allocSize) {
      char *pNewMem = realloc(pCmd->payload, pCmd->allocSize + tblListLen);
      if (pNewMem == NULL) {
        code = TSDB_CODE_CLI_OUT_OF_MEMORY;
        sprintf(pCmd->payload, "failed to allocate memory");
        return code;
      }

      pCmd->payload = pNewMem;
      pCmd->allocSize = pCmd->allocSize + tblListLen;
      pMsg = pCmd->payload;
    }

    payloadLen += sprintf(pMsg + payloadLen, "%s,", pMeterMetaInfo->name);
  }

  *(pMsg + payloadLen) = '\0';
  pCmd->payloadLen = payloadLen + 1;

  return TSDB_CODE_SUCCESS;
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  const int32_t MAX_TABLE_NAME_LENGTH = 12 * 1024 * 1024;  // 12MB list

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlObj *pSql = pObj->pSql;
  SSqlRes *pRes = &pSql->res;

  pRes->numOfTotal = 0;  // the number of getting table meta from server
  pRes->code = 0;

  assert(pSql->fp == NULL);
  tscTrace("%p tableNameList: %s pObj:%p", pSql, tableNameList, pObj);

  int32_t tblListLen = strlen(tableNameList);
  if (tblListLen > MAX_TABLE_NAME_LENGTH) {
    tscError("%p tableNameList too long, length:%d, maximum allowed:%d", pSql, tblListLen, MAX_TABLE_NAME_LENGTH);
    pRes->code = TSDB_CODE_INVALID_SQL;
    return pRes->code;
  }

  char *str = calloc(1, tblListLen + 1);
  if (str == NULL) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("%p failed to malloc sql string buffer", pSql);
    return pRes->code;
  }

  strtolower(str, tableNameList);
  pRes->code = (uint8_t)tscParseTblNameList(pSql, str, tblListLen);

  /*
   * set the qhandle to 0 before return in order to erase the qhandle value assigned in the previous successful query.
   * If qhandle is NOT set 0, the function of taos_free_result() will send message to server by calling tscProcessSql()
   * to free connection, which may cause segment fault, when the parse phrase is not even successfully executed.
   */
  pRes->qhandle = 0;
  pSql->thandle = NULL;
  free(str);

  if (pRes->code != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }

  tscDoQuery(pSql);

  tscTrace("%p load multi metermeta result:%d %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscFreeSqlObjPartial(pSql);
  }

  return pRes->code;
}
