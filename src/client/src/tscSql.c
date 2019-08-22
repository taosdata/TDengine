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

#include <stdio.h>
#include <stdlib.h>

#include "os.h"
#include "tcache.h"
#include "tlog.h"
#include "trpc.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tsocket.h"
#include "tsql.h"
#include "ttimer.h"
#include "tutil.h"

TAOS *taos_connect_imp(char *ip, char *user, char *pass, char *db, int port, void (*fp)(void *, TAOS_RES *, int),
                       void *param, void **taos) {
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

  if (ip && ip[0]) {
    strcpy(tsServerIpStr, ip);
    tsServerIp = inet_addr(ip);
  }

  pObj = (STscObj *)malloc(sizeof(STscObj));
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
  memset(pSql, 0, sizeof(SSqlObj));
  pSql->pTscObj = pObj;
  pSql->signature = pSql;
  sem_init(&pSql->rspSem, 0, 0);
  sem_init(&pSql->emptyRspSem, 0, 1);
  pObj->pSql = pSql;
  pSql->fp = fp;
  pSql->param = param;
  if (taos != NULL) {
    *taos = pObj;
  }

  pSql->cmd.command = TSDB_SQL_CONNECT;
  tscAllocPayloadWithSize(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE);

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

TAOS *taos_connect(char *ip, char *user, char *pass, char *db, int port) {
  if (ip != NULL && (strcmp("127.0.0.1", ip) == 0 || strcasecmp("localhost", ip) == 0)) {
    ip = tsServerIpStr;
  }

  if (ip == NULL) ip = tsServerIpStr;
  tscTrace("try to create a connection to %s", ip);

  void *taos = taos_connect_imp(ip, user, pass, db, port, NULL, NULL, NULL);
  if (taos != NULL) {
    char *server_version = taos_get_server_info(taos);
    if (server_version && strcmp(server_version, version) != 0) {
      tscError("taos:%p, server version:%s not equal with client version:%s, close connection",
               taos, server_version, version);
      taos_close(taos);
      globalCode = TSDB_CODE_INVALID_CLIENT_VERSION;
      return NULL;
    }
  }

  return taos;
}

TAOS *taos_connect_a(char *ip, char *user, char *pass, char *db, int port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, void **taos) {
  if (ip == NULL) {
    ip = tsServerIpStr;
  }
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

int taos_query(TAOS *taos, char *sqlstr) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlObj *pSql = pObj->pSql;
  SSqlRes *pRes = &pSql->res;

  pRes->numOfRows = 1;
  pRes->numOfTotal = 0;

  tscTrace("%p SQL: %s pObj:%p", pSql, sqlstr, pObj);

  int32_t sqlLen = strlen(sqlstr);
  if (sqlLen > TSDB_MAX_SQL_LEN) {
    tscError("%p sql too long", pSql);
    pRes->code = TSDB_CODE_INVALID_SQL;
    return pRes->code;
  }

  pSql->sqlstr = realloc(pSql->sqlstr, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("%p failed to malloc sql string buffer", pSql);
    tscTrace("%p SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
    return pRes->code;
  }

  strtolower(pSql->sqlstr, sqlstr);

  pRes->code = (uint8_t)tsParseSql(pSql, pObj->acctId, pObj->db, false);

  /*
   * set the qhandle to 0 before return in order to erase the qhandle value assigned in the previous successful query.
   * If qhandle is NOT set 0, the function of taos_free_result() will send message to server by calling tscProcessSql()
   * to free connection, which may cause segment fault, when the parse phrase is not even successfully executed.
   */
  pRes->qhandle = 0;
  pSql->thandle = NULL;

  if (pRes->code != TSDB_CODE_SUCCESS) return pRes->code;

  tscDoQuery(pSql);

  tscTrace("%p SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscFreeSqlObjPartial(pSql);
  }

  return pRes->code;
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

  return pSql->cmd.fieldsInfo.numOfOutputCols;
}

int taos_field_count(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) return 0;

  return pObj->pSql->cmd.fieldsInfo.numOfOutputCols;
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

  pRes->row = 0;
  pRes->numOfRows = 0;
  pCmd->type = 0;
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
  pRes->row = 0;
  pRes->numOfRows = 0;
  pCmd->type = 0;
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

TAOS_ROW taos_fetch_row_impl(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  STscObj *pObj = pSql->pTscObj;
  int      wccount = 0;

  if (pRes->qhandle == 0) return NULL;

  if (pRes->row >= pRes->numOfRows) {
    if (pObj->pSql != pSql) return NULL;

    pRes->row = 0;
    pRes->numOfRows = 0;
    pCmd->type = 0;
    if (pCmd->command < TSDB_SQL_LOCAL) {
      pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    }

    tscProcessSql(pSql);
    if (pRes->numOfRows == 0) {
      return NULL;
    }

    // secondary merge has handle this situation
    if (pCmd->command != TSDB_SQL_RETRIEVE_METRIC) {
      pRes->numOfTotal += pRes->numOfRows;
    }
  }

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
      /*
       * convert unicode to native code in a temporary buffer extra one byte for terminated symbol
       */
      if (pRes->buffer[wccount] == NULL) {
        pRes->buffer[wccount] = (char *)calloc(1, pField->bytes + 1);
      } else {
        pRes->buffer[wccount] = realloc(pRes->buffer[wccount], pField->bytes + 1);
      }

      /* string terminated */
      memset(pRes->buffer[wccount], 0, pField->bytes);

      if (taosUcs4ToMbs(pRes->tsrow[i], pField->bytes, pRes->buffer[wccount])) {
        pRes->tsrow[i] = pRes->buffer[wccount];
      } else {
        tscError("%p charset:%s to %s. val:%ls convert failed.", pSql, DEFAULT_UNICODE_ENCODEC, tsCharset, pRes->tsrow);
        pRes->tsrow[i] = NULL;
      }
      wccount++;
    }
  }

  assert(wccount <= pRes->numOfnchar);
  pRes->row++;

  return pRes->tsrow;
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
  while (rows == NULL && tscProjectionQueryOnMetric(pSql)) {
    /* reach the maximum number of output rows, abort */
    if (pCmd->globalLimit > 0 && pRes->numOfTotal >= pCmd->globalLimit) {
      return NULL;
    }

    /*
     * update the limit and offset value according to current retrieval results
     * Note: if pRes->offset > 0, pRes->numOfRows = 0, pRes->numOfTotal = 0;
     */
    pCmd->limit.limit = pCmd->globalLimit - pRes->numOfTotal;
    pCmd->limit.offset = pRes->offset;

    assert((pRes->offset >= 0 && pRes->numOfRows == 0) || (pRes->offset == 0 && pRes->numOfRows >= 0));

    if ((++pCmd->vnodeIdx) <= pCmd->pMetricMeta->numOfVnodes) {
      pCmd->command = TSDB_SQL_SELECT;
      assert(pSql->fp == NULL);
      tscProcessSql(pSql);
      rows = taos_fetch_row_impl(res);
    }

    // check!!!
    if (rows != NULL || pCmd->vnodeIdx >= pCmd->pMetricMeta->numOfVnodes) {
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

  // projection query on metric, pipeline retrieve data from vnode list, instead
  // of two-stage mergevnodeProcessMsgFromShell free qhandle
  nRows = taos_fetch_block_impl(res, rows);
  if (*rows == NULL && tscProjectionQueryOnMetric(pSql)) {
    /* reach the maximum number of output rows, abort */
    if (pCmd->globalLimit > 0 && pRes->numOfTotal >= pCmd->globalLimit) {
      return 0;
    }

    /* update the limit value according to current retrieval results */
    pCmd->limit.limit = pSql->cmd.globalLimit - pRes->numOfTotal;

    if ((++pSql->cmd.vnodeIdx) <= pSql->cmd.pMetricMeta->numOfVnodes) {
      pSql->cmd.command = TSDB_SQL_SELECT;
      assert(pSql->fp == NULL);
      tscProcessSql(pSql);
      nRows = taos_fetch_block_impl(res, rows);
    }
  }

  return nRows;
}

int taos_select_db(TAOS *taos, char *db) {
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

  pCmd->type = 1;  // set freeFlag to 1 in retrieve message if there are
                   // un-retrieved results

  /*
   * case 1. Partial data have been retrieved from vnodes, but not all data has been retrieved yet. We need to recycle
   *         the connection by noticing the vnode return 0 results.
   * case 2. When the query response is received from vnodes and the numOfRows is set to 0, the user calls
   *         taos_free_result before the taos_fetch_row is called in non-stream computing, we need to recycle the
   *         connection.
   * case 3. If the query process is cancelled by user in stable query, tscProcessSql should not be called for each
   *         subquery. Because the failure of execution tsProcessSql may trigger the callback function
   *         be executed, and the retry efforts may result in double free the
   * resources, e.g.,SRetrieveSupport
   */
  if (pRes->code != TSDB_CODE_QUERY_CANCELLED &&
      ((pRes->numOfRows > 0 && pCmd->command < TSDB_SQL_LOCAL) ||
       (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows == 0 && pCmd->command == TSDB_SQL_SELECT &&
        pSql->pStream == NULL && pCmd->pMeterMeta != NULL))) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    tscProcessSql(pSql);

    if (pSql->fp) {
      pSql->freed = 1;
    } else {
      pSql->thandle = NULL;

      /*
      * remove allocated resources and release metermeta/metricmeta references in cache
      * since current query is completed
      */
      tscFreeSqlObjPartial(pSql);
      tscTrace("%p sql result is freed by app", pSql);
    }
  } else {
    if (pSql->fp) {
      assert(pRes->numOfRows == 0 || (pCmd->command > TSDB_SQL_LOCAL));
      pSql->thandle = NULL;
      tscFreeSqlObj(pSql);
      tscTrace("%p Async SqlObj is freed by app", pSql);
    } else {
      pSql->thandle = NULL;

      /*
       * remove allocated resources and release metermeta/metricmeta references in cache
       * since current query is completed
       */
      tscFreeSqlObjPartial(pSql);
      tscTrace("%p sql result is freed", pSql);
    }
  }
}

int taos_errno(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  int      code;

  if (pObj == NULL || pObj->signature != pObj) return globalCode;

  if (pObj->pSql->res.code == -1)
    code = TSDB_CODE_OTHERS;
  else
    code = pObj->pSql->res.code;

  return code;
}

char *taos_errstr(TAOS *taos) {
  STscObj *     pObj = (STscObj *)taos;
  unsigned char code;
  char          temp[256] = {0};

  if (pObj == NULL || pObj->signature != pObj) return tsError[globalCode];

  if (pObj->pSql->res.code == -1)
    code = TSDB_CODE_OTHERS;
  else
    code = pObj->pSql->res.code;

  if (code == TSDB_CODE_INVALID_SQL) {
    sprintf(temp, "invalid SQL: %s", pObj->pSql->cmd.payload);
    strcpy(pObj->pSql->cmd.payload, temp);
    return pObj->pSql->cmd.payload;
  } else {
    return tsError[code];
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

  if (tscIsTwoStageMergeMetricQuery(pSql)) {
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
        len += sprintf(str + len, "%ld ", *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT:
        len += sprintf(str + len, "%f ", *((float *)row[i]));
        break;

      case TSDB_DATA_TYPE_DOUBLE:
        len += sprintf(str + len, "%lf ", *((double *)row[i]));
        break;

      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR:
        /* limit the max length of string to no greater than the maximum length,
         * in case of not null-terminated string */
        len += snprintf(str + len, (size_t)fields[i].bytes + 1, "%s ", (char *)row[i]);
        break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += sprintf(str + len, "%ld ", *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += sprintf(str + len, "%d ", *((int8_t *)row[i]));
      default:
        break;
    }
  }

  return len;
}

int taos_validate_sql(TAOS *taos, char *sql) {
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
  if (sqlLen > TSDB_MAX_SQL_LEN) {
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

  pRes->code = (uint8_t)tsParseSql(pSql, pObj->acctId, pObj->db, false);
  int code = pRes->code;

  tscTrace("%p Valid SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
  taos_free_result(pSql);

  return code;
}
