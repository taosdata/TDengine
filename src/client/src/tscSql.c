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

#include "hash.h"
#include "os.h"
#include "qast.h"
#include "tcache.h"
#include "tnote.h"
#include "trpc.h"
#include "tscLog.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscSubquery.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tscompression.h"
#include "tsocket.h"
#include "ttimer.h"
#include "ttokendef.h"
#include "tutil.h"

static bool validImpl(const char* str, size_t maxsize) {
  if (str == NULL) {
    return false;
  }
  
  size_t len = strlen(str);
  if (len <= 0 || len > maxsize) {
    return false;
  }
  
  return true;
}

static bool validUserName(const char* user) {
  return validImpl(user, TSDB_USER_LEN);
}

static bool validPassword(const char* passwd) {
  return validImpl(passwd, TSDB_PASSWORD_LEN);
}

STscObj *taosConnectImpl(const char *ip, const char *user, const char *pass, const char *db, uint16_t port,
                       void (*fp)(void *, TAOS_RES *, int), void *param, void **taos) {
  taos_init();
  
  if (!validUserName(user)) {
    terrno = TSDB_CODE_INVALID_ACCT;
    return NULL;
  }

  if (!validPassword(pass)) {
    terrno = TSDB_CODE_INVALID_PASS;
    return NULL;
  }

  void* pMgmtConn = NULL;
  if (tscInitRpc(user, pass, &pMgmtConn) != 0) {
    terrno = TSDB_CODE_NETWORK_UNAVAIL;
    return NULL;
  }

  tscMgmtIpSet.numOfIps = 0;

  if (ip && ip[0]) {
    tscMgmtIpSet.inUse = 0;
    tscMgmtIpSet.numOfIps = 1;
    strcpy(tscMgmtIpSet.fqdn[0], ip);
    tscMgmtIpSet.port[0] = port? port: tsMnodeShellPort;
  } else {
    if (tsMaster[0] != 0) {
      taosGetFqdnPortFromEp(tsMaster, tscMgmtIpSet.fqdn[tscMgmtIpSet.numOfIps], &tscMgmtIpSet.port[tscMgmtIpSet.numOfIps]);
      tscMgmtIpSet.numOfIps++;
    }

    if (tsSecond[0] != 0) {
      taosGetFqdnPortFromEp(tsSecond, tscMgmtIpSet.fqdn[tscMgmtIpSet.numOfIps], &tscMgmtIpSet.port[tscMgmtIpSet.numOfIps]);
      tscMgmtIpSet.numOfIps++;
    }
  }
  
  STscObj *pObj = (STscObj *)calloc(1, sizeof(STscObj));
  if (NULL == pObj) {
    terrno = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return NULL;
  }

  pObj->signature = pObj;

  strncpy(pObj->user, user, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), pObj->pass);
  pObj->mgmtPort = port ? port : tsMnodeShellPort;

  if (db) {
    int32_t len = strlen(db);
    /* db name is too long */
    if (len > TSDB_DB_NAME_LEN) {
      free(pObj);
      terrno = TSDB_CODE_INVALID_DB;
      return NULL;
    }

    char tmp[TSDB_DB_NAME_LEN + 1] = {0};
    strcpy(tmp, db);

    strdequote(tmp);
    strtolower(pObj->db, tmp);
  }

  pObj->pMgmtConn = pMgmtConn;
  pthread_mutex_init(&pObj->mutex, NULL);

  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (NULL == pSql) {
    terrno = TSDB_CODE_CLI_OUT_OF_MEMORY;
    free(pObj);
    return NULL;
  }

  pSql->pTscObj = pObj;
  pSql->signature = pSql;
  pSql->maxRetry = TSDB_MAX_REPLICA_NUM;
  
  tsem_init(&pSql->rspSem, 0, 0);
  
  pObj->pSql = pSql;
  pSql->fp = fp;
  pSql->param = param;
  if (taos != NULL) {
    *taos = pObj;
  }

  pSql->cmd.command = TSDB_SQL_CONNECT;
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    terrno = TSDB_CODE_CLI_OUT_OF_MEMORY;
    free(pSql);
    free(pObj);
    return NULL;
  }

  // tsRpcHeaderSize will be updated during RPC initialization, so only after it initialization, this value is valid
  tsInsertHeadSize = tsRpcHeadSize + sizeof(SMsgDesc) + sizeof(SSubmitMsg);
  return pObj;
}

static void syncConnCallback(void *param, TAOS_RES *tres, int code) {
  STscObj *pObj = (STscObj *)param;
  assert(pObj != NULL && pObj->pSql != NULL);
  
  if (code < 0) {
    pObj->pSql->res.code = code;
  }
  
  sem_post(&pObj->pSql->rspSem);
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  tscTrace("try to create a connection to %s", ip);

  STscObj *pObj = taosConnectImpl(ip, user, pass, db, port, NULL, NULL, NULL);
  if (pObj != NULL) {
    SSqlObj* pSql = pObj->pSql;
    assert(pSql != NULL);
    
    pSql->fp = syncConnCallback;
    pSql->param = pObj;
    
    tscProcessSql(pSql);
    sem_wait(&pSql->rspSem);
    
    if (pSql->res.code != TSDB_CODE_SUCCESS) {
      terrno = pSql->res.code;
      taos_close(pObj);
      return NULL;
    }
    
    tscTrace("%p DB connection is opening", pObj);
    
    // version compare only requires the first 3 segments of the version string
    int code = taosCheckVersion(version, taos_get_server_info(pObj), 3);
    if (code != 0) {
      terrno = code;
      taos_close(pObj);
      return NULL;
    } else {
      return pObj;
    }
  }

  return NULL;
}

TAOS *taos_connect_a(char *ip, char *user, char *pass, char *db, uint16_t port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, void **taos) {
  STscObj* pObj = taosConnectImpl(ip, user, pass, db, port, fp, param, taos);
  if (pObj == NULL) {
    return NULL;
  }
  
  SSqlObj* pSql = pObj->pSql;
  
  pSql->res.code = tscProcessSql(pSql);
  tscTrace("%p DB async connection is opening", pObj);
  
  return pObj;
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
  SSqlCmd *pCmd = &pSql->cmd;
  
  pRes->numOfRows  = 1;
  pRes->numOfTotal = 0;
  pRes->numOfTotalInCurrentClause = 0;

  pCmd->curSql = NULL;
  if (NULL != pCmd->pTableList) {
    taosHashCleanup(pCmd->pTableList);
    pCmd->pTableList = NULL;
  }

  tscDump("%p pObj:%p, SQL: %s", pSql, pObj, pSql->sqlstr);

  pRes->code = (uint8_t)tsParseSql(pSql, false);

  /*
   * set the qhandle to 0 before return in order to erase the qhandle value assigned in the previous successful query.
   * If qhandle is NOT set 0, the function of taos_free_result() will send message to server by calling tscProcessSql()
   * to free connection, which may cause segment fault, when the parse phrase is not even successfully executed.
   */
  pRes->qhandle = 0;

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

static void waitForQueryRsp(void *param, TAOS_RES *tres, int code) {
  assert(param != NULL);
  SSqlObj *pSql = ((STscObj *)param)->pSql;
  
  // valid error code is less than 0
  if (code < 0) {
    pSql->res.code = code;
  }
  
  sem_post(&pSql->rspSem);
}

int taos_query(TAOS *taos, const char *sqlstr) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }
  
  SSqlObj* pSql = pObj->pSql;
  
  size_t sqlLen = strlen(sqlstr);
  doAsyncQuery(pObj, pSql, waitForQueryRsp, taos, sqlstr, sqlLen);

  // wait for the callback function to post the semaphore
  sem_wait(&pSql->rspSem);
  return pSql->res.code;
}

TAOS_RES *taos_use_result(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_DISCONNECTED;
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

  int32_t num = 0;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  if (pQueryInfo == NULL) {
    return num;
  }

  size_t numOfCols = tscNumOfFields(pQueryInfo);
  for(int32_t i = 0; i < numOfCols; ++i) {
    SFieldSupInfo* pInfo = taosArrayGet(pQueryInfo->fieldsInfo.pSupportInfo, i);
    if (pInfo->visible) {
      num++;
    }
  }
  
  return num;
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

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  if (pQueryInfo == NULL) {
    return NULL;
  }
  
  size_t numOfCols = tscNumOfFields(pQueryInfo);
  if (numOfCols == 0) {
    return NULL;
  }
  
  return pQueryInfo->fieldsInfo.pFields->pData;
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
    pRes->numOfTotalInCurrentClause += pRes->numOfRows;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  if (pQueryInfo == NULL)
    return 0;

  for (int i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    pRes->tsrow[i] = tscGetResultColumnChr(pRes, pQueryInfo, i);
  }

  *rows = pRes->tsrow;

  return (pQueryInfo->order.order == TSDB_ORDER_DESC) ? pRes->numOfRows : -pRes->numOfRows;
}

static void transferNcharData(SSqlObj *pSql, int32_t columnIndex, TAOS_FIELD *pField) {
  SSqlRes *pRes = &pSql->res;

  if (isNull(pRes->tsrow[columnIndex], pField->type)) {
    pRes->tsrow[columnIndex] = NULL;
  } else if (pField->type == TSDB_DATA_TYPE_NCHAR) {
    // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
    if (pRes->buffer[columnIndex] == NULL) {
      pRes->buffer[columnIndex] = malloc(pField->bytes + TSDB_NCHAR_SIZE);
    }

    /* string terminated char for binary data*/
    memset(pRes->buffer[columnIndex], 0, pField->bytes + TSDB_NCHAR_SIZE);

    if (taosUcs4ToMbs(pRes->tsrow[columnIndex], pField->bytes, pRes->buffer[columnIndex])) {
      pRes->tsrow[columnIndex] = pRes->buffer[columnIndex];
    } else {
      tscError("%p charset:%s to %s. val:%ls convert failed.", pSql, DEFAULT_UNICODE_ENCODEC, tsCharset, pRes->tsrow);
      pRes->tsrow[columnIndex] = NULL;
    }
  }
}

static char *getArithemicInputSrc(void *param, const char *name, int32_t colId) {
//  SArithmeticSupport *pSupport = (SArithmeticSupport *)param;
//  SArithExprInfo *  pExpr = pSupport->pArithExpr;

//  int32_t index = -1;
//  for (int32_t i = 0; i < pExpr->numOfCols; ++i) {
//    if (strcmp(name, pExpr->colList[i].name) == 0) {
//      index = i;
//      break;
//    }
//  }
//
//  assert(index >= 0 && index < pExpr->numOfCols);
//  return pSupport->data[index] + pSupport->offset * pSupport->elemSize[index];
return 0;
}

static void **doSetResultRowData(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  assert(pRes->row >= 0 && pRes->row <= pRes->numOfRows);

  if (pRes->row >= pRes->numOfRows) {  // all the results has returned to invoker
    tfree(pRes->tsrow);
    return pRes->tsrow;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  //todo refactor move away
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for(int32_t k = 0; k < numOfExprs; ++k) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, k);
    
    if (k > 0) {
      SSqlExpr* pPrev = tscSqlExprGet(pQueryInfo, k - 1);
      pExpr->offset = pPrev->offset + pPrev->resBytes;
    }
  }
  
  int32_t num = 0;
  for (int i = 0; i < tscNumOfFields(pQueryInfo); ++i) {
    SFieldSupInfo* pInfo = tscFieldInfoGetSupp(&pQueryInfo->fieldsInfo, i);
    if (pInfo->pSqlExpr != NULL) {
      pRes->tsrow[i] = tscGetResultColumnChr(pRes, pQueryInfo, i) + pInfo->pSqlExpr->resBytes * pRes->row;
    } else {
      assert(0);
    }

    // primary key column cannot be null in interval query, no need to check
    if (i == 0 && pQueryInfo->intervalTime > 0) {
      continue;
    }

    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
    transferNcharData(pSql, i, pField);

    // calculate the result from several other columns
    if (pInfo->pArithExprInfo != NULL) {
      SArithmeticSupport *sas = (SArithmeticSupport *)calloc(1, sizeof(SArithmeticSupport));
      sas->offset = 0;
      sas->pArithExpr = pInfo->pArithExprInfo;
      
//      sas->numOfCols = sas->pArithExpr->numOfCols;
      
      if (pRes->buffer[i] == NULL) {
        pRes->buffer[i] = malloc(tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i)->bytes);
      }
      
      for(int32_t k = 0; k < sas->numOfCols; ++k) {
//        int32_t columnIndex = sas->pArithExpr->colList[k].colIndex;
//        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, columnIndex);
//
//        sas->elemSize[k] = pExpr->resBytes;
//        sas->data[k] = (pRes->data + pRes->numOfRows* pExpr->offset) + pRes->row*pExpr->resBytes;
      }

      tExprTreeCalcTraverse(sas->pArithExpr->pExpr, 1, pRes->buffer[i], sas, TSDB_ORDER_ASC, getArithemicInputSrc);
      pRes->tsrow[i] = pRes->buffer[i];
      
      free(sas); //todo optimization
    }
  }

  assert(num <= pQueryInfo->fieldsInfo.numOfOutput);

  pRes->row++;  // index increase one-step
  return pRes->tsrow;
}

static bool tscHashRemainDataInSubqueryResultSet(SSqlObj *pSql) {
  bool     hasData = true;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    bool allSubqueryExhausted = true;

    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] == NULL) {
        continue;
      }

//      SSqlRes *pRes1 = &pSql->pSubs[i]->res;
      SSqlCmd *pCmd1 = &pSql->pSubs[i]->cmd;

      SQueryInfo *    pQueryInfo1 = tscGetQueryInfoDetail(pCmd1, pCmd1->clauseIndex);
//      STableMetaInfo *pMetaInfo = tscGetMetaInfo(pQueryInfo1, 0);

      assert(pQueryInfo1->numOfTables == 1);

      /*
       * if the global limitation is not reached, and current result has not exhausted, or next more vnodes are
       * available, goes on
       */
//      if (pMetaInfo->vnodeIndex < pMetaInfo->pMetricMeta->numOfVnodes && pRes1->row < pRes1->numOfRows &&
//          (!tscHasReachLimitation(pQueryInfo1, pRes1))) {
//        allSubqueryExhausted = false;
//        break;
//      }
    }

    hasData = !allSubqueryExhausted;
  } else {  // otherwise, in case inner join, if any subquery exhausted, query completed.
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] == 0) {
        continue;
      }

      SSqlRes *   pRes1 = &pSql->pSubs[i]->res;
      SQueryInfo *pQueryInfo1 = tscGetQueryInfoDetail(&pSql->pSubs[i]->cmd, 0);

      if ((pRes1->row >= pRes1->numOfRows && tscHasReachLimitation(pQueryInfo1, pRes1) &&
           tscProjectionQueryOnTable(pQueryInfo1)) ||
          (pRes1->numOfRows == 0)) {
        hasData = false;
        break;
      }
    }
  }

  return hasData;
}

static UNUSED_FUNC void **tscBuildResFromSubqueries(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  
  while (1) {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
    size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
    
    if (pRes->tsrow == NULL) {
      pRes->tsrow = calloc(numOfExprs, POINTER_BYTES);
    }

    bool success = false;

    int32_t numOfTableHasRes = 0;
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] != 0) {
        numOfTableHasRes++;
      }
    }

    if (numOfTableHasRes >= 2) {  // do merge result
      success = (doSetResultRowData(pSql->pSubs[0]) != NULL) && (doSetResultRowData(pSql->pSubs[1]) != NULL);
    } else {  // only one subquery
      SSqlObj *pSub = pSql->pSubs[0];
      if (pSub == NULL) {
        pSub = pSql->pSubs[1];
      }

      success = (doSetResultRowData(pSub) != NULL);
    }

    if (success) {  // current row of final output has been built, return to app
      for (int32_t i = 0; i < numOfExprs; ++i) {
        int32_t tableIndex = pRes->pColumnIndex[i].tableIndex;
        int32_t columnIndex = pRes->pColumnIndex[i].columnIndex;

        SSqlRes *pRes1 = &pSql->pSubs[tableIndex]->res;
        pRes->tsrow[i] = pRes1->tsrow[columnIndex];
      }

      pRes->numOfTotalInCurrentClause++;

      break;
    } else {  // continue retrieve data from vnode
      if (!tscHashRemainDataInSubqueryResultSet(pSql)) {
        tscTrace("%p at least one subquery exhausted, free all other %d subqueries", pSql, pSql->numOfSubs - 1);
        SSubqueryState *pState = NULL;

        // free all sub sqlobj
        for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
          SSqlObj *pChildObj = pSql->pSubs[i];
          if (pChildObj == NULL) {
            continue;
          }

          SJoinSubquerySupporter *pSupporter = (SJoinSubquerySupporter *)pChildObj->param;
          pState = pSupporter->pState;

          tscDestroyJoinSupporter(pChildObj->param);
          taos_free_result(pChildObj);
        }

        free(pState);
        return NULL;
      }

      tscFetchDatablockFromSubquery(pSql);
      if (pRes->code != TSDB_CODE_SUCCESS) {
        return NULL;
      }
    }
  }

  return pRes->tsrow;
}

static void waitForRetrieveRsp(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlObj* pSql = (SSqlObj*) tres;
  
  if (numOfRows < 0) { // set the error code
    pSql->res.code = -numOfRows;
  }
  sem_post(&pSql->rspSem);
}

TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  SSqlObj *pSql = (SSqlObj *)res;
  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_DISCONNECTED;
    return NULL;
  }
  
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->qhandle == 0 ||
      pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      pCmd->command == TSDB_SQL_INSERT) {
    return NULL;
  }
  
  // current data are exhausted, fetch more data
  if (pRes->data == NULL || (pRes->data != NULL && pRes->row >= pRes->numOfRows && pRes->completed != true &&
      (pCmd->command == TSDB_SQL_RETRIEVE || pCmd->command == TSDB_SQL_RETRIEVE_METRIC ||
      pCmd->command == TSDB_SQL_FETCH || pCmd->command == TSDB_SQL_DESCRIBE_TABLE))) {
    taos_fetch_rows_a(res, waitForRetrieveRsp, pSql->pTscObj);
    
    sem_wait(&pSql->rspSem);
  }
  
  return doSetResultRowData(pSql);
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
#if 0
  SSqlObj *pSql = (SSqlObj *)res;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  int nRows = 0;

  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_DISCONNECTED;
    *rows = NULL;
    return 0;
  }

  // projection query on metric, pipeline retrieve data from vnode list,
  // instead of two-stage mergednodeProcessMsgFromShell free qhandle
  nRows = taos_fetch_block_impl(res, rows);

  // current subclause is completed, try the next subclause
  while (rows == NULL && pCmd->clauseIndex < pCmd->numOfClause - 1) {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

    pSql->cmd.command = pQueryInfo->command;
    pCmd->clauseIndex++;

    pRes->numOfTotal += pRes->numOfTotalInCurrentClause;
    pRes->numOfTotalInCurrentClause = 0;
    pRes->rspType = 0;

    pSql->numOfSubs = 0;
    tfree(pSql->pSubs);

    assert(pSql->fp == NULL);

    tscTrace("%p try data in the next subclause:%d, total subclause:%d", pSql, pCmd->clauseIndex, pCmd->numOfClause);
    tscProcessSql(pSql);

    nRows = taos_fetch_block_impl(res, rows);
  }

  return nRows;
#endif

  (*rows) = taos_fetch_row(res);
  return ((*rows) != NULL)? 1:0;
}

int taos_select_db(TAOS *taos, const char *db) {
  char sql[256] = {0};

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  snprintf(sql, tListLen(sql), "use %s", db);
  return taos_query(taos, sql);
}

void taos_free_result_imp(TAOS_RES *res, int keepCmd) {
  if (res == NULL) return;

  SSqlObj *pSql = (SSqlObj *)res;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  tscTrace("%p start to free result", pSql);

  if (pSql->signature != pSql) return;

  if (pRes == NULL || pRes->qhandle == 0) {
    /* Query rsp is not received from vnode, so the qhandle is NULL */
    tscTrace("%p qhandle is null, abort free, fp:%p", pSql, pSql->fp);
    STscObj* pTscObj = pSql->pTscObj;
    
    if (pTscObj->pSql != pSql) {
      tscTrace("%p SqlObj is freed by app", pSql);
      tscFreeSqlObj(pSql);
    } else {
      if (keepCmd) {
        tscFreeSqlResult(pSql);
      } else {
        tscFreeSqlObjPartial(pSql);
      }
    }
    
    return;
  }

  // set freeFlag to 1 in retrieve message if there are un-retrieved results
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  if (pQueryInfo == NULL) {
    tscFreeSqlObjPartial(pSql);
    return;
  }

  pQueryInfo->type = TSDB_QUERY_TYPE_FREE_RESOURCE;

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

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
  if ((pCmd->command == TSDB_SQL_SELECT || pCmd->command == TSDB_SQL_SHOW || pCmd->command == TSDB_SQL_RETRIEVE ||
      pCmd->command == TSDB_SQL_FETCH) &&
      (pRes->code != TSDB_CODE_QUERY_CANCELLED && ((pRes->numOfRows > 0 && pCmd->command < TSDB_SQL_LOCAL && pRes->completed == false) ||
       (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows == 0 && pCmd->command == TSDB_SQL_SELECT &&
        pSql->pStream == NULL && pTableMetaInfo->pTableMeta != NULL)))) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;

    tscTrace("%p code:%d, numOfRows:%d, command:%d", pSql, pRes->code, pRes->numOfRows, pCmd->command);

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
      if (keepCmd) {
        tscFreeSqlResult(pSql);
        tscTrace("%p sql result is freed by app while sql command is kept", pSql);
      } else {
        tscFreeSqlObjPartial(pSql);
        tscTrace("%p sql result is freed by app", pSql);
      }
    } else {  // for async release, remove its link
      STscObj* pObj = pSql->pTscObj;
      if (pObj->pSql == pSql) {
        pObj->pSql = NULL;
      }
    }
  } else {
    // if no free resource msg is sent to vnode, we free this object immediately.
    STscObj* pTscObj = pSql->pTscObj;
    
    if (pTscObj->pSql != pSql) {
      tscFreeSqlObj(pSql);
      tscTrace("%p sql result is freed by app", pSql);
    } else {
      if (keepCmd) {
        tscFreeSqlResult(pSql);
        tscTrace("%p sql result is freed while sql command is kept", pSql);
      } else {
        tscFreeSqlObjPartial(pSql);
        tscTrace("%p sql result is freed by app", pSql);
      }
    }
  }
}

void taos_free_result(TAOS_RES *res) { taos_free_result_imp(res, 0); }

// todo should not be used in async query
int taos_errno(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;

  if (pObj == NULL || pObj->signature != pObj) {
    return terrno;
  }

  return pObj->pSql->res.code;
}

/*
 * In case of invalid sql error, additional information is attached to explain
 * why the sql is invalid
 */
static bool hasAdditionalErrorInfo(int32_t code, SSqlCmd *pCmd) {
  if (code != TSDB_CODE_INVALID_SQL) {
    return false;
  }

  size_t len = strlen(pCmd->payload);

  char *z = NULL;
  if (len > 0) {
    z = strstr(pCmd->payload, "invalid SQL");
  }

  return z != NULL;
}

// todo should not be used in async model
char *taos_errstr(TAOS *taos) {
  STscObj *pObj = (STscObj *)taos;

  if (pObj == NULL || pObj->signature != pObj)
    return (char*)tstrerror(terrno);

  SSqlObj* pSql = pObj->pSql;
  
  if (hasAdditionalErrorInfo(pSql->res.code, &pSql->cmd)) {
    return pSql->cmd.payload;
  } else {
    return (char*)tstrerror(pSql->res.code);
  }
}

void taos_config(int debug, char *log_path) {
  uDebugFlag = debug;
  strcpy(tsLogDir, log_path);
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
  SSqlCmd *pCmd = &pSql->cmd;

  if (pSql->signature != pSql) return;
  tscTrace("%p start to cancel query", res);

  pSql->res.code = TSDB_CODE_QUERY_CANCELLED;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    tscKillSTableQuery(pSql);
    return;
  }

  if (pSql->cmd.command >= TSDB_SQL_LOCAL) {
    return;
  }

  //taosStopRpcConn(pSql->thandle);
  tscTrace("%p query is cancelled", res);
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  int len = 0;
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
        len += sprintf(str + len, "%d", *((char *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d", *((short *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d", *((int *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
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
        size_t xlen = 0;
        for (xlen = 0; xlen <= fields[i].bytes; xlen++) {
          char c = ((char *)row[i])[xlen];
          if (c == 0) break;
          str[len++] = c;
        }
        str[len] = 0;
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

int taos_validate_sql(TAOS *taos, const char *sql) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlObj *pSql = pObj->pSql;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  pRes->numOfRows  = 1;
  pRes->numOfTotal = 0;
  pRes->numOfTotalInCurrentClause = 0;

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

  pCmd->curSql = NULL;
  if (NULL != pCmd->pTableList) {
    taosHashCleanup(pCmd->pTableList);
    pCmd->pTableList = NULL;
  }

  pRes->code = (uint8_t)tsParseSql(pSql, false);
  int code = pRes->code;

  tscTrace("%p Valid SQL result:%d, %s pObj:%p", pSql, pRes->code, taos_errstr(taos), pObj);
  taos_free_result(pSql);

  return code;
}

static int tscParseTblNameList(SSqlObj *pSql, const char *tblNameList, int32_t tblListLen) {
  // must before clean the sqlcmd object
  tscCleanSqlCmd(&pSql->cmd);

  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->command = TSDB_SQL_MULTI_META;
  pCmd->count = 0;

  int   code = TSDB_CODE_INVALID_TABLE_ID;
  char *str = (char *)tblNameList;

  SQueryInfo *pQueryInfo = NULL;
  tscGetQueryInfoDetailSafely(pCmd, pCmd->clauseIndex, &pQueryInfo);

  STableMetaInfo *pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);

  if ((code = tscAllocPayload(pCmd, tblListLen + 16)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  char *nextStr;
  char  tblName[TSDB_TABLE_ID_LEN];
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
      code = TSDB_CODE_INVALID_TABLE_ID;
      sprintf(pCmd->payload, "table name is invalid");
      return code;
    }

    if ((code = tscSetTableId(pTableMetaInfo, &sToken, pSql)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (++pCmd->count > TSDB_MULTI_METERMETA_MAX_NUM) {
      code = TSDB_CODE_INVALID_TABLE_ID;
      sprintf(pCmd->payload, "tables over the max number");
      return code;
    }

    if (payloadLen + strlen(pTableMetaInfo->name) + 128 >= pCmd->allocSize) {
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

    payloadLen += sprintf(pMsg + payloadLen, "%s,", pTableMetaInfo->name);
  }

  *(pMsg + payloadLen) = '\0';
  pCmd->payloadLen = payloadLen + 1;

  return TSDB_CODE_SUCCESS;
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  const int32_t MAX_TABLE_NAME_LENGTH = 12 * 1024 * 1024;  // 12MB list

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlObj *pSql = pObj->pSql;
  SSqlRes *pRes = &pSql->res;

  pRes->numOfTotal = 0;  // the number of getting table meta from server
  pRes->numOfTotalInCurrentClause = 0;

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
