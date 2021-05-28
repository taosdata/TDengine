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
#include "tutil.h"

#include "tnote.h"
#include "trpc.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tscUtil.h"
#include "tsched.h"
#include "tschemautil.h"
#include "tsclient.h"

static void tscAsyncQueryRowsForNextVnode(void *param, TAOS_RES *tres, int numOfRows);

/*
 * Proxy function to perform sequentially query&retrieve operation.
 * If sql queries upon a super table and two-stage merge procedure is not involved (when employ the projection
 * query), it will sequentially query&retrieve data for all vnodes
 */
static void tscAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows);

void doAsyncQuery(STscObj* pObj, SSqlObj* pSql, __async_cb_func_t fp, void* param, const char* sqlstr, size_t sqlLen) {
  SSqlCmd* pCmd = &pSql->cmd;

  pSql->signature = pSql;
  pSql->param     = param;
  pSql->pTscObj   = pObj;
  pSql->parseRetry= 0;
  pSql->maxRetry  = TSDB_MAX_REPLICA;
  pSql->fp        = fp;
  pSql->fetchFp   = fp;

  registerSqlObj(pSql);

  pSql->sqlstr = calloc(1, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to malloc sql string buffer", pSql->self);
    pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscAsyncResultOnError(pSql);
    return;
  }

  strntolower(pSql->sqlstr, sqlstr, (int32_t)sqlLen);

  tscDebugL("0x%"PRIx64" SQL: %s", pSql->self, pSql->sqlstr);
  pCmd->curSql = pSql->sqlstr;
  pCmd->resColumnId = TSDB_RES_COL_ID;

  int32_t code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) return;
  
  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscAsyncResultOnError(pSql);
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  executeQuery(pSql, pQueryInfo);
}

// TODO return the correct error code to client in tscQueueAsyncError
void taos_query_a(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param) {
  taos_query_ra(taos, sqlstr, fp, param);
}

TAOS_RES * taos_query_ra(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    tscError("pObj:%p is NULL or freed", pObj);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_DISCONNECTED);
    return NULL;
  }
  
  int32_t sqlLen = (int32_t)strlen(sqlstr);
  if (sqlLen > tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    tscQueueAsyncError(fp, param, terrno);
    return NULL;
  }
  
  nPrintTsc("%s", sqlstr);
  
  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_OUT_OF_MEMORY);
    return NULL;
  }
  
  doAsyncQuery(pObj, pSql, fp, param, sqlstr, sqlLen);

  return pSql;
}


static void tscAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows) {
  if (tres == NULL) {
    return;
  }

  SSqlObj *pSql = (SSqlObj *)tres;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (numOfRows == 0) {
    if (hasMoreVnodesToTry(pSql)) { // sequentially retrieve data from remain vnodes.
      tscTryQueryNextVnode(pSql, tscAsyncQueryRowsForNextVnode);
    } else {
      /*
       * all available virtual node has been checked already, now we need to check
       * for the next subclause queries
       */
      if (pCmd->active->sibling != NULL) {
        pCmd->active = pCmd->active->sibling;
        tscTryQueryNextClause(pSql, tscAsyncQueryRowsForNextVnode);
        return;
      }

      /*
       * 1. has reach the limitation
       * 2. no remain virtual nodes to be retrieved anymore
       */
      (*pSql->fetchFp)(param, pSql, 0);
    }
    
    return;
  }
  
  // local merge has handle this situation during super table non-projection query.
  if (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE) {
    pRes->numOfClauseTotal += pRes->numOfRows;
  }

  (*pSql->fetchFp)(param, tres, numOfRows);
}

// actual continue retrieve function with user-specified callback function
static void tscProcessAsyncRetrieveImpl(void *param, TAOS_RES *tres, int numOfRows, __async_cb_func_t fp) {
  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {  // error
    tscError("sql object is NULL");
    return;
  }

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if ((pRes->qId == 0 || numOfRows != 0) && pCmd->command < TSDB_SQL_LOCAL) {
    if (pRes->qId == 0 && numOfRows != 0) {
      tscError("qhandle is NULL");
    } else {
      pRes->code = numOfRows;
    }

    tscAsyncResultOnError(pSql);
    return;
  }

  pSql->fp = fp;
  if (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }

  if (pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscFetchDatablockForSubquery(pSql);
  } else {
    tscBuildAndSendRequest(pSql, NULL);
  }
}

/*
 * retrieve callback for fetch rows proxy.
 * The below two functions both serve as the callback function of query virtual node.
 * query callback first, and then followed by retrieve callback
 */
static void tscAsyncQueryRowsForNextVnode(void *param, TAOS_RES *tres, int numOfRows) {
  // query completed, continue to retrieve
  tscProcessAsyncRetrieveImpl(param, tres, numOfRows, tscAsyncFetchRowsProxy);
}

void taos_fetch_rows_a(TAOS_RES *tres, __async_cb_func_t fp, void *param) {
  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("sql object is NULL");
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_DISCONNECTED);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  // user-defined callback function is stored in fetchFp
  pSql->fetchFp = fp;
  pSql->fp      = tscAsyncFetchRowsProxy;
  pSql->param   = param;

  if (pRes->qId == 0) {
    tscError("qhandle is invalid");
    pRes->code = TSDB_CODE_TSC_INVALID_QHANDLE;
    tscAsyncResultOnError(pSql);
    return;
  }

  tscResetForNextRetrieve(pRes);
  
  // handle the sub queries of join query
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  if (pQueryInfo->pUpstream != NULL && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {
    SSchedMsg schedMsg = {0};
    schedMsg.fp = doRetrieveSubqueryData;
    schedMsg.ahandle = (void *)pSql;
    schedMsg.thandle = (void *)1;
    schedMsg.msg = 0;
    taosScheduleTask(tscQhandle, &schedMsg);
    return;
  }

  if (pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscFetchDatablockForSubquery(pSql);
  } else if (pRes->completed) {
    if(pCmd->command == TSDB_SQL_FETCH || (pCmd->command >= TSDB_SQL_SERV_STATUS && pCmd->command <= TSDB_SQL_CURRENT_USER)) {
      if (hasMoreVnodesToTry(pSql)) {  // sequentially retrieve data from remain vnodes.
        tscTryQueryNextVnode(pSql, tscAsyncQueryRowsForNextVnode);
      } else {
        /*
         * all available virtual nodes in current clause has been checked already, now try the
         * next one in the following union subclause
         */
        if (pCmd->active->sibling != NULL) {
          pCmd->active = pCmd->active->sibling;  // todo refactor
          tscTryQueryNextClause(pSql, tscAsyncQueryRowsForNextVnode);
          return;
        }

        /*
         * 1. has reach the limitation
         * 2. no remain virtual nodes to be retrieved anymore
         */
        (*pSql->fetchFp)(param, pSql, 0);
      }

      return;
    } else if (pCmd->command == TSDB_SQL_RETRIEVE || pCmd->command == TSDB_SQL_RETRIEVE_LOCALMERGE) {
      // in case of show command, return no data
      (*pSql->fetchFp)(param, pSql, 0);
    } else {
      assert(0);
    }
  } else { // current query is not completed, continue retrieve from node
    if (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
      pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    }

    SQueryInfo* pQueryInfo1 = tscGetQueryInfo(&pSql->cmd);
    tscBuildAndSendRequest(pSql, pQueryInfo1);
  }
}

// this function will be executed by queue task threads, so the terrno is not valid
static void tscProcessAsyncError(SSchedMsg *pMsg) {
  void (*fp)() = pMsg->ahandle;
  terrno = *(int32_t*) pMsg->msg;
  tfree(pMsg->msg);
  (*fp)(pMsg->thandle, NULL, terrno);
}

void tscQueueAsyncError(void(*fp), void *param, int32_t code) {
  int32_t* c = malloc(sizeof(int32_t));
  *c = code;
  
  SSchedMsg schedMsg = {0};
  schedMsg.fp = tscProcessAsyncError;
  schedMsg.ahandle = fp;
  schedMsg.thandle = param;
  schedMsg.msg = c;
  taosScheduleTask(tscQhandle, &schedMsg);
}

static void tscAsyncResultCallback(SSchedMsg *pMsg) {
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)pMsg->ahandle);
  if (pSql == NULL || pSql->signature != pSql) {
    tscDebug("%p SqlObj is freed, not add into queue async res", pMsg->ahandle);
    return;
  }

  assert(pSql->res.code != TSDB_CODE_SUCCESS);
  tscError("0x%"PRIx64" async result callback, code:%s", pSql->self, tstrerror(pSql->res.code));

  SSqlRes *pRes = &pSql->res;
  if (pSql->fp == NULL || pSql->fetchFp == NULL){
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }

  pSql->fp = pSql->fetchFp;
  (*pSql->fp)(pSql->param, pSql, pRes->code);
  taosReleaseRef(tscObjRef, pSql->self);
}

void tscAsyncResultOnError(SSqlObj* pSql) {
  SSchedMsg schedMsg = {0};
  schedMsg.fp = tscAsyncResultCallback;
  schedMsg.ahandle = (void *)pSql->self;
  schedMsg.thandle = (void *)1;
  schedMsg.msg = 0;
  taosScheduleTask(tscQhandle, &schedMsg);
}

int tscSendMsgToServer(SSqlObj *pSql);

static int32_t updateMetaBeforeRetryQuery(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo, SQueryInfo* pQueryInfo) {
  // handle the invalid table error code for super table.
  // update the pExpr info, colList info, number of table columns
  // TODO Re-parse this sql and issue the corresponding subquery as an alternative for this case.
  if (pSql->retryReason == TSDB_CODE_TDB_INVALID_TABLE_ID) {
    int32_t numOfExprs = (int32_t) tscNumOfExprs(pQueryInfo);
    int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    int32_t numOfTags = tscGetNumOfTags(pTableMetaInfo->pTableMeta);

    SSchema *pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);
    SSchema *pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);

    for (int32_t i = 0; i < numOfExprs; ++i) {
      SSqlExpr *pExpr = &(tscExprGet(pQueryInfo, i)->base);

      // update the table uid
      pExpr->uid = pTableMetaInfo->pTableMeta->id.uid;

      if (pExpr->colInfo.colIndex >= 0) {
        int32_t index = pExpr->colInfo.colIndex;

        if ((TSDB_COL_IS_NORMAL_COL(pExpr->colInfo.flag) && index >= numOfCols) ||
            (TSDB_COL_IS_TAG(pExpr->colInfo.flag) && (index < 0 || index >= numOfTags))) {
          return pSql->retryReason;
        }

        if (TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
          if ((pTagSchema[pExpr->colInfo.colIndex].colId != pExpr->colInfo.colId) &&
              strcasecmp(pExpr->colInfo.name, pTagSchema[pExpr->colInfo.colIndex].name) != 0) {
            return pSql->retryReason;
          }
        } else if (TSDB_COL_IS_NORMAL_COL(pExpr->colInfo.flag)) {
          if ((pSchema[pExpr->colInfo.colIndex].colId != pExpr->colInfo.colId) &&
              strcasecmp(pExpr->colInfo.name, pSchema[pExpr->colInfo.colIndex].name) != 0) {
            return pSql->retryReason;
          }
        } else { // do nothing for udc
        }
      }
    }

    // validate the table columns information
    for (int32_t i = 0; i < taosArrayGetSize(pQueryInfo->colList); ++i) {
      SColumn *pCol = taosArrayGetP(pQueryInfo->colList, i);
      if (pCol->columnIndex >= numOfCols) {
        return pSql->retryReason;
      }
    }
  } else {
    // do nothing
  }

  return TSDB_CODE_SUCCESS;
}

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code) {
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)param);
  if (pSql == NULL) return;

  assert(pSql->signature == pSql && (int64_t)param == pSql->self);

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  pRes->code = code;

  SSqlObj *sub = (SSqlObj*) res;
  const char* msg = (sub->cmd.command == TSDB_SQL_STABLEVGROUP)? "vgroup-list":"table-meta";
  if (code != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64" get %s failed, code:%s", pSql->self, msg, tstrerror(code));
    goto _error;
  }

  tscDebug("0x%"PRIx64" get %s successfully", pSql->self, msg);
  if (pSql->pStream == NULL) {
    SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

    // check if it is a sub-query of super table query first, if true, enter another routine
    if (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, (TSDB_QUERY_TYPE_STABLE_SUBQUERY | TSDB_QUERY_TYPE_SUBQUERY |
                                               TSDB_QUERY_TYPE_TAG_FILTER_QUERY))) {
      tscDebug("0x%" PRIx64 " update cached table-meta, continue to process sql and send the corresponding query", pSql->self);
      STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

      code = tscGetTableMeta(pSql, pTableMetaInfo);
      assert(code == TSDB_CODE_TSC_ACTION_IN_PROGRESS || code == TSDB_CODE_SUCCESS);

      if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
        taosReleaseRef(tscObjRef, pSql->self);
        return;
      }

      assert((tscGetNumOfTags(pTableMetaInfo->pTableMeta) != 0));
      code = updateMetaBeforeRetryQuery(pSql, pTableMetaInfo, pQueryInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      // tscBuildAndSendRequest can add error into async res
      tscBuildAndSendRequest(pSql, NULL);
      taosReleaseRef(tscObjRef, pSql->self);
      return;
    } else {  // continue to process normal async query
      if (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT)) {
        tscDebug("0x%" PRIx64 " continue parse sql after get table-meta", pSql->self);

        code = tsParseSql(pSql, false);
        if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
          taosReleaseRef(tscObjRef, pSql->self);
          return;
        } else if (code != TSDB_CODE_SUCCESS) {
          goto _error;
        }

        if (TSDB_QUERY_HAS_TYPE(pCmd->insertParam.insertType, TSDB_QUERY_TYPE_STMT_INSERT)) {
          STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
          code = tscGetTableMeta(pSql, pTableMetaInfo);
          if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
            taosReleaseRef(tscObjRef, pSql->self);
            return;
          } else {
            assert(code == TSDB_CODE_SUCCESS);
          }

          (*pSql->fp)(pSql->param, pSql, code);
        } else {
          if (TSDB_QUERY_HAS_TYPE(pCmd->insertParam.insertType, TSDB_QUERY_TYPE_FILE_INSERT)) {
            tscImportDataFromFile(pSql);
          } else {
            tscHandleMultivnodeInsert(pSql);
          }
        }
      } else {
        if (pSql->retryReason != TSDB_CODE_SUCCESS) {
          tscDebug("0x%" PRIx64 " update cached table-meta, re-validate sql statement and send query again",
                   pSql->self);
          tscResetSqlCmd(pCmd, false);
          pSql->retryReason = TSDB_CODE_SUCCESS;
        } else {
          tscDebug("0x%" PRIx64 " cached table-meta, continue validate sql statement and send query", pSql->self);
        }

        code = tsParseSql(pSql, true);
        if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
          taosReleaseRef(tscObjRef, pSql->self);
          return;
        } else if (code != TSDB_CODE_SUCCESS) {
          goto _error;
        }

        SQueryInfo *pQueryInfo1 = tscGetQueryInfo(pCmd);
        executeQuery(pSql, pQueryInfo1);
      }

      taosReleaseRef(tscObjRef, pSql->self);
      return;
    }
  } else {  // stream computing
    tscDebug("0x%"PRIx64" stream:%p meta is updated, start new query, command:%d", pSql->self, pSql->pStream, pCmd->command);

    SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
    if (tscNumOfExprs(pQueryInfo) == 0) {
      tsParseSql(pSql, false);
    }

    (*pSql->fp)(pSql->param, pSql, code);
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }

  taosReleaseRef(tscObjRef, pSql->self);
  return;

  _error:
  pRes->code = code;
  tscAsyncResultOnError(pSql);
  taosReleaseRef(tscObjRef, pSql->self);
}
