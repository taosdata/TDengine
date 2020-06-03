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
#include "tscProfile.h"
#include "tscSubquery.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tsched.h"
#include "tschemautil.h"
#include "tsclient.h"

static void tscProcessFetchRow(SSchedMsg *pMsg);
static void tscAsyncQueryRowsForNextVnode(void *param, TAOS_RES *tres, int numOfRows);

static void tscProcessAsyncRetrieveImpl(void *param, TAOS_RES *tres, int numOfRows, void (*fp)());

/*
 * Proxy function to perform sequentially query&retrieve operation.
 * If sql queries upon a super table and two-stage merge procedure is not involved (when employ the projection
 * query), it will sequentially query&retrieve data for all vnodes
 */
static void tscAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows);
static void tscAsyncFetchSingleRowProxy(void *param, TAOS_RES *tres, int numOfRows);

int doAsyncParseSql(SSqlObj* pSql, const char* sqlstr, size_t sqlLen) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  int32_t code = tscAllocPayload(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("failed to malloc payload");
    tscQueueAsyncError(pSql->fp, pSql->param, TSDB_CODE_CLI_OUT_OF_MEMORY);
    return code;
  }
  
  // todo check for OOM problem
  pSql->sqlstr = calloc(1, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    tscError("%p failed to malloc sql string buffer", pSql);
    tscQueueAsyncError(pSql->fp, pSql->param, TSDB_CODE_CLI_OUT_OF_MEMORY);
    free(pCmd->payload);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }
  
  pRes->qhandle = 0;
  pRes->numOfRows = 1;
  
  strtolower(pSql->sqlstr, sqlstr);
  tscDump("%p SQL: %s", pSql, pSql->sqlstr);
  
  return tsParseSql(pSql, true);
}

void doAsyncQuery(STscObj* pObj, SSqlObj* pSql, void (*fp)(), void* param, const char* sqlstr, size_t sqlLen) {
  pSql->signature = pSql;
  pSql->param     = param;
  pSql->pTscObj   = pObj;
  pSql->maxRetry  = TSDB_MAX_REPLICA_NUM;
  pSql->fp        = fp;
  
  int32_t code = doAsyncParseSql(pSql, sqlstr, sqlLen);
  if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
  
  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscQueueAsyncRes(pSql);
    return;
  }
  
  tscDoQuery(pSql);
}

// TODO return the correct error code to client in tscQueueAsyncError
void taos_query_a(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    tscError("bug!!! pObj:%p", pObj);
    terrno = TSDB_CODE_DISCONNECTED;
    tscQueueAsyncError(fp, param, TSDB_CODE_DISCONNECTED);
    return;
  }
  
  int32_t sqlLen = strlen(sqlstr);
  if (sqlLen > tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_INVALID_SQL;
    tscQueueAsyncError(fp, param, TSDB_CODE_INVALID_SQL);
    return;
  }
  
  taosNotePrintTsc(sqlstr);
  
  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    terrno = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscQueueAsyncError(fp, param, TSDB_CODE_CLI_OUT_OF_MEMORY);
    return;
  }
  
  doAsyncQuery(pObj, pSql, fp, param, sqlstr, sqlLen);
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
      if (pCmd->clauseIndex < pCmd->numOfClause - 1) {
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
static void tscProcessAsyncRetrieveImpl(void *param, TAOS_RES *tres, int numOfRows, void (*fp)()) {
  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {  // error
    tscError("sql object is NULL");
    return;
  }

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if ((pRes->qhandle == 0 || numOfRows != 0) && pCmd->command < TSDB_SQL_LOCAL) {
    if (pRes->qhandle == 0 && numOfRows != 0) {
      tscError("qhandle is NULL");
    } else {
      pRes->code = numOfRows;
    }

    tscQueueAsyncError(pSql->fetchFp, param, pRes->code);
    return;
  }

  pSql->fp = fp;
  if (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }
  tscProcessSql(pSql);
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

void tscAsyncQuerySingleRowForNextVnode(void *param, TAOS_RES *tres, int numOfRows) {
  // query completed, continue to retrieve
  tscProcessAsyncRetrieveImpl(param, tres, numOfRows, tscAsyncFetchSingleRowProxy);
}

void taos_fetch_rows_a(TAOS_RES *taosa, void (*fp)(void *, TAOS_RES *, int), void *param) {
  SSqlObj *pSql = (SSqlObj *)taosa;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("sql object is NULL");
    tscQueueAsyncError(fp, param, TSDB_CODE_DISCONNECTED);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (pRes->qhandle == 0) {
    tscError("qhandle is NULL");
    tscQueueAsyncError(fp, param, TSDB_CODE_INVALID_QHANDLE);
    return;
  }

  // user-defined callback function is stored in fetchFp
  pSql->fetchFp = fp;
  pSql->fp = tscAsyncFetchRowsProxy;

  pSql->param = param;
  tscResetForNextRetrieve(pRes);
  
  // handle the sub queries of join query
  if (pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscFetchDatablockFromSubquery(pSql);
  } else if (pRes->completed && pCmd->command == TSDB_SQL_FETCH) {
    if (hasMoreVnodesToTry(pSql)) { // sequentially retrieve data from remain vnodes.
      tscTryQueryNextVnode(pSql, tscAsyncQueryRowsForNextVnode);
      return;
    } else {
      /*
       * all available virtual node has been checked already, now we need to check
       * for the next subclause queries
       */
      if (pCmd->clauseIndex < pCmd->numOfClause - 1) {
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
  } else { // current query is not completed, continue retrieve from node
    if (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
      pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    }
  
    tscProcessSql(pSql);
  }
}

void taos_fetch_row_a(TAOS_RES *taosa, void (*fp)(void *, TAOS_RES *, TAOS_ROW), void *param) {
  SSqlObj *pSql = (SSqlObj *)taosa;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("sql object is NULL");
    tscQueueAsyncError(fp, param, TSDB_CODE_DISCONNECTED);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (pRes->qhandle == 0) {
    tscError("qhandle is NULL");
    tscQueueAsyncError(fp, param, TSDB_CODE_INVALID_QHANDLE);
    return;
  }

  pSql->fetchFp = fp;
  pSql->param = param;
  
  if (pRes->row >= pRes->numOfRows) {
    tscResetForNextRetrieve(pRes);
    pSql->fp = tscAsyncFetchSingleRowProxy;
    
    if (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
      pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    }
    
    tscProcessSql(pSql);
  } else {
    SSchedMsg schedMsg;
    schedMsg.fp = tscProcessFetchRow;
    schedMsg.ahandle = pSql;
    schedMsg.thandle = pRes->tsrow;
    schedMsg.msg = NULL;
    taosScheduleTask(tscQhandle, &schedMsg);
  }
}

void tscAsyncFetchSingleRowProxy(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlObj *pSql = (SSqlObj *)tres;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  if (numOfRows == 0) {
    if (hasMoreVnodesToTry(pSql)) {     // sequentially retrieve data from remain vnodes.
      tscTryQueryNextVnode(pSql, tscAsyncQuerySingleRowForNextVnode);
    } else {
      /*
       * 1. has reach the limitation
       * 2. no remain virtual nodes to be retrieved anymore
       */
      (*pSql->fetchFp)(pSql->param, pSql, NULL);
    }
    return;
  }
  
  for (int i = 0; i < pCmd->numOfCols; ++i){
    SFieldSupInfo* pSup = taosArrayGet(pQueryInfo->fieldsInfo.pSupportInfo, i);
    if (pSup->pSqlExpr != NULL) {
//      pRes->tsrow[i] = TSC_GET_RESPTR_BASE(pRes, pQueryInfo, i) + pSup->pSqlExpr->resBytes * pRes->row;
    } else {
      //todo add
    }
  }
  
  pRes->row++;

  (*pSql->fetchFp)(pSql->param, pSql, pSql->res.tsrow);
}

void tscProcessFetchRow(SSchedMsg *pMsg) {
  SSqlObj *pSql = (SSqlObj *)pMsg->ahandle;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  for (int i = 0; i < pCmd->numOfCols; ++i) {
    SFieldSupInfo* pSup = taosArrayGet(pQueryInfo->fieldsInfo.pSupportInfo, i);

    if (pSup->pSqlExpr != NULL) {
      tscGetResultColumnChr(pRes, &pQueryInfo->fieldsInfo, i);
    } else {
//      todo add
    }
  }
  
  pRes->row++;
  (*pSql->fetchFp)(pSql->param, pSql, pRes->tsrow);
}

void tscProcessAsyncRes(SSchedMsg *pMsg) {
  SSqlObj *pSql = (SSqlObj *)pMsg->ahandle;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  void *taosres = pSql;

  // pCmd may be released, so cache pCmd->command
  int cmd = pCmd->command;
  int code = pRes->code;

  // in case of async insert, restore the user specified callback function
  bool shouldFree = tscShouldBeFreed(pSql);

  if (cmd == TSDB_SQL_INSERT) {
    assert(pSql->fp != NULL);
    pSql->fp = pSql->fetchFp;
  }

  (*pSql->fp)(pSql->param, taosres, code);

  if (shouldFree) {
    tscTrace("%p sqlObj is automatically freed in async res", pSql);
    tscFreeSqlObj(pSql);
  }
}

static void tscProcessAsyncError(SSchedMsg *pMsg) {
  void (*fp)() = pMsg->ahandle;
  (*fp)(pMsg->thandle, NULL, *(int32_t*)pMsg->msg);
}

void tscQueueAsyncError(void(*fp), void *param, int32_t code) {
  int32_t* c = malloc(sizeof(int32_t));
  *c = code;
  
  SSchedMsg schedMsg;
  schedMsg.fp = tscProcessAsyncError;
  schedMsg.ahandle = fp;
  schedMsg.thandle = param;
  schedMsg.msg = c;
  taosScheduleTask(tscQhandle, &schedMsg);
}

void tscQueueAsyncRes(SSqlObj *pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    tscTrace("%p SqlObj is freed, not add into queue async res", pSql);
    return;
  } else {
    tscError("%p add into queued async res, code:%s", pSql, tstrerror(pSql->res.code));
  }

  SSchedMsg schedMsg;
  schedMsg.fp = tscProcessAsyncRes;
  schedMsg.ahandle = pSql;
  schedMsg.thandle = (void *)1;
  schedMsg.msg = NULL;
  taosScheduleTask(tscQhandle, &schedMsg);
}

void tscProcessAsyncFree(SSchedMsg *pMsg) {
  SSqlObj *pSql = (SSqlObj *)pMsg->ahandle;
  tscTrace("%p sql is freed", pSql);
  taos_free_result(pSql);
}

void tscQueueAsyncFreeResult(SSqlObj *pSql) {
  tscTrace("%p sqlObj put in queue to async free", pSql);

  SSchedMsg schedMsg;
  schedMsg.fp = tscProcessAsyncFree;
  schedMsg.ahandle = pSql;
  schedMsg.thandle = (void *)1;
  schedMsg.msg = NULL;
  taosScheduleTask(tscQhandle, &schedMsg);
}

int tscSendMsgToServer(SSqlObj *pSql);

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code) {
  SSqlObj *pSql = (SSqlObj *)param;
  if (pSql == NULL || pSql->signature != pSql) return;

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (code != TSDB_CODE_SUCCESS) {
    pRes->code = code;
    tscQueueAsyncRes(pSql);
    return;
  }

  if (pSql->pStream == NULL) {
    // check if it is a sub-query of super table query first, if true, enter another routine
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
    if ((pQueryInfo->type & TSDB_QUERY_TYPE_STABLE_SUBQUERY) == TSDB_QUERY_TYPE_STABLE_SUBQUERY) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      if (pTableMetaInfo->pTableMeta == NULL){
        code = tscGetTableMeta(pSql, pTableMetaInfo);
        assert(code == TSDB_CODE_SUCCESS);      
      }     
      
      assert((tscGetNumOfTags(pTableMetaInfo->pTableMeta) != 0) && pTableMetaInfo->vgroupIndex >= 0 && pSql->param != NULL);

      SRetrieveSupport *trs = (SRetrieveSupport *)pSql->param;
      SSqlObj *         pParObj = trs->pParentSqlObj;
      
      assert(pParObj->signature == pParObj && trs->subqueryIndex == pTableMetaInfo->vgroupIndex &&
          tscGetNumOfTags(pTableMetaInfo->pTableMeta) != 0);

      tscTrace("%p get metricMeta during super table query successfully", pSql);      

      code = tscGetSTableVgroupInfo(pSql, 0);
      pRes->code = code;

      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
    } else {  // normal async query continues
      if (pCmd->parseFinished) {
        tscTrace("%p re-send data to vnode in table Meta callback since sql parsed completed", pSql);
        
        STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
        code = tscGetTableMeta(pSql, pTableMetaInfo);
        assert(code == TSDB_CODE_SUCCESS);
      
        if (pTableMetaInfo->pTableMeta) {
          // todo update the submit message according to the new table meta
          // 1. table uid, 2. ip address
          code = tscSendMsgToServer(pSql);
          if (code == TSDB_CODE_SUCCESS) return;
        }
      } else {
        code = tsParseSql(pSql, false);
        if ((pQueryInfo->type & TSDB_QUERY_TYPE_STMT_INSERT) == TSDB_QUERY_TYPE_STMT_INSERT) {
          STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
          code = tscGetTableMeta(pSql, pTableMetaInfo);
          assert(code == TSDB_CODE_SUCCESS && pTableMetaInfo->pTableMeta != NULL);
          (*pSql->fp)(pSql->param, pSql, code);
          return;
        }
        
        if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
      }
    }

  } else {  // stream computing
    STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
    code = tscGetTableMeta(pSql, pTableMetaInfo);
    pRes->code = code;

    if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;

    if (code == TSDB_CODE_SUCCESS && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      code = tscGetSTableVgroupInfo(pSql, pCmd->clauseIndex);
      pRes->code = code;

      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscQueueAsyncRes(pSql);
    return;
  }

  if (pSql->pStream) {
    tscTrace("%p stream:%p meta is updated, start new query, command:%d", pSql, pSql->pStream, pSql->cmd.command);
    /*
     * NOTE:
     * transfer the sql function for super table query before get meter/metric meta,
     * since in callback functions, only tscProcessSql(pStream->pSql) is executed!
     */
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
    
    tscTansformSQLFuncForSTableQuery(pQueryInfo);
    tscIncStreamExecutionCount(pSql->pStream);
  } else {
    tscTrace("%p get tableMeta successfully", pSql);
  }

  tscDoQuery(pSql);
}
