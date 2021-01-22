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
    tscError("%p failed to malloc sql string buffer", pSql);
    pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscAsyncResultOnError(pSql);
    return;
  }

  strntolower(pSql->sqlstr, sqlstr, (int32_t)sqlLen);

  tscDebugL("%p SQL: %s", pSql, pSql->sqlstr);
  pCmd->curSql = pSql->sqlstr;

  int32_t code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) return;
  
  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscAsyncResultOnError(pSql);
    return;
  }

  tscDoQuery(pSql);
}

// TODO return the correct error code to client in tscQueueAsyncError
void taos_query_a(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    tscError("bug!!! pObj:%p", pObj);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_DISCONNECTED);
    return;
  }
  
  int32_t sqlLen = (int32_t)strlen(sqlstr);
  if (sqlLen > tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    tscQueueAsyncError(fp, param, terrno);
    return;
  }
  
  nPrintTsc("%s", sqlstr);
  
  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_OUT_OF_MEMORY);
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
static void tscProcessAsyncRetrieveImpl(void *param, TAOS_RES *tres, int numOfRows, __async_cb_func_t fp) {
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
    tscProcessSql(pSql);
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

void taos_fetch_rows_a(TAOS_RES *taosa, __async_cb_func_t fp, void *param) {
  SSqlObj *pSql = (SSqlObj *)taosa;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("sql object is NULL");
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_DISCONNECTED);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  // user-defined callback function is stored in fetchFp
  pSql->fetchFp = fp;
  pSql->fp = tscAsyncFetchRowsProxy;

  if (pRes->qhandle == 0) {
    tscError("qhandle is NULL");
    pRes->code = TSDB_CODE_TSC_INVALID_QHANDLE;
    pSql->param = param;

    tscAsyncResultOnError(pSql);
    return;
  }

  pSql->param = param;
  tscResetForNextRetrieve(pRes);
  
  // handle the sub queries of join query
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
  
    tscProcessSql(pSql);
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

void tscAsyncResultOnError(SSqlObj *pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    tscDebug("%p SqlObj is freed, not add into queue async res", pSql);
    return;
  }

  assert(pSql->res.code != TSDB_CODE_SUCCESS);
  tscError("%p invoke user specified function due to error occured, code:%s", pSql, tstrerror(pSql->res.code));

  SSqlRes *pRes = &pSql->res;
  if (pSql->fp == NULL || pSql->fetchFp == NULL){
    return;
  }

  pSql->fp = pSql->fetchFp;
  (*pSql->fp)(pSql->param, pSql, pRes->code);
}

int tscSendMsgToServer(SSqlObj *pSql);

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
    tscError("%p get %s failed, code:%s", pSql, msg, tstrerror(code));
    goto _error;
  }

  tscDebug("%p get %s successfully", pSql, msg);
  if (pSql->pStream == NULL) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

    // check if it is a sub-query of super table query first, if true, enter another routine
    if (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, (TSDB_QUERY_TYPE_STABLE_SUBQUERY|TSDB_QUERY_TYPE_SUBQUERY|TSDB_QUERY_TYPE_TAG_FILTER_QUERY))) {
      tscDebug("%p update local table meta, continue to process sql and send the corresponding query", pSql);

      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      code = tscGetTableMeta(pSql, pTableMetaInfo);
      assert(code == TSDB_CODE_TSC_ACTION_IN_PROGRESS || code == TSDB_CODE_SUCCESS);

      if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {        
        taosReleaseRef(tscObjRef, pSql->self);
        return;
      }

      assert((tscGetNumOfTags(pTableMetaInfo->pTableMeta) != 0));

      // tscProcessSql can add error into async res
      tscProcessSql(pSql);
      taosReleaseRef(tscObjRef, pSql->self);
      return;
    } else {  // continue to process normal async query
      if (pCmd->parseFinished) {
        tscDebug("%p update local table meta, continue to process sql and send corresponding query", pSql);

        STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
        code = tscGetTableMeta(pSql, pTableMetaInfo);

        assert(code == TSDB_CODE_TSC_ACTION_IN_PROGRESS || code == TSDB_CODE_SUCCESS);
        if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
          taosReleaseRef(tscObjRef, pSql->self);
          return;
        }

        assert(pCmd->command != TSDB_SQL_INSERT);

        if (pCmd->command == TSDB_SQL_SELECT) {
          tscDebug("%p redo parse sql string and proceed", pSql);
          pCmd->parseFinished = false;
          tscResetSqlCmd(pCmd, true);

          code = tsParseSql(pSql, true);
          if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
            taosReleaseRef(tscObjRef, pSql->self);
            return;
          } else if (code != TSDB_CODE_SUCCESS) {
            goto _error;
          }

          tscProcessSql(pSql);
        } else {  // in all other cases, simple retry
          tscProcessSql(pSql);
        }

        taosReleaseRef(tscObjRef, pSql->self);
        return;
      } else {
        tscDebug("%p continue parse sql after get table meta", pSql);

        code = tsParseSql(pSql, false);
        if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
          taosReleaseRef(tscObjRef, pSql->self);
          return;
        } else if (code != TSDB_CODE_SUCCESS) {
          goto _error;
        }

        if (pCmd->insertType == TSDB_QUERY_TYPE_STMT_INSERT) {
          STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
          code = tscGetTableMeta(pSql, pTableMetaInfo);
          if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
            taosReleaseRef(tscObjRef, pSql->self);
            return;
          } else {
            assert(code == TSDB_CODE_SUCCESS);      
          }

          (*pSql->fp)(pSql->param, pSql, code);
          taosReleaseRef(tscObjRef, pSql->self);
          return;
        }

        // proceed to invoke the tscDoQuery();
      }
    }

  } else {  // stream computing
    STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);

    code = tscGetTableMeta(pSql, pTableMetaInfo);
    if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
      taosReleaseRef(tscObjRef, pSql->self);
      return;
    } else if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      code = tscGetSTableVgroupInfo(pSql, pCmd->clauseIndex);
      if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
        taosReleaseRef(tscObjRef, pSql->self);
        return;
      } else if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
    }

    tscDebug("%p stream:%p meta is updated, start new query, command:%d", pSql, pSql->pStream, pSql->cmd.command);
    if (!pSql->cmd.parseFinished) {
      tsParseSql(pSql, false);
    }

    (*pSql->fp)(pSql->param, pSql, code);
    
    taosReleaseRef(tscObjRef, pSql->self);

    return;
  }

  tscDoQuery(pSql);

  taosReleaseRef(tscObjRef, pSql->self);
  
  return;

  _error:
  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscAsyncResultOnError(pSql);
  }

  taosReleaseRef(tscObjRef, pSql->self);
}
