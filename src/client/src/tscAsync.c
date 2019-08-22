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

#include "tlog.h"
#include "trpc.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tsocket.h"
#include "tsql.h"
#include "tutil.h"

void tscProcessFetchRow(SSchedMsg *pMsg);
void tscProcessAsyncRetrieve(void *param, TAOS_RES *tres, int numOfRows);
static void tscProcessAsyncRetrieveNextVnode(void *param, TAOS_RES *tres, int numOfRows);
static void tscProcessAsyncContinueRetrieve(void *param, TAOS_RES *tres, int numOfRows);

static void tscProcessAsyncRetrieveImpl(void *param, TAOS_RES *tres, int numOfRows, void (*fp)());

/*
 * proxy function to perform sequentially query&retrieve operation.
 * If sql queries upon metric and two-stage merge procedure is not needed,
 * it will sequentially query&retrieve data for all vnodes in pCmd->pMetricMeta
 */
static void tscProcessAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows);

void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *, TAOS_RES *, int), void *param) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    tscError("bug!!! pObj:%p", pObj);
    globalCode = TSDB_CODE_DISCONNECTED;
    tscQueueAsyncError(fp, param);
    return;
  }

  SSqlObj *pSql = (SSqlObj *)malloc(sizeof(SSqlObj));
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    tscQueueAsyncError(fp, param);
    return;
  }

  memset(pSql, 0, sizeof(SSqlObj));
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  pSql->signature = pSql;
  pSql->pTscObj = pObj;
  pSql->fp = fp;
  pSql->param = param;

  tscAllocPayloadWithSize(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE);

  int32_t sqlLen = strlen(sqlstr);
  if (sqlLen > TSDB_MAX_SQL_LEN) {
    tscError("%p sql string too long", pSql);
    tscQueueAsyncError(fp, param);
    return;
  }

  pSql->sqlstr = malloc(sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    tscError("%p failed to malloc sql string buffer", pSql);
    tscQueueAsyncError(fp, param);
    return;
  }

  pRes->qhandle = 0;
  pRes->numOfRows = 1;

  strtolower(pSql->sqlstr, sqlstr);
  tscTrace("%p Async SQL: %s, pObj:%p", pSql, pSql->sqlstr, pObj);

  int32_t code = tsParseSql(pSql, pObj->acctId, pObj->db, true);
  if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;

  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = (uint8_t)code;
    tscQueueAsyncRes(pSql);
    return;
  }

  tscDoQuery(pSql);
}

static void tscProcessAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows) {
  if (tres == NULL) {
    return;
  }

  SSqlObj *pSql = (SSqlObj *)tres;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  // sequentially retrieve data from remain vnodes first, query vnode specified by vnodeIdx
  if (numOfRows == 0 && tscProjectionQueryOnMetric(pSql)) {
    // vnode is denoted by vnodeIdx, continue to query vnode specified by vnodeIdx
    assert(pCmd->vnodeIdx >= 1);

    /* reach the maximum number of output rows, abort */
    if (pCmd->globalLimit > 0 && pRes->numOfTotal >= pCmd->globalLimit) {
      (*pSql->fetchFp)(param, tres, 0);
      return;
    }

    /* update the limit value according to current retrieval results */
    pCmd->limit.limit = pCmd->globalLimit - pRes->numOfTotal;

    if ((++(pSql->cmd.vnodeIdx)) <= pCmd->pMetricMeta->numOfVnodes) {
      pCmd->command = TSDB_SQL_SELECT;  // reset flag to launch query first.

      pRes->row = 0;
      pRes->numOfRows = 0;
      pCmd->type = 0;

      pSql->fp = tscProcessAsyncRetrieveNextVnode;
      tscProcessSql(pSql);
      return;
    }
  } else { // localreducer has handle this situation
    if (pCmd->command != TSDB_SQL_RETRIEVE_METRIC) {
      pRes->numOfTotal += pRes->numOfRows;
    }
  }

  (*pSql->fetchFp)(param, tres, numOfRows);
}

// actual continue retrieve function with user-specified callback function
static void tscProcessAsyncRetrieveImpl(void *param, TAOS_RES *tres, int numOfRows, void (*fp)()) {
  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {  // error
    tscError("sql object is NULL");
    tscQueueAsyncError(pSql->fetchFp, param);
    return;
  }

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pRes->qhandle == 0 || numOfRows != 0) {
    if (pRes->qhandle == 0) {
      tscError("qhandle is NULL");
    } else {
      pRes->code = numOfRows;
    }

    tscQueueAsyncError(pSql->fetchFp, param);
    return;
  }

  pSql->fp = fp;
  if (pCmd->command != TSDB_SQL_RETRIEVE_METRIC && pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }
  tscProcessSql(pSql);
}

/*
 * retrieve callback for fetch rows proxy. It serves as the callback function of querying vnode
 */
static void tscProcessAsyncRetrieveNextVnode(void *param, TAOS_RES *tres, int numOfRows) {
  tscProcessAsyncRetrieveImpl(param, tres, numOfRows, tscProcessAsyncFetchRowsProxy);
}

static void tscProcessAsyncContinueRetrieve(void *param, TAOS_RES *tres, int numOfRows) {
  tscProcessAsyncRetrieveImpl(param, tres, numOfRows, tscProcessAsyncRetrieve);
}

void taos_fetch_rows_a(TAOS_RES *taosa, void (*fp)(void *, TAOS_RES *, int), void *param) {
  SSqlObj *pSql = (SSqlObj *)taosa;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("sql object is NULL");
    globalCode = TSDB_CODE_DISCONNECTED;
    tscQueueAsyncError(fp, param);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (pRes->qhandle == 0) {
    tscError("qhandle is NULL");
    tscQueueAsyncError(fp, param);
    return;
  }

  // user-defined callback function is stored in fetchFp
  pSql->fetchFp = fp;
  pSql->fp = tscProcessAsyncFetchRowsProxy;

  pSql->param = param;

  pRes->row = 0;
  pRes->numOfRows = 0;
  pCmd->type = 0;

  if (pCmd->command != TSDB_SQL_RETRIEVE_METRIC && pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }
  tscProcessSql(pSql);
}

void taos_fetch_row_a(TAOS_RES *taosa, void (*fp)(void *, TAOS_RES *, TAOS_ROW), void *param) {
  SSqlObj *pSql = (SSqlObj *)taosa;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("sql object is NULL");
    globalCode = TSDB_CODE_DISCONNECTED;
    tscQueueAsyncError(fp, param);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (pRes->qhandle == 0) {
    tscError("qhandle is NULL");
    tscQueueAsyncError(fp, param);
    return;
  }

  pSql->fetchFp = fp;
  pSql->param = param;

  if (pRes->row >= pRes->numOfRows) {
    pRes->row = 0;
    pRes->numOfRows = 0;
    pCmd->type = 0;
    pSql->fp = tscProcessAsyncRetrieve;
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
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

void tscProcessAsyncRetrieve(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlObj *pSql = (SSqlObj *)tres;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (numOfRows == 0) {
    // sequentially retrieve data from remain vnodes.
    if (tscProjectionQueryOnMetric(pSql)) {
      /*
       * vnode is denoted by vnodeIdx, continue to query vnode specified by vnodeIdx till all vnode have been retrieved
       */
      assert(pCmd->vnodeIdx >= 1);

      /* reach the maximum number of output rows, abort */
      if (pCmd->globalLimit > 0 && pRes->numOfTotal >= pCmd->globalLimit) {
        (*pSql->fetchFp)(pSql->param, pSql, NULL);
        return;
      }

      /* update the limit value according to current retrieval results */
      pCmd->limit.limit = pCmd->globalLimit - pRes->numOfTotal;

      if ((++pCmd->vnodeIdx) <= pCmd->pMetricMeta->numOfVnodes) {
        pCmd->command = TSDB_SQL_SELECT;  // reset flag to launch query first.

        pRes->row = 0;
        pRes->numOfRows = 0;
        pCmd->type = 0;

        pSql->fp = tscProcessAsyncContinueRetrieve;
        tscProcessSql(pSql);
        return;
      }
    } else {
      (*pSql->fetchFp)(pSql->param, pSql, NULL);
    }
  } else {
    for (int i = 0; i < pCmd->numOfCols; ++i)
      pRes->tsrow[i] = TSC_GET_RESPTR_BASE(pRes, pCmd, i, pCmd->order) + pRes->bytes[i] * pRes->row;
    pRes->row++;

    (*pSql->fetchFp)(pSql->param, pSql, pSql->res.tsrow);
  }
}

void tscProcessFetchRow(SSchedMsg *pMsg) {
  SSqlObj *pSql = (SSqlObj *)pMsg->ahandle;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  for (int i = 0; i < pCmd->numOfCols; ++i)
    pRes->tsrow[i] = TSC_GET_RESPTR_BASE(pRes, pCmd, i, pCmd->order) + pRes->bytes[i] * pRes->row;
  pRes->row++;

  (*pSql->fetchFp)(pSql->param, pSql, pRes->tsrow);
}

void tscProcessAsyncRes(SSchedMsg *pMsg) {
  SSqlObj *pSql = (SSqlObj *)pMsg->ahandle;
  STscObj *pTscObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  void *taosres = pSql;

  // pCmd may be released, so cache pCmd->command
  int cmd = pCmd->command;
  int code = pRes->code ? -pRes->code : pRes->numOfRows;

  if ((tscKeepConn[cmd] == 0 || (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS)) &&
      pSql->pStream == NULL) {
    if (pSql->thandle) taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pTscObj->user);

    pSql->thandle = NULL;
  }

  // in case of async insert, restore the user specified callback function
  bool shouldFree = tscShouldFreeAsyncSqlObj(pSql);

  if (cmd == TSDB_SQL_INSERT) {
    assert(pSql->fp != NULL);
    pSql->fp = pSql->fetchFp;
  }

  (*pSql->fp)(pSql->param, taosres, code);

  if (shouldFree) {
    tscFreeSqlObj(pSql);
    tscTrace("%p Async sql is automatically freed in async res", pSql);
  }
}

void tscProcessAsyncError(SSchedMsg *pMsg) {
  void (*fp)() = pMsg->ahandle;

  (*fp)(pMsg->thandle, NULL, -1);
}

void tscQueueAsyncError(void(*fp), void *param) {
  SSchedMsg schedMsg;
  schedMsg.fp = tscProcessAsyncError;
  schedMsg.ahandle = fp;
  schedMsg.thandle = param;
  schedMsg.msg = NULL;
  taosScheduleTask(tscQhandle, &schedMsg);
}

void tscQueueAsyncRes(SSqlObj *pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    tscTrace("%p SqlObj is freed, not add into queue async res", pSql);
    return;
  } else {
    tscTrace("%p add into queued async res, code:%d", pSql, pSql->res.code);
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

void tscAsyncInsertMultiVnodesProxy(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlObj *pSql = (SSqlObj *)param;
  SSqlCmd *pCmd = &pSql->cmd;
  int32_t  code = TSDB_CODE_SUCCESS;

  assert(!pCmd->isInsertFromFile && pSql->signature == pSql);

  SDataBlockList *pDataBlocks = pCmd->pDataBlocks;
  if (pDataBlocks == NULL || pCmd->vnodeIdx >= pDataBlocks->nSize) {
    // restore user defined fp
    pSql->fp = pSql->fetchFp;
    tscTrace("%p Async insertion completed, destroy data block list", pSql);

    // release data block data
    pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

    // all data has been sent to vnode, call user function
    (*pSql->fp)(pSql->param, tres, numOfRows);
  } else {
    do {
      code = tscCopyDataBlockToPayload(pSql, pDataBlocks->pData[pCmd->vnodeIdx++]);
      if (code != TSDB_CODE_SUCCESS) {
        tscTrace("%p prepare submit data block failed in async insertion, vnodeIdx:%d, total:%d, code:%d",
                 pSql, pCmd->vnodeIdx - 1, pDataBlocks->nSize, code);
      }

    } while (code != TSDB_CODE_SUCCESS && pCmd->vnodeIdx < pDataBlocks->nSize);

    // build submit msg may fail
    if (code == TSDB_CODE_SUCCESS) {
      tscTrace("%p async insertion, vnodeIdx:%d, total:%d", pSql, pCmd->vnodeIdx - 1, pDataBlocks->nSize);
      tscProcessSql(pSql);
    }
  }
}

int tscSendMsgToServer(SSqlObj *pSql);

void tscMeterMetaCallBack(void *param, TAOS_RES *res, int code) {
  SSqlObj *pSql = (SSqlObj *)param;
  if (pSql == NULL || pSql->signature != pSql) return;

  STscObj *pObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pSql->fp == NULL) {
    tscError("%p callBack is NULL!!!", pSql);
    return;
  }

  if (pSql->fp == (void *)1) {
    pSql->fp = NULL;

    if (code != 0) {
      code = abs(code);
      pRes->code = code;
      tscTrace("%p failed to renew meterMeta", pSql);
      sem_post(&pSql->rspSem);
    } else {
      tscTrace("%p renew meterMeta successfully, command:%d, code:%d, thandle:%p, retry:%d",
               pSql, pSql->cmd.command, pSql->res.code, pSql->thandle, pSql->retry);

      assert(pSql->cmd.pMeterMeta == NULL);
      tscGetMeterMeta(pSql, pSql->cmd.name);

      code = tscSendMsgToServer(pSql);
      if (code != 0) {
        pRes->code = code;
        sem_post(&pSql->rspSem);
      }
    }

    return;
  }

  if (code != 0) {
    pRes->code = (uint8_t)abs(code);
    tscQueueAsyncRes(pSql);
    return;
  }

  if (pSql->pStream == NULL) {
    // check if it is a sub-query of metric query first, if true, enter another routine
    // todo refactor
    if (pSql->fp == tscRetrieveDataRes || pSql->fp == tscRetrieveFromVnodeCallBack) {
      assert(pCmd->pMeterMeta->numOfTags != 0 && pCmd->vnodeIdx > 0 && pSql->param != NULL);

      SRetrieveSupport *trs = (SRetrieveSupport *)pSql->param;
      SSqlObj *         pParObj = trs->pParentSqlObj;
      assert(pParObj->signature == pParObj && trs->vnodeIdx == pCmd->vnodeIdx && pSql->cmd.pMeterMeta->numOfTags != 0);
      tscTrace("%p get metricMeta during metric query successfully", pSql);

      code = tscGetMeterMeta(pSql, pSql->cmd.name);
      pRes->code = code;

      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;

      code = tscGetMetricMeta(pSql, pSql->cmd.name);
      pRes->code = code;

      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
    } else {  // normal async query continues
      code = tsParseSql(pSql, pObj->acctId, pObj->db, false);
      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
    }
  } else {  // stream computing
    code = tscGetMeterMeta(pSql, pSql->cmd.name);
    pRes->code = code;

    if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;

    if (code == TSDB_CODE_SUCCESS && UTIL_METER_IS_METRIC(pCmd)) {
      code = tscGetMetricMeta(pSql, pSql->cmd.name);
      pRes->code = code;

      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
    }
  }

  if (code != 0) {
    tscQueueAsyncRes(pSql);
    return;
  }

  if (pSql->pStream) {
    tscTrace("%p stream:%p meta is updated, start new query, command:%d", pSql, pSql->pStream, pSql->cmd.command);
    /*
     * NOTE:
     * transfer the sql function for metric query before get meter/metric meta,
     * since in callback functions, only tscProcessSql(pStream->pSql) is executed!
     */
    tscTansformSQLFunctionForMetricQuery(&pSql->cmd);
    tscIncStreamExecutionCount(pSql->pStream);
  } else {
    tscTrace("%p get meterMeta/metricMeta successfully", pSql);
  }

  tscDoQuery(pSql);
}
