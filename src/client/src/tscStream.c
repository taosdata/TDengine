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

#include "tlog.h"
#include "tsql.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#include "tsclient.h"
#include "tscUtil.h"

#include "tscProfile.h"

static void tscProcessStreamQueryCallback(void *param, TAOS_RES *tres, int numOfRows);
static void tscProcessStreamRetrieveResult(void *param, TAOS_RES *res, int numOfRows);
static void tscSetNextLaunchTimer(SSqlStream *pStream, SSqlObj *pSql);
static void tscSetRetryTimer(SSqlStream *pStream, SSqlObj *pSql, int64_t timer);

static bool isProjectStream(SSqlCmd *pCmd) {
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->sqlFuncId != TSDB_FUNC_PRJ) {
      return false;
    }
  }

  return true;
}

static int64_t tscGetRetryDelayTime(int64_t slidingTime) {
  float RETRY_RANGE_FACTOR = 0.3;

  int64_t retryDelta = (int64_t)tsStreamCompRetryDelay * RETRY_RANGE_FACTOR;
  retryDelta = ((rand() % retryDelta) + tsStreamCompRetryDelay) * 1000L;

  if (slidingTime < retryDelta) {
    return slidingTime;
  } else {
    return retryDelta;
  }
}

static void tscProcessStreamLaunchQuery(SSchedMsg *pMsg) {
  SSqlStream *pStream = (SSqlStream *)pMsg->ahandle;
  SSqlObj *   pSql = pStream->pSql;

  pSql->fp = tscProcessStreamQueryCallback;
  pSql->param = pStream;

  int code = tscGetMeterMeta(pSql, pSql->cmd.name);
  pSql->res.code = code;

  if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;

  if (code == 0 && UTIL_METER_IS_METRIC(&pSql->cmd)) {
    code = tscGetMetricMeta(pSql, pSql->cmd.name);
    pSql->res.code = code;

    if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
  }

  tscTansformSQLFunctionForMetricQuery(&pSql->cmd);

  // failed to get meter/metric meta, retry in 10sec.
  if (code != TSDB_CODE_SUCCESS) {
    int64_t retryDelayTime = tscGetRetryDelayTime(pStream->slidingTime);
    tscError("%p stream:%p,get metermeta failed, retry in %ldms.", pStream->pSql, pStream, retryDelayTime);

    tscSetRetryTimer(pStream, pSql, retryDelayTime);
    return;
  }

  tscTrace("%p stream:%p start stream query on:%s", pSql, pStream, pSql->cmd.name);
  tscProcessSql(pStream->pSql);

  tscIncStreamExecutionCount(pStream);
}

static void tscProcessStreamTimer(void *handle, void *tmrId) {
  SSqlStream *pStream = (SSqlStream *)handle;
  if (pStream == NULL) return;
  if (pStream->pTimer != tmrId) return;
  pStream->pTimer = NULL;

  pStream->numOfRes = 0;  // reset the numOfRes.
  SSqlObj *pSql = pStream->pSql;

  if (isProjectStream(&pSql->cmd)) {
    /*
     * pSql->cmd.etime, which is the start time, does not change in case of
     * repeat first execution, once the first execution failed.
     */
    pSql->cmd.stime = pStream->stime;  // start time

    pSql->cmd.etime = taosGetTimestampMs();  // end time
    if (pSql->cmd.etime > pStream->etime) {
      pSql->cmd.etime = pStream->etime;
    }
  } else {
    pSql->cmd.stime = pStream->stime - pStream->interval;
    pSql->cmd.etime = pStream->stime - 1;
  }

  // launch stream computing in a new thread
  SSchedMsg schedMsg;
  schedMsg.fp = tscProcessStreamLaunchQuery;
  schedMsg.ahandle = pStream;
  schedMsg.thandle = (void *)1;
  schedMsg.msg = NULL;
  taosScheduleTask(tscQhandle, &schedMsg);
}

static void tscProcessStreamQueryCallback(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlStream *pStream = (SSqlStream *)param;
  if (tres == NULL || numOfRows < 0) {
    int64_t retryDelay = tscGetRetryDelayTime(pStream->slidingTime);
    tscError("%p stream:%p, query data failed, code:%d, retry in %ldms", pStream->pSql, pStream, numOfRows, retryDelay);

    tscClearSqlMetaInfoForce(&(pStream->pSql->cmd));
    tscSetRetryTimer(pStream, pStream->pSql, retryDelay);
    return;
  }

  taos_fetch_rows_a(tres, tscProcessStreamRetrieveResult, param);
}

static void tscSetTimestampForRes(SSqlStream *pStream, SSqlObj *pSql, int32_t numOfRows) {
  SSqlRes *pRes = &pSql->res;
  int64_t  timestamp = *(int64_t *)pRes->data;

  if (timestamp != pStream->stime) {
    // reset the timestamp of each agg point by using start time of each interval
    *((int64_t *)pRes->data) = pStream->stime - pStream->interval;
    tscWarn("%p stream:%p, timestamp of points is:%lld, reset to %lld", pSql, pStream, timestamp,
            pStream->stime - pStream->interval);
  }
}

static void tscProcessStreamRetrieveResult(void *param, TAOS_RES *res, int numOfRows) {
  SSqlStream *pStream = (SSqlStream *)param;
  SSqlObj *   pSql = (SSqlObj *)res;

  if (pSql == NULL || numOfRows < 0) {
    int64_t retryDelayTime = tscGetRetryDelayTime(pStream->slidingTime);
    tscError("%p stream:%p, retrieve data failed, code:%d, retry in %ldms", pSql, pStream, numOfRows, retryDelayTime);
    tscClearSqlMetaInfoForce(&(pStream->pSql->cmd));

    tscSetRetryTimer(pStream, pStream->pSql, retryDelayTime);
    return;
  }

  if (numOfRows > 0) {  // save
    // when reaching here the first execution of stream computing is successful.
    pStream->numOfRes += numOfRows;
    TAOS_ROW row = NULL;  //;
    while ((row = taos_fetch_row(res)) != NULL) {
      //        char result[512] = {0};
      //        taos_print_row(result, row, pSql->cmd.fieldsInfo.pFields, pSql->cmd.fieldsInfo.numOfOutputCols);
      //        tscPrint("%p stream:%p query result: %s", pSql, pStream, result);
      tscTrace("%p stream:%p fetch result", pSql, pStream);
      if (isProjectStream(&pSql->cmd)) {
        pStream->stime = *(TSKEY *)row[0];
      } else {
        tscSetTimestampForRes(pStream, pSql, numOfRows);
      }

      // user callback function
      (*pStream->fp)(pStream->param, res, row);
    }

    // actually only one row is returned. this following is not necessary
    taos_fetch_rows_a(res, tscProcessStreamRetrieveResult, pStream);
  } else {  // numOfRows == 0, all data has been retrieved
    pStream->useconds += pSql->res.useconds;

    if (pStream->numOfRes == 0) {
      if (pSql->cmd.interpoType == TSDB_INTERPO_SET_VALUE || pSql->cmd.interpoType == TSDB_INTERPO_NULL) {
        SSqlCmd *pCmd = &pSql->cmd;
        SSqlRes *pRes = &pSql->res;

        /* failed to retrieve any result in this retrieve */
        pSql->res.numOfRows = 1;
        void *row[TSDB_MAX_COLUMNS] = {0};
        char  tmpRes[TSDB_MAX_BYTES_PER_ROW] = {0};

        void *oldPtr = pSql->res.data;
        pSql->res.data = tmpRes;

        for (int32_t i = 1; i < pSql->cmd.fieldsInfo.numOfOutputCols; ++i) {
          int16_t     offset = tscFieldInfoGetOffset(pCmd, i);
          TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

          assignVal(pSql->res.data + offset, (char *)(&pCmd->defaultVal[i]), pField->bytes, pField->type);
          row[i] = pSql->res.data + offset;
        }

        tscSetTimestampForRes(pStream, pSql, numOfRows);
        row[0] = pRes->data;

        //            char result[512] = {0};
        //            taos_print_row(result, row, pSql->cmd.fieldsInfo.pFields, pSql->cmd.fieldsInfo.numOfOutputCols);
        //            tscPrint("%p stream:%p query result: %s", pSql, pStream, result);
        tscTrace("%p stream:%p fetch result", pSql, pStream);

        // user callback function
        (*pStream->fp)(pStream->param, res, row);

        pRes->numOfRows = 0;
        pRes->data = oldPtr;
      } else if (isProjectStream(&pSql->cmd)) {
        /* no resuls in the query range, retry */
        // todo set retry dynamic time
        int32_t retry = tsProjectExecInterval;
        tscError("%p stream:%p, retrieve no data, code:%d, retry in %lldms", pSql, pStream, numOfRows, retry);

        tscClearSqlMetaInfoForce(&(pStream->pSql->cmd));
        tscSetRetryTimer(pStream, pStream->pSql, retry);
        return;
      }
    } else {
      if (isProjectStream(&pSql->cmd)) {
        pStream->stime += 1;
      }
    }

    tscTrace("%p stream:%p, query on:%s, fetch result completed, fetched rows:%d.", pSql, pStream, pSql->cmd.name,
             pStream->numOfRes);

    /* release the metric/meter meta information reference, so data in cache can be updated */
    tscClearSqlMetaInfo(&(pSql->cmd));
    tscSetNextLaunchTimer(pStream, pSql);
  }
}

static void tscSetRetryTimer(SSqlStream *pStream, SSqlObj *pSql, int64_t timer) {
  if (isProjectStream(&pSql->cmd)) {
    int64_t now = taosGetTimestampMs();
    int64_t etime = now > pStream->etime ? pStream->etime : now;

    if (pStream->etime < now && now - pStream->etime > tsMaxRetentWindow) {
      /*
       * current time window will be closed, since it too early to exceed the maxRetentWindow value
       */
      tscTrace("%p stream:%p, etime:%lld is too old, exceeds the max retention time window:%lld, stop the stream",
               pStream->pSql, pStream, pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      taos_close_stream(pStream);
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      return;
    }

    tscTrace("%p stream:%p, next query start at %lld, in %lldms. query range %lld-%lld",
             pStream->pSql, pStream, now + timer, timer, pStream->stime, etime);
  } else {
    tscTrace("%p stream:%p, next query start at %lld, in %lldms. query range %lld-%lld",
             pStream->pSql, pStream, pStream->stime, timer, pStream->stime - pStream->interval, pStream->stime - 1);
  }

  pSql->cmd.command = TSDB_SQL_SELECT;

  // start timer for next computing
  taosTmrReset(tscProcessStreamTimer, timer, pStream, tscTmr, &pStream->pTimer);
}

static void tscSetNextLaunchTimer(SSqlStream *pStream, SSqlObj *pSql) {
  int64_t timer = 0;

  if (isProjectStream(&pSql->cmd)) {
    /*
     * for project query, no mater fetch data successfully or not, next launch will issue
     * more than the sliding time window
     */
    timer = pStream->slidingTime;
    if (pStream->stime > pStream->etime) {
      tscTrace("%p stream:%p, stime:%lld is larger than end time: %lld, stop the stream",
               pStream->pSql, pStream, pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      taos_close_stream(pStream);
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      return;
    }
  } else {
    pStream->stime += pStream->slidingTime;
    if ((pStream->stime - pStream->interval) >= pStream->etime) {
      tscTrace("%p stream:%p, stime:%ld is larger than end time: %ld, stop the stream",
               pStream->pSql, pStream, pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      taos_close_stream(pStream);
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      return;
    }

    timer = pStream->stime - taosGetTimestampSec() * 1000L;
    if (timer < 0) {
      timer = 0;
    }
  }

  int64_t delayDelta = (int64_t)(pStream->slidingTime * 0.1);
  delayDelta = (rand() % delayDelta);
  if (delayDelta > tsMaxStreamComputDelay) {
    delayDelta = tsMaxStreamComputDelay;
  }

  timer += delayDelta;  // a random number
  tscSetRetryTimer(pStream, pSql, timer);
}

TAOS_STREAM *taos_open_stream(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                              int64_t stime, void *param, void (*callback)(void *)) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) return NULL;

  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    if (tscEmbedded) {
      tscError("%p server out of memory", pSql);
      pSql->res.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    } else {
      tscError("%p client out of memory", pSql);
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    }

    return NULL;
  }

  pSql->signature = pSql;
  pSql->pTscObj = pObj;
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  tscAllocPayloadWithSize(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE);

  int32_t len = strlen(sqlstr);
  pSql->sqlstr = malloc(strlen(sqlstr) + 1);
  if (pSql->sqlstr == NULL) {
    if (tscEmbedded) {
      tscError("%p server out of memory", pSql);
      pSql->res.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    } else {
      tscError("%p client out of memory", pSql);
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    }

    return NULL;
  }
  strcpy(pSql->sqlstr, sqlstr);
  pSql->sqlstr[len] = 0;

  sem_init(&pSql->rspSem, 0, 0);
  sem_init(&pSql->emptyRspSem, 0, 1);

  SSqlInfo SQLInfo = {0};
  tSQLParse(&SQLInfo, pSql->sqlstr);
  pRes->code = tscToSQLCmd(pSql, &SQLInfo);
  SQLInfoDestroy(&SQLInfo);

  if (pRes->code != 0) {
    tscError("%p open stream failed, sql:%s, reason:%s, code:%d", pSql, sqlstr, pCmd->payload, pRes->code);
    tscFreeSqlObj(pSql);
    return NULL;
  }

  SSqlStream *pStream = (SSqlStream *)calloc(1, sizeof(SSqlStream));
  if (pStream == NULL) return NULL;

  pStream->fp = fp;
  pStream->callback = callback;
  pStream->param = param;
  pStream->pSql = pSql;
  pStream->ctime = taosGetTimestampMs();
  pStream->etime = (pCmd->etime) ? pCmd->etime : INT64_MAX;

  pSql->pStream = pStream;
  tscAddIntoStreamList(pStream);

  if (pCmd->nAggTimeInterval < tsMinIntervalTime) {
    tscWarn("%p stream:%p, original sample interval:%ldms too small, reset to:%ldms", pSql, pStream,
            pCmd->nAggTimeInterval, tsMinIntervalTime);

    pCmd->nAggTimeInterval = tsMinIntervalTime;
  }
  pStream->interval = pCmd->nAggTimeInterval;  // it shall be derived from sql string

  if (pCmd->nSlidingTime == 0) {
    pCmd->nSlidingTime = pCmd->nAggTimeInterval;
  }

  if (pCmd->nSlidingTime < tsMinSlidingTime) {
    tscWarn("%p stream:%p, original sliding value:%lldms too small, reset to:%lldms",
        pSql, pStream, pCmd->nSlidingTime, tsMinSlidingTime);

    pCmd->nSlidingTime = tsMinSlidingTime;
  }

  if (pCmd->nSlidingTime > pCmd->nAggTimeInterval) {
    tscWarn("%p stream:%p, sliding value:%lldms can not be larger than interval range, reset to:%lldms",
        pSql, pStream, pCmd->nSlidingTime, pCmd->nAggTimeInterval);

    pCmd->nSlidingTime = pCmd->nAggTimeInterval;
  }

  pStream->slidingTime = pCmd->nSlidingTime;

  if (isProjectStream(pCmd)) {
    // no data in table, flush all data till now to destination meter, 10sec delay
    pStream->interval = tsProjectExecInterval;
    pStream->slidingTime = tsProjectExecInterval;

    if (stime != 0) {  // first projection start from the latest event timestamp
      assert(stime >= pCmd->etime);
      stime += 1;  // exclude the last records from table
    } else {
      stime = pCmd->etime;
    }
  } else {
    // timewindow based aggregation stream
    if (stime == 0) {  // no data in meter till now
      stime = ((int64_t)taosGetTimestampSec() * 1000L / pStream->interval) * pStream->interval;
      tscWarn("%p stream:%p, last timestamp:0, reset to:%lld", pSql, pStream, stime, stime);
    } else {
      int64_t newStime = (stime / pStream->interval) * pStream->interval;
      if (newStime != stime) {
        tscWarn("%p stream:%p, last timestamp:%lld, reset to:%lld", pSql, pStream, stime, newStime);
        stime = newStime;
      }
    }
  }

  pStream->stime = stime;

  int64_t timer = pStream->stime - taosGetTimestampSec() * 1000L;
  if (timer < 0) timer = 0;

  int64_t delayDelta = (int64_t)(pStream->interval * 0.1);
  if (delayDelta > tsMaxStreamComputDelay) {
    delayDelta = tsMaxStreamComputDelay;
  }

  srand(time(NULL));
  timer += (rand() % delayDelta);  // a random number

  if (timer < tsStreamCompStartDelay || timer > tsMaxStreamComputDelay) {
    timer = (timer % tsStreamCompStartDelay) + tsStreamCompStartDelay;
  }

  taosTmrReset(tscProcessStreamTimer, timer, pStream, tscTmr, &pStream->pTimer);
  tscTrace("%p stream:%p is opened, query on:%s, interval:%ld, sliding:%ld, first launched in:%ld ms, sql:%s",
      pSql, pStream, pSql->cmd.name, pStream->interval, pStream->slidingTime, timer, sqlstr);

  return pStream;
}

void taos_close_stream(TAOS_STREAM *handle) {
  SSqlStream *pStream = (SSqlStream *)handle;

  SSqlObj *pSql = (SSqlObj *)__sync_val_compare_and_swap_64(&pStream->pSql, pStream->pSql, 0);
  if (pSql == NULL) {
    return;
  }

  /*
   * stream may be closed twice, 1. drop dst table, 2. kill stream
   * Here, we need a check before release memory
   */
  if (pSql->signature == pSql) {
    tscRemoveFromStreamList(pStream, pSql);

    taosTmrStopA(&(pStream->pTimer));
    tscFreeSqlObj(pSql);
    pStream->pSql = NULL;

    tscTrace("%p stream:%p is closed", pSql, pStream);
    tfree(pStream);
  }
}
