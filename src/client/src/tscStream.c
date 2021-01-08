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

#include <tschemautil.h>
#include "os.h"
#include "taosmsg.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tsched.h"
#include "tcache.h"
#include "tsclient.h"
#include "ttimer.h"
#include "tutil.h"

#include "tscProfile.h"

static void tscProcessStreamQueryCallback(void *param, TAOS_RES *tres, int numOfRows);
static void tscProcessStreamRetrieveResult(void *param, TAOS_RES *res, int numOfRows);
static void tscSetNextLaunchTimer(SSqlStream *pStream, SSqlObj *pSql);
static void tscSetRetryTimer(SSqlStream *pStream, SSqlObj *pSql, int64_t timer);

static int64_t getDelayValueAfterTimewindowClosed(SSqlStream* pStream, int64_t launchDelay) {
  return taosGetTimestamp(pStream->precision) + launchDelay - pStream->stime - 1;
}

static bool isProjectStream(SQueryInfo* pQueryInfo) {
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId != TSDB_FUNC_PRJ) {
      return false;
    }
  }

  return true;
}

static int64_t tscGetRetryDelayTime(SSqlStream* pStream, int64_t slidingTime, int16_t prec) {
  float retryRangeFactor = 0.3f;
  int64_t retryDelta = (int64_t)(tsStreamCompRetryDelay * retryRangeFactor);
  retryDelta = ((rand() % retryDelta) + tsStreamCompRetryDelay) * 1000L;

  if (pStream->interval.intervalUnit != 'n' && pStream->interval.intervalUnit != 'y') {
    // change to ms
    if (prec == TSDB_TIME_PRECISION_MICRO) {
      slidingTime = slidingTime / 1000;
    }

    if (slidingTime < retryDelta) {
      return slidingTime;
    }
  }
  
  return retryDelta;
}

static void doLaunchQuery(void* param, TAOS_RES* tres, int32_t code) {
  SSqlStream *pStream = (SSqlStream *)param;
  assert(pStream->pSql == tres && code == TSDB_CODE_SUCCESS);

  SSqlObj* pSql = (SSqlObj*) tres;
  pSql->fp = doLaunchQuery;
  pSql->fetchFp = doLaunchQuery;
  pSql->res.completed = false;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  code = tscGetTableMeta(pSql, pTableMetaInfo);
  if (code == 0 && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    code = tscGetSTableVgroupInfo(pSql, 0);
  }

  // failed to get table Meta or vgroup list, retry in 10sec.
  if (code == TSDB_CODE_SUCCESS) {
    tscTansformSQLFuncForSTableQuery(pQueryInfo);
    tscDebug("%p stream:%p, start stream query on:%s", pSql, pStream, pTableMetaInfo->name);

    pSql->fp = tscProcessStreamQueryCallback;
    pSql->fetchFp = tscProcessStreamQueryCallback;
    tscDoQuery(pSql);
    tscIncStreamExecutionCount(pStream);
  } else if (code != TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    pSql->res.code = code;
    int64_t retryDelayTime = tscGetRetryDelayTime(pStream, pStream->interval.sliding, pStream->precision);
    tscDebug("%p stream:%p, get table Meta failed, retry in %" PRId64 "ms", pSql, pStream, retryDelayTime);
    tscSetRetryTimer(pStream, pSql, retryDelayTime);
  }
}

static void tscProcessStreamLaunchQuery(SSchedMsg *pMsg) {
  SSqlStream *pStream = (SSqlStream *)pMsg->ahandle;
  doLaunchQuery(pStream, pStream->pSql, 0);
}

static void tscProcessStreamTimer(void *handle, void *tmrId) {
  SSqlStream *pStream = (SSqlStream *)handle;
  if (pStream == NULL || pStream->pTimer != tmrId) {
    return;
  }

  pStream->pTimer = NULL;

  pStream->numOfRes = 0;  // reset the numOfRes.
  SSqlObj *pSql = pStream->pSql;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  tscDebug("%p add into timer", pSql);

  if (pStream->isProject) {
    /*
     * pQueryInfo->window.ekey, which is the start time, does not change in case of
     * repeat first execution, once the first execution failed.
     */
    pQueryInfo->window.skey = pStream->stime;  // start time

    pQueryInfo->window.ekey = taosGetTimestamp(pStream->precision);  // end time
    if (pQueryInfo->window.ekey > pStream->etime) {
      pQueryInfo->window.ekey = pStream->etime;
    }
  } else {
    pQueryInfo->window.skey = pStream->stime;
    int64_t etime = taosGetTimestamp(pStream->precision);
    // delay to wait all data in last time window
    if (pStream->precision == TSDB_TIME_PRECISION_MICRO) {
      etime -= tsMaxStreamComputDelay * 1000l;
    } else {
      etime -= tsMaxStreamComputDelay;
    }
    if (etime > pStream->etime) {
      etime = pStream->etime;
    } else if (pStream->interval.intervalUnit != 'y' && pStream->interval.intervalUnit != 'n') {
      etime = pStream->stime + (etime - pStream->stime) / pStream->interval.interval * pStream->interval.interval;
    } else {
      etime = taosTimeTruncate(etime, &pStream->interval, pStream->precision);
    }
    pQueryInfo->window.ekey = etime;
    if (pQueryInfo->window.skey >= pQueryInfo->window.ekey) {
      int64_t timer = pStream->interval.sliding;
      if (pStream->interval.intervalUnit == 'y' || pStream->interval.intervalUnit == 'n') {
        timer = 86400 * 1000l;
      } else if (pStream->precision == TSDB_TIME_PRECISION_MICRO) {
        timer /= 1000l;
      }
      tscSetRetryTimer(pStream, pSql, timer);
      return;
    }
  }

  // launch stream computing in a new thread
  SSchedMsg schedMsg = { 0 };
  schedMsg.fp = tscProcessStreamLaunchQuery;
  schedMsg.ahandle = pStream;
  schedMsg.thandle = (void *)1;
  schedMsg.msg = NULL;
  taosScheduleTask(tscQhandle, &schedMsg);
}

static void tscProcessStreamQueryCallback(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlStream *pStream = (SSqlStream *)param;
  if (tres == NULL || numOfRows < 0) {
    int64_t retryDelay = tscGetRetryDelayTime(pStream, pStream->interval.sliding, pStream->precision);
    tscError("%p stream:%p, query data failed, code:0x%08x, retry in %" PRId64 "ms", pStream->pSql, pStream, numOfRows,
             retryDelay);

    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pStream->pSql->cmd, 0, 0);

    char* name = pTableMetaInfo->name;
    taosHashRemove(tscTableMetaInfo, name, strnlen(name, TSDB_TABLE_FNAME_LEN));
    pTableMetaInfo->vgroupList = tscVgroupInfoClear(pTableMetaInfo->vgroupList);

    tscSetRetryTimer(pStream, pStream->pSql, retryDelay);
    return;
  }

  taos_fetch_rows_a(tres, tscProcessStreamRetrieveResult, param);
}

// no need to be called as this is alreay done in the query
static void tscStreamFillTimeGap(SSqlStream* pStream, TSKEY ts) {
#if 0
  SSqlObj *   pSql = pStream->pSql;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  if (pQueryInfo->fillType != TSDB_FILL_SET_VALUE && pQueryInfo->fillType != TSDB_FILL_NULL) {
    return;
  }

  SSqlRes *pRes = &pSql->res;
  /* failed to retrieve any result in this retrieve */
  pSql->res.numOfRows = 1;
  void *row[TSDB_MAX_COLUMNS] = {0};
  char  tmpRes[TSDB_MAX_BYTES_PER_ROW] = {0};
  void *oldPtr = pSql->res.data;
  pSql->res.data = tmpRes;
  int32_t rowNum = 0;

  while (pStream->stime + pStream->slidingTime < ts) {
    pStream->stime += pStream->slidingTime;
    *(TSKEY*)row[0] =  pStream->stime;
    for (int32_t i = 1; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
      int16_t     offset = tscFieldInfoGetOffset(pQueryInfo, i);
      TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
      assignVal(pSql->res.data + offset, (char *)(&pQueryInfo->fillVal[i]), pField->bytes, pField->type);
      row[i] = pSql->res.data + offset;
    }
    (*pStream->fp)(pStream->param, pSql, row);
    ++rowNum;
  }

  if (rowNum > 0) {
    tscDebug("%p stream:%p %d rows padded", pSql, pStream, rowNum);
  }

  pRes->numOfRows = 0;
  pRes->data = oldPtr;
#endif
}

static void tscProcessStreamRetrieveResult(void *param, TAOS_RES *res, int numOfRows) {
  SSqlStream *    pStream = (SSqlStream *)param;
  SSqlObj *       pSql = (SSqlObj *)res;

  if (pSql == NULL || numOfRows < 0) {
    int64_t retryDelayTime = tscGetRetryDelayTime(pStream, pStream->interval.sliding, pStream->precision);
    tscError("%p stream:%p, retrieve data failed, code:0x%08x, retry in %" PRId64 "ms", pSql, pStream, numOfRows, retryDelayTime);
  
    tscSetRetryTimer(pStream, pStream->pSql, retryDelayTime);
    return;
  }

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  if (numOfRows > 0) { // when reaching here the first execution of stream computing is successful.
    for(int32_t i = 0; i < numOfRows; ++i) {
      TAOS_ROW row = taos_fetch_row(res);
      if (row != NULL) {
        tscDebug("%p stream:%p fetch result", pSql, pStream);
        tscStreamFillTimeGap(pStream, *(TSKEY*)row[0]);
        pStream->stime = *(TSKEY *)row[0];
        // user callback function
        (*pStream->fp)(pStream->param, res, row);
        pStream->numOfRes++;
      }
    }

    if (!pStream->isProject) {
      pStream->stime = taosTimeAdd(pStream->stime, pStream->interval.sliding, pStream->interval.slidingUnit, pStream->precision);
    }
    // actually only one row is returned. this following is not necessary
    taos_fetch_rows_a(res, tscProcessStreamRetrieveResult, pStream);
  } else {  // numOfRows == 0, all data has been retrieved
    pStream->useconds += pSql->res.useconds;
    if (pStream->numOfRes == 0) {
      if (pStream->isProject) {
        /* no resuls in the query range, retry */
        // todo set retry dynamic time
        int32_t retry = tsProjectExecInterval;
        tscError("%p stream:%p, retrieve no data, code:0x%08x, retry in %" PRId32 "ms", pSql, pStream, numOfRows, retry);

        tscSetRetryTimer(pStream, pStream->pSql, retry);
        return;
      }
    } else if (pStream->isProject) {
      pStream->stime += 1;
    }

    tscDebug("%p stream:%p, query on:%s, fetch result completed, fetched rows:%" PRId64, pSql, pStream, pTableMetaInfo->name,
             pStream->numOfRes);

    tfree(pTableMetaInfo->pTableMeta);

    tscFreeSqlResult(pSql);
    tfree(pSql->pSubs);
    pSql->subState.numOfSub = 0;
    pTableMetaInfo->vgroupList = tscVgroupInfoClear(pTableMetaInfo->vgroupList);
    tscSetNextLaunchTimer(pStream, pSql);
  }
}

static void tscSetRetryTimer(SSqlStream *pStream, SSqlObj *pSql, int64_t timer) {
  int64_t delay = getDelayValueAfterTimewindowClosed(pStream, timer);
  
  if (pStream->isProject) {
    int64_t now = taosGetTimestamp(pStream->precision);
    int64_t etime = now > pStream->etime ? pStream->etime : now;

    if (pStream->etime < now && now - pStream->etime > tsMaxRetentWindow) {
      /*
       * current time window will be closed, since it too early to exceed the maxRetentWindow value
       */
      tscDebug("%p stream:%p, etime:%" PRId64 " is too old, exceeds the max retention time window:%" PRId64 ", stop the stream",
               pStream->pSql, pStream, pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      taos_close_stream(pStream);
      return;
    }
  
    tscDebug("%p stream:%p, next start at %" PRId64 ", in %" PRId64 "ms. delay:%" PRId64 "ms qrange %" PRId64 "-%" PRId64, pStream->pSql, pStream,
             now + timer, timer, delay, pStream->stime, etime);
  } else {
    tscDebug("%p stream:%p, next start at %" PRId64 ", in %" PRId64 "ms. delay:%" PRId64 "ms qrange %" PRId64 "-%" PRId64, pStream->pSql, pStream,
             pStream->stime, timer, delay, pStream->stime - pStream->interval.interval, pStream->stime - 1);
  }

  pSql->cmd.command = TSDB_SQL_SELECT;

  // start timer for next computing
  taosTmrReset(tscProcessStreamTimer, (int32_t)timer, pStream, tscTmr, &pStream->pTimer);
}

static int64_t getLaunchTimeDelay(const SSqlStream* pStream) {
  int64_t maxDelay =
      (pStream->precision == TSDB_TIME_PRECISION_MICRO) ? tsMaxStreamComputDelay * 1000L : tsMaxStreamComputDelay;
  
  int64_t delayDelta = maxDelay;
  if (pStream->interval.intervalUnit != 'n' && pStream->interval.intervalUnit != 'y') {
    delayDelta = (int64_t)(pStream->interval.sliding * tsStreamComputDelayRatio);
    if (delayDelta > maxDelay) {
      delayDelta = maxDelay;
    }
    int64_t remainTimeWindow = pStream->interval.sliding - delayDelta;
    if (maxDelay > remainTimeWindow) {
      maxDelay = (int64_t)(remainTimeWindow / 1.5f);
    }
  }
  
  int64_t currentDelay = (rand() % maxDelay);  // a random number
  currentDelay += delayDelta;
  if (pStream->interval.intervalUnit != 'n' && pStream->interval.intervalUnit != 'y') {
    assert(currentDelay < pStream->interval.sliding);
  }
  
  return currentDelay;
}


static void tscSetNextLaunchTimer(SSqlStream *pStream, SSqlObj *pSql) {
  int64_t timer = 0;
  
  if (pStream->isProject) {
    /*
     * for project query, no mater fetch data successfully or not, next launch will issue
     * more than the sliding time window
     */
    timer = pStream->interval.sliding;
    if (pStream->stime > pStream->etime) {
      tscDebug("%p stream:%p, stime:%" PRId64 " is larger than end time: %" PRId64 ", stop the stream", pStream->pSql, pStream,
               pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      taos_close_stream(pStream);
      return;
    }
  } else {
    int64_t stime = taosTimeTruncate(pStream->stime - 1, &pStream->interval, pStream->precision);
    //int64_t stime = taosGetIntervalStartTimestamp(pStream->stime - 1, pStream->interval.interval, pStream->interval.interval, pStream->interval.intervalUnit, pStream->precision);
    if (stime >= pStream->etime) {
      tscDebug("%p stream:%p, stime:%" PRId64 " is larger than end time: %" PRId64 ", stop the stream", pStream->pSql, pStream,
               pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      taos_close_stream(pStream);
      return;
    }
    
    timer = pStream->stime - taosGetTimestamp(pStream->precision);
    if (timer < 0) {
      timer = 0;
    }
  }

  timer += getLaunchTimeDelay(pStream);
  
  if (pStream->precision == TSDB_TIME_PRECISION_MICRO) {
    timer = timer / 1000L;
  }

  tscSetRetryTimer(pStream, pSql, timer);
}

static int32_t tscSetSlidingWindowInfo(SSqlObj *pSql, SSqlStream *pStream) {
  int64_t minIntervalTime =
      (pStream->precision == TSDB_TIME_PRECISION_MICRO) ? tsMinIntervalTime * 1000L : tsMinIntervalTime;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  if (!pStream->isProject && pQueryInfo->interval.interval == 0) {
    sprintf(pSql->cmd.payload, "the interval value is 0");
    return -1;
  }
  
  if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit!= 'y' && pQueryInfo->interval.interval < minIntervalTime) {
    tscWarn("%p stream:%p, original sample interval:%" PRId64 " too small, reset to:%" PRId64, pSql, pStream,
            (int64_t)pQueryInfo->interval.interval, minIntervalTime);
    pQueryInfo->interval.interval = minIntervalTime;
  }

  pStream->interval.intervalUnit = pQueryInfo->interval.intervalUnit;
  pStream->interval.interval = pQueryInfo->interval.interval;  // it shall be derived from sql string

  if (pQueryInfo->interval.sliding <= 0) {
    pQueryInfo->interval.sliding = pQueryInfo->interval.interval;
    pQueryInfo->interval.slidingUnit = pQueryInfo->interval.intervalUnit;
  }

  int64_t minSlidingTime =
      (pStream->precision == TSDB_TIME_PRECISION_MICRO) ? tsMinSlidingTime * 1000L : tsMinSlidingTime;

  if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit!= 'y' && pQueryInfo->interval.sliding < minSlidingTime) {
    tscWarn("%p stream:%p, original sliding value:%" PRId64 " too small, reset to:%" PRId64, pSql, pStream,
        pQueryInfo->interval.sliding, minSlidingTime);

    pQueryInfo->interval.sliding = minSlidingTime;
  }

  if (pQueryInfo->interval.sliding > pQueryInfo->interval.interval) {
    tscWarn("%p stream:%p, sliding value:%" PRId64 " can not be larger than interval range, reset to:%" PRId64, pSql, pStream,
            pQueryInfo->interval.sliding, pQueryInfo->interval.interval);

    pQueryInfo->interval.sliding = pQueryInfo->interval.interval;
  }

  pStream->interval.slidingUnit = pQueryInfo->interval.slidingUnit;
  pStream->interval.sliding = pQueryInfo->interval.sliding;
  
  if (pStream->isProject) {
    pQueryInfo->interval.interval = 0; // clear the interval value to avoid the force time window split by query processor
    pQueryInfo->interval.sliding = 0;
  }

  return TSDB_CODE_SUCCESS;
}

static int64_t tscGetStreamStartTimestamp(SSqlObj *pSql, SSqlStream *pStream, int64_t stime) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  if (pStream->isProject) {
    // no data in table, flush all data till now to destination meter, 10sec delay
    pStream->interval.interval = tsProjectExecInterval;
    pStream->interval.sliding = tsProjectExecInterval;

    if (stime != 0) {  // first projection start from the latest event timestamp
      assert(stime >= pQueryInfo->window.skey);
      stime += 1;  // exclude the last records from table
    } else {
      stime = pQueryInfo->window.skey;
    }
  } else {             // timewindow based aggregation stream
    if (stime == 0) {  // no data in meter till now
      if (pQueryInfo->window.skey != INT64_MIN) {
        stime = pQueryInfo->window.skey;
      }
      stime = taosTimeTruncate(stime, &pStream->interval, pStream->precision);
    } else {
      int64_t newStime = taosTimeTruncate(stime, &pStream->interval, pStream->precision);
      if (newStime != stime) {
        tscWarn("%p stream:%p, last timestamp:%" PRId64 ", reset to:%" PRId64, pSql, pStream, stime, newStime);
        stime = newStime;
      }
    }
  }

  return stime;
}

static int64_t tscGetLaunchTimestamp(const SSqlStream *pStream) {
  int64_t timer = 0, now = taosGetTimestamp(pStream->precision);
  if (pStream->stime > now) {
    timer = pStream->stime - now;
  }

  int64_t startDelay =
      (pStream->precision == TSDB_TIME_PRECISION_MICRO) ? tsStreamCompStartDelay * 1000L : tsStreamCompStartDelay;
  
  timer += getLaunchTimeDelay(pStream);
  timer += startDelay;
  
  return (pStream->precision == TSDB_TIME_PRECISION_MICRO) ? timer / 1000L : timer;
}

static void tscCreateStream(void *param, TAOS_RES *res, int code) {
  SSqlStream* pStream = (SSqlStream*)param;
  SSqlObj* pSql = pStream->pSql;
  SSqlCmd* pCmd = &pSql->cmd;

  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscError("%p open stream failed, sql:%s, reason:%s, code:%s", pSql, pSql->sqlstr, pCmd->payload, tstrerror(code));

    pStream->fp(pStream->param, NULL, NULL);
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  pStream->isProject = isProjectStream(pQueryInfo);
  pStream->precision = tinfo.precision;

  pStream->ctime = taosGetTimestamp(pStream->precision);
  pStream->etime = pQueryInfo->window.ekey;

  if (tscSetSlidingWindowInfo(pSql, pStream) != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;

    tscError("%p stream %p open failed, since the interval value is incorrect", pSql, pStream);
    pStream->fp(pStream->param, NULL, NULL);
    return;
  }

  pStream->stime = tscGetStreamStartTimestamp(pSql, pStream, pStream->stime);

  int64_t starttime = tscGetLaunchTimestamp(pStream);
  pCmd->command = TSDB_SQL_SELECT;

  registerSqlObj(pSql);
  tscAddIntoStreamList(pStream);

  taosTmrReset(tscProcessStreamTimer, (int32_t)starttime, pStream, tscTmr, &pStream->pTimer);

  tscDebug("%p stream:%p is opened, query on:%s, interval:%" PRId64 ", sliding:%" PRId64 ", first launched in:%" PRId64 ", sql:%s", pSql,
           pStream, pTableMetaInfo->name, pStream->interval.interval, pStream->interval.sliding, starttime, pSql->sqlstr);
}

void tscSetStreamDestTable(SSqlStream* pStream, const char* dstTable) {
  pStream->dstTable = dstTable;
}

TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sqlstr, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                              int64_t stime, void *param, void (*callback)(void *)) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) return NULL;

  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    return NULL;
  }

  pSql->signature = pSql;
  pSql->pTscObj = pObj;

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SSqlStream *pStream = (SSqlStream *)calloc(1, sizeof(SSqlStream));
  if (pStream == NULL) {
    tscError("%p open stream failed, sql:%s, reason:%s, code:0x%08x", pSql, sqlstr, pCmd->payload, pRes->code);
    tscFreeSqlObj(pSql);
    return NULL;
  }

  pStream->stime = stime;
  pStream->fp = fp;
  pStream->callback = callback;
  pStream->param = param;
  pStream->pSql = pSql;
  pSql->pStream = pStream;
  pSql->param = pStream;
  pSql->maxRetry = TSDB_MAX_REPLICA;

  pSql->sqlstr = calloc(1, strlen(sqlstr) + 1);
  if (pSql->sqlstr == NULL) {
    tscError("%p failed to malloc sql string buffer", pSql);
    tscFreeSqlObj(pSql);
    return NULL;
  }

  strtolower(pSql->sqlstr, sqlstr);

  tscDebugL("%p SQL: %s", pSql, pSql->sqlstr);
  tsem_init(&pSql->rspSem, 0, 0);

  pSql->fp = tscCreateStream;
  pSql->fetchFp = tscCreateStream;
  int32_t code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_SUCCESS) {
    tscCreateStream(pStream, pSql, code);
  } else if (code != TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    tscError("%p open stream failed, sql:%s, code:%s", pSql, sqlstr, tstrerror(pRes->code));
    tscFreeSqlObj(pSql);
    free(pStream);
    return NULL;
  }

  return pStream;
}

void taos_close_stream(TAOS_STREAM *handle) {
  SSqlStream *pStream = (SSqlStream *)handle;

  SSqlObj *pSql = (SSqlObj *)atomic_exchange_ptr(&pStream->pSql, 0);
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

    tscDebug("%p stream:%p is closed", pSql, pStream);
    // notify CQ to release the pStream object
    pStream->fp(pStream->param, NULL, NULL);
    pStream->pSql = NULL;

    taos_free_result(pSql);
    tfree(pStream);
  }
}
