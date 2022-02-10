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
#include "taosmsg.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tsched.h"
#include "tcache.h"
#include "tsclient.h"
#include "ttimer.h"
#include "tutil.h"

#include "tscProfile.h"
#include "tscSubquery.h"

static void tscProcessStreamQueryCallback(void *param, TAOS_RES *tres, int numOfRows);
static void tscProcessStreamRetrieveResult(void *param, TAOS_RES *res, int numOfRows);
static void tscSetNextLaunchTimer(SSqlStream *pStream, SSqlObj *pSql);
static void tscSetRetryTimer(SSqlStream *pStream, SSqlObj *pSql, int64_t timer);
static int64_t getLaunchTimeDelay(const SSqlStream* pStream);

static int64_t getDelayValueAfterTimewindowClosed(SSqlStream* pStream, int64_t launchDelay) {
  return taosGetTimestamp(pStream->precision) + launchDelay - pStream->stime - 1;
}

static bool isProjectStream(SQueryInfo* pQueryInfo) {
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SExprInfo *pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId != TSDB_FUNC_PRJ) {
      return false;
    }
  }

  return true;
}

static int64_t tscGetRetryDelayTime(SSqlStream* pStream, int64_t slidingTime, int16_t prec) {
  float retryRangeFactor = 0.3f;
  int64_t retryDelta = (int64_t)(tsRetryStreamCompDelay * retryRangeFactor);
  retryDelta = (rand() % retryDelta) + tsRetryStreamCompDelay;

  if (pStream->interval.intervalUnit != 'n' && pStream->interval.intervalUnit != 'y') {
    // change to ms
    slidingTime = convertTimePrecision(slidingTime, pStream->precision, TSDB_TIME_PRECISION_MILLI);

    if (slidingTime < retryDelta) {
      return slidingTime;
    }
  }
  
  return retryDelta;
}

static void setRetryInfo(SSqlStream* pStream, int32_t code) {
  SSqlObj* pSql = pStream->pSql;

  pSql->res.code = code;
  int64_t retryDelayTime = tscGetRetryDelayTime(pStream, pStream->interval.sliding, pStream->precision);
  tscDebug("0x%"PRIx64" stream:%p, get table Meta failed, retry in %" PRId64 "ms", pSql->self, pStream, retryDelayTime);
  tscSetRetryTimer(pStream, pSql, retryDelayTime);
}

static void doLaunchQuery(void* param, TAOS_RES* tres, int32_t code) {
  SSqlStream *pStream = (SSqlStream *)param;
  assert(pStream->pSql == tres);

  SSqlObj* pSql = (SSqlObj*) tres;

  pSql->fp      = doLaunchQuery;
  pSql->fetchFp = doLaunchQuery;
  pSql->res.completed = false;

  if (code != TSDB_CODE_SUCCESS) {
    setRetryInfo(pStream, code);
    return;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  code = tscGetTableMeta(pSql, pTableMetaInfo);
  if (code == 0 && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    code = tscGetSTableVgroupInfo(pSql, pQueryInfo);
  }

  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    return;
  }

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) && (pTableMetaInfo->pVgroupTables == NULL) && (pTableMetaInfo->vgroupList == NULL || pTableMetaInfo->vgroupList->numOfVgroups <= 0)) {
    tscDebug("0x%"PRIx64" empty vgroup list", pSql->self);
    pTableMetaInfo->vgroupList = tscVgroupInfoClear(pTableMetaInfo->vgroupList);
    code = TSDB_CODE_TSC_APP_ERROR;
  }

  // failed to get table Meta or vgroup list, retry in 10sec.
  if (code == TSDB_CODE_SUCCESS) {
    tscTansformFuncForSTableQuery(pQueryInfo);

    tscDebug("0x%"PRIx64" stream:%p, start stream query on:%s QueryInfo->skey=%"PRId64" ekey=%"PRId64" ", pSql->self, pStream, tNameGetTableName(&pTableMetaInfo->name), pQueryInfo->window.skey, pQueryInfo->window.ekey);

    pQueryInfo->command = TSDB_SQL_SELECT;

    pSql->fp = tscProcessStreamQueryCallback;
    pSql->fetchFp = tscProcessStreamQueryCallback;
    executeQuery(pSql, pQueryInfo);
    tscIncStreamExecutionCount(pStream);
  } else {
    setRetryInfo(pStream, code);
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

  // pSql ==  NULL  maybe killStream already called
  if(pSql == NULL) {
    return ;
  }
  SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  tscDebug("0x%"PRIx64" add into timer", pSql->self);

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
    int64_t one = convertTimePrecision(1, TSDB_TIME_PRECISION_MILLI, pStream->precision);
    // delay to wait all data in last time window
    etime -= convertTimePrecision(tsMaxStreamComputDelay, TSDB_TIME_PRECISION_MILLI, pStream->precision);
    if (etime > pStream->etime) {
      etime = pStream->etime;
    } else if (pStream->interval.intervalUnit != 'y' && pStream->interval.intervalUnit != 'n') {
      if(pStream->stime == INT64_MIN) {
        etime = taosTimeTruncate(etime, &pStream->interval, pStream->precision) - one;
      } else {
        etime = pStream->stime + (etime - pStream->stime) / pStream->interval.interval * pStream->interval.interval - one;
      }
    } else {
      etime = taosTimeTruncate(etime, &pStream->interval, pStream->precision) - one;
    }
    pQueryInfo->window.ekey = etime;
    if (pQueryInfo->window.skey >= pQueryInfo->window.ekey) {
      int64_t timer = pStream->interval.sliding;
      if (pStream->interval.intervalUnit == 'y' || pStream->interval.intervalUnit == 'n') {
        timer = 86400 * 1000l;
      } else {
        int32_t loop = 10000;
        int64_t next_time = pStream->stime; 
        while(1) {
          // get next time
          next_time = taosTimeAdd(next_time, pStream->interval.sliding, pStream->interval.intervalUnit, TSDB_TIME_PRECISION_MILLI);
          timer = next_time - taosGetTimestamp(pStream->precision); // next time - now()
          if(timer < 0 && --loop > 0 ) {
            //tscDebug("CQ next time < now so loop add sliding. next_time=%" PRId64, next_time);
            continue;
          }

          // calc launch delay time
          int64_t delay = getLaunchTimeDelay((const SSqlStream*)pStream);
          timer += delay;
          tscDebug("CQ execute next query after %" PRId64 "ms (delay=%" PRId64 ")", timer, delay);
          break;
        }  
      }
      tscSetRetryTimer(pStream, pSql, timer);
      return;
    }
  }

  tscDebug("CQ ProcessStreamTimer skey=%" PRId64 " ekey=%" PRId64 " stime=%" PRId64 " etime=%" PRId64, pQueryInfo->window.skey, pQueryInfo->window.ekey, pStream->stime, pStream->etime);
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
    tscError("0x%"PRIx64" stream:%p, query data failed, code:0x%08x, retry in %" PRId64 "ms", pStream->pSql->self,
        pStream, numOfRows, retryDelay);

    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pStream->pSql->cmd, 0);

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    tNameExtractFullName(&pTableMetaInfo->name, name);

    taosHashRemove(tscTableMetaMap, name, strnlen(name, TSDB_TABLE_FNAME_LEN));

    tfree(pTableMetaInfo->pTableMeta);

    tscFreeSqlResult(pStream->pSql);
    tscFreeSubobj(pStream->pSql);
    tfree(pStream->pSql->pSubs);
    pStream->pSql->subState.numOfSub = 0;

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
  SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  
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
    tscDebug("0x%"PRIx64" stream:%p %d rows padded", pSql, pStream, rowNum);
  }

  pRes->numOfRows = 0;
  pRes->data = oldPtr;
#endif
}

// callback send values
int32_t ok_cnt = 0;
int32_t err_cnt = 0;
void cbSendValues(void *param, TAOS_RES *res, int code) {
  if(code < 0) {
    err_cnt ++;
    tscError("CQ Send Failed. code=0x%x ok_cnt=%d err_cnt=%d", code, ok_cnt,err_cnt);
  } else {
    ok_cnt ++;
    tscInfo("CQ Send OK. row=%d ok_cnt=%d err_cnt=%d", code, ok_cnt, err_cnt);
  }
}

// append values
size_t appendValues(TAOS_FIELD* fields, int32_t numCols, TAOS_ROW row, char* pBuf, size_t bufLen, size_t curLen, bool *full) {
  // calc buf is full
  size_t needLen = 0;
  size_t rowLen  = 0;
  int i;
  for(i = 0; i < numCols; i++) {
    needLen += fields[i].bytes;
  }

  // estimate length for use
  needLen += numCols * 5 + 20;
  if(needLen >= bufLen - curLen) {
    *full = true;
    return 0;
  }

  // row append to values
  char* strRow = tmalloc(needLen); 
  char *value = pBuf + curLen;

  strcpy(value, "(");
  rowLen += taos_print_row_ex(strRow, row, fields, numCols, ',', true);
  strcat(value, strRow);
  strcat(value, ")");
  rowLen +=2;

  tfree(strRow);
  return rowLen;
}

bool sqlBufSend(TAOS *taos, char *sqlBuf) {
  // if no enough free session, wait max 10s
  int32_t sleepCnt = 0;
  do {
    int32_t session = taos_unused_session(taos);
    if(session > 1000) {
      break;
    }
    taosMsleep(500);
    tscInfo("CQ session < 1000. session=%d Wait 0.5s cnt=%d", session, sleepCnt);
  } while(++sleepCnt < 20);

  strcat(sqlBuf, ";");
  taos_query_ra(taos, sqlBuf, cbSendValues, NULL);
  return true;
}

#define STR_SQL_INSERT "insert into "
// send one table all rows for once
bool sendChildTalbe(TAOS *taos, char *superName, char *tableName, TAOS_FIELD *fields, int32_t numCols, 
                    SArray *arr, char* sqlBuf, int32_t bufLen) {

  char dbName[TSDB_DB_NAME_LEN] = "";
  char dbTable[TSDB_TABLE_FNAME_LEN];
  size_t numRows = taosArrayGetSize(arr);
  if(numRows == 0)
    return false;
  // obtain dbname
  char * p = strstr(superName, ".");
  if(p) { // if have db prefix , under this db create table
    int32_t len = (int32_t)(p - superName);
    strncpy(dbName, superName, len);
    dbName[len] = 0; // append str end
    sprintf(dbTable, "%s.%s", dbName, tableName);
  } else {
    // no db prefix
    strcpy(dbTable, tableName);
  }

  // first enter
  if(sqlBuf[0] == 0) {
    strcpy(sqlBuf, STR_SQL_INSERT);
  } else { // check need send
   if( bufLen - strlen(sqlBuf) < 300) {
     sqlBufSend(taos, sqlBuf);
     strcpy(sqlBuf, STR_SQL_INSERT);
   }
  }

  // init
  int32_t preLen = (int32_t)strlen(sqlBuf);
  char *subBuf = sqlBuf + preLen;
  int32_t subLen = bufLen - preLen; 
  sprintf(subBuf, " %s using %s tags(0) values ", dbTable, superName);
  size_t curLen = strlen(subBuf);

  TAOS_ROW row ;
  bool full = false;

  for(size_t i = 0; i < numRows; i++) {
    row = (TAOS_ROW)taosArrayGetP(arr, i);
    if(row == NULL)
      continue;
    if(subLen > 200)
       curLen += appendValues(fields, numCols, row, subBuf, subLen - 100, curLen, &full);
    else
      full = true;
    if(full) { // need send
      // send current
      sqlBufSend(taos, sqlBuf);

      // init reset
      strcpy(sqlBuf, STR_SQL_INSERT);
      preLen = (int32_t)strlen(sqlBuf);
      subBuf = sqlBuf + preLen;
      subLen = bufLen - preLen; 
      sprintf(subBuf, " %s using %s tags(0) values ", dbTable, superName);
      curLen = strlen(subBuf);

      // retry append. if full is true again, ignore this row
      curLen += appendValues(fields, numCols, row, subBuf, subLen - 100, curLen, &full);
      full = false; // reset to false
    }
    tfree(row);
  }

  return true;
}

// write cq result to another table
bool toAnotherTable(STscObj *pTscObj, char *superName, TAOS_FIELD *fields, int32_t numCols, SHashObj *tbHash, int32_t numRows) {
  
  int32_t bufLen = TSDB_MAX_SQL_LEN/2 - 128;
  char * sqlBuf = tmalloc(bufLen);
  sqlBuf[0] = 0; // init
  
  ok_cnt = 0;
  err_cnt = 0;
  int cnt_table = 0;
  void *pIter = taosHashIterate(tbHash, NULL);
  while(pIter) {
    SArray *arr = *(SArray**)pIter;
    if(arr) {
      // get key as tableName
      SHashNode *pNode = (SHashNode *)GET_HASH_PNODE(pIter);
      char *data = (char *)GET_HASH_NODE_KEY(pNode);
      uint32_t len = pNode->keyLen;
      char *key = tmalloc(len + 1);
      memcpy(key, data, len);
      key[len] = 0; // string end '\0'

      // send all this table rows
      sendChildTalbe(pTscObj, superName, key, fields, numCols, arr, sqlBuf, bufLen);
      // release SArray
      taosArrayDestroy(arr);
      tfree(key);
      cnt_table ++;
    }
    pIter = taosHashIterate(tbHash, pIter);
  }

  if(sqlBuf[0]) {
    sqlBufSend(pTscObj, sqlBuf);
  }

  tscInfo("CQ ===== stream %d rows write to %d tables ===== \n", numRows, cnt_table);
  tfree(sqlBuf);

  return true;
}

// add row to hash to group by tbname
bool tbHashAdd(SHashObj *tbHash, TAOS_ROW row, TAOS_FIELD* fields, int32_t idx, int32_t numCols) {
  void *v = row[idx];
  TAOS_FIELD *field = &fields[idx];
  VarDataLenT len = 0;
  char str[128];
  memset(str, 0, sizeof(str));
  char *key = str;
  
  // get key and len
  if(field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_NCHAR) {
    key = v;
    len = varDataLen((char*)v - VARSTR_HEADER_SIZE);
  } else {
    len = taos_print_field(str, v, field);
  }
  if(len == 0) {
    return false;
  }

  // append key with len
  SArray *arr = NULL;
  void* pdata = taosHashGet(tbHash, key, len);
  if(pdata) {
    arr = *(SArray **)pdata;
  }
  // if group is null create new
  if(arr == NULL) {
    arr = (SArray *)taosArrayInit(10, sizeof(TAOS_ROW));
    if(arr == NULL) {
      tscError("tbHashAdd tbHash:%p, taosArrayInit(10,sizeof(TAOS_ROW) return NULL.", tbHash);
      return false;
    }
    taosHashPut(tbHash, key, len, &arr, sizeof(SArray *));
  }

  // append to group
  int32_t new_len = sizeof(void*) * numCols;
  TAOS_ROW new_row = (TAOS_ROW)tmalloc(new_len);
  memcpy(new_row, row, new_len);
  taosArrayPush(arr, &new_row);
  return true;
}

static void tscProcessStreamRetrieveResult(void *param, TAOS_RES *res, int numOfRows) {
  SSqlStream *    pStream = (SSqlStream *)param;
  SSqlObj *       pSql = (SSqlObj *)res;
  bool toAnother = pStream->to != NULL;

  if (pSql == NULL || numOfRows < 0) {
    int64_t retryDelayTime = tscGetRetryDelayTime(pStream, pStream->interval.sliding, pStream->precision);
    tscError("stream:%p, retrieve data failed, code:0x%08x, retry in %" PRId64 " ms", pStream, numOfRows, retryDelayTime);
    tscSetRetryTimer(pStream, pStream->pSql, retryDelayTime);
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  STableMetaInfo *pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];

  if (numOfRows > 0) { // when reaching here the first execution of stream computing is successful.
    // init hash
    SHashObj* tbHash = NULL;
    int32_t colIdx = -1;
    TAOS_FIELD *fields = NULL;
    int32_t dstColsNum = pStream->dstCols;
    int32_t fieldsNum = 0;

    if(toAnother) {
      //init hash
      tbHash = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
      fields = taos_fetch_fields(res);
      fieldsNum = tscNumOfFields(pQueryInfo);
      if(dstColsNum == -1) 
         dstColsNum = fieldsNum;
      //search split column
      char *split = "tbname"; // default
      if(pStream->split)
         split = pStream->split;
      for(int32_t i = 1; i < fieldsNum; i++ ) {
        if(strcasecmp(fields[i].name, split) == 0 ) {
          colIdx = i;
          break;
        }
      }

      // set default with last fields if
      if(colIdx == -1) { 
        colIdx = fieldsNum - 1; 
      }
    }

    // save rows
    for(int32_t i = 0; i < numOfRows; ++i) {
      TAOS_ROW row = taos_fetch_row(res);
      if (row != NULL) {
        tscDebug("0x%"PRIx64" stream:%p fetch result row=%d", pSql->self, pStream, i);
        tscStreamFillTimeGap(pStream, *(TSKEY*)row[0]);
        pStream->stime = *(TSKEY *)row[0];
        // write to another table if true
        if(toAnother) {
          tbHashAdd(tbHash, row, fields, colIdx, dstColsNum);
          if(i == numOfRows - 1) //write last row to record last query time avoid query from begin for each
            (*pStream->fp)(pStream->param, res, row); 
        } else {
          (*pStream->fp)(pStream->param, res, row);
        }
        pStream->numOfRes++;
      }
    }
    
    // write Another
    if(toAnother) {
      toAnotherTable(pSql->pTscObj, pStream->to, fields, dstColsNum, tbHash, numOfRows);
      taosHashCleanup(tbHash);
    }
      
    if (!pStream->isProject) {
      pStream->stime = taosTimeAdd(pStream->stime, pStream->interval.sliding, pStream->interval.slidingUnit, pStream->precision);
    }
    int32_t code = TSDB_CODE_SUCCESS;
    if(pQueryInfo && pQueryInfo->pQInfo)
      code = pQueryInfo->pQInfo->code;
    // actually only one row is returned. this following is not necessary
    if(code == TSDB_CODE_SUCCESS) { 
      taos_fetch_rows_a(res, tscProcessStreamRetrieveResult, pStream);
    }
  } else {  // numOfRows == 0, all data has been retrieved
    pStream->useconds += pSql->res.useconds;
    if (pStream->numOfRes == 0) {
      if (pStream->isProject) {
        /* no resuls in the query range, retry */
        // todo set retry dynamic time
        int32_t retry = tsProjectExecInterval;
        tscError("0x%"PRIx64" stream:%p, retrieve no data, code:0x%08x, retry in %" PRId32 "ms", pSql->self, pStream, numOfRows, retry);
        tscSetRetryTimer(pStream, pStream->pSql, retry);
        return;
      }
    } else if (pStream->isProject) {
      pStream->stime += 1;
    }

    tscDebug("0x%"PRIx64" stream:%p, query on:%s, fetch result completed, fetched rows:%" PRId64, pSql->self, pStream, tNameGetTableName(&pTableMetaInfo->name),
             pStream->numOfRes);

    tfree(pTableMetaInfo->pTableMeta);
    if (pQueryInfo->pQInfo != NULL) {
      qDestroyQueryInfo(pQueryInfo->pQInfo);
      pQueryInfo->pQInfo = NULL;
    }

    tscFreeSqlResult(pSql);
    tscFreeSubobj(pSql);    
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
    int64_t maxRetent = tsMaxRetentWindow * 1000;
    if(pStream->precision == TSDB_TIME_PRECISION_MICRO) {
      maxRetent *= 1000;
    }
         
    if (pStream->etime < now && now - pStream->etime > maxRetent) {
      /*
       * current time window will be closed, since it too early to exceed the maxRetentWindow value
       */
      tscDebug("0x%"PRIx64" stream:%p, etime:%" PRId64 " is too old, exceeds the max retention time window:%" PRId64 ", stop the stream",
               pStream->pSql->self, pStream, pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      taos_close_stream(pStream);
      return;
    }
  
    tscDebug("0x%"PRIx64" stream:%p, next start at %" PRId64 "(ts window ekey), in %" PRId64 " ms. delay:%" PRId64 "ms qrange %" PRId64 "-%" PRId64, pStream->pSql->self, pStream,
             now + timer, timer, delay, pStream->stime, etime);
  } else {
    tscDebug("0x%"PRIx64" stream:%p, next start at %" PRId64 " - %" PRId64 " end, in %" PRId64 "ms. delay:%" PRId64 "ms qrange %" PRId64 "-%" PRId64, pStream->pSql->self, pStream,
             pStream->stime, pStream->etime, timer, delay, pStream->stime - pStream->interval.interval, pStream->stime - 1);
  }

  pSql->cmd.command = TSDB_SQL_SELECT;

  // start timer for next computing
  taosTmrReset(tscProcessStreamTimer, (int32_t)timer, pStream, tscTmr, &pStream->pTimer);
}

// get need delay time for every launch to exeucte query, include first and next launch 
static int64_t getLaunchTimeDelay(const SSqlStream* pStream) {
  // step 1 read setting delay time in taos.cfg
  int64_t maxDelay = convertTimePrecision(tsMaxStreamComputDelay, TSDB_TIME_PRECISION_MILLI, pStream->precision);
  int64_t ratioDelay = maxDelay;
  if (pStream->interval.intervalUnit != 'n' && pStream->interval.intervalUnit != 'y') {
    ratioDelay= (int64_t)(pStream->interval.sliding * tsStreamComputDelayRatio);
    if (ratioDelay > maxDelay) {
      ratioDelay = maxDelay;
    }
    int64_t remainTimeWindow = pStream->interval.sliding - ratioDelay;
    if (maxDelay > remainTimeWindow) {
      maxDelay = (int64_t)(remainTimeWindow / 1.5f);
    }
  }
  
  // PART 2 calc allDelay = rand delay + fixed delay
  int64_t allDelay = (rand() % maxDelay) + ratioDelay;
  if (pStream->interval.intervalUnit != 'n' && pStream->interval.intervalUnit != 'y') {
    if(allDelay >= pStream->interval.sliding) {
      tscWarn("CQ delay >= sliding error. delay=%" PRId64 " sliding=%" PRId64 ". so set delay=sliding/2.", allDelay, pStream->interval.sliding);
      allDelay = pStream->interval.sliding / 2;
    }
  }

  tscDebug("getLanchDelay allDelay=%" PRId64 "(ratioDelay=%" PRId64 ")", allDelay, ratioDelay);
  return allDelay;
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
      tscDebug("0x%"PRIx64" stream:%p, stime:%" PRId64 " is larger than end time: %" PRId64 ", stop the stream",
          pStream->pSql->self, pStream, pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      taos_close_stream(pStream);
      return;
    }
  } else {
    int64_t tsc_stime = taosTimeTruncate(pStream->stime - 1, &pStream->interval, pStream->precision);
    if (tsc_stime >= pStream->etime) {
      tscDebug("0x%"PRIx64" stream:%p, stime:%" PRId64 " is larger than end time: %" PRId64 ", stop the stream", pStream->pSql->self, pStream,
               pStream->stime, pStream->etime);
      // TODO : How to terminate stream here
      if (pStream->callback) {
        // Callback function from upper level
        pStream->callback(pStream->param);
      }
      taos_close_stream(pStream);
      return;
    }

    if (pStream->stime > 0) {
      timer = pStream->stime - taosGetTimestamp(pStream->precision);
      if (timer < 0) {
        timer = 0;
      }
    }
  }

  timer += getLaunchTimeDelay(pStream);
  
  timer = convertTimePrecision(timer, pStream->precision, TSDB_TIME_PRECISION_MILLI);
  tscSetRetryTimer(pStream, pSql, timer);
}

static int32_t tscSetSlidingWindowInfo(SSqlObj *pSql, SSqlStream *pStream) {
  int64_t minIntervalTime =
      convertTimePrecision(tsMinIntervalTime, TSDB_TIME_PRECISION_MILLI, pStream->precision);
  
  SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);

  if (!pStream->isProject && pQueryInfo->interval.interval == 0) {
    sprintf(pSql->cmd.payload, "the interval value is 0");
    return -1;
  }
  
  if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit!= 'y' && pQueryInfo->interval.interval < minIntervalTime) {
    tscWarn("0x%"PRIx64" stream:%p, original sample interval:%" PRId64 " too small, reset to:%" PRId64, pSql->self, pStream,
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
      convertTimePrecision(tsMinSlidingTime, TSDB_TIME_PRECISION_MILLI, pStream->precision);

  if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit!= 'y' && pQueryInfo->interval.sliding < minSlidingTime) {
    tscWarn("0x%"PRIx64" stream:%p, original sliding value:%" PRId64 " too small, reset to:%" PRId64, pSql->self, pStream,
        pQueryInfo->interval.sliding, minSlidingTime);

    pQueryInfo->interval.sliding = minSlidingTime;
  }

  if (pQueryInfo->interval.sliding > pQueryInfo->interval.interval) {
    tscWarn("0x%"PRIx64" stream:%p, sliding value:%" PRId64 " can not be larger than interval range, reset to:%" PRId64, pSql->self, pStream,
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

static int64_t tscGetStreamStartTimestamp(SSqlObj *pSql, SSqlStream *pStream, int64_t tsc_stime) {
  SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  
  if (pStream->isProject) {
    // no data in table, flush all data till now to destination meter, 10sec delay
    pStream->interval.interval = tsProjectExecInterval;
    pStream->interval.sliding = tsProjectExecInterval;

    if (tsc_stime != INT64_MIN) {  // first projection start from the latest event timestamp
      assert(tsc_stime >= pQueryInfo->window.skey);
      tsc_stime += 1;  // exclude the last records from table
    } else {
      tsc_stime = pQueryInfo->window.skey;
    }
  } else {             // timewindow based aggregation stream
    if (tsc_stime == INT64_MIN) {  // no data in meter till now
      if (pQueryInfo->window.skey != INT64_MIN) {
        tsc_stime = pQueryInfo->window.skey;
      } else {
        return tsc_stime;
      }

      tsc_stime = taosTimeTruncate(tsc_stime, &pStream->interval, pStream->precision);
    } else {
      int64_t newStime = taosTimeTruncate(tsc_stime, &pStream->interval, pStream->precision);
      if (newStime != tsc_stime) {
        tscWarn("0x%"PRIx64" stream:%p, last timestamp:%" PRId64 ", reset to:%" PRId64, pSql->self, pStream, tsc_stime, newStime);
        tsc_stime = newStime;
      }
    }
  }

  return tsc_stime;
}

static int64_t tscGetFirstLaunchTime(const SSqlStream *pStream) {
  // PART 1 now to stime span
  int64_t timer = 0, now = taosGetTimestamp(pStream->precision);
  if (pStream->stime > now) {
    timer = pStream->stime - now;
  }

  // PART 2 stream first Launch need delay, setting with taos.cfg 
  timer += convertTimePrecision(tsFirstLaunchDelay, TSDB_TIME_PRECISION_MILLI, pStream->precision);

  // PART 3 every launch need delay, include first and next launch 
  timer += getLaunchTimeDelay(pStream);
  
  return convertTimePrecision(timer, pStream->precision, TSDB_TIME_PRECISION_MILLI);
}

static void tscCreateStream(void *param, TAOS_RES *res, int code) {
  SSqlStream* pStream = (SSqlStream*)param;
  SSqlObj* pSql = pStream->pSql;
  SSqlCmd* pCmd = &pSql->cmd;

  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscError("0x%"PRIx64" open stream failed, sql:%s, reason:%s, code:%s", pSql->self, pSql->sqlstr, pCmd->payload, tstrerror(code));

    pStream->fp(pStream->param, NULL, NULL);
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  pStream->isProject = isProjectStream(pQueryInfo);
  pStream->precision = tinfo.precision;

  pStream->ctime = taosGetTimestamp(pStream->precision);
  pStream->etime = pQueryInfo->window.ekey;

  if (tscSetSlidingWindowInfo(pSql, pStream) != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;

    tscError("0x%"PRIx64" stream %p open failed, since the interval value is incorrect", pSql->self, pStream);
    pStream->fp(pStream->param, NULL, NULL);
    return;
  }

  pStream->stime = tscGetStreamStartTimestamp(pSql, pStream, pStream->stime);

  // set stime with ltime if ltime > stime
  const char* dstTable = pStream->dstTable? pStream->dstTable: "";
  tscDebug(" CQ table=%s ltime is %"PRId64, dstTable, pStream->ltime);
  if(pStream->ltime != INT64_MIN && pStream->ltime > pStream->stime) {
    tscWarn(" CQ set stream %s stime=%"PRId64" replace with ltime=%"PRId64" if ltime>0  ", dstTable, pStream->stime, pStream->ltime);
    pStream->stime = pStream->ltime;
  }

  int64_t starttime = tscGetFirstLaunchTime(pStream);
  pCmd->command = TSDB_SQL_SELECT;

  tscAddIntoStreamList(pStream);

  taosTmrReset(tscProcessStreamTimer, (int32_t)starttime, pStream, tscTmr, &pStream->pTimer);

  tscDebug("0x%"PRIx64" stream:%p is opened, query on:%s, interval:%" PRId64 ", sliding:%" PRId64 ", first launched in:%" PRId64 ", sql:%s", pSql->self,
           pStream, tNameGetTableName(&pTableMetaInfo->name), pStream->interval.interval, pStream->interval.sliding, starttime, pSql->sqlstr);
}

void tscSetStreamDestTable(SSqlStream* pStream, const char* dstTable) {
  pStream->dstTable = dstTable;
}

// fetchFp call back
void fetchFpStreamLastRow(void* param ,TAOS_RES* res, int num) {
  SSqlStream* pStream = (SSqlStream*)param;
  SSqlObj* pSql = res;
  
  // get row data set to ltime
  tscSetSqlOwner(pSql);
  TAOS_ROW row = doSetResultRowData(pSql);
  if( row && row[0] ) {
    pStream->ltime = *((int64_t*)row[0]);
    const char* dstTable = pStream->dstTable? pStream->dstTable: "";
    tscDebug(" CQ stream table=%s last row time=%"PRId64" .", dstTable, pStream->ltime);
  }
  tscClearSqlOwner(pSql);

  // no condition call 
  tscCreateStream(param, pStream->pSql, TSDB_CODE_SUCCESS);
  taos_free_result(res);
}

//  fp callback 
void fpStreamLastRow(void* param ,TAOS_RES* res, int code) {
  // check result successful
  if (code != TSDB_CODE_SUCCESS) {
    tscCreateStream(param, res, TSDB_CODE_SUCCESS);
    taos_free_result(res);
    return ;
  }

  // asynchronous fetch last row data
  taos_fetch_rows_a(res, fetchFpStreamLastRow, param);
}

void cbParseSql(void* param, TAOS_RES* res, int code) {
  // check result successful
  SSqlStream* pStream = (SSqlStream*)param;
  SSqlObj* pSql = pStream->pSql;
  SSqlCmd* pCmd = &pSql->cmd;
  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscDebug("0x%"PRIx64" open stream parse sql failed, sql:%s, reason:%s, code:%s", pSql->self, pSql->sqlstr, pCmd->payload, tstrerror(code));
    pStream->fp(pStream->param, NULL, NULL);
    return;
  }

  // check dstTable valid
  if(pStream->dstTable == NULL || strlen(pStream->dstTable) == 0) {
    tscDebug(" cbParseSql dstTable is empty.");
    tscCreateStream(param, res, code);
    return ;
  }

  // query stream last row time async
  char sql[128] = "";
  sprintf(sql, "select last_row(*) from %s;", pStream->dstTable); 
  taos_query_a(pSql->pTscObj, sql, fpStreamLastRow, param);
  return ;
}

void splitStreamSql(const char *str, char **sql, char **to, char **split) {
  // OLD FORMAT only sql  
  if(strncmp(str, LABEL_SQL, LABEL_SQL_LEN) != 0) {
    *sql = tmalloc(strlen(str) + 1);
    if(*sql == NULL)
       return ;
    strcpy(*sql, str);
    return ;
  }

  // NEW FORMAT sql:...to:...split:...
  char *p1 = strstr(str + LABEL_SQL_LEN, LABEL_TO);
  if(p1 == NULL) {
    char *p = (char *)str + LABEL_SQL_LEN;
    *sql = (char *)tmalloc(strlen(p) + 1);
    strcpy(*sql, p);
    return ;
  }
  // SQL value
  int32_t len = (int32_t)(p1 - str - LABEL_SQL_LEN);
  *sql = (char *)tmalloc(len + 1);
  strncpy(*sql, str + LABEL_SQL_LEN, len);
  (*sql)[len] = 0;  // str end 

  // TO value
  char *p2 = strstr(p1 + LABEL_TO_LEN, LABEL_SPLIT);
  if(p2 == NULL) {
    char *p = p1 + LABEL_TO_LEN;
    *to = (char *)tmalloc(strlen(p) + 1);
    strcpy(*to, p);
    return ;
  }
  len = (int32_t)(p2 - p1 - LABEL_TO_LEN);
  *to = (char *)tmalloc(len + 1);
  strncpy(*to, p1 + LABEL_TO_LEN, len);
  (*to)[len] = 0;  // str end 

  // SPLIT value
  char *p = p2 + LABEL_SPLIT_LEN; 
  *split = (char *)tmalloc(strlen(p) + 1);
  strcpy(*split, p);
}

TAOS_STREAM *taos_open_stream_withname(TAOS *taos, const char* dstTable, int32_t dstCols, const char *sqlstr, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                              int64_t tsc_stime, void *param, void (*callback)(void *), void* cqhandle) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) return NULL;

  if(fp == NULL){
    tscError(" taos_open_stream api fp param must not NULL.");
    return NULL;
  }

  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    return NULL;
  }

  pSql->signature = pSql;
  pSql->pTscObj = pObj;
  pSql->rootObj = pSql;

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  SSqlStream *pStream = (SSqlStream *)calloc(1, sizeof(SSqlStream));
  if (pStream == NULL) {
    tscError("0x%"PRIx64" open stream failed, sql:%s, reason:%s, code:0x%08x", pSql->self, sqlstr, pCmd->payload, pRes->code);
    tscFreeSqlObj(pSql);
    return NULL;
  }

  pStream->ltime = INT64_MIN;
  pStream->stime = tsc_stime;
  pStream->fp = fp;
  pStream->callback = callback;
  pStream->param = param;
  pStream->pSql = pSql;
  pStream->cqhandle = cqhandle;
  pStream->dstCols = dstCols;
  pStream->to = NULL;
  pStream->split = NULL; 
  pSql->pStream = pStream;
  pSql->param = pStream;
  pSql->maxRetry = TSDB_MAX_REPLICA;
  tscSetStreamDestTable(pStream, dstTable);

  pSql->pStream  = pStream;
  pSql->param    = pStream;
  pSql->maxRetry = TSDB_MAX_REPLICA;
  
  // split stream sqlstr to sql,to,split
  splitStreamSql(sqlstr, &pSql->sqlstr, &pStream->to, &pStream->split);
  if (pSql->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to malloc sql string buffer", pSql->self);
    tscFreeSqlObj(pSql);
    free(pStream);
    return NULL;
  }

  strtolower(pSql->sqlstr, pSql->sqlstr);
  pSql->fp      = tscCreateStream;
  pSql->fetchFp = tscCreateStream;
  pSql->cmd.resColumnId = TSDB_RES_COL_ID;

  tsem_init(&pSql->rspSem, 0, 0);
  registerSqlObj(pSql);

  tscDebugL("0x%"PRIx64" SQL: %s", pSql->self, pSql->sqlstr);

  pSql->fp      = cbParseSql;
  pSql->fetchFp = cbParseSql;

  registerSqlObj(pSql);

  int32_t code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_SUCCESS) {
    cbParseSql(pStream, pSql, code);
  } else if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
     tscDebug(" CQ taos_open_stream IN Process. sql=%s", sqlstr);
  } else {
    tscError("0x%"PRIx64" open stream failed, sql:%s, code:%s", pSql->self, sqlstr, tstrerror(code));
    taosReleaseRef(tscObjRef, pSql->self);
    free(pStream);
    return NULL;
  }

  return pStream;
}

TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sqlstr, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                              int64_t tsc_stime, void *param, void (*callback)(void *)) {  
  return taos_open_stream_withname(taos, "", -1, sqlstr, fp, tsc_stime, param, callback, NULL);
}

void taos_close_stream(TAOS_STREAM *handle) {
  SSqlStream *pStream = (SSqlStream *)handle;

  SSqlObj *pSql = pStream->pSql;
  if (pSql == NULL) {
    return;
  }

  /*
   * stream may be closed twice, 1. drop dst table, 2. kill stream
   * Here, we need a check before release memory
   */
  if (pSql->signature == pSql) {
    tscRemoveFromStreamList(pStream, pSql);
    pStream->pSql = NULL;

    taosTmrStopA(&(pStream->pTimer));

    tscDebug("0x%"PRIx64" stream:%p is closed", pSql->self, pStream);
    // notify CQ to release the pStream object
    pStream->fp(pStream->param, NULL, NULL);

    taos_free_result(pSql);

    // free malloc
    if(pStream->to) {
      tfree(pStream->to);
      pStream->to = NULL;
    }
    if(pStream->split) {
      tfree(pStream->split);
      pStream->split = NULL;
    }
    tfree(pStream);
  }
}
