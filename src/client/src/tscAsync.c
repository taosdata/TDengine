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
#include "osAtomic.h"
#include "tarray.h"
#include "tutil.h"

#include "qTableMeta.h"
#include "tnote.h"
#include "trpc.h"
#include "tscBatchWrite.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tscUtil.h"
#include "tsclient.h"

static void tscAsyncQueryRowsForNextVnode(void *param, TAOS_RES *tres, int numOfRows);

/*
 * Proxy function to perform sequentially query&retrieve operation.
 * If sql queries upon a super table and two-stage merge procedure is not involved (when employ the projection
 * query), it will sequentially query&retrieve data for all vnodes
 */
static void tscAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows);

// like select * from st1 ,  st2  where ... format
static inline int32_t likeBlanksCommaBlans(char * str) {
  char *p = str;
  int32_t cnt1 = 0; //  ' ' count
  int32_t cnt2 = 0; //  ','  count

  while (*p != 0) {
    if(*p == ' ')
      cnt1++;
    else if(*p == ',')
      cnt2++;
    else
      return cnt2 == 0 ? 0 : cnt1 + cnt2;
    p++;
  }

  return 0;
}


// return tbname start , put tbname end to args pe
static char *searchTBName(char *from_end, char **pend) {
  char *p = from_end;
  // remove pre blanks
  while(*p == ' ') {
    ++p;
  }
  char *tbname = p;

  if(*p == 0)
    return NULL;

  // goto next blank
  while(1) {
    p++;

    if(*p == ' ') {
      // if following not have ,  this is end

      // format like  select * from stb1  ,   stb2  , stb3 where ...
      int32_t len = likeBlanksCommaBlans(p);
      if(len > 0) {
        p += len;
        continue;
      }

      // tbname is end
      if(pend)
        *pend = p;

      return tbname;
    } else if(*p == ';' || *p == 0) {
      // sql end flag '\0' or ';' end
      if(pend)
       *pend = p;

      return tbname;
    }
  }

  return NULL;
}

// return names min pointer
static inline char *searchEndPart(char *tbname_end) {
  char* names[] = {
    " group ",
    " order ",
    " interval(",
    " interval (",
    " session(",
    " session (",
    " state_window(",
    " state_window (",
    " slimit ",
    " slimit(",
    " limit ",
    " limit(",
    " sliding ",
    " fill(",
    " fill ("
    " >>",
    ";"
  };

  int32_t count = sizeof(names)/sizeof(char *);
  char * p = NULL;
  for(int32_t i = 0; i < count; i++) {
    char * p1 = strstr(tbname_end, names[i]);
    if (p1) {
      if (p == NULL || p1 < p)
        p = p1;
    }
  }

  if(p == NULL) {
    // move string end
    p = tbname_end + strlen(tbname_end);
  }

  return p;
}

// get brackets context and set context to pend
static inline char *bracketsString(char *select, char **pend){
  char *p = select;
  int32_t cnt = 0;
  while (*p) {
    if(*p == '(') {
      // left bracket
      cnt++;
    } else if(*p == ')') {
      // right bracket
      cnt--;
    }

    if(cnt < 0) {
      // this is end
      if(pend)
        *pend = p;
      // copy str to new
       int len = p - 1 - select;
       if(len == 0)
          return NULL;
       len += 1; // string end
       char *str = (char *)malloc(len);
       strncpy(str, select, len);
       str[len] = 0;

       return str;
    }
    ++p;
  }

  return NULL;
}

//
// return new malloc buffer, NULL is need not insert or failed tags example is 'tags=3'
// sql part :
//   select * from       st         where age=1      order by ts;
//   -------------       ---         -----------      -----------
//   select part     tbname part   condition part      end part
//
static inline char *insertTags(char *sql, char *tags) {
  char *p = sql;
  // remove pre blanks
  while(*p == ' ') {
    ++p;
  }

  // filter not query sql
  if(strncmp(p, "select ", 7) != 0) {
    return NULL;
  }

  // specail check
  char *from  = strstr(p, " from ");
  char *block = strstr(p, " _block_dist() ");
  if (from == NULL || block != NULL) {
    return NULL;
  }

  char *select = strstr(p + 7, "select "); // sub select sql
  char *union_all = strstr(p + 7, " union all ");

  // need append tags filter
  int32_t bufLen = strlen(sql) + 1 + TSDB_TAGS_LEN;
  char *buf = malloc(bufLen);
  memset(buf, 0, bufLen);

  // case1 if have sub select, tags only append to sub select sql
  if(select && union_all) {
    // union all like select * from t1 union all select * from t2 union all select * from ....
    size_t len = strlen(sql) + 10;
    // part1
    char *part1 = (char *)malloc(len);
    memset(part1, 0, len);
    strncpy(part1, p, union_all - p);
    char *p1 = insertTags(part1, tags);
    free(part1);
    if(p1 == NULL) {
      free(buf);
      return NULL;
    }

    // part2
    char *part2 = union_all + sizeof(" union all ") - 1;
    char *p2 = insertTags(part2, tags);
    if(p2 == NULL) {
      free(buf);
      free(p1);
      return NULL;
    }

    // combine p1 + union all +  p2
    len = strlen(p1) + strlen(p2) + 32;
    char *all = (char *)malloc(len);
    strcpy(all, p1);
    strcat(all, " union all ");
    strcat(all, p2);

    free(p1);
    free(p2);
    free(buf);

    return all;
  }
  else if(select) {
    char *part1_end = select - 1;
    char *part2 = NULL;
    char *part3_start = 0;
    char *sub_sql = bracketsString(select, &part3_start);
    if (sub_sql == NULL) {
      // unknown format, can not insert tags
      tscError("TAGS found sub select sql but can not parse brackets format. select=%s sql=%s", select, sql);
      free(buf);
      return NULL;
    }

    // nest call
    part2 = insertTags(sub_sql, tags);
    free(sub_sql);
    if (part2 == NULL) {
      // unknown format, can not insert tags
      tscError("TAGS insertTags sub select sql failed. subsql=%s sql=%s", sub_sql, sql);
      free(buf);
      return NULL;
    }

    // new string is part1 + part2 + part 3
    strncpy(buf, p, part1_end - p + 1);
    strcat(buf, part2);
    strcat(buf, part3_start);

    // return ok 1
    //      like select * from (select * from st where age>1) where age == 2;
    //   after-> select * from (select * from st where (tags=3) and (age>1) ) where age == 2;
    return buf;
  }

  char *tbname_end = NULL;
  char *tbname = searchTBName(from + sizeof(" from ") - 1, &tbname_end);
  if(tbname == NULL || tbname_end == NULL) {
    // unexpect string format
    free(buf);
    return NULL;
  }

  // condition part
  char *where = strstr(tbname_end, " where ");
  char *end_part  = searchEndPart(tbname_end);
  if(end_part == NULL) {
    // invalid sql
    free(buf);
    return NULL;
  }

  // case2 no condition part
  if(where == NULL) {
    strncpy(buf, p, end_part - p);
    strcat(buf, " where ");
    strcat(buf, tags);
    strcat(buf, end_part);

    // return ok 2
    //      like select * from st order by ts;
    //   after-> select * from st where tags=3 order by ts;
    return buf;
  }

  // case3 found condition part
  char *cond_part = where + sizeof("where ");
  strncpy(buf, p, cond_part - p); // where before part(include where )
  strcat(buf, "(");
  int32_t cond_len = end_part - cond_part;
  // cat cond part
  strncat(buf, cond_part, cond_len);
  strcat(buf, ") and (");
  // cat tags part
  strcat(buf, tags);
  strcat(buf, ")");
  // cat end part
  strcat(buf, end_part);

  // return ok 3
  //      like select * from st where age=1 order by ts;
  //   after-> select * from st where (age=1) and (tags=3) order by ts;
  return buf;
}

// if return true success, false is not append privilege sql
bool appendTagsFilter(SSqlObj* pSql) {
  // valid tags
  STscObj * pTscObj = pSql->pTscObj;
  if(pTscObj->tags[0] == 0) {
    tscDebug("TAGS 0x%" PRIx64 " tags empty. user=%s", pSql->self, pTscObj->user);
    return false;
  }

  // check tags is blank or ''
  char* p1 = pTscObj->tags;
  if (strcmp(p1, "\'\'") == 0) {
    tscDebug("TAGS 0x%" PRIx64 " tags is empty. user=%s", pSql->self, pTscObj->user);
    return false;
  }
  bool blank = true;
  while(*p1 != 0) {
    if(*p1 != ' ') {
      blank = false;
      break;
    }
    ++p1;
  }
  // result
  if(blank) {
    tscDebug("TAGS 0x%" PRIx64 " tags is all blank. user=%s", pSql->self, pTscObj->user);
    return false;
  }

  char * p = insertTags(pSql->sqlstr, pTscObj->tags);
  if(p == NULL) {
    return false;
  }

  // replace new
  char * old = pSql->sqlstr;
  pSql->sqlstr = p;
  tscDebug("TAGS 0x%" PRIx64 " replace sqlstr ok. old=%s new=%s tags=%s", pSql->self, old, p, pTscObj->tags);
  free(old);

  return true;
}

void doAsyncQuery(STscObj* pObj, SSqlObj* pSql, __async_cb_func_t fp, void* param, const char* sqlstr, size_t sqlLen) {
  SSqlCmd* pCmd = &pSql->cmd;

  pSql->signature = pSql;
  pSql->param     = param;
  pSql->pTscObj   = pObj;
  pSql->parseRetry= 0;
  pSql->maxRetry  = TSDB_MAX_REPLICA;
  pSql->fp        = fp;
  pSql->fetchFp   = fp;
  pSql->rootObj   = pSql;

  registerSqlObj(pSql);

  pSql->sqlstr = calloc(1, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to malloc sql string buffer", pSql->self);
    pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscAsyncResultOnError(pSql);
    return;
  }

  strntolower(pSql->sqlstr, sqlstr, (int32_t)sqlLen);

  appendTagsFilter(pSql);

  tscDebugL("0x%"PRIx64" SQL: %s", pSql->self, pSql->sqlstr);
  pCmd->resColumnId = TSDB_RES_COL_ID;

  taosAcquireRef(tscObjRef, pSql->self);
  int32_t code = tsParseSql(pSql, true);

  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }
  
  if (code != TSDB_CODE_SUCCESS) {
    pSql->res.code = code;
    tscAsyncResultOnError(pSql);
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }
  
  if (pObj->dispatcherManager != NULL) {
    SAsyncBatchWriteDispatcher * dispatcher = dispatcherAcquire(pObj->dispatcherManager);
    if (dispatcherTryDispatch(dispatcher, pSql)) {
      taosReleaseRef(tscObjRef, pSql->self);
      tscDebug("sql obj %p has been buffer in insert buffer", pSql);
      return;
    }
  }
  
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  executeQuery(pSql, pQueryInfo);
  taosReleaseRef(tscObjRef, pSql->self);
}

// TODO return the correct error code to client in tscQueueAsyncError
void taos_query_a(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param) {
  taos_query_ra(taos, sqlstr, fp, param, tsWriteBatchSize > 0);
}

TAOS_RES * taos_query_ra(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param, bool enableBatch) {
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
  
  SSqlObj *pSql = tscAllocSqlObj();
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_OUT_OF_MEMORY);
    return NULL;
  }
  
  pSql->enableBatch = enableBatch;
  
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
  if (pCmd->command != TSDB_SQL_RETRIEVE_GLOBALMERGE) {
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
    if (pRes->code == TSDB_CODE_SUCCESS) {
      pRes->code = TSDB_CODE_TSC_INVALID_QHANDLE;           
    }

    tscAsyncResultOnError(pSql);
    return;
  }

  pSql->fp = fp;
  if (pCmd->command != TSDB_SQL_RETRIEVE_GLOBALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
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

  tscResetForNextRetrieve(pRes);
  
  // handle outer query based on the already retrieved nest query results.
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  if (pQueryInfo->pUpstream != NULL && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {
    SSchedMsg schedMsg = {0};
    schedMsg.fp      = doRetrieveSubqueryData;
    schedMsg.ahandle = (void *)pSql;
    schedMsg.thandle = (void *)1;
    schedMsg.msg     = 0;
    taosScheduleTask(tscQhandle, &schedMsg);
    return;
  }

  if (pRes->qId == 0 && pSql->cmd.command != TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    tscError("qhandle is invalid");
    pRes->code = TSDB_CODE_TSC_INVALID_QHANDLE;
    tscAsyncResultOnError(pSql);
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
    } else if (pCmd->command == TSDB_SQL_RETRIEVE || pCmd->command == TSDB_SQL_RETRIEVE_GLOBALMERGE) {
      // in case of show command, return no data
      (*pSql->fetchFp)(param, pSql, 0);
    } else {
      assert(0);
    }
  } else { // current query is not completed, continue retrieve from node
    if (pCmd->command != TSDB_SQL_RETRIEVE_GLOBALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
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

  // probe send error , but result be responsed by server async
  if(pSql->res.code == TSDB_CODE_SUCCESS) {
    return ;
  }
  
  if (tsShortcutFlag && (pSql->res.code == TSDB_CODE_RPC_SHORTCUT)) {
    tscDebug("0x%" PRIx64 " async result callback, code:%s", pSql->self, tstrerror(pSql->res.code));
    pSql->res.code = TSDB_CODE_SUCCESS;
  } else {
    tscError("0x%" PRIx64 " async result callback, code:%s", pSql->self, tstrerror(pSql->res.code));
  }

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
void tscClearTableMeta(SSqlObj *pSql);

static void freeElem(void* p) {
  tfree(*(char**)p);
}

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code) {
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)param);
  if (pSql == NULL) return;

  assert(pSql->signature == pSql && (int64_t)param == pSql->self);

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  pRes->code = code;

  SSqlObj *sub = (SSqlObj*) res;
  const char* msg = (sub->cmd.command == TSDB_SQL_STABLEVGROUP)? "vgroup-list":"multi-tableMeta";
  if (code != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64" get %s failed, code:%s", pSql->self, msg, tstrerror(code));
    if (code == TSDB_CODE_RPC_FQDN_ERROR) {
      size_t sz = strlen(tscGetErrorMsgPayload(&sub->cmd));
      tscAllocPayload(&pSql->cmd, (int)sz + 1); 
      memcpy(tscGetErrorMsgPayload(&pSql->cmd), tscGetErrorMsgPayload(&sub->cmd), sz);
    } else if (code == TSDB_CODE_MND_INVALID_TABLE_NAME) {
      if (sub->cmd.command == TSDB_SQL_MULTI_META && pSql->cmd.hashedTableNames) {
        tscClearTableMeta(pSql);
        taosArrayDestroyEx(&pSql->cmd.hashedTableNames, freeElem);
        pSql->cmd.hashedTableNames = NULL;
      }
    }
    goto _error;
  }

  if (sub->cmd.command == TSDB_SQL_MULTI_META) {
    if (pSql->cmd.hashedTableNames) {
      taosArrayDestroyEx(&pSql->cmd.hashedTableNames, freeElem);
      pSql->cmd.hashedTableNames = NULL;
    }
  }

  tscDebug("0x%"PRIx64" get %s successfully", pSql->self, msg);
  if (pSql->pStream == NULL) {
    SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

    if (pQueryInfo != NULL && TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT)) {
      tscDebug("0x%" PRIx64 " continue parse sql after get table-meta", pSql->self);

      code = tsParseSql(pSql, false);
      if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
        taosReleaseRef(tscObjRef, pSql->self);
        return;
      } else if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      if (TSDB_QUERY_HAS_TYPE(pCmd->insertParam.insertType, TSDB_QUERY_TYPE_STMT_INSERT)) {  // stmt insert
        (*pSql->fp)(pSql->param, pSql, code);
      } else if (TSDB_QUERY_HAS_TYPE(pCmd->insertParam.insertType, TSDB_QUERY_TYPE_FILE_INSERT)) { // file insert
        tscImportDataFromFile(pSql);
      } else {  // sql string insert
        tscHandleMultivnodeInsert(pSql);
      }
    } else {
      if (pSql->retryReason != TSDB_CODE_SUCCESS) {
        tscDebug("0x%" PRIx64 " update cached table-meta, re-validate sql statement and send query again", pSql->self);
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

  _error:
  pRes->code = code;
  tscAsyncResultOnError(pSql);
  taosReleaseRef(tscObjRef, pSql->self);
}

void tscClearTableMeta(SSqlObj *pSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  int32_t n = taosArrayGetSize(pCmd->hashedTableNames);
  for (int32_t i = 0; i < n; i++) {
    char *t = taosArrayGetP(pCmd->hashedTableNames, i);
    taosHashRemove(UTIL_GET_TABLEMETA(pSql), t, strlen(t));
  }
}
