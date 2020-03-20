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
#include "tscJoinProcess.h"
#include "tsclient.h"
#include "qtsbuf.h"

static void freeSubqueryObj(SSqlObj* pSql);

static bool doCompare(int32_t order, int64_t left, int64_t right) {
  if (order == TSQL_SO_ASC) {
    return left < right;
  } else {
    return left > right;
  }
}

static int64_t doTSBlockIntersect(SSqlObj* pSql, SJoinSubquerySupporter* pSupporter1,
                                  SJoinSubquerySupporter* pSupporter2, TSKEY* st, TSKEY* et) {
  STSBuf* output1 = tsBufCreate(true);
  STSBuf* output2 = tsBufCreate(true);

  *st = INT64_MAX;
  *et = INT64_MIN;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
  
  SLimitVal* pLimit = &pQueryInfo->limit;
  int32_t    order = pQueryInfo->order.order;
  
  SQueryInfo* pSubQueryInfo1 = tscGetQueryInfoDetail(&pSql->pSubs[0]->cmd, 0);
  SQueryInfo* pSubQueryInfo2 = tscGetQueryInfoDetail(&pSql->pSubs[1]->cmd, 0);
  
  pSubQueryInfo1->tsBuf = output1;
  pSubQueryInfo2->tsBuf = output2;

  tsBufResetPos(pSupporter1->pTSBuf);
  tsBufResetPos(pSupporter2->pTSBuf);

  // TODO add more details information
  if (!tsBufNextPos(pSupporter1->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscTrace("%p input1 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  if (!tsBufNextPos(pSupporter2->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscTrace("%p input2 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  int64_t numOfInput1 = 1;
  int64_t numOfInput2 = 1;

  while (1) {
    STSElem elem1 = tsBufGetElem(pSupporter1->pTSBuf);
    STSElem elem2 = tsBufGetElem(pSupporter2->pTSBuf);

#ifdef _DEBUG_VIEW
    // for debug purpose
    tscPrint("%" PRId64 ", tags:%d \t %" PRId64 ", tags:%d", elem1.ts, elem1.tag, elem2.ts, elem2.tag);
#endif

    if (elem1.tag < elem2.tag || (elem1.tag == elem2.tag && doCompare(order, elem1.ts, elem2.ts))) {
      if (!tsBufNextPos(pSupporter1->pTSBuf)) {
        break;
      }

      numOfInput1++;
    } else if (elem1.tag > elem2.tag || (elem1.tag == elem2.tag && doCompare(order, elem2.ts, elem1.ts))) {
      if (!tsBufNextPos(pSupporter2->pTSBuf)) {
        break;
      }

      numOfInput2++;
    } else {
      /*
       * in case of stable query, limit/offset is not applied here. the limit/offset is applied to the
       * final results which is acquired after the secondry merge of in the client.
       */
      if (pLimit->offset == 0 || pQueryInfo->intervalTime > 0 || QUERY_IS_STABLE_QUERY(pQueryInfo->type)) {
        if (*st > elem1.ts) {
          *st = elem1.ts;
        }
  
        if (*et < elem1.ts) {
          *et = elem1.ts;
        }
        
        tsBufAppend(output1, elem1.vnode, elem1.tag, (const char*)&elem1.ts, sizeof(elem1.ts));
        tsBufAppend(output2, elem2.vnode, elem2.tag, (const char*)&elem2.ts, sizeof(elem2.ts));
      } else {
        pLimit->offset -= 1;
      }

      if (!tsBufNextPos(pSupporter1->pTSBuf)) {
        break;
      }

      numOfInput1++;

      if (!tsBufNextPos(pSupporter2->pTSBuf)) {
        break;
      }

      numOfInput2++;
    }
  }

  /*
   * failed to set the correct ts order yet in two cases:
   * 1. only one element
   * 2. only one element for each tag.
   */
  if (output1->tsOrder == -1) {
    output1->tsOrder = TSQL_SO_ASC;
    output2->tsOrder = TSQL_SO_ASC;
  }

  tsBufFlush(output1);
  tsBufFlush(output2);

  tsBufDestory(pSupporter1->pTSBuf);
  tsBufDestory(pSupporter2->pTSBuf);

  tscTrace("%p input1:%" PRId64 ", input2:%" PRId64 ", final:%" PRId64 " for secondary query after ts blocks "
           "intersecting, skey:%" PRId64 ", ekey:%" PRId64, pSql,
           numOfInput1, numOfInput2, output1->numOfTotal, *st, *et);

  return output1->numOfTotal;
}

// todo handle failed to create sub query
SJoinSubquerySupporter* tscCreateJoinSupporter(SSqlObj* pSql, SSubqueryState* pState, int32_t index) {
  SJoinSubquerySupporter* pSupporter = calloc(1, sizeof(SJoinSubquerySupporter));
  if (pSupporter == NULL) {
    return NULL;
  }

  pSupporter->pObj = pSql;
  pSupporter->pState = pState;

  pSupporter->subqueryIndex = index;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
  
  pSupporter->interval = pQueryInfo->intervalTime;
  pSupporter->limit = pQueryInfo->limit;

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, pSql->cmd.clauseIndex, index);
  pSupporter->uid = pTableMetaInfo->pTableMeta->uid;
  
  assert (pSupporter->uid != 0);

  getTmpfilePath("join-", pSupporter->path);
  pSupporter->f = fopen(pSupporter->path, "w");

  if (pSupporter->f == NULL) {
    tscError("%p failed to create tmp file:%s, reason:%s", pSql, pSupporter->path, strerror(errno));
  }

  return pSupporter;
}

void tscDestroyJoinSupporter(SJoinSubquerySupporter* pSupporter) {
  if (pSupporter == NULL) {
    return;
  }

  tscSqlExprInfoDestroy(&pSupporter->exprsInfo);
  tscColumnBaseInfoDestroy(&pSupporter->colList);

  tscClearFieldInfo(&pSupporter->fieldsInfo);

  if (pSupporter->f != NULL) {
    fclose(pSupporter->f);
    unlink(pSupporter->path);
  }

  tscTagCondRelease(&pSupporter->tagCond);
  free(pSupporter);
}

/*
 * need the secondary query process
 * In case of count(ts)/count(*)/spread(ts) query, that are only applied to
 * primary timestamp column , the secondary query is not necessary
 *
 */
bool needSecondaryQuery(SQueryInfo* pQueryInfo) {
  for (int32_t i = 0; i < pQueryInfo->colList.numOfCols; ++i) {
    SColumnBase* pBase = tscColumnBaseInfoGet(&pQueryInfo->colList, i);
    if (pBase->colIndex.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return true;
    }
  }

  return false;
}

/*
 * launch secondary stage query to fetch the result that contains timestamp in set
 */
int32_t tscLaunchSecondPhaseSubqueries(SSqlObj* pSql) {
  int32_t                 numOfSub = 0;
  SJoinSubquerySupporter* pSupporter = NULL;
  
  /*
   * If the columns are not involved in the final select clause, the secondary query will not be launched
   * for the subquery.
   */
  SSubqueryState* pState = NULL;
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    pSupporter = pSql->pSubs[i]->param;
    if (pSupporter->exprsInfo.numOfExprs > 0) {
      ++numOfSub;
    }
  }
  
  assert(numOfSub > 0);
  
  // scan all subquery, if one sub query has only ts, ignore it
  tscTrace("%p start to launch secondary subqueries, total:%d, only:%d needs to query, others are not retrieve in "
      "select clause", pSql, pSql->numOfSubs, numOfSub);

  /*
   * the subqueries that do not actually launch the secondary query to virtual node is set as completed.
   */
  pState = pSupporter->pState;
  pState->numOfTotal = pSql->numOfSubs;
  pState->numOfCompleted = (pSql->numOfSubs - numOfSub);
  
  bool success = true;
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj *pPrevSub = pSql->pSubs[i];
    pSql->pSubs[i] = NULL;
    
    pSupporter = pPrevSub->param;
  
    if (pSupporter->exprsInfo.numOfExprs == 0) {
      tscTrace("%p subIndex: %d, not need to launch query, ignore it", pSql, i);
    
      tscDestroyJoinSupporter(pSupporter);
      tscFreeSqlObj(pPrevSub);
    
      pSql->pSubs[i] = NULL;
      continue;
    }
  
    SQueryInfo *pSubQueryInfo = tscGetQueryInfoDetail(&pPrevSub->cmd, 0);
    STSBuf *pTSBuf = pSubQueryInfo->tsBuf;
    pSubQueryInfo->tsBuf = NULL;
  
    // free result for async object will also free sqlObj
    assert(pSubQueryInfo->exprsInfo.numOfExprs == 1); // ts_comp query only requires one resutl columns
    taos_free_result(pPrevSub);
  
    SSqlObj *pNew = createSubqueryObj(pSql, (int16_t) i, tscJoinQueryCallback, pSupporter, NULL);
    if (pNew == NULL) {
      tscDestroyJoinSupporter(pSupporter);
      success = false;
      break;
    }
  
    tscClearSubqueryInfo(&pNew->cmd);
    pSql->pSubs[i] = pNew;
  
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pQueryInfo->tsBuf = pTSBuf;  // transfer the ownership of timestamp comp-z data to the new created object
  
    // set the second stage sub query for join process
    pQueryInfo->type |= TSDB_QUERY_TYPE_JOIN_SEC_STAGE;
  
    pQueryInfo->intervalTime = pSupporter->interval;
    pQueryInfo->groupbyExpr = pSupporter->groupbyExpr;
  
    tscColumnBaseInfoCopy(&pQueryInfo->colList, &pSupporter->colList, 0);
    tscTagCondCopy(&pQueryInfo->tagCond, &pSupporter->tagCond);
  
    tscSqlExprCopy(&pQueryInfo->exprsInfo, &pSupporter->exprsInfo, pSupporter->uid, false);
    tscFieldInfoCopyAll(&pQueryInfo->fieldsInfo, &pSupporter->fieldsInfo);
    
    pSupporter->exprsInfo.numOfExprs = 0;
    pSupporter->fieldsInfo.numOfOutputCols = 0;
    
    /*
     * if the first column of the secondary query is not ts function, add this function.
     * Because this column is required to filter with timestamp after intersecting.
     */
    if (pSupporter->exprsInfo.pExprs[0]->functionId != TSDB_FUNC_TS) {
      tscAddTimestampColumn(pQueryInfo, TSDB_FUNC_TS, 0);
    }
  
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    assert(pNew->numOfSubs == 0 && pNew->cmd.numOfClause == 1 && pNewQueryInfo->numOfTables == 1);
  
    tscFieldInfoCalOffset(pNewQueryInfo);
  
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pNewQueryInfo, 0);
  
    /*
     * When handling the projection query, the offset value will be modified for table-table join, which is changed
     * during the timestamp intersection.
     */
    pSupporter->limit = pQueryInfo->limit;
    pNewQueryInfo->limit = pSupporter->limit;
  
    // fetch the join tag column
    if (UTIL_TABLE_IS_SUPERTABLE(pTableMetaInfo)) {
      SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);
      assert(pQueryInfo->tagCond.joinInfo.hasJoin);
    
      int16_t tagColIndex = tscGetJoinTagColIndexByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->uid);
      pExpr->param[0].i64Key = tagColIndex;
      pExpr->numOfParams = 1;
    }
  
    tscPrintSelectClause(pNew, 0);
  
    tscTrace("%p subquery:%p tableIndex:%d, vnodeIdx:%d, type:%d, exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pTableMetaInfo->vnodeIndex, pNewQueryInfo->type,
             pNewQueryInfo->exprsInfo.numOfExprs, pNewQueryInfo->colList.numOfCols,
             pNewQueryInfo->fieldsInfo.numOfOutputCols, pNewQueryInfo->pTableMetaInfo[0]->name);
  }
  
  //prepare the subqueries object failed, abort
  if (!success) {
    pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("%p failed to prepare subqueries objs for secondary phase query, numOfSub:%d, code:%d", pSql,
        pSql->numOfSubs, pSql->res.code);
    freeSubqueryObj(pSql);
    
    return pSql->res.code;
  }
  
  for(int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }
    
    tscProcessSql(pSub);
  }

  return TSDB_CODE_SUCCESS;
}

void freeSubqueryObj(SSqlObj* pSql) {
  SSubqueryState* pState = NULL;

  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] != NULL) {
      SJoinSubquerySupporter* p = pSql->pSubs[i]->param;
      pState = p->pState;

      tscDestroyJoinSupporter(p);

      if (pSql->pSubs[i]->res.code == TSDB_CODE_SUCCESS) {
        taos_free_result(pSql->pSubs[i]);
      }
    }
  }

  tfree(pState);
  pSql->numOfSubs = 0;
}

static void doQuitSubquery(SSqlObj* pParentSql) {
  freeSubqueryObj(pParentSql);

  tsem_wait(&pParentSql->emptyRspSem);
  tsem_wait(&pParentSql->emptyRspSem);

  tsem_post(&pParentSql->rspSem);
}

static void quitAllSubquery(SSqlObj* pSqlObj, SJoinSubquerySupporter* pSupporter) {
  int32_t numOfTotal = pSupporter->pState->numOfTotal;
  int32_t finished = atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1);
  
  if (finished >= numOfTotal) {
    pSqlObj->res.code = abs(pSupporter->pState->code);
    tscError("%p all subquery return and query failed, global code:%d", pSqlObj, pSqlObj->res.code);

    doQuitSubquery(pSqlObj);
  }
}

// update the query time range according to the join results on timestamp
static void updateQueryTimeRange(SQueryInfo* pQueryInfo, int64_t st, int64_t et) {
  assert(pQueryInfo->stime <= st && pQueryInfo->etime >= et);

  pQueryInfo->stime = st;
  pQueryInfo->etime = et;
}

static void joinRetrieveCallback(void* param, TAOS_RES* tres, int numOfRows) {
  SJoinSubquerySupporter* pSupporter = (SJoinSubquerySupporter*)param;
  SSqlObj*                pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  if ((pQueryInfo->type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) == 0) {
    if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
      tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, numOfRows,
               pSupporter->pState->code);

      quitAllSubquery(pParentSql, pSupporter);
      return;
    }

    if (numOfRows > 0) {  // write the data into disk
      fwrite(pSql->res.data, pSql->res.numOfRows, 1, pSupporter->f);
      fclose(pSupporter->f);

      STSBuf* pBuf = tsBufCreateFromFile(pSupporter->path, true);
      if (pBuf == NULL) {
        tscError("%p invalid ts comp file from vnode, abort sub query, file size:%d", pSql, numOfRows);

        pSupporter->pState->code = TSDB_CODE_APP_ERROR;  // todo set the informative code
        quitAllSubquery(pParentSql, pSupporter);
        return;
      }

      if (pSupporter->pTSBuf == NULL) {
        tscTrace("%p create tmp file for ts block:%s", pSql, pBuf->path);
        pSupporter->pTSBuf = pBuf;
      } else {
        assert(pQueryInfo->numOfTables == 1);  // for subquery, only one metermetaInfo
        STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

        tsBufMerge(pSupporter->pTSBuf, pBuf, pTableMetaInfo->vnodeIndex);
        tsBufDestory(pBuf);
      }

      // open new file to save the result
      getTmpfilePath("ts-join", pSupporter->path);
      pSupporter->f = fopen(pSupporter->path, "w");
      pSql->res.row = pSql->res.numOfRows;

      taos_fetch_rows_a(tres, joinRetrieveCallback, param);
    } else if (numOfRows == 0) {  // no data from this vnode anymore
      SQueryInfo* pParentQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, pParentSql->cmd.clauseIndex);
      
      //todo refactor
      if (tscNonOrderedProjectionQueryOnSTable(pParentQueryInfo, 0)) {
        STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
        assert(pQueryInfo->numOfTables == 1);

        // for projection query, need to try next vnode
        int32_t totalVnode = pTableMetaInfo->pMetricMeta->numOfVnodes;
        if ((++pTableMetaInfo->vnodeIndex) < totalVnode) {
          tscTrace("%p current vnode:%d exhausted, try next:%d. total vnode:%d. current numOfRes:%d", pSql,
                   pTableMetaInfo->vnodeIndex - 1, pTableMetaInfo->vnodeIndex, totalVnode, pRes->numOfTotal);
  
          pSql->cmd.command = TSDB_SQL_SELECT;
          pSql->fp = tscJoinQueryCallback;
          tscProcessSql(pSql);

          return;
        }
      }
  
      int32_t numOfTotal = pSupporter->pState->numOfTotal;
      int32_t finished = atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1);
  
      if (finished >= numOfTotal) {
        assert(finished == numOfTotal);
        
        if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
          tscTrace("%p sub:%p, numOfSub:%d, quit from further procedure due to other queries failure", pParentSql, tres,
                   pSupporter->subqueryIndex);
          doQuitSubquery(pParentSql);
          return;
        }

        tscTrace("%p all subqueries retrieve ts complete, do ts block intersect", pParentSql);

        SJoinSubquerySupporter* p1 = pParentSql->pSubs[0]->param;
        SJoinSubquerySupporter* p2 = pParentSql->pSubs[1]->param;

        TSKEY st, et;

        int64_t num = doTSBlockIntersect(pParentSql, p1, p2, &st, &et);
        if (num <= 0) {  // no result during ts intersect
          tscTrace("%p free all sub SqlObj and quit", pParentSql);
          doQuitSubquery(pParentSql);
        } else {
          updateQueryTimeRange(pParentQueryInfo, st, et);
          tscLaunchSecondPhaseSubqueries(pParentSql);
        }
      }
    } else {  // failure of sub query
      tscError("%p sub query failed, code:%d, index:%d", pSql, numOfRows, pSupporter->subqueryIndex);
      pSupporter->pState->code = numOfRows;

      quitAllSubquery(pParentSql, pSupporter);
      return;
    }

  } else {  // secondary stage retrieve, driven by taos_fetch_row or other functions
    if (numOfRows < 0) {
      pSupporter->pState->code = numOfRows;
      tscError("%p retrieve failed, code:%d, index:%d", pSql, numOfRows, pSupporter->subqueryIndex);
    }

    if (numOfRows >= 0) {
      pSql->res.numOfTotal += pSql->res.numOfRows;
    }
  
    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && numOfRows == 0) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      assert(pQueryInfo->numOfTables == 1);

      // for projection query, need to try next vnode if current vnode is exhausted
      if ((++pTableMetaInfo->vnodeIndex) < pTableMetaInfo->pMetricMeta->numOfVnodes) {
        pSupporter->pState->numOfCompleted = 0;
        pSupporter->pState->numOfTotal = 1;

        pSql->cmd.command = TSDB_SQL_SELECT;
        pSql->fp = tscJoinQueryCallback;
        tscProcessSql(pSql);

        return;
      }
    }
  
    int32_t numOfTotal = pSupporter->pState->numOfTotal;
    int32_t finished = atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1);
  
    if (finished >= numOfTotal) {
      assert(finished == numOfTotal);
      tscTrace("%p all %d secondary subquery retrieves completed, global code:%d", tres, numOfTotal,
               pParentSql->res.code);

      if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
        pParentSql->res.code = abs(pSupporter->pState->code);
        freeSubqueryObj(pParentSql);
      }

      tsem_post(&pParentSql->rspSem);
    } else {
      tscTrace("%p sub:%p completed, completed:%d, total:%d", pParentSql, tres, finished, numOfTotal);
    }
  }
}

static SJoinSubquerySupporter* tscUpdateSubqueryStatus(SSqlObj* pSql, int32_t numOfFetch) {
  int32_t notInvolved = 0;
  SJoinSubquerySupporter* pSupporter = NULL;
  SSubqueryState* pState = NULL;
  
  for(int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] == NULL) {
      notInvolved++;
    } else {
      pSupporter = (SJoinSubquerySupporter*)pSql->pSubs[i]->param;
      pState = pSupporter->pState;
    }
  }
  
  pState->numOfTotal = pSql->numOfSubs;
  pState->numOfCompleted = pSql->numOfSubs - numOfFetch;
  
  return pSupporter;
}

void tscFetchDatablockFromSubquery(SSqlObj* pSql) {
  int32_t numOfFetch = 0;
  assert(pSql->numOfSubs >= 1);
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] == NULL) {  // this subquery does not need to involve in secondary query
      continue;
    }
    
    SSqlRes *pRes = &pSql->pSubs[i]->res;
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->pSubs[i]->cmd, 0);
  
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
      if (pRes->row >= pRes->numOfRows && pTableMetaInfo->vnodeIndex < pTableMetaInfo->pMetricMeta->numOfVnodes &&
          (!tscHasReachLimitation(pQueryInfo, pRes))) {
        numOfFetch++;
      }
    } else {
      if (pRes->row >= pRes->numOfRows && (!tscHasReachLimitation(pQueryInfo, pRes))) {
        numOfFetch++;
      }
    }
  }

  if (numOfFetch <= 0) {
    return;
  }

  // TODO multi-vnode retrieve for projection query with limitation has bugs, since the global limiation is not handled
  tscTrace("%p retrieve data from %d subqueries", pSql, numOfFetch);

  SJoinSubquerySupporter* pSupporter = tscUpdateSubqueryStatus(pSql, numOfFetch);
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj* pSql1 = pSql->pSubs[i];
    if (pSql1 == NULL) {
      continue;
    }
    
    SSqlRes* pRes1 = &pSql1->res;
    SSqlCmd* pCmd1 = &pSql1->cmd;

    pSupporter = (SJoinSubquerySupporter*)pSql1->param;

    // wait for all subqueries completed
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd1, 0);
    assert(pRes1->numOfRows >= 0 && pQueryInfo->numOfTables == 1);

    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    
    if (pRes1->row >= pRes1->numOfRows) {
      tscTrace("%p subquery:%p retrieve data from vnode, subquery:%d, vnodeIndex:%d", pSql, pSql1,
               pSupporter->subqueryIndex, pTableMetaInfo->vnodeIndex);

      tscResetForNextRetrieve(pRes1);
      pSql1->fp = joinRetrieveCallback;

      if (pCmd1->command < TSDB_SQL_LOCAL) {
        pCmd1->command = (pCmd1->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
      }

      tscProcessSql(pSql1);
    }
  }

  // wait for all subquery completed
  tsem_wait(&pSql->rspSem);
  
  // update the records for each subquery
  for(int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] == NULL) {
      continue;
    }
    
    SSqlRes* pRes1 = &pSql->pSubs[i]->res;
    pRes1->numOfTotalInCurrentClause += pRes1->numOfRows;
  }
}

// all subqueries return, set the result output index
void tscSetupOutputColumnIndex(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  tscTrace("%p all subquery response, retrieve data", pSql);

  if (pRes->pColumnIndex != NULL) {
    return;  // the column transfer support struct has been built
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  pRes->pColumnIndex = calloc(1, sizeof(SColumnIndex) * pQueryInfo->fieldsInfo.numOfOutputCols);

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);

    int32_t tableIndexOfSub = -1;
    for (int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, j);
      if (pTableMetaInfo->pTableMeta->uid == pExpr->uid) {
        tableIndexOfSub = j;
        break;
      }
    }

    assert(tableIndexOfSub >= 0 && tableIndexOfSub < pQueryInfo->numOfTables);
    
    SSqlCmd* pSubCmd = &pSql->pSubs[tableIndexOfSub]->cmd;
    SQueryInfo* pSubQueryInfo = tscGetQueryInfoDetail(pSubCmd, 0);
    
    for (int32_t k = 0; k < pSubQueryInfo->exprsInfo.numOfExprs; ++k) {
      SSqlExpr* pSubExpr = tscSqlExprGet(pSubQueryInfo, k);
      if (pExpr->functionId == pSubExpr->functionId && pExpr->colInfo.colId == pSubExpr->colInfo.colId) {
        pRes->pColumnIndex[i] = (SColumnIndex){.tableIndex = tableIndexOfSub, .columnIndex = k};
        break;
      }
    }
  }
}

void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code) {
  SSqlObj* pSql = (SSqlObj*)tres;
  //  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  //  int32_t idx = pSql->cmd.vnodeIdx;

  //  SVnodeSidList *vnodeInfo = NULL;
  //  if (pTableMetaInfo->pMetricMeta != NULL) {
  //    vnodeInfo = tscGetVnodeSidList(pTableMetaInfo->pMetricMeta, idx - 1);
  //  }

  SJoinSubquerySupporter* pSupporter = (SJoinSubquerySupporter*)param;

  //  if (atomic_add_fetch_32(pSupporter->numOfComplete, 1) >=
  //      pSupporter->numOfTotal) {
  //    SSqlObj *pParentObj = pSupporter->pObj;
  //
  //    if ((pSql->cmd.type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) != 1) {
  //      int32_t num = 0;
  //      tscFetchDatablockFromSubquery(pParentObj);
  //      TSKEY* ts = tscGetQualifiedTSList(pParentObj, &num);
  //
  //      if (num <= 0) {
  //        // no qualified result
  //      }
  //
  //      tscLaunchSecondPhaseSubqueries(pSql, ts, num);
  //    } else {

  //    }
  //  } else {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  if ((pQueryInfo->type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) != TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
    if (code != TSDB_CODE_SUCCESS) {  // direct call joinRetrieveCallback and set the error code
      joinRetrieveCallback(param, pSql, code);
    } else {  // first stage query, continue to retrieve data
      pSql->fp = joinRetrieveCallback;
      pSql->cmd.command = TSDB_SQL_FETCH;
      tscProcessSql(pSql);
    }

  } else {  // second stage join subquery
    SSqlObj* pParentSql = pSupporter->pObj;

    if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
      tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, code,
               pSupporter->pState->code);
      quitAllSubquery(pParentSql, pSupporter);

      return;
    }

    if (code != TSDB_CODE_SUCCESS) {
      tscError("%p sub query failed, code:%d, set global code:%d, index:%d", pSql, code, code,
               pSupporter->subqueryIndex);
      pSupporter->pState->code = code;  // todo set the informative code

      quitAllSubquery(pParentSql, pSupporter);
    } else {
      int32_t numOfTotal = pSupporter->pState->numOfTotal;
      int32_t finished = atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1);
      
      if (finished >= numOfTotal) {
        assert(finished == numOfTotal);
        
        tscSetupOutputColumnIndex(pParentSql);
        STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

        /**
         * if the query is a continue query (vnodeIndex > 0 for projection query) for next vnode, do the retrieval of
         * data instead of returning to its invoker
         */
        if (pTableMetaInfo->vnodeIndex > 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
          assert(pTableMetaInfo->vnodeIndex < pTableMetaInfo->pMetricMeta->numOfVnodes);
          pSupporter->pState->numOfCompleted = 0;  // reset the record value

          pSql->fp = joinRetrieveCallback;  // continue retrieve data
          pSql->cmd.command = TSDB_SQL_FETCH;
          tscProcessSql(pSql);
        } else {  // first retrieve from vnode during the secondary stage sub-query
          if (pParentSql->fp == NULL) {
            tsem_wait(&pParentSql->emptyRspSem);
            tsem_wait(&pParentSql->emptyRspSem);

            tsem_post(&pParentSql->rspSem);
          } else {
            // set the command flag must be after the semaphore been correctly set.
            //    pPObj->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
            //    if (pPObj->res.code == TSDB_CODE_SUCCESS) {
            //      (*pPObj->fp)(pPObj->param, pPObj, 0);
            //    } else {
            //      tscQueueAsyncRes(pPObj);
            //    }
            assert(0);
          }
        }
      }
    }
  }
}