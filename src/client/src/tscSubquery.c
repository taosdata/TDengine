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

#include "tscSubquery.h"
#include "os.h"
#include "qtsbuf.h"
#include "tsclient.h"
#include "tscLog.h"

typedef struct SInsertSupporter {
  SSubqueryState* pState;
  SSqlObj*  pSql;
} SInsertSupporter;

static void freeSubqueryObj(SSqlObj* pSql);

static bool doCompare(int32_t order, int64_t left, int64_t right) {
  if (order == TSDB_ORDER_ASC) {
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
    output1->tsOrder = TSDB_ORDER_ASC;
    output2->tsOrder = TSDB_ORDER_ASC;
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

  tscSqlExprInfoDestroy(pSupporter->exprsInfo);
  tscColumnListDestroy(pSupporter->colList);

  tscFieldInfoClear(&pSupporter->fieldsInfo);

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
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumn* base = taosArrayGet(pQueryInfo->colList, i);
    if (base->colIndex.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
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
    if (taosArrayGetSize(pSupporter->exprsInfo) > 0) {
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
  
    if (taosArrayGetSize(pSupporter->exprsInfo) == 0) {
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
    assert(tscSqlExprNumOfExprs(pSubQueryInfo) == 1); // ts_comp query only requires one resutl columns
    taos_free_result(pPrevSub);
  
    SSqlObj *pNew = createSubqueryObj(pSql, (int16_t) i, tscJoinQueryCallback, pSupporter, TSDB_SQL_SELECT, NULL);
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
  
    tscColumnListCopy(pQueryInfo->colList, pSupporter->colList, 0);
    tscTagCondCopy(&pQueryInfo->tagCond, &pSupporter->tagCond);
  
    pQueryInfo->exprsInfo = tscSqlExprCopy(pSupporter->exprsInfo, pSupporter->uid, false);
    tscFieldInfoCopy(&pQueryInfo->fieldsInfo, &pSupporter->fieldsInfo);
    
    pSupporter->fieldsInfo.numOfOutput = 0;
    
    /*
     * if the first column of the secondary query is not ts function, add this function.
     * Because this column is required to filter with timestamp after intersecting.
     */
    SSqlExpr* pExpr = taosArrayGet(pSupporter->exprsInfo, 0);
    if (pExpr->functionId != TSDB_FUNC_TS) {
      tscAddTimestampColumn(pQueryInfo, TSDB_FUNC_TS, 0);
    }
  
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    assert(pNew->numOfSubs == 0 && pNew->cmd.numOfClause == 1 && pNewQueryInfo->numOfTables == 1);
  
    tscFieldInfoUpdateOffset(pNewQueryInfo);
  
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
  
    size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
    tscTrace("%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type,
             taosArrayGetSize(pNewQueryInfo->exprsInfo), numOfCols,
             pNewQueryInfo->fieldsInfo.numOfOutput, pNewQueryInfo->pTableMetaInfo[0]->name);
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

//  tsem_wait(&pParentSql->emptyRspSem);
//  tsem_wait(&pParentSql->emptyRspSem);

//  tsem_post(&pParentSql->rspSem);
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
  assert(pQueryInfo->window.skey <= st && pQueryInfo->window.ekey >= et);

  pQueryInfo->window.skey = st;
  pQueryInfo->window.ekey = et;
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

        tsBufMerge(pSupporter->pTSBuf, pBuf, pTableMetaInfo->vgroupIndex);
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
//        int32_t totalVnode = pTableMetaInfo->pMetricMeta->numOfVnodes;
        int32_t totalVnode = 0;
        if ((++pTableMetaInfo->vgroupIndex) < totalVnode) {
          tscTrace("%p current vnode:%d exhausted, try next:%d. total vnode:%d. current numOfRes:%d", pSql,
                   pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVnode, pRes->numOfTotal);
  
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
//      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      assert(pQueryInfo->numOfTables == 1);

      // for projection query, need to try next vnode if current vnode is exhausted
//      if ((++pTableMetaInfo->vgroupIndex) < pTableMetaInfo->pMetricMeta->numOfVnodes) {
//        pSupporter->pState->numOfCompleted = 0;
//        pSupporter->pState->numOfTotal = 1;
//
//        pSql->cmd.command = TSDB_SQL_SELECT;
//        pSql->fp = tscJoinQueryCallback;
//        tscProcessSql(pSql);
//
//        return;
//      }
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

//      tsem_post(&pParentSql->rspSem);
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
  
//    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
//      if (pRes->row >= pRes->numOfRows && pTableMetaInfo->vgroupIndex < pTableMetaInfo->pMetricMeta->numOfVnodes &&
//          (!tscHasReachLimitation(pQueryInfo, pRes))) {
//        numOfFetch++;
//      }
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
      tscTrace("%p subquery:%p retrieve data from vnode, subquery:%d, vgroupIndex:%d", pSql, pSql1,
               pSupporter->subqueryIndex, pTableMetaInfo->vgroupIndex);

      tscResetForNextRetrieve(pRes1);
      pSql1->fp = joinRetrieveCallback;

      if (pCmd1->command < TSDB_SQL_LOCAL) {
        pCmd1->command = (pCmd1->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
      }

      tscProcessSql(pSql1);
    }
  }

  // wait for all subquery completed
//  tsem_wait(&pSql->rspSem);
  
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
  pRes->pColumnIndex = calloc(1, sizeof(SColumnIndex) * pQueryInfo->fieldsInfo.numOfOutput);

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
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
    
    size_t numOfExprs = taosArrayGetSize(pSubQueryInfo->exprsInfo);
    for (int32_t k = 0; k < numOfExprs; ++k) {
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
         * if the query is a continue query (vgroupIndex > 0 for projection query) for next vnode, do the retrieval of
         * data instead of returning to its invoker
         */
        if (pTableMetaInfo->vgroupIndex > 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
//          assert(pTableMetaInfo->vgroupIndex < pTableMetaInfo->pMetricMeta->numOfVnodes);
          pSupporter->pState->numOfCompleted = 0;  // reset the record value

          pSql->fp = joinRetrieveCallback;  // continue retrieve data
          pSql->cmd.command = TSDB_SQL_FETCH;
          tscProcessSql(pSql);
        } else {  // first retrieve from vnode during the secondary stage sub-query
          if (pParentSql->fp == NULL) {
//            tsem_post(&pParentSql->rspSem);
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

/////////////////////////////////////////////////////////////////////////////////////////
static void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code);

static SSqlObj *tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj);

// todo merge with callback
int32_t tscLaunchJoinSubquery(SSqlObj *pSql, int16_t tableIndex, SJoinSubquerySupporter *pSupporter) {
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  pSql->res.qhandle = 0x1;
  pSql->res.numOfRows = 0;
  
  if (pSql->pSubs == NULL) {
    pSql->pSubs = calloc(pSupporter->pState->numOfTotal, POINTER_BYTES);
    if (pSql->pSubs == NULL) {
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }
  }
  
  SSqlObj *pNew = createSubqueryObj(pSql, tableIndex, tscJoinQueryCallback, pSupporter, TSDB_SQL_SELECT, NULL);
  if (pNew == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }
  
  pSql->pSubs[pSql->numOfSubs++] = pNew;
  assert(pSql->numOfSubs <= pSupporter->pState->numOfTotal);
  
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    addGroupInfoForSubquery(pSql, pNew, 0, tableIndex);
    
    // refactor as one method
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    assert(pNewQueryInfo != NULL);
    
    // update the table index
    size_t num = taosArrayGetSize(pNewQueryInfo->colList);
    for (int32_t i = 0; i < num; ++i) {
      SColumn* pCol = taosArrayGetP(pNewQueryInfo->colList, i);
      pCol->colIndex.tableIndex = 0;
    }
    
    tscColumnListCopy(pSupporter->colList, pNewQueryInfo->colList, 0);
  
    pSupporter->exprsInfo = tscSqlExprCopy(pNewQueryInfo->exprsInfo, pSupporter->uid, false);
    tscFieldInfoCopy(&pSupporter->fieldsInfo, &pNewQueryInfo->fieldsInfo);
    
    tscTagCondCopy(&pSupporter->tagCond, &pNewQueryInfo->tagCond);
    
    pNew->cmd.numOfCols = 0;
    pNewQueryInfo->intervalTime = 0;
    memset(&pNewQueryInfo->limit, 0, sizeof(SLimitVal));
    
    // backup the data and clear it in the sqlcmd object
    pSupporter->groupbyExpr = pNewQueryInfo->groupbyExpr;
    memset(&pNewQueryInfo->groupbyExpr, 0, sizeof(SSqlGroupbyExpr));
    
    // this data needs to be transfer to support struct
    pNewQueryInfo->fieldsInfo.numOfOutput = 0;
    
    // set the ts,tags that involved in join, as the output column of intermediate result
    tscClearSubqueryInfo(&pNew->cmd);
    
    SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
    SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
    
    tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);
    
    // set the tags value for ts_comp function
    SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);
    
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pNewQueryInfo, 0);
    int16_t         tagColIndex = tscGetJoinTagColIndexByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->uid);
    
    pExpr->param->i64Key = tagColIndex;
    pExpr->numOfParams = 1;
    
    // add the filter tag column
    size_t s = taosArrayGetSize(pSupporter->colList);
    
    for (int32_t i = 0; i < s; ++i) {
      SColumn *pCol = taosArrayGetP(pSupporter->colList, i);
      
      if (pCol->numOfFilters > 0) {  // copy to the pNew->cmd.colList if it is filtered.
        SColumn* p = tscColumnClone(pCol);
        taosArrayPush(pNewQueryInfo->colList, &p);
      }
    }
  
    size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
  
    tscTrace("%p subquery:%p tableIndex:%d, vnodeIdx:%d, type:%d, transfer to ts_comp query to retrieve timestamps, "
             "exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
             pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type,
             tscSqlExprNumOfExprs(pNewQueryInfo), numOfCols,
             pNewQueryInfo->fieldsInfo.numOfOutput, pNewQueryInfo->pTableMetaInfo[0]->name);
    tscPrintSelectClause(pNew, 0);

  } else {
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pNewQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;
  }

#ifdef _DEBUG_VIEW
  tscPrintSelectClause(pNew, 0);
#endif
  
  return tscProcessSql(pNew);
}

// todo support async join query
int32_t tscHandleMasterJoinQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  assert((pQueryInfo->type & TSDB_QUERY_TYPE_SUBQUERY) == 0);
  
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  
  pState->numOfTotal = pQueryInfo->numOfTables;
  
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    SJoinSubquerySupporter *pSupporter = tscCreateJoinSupporter(pSql, pState, i);
    
    if (pSupporter == NULL) {  // failed to create support struct, abort current query
      tscError("%p tableIndex:%d, failed to allocate join support object, abort further query", pSql, i);
      pState->numOfCompleted = pQueryInfo->numOfTables - i - 1;
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      
      return pSql->res.code;
    }
    
    int32_t code = tscLaunchJoinSubquery(pSql, i, pSupporter);
    if (code != TSDB_CODE_SUCCESS) {  // failed to create subquery object, quit query
      tscDestroyJoinSupporter(pSupporter);
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      
      break;
    }
  }
  
//  tsem_wait(&pSql->rspSem);
  
  if (pSql->numOfSubs <= 0) {
    pSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
  } else {
    pSql->cmd.command = TSDB_SQL_METRIC_JOIN_RETRIEVE;
  }
  
  return TSDB_CODE_SUCCESS;
}

static void doCleanupSubqueries(SSqlObj *pSql, int32_t numOfSubs, SSubqueryState* pState) {
  assert(numOfSubs <= pSql->numOfSubs && numOfSubs >= 0 && pState != NULL);
  
  for(int32_t i = 0; i < numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    assert(pSub != NULL);
    
    SRetrieveSupport* pSupport = pSub->param;
    
    tfree(pSupport->localBuffer);
    
    pthread_mutex_unlock(&pSupport->queryMutex);
    pthread_mutex_destroy(&pSupport->queryMutex);
    
    tfree(pSupport);
    
    tscFreeSqlObj(pSub);
  }
  
  free(pState);
}

int32_t tscHandleMasterSTableQuery(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  // pRes->code check only serves in launching metric sub-queries
  if (pRes->code == TSDB_CODE_QUERY_CANCELLED) {
    pCmd->command = TSDB_SQL_RETRIEVE_METRIC;  // enable the abort of kill metric function.
    return pRes->code;
  }
  
  tExtMemBuffer **  pMemoryBuf = NULL;
  tOrderDescriptor *pDesc = NULL;
  SColumnModel *    pModel = NULL;
  
  pRes->qhandle = 1;  // hack the qhandle check
  
  const uint32_t nBufferSize = (1u << 16);  // 64KB
  
  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  pSql->numOfSubs = pTableMetaInfo->vgroupList->numOfVgroups;
  assert(pSql->numOfSubs > 0);
  
  int32_t ret = tscLocalReducerEnvCreate(pSql, &pMemoryBuf, &pDesc, &pModel, nBufferSize);
  if (ret != 0) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    if (pSql->fp) {
      tscQueueAsyncRes(pSql);
    }
    return pRes->code;
  }
  
  pSql->pSubs = calloc(pSql->numOfSubs, POINTER_BYTES);
  
  tscTrace("%p retrieved query data from %d vnode(s)", pSql, pSql->numOfSubs);
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = pSql->numOfSubs;
  pRes->code = TSDB_CODE_SUCCESS;
  
  int32_t i = 0;
  for (; i < pSql->numOfSubs; ++i) {
    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    if (trs == NULL) {
      tscError("%p failed to malloc buffer for SRetrieveSupport, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      break;
    }
    
    trs->pExtMemBuffer = pMemoryBuf;
    trs->pOrderDescriptor = pDesc;
    trs->pState = pState;
    
    trs->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
    if (trs->localBuffer == NULL) {
      tscError("%p failed to malloc buffer for local buffer, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      tfree(trs);
      break;
    }
    
    trs->subqueryIndex = i;
    trs->pParentSqlObj = pSql;
    trs->pFinalColModel = pModel;
    
    pthread_mutexattr_t mutexattr = {0};
    pthread_mutexattr_settype(&mutexattr, PTHREAD_MUTEX_RECURSIVE_NP);
    pthread_mutex_init(&trs->queryMutex, &mutexattr);
    pthread_mutexattr_destroy(&mutexattr);
    
    SSqlObj *pNew = tscCreateSqlObjForSubquery(pSql, trs, NULL);
    if (pNew == NULL) {
      tscError("%p failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      tfree(trs->localBuffer);
      tfree(trs);
      break;
    }
    
    // todo handle multi-vnode situation
    if (pQueryInfo->tsBuf) {
      SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
      pNewQueryInfo->tsBuf = tsBufClone(pQueryInfo->tsBuf);
    }
    
    tscTrace("%p sub:%p create subquery success. orderOfSub:%d", pSql, pNew, trs->subqueryIndex);
  }
  
  if (i < pSql->numOfSubs) {
    tscError("%p failed to prepare subquery structure and launch subqueries", pSql);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pSql->numOfSubs);
    doCleanupSubqueries(pSql, i, pState);
    return pRes->code;   // free all allocated resource
  }
  
  if (pRes->code == TSDB_CODE_QUERY_CANCELLED) {
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pSql->numOfSubs);
    doCleanupSubqueries(pSql, i, pState);
    return pRes->code;
  }
  
  for(int32_t j = 0; j < pSql->numOfSubs; ++j) {
    SSqlObj* pSub = pSql->pSubs[j];
    SRetrieveSupport* pSupport = pSub->param;
    
    tscTrace("%p sub:%p launch subquery, orderOfSub:%d.", pSql, pSub, pSupport->subqueryIndex);
    tscProcessSql(pSub);
  }
  
  return TSDB_CODE_SUCCESS;
}

static void tscFreeSubSqlObj(SRetrieveSupport *trsupport, SSqlObj *pSql) {
  tscTrace("%p start to free subquery result", pSql);
  
  if (pSql->res.code == TSDB_CODE_SUCCESS) {
    taos_free_result(pSql);
  }
  
  tfree(trsupport->localBuffer);
  
  pthread_mutex_unlock(&trsupport->queryMutex);
  pthread_mutex_destroy(&trsupport->queryMutex);
  
  tfree(trsupport);
}

static void tscRetrieveFromDnodeCallBack(void *param, TAOS_RES *tres, int numOfRows);
static void tscHandleSubqueryError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows);

static void tscAbortFurtherRetryRetrieval(SRetrieveSupport *trsupport, TAOS_RES *tres, int32_t errCode) {
// set no disk space error info
#ifdef WINDOWS
  LPVOID lpMsgBuf;
  FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
                GetLastError(), MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),  // Default language
                (LPTSTR)&lpMsgBuf, 0, NULL);
  tscError("sub:%p failed to flush data to disk:reason:%s", tres, lpMsgBuf);
  LocalFree(lpMsgBuf);
#else
  char buf[256] = {0};
  strerror_r(errno, buf, 256);
  tscError("sub:%p failed to flush data to disk:reason:%s", tres, buf);
#endif
  
  trsupport->pState->code = -errCode;
  trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
  
  pthread_mutex_unlock(&trsupport->queryMutex);
  
  tscHandleSubqueryError(trsupport, tres, trsupport->pState->code);
}

void tscHandleSubqueryError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows) {
  SSqlObj *pPObj = trsupport->pParentSqlObj;
  int32_t  subqueryIndex = trsupport->subqueryIndex;
  
  assert(pSql != NULL);
  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfCompleted < pState->numOfTotal && pState->numOfCompleted >= 0 &&
      pPObj->numOfSubs == pState->numOfTotal);
  
  // retrieved in subquery failed. OR query cancelled in retrieve phase.
  if (pState->code == TSDB_CODE_SUCCESS && pPObj->res.code != TSDB_CODE_SUCCESS) {
    pState->code = pPObj->res.code;
    
    /*
     * kill current sub-query connection, which may retrieve data from vnodes;
     * Here we get: pPObj->res.code == TSDB_CODE_QUERY_CANCELLED
     */
    pSql->res.numOfRows = 0;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;  // disable retry efforts
    tscTrace("%p query is cancelled, sub:%p, orderOfSub:%d abort retrieve, code:%d", trsupport->pParentSqlObj, pSql,
             subqueryIndex, pState->code);
  }
  
  if (numOfRows >= 0) {  // current query is successful, but other sub query failed, still abort current query.
    tscTrace("%p sub:%p retrieve numOfRows:%d,orderOfSub:%d", pPObj, pSql, numOfRows, subqueryIndex);
    tscError("%p sub:%p abort further retrieval due to other queries failure,orderOfSub:%d,code:%d", pPObj, pSql,
             subqueryIndex, pState->code);
  } else {
    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY && pState->code == TSDB_CODE_SUCCESS) {
      /*
       * current query failed, and the retry count is less than the available
       * count, retry query clear previous retrieved data, then launch a new sub query
       */
      tExtMemBufferClear(trsupport->pExtMemBuffer[subqueryIndex]);
      
      // clear local saved number of results
      trsupport->localBuffer->numOfElems = 0;
      pthread_mutex_unlock(&trsupport->queryMutex);
      
      tscTrace("%p sub:%p retrieve failed, code:%d, orderOfSub:%d, retry:%d", trsupport->pParentSqlObj, pSql, numOfRows,
               subqueryIndex, trsupport->numOfRetry);
      
      SSqlObj *pNew = tscCreateSqlObjForSubquery(trsupport->pParentSqlObj, trsupport, pSql);
      if (pNew == NULL) {
        tscError("%p sub:%p failed to create new subquery sqlObj due to out of memory, abort retry",
                 trsupport->pParentSqlObj, pSql);
        
        pState->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
        trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
        return;
      }
      
      tscProcessSql(pNew);
      return;
    } else {  // reach the maximum retry count, abort
      atomic_val_compare_exchange_32(&pState->code, TSDB_CODE_SUCCESS, numOfRows);
      tscError("%p sub:%p retrieve failed,code:%d,orderOfSub:%d failed.no more retry,set global code:%d", pPObj, pSql,
               numOfRows, subqueryIndex, pState->code);
    }
  }
  
  int32_t numOfTotal = pState->numOfTotal;
  
  int32_t finished = atomic_add_fetch_32(&pState->numOfCompleted, 1);
  if (finished < numOfTotal) {
    tscTrace("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pPObj, pSql, trsupport->subqueryIndex, finished);
    return tscFreeSubSqlObj(trsupport, pSql);
  }
  
  // all subqueries are failed
  tscError("%p retrieve from %d vnode(s) completed,code:%d.FAILED.", pPObj, pState->numOfTotal, pState->code);
  pPObj->res.code = pState->code;
  
  // release allocated resource
  tscLocalReducerEnvDestroy(trsupport->pExtMemBuffer, trsupport->pOrderDescriptor, trsupport->pFinalColModel,
                            pState->numOfTotal);
  
  tfree(trsupport->pState);
  tscFreeSubSqlObj(trsupport, pSql);
  
  // in case of second stage join subquery, invoke its callback function instead of regular QueueAsyncRes
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pPObj->cmd, 0);
  
  if ((pQueryInfo->type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) == TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
    (*pPObj->fp)(pPObj->param, pPObj, pPObj->res.code);
  } else {  // regular super table query
    if (pPObj->res.code != TSDB_CODE_SUCCESS) {
      tscQueueAsyncRes(pPObj);
    }
  }
}

static void tscAllDataRetrievedFromDnode(SRetrieveSupport *trsupport, SSqlObj* pSql) {
  int32_t           idx = trsupport->subqueryIndex;
  SSqlObj *         pPObj = trsupport->pParentSqlObj;
  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;
  
  SSubqueryState* pState = trsupport->pState;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];
  
  // data in from current vnode is stored in cache and disk
  uint32_t numOfRowsFromSubquery = trsupport->pExtMemBuffer[idx]->numOfTotalElems + trsupport->localBuffer->numOfElems;
    tscTrace("%p sub:%p all data retrieved from ip:%u,vgId:%d, numOfRows:%d, orderOfSub:%d", pPObj, pSql,
        pTableMetaInfo->vgroupList->vgroups[0].ipAddr[0].fqdn, pTableMetaInfo->vgroupList->vgroups[0].vgId,
        numOfRowsFromSubquery, idx);
  
  tColModelCompact(pDesc->pColumnModel, trsupport->localBuffer, pDesc->pColumnModel->capacity);

#ifdef _DEBUG_VIEW
  printf("%" PRIu64 " rows data flushed to disk:\n", trsupport->localBuffer->numOfElems);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, trsupport->localBuffer->data, trsupport->localBuffer->numOfElems,
                       trsupport->localBuffer->numOfElems, colInfo);
#endif
  
  if (tsTotalTmpDirGB != 0 && tsAvailTmpDirGB < tsMinimalTmpDirGB) {
    tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pPObj, pSql,
             tsAvailTmpDirGB, tsMinimalTmpDirGB);
    tscAbortFurtherRetryRetrieval(trsupport, pSql, TSDB_CODE_CLI_NO_DISKSPACE);
    return;
  }
  
  // each result for a vnode is ordered as an independant list,
  // then used as an input of loser tree for disk-based merge routine
  int32_t ret = tscFlushTmpBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer,
                                  pQueryInfo->groupbyExpr.orderType);
  if (ret != 0) { // set no disk space error info, and abort retry
    return tscAbortFurtherRetryRetrieval(trsupport, pSql, TSDB_CODE_CLI_NO_DISKSPACE);
  }
  
  // keep this value local variable, since the pState variable may be released by other threads, if atomic_add opertion
  // increases the finished value up to pState->numOfTotal value, which means all subqueries are completed.
  // In this case, the comparsion between finished value and released pState->numOfTotal is not safe.
  int32_t numOfTotal = pState->numOfTotal;
  
  int32_t finished = atomic_add_fetch_32(&pState->numOfCompleted, 1);
  if (finished < numOfTotal) {
    tscTrace("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pPObj, pSql, trsupport->subqueryIndex, finished);
    return tscFreeSubSqlObj(trsupport, pSql);
  }
  
  // all sub-queries are returned, start to local merge process
  pDesc->pColumnModel->capacity = trsupport->pExtMemBuffer[idx]->numOfElemsPerPage;
  
  tscTrace("%p retrieve from %d vnodes completed.final NumOfRows:%d,start to build loser tree", pPObj,
           pState->numOfTotal, pState->numOfRetrievedRows);
  
  SQueryInfo *pPQueryInfo = tscGetQueryInfoDetail(&pPObj->cmd, 0);
  tscClearInterpInfo(pPQueryInfo);
  
  tscCreateLocalReducer(trsupport->pExtMemBuffer, pState->numOfTotal, pDesc, trsupport->pFinalColModel,
                        &pPObj->cmd, &pPObj->res);
  tscTrace("%p build loser tree completed", pPObj);
  
  pPObj->res.precision = pSql->res.precision;
  pPObj->res.numOfRows = 0;
  pPObj->res.row = 0;
  
  // only free once
  tfree(trsupport->pState);
  tscFreeSubSqlObj(trsupport, pSql);
  
  // set the command flag must be after the semaphore been correctly set.
  pPObj->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
  if (pPObj->res.code == TSDB_CODE_SUCCESS) {
    (*pPObj->fp)(pPObj->param, pPObj, 0);
  } else {
    tscQueueAsyncRes(pPObj);
  }
}

static void tscRetrieveFromDnodeCallBack(void *param, TAOS_RES *tres, int numOfRows) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;
  int32_t           idx = trsupport->subqueryIndex;
  SSqlObj *         pPObj = trsupport->pParentSqlObj;
  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;
  
  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {  // sql object has been released in error process, return immediately
    tscTrace("%p subquery has been released, idx:%d, abort", pPObj, idx);
    return;
  }
  
  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfCompleted < pState->numOfTotal && pState->numOfCompleted >= 0 &&
      pPObj->numOfSubs == pState->numOfTotal);
  
  // query process and cancel query process may execute at the same time
  pthread_mutex_lock(&trsupport->queryMutex);
  
  if (numOfRows < 0 || pState->code < 0 || pPObj->res.code != TSDB_CODE_SUCCESS) {
    return tscHandleSubqueryError(trsupport, pSql, numOfRows);
  }
  
  SSqlRes *   pRes = &pSql->res;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  if (numOfRows > 0) {
    assert(pRes->numOfRows == numOfRows);
    int64_t num = atomic_add_fetch_64(&pState->numOfRetrievedRows, numOfRows);
    
//    tscTrace("%p sub:%p retrieve numOfRows:%d totalNumOfRows:%d from ip:%u,vid:%d,orderOfSub:%d", pPObj, pSql,
//             pRes->numOfRows, pState->numOfRetrievedRows, pSvd->ip, pSvd->vnode, idx);
    
    if (num > tsMaxNumOfOrderedResults && tscIsProjectionQueryOnSTable(pQueryInfo, 0)) {
      tscError("%p sub:%p num of OrderedRes is too many, max allowed:%" PRId64 " , current:%" PRId64,
               pPObj, pSql, tsMaxNumOfOrderedResults, num);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_SORTED_RES_TOO_MANY);
      return;
    }

#ifdef _DEBUG_VIEW
    printf("received data from vnode: %"PRIu64" rows\n", pRes->numOfRows);
    SSrcColumnInfo colInfo[256] = {0};

    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, pRes->data, pRes->numOfRows, pRes->numOfRows, colInfo);
#endif
    
    if (tsTotalTmpDirGB != 0 && tsAvailTmpDirGB < tsMinimalTmpDirGB) {
      tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pPObj, pSql,
               tsAvailTmpDirGB, tsMinimalTmpDirGB);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
      return;
    }
    
    int32_t ret = saveToBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pRes->data,
                               pRes->numOfRows, pQueryInfo->groupbyExpr.orderType);
    if (ret < 0) { // set no disk space error info, and abort retry
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
      
    } else if (pRes->completed) {
      tscAllDataRetrievedFromDnode(trsupport, pSql);
      return;
      
    } else { // continue fetch data from dnode
      pthread_mutex_unlock(&trsupport->queryMutex);
      taos_fetch_rows_a(tres, tscRetrieveFromDnodeCallBack, param);
    }
    
    pthread_mutex_unlock(&trsupport->queryMutex);
  } else { // all data has been retrieved to client
    tscAllDataRetrievedFromDnode(trsupport, pSql);
  }
}

static SSqlObj *tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj) {
  const int32_t table_index = 0;
  
  SSqlObj *pNew = createSubqueryObj(pSql, table_index, tscRetrieveDataRes, trsupport, TSDB_SQL_SELECT, prevSqlObj);
  if (pNew != NULL) {  // the sub query of two-stage super table query
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pQueryInfo->type |= TSDB_QUERY_TYPE_STABLE_SUBQUERY;
    
    assert(pQueryInfo->numOfTables == 1 && pNew->cmd.numOfClause == 1);
    
    // launch subquery for each vnode, so the subquery index equals to the vgroupIndex.
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, table_index);
    pTableMetaInfo->vgroupIndex = trsupport->subqueryIndex;
    
    pSql->pSubs[trsupport->subqueryIndex] = pNew;
  }
  
  return pNew;
}

void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *) param;
  
  SSqlObj*  pParentSql = trsupport->pParentSqlObj;
  SSqlObj*  pSql = (SSqlObj *) tres;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  assert(pSql->cmd.numOfClause == 1 && pQueryInfo->numOfTables == 1);
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SCMVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[0];
  
  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfCompleted < pState->numOfTotal && pState->numOfCompleted >= 0 &&
      pParentSql->numOfSubs == pState->numOfTotal);
  
  if (pParentSql->res.code != TSDB_CODE_SUCCESS || pState->code != TSDB_CODE_SUCCESS) {
    
    // stable query is killed, abort further retry
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    
    if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
      code = pParentSql->res.code;
    } else {
      code = pState->code;
    }
    
    tscTrace("%p query cancelled or failed, sub:%p, orderOfSub:%d abort, code:%s", pParentSql, pSql,
             trsupport->subqueryIndex, tstrerror(code));
  }
  
  /*
   * if a query on a vnode is failed, all retrieve operations from vnode that occurs later
   * than this one are actually not necessary, we simply call the tscRetrieveFromDnodeCallBack
   * function to abort current and remain retrieve process.
   *
   * NOTE: thread safe is required.
   */
  if (code != TSDB_CODE_SUCCESS) {
    if (trsupport->numOfRetry++ >= MAX_NUM_OF_SUBQUERY_RETRY) {
      tscTrace("%p sub:%p reach the max retry times, set global code:%d", pParentSql, pSql, code);
      atomic_val_compare_exchange_32(&pState->code, 0, code);
    } else {  // does not reach the maximum retry time, go on
      tscTrace("%p sub:%p failed code:%s, retry:%d", pParentSql, pSql, tstrerror(code), trsupport->numOfRetry);
      
      SSqlObj *pNew = tscCreateSqlObjForSubquery(pParentSql, trsupport, pSql);
      if (pNew == NULL) {
        tscError("%p sub:%p failed to create new subquery due to out of memory, abort retry, vgId:%d, orderOfSub:%d",
                 trsupport->pParentSqlObj, pSql, pVgroup->vgId, trsupport->subqueryIndex);
        
        pState->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
        trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
      } else {
        SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
        assert(pNewQueryInfo->pTableMetaInfo[0]->pTableMeta != NULL);
        
        tscProcessSql(pNew);
        return;
      }
    }
  }
  
  if (pState->code != TSDB_CODE_SUCCESS) {  // at least one peer subquery failed, abort current query
    tscTrace("%p sub:%p query failed,ip:%u,vgId:%d,orderOfSub:%d,global code:%d", pParentSql, pSql,
               pVgroup->ipAddr[0].fqdn, pVgroup->vgId, trsupport->subqueryIndex, pState->code);
  
    tscHandleSubqueryError(param, tres, pState->code);
  } else {  // success, proceed to retrieve data from dnode
    tscTrace("%p sub:%p query complete, ip:%u, vgId:%d, orderOfSub:%d, retrieve data", trsupport->pParentSqlObj, pSql,
               pVgroup->ipAddr[0].fqdn, pVgroup->vgId, trsupport->subqueryIndex);
    
    if (pSql->res.qhandle == 0) { // qhandle is NULL, code is TSDB_CODE_SUCCESS means no results generated from this vnode
      tscRetrieveFromDnodeCallBack(param, pSql, 0);
    } else {
      taos_fetch_rows_a(tres, tscRetrieveFromDnodeCallBack, param);
    }
    
  }
}

static void multiVnodeInsertMerge(void* param, TAOS_RES* tres, int numOfRows) {
  SInsertSupporter *pSupporter = (SInsertSupporter *)param;
  SSqlObj* pParentObj = pSupporter->pSql;
  SSqlCmd* pParentCmd = &pParentObj->cmd;
  
  SSubqueryState* pState = pSupporter->pState;
  int32_t total = pState->numOfTotal;
  
  // increase the total inserted rows
  if (numOfRows > 0) {
    pParentObj->res.numOfRows += numOfRows;
  }
  
  int32_t completed = atomic_add_fetch_32(&pState->numOfCompleted, 1);
  if (completed < total) {
    return;
  }
  
  tscTrace("%p Async insertion completed, total inserted:%d", pParentObj, pParentObj->res.numOfRows);
  
  tfree(pState);
  tfree(pSupporter);
  
  // release data block data
  pParentCmd->pDataBlocks = tscDestroyBlockArrayList(pParentCmd->pDataBlocks);
  
  // restore user defined fp
  pParentObj->fp = pParentObj->fetchFp;
  
  // all data has been sent to vnode, call user function
  (*pParentObj->fp)(pParentObj->param, tres, numOfRows);
}

int32_t tscHandleMultivnodeInsert(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  pRes->qhandle = 1;  // hack the qhandle check
  SDataBlockList *pDataBlocks = pCmd->pDataBlocks;
  
  pSql->pSubs = calloc(pDataBlocks->nSize, POINTER_BYTES);
  pSql->numOfSubs = pDataBlocks->nSize;
  assert(pDataBlocks->nSize > 0);
  
  tscTrace("%p submit data to %d vnode(s)", pSql, pDataBlocks->nSize);
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = pSql->numOfSubs;
  
  pRes->code = TSDB_CODE_SUCCESS;
  
  int32_t i = 0;
  for (; i < pSql->numOfSubs; ++i) {
    SInsertSupporter* pSupporter = calloc(1, sizeof(SInsertSupporter));
    pSupporter->pSql = pSql;
    pSupporter->pState = pState;
    
    SSqlObj *pNew = createSubqueryObj(pSql, 0, multiVnodeInsertMerge, pSupporter, TSDB_SQL_INSERT, NULL);
    if (pNew == NULL) {
      tscError("%p failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      break;
    }
    
    pSql->pSubs[i] = pNew;
    tscTrace("%p sub:%p create subObj success. orderOfSub:%d", pSql, pNew, i);
  }
  
  if (i < pSql->numOfSubs) {
    tscError("%p failed to prepare subObj structure and launch sub-insertion", pSql);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    return pRes->code;  // free all allocated resource
  }
  
  for (int32_t j = 0; j < pSql->numOfSubs; ++j) {
    SSqlObj *pSub = pSql->pSubs[j];
    int32_t code = tscCopyDataBlockToPayload(pSub, pDataBlocks->pData[j]);
    
    if (code != TSDB_CODE_SUCCESS) {
      tscTrace("%p prepare submit data block failed in async insertion, vnodeIdx:%d, total:%d, code:%d", pSql, j,
               pDataBlocks->nSize, code);
    }
    
    tscTrace("%p sub:%p launch sub insert, orderOfSub:%d", pSql, pSub, j);
    tscProcessSql(pSub);
  }
  
  return TSDB_CODE_SUCCESS;
}
