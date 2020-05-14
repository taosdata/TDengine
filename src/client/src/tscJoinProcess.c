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
#include "tcache.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tscompression.h"
#include "ttime.h"
#include "tutil.h"

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
    int32_t ret = tVariantCompare(&elem1.tag,&elem2.tag );
    if (ret < 0 || (ret == 0 && doCompare(order, elem1.ts, elem2.ts))) {
      if (!tsBufNextPos(pSupporter1->pTSBuf)) {
        break;
      }

      numOfInput1++;
    } else if (ret > 0 || (ret == 0 && doCompare(order, elem2.ts, elem1.ts))) {
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
        
        tsBufAppend(output1, elem1.vnode, &elem1.tag, (const char*)&elem1.ts, sizeof(elem1.ts));
        tsBufAppend(output2, elem2.vnode, &elem2.tag, (const char*)&elem2.ts, sizeof(elem2.ts));
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

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, pSql->cmd.clauseIndex, index);
  pSupporter->uid = pMeterMetaInfo->pMeterMeta->uid;
  
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

int32_t tscLaunchSecondPhaseDirectly(SSqlObj* pSql, SSubqueryState* pState) {
  /*
   * If the columns are not involved in the final select clause, the secondary query will not be launched
   * for the subquery.
   */
  pSql->res.qhandle = 0x1;
  pSql->res.numOfRows = 0;
  
  tscTrace("%p start to launch secondary subqueries", pSql);
  bool success = true;
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
  
    SJoinSubquerySupporter *pSupporter = tscCreateJoinSupporter(pSql, pState, i);
    assert(pSupporter != NULL);
    
    SSqlObj *pNew = createSubqueryObj(pSql, (int16_t) i, tscJoinQueryCallback, pSupporter, NULL);
    if (pNew == NULL) {
      tscDestroyJoinSupporter(pSupporter);
      success = false;
      break;
    }
    
    pSql->pSubs[i] = pNew;
    
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pQueryInfo->tsBuf = NULL;  // transfer the ownership of timestamp comp-z data to the new created object
    
    // set the second stage sub query for join process
    pQueryInfo->type |= TSDB_QUERY_TYPE_JOIN_SEC_STAGE;
    
    /*
     * if the first column of the secondary query is not ts function, add this function.
     * Because this column is required to filter with timestamp after intersecting.
     */
    assert(pNew->numOfSubs == 0 && pNew->cmd.numOfClause == 1 && pQueryInfo->numOfTables == 1);

    /*
     * if the first column of the secondary query is not ts function, add this function.
     * Because this column is required to filter with timestamp after intersecting.
     */
    if (tscSqlExprNumOfExprs(pQueryInfo) == 0) {
      SColumnIndex index = {0};
      SSqlExpr* pExpr = tscSqlExprInsert(pQueryInfo, 0, TSDB_FUNC_COUNT, &index, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), sizeof(int64_t));
      SColumnList columnList = {0};
      columnList.num = 1;
      columnList.ids[0] = index;
      insertResultField(pQueryInfo, 0, &columnList, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, "ts", pExpr);
    } else if (tscSqlExprGet(pQueryInfo, 0)->functionId != TSDB_FUNC_TS) {
      tscAddTimestampColumn(pQueryInfo, TSDB_FUNC_TS, 0);
    }
    
    tscFieldInfoCalOffset(pQueryInfo);
    
    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
    
    /*
     * When handling the projection query, the offset value will be modified for table-table join, which is changed
     * during the timestamp intersection.
     */
    // fetch the join tag column
    if (UTIL_METER_IS_SUPERTABLE(pMeterMetaInfo)) {
      SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, 0);
      assert(pQueryInfo->tagCond.joinInfo.hasJoin);
      
      int16_t tagColIndex = tscGetJoinTagColIndexByUid(&pQueryInfo->tagCond, pMeterMetaInfo->pMeterMeta->uid);
      pExpr->param[0].i64Key = tagColIndex;
      pExpr->numOfParams = 1;
    }
    
    tscPrintSelectClause(pNew, 0);
    
    tscTrace("%p subquery:%p tableIndex:%d, vnodeIdx:%d, type:%d, exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pMeterMetaInfo->vnodeIndex, pQueryInfo->type,
             pQueryInfo->exprsInfo.numOfExprs, pQueryInfo->colList.numOfCols,
             pQueryInfo->fieldsInfo.numOfOutputCols, pQueryInfo->pMeterInfo[0]->name);
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
  
    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pNewQueryInfo, 0);
  
    /*
     * When handling the projection query, the offset value will be modified for table-table join, which is changed
     * during the timestamp intersection.
     */
    pSupporter->limit = pQueryInfo->limit;
    pNewQueryInfo->limit = pSupporter->limit;
  
    // fetch the join tag column
    if (UTIL_METER_IS_SUPERTABLE(pMeterMetaInfo)) {
      SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);
      assert(pQueryInfo->tagCond.joinInfo.hasJoin);
    
      int16_t tagColIndex = tscGetJoinTagColIndexByUid(&pQueryInfo->tagCond, pMeterMetaInfo->pMeterMeta->uid);
      pExpr->param[0].i64Key = tagColIndex;
      pExpr->numOfParams = 1;
    }
  
    tscPrintSelectClause(pNew, 0);
  
    tscTrace("%p subquery:%p tableIndex:%d, vnodeIdx:%d, type:%d, exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pMeterMetaInfo->vnodeIndex, pNewQueryInfo->type,
             pNewQueryInfo->exprsInfo.numOfExprs, pNewQueryInfo->colList.numOfCols,
             pNewQueryInfo->fieldsInfo.numOfOutputCols, pNewQueryInfo->pMeterInfo[0]->name);
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

static void freeSubqueryObj(SSqlObj* pSql) {
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
        SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

        tsBufMerge(pSupporter->pTSBuf, pBuf, pMeterMetaInfo->vnodeIndex);
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
        SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
        assert(pQueryInfo->numOfTables == 1);

        // for projection query, need to try next vnode
        int32_t totalVnode = pMeterMetaInfo->pMetricMeta->numOfVnodes;
        if ((++pMeterMetaInfo->vnodeIndex) < totalVnode) {
          tscTrace("%p current vnode:%d exhausted, try next:%d. total vnode:%d. current numOfRes:%d", pSql,
                   pMeterMetaInfo->vnodeIndex - 1, pMeterMetaInfo->vnodeIndex, totalVnode, pRes->numOfTotal);
  
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
      SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
      assert(pQueryInfo->numOfTables == 1);

      // for projection query, need to try next vnode if current vnode is exhausted
      if ((++pMeterMetaInfo->vnodeIndex) < pMeterMetaInfo->pMetricMeta->numOfVnodes) {
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
  
    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
  
    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
      if (pRes->row >= pRes->numOfRows && pMeterMetaInfo->vnodeIndex < pMeterMetaInfo->pMetricMeta->numOfVnodes &&
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

    SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
    
    if (pRes1->row >= pRes1->numOfRows) {
      tscTrace("%p subquery:%p retrieve data from vnode, subquery:%d, vnodeIndex:%d", pSql, pSql1,
               pSupporter->subqueryIndex, pMeterMetaInfo->vnodeIndex);

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
  pRes->pColumnIndex = calloc(1, sizeof(SColumnIndex) * pQueryInfo->exprsInfo.numOfExprs);

  for (int32_t i = 0; i < pQueryInfo->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);

    int32_t tableIndexOfSub = -1;
    for (int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
      SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, j);
      if (pMeterMetaInfo->pMeterMeta->uid == pExpr->uid) {
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
  //  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0, 0);

  //  int32_t idx = pSql->cmd.vnodeIdx;

  //  SVnodeSidList *vnodeInfo = NULL;
  //  if (pMeterMetaInfo->pMetricMeta != NULL) {
  //    vnodeInfo = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, idx - 1);
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
        SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

        /**
         * if the query is a continue query (vnodeIndex > 0 for projection query) for next vnode, do the retrieval of
         * data instead of returning to its invoker
         */
        if (pMeterMetaInfo->vnodeIndex > 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
          assert(pMeterMetaInfo->vnodeIndex < pMeterMetaInfo->pMetricMeta->numOfVnodes);
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

static int32_t getDataStartOffset() {
  return sizeof(STSBufFileHeader) + TS_COMP_FILE_VNODE_MAX * sizeof(STSVnodeBlockInfo);
}

static int32_t doUpdateVnodeInfo(STSBuf* pTSBuf, int64_t offset, STSVnodeBlockInfo* pVInfo) {
  if (offset < 0 || offset >= getDataStartOffset()) {
    return -1;
  }

  if (fseek(pTSBuf->f, offset, SEEK_SET) != 0) {
    return -1;
  }

  fwrite(pVInfo, sizeof(STSVnodeBlockInfo), 1, pTSBuf->f);
  return 0;
}

// update prev vnode length info in file
static void TSBufUpdateVnodeInfo(STSBuf* pTSBuf, int32_t index, STSVnodeBlockInfo* pBlockInfo) {
  int32_t offset = sizeof(STSBufFileHeader) + index * sizeof(STSVnodeBlockInfo);
  doUpdateVnodeInfo(pTSBuf, offset, pBlockInfo);
}

static STSBuf* allocResForTSBuf(STSBuf* pTSBuf) {
  const int32_t INITIAL_VNODEINFO_SIZE = 4;

  pTSBuf->numOfAlloc = INITIAL_VNODEINFO_SIZE;
  pTSBuf->pData = calloc(pTSBuf->numOfAlloc, sizeof(STSVnodeBlockInfoEx));
  if (pTSBuf->pData == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }

  pTSBuf->tsData.rawBuf = malloc(MEM_BUF_SIZE);
  if (pTSBuf->tsData.rawBuf == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }

  pTSBuf->bufSize = MEM_BUF_SIZE;
  pTSBuf->tsData.threshold = MEM_BUF_SIZE;
  pTSBuf->tsData.allocSize = MEM_BUF_SIZE;

  pTSBuf->assistBuf = malloc(MEM_BUF_SIZE);
  if (pTSBuf->assistBuf == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }

  pTSBuf->block.payload = malloc(MEM_BUF_SIZE);
  if (pTSBuf->block.payload == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }
  
  pTSBuf->fileSize += getDataStartOffset();
  return pTSBuf;
}

static int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader);

/**
 * todo error handling
 * support auto closeable tmp file
 * @param path
 * @return
 */
STSBuf* tsBufCreate(bool autoDelete) {
  STSBuf* pTSBuf = calloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }

  getTmpfilePath("join", pTSBuf->path);
  pTSBuf->f = fopen(pTSBuf->path, "w+");
  if (pTSBuf->f == NULL) {
    free(pTSBuf);
    return NULL;
  }

  if (NULL == allocResForTSBuf(pTSBuf)) {
    return NULL;
  }

  // update the header info
  STSBufFileHeader header = {.magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = TSQL_SO_ASC};
  STSBufUpdateHeader(pTSBuf, &header);

  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSQL_SO_ASC;

  pTSBuf->autoDelete = autoDelete;
  pTSBuf->tsOrder = -1;

  return pTSBuf;
}

STSBuf* tsBufCreateFromFile(const char* path, bool autoDelete) {
  STSBuf* pTSBuf = calloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }

  strncpy(pTSBuf->path, path, PATH_MAX);

  pTSBuf->f = fopen(pTSBuf->path, "r+");
  if (pTSBuf->f == NULL) {
    free(pTSBuf);
    return NULL;
  }

  if (allocResForTSBuf(pTSBuf) == NULL) {
    return NULL;
  }

  // validate the file magic number
  STSBufFileHeader header = {0};
  fseek(pTSBuf->f, 0, SEEK_SET);
  fread(&header, 1, sizeof(header), pTSBuf->f);

  // invalid file
  if (header.magic != TS_COMP_FILE_MAGIC) {
    return NULL;
  }

  if (header.numOfVnode > pTSBuf->numOfAlloc) {
    pTSBuf->numOfAlloc = header.numOfVnode;
    STSVnodeBlockInfoEx* tmp = realloc(pTSBuf->pData, sizeof(STSVnodeBlockInfoEx) * pTSBuf->numOfAlloc);
    if (tmp == NULL) {
      tsBufDestory(pTSBuf);
      return NULL;
    }

    pTSBuf->pData = tmp;
  }

  pTSBuf->numOfVnodes = header.numOfVnode;

  // check the ts order
  pTSBuf->tsOrder = header.tsOrder;
  if (pTSBuf->tsOrder != TSQL_SO_ASC && pTSBuf->tsOrder != TSQL_SO_DESC) {
    tscError("invalid order info in buf:%d", pTSBuf->tsOrder);
    tsBufDestory(pTSBuf);
    return NULL;
  }

  size_t infoSize = sizeof(STSVnodeBlockInfo) * pTSBuf->numOfVnodes;

  STSVnodeBlockInfo* buf = (STSVnodeBlockInfo*)calloc(1, infoSize);
  fread(buf, infoSize, 1, pTSBuf->f);

  // the length value for each vnode is not kept in file, so does not set the length value
  for (int32_t i = 0; i < pTSBuf->numOfVnodes; ++i) {
    STSVnodeBlockInfoEx* pBlockList = &pTSBuf->pData[i];
    memcpy(&pBlockList->info, &buf[i], sizeof(STSVnodeBlockInfo));
  }

  free(buf);

  fseek(pTSBuf->f, 0, SEEK_END);

  struct stat fileStat;
  fstat(fileno(pTSBuf->f), &fileStat);

  pTSBuf->fileSize = (uint32_t)fileStat.st_size;
  tsBufResetPos(pTSBuf);

  // ascending by default
  pTSBuf->cur.order = TSQL_SO_ASC;

  pTSBuf->autoDelete = autoDelete;

  tscTrace("create tsBuf from file:%s, fd:%d, size:%d, numOfVnode:%d, autoDelete:%d", pTSBuf->path, fileno(pTSBuf->f),
           pTSBuf->fileSize, pTSBuf->numOfVnodes, pTSBuf->autoDelete);

  return pTSBuf;
}

void* tsBufDestory(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return NULL;
  }

  tfree(pTSBuf->assistBuf);
  tfree(pTSBuf->tsData.rawBuf);

  tfree(pTSBuf->pData);
  tfree(pTSBuf->block.payload);
  
  tVariantDestroy(&pTSBuf->block.tag);

  fclose(pTSBuf->f);

  if (pTSBuf->autoDelete) {
    tscTrace("tsBuf %p destroyed, delete tmp file:%s", pTSBuf, pTSBuf->path);
    unlink(pTSBuf->path);
  } else {
    tscTrace("tsBuf %p destroyed, tmp file:%s, remains", pTSBuf, pTSBuf->path);
  }

  free(pTSBuf);
  return NULL;
}

static STSVnodeBlockInfoEx* tsBufGetLastVnodeInfo(STSBuf* pTSBuf) {
  int32_t last = pTSBuf->numOfVnodes - 1;

  assert(last >= 0);
  return &pTSBuf->pData[last];
}

static STSVnodeBlockInfoEx* addOneVnodeInfo(STSBuf* pTSBuf, int32_t vnodeId) {
  if (pTSBuf->numOfAlloc <= pTSBuf->numOfVnodes) {
    uint32_t newSize = (uint32_t)(pTSBuf->numOfAlloc * 1.5);
    assert(newSize > pTSBuf->numOfAlloc);

    STSVnodeBlockInfoEx* tmp = (STSVnodeBlockInfoEx*)realloc(pTSBuf->pData, sizeof(STSVnodeBlockInfoEx) * newSize);
    if (tmp == NULL) {
      return NULL;
    }

    pTSBuf->pData = tmp;
    pTSBuf->numOfAlloc = newSize;
    memset(&pTSBuf->pData[pTSBuf->numOfVnodes], 0, sizeof(STSVnodeBlockInfoEx) * (newSize - pTSBuf->numOfVnodes));
  }

  if (pTSBuf->numOfVnodes > 0) {
    STSVnodeBlockInfoEx* pPrevBlockInfoEx = tsBufGetLastVnodeInfo(pTSBuf);

    // update prev vnode length info in file
    TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, &pPrevBlockInfoEx->info);
  }

  // set initial value for vnode block
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[pTSBuf->numOfVnodes].info;
  pBlockInfo->vnode = vnodeId;
  pBlockInfo->offset = pTSBuf->fileSize;
  assert(pBlockInfo->offset >= getDataStartOffset());

  // update vnode info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes, pBlockInfo);

  // add one vnode info
  pTSBuf->numOfVnodes += 1;

  // update the header info
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};

  STSBufUpdateHeader(pTSBuf, &header);
  return tsBufGetLastVnodeInfo(pTSBuf);
}

static void shrinkBuffer(STSList* ptsData) {
  // shrink tmp buffer size if it consumes too many memory compared to the pre-defined size
  if (ptsData->allocSize >= ptsData->threshold * 2) {
    ptsData->rawBuf = realloc(ptsData->rawBuf, MEM_BUF_SIZE);
    ptsData->allocSize = MEM_BUF_SIZE;
  }
}

static void writeDataToDisk(STSBuf* pTSBuf) {
  if (pTSBuf->tsData.len == 0) {
    return;
  }

  STSBlock* pBlock = &pTSBuf->block;

  pBlock->numOfElem = pTSBuf->tsData.len / TSDB_KEYSIZE;
  pBlock->compLen =
      tsCompressTimestamp(pTSBuf->tsData.rawBuf, pTSBuf->tsData.len, pTSBuf->tsData.len / TSDB_KEYSIZE, pBlock->payload,
                          pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);

  /*int64_t r =*/ fseek(pTSBuf->f, pTSBuf->fileSize, SEEK_SET);

  /*
   * format for output data:
   * 1. tags, number of ts, size after compressed, payload, size after compressed
   * 2. tags, number of ts, size after compressed, payload, size after compressed
   *
   * both side has the compressed length is used to support load data forwards/backwords.
   */
  fwrite(&pBlock->tag.nType, sizeof(pBlock->tag.nType), 1, pTSBuf->f);
  fwrite(&pBlock->tag.nLen,  sizeof(pBlock->tag.nLen), 1, pTSBuf->f);
  
  if (pBlock->tag.nType == TSDB_DATA_TYPE_BINARY || pBlock->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    fwrite(pBlock->tag.pz, (size_t)pBlock->tag.nLen, 1, pTSBuf->f);
  } else if (pBlock->tag.nType != TSDB_DATA_TYPE_NULL) {
    fwrite(&pBlock->tag.i64Key, sizeof(int64_t), 1, pTSBuf->f);
  }
  
  fwrite(&pBlock->numOfElem, sizeof(pBlock->numOfElem), 1, pTSBuf->f);
  fwrite(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);
  fwrite(pBlock->payload, (size_t)pBlock->compLen,  1, pTSBuf->f);
  fwrite(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);

  int32_t blockSize = sizeof(pBlock->tag.nType) + sizeof(pBlock->tag.nLen) + pBlock->tag.nLen + sizeof(pBlock->numOfElem) + sizeof(pBlock->compLen) * 2 + pBlock->compLen;
  pTSBuf->fileSize += blockSize;

  pTSBuf->tsData.len = 0;

  STSVnodeBlockInfoEx* pVnodeBlockInfoEx = tsBufGetLastVnodeInfo(pTSBuf);

  pVnodeBlockInfoEx->info.compLen += blockSize;
  pVnodeBlockInfoEx->info.numOfBlocks += 1;

  shrinkBuffer(&pTSBuf->tsData);
}

static void expandBuffer(STSList* ptsData, int32_t inputSize) {
  if (ptsData->allocSize - ptsData->len < inputSize) {
    int32_t newSize = inputSize + ptsData->len;
    char*   tmp = realloc(ptsData->rawBuf, (size_t)newSize);
    if (tmp == NULL) {
      // todo
    }

    ptsData->rawBuf = tmp;
    ptsData->allocSize = newSize;
  }
}

STSBlock* readDataFromDisk(STSBuf* pTSBuf, int32_t order, bool decomp) {
  STSBlock* pBlock = &pTSBuf->block;

  // clear the memory buffer
  tVariant t = pBlock->tag;
  void* tmp = pBlock->payload;
  memset(pBlock, 0, sizeof(STSBlock));
  
  pBlock->payload = tmp;
  pBlock->tag = t;
  
  if (order == TSQL_SO_DESC) {
    /*
     * set the right position for the reversed traverse, the reversed traverse is started from
     * the end of each comp data block
     */
    fseek(pTSBuf->f, -sizeof(pBlock->padding), SEEK_CUR);
    fread(&pBlock->padding, sizeof(pBlock->padding), 1, pTSBuf->f);

    pBlock->compLen = pBlock->padding;
    int32_t offset = pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag.nType) + sizeof(pBlock->tag.nLen) + pBlock->tag.nLen ;
    fseek(pTSBuf->f, -offset, SEEK_CUR);
  }

  fread(&pBlock->tag.nType, sizeof(pBlock->tag.nType), 1, pTSBuf->f);
  fread(&pBlock->tag.nLen, sizeof(pBlock->tag.nLen), 1, pTSBuf->f);
  
  // NOTE: mix types tags are not supported
  if (pBlock->tag.nType == TSDB_DATA_TYPE_BINARY || pBlock->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    char* tp = realloc(pBlock->tag.pz, pBlock->tag.nLen + 1);
    assert(tp != NULL);
    
    memset(tp, 0, pBlock->tag.nLen + 1);
    pBlock->tag.pz = tp;
    
    fread(pBlock->tag.pz, (size_t)pBlock->tag.nLen, 1, pTSBuf->f);
  } else if (pBlock->tag.nType != TSDB_DATA_TYPE_NULL) {
    fread(&pBlock->tag.i64Key, sizeof(int64_t), 1, pTSBuf->f);
  }
  
  fread(&pBlock->numOfElem, sizeof(pBlock->numOfElem), 1, pTSBuf->f);

  fread(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);
  fread(pBlock->payload, (size_t)pBlock->compLen, 1, pTSBuf->f);

  if (decomp) {
    pTSBuf->tsData.len =
        tsDecompressTimestamp(pBlock->payload, pBlock->compLen, pBlock->numOfElem, pTSBuf->tsData.rawBuf,
                              pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);
  }

  // read the comp length at the length of comp block
  fread(&pBlock->padding, sizeof(pBlock->padding), 1, pTSBuf->f);

  // for backwards traverse, set the start position at the end of previous block
  if (order == TSQL_SO_DESC) {
    int32_t offset = pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    int64_t r = fseek(pTSBuf->f, -offset, SEEK_CUR);
    UNUSED(r);
  }

  return pBlock;
}

// set the order of ts buffer if the ts order has not been set yet
static int32_t setCheckTSOrder(STSBuf* pTSBuf, const char* pData, int32_t len) {
  STSList* ptsData = &pTSBuf->tsData;

  if (pTSBuf->tsOrder == -1) {
    if (ptsData->len > 0) {
      TSKEY lastKey = *(TSKEY*)(ptsData->rawBuf + ptsData->len - TSDB_KEYSIZE);

      if (lastKey > *(TSKEY*)pData) {
        pTSBuf->tsOrder = TSQL_SO_DESC;
      } else {
        pTSBuf->tsOrder = TSQL_SO_ASC;
      }
    } else if (len > TSDB_KEYSIZE) {
      // no data in current vnode, more than one ts is added, check the orders
      TSKEY k1 = *(TSKEY*)(pData);
      TSKEY k2 = *(TSKEY*)(pData + TSDB_KEYSIZE);

      if (k1 < k2) {
        pTSBuf->tsOrder = TSQL_SO_ASC;
      } else if (k1 > k2) {
        pTSBuf->tsOrder = TSQL_SO_DESC;
      } else {
        // todo handle error
      }
    }
  } else {
    // todo the timestamp order is set, check the asc/desc order of appended data
  }

  return TSDB_CODE_SUCCESS;
}

void tsBufAppend(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag, const char* pData, int32_t len) {
  STSVnodeBlockInfoEx* pBlockInfo = NULL;
  
  STSList* ptsData = &pTSBuf->tsData;
  int32_t  tagEqual = 0;

  if (pTSBuf->numOfVnodes == 0 || tsBufGetLastVnodeInfo(pTSBuf)->info.vnode != vnodeId) {
    writeDataToDisk(pTSBuf);
    shrinkBuffer(ptsData);

    pBlockInfo = addOneVnodeInfo(pTSBuf, vnodeId);
  } else {
    pBlockInfo = tsBufGetLastVnodeInfo(pTSBuf);
  }

  assert(pBlockInfo->info.vnode == vnodeId);
  tagEqual = tVariantCompare(&pTSBuf->block.tag, tag);

  if (tagEqual != 0 && ptsData->len > 0) {
    // new arrived data with different tags value, save current value into disk first
    writeDataToDisk(pTSBuf);
  } else {
    expandBuffer(ptsData, len);
  }
  
  tVariantDestroy(&pTSBuf->block.tag);
  tVariantAssign(&pTSBuf->block.tag, tag);
  
  memcpy(ptsData->rawBuf + ptsData->len, pData, (size_t)len);

  // todo check return value
  setCheckTSOrder(pTSBuf, pData, len);

  ptsData->len += len;
  pBlockInfo->len += len;

  pTSBuf->numOfTotal += len / TSDB_KEYSIZE;

  // the size of raw data exceeds the size of the default prepared buffer, so
  // during getBufBlock, the output buffer needs to be large enough.
  if (ptsData->len >= ptsData->threshold) {
    writeDataToDisk(pTSBuf);
    shrinkBuffer(ptsData);
  }

  tsBufResetPos(pTSBuf);
}

void tsBufFlush(STSBuf* pTSBuf) {
  if (pTSBuf->tsData.len <= 0) {
    return;
  }

  writeDataToDisk(pTSBuf);
  shrinkBuffer(&pTSBuf->tsData);

  STSVnodeBlockInfoEx* pBlockInfoEx = tsBufGetLastVnodeInfo(pTSBuf);

  // update prev vnode length info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, &pBlockInfoEx->info);

  // save the ts order into header
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);

  fsync(fileno(pTSBuf->f));
}

static int32_t tsBufFindVnodeIndexFromId(STSVnodeBlockInfoEx* pVnodeInfoEx, int32_t numOfVnodes, int32_t vnodeId) {
  int32_t j = -1;
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (pVnodeInfoEx[i].info.vnode == vnodeId) {
      j = i;
      break;
    }
  }

  return j;
}

// todo opt performance by cache blocks info
static int32_t tsBufFindBlock(STSBuf* pTSBuf, STSVnodeBlockInfo* pBlockInfo, int32_t blockIndex) {
  if (fseek(pTSBuf->f, pBlockInfo->offset, SEEK_SET) != 0) {
    return -1;
  }

  // sequentially read the compressed data blocks, start from the beginning of the comp data block of this vnode
  int32_t i = 0;
  bool    decomp = false;

  while ((i++) <= blockIndex) {
    if (readDataFromDisk(pTSBuf, TSQL_SO_ASC, decomp) == NULL) {
      return -1;
    }
  }

  // set the file position to be the end of previous comp block
  if (pTSBuf->cur.order == TSQL_SO_DESC) {
    STSBlock* pBlock = &pTSBuf->block;
    int32_t   compBlockSize =
        pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    fseek(pTSBuf->f, -compBlockSize, SEEK_CUR);
  }

  return 0;
}

static int32_t tsBufFindBlockByTag(STSBuf* pTSBuf, STSVnodeBlockInfo* pBlockInfo, tVariant* tag) {
  bool decomp = false;

  int64_t offset = 0;
  if (pTSBuf->cur.order == TSQL_SO_ASC) {
    offset = pBlockInfo->offset;
  } else {  // reversed traverse starts from the end of block
    offset = pBlockInfo->offset + pBlockInfo->compLen;
  }

  if (fseek(pTSBuf->f, offset, SEEK_SET) != 0) {
    return -1;
  }

  for (int32_t i = 0; i < pBlockInfo->numOfBlocks; ++i) {
    if (readDataFromDisk(pTSBuf, pTSBuf->cur.order, decomp) == NULL) {
      return -1;
    }

    if (0 == tVariantCompare(&pTSBuf->block.tag, tag)) {
      return i;
    }
  }

  return -1;
}

static void tsBufGetBlock(STSBuf* pTSBuf, int32_t vnodeIndex, int32_t blockIndex) {
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[vnodeIndex].info;
  if (pBlockInfo->numOfBlocks <= blockIndex) {
    assert(false);
  }

  STSCursor* pCur = &pTSBuf->cur;
  if (pCur->vnodeIndex == vnodeIndex && ((pCur->blockIndex <= blockIndex && pCur->order == TSQL_SO_ASC) ||
                                         (pCur->blockIndex >= blockIndex && pCur->order == TSQL_SO_DESC))) {
    int32_t i = 0;
    bool    decomp = false;
    int32_t step = abs(blockIndex - pCur->blockIndex);

    while ((++i) <= step) {
      if (readDataFromDisk(pTSBuf, pCur->order, decomp) == NULL) {
        return;
      }
    }
  } else {
    if (tsBufFindBlock(pTSBuf, pBlockInfo, blockIndex) == -1) {
      assert(false);
    }
  }

  STSBlock* pBlock = &pTSBuf->block;

  size_t s = pBlock->numOfElem * TSDB_KEYSIZE;

  /*
   * In order to accommodate all the qualified data, the actual buffer size for one block with identical tags value
   * may exceed the maximum allowed size during *tsBufAppend* function by invoking expandBuffer function
   */
  if (s > pTSBuf->tsData.allocSize) {
    expandBuffer(&pTSBuf->tsData, s);
  }

  pTSBuf->tsData.len =
      tsDecompressTimestamp(pBlock->payload, pBlock->compLen, pBlock->numOfElem, pTSBuf->tsData.rawBuf,
                            pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);

  assert((pTSBuf->tsData.len / TSDB_KEYSIZE == pBlock->numOfElem) && (pTSBuf->tsData.allocSize >= pTSBuf->tsData.len));

  pCur->vnodeIndex = vnodeIndex;
  pCur->blockIndex = blockIndex;

  pCur->tsIndex = (pCur->order == TSQL_SO_ASC) ? 0 : pBlock->numOfElem - 1;
}

STSVnodeBlockInfo* tsBufGetVnodeBlockInfo(STSBuf* pTSBuf, int32_t vnodeId) {
  int32_t j = tsBufFindVnodeIndexFromId(pTSBuf->pData, pTSBuf->numOfVnodes, vnodeId);
  if (j == -1) {
    return NULL;
  }

  return &pTSBuf->pData[j].info;
}

int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader) {
  if ((pTSBuf->f == NULL) || pHeader == NULL || pHeader->numOfVnode < 0 || pHeader->magic != TS_COMP_FILE_MAGIC) {
    return -1;
  }

  int64_t r = fseek(pTSBuf->f, 0, SEEK_SET);
  if (r != 0) {
    return -1;
  }

  fwrite(pHeader, sizeof(STSBufFileHeader), 1, pTSBuf->f);
  return 0;
}

bool tsBufNextPos(STSBuf* pTSBuf) {
  if (pTSBuf == NULL || pTSBuf->numOfVnodes == 0) {
    return false;
  }

  STSCursor* pCur = &pTSBuf->cur;

  // get the first/last position according to traverse order
  if (pCur->vnodeIndex == -1) {
    if (pCur->order == TSQL_SO_ASC) {
      tsBufGetBlock(pTSBuf, 0, 0);

      if (pTSBuf->block.numOfElem == 0) {  // the whole list is empty, return
        tsBufResetPos(pTSBuf);
        return false;
      } else {
        return true;
      }

    } else {  // get the last timestamp record in the last block of the last vnode
      assert(pTSBuf->numOfVnodes > 0);

      int32_t vnodeIndex = pTSBuf->numOfVnodes - 1;
      pCur->vnodeIndex = vnodeIndex;

      int32_t            vnodeId = pTSBuf->pData[pCur->vnodeIndex].info.vnode;
      STSVnodeBlockInfo* pBlockInfo = tsBufGetVnodeBlockInfo(pTSBuf, vnodeId);
      int32_t            blockIndex = pBlockInfo->numOfBlocks - 1;

      tsBufGetBlock(pTSBuf, vnodeIndex, blockIndex);

      pCur->tsIndex = pTSBuf->block.numOfElem - 1;
      if (pTSBuf->block.numOfElem == 0) {
        tsBufResetPos(pTSBuf);
        return false;
      } else {
        return true;
      }
    }
  }

  int32_t step = pCur->order == TSQL_SO_ASC ? 1 : -1;

  while (1) {
    assert(pTSBuf->tsData.len == pTSBuf->block.numOfElem * TSDB_KEYSIZE);

    if ((pCur->order == TSQL_SO_ASC && pCur->tsIndex >= pTSBuf->block.numOfElem - 1) ||
        (pCur->order == TSQL_SO_DESC && pCur->tsIndex <= 0)) {
      int32_t vnodeId = pTSBuf->pData[pCur->vnodeIndex].info.vnode;

      STSVnodeBlockInfo* pBlockInfo = tsBufGetVnodeBlockInfo(pTSBuf, vnodeId);
      if (pBlockInfo == NULL || (pCur->blockIndex >= pBlockInfo->numOfBlocks - 1 && pCur->order == TSQL_SO_ASC) ||
          (pCur->blockIndex <= 0 && pCur->order == TSQL_SO_DESC)) {
        if ((pCur->vnodeIndex >= pTSBuf->numOfVnodes - 1 && pCur->order == TSQL_SO_ASC) ||
            (pCur->vnodeIndex <= 0 && pCur->order == TSQL_SO_DESC)) {
          pCur->vnodeIndex = -1;
          return false;
        }
        
        if (pBlockInfo == NULL) {
          return false;
        }

        int32_t blockIndex = pCur->order == TSQL_SO_ASC ? 0 : pBlockInfo->numOfBlocks - 1;
        tsBufGetBlock(pTSBuf, pCur->vnodeIndex + step, blockIndex);
        break;

      } else {
        tsBufGetBlock(pTSBuf, pCur->vnodeIndex, pCur->blockIndex + step);
        break;
      }
    } else {
      pCur->tsIndex += step;
      break;
    }
  }

  return true;
}

void tsBufResetPos(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return;
  }

  pTSBuf->cur = (STSCursor){.tsIndex = -1, .blockIndex = -1, .vnodeIndex = -1, .order = pTSBuf->cur.order};
}

STSElem tsBufGetElem(STSBuf* pTSBuf) {
  STSElem    elem1 = {.vnode = -1};
  STSCursor* pCur = &pTSBuf->cur;

  if (pTSBuf == NULL || pCur->vnodeIndex < 0) {
    return elem1;
  }

  STSBlock* pBlock = &pTSBuf->block;

  elem1.vnode = pTSBuf->pData[pCur->vnodeIndex].info.vnode;
  elem1.ts = *(TSKEY*)(pTSBuf->tsData.rawBuf + pCur->tsIndex * TSDB_KEYSIZE);
  elem1.tag.nType = pBlock->tag.nType;
  elem1.tag.nLen = pBlock->tag.nLen;
  elem1.tag.pz = pBlock->tag.pz;

  return elem1;
}

/**
 * current only support ts comp data from two vnode merge
 * @param pDestBuf
 * @param pSrcBuf
 * @param vnodeId
 * @return
 */
int32_t tsBufMerge(STSBuf* pDestBuf, const STSBuf* pSrcBuf, int32_t vnodeId) {
  if (pDestBuf == NULL || pSrcBuf == NULL || pSrcBuf->numOfVnodes <= 0) {
    return 0;
  }

  if (pDestBuf->numOfVnodes + pSrcBuf->numOfVnodes > TS_COMP_FILE_VNODE_MAX) {
    return -1;
  }

  // src can only have one vnode index
  if (pSrcBuf->numOfVnodes > 1) {
    return -1;
  }

  // there are data in buffer, flush to disk first
  tsBufFlush(pDestBuf);

  // compared with the last vnode id
  if (vnodeId != tsBufGetLastVnodeInfo(pDestBuf)->info.vnode) {
    int32_t oldSize = pDestBuf->numOfVnodes;
    int32_t newSize = oldSize + pSrcBuf->numOfVnodes;

    if (pDestBuf->numOfAlloc < newSize) {
      pDestBuf->numOfAlloc = newSize;

      STSVnodeBlockInfoEx* tmp = realloc(pDestBuf->pData, sizeof(STSVnodeBlockInfoEx) * newSize);
      if (tmp == NULL) {
        return -1;
      }

      pDestBuf->pData = tmp;
    }

    // directly copy the vnode index information
    memcpy(&pDestBuf->pData[oldSize], pSrcBuf->pData, (size_t)pSrcBuf->numOfVnodes * sizeof(STSVnodeBlockInfoEx));

    // set the new offset value
    for (int32_t i = 0; i < pSrcBuf->numOfVnodes; ++i) {
      STSVnodeBlockInfoEx* pBlockInfoEx = &pDestBuf->pData[i + oldSize];
      pBlockInfoEx->info.offset = (pSrcBuf->pData[i].info.offset - getDataStartOffset()) + pDestBuf->fileSize;
      pBlockInfoEx->info.vnode = vnodeId;
    }

    pDestBuf->numOfVnodes = newSize;
  } else {
    STSVnodeBlockInfoEx* pBlockInfoEx = tsBufGetLastVnodeInfo(pDestBuf);

    pBlockInfoEx->len += pSrcBuf->pData[0].len;
    pBlockInfoEx->info.numOfBlocks += pSrcBuf->pData[0].info.numOfBlocks;
    pBlockInfoEx->info.compLen += pSrcBuf->pData[0].info.compLen;
    pBlockInfoEx->info.vnode = vnodeId;
  }

  int32_t r = fseek(pDestBuf->f, 0, SEEK_END);
  assert(r == 0);

  int64_t offset = getDataStartOffset();
  int32_t size = pSrcBuf->fileSize - offset;

#ifdef LINUX
  ssize_t rc = tsendfile(fileno(pDestBuf->f), fileno(pSrcBuf->f), &offset, size);
#else
  ssize_t rc = fsendfile(pDestBuf->f, pSrcBuf->f, &offset, size);
#endif

  if (rc == -1) {
    tscError("failed to merge tsBuf from:%s to %s, reason:%s\n", pSrcBuf->path, pDestBuf->path, strerror(errno));
    return -1;
  }

  if (rc != size) {
    tscError("failed to merge tsBuf from:%s to %s, reason:%s\n", pSrcBuf->path, pDestBuf->path, strerror(errno));
    return -1;
  }

  pDestBuf->numOfTotal += pSrcBuf->numOfTotal;

  int32_t oldSize = pDestBuf->fileSize;

  struct stat fileStat;
  fstat(fileno(pDestBuf->f), &fileStat);
  pDestBuf->fileSize = (uint32_t)fileStat.st_size;

  assert(pDestBuf->fileSize == oldSize + size);

  tscTrace("tsBuf merge success, %p, path:%s, fd:%d, file size:%d, numOfVnode:%d, autoDelete:%d", pDestBuf,
           pDestBuf->path, fileno(pDestBuf->f), pDestBuf->fileSize, pDestBuf->numOfVnodes, pDestBuf->autoDelete);

  return 0;
}

STSBuf* tsBufCreateFromCompBlocks(const char* pData, int32_t numOfBlocks, int32_t len, int32_t order) {
  STSBuf* pTSBuf = tsBufCreate(true);

  STSVnodeBlockInfo* pBlockInfo = &(addOneVnodeInfo(pTSBuf, 0)->info);
  pBlockInfo->numOfBlocks = numOfBlocks;
  pBlockInfo->compLen = len;
  pBlockInfo->offset = getDataStartOffset();
  pBlockInfo->vnode = 0;

  // update prev vnode length info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, pBlockInfo);

  fseek(pTSBuf->f, pBlockInfo->offset, SEEK_SET);
  fwrite((void*)pData, 1, len, pTSBuf->f);
  pTSBuf->fileSize += len;

  pTSBuf->tsOrder = order;
  assert(order == TSQL_SO_ASC || order == TSQL_SO_DESC);

  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);

  fsync(fileno(pTSBuf->f));

  return pTSBuf;
}

STSElem tsBufGetElemStartPos(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag) {
  STSElem elem = {.vnode = -1};

  if (pTSBuf == NULL) {
    return elem;
  }

  int32_t j = tsBufFindVnodeIndexFromId(pTSBuf->pData, pTSBuf->numOfVnodes, vnodeId);
  if (j == -1) {
    return elem;
  }

  // for debug purpose
  //  tsBufDisplay(pTSBuf);

  STSCursor*         pCur = &pTSBuf->cur;
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[j].info;

  int32_t blockIndex = tsBufFindBlockByTag(pTSBuf, pBlockInfo, tag);
  if (blockIndex < 0) {
    return elem;
  }

  pCur->vnodeIndex = j;
  pCur->blockIndex = blockIndex;
  tsBufGetBlock(pTSBuf, j, blockIndex);

  return tsBufGetElem(pTSBuf);
}

STSCursor tsBufGetCursor(STSBuf* pTSBuf) {
  STSCursor c = {.vnodeIndex = -1};
  if (pTSBuf == NULL) {
    return c;
  }

  return pTSBuf->cur;
}

void tsBufSetCursor(STSBuf* pTSBuf, STSCursor* pCur) {
  if (pTSBuf == NULL || pCur == NULL) {
    return;
  }

  //  assert(pCur->vnodeIndex != -1 && pCur->tsIndex >= 0 && pCur->blockIndex >= 0);
  if (pCur->vnodeIndex != -1) {
    tsBufGetBlock(pTSBuf, pCur->vnodeIndex, pCur->blockIndex);
  }

  pTSBuf->cur = *pCur;
}

void tsBufSetTraverseOrder(STSBuf* pTSBuf, int32_t order) {
  if (pTSBuf == NULL) {
    return;
  }

  pTSBuf->cur.order = order;
}

STSBuf* tsBufClone(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return NULL;
  }

  return tsBufCreateFromFile(pTSBuf->path, false);
}

void tsBufDisplay(STSBuf* pTSBuf) {
  printf("-------start of ts comp file-------\n");
  printf("number of vnode:%d\n", pTSBuf->numOfVnodes);

  int32_t old = pTSBuf->cur.order;
  pTSBuf->cur.order = TSQL_SO_ASC;

  tsBufResetPos(pTSBuf);

  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    printf("%d-%" PRId64 "\n", elem.vnode, elem.ts);
  }

  pTSBuf->cur.order = old;
  printf("-------end of ts comp file-------\n");
}
