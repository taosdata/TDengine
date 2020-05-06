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
#include <tcompare.h>
#include <tschemautil.h>
#include "os.h"
#include "qtsbuf.h"
#include "tscLog.h"
#include "tsclient.h"

typedef struct SInsertSupporter {
  SSubqueryState* pState;
  SSqlObj*  pSql;
} SInsertSupporter;

static void freeJoinSubqueryObj(SSqlObj* pSql);
static bool tscHashRemainDataInSubqueryResultSet(SSqlObj *pSql);

static bool tsCompare(int32_t order, int64_t left, int64_t right) {
  if (order == TSDB_ORDER_ASC) {
    return left < right;
  } else {
    return left > right;
  }
}

static int64_t doTSBlockIntersect(SSqlObj* pSql, SJoinSupporter* pSupporter1,
                                  SJoinSupporter* pSupporter2, TSKEY* st, TSKEY* et) {
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
    tscPrint("%" PRId64 ", tags:%d \t %" PRId64 ", tags:%d", elem1.ts, elem1.tag, elem2.ts, elem2.tag);
#endif

    if (elem1.tag < elem2.tag || (elem1.tag == elem2.tag && tsCompare(order, elem1.ts, elem2.ts))) {
      if (!tsBufNextPos(pSupporter1->pTSBuf)) {
        break;
      }

      numOfInput1++;
    } else if (elem1.tag > elem2.tag || (elem1.tag == elem2.tag && tsCompare(order, elem2.ts, elem1.ts))) {
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
SJoinSupporter* tscCreateJoinSupporter(SSqlObj* pSql, SSubqueryState* pState, int32_t index) {
  SJoinSupporter* pSupporter = calloc(1, sizeof(SJoinSupporter));
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

void tscDestroyJoinSupporter(SJoinSupporter* pSupporter) {
  if (pSupporter == NULL) {
    return;
  }

  if (pSupporter->exprList != NULL) {
    tscSqlExprInfoDestroy(pSupporter->exprList);
  }
  
  if (pSupporter->colList != NULL) {
    tscColumnListDestroy(pSupporter->colList);
  }

  tscFieldInfoClear(&pSupporter->fieldsInfo);

  if (pSupporter->f != NULL) {
    fclose(pSupporter->f);
    unlink(pSupporter->path);
    pSupporter->f = NULL;
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
  int32_t         numOfSub = 0;
  SJoinSupporter* pSupporter = NULL;
  
  /*
   * If the columns are not involved in the final select clause,
   * the corresponding query will not be issued.
   */
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    pSupporter = pSql->pSubs[i]->param;
    if (taosArrayGetSize(pSupporter->exprList) > 0) {
      ++numOfSub;
    }
  }
  
  assert(numOfSub > 0);
  
  // scan all subquery, if one sub query has only ts, ignore it
  tscTrace("%p start to launch secondary subqueries, total:%d, only:%d needs to query, others are not retrieve in "
      "select clause", pSql, pSql->numOfSubs, numOfSub);

  //the subqueries that do not actually launch the secondary query to virtual node is set as completed.
  SSubqueryState* pState = pSupporter->pState;
  pState->numOfTotal = pSql->numOfSubs;
  pState->numOfCompleted = (pSql->numOfSubs - numOfSub);
  
  bool success = true;
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj *pPrevSub = pSql->pSubs[i];
    pSql->pSubs[i] = NULL;
    
    pSupporter = pPrevSub->param;
  
    if (taosArrayGetSize(pSupporter->exprList) == 0) {
      tscTrace("%p subIndex: %d, no need to launch query, ignore it", pSql, i);
    
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
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE);
  
    pQueryInfo->intervalTime = pSupporter->interval;
    pQueryInfo->groupbyExpr = pSupporter->groupbyExpr;
    
    tscTagCondCopy(&pQueryInfo->tagCond, &pSupporter->tagCond);
  
    pQueryInfo->colList = pSupporter->colList;
    pQueryInfo->exprList = pSupporter->exprList;
    pQueryInfo->fieldsInfo = pSupporter->fieldsInfo;
    
    pSupporter->exprList = NULL;
    pSupporter->colList = NULL;
    memset(&pSupporter->fieldsInfo, 0, sizeof(SFieldInfo));
  
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
      SSqlExpr* pExpr = tscSqlExprGet(pNewQueryInfo, 0);
      assert(pQueryInfo->tagCond.joinInfo.hasJoin);
    
      int16_t tagColIndex = tscGetJoinTagColIndexByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->uid);
      pExpr->param[0].i64Key = tagColIndex;
      pExpr->numOfParams = 1;
    }
  
    size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
    tscTrace("%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type,
             taosArrayGetSize(pNewQueryInfo->exprList), numOfCols,
             pNewQueryInfo->fieldsInfo.numOfOutput, pNewQueryInfo->pTableMetaInfo[0]->name);
  }
  
  //prepare the subqueries object failed, abort
  if (!success) {
    pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("%p failed to prepare subqueries objs for secondary phase query, numOfSub:%d, code:%d", pSql,
        pSql->numOfSubs, pSql->res.code);
    freeJoinSubqueryObj(pSql);
    
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

void freeJoinSubqueryObj(SSqlObj* pSql) {
  SSubqueryState* pState = NULL;

  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }
    
    SJoinSupporter* p = pSub->param;
    pState = p->pState;

    tscDestroyJoinSupporter(p);

    if (pSub->res.code == TSDB_CODE_SUCCESS) {
      taos_free_result(pSub);
    }
  }

  tfree(pState);
  pSql->numOfSubs = 0;
}

static void quitAllSubquery(SSqlObj* pSqlObj, SJoinSupporter* pSupporter) {
  int32_t numOfTotal = pSupporter->pState->numOfTotal;
  int32_t finished = atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1);
  
  pSqlObj->res.code = pSupporter->pState->code;
  if (finished >= numOfTotal) {
    tscError("%p all subquery return and query failed, global code:%d", pSqlObj, pSqlObj->res.code);
    freeJoinSubqueryObj(pSqlObj);
  }
}

// update the query time range according to the join results on timestamp
static void updateQueryTimeRange(SQueryInfo* pQueryInfo, int64_t st, int64_t et) {
  assert(pQueryInfo->window.skey <= st && pQueryInfo->window.ekey >= et);

  pQueryInfo->window.skey = st;
  pQueryInfo->window.ekey = et;
}

static void tSIntersectionAndLaunchSecQuery(SJoinSupporter* pSupporter, SSqlObj* pSql) {
  SSqlObj* pParentSql = pSupporter->pObj;
//  SSqlCmd* pCmd = &pSql->cmd;
//  SSqlRes* pRes = &pSql->res;
  
//  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  SQueryInfo* pParentQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, pParentSql->cmd.clauseIndex);
  
//  if (tscNonOrderedProjectionQueryOnSTable(pParentQueryInfo, 0)) {
//    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
//    assert(pQueryInfo->numOfTables == 1);
//
//    // for projection query, need to try next vnode
////        int32_t totalVnode = pTableMetaInfo->pMetricMeta->numOfVnodes;
//    int32_t totalVnode = 0;
//    if ((++pTableMetaInfo->vgroupIndex) < totalVnode) {
//      tscTrace("%p current vnode:%d exhausted, try next:%d. total vnode:%d. current numOfRes:%d", pSql,
//               pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVnode, pRes->numOfTotal);
//
//      pSql->cmd.command = TSDB_SQL_SELECT;
//      pSql->fp = tscJoinQueryCallback;
//      tscProcessSql(pSql);
//
//      return;
//    }
//  }
  
  int32_t numOfTotal = pSupporter->pState->numOfTotal;
  int32_t finished = atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1);
  
  if (finished >= numOfTotal) {
    assert(finished == numOfTotal);
    
    if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
      tscTrace("%p sub:%p, numOfSub:%d, quit from further procedure due to other queries failure", pParentSql, pSql,
               pSupporter->subqueryIndex);
      freeJoinSubqueryObj(pParentSql);
      return;
    }
    
    tscTrace("%p all subqueries retrieve ts complete, do ts block intersect", pParentSql);
    
    SJoinSupporter* p1 = pParentSql->pSubs[0]->param;
    SJoinSupporter* p2 = pParentSql->pSubs[1]->param;
    
    TSKEY st, et;
    int64_t num = doTSBlockIntersect(pParentSql, p1, p2, &st, &et);
    if (num <= 0) {  // no result during ts intersect
      tscTrace("%p free all sub SqlObj and quit", pParentSql);
      freeJoinSubqueryObj(pParentSql);
    } else {
      updateQueryTimeRange(pParentQueryInfo, st, et);
      tscLaunchSecondPhaseSubqueries(pParentSql);
    }
  }
}

int32_t tagsOrderCompar(const void* p1, const void* p2) {
  STidTags* t1 = (STidTags*) p1;
  STidTags* t2 = (STidTags*) p2;
  
  if (t1->vgId != t2->vgId) {
    return (t1->vgId > t2->vgId)? 1:-1;
  } else {
    if (t1->tid != t2->tid) {
      return (t1->tid > t2->tid)? 1:-1;
    } else {
      return 0;
    }
  }
}

static void doBuildVgroupTableInfo(SArray* res, STableMetaInfo* pTableMetaInfo) {
  SArray* pGroup = taosArrayInit(4, sizeof(SVgroupTableInfo));
  
  SArray* vgTableIdItem = taosArrayInit(4, sizeof(STableIdInfo));
  int32_t size = taosArrayGetSize(res);
  
  STidTags* prev = taosArrayGet(res, 0);
  int32_t prevVgId = prev->vgId;
  
  STableIdInfo item = {.uid = prev->uid, .tid = prev->tid, .key = INT64_MIN};
  taosArrayPush(vgTableIdItem, &item);
  
  for(int32_t k = 1; k < size; ++k) {
    STidTags* t1 = taosArrayGet(res, k);
    if (prevVgId != t1->vgId) {
      
      SVgroupTableInfo info = {0};
      
      SVgroupsInfo* pvg = pTableMetaInfo->vgroupList;
      for(int32_t m = 0; m < pvg->numOfVgroups; ++m) {
        if (prevVgId == pvg->vgroups[m].vgId) {
          info.vgInfo = pvg->vgroups[m];
          break;
        }
      }
      
      assert(info.vgInfo.numOfIps != 0);
      info.itemList = vgTableIdItem;
      taosArrayPush(pGroup, &info);
      
      vgTableIdItem = taosArrayInit(4, sizeof(STableIdInfo));
      STableIdInfo item1 = {.uid = t1->uid, .tid = t1->tid, .key = INT64_MIN};
      taosArrayPush(vgTableIdItem, &item1);
      prevVgId = t1->vgId;
    } else {
      taosArrayPush(vgTableIdItem, &item);
    }
  }
  
  if (taosArrayGetSize(vgTableIdItem) > 0) {
    SVgroupTableInfo info = {0};
    SVgroupsInfo* pvg = pTableMetaInfo->vgroupList;
    
    for(int32_t m = 0; m < pvg->numOfVgroups; ++m) {
      if (prevVgId == pvg->vgroups[m].vgId) {
        info.vgInfo = pvg->vgroups[m];
        break;
      }
    }
    
    assert(info.vgInfo.numOfIps != 0);
    info.itemList = vgTableIdItem;
    taosArrayPush(pGroup, &info);
  }
  
  pTableMetaInfo->pVgroupTables = pGroup;
}

static void issueTSCompQuery(SSqlObj* pSql, SJoinSupporter* pSupporter, SSqlObj* pParent) {
  SSqlCmd* pCmd = &pSql->cmd;
  tscClearSubqueryInfo(pCmd);
  tscFreeSqlResult(pSql);
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  tscInitQueryInfo(pQueryInfo);

  TSDB_QUERY_CLEAR_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
  TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_MULTITABLE_QUERY);
  
  pCmd->command = TSDB_SQL_SELECT;
  pSql->fp = tscJoinQueryCallback;
  
  SSchema colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
  
  SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscAddSpecialColumnForSelect(pQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);
  
  // set the tags value for ts_comp function
  SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, 0);
  int16_t tagColIndex = tscGetJoinTagColIndexByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->uid);
  
  pExpr->param->i64Key = tagColIndex;
  pExpr->numOfParams = 1;
  
  // add the filter tag column
  if (pSupporter->colList != NULL) {
    size_t s = taosArrayGetSize(pSupporter->colList);
    
    for (int32_t i = 0; i < s; ++i) {
      SColumn *pCol = taosArrayGetP(pSupporter->colList, i);
      
      if (pCol->numOfFilters > 0) {  // copy to the pNew->cmd.colList if it is filtered.
        SColumn *p = tscColumnClone(pCol);
        taosArrayPush(pQueryInfo->colList, &p);
      }
    }
  }
  
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  
  tscTrace(
      "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, transfer to ts_comp query to retrieve timestamps, "
      "exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
      pParent, pSql, 0, pTableMetaInfo->vgroupIndex, pQueryInfo->type, tscSqlExprNumOfExprs(pQueryInfo),
      numOfCols, pQueryInfo->fieldsInfo.numOfOutput, pTableMetaInfo->name);
  
  tscProcessSql(pSql);
}

static void joinRetrieveCallback(void* param, TAOS_RES* tres, int numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;
  
  SSqlObj* pParentSql = pSupporter->pObj;
  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
//  if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
//    tscError("%p abort query due to other subquery failure. code:%d, global code:%s", pSql, numOfRows,
//             tstrerror(pSupporter->pState->code));
//
//    quitAllSubquery(pParentSql, pSupporter);
//    return;
//  }
//
//  if (numOfRows < 0) {
//    tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);
//    pSupporter->pState->code = numOfRows;
//    quitAllSubquery(pParentSql, pSupporter);
//    return;
//  }
  
  // response of tag retrieve
  if (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY)) {
    if (numOfRows == 0 || pSql->res.completed) {
      
      if (numOfRows > 0) {
        size_t length = pSupporter->totalLen + pSql->res.rspLen;
        char* tmp = realloc(pSupporter->pIdTagList, length);
        assert(tmp != NULL);
        pSupporter->pIdTagList = tmp;
        
        memcpy(pSupporter->pIdTagList, pSql->res.data, pSql->res.rspLen);
        pSupporter->totalLen += pSql->res.rspLen;
        pSupporter->num += pSql->res.numOfRows;
      }
      
      int32_t numOfTotal = pSupporter->pState->numOfTotal;
      int32_t finished = atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1);
  
      if (finished < numOfTotal) {
        return;
      }
      
      // all subqueries are returned, start to compare the tags
      assert(finished == numOfTotal);
      tscTrace("%p all subqueries retrieve tags complete, do tags match", pParentSql);
  
      SJoinSupporter* p1 = pParentSql->pSubs[0]->param;
      SJoinSupporter* p2 = pParentSql->pSubs[1]->param;
  
      qsort(p1->pIdTagList, p1->num, p1->tagSize, tagsOrderCompar);
      qsort(p2->pIdTagList, p2->num, p2->tagSize, tagsOrderCompar);
      
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
      SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);// todo: tags mismatch, tags not completed
  
      SColumn *pCol = taosArrayGetP(pTableMetaInfo->tagColList, 0);
      SSchema *pColSchema = &pSchema[pCol->colIndex.columnIndex];
      
      SArray* s1 = taosArrayInit(p1->num, p1->tagSize);
      SArray* s2 = taosArrayInit(p2->num, p2->tagSize);
      
      int32_t i = 0, j = 0;
      while(i < p1->num && j < p2->num) {
        STidTags* pp1 = (STidTags*) p1->pIdTagList + i * p1->tagSize;
        STidTags* pp2 = (STidTags*) p2->pIdTagList + j * p2->tagSize;
       
        int32_t ret = doCompare(pp1->tag, pp2->tag, pColSchema->type, pColSchema->bytes);
        if (ret == 0) {
          taosArrayPush(s1, pp1);
          taosArrayPush(s2, pp2);
          j++;
          i++;
        } else if (ret > 0) {
          j++;
        } else {
          i++;
        }
      }
      
      if (taosArrayGetSize(s1) == 0 || taosArrayGetSize(s2) == 0) {// no results,return.
        tscTrace("%p free all sub SqlObj and quit", pParentSql);
        freeJoinSubqueryObj(pParentSql);
        return;
      } else {
        SSqlCmd* pSubCmd1 = &pParentSql->pSubs[0]->cmd;
        SSqlCmd* pSubCmd2 = &pParentSql->pSubs[1]->cmd;
        
        SQueryInfo* pQueryInfo1 = tscGetQueryInfoDetail(pSubCmd1, 0);
        STableMetaInfo* pTableMetaInfo1 = tscGetMetaInfo(pQueryInfo1, 0);
        doBuildVgroupTableInfo(s1, pTableMetaInfo1);
        
        SQueryInfo* pQueryInfo2 = tscGetQueryInfoDetail(pSubCmd2, 0);
        STableMetaInfo* pTableMetaInfo2 = tscGetMetaInfo(pQueryInfo2, 0);
        doBuildVgroupTableInfo(s2, pTableMetaInfo2);
  
        pSupporter->pState->numOfCompleted = 0;
        pSupporter->pState->code = 0;
        pSupporter->pState->numOfTotal = 2;
        
        for(int32_t m = 0; m < pParentSql->numOfSubs; ++m) {
          SSqlObj* psub = pParentSql->pSubs[m];
          issueTSCompQuery(psub, psub->param, pParentSql);
        }
      }
      
    } else {
      size_t length = pSupporter->totalLen + pSql->res.rspLen;
      char* tmp = realloc(pSupporter->pIdTagList, length);
      assert(tmp != NULL);
      
      pSupporter->pIdTagList = tmp;
      
      memcpy(pSupporter->pIdTagList, pSql->res.data, pSql->res.rspLen);
      pSupporter->totalLen += pSql->res.rspLen;
      pSupporter->num += pSql->res.numOfRows;
  
      // continue retrieve data from vnode
      taos_fetch_rows_a(tres, joinRetrieveCallback, param);
    }
    
    return;
  }
  
  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
    if (numOfRows < 0) {
      tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);
      pSupporter->pState->code = numOfRows;
      quitAllSubquery(pParentSql, pSupporter);
      return;
    }

    if (numOfRows == 0) {
      tSIntersectionAndLaunchSecQuery(pSupporter, pSql);
      return;
    }

    // write the compressed timestamp to disk file
    fwrite(pSql->res.data, pSql->res.numOfRows, 1, pSupporter->f);
    fclose(pSupporter->f);
    pSupporter->f = NULL;
    
    STSBuf* pBuf = tsBufCreateFromFile(pSupporter->path, true);
    if (pBuf == NULL) {
      tscError("%p invalid ts comp file from vnode, abort subquery, file size:%d", pSql, numOfRows);

      pSupporter->pState->code = TSDB_CODE_APP_ERROR;  // todo set the informative code
      quitAllSubquery(pParentSql, pSupporter);
      return;
    }

    if (pSupporter->pTSBuf == NULL) {
      tscTrace("%p create tmp file for ts block:%s, size:%d bytes", pSql, pBuf->path, numOfRows);
      pSupporter->pTSBuf = pBuf;
    } else {
      assert(pQueryInfo->numOfTables == 1);  // for subquery, only one
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

      tsBufMerge(pSupporter->pTSBuf, pBuf, pTableMetaInfo->vgroupIndex);
      tsBufDestory(pBuf);
    }
    
    if (pSql->res.completed) {
      tSIntersectionAndLaunchSecQuery(pSupporter, pSql);
    } else { // open a new file to save the incoming result
      getTmpfilePath("ts-join", pSupporter->path);
      pSupporter->f = fopen(pSupporter->path, "w");
      pSql->res.row = pSql->res.numOfRows;

      taos_fetch_rows_a(tres, joinRetrieveCallback, param);
    }
  } else {  // secondary stage retrieve, driven by taos_fetch_row or other functions
    if (numOfRows < 0) {
      pSupporter->pState->code = numOfRows;
      tscError("%p retrieve failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);
    }

    if (numOfRows >= 0) {
      pSql->res.numOfTotal += pSql->res.numOfRows;
    }
  
    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && numOfRows == 0) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      assert(pQueryInfo->numOfTables == 1);
  
        // for projection query, need to try next vnode if current vnode is exhausted
      if ((++pTableMetaInfo->vgroupIndex) < pTableMetaInfo->vgroupList->numOfVgroups) {
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
        pParentSql->res.code = pSupporter->pState->code;
        freeJoinSubqueryObj(pParentSql);
        pParentSql->res.completed = true;
      }
  
      // update the records for each subquery in parent sql object.
      for(int32_t i = 0; i < pParentSql->numOfSubs; ++i) {
        if (pParentSql->pSubs[i] == NULL) {
          continue;
        }
    
        SSqlRes* pRes1 = &pParentSql->pSubs[i]->res;
        pRes1->numOfTotalInCurrentClause += pRes1->numOfRows;
      }
  
      // data has retrieved to client, build the join results
      tscBuildResFromSubqueries(pParentSql);
    } else {
      tscTrace("%p sub:%p completed, completed:%d, total:%d", pParentSql, tres, finished, numOfTotal);
    }
  }
}

static SJoinSupporter* tscUpdateSubqueryStatus(SSqlObj* pSql, int32_t numOfFetch) {
  int32_t notInvolved = 0;
  SJoinSupporter* pSupporter = NULL;
  SSubqueryState* pState = NULL;
  
  for(int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] == NULL) {
      notInvolved++;
    } else {
      pSupporter = (SJoinSupporter*)pSql->pSubs[i]->param;
      pState = pSupporter->pState;
    }
  }
  
  pState->numOfTotal = pSql->numOfSubs;
  pState->numOfCompleted = pSql->numOfSubs - numOfFetch;
  
  return pSupporter;
}

void tscFetchDatablockFromSubquery(SSqlObj* pSql) {
  assert(pSql->numOfSubs >= 1);
  
  int32_t numOfFetch = 0;
  bool hasData = true;
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    // if the subquery is NULL, it does not involved in the final result generation
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }
    
    SSqlRes *pRes = &pSub->res;
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSub->cmd, 0);
//    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
//    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
//      if (pRes->row >= pRes->numOfRows && pTableMetaInfo->vgroupIndex < pTableMetaInfo->vgroupList->numOfVgroups &&
//          (!tscHasReachLimitation(pQueryInfo, pRes)) && !pRes->completed) {
//        numOfFetch++;
//      }
//    } else {
      if (!tscHasReachLimitation(pQueryInfo, pRes)) {
        if (pRes->row >= pRes->numOfRows) {
          hasData = false;

          if (!pRes->completed) {
            numOfFetch++;
          }
        }
      } else {  // has reach the limitation, no data anymore
        hasData = false;
      }
      
    }
//  }

  // has data remains in client side, and continue to return data to app
  if (hasData) {
    tscBuildResFromSubqueries(pSql);
    return;
  } else if (numOfFetch <= 0) {
    pSql->res.completed = true;
    freeJoinSubqueryObj(pSql);
    
    if (pSql->res.code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, 0);
    } else {
      tscQueueAsyncRes(pSql);
    }
    
    return;
  }

  // TODO multi-vnode retrieve for projection query with limitation has bugs, since the global limiation is not handled
  tscTrace("%p retrieve data from %d subqueries", pSql, numOfFetch);
  SJoinSupporter* pSupporter = tscUpdateSubqueryStatus(pSql, numOfFetch);
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj* pSql1 = pSql->pSubs[i];
    if (pSql1 == NULL) {
      continue;
    }
    
    SSqlRes* pRes1 = &pSql1->res;
    SSqlCmd* pCmd1 = &pSql1->cmd;

    pSupporter = (SJoinSupporter*)pSql1->param;

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
    
    size_t numOfExprs = taosArrayGetSize(pSubQueryInfo->exprList);
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
  
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;
  
  // There is only one subquery and table for each subquery.
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  assert(pQueryInfo->numOfTables == 1 && pSql->cmd.numOfClause == 1);
  
  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
    if (code != TSDB_CODE_SUCCESS) {  // direct call joinRetrieveCallback and set the error code
      joinRetrieveCallback(param, pSql, code);
    } else {  // first stage query, continue to retrieve compressed time stamp data
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
      tscError("%p sub query failed, code:%s, set global code:%s, index:%d", pSql, tstrerror(code), tstrerror(code),
               pSupporter->subqueryIndex);
      
      pSupporter->pState->code = code;
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
          pSupporter->pState->numOfCompleted = 0;  // reset the record value

          pSql->fp = joinRetrieveCallback;  // continue retrieve data
          pSql->cmd.command = TSDB_SQL_FETCH;
          tscProcessSql(pSql);
        } else {  // first retrieve from vnode during the secondary stage sub-query
          // set the command flag must be after the semaphore been correctly set.
          if (pParentSql->res.code == TSDB_CODE_SUCCESS) {
            (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
          } else {
            tscQueueAsyncRes(pParentSql);
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
int32_t tscLaunchJoinSubquery(SSqlObj *pSql, int16_t tableIndex, SJoinSupporter *pSupporter) {
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
    
    pSupporter->colList = pNewQueryInfo->colList;
    pNewQueryInfo->colList = NULL;
    
    pSupporter->exprList = pNewQueryInfo->exprList;
    pNewQueryInfo->exprList = NULL;
    
    pSupporter->fieldsInfo = pNewQueryInfo->fieldsInfo;
  
    // this data needs to be transfer to support struct
    memset(&pNewQueryInfo->fieldsInfo, 0, sizeof(SFieldInfo));
    
    pSupporter->tagCond = pNewQueryInfo->tagCond;
    memset(&pNewQueryInfo->tagCond, 0, sizeof(STagCond));
    
    pNew->cmd.numOfCols = 0;
    pNewQueryInfo->intervalTime = 0;
    memset(&pNewQueryInfo->limit, 0, sizeof(SLimitVal));
    
    // backup the data and clear it in the sqlcmd object
    pSupporter->groupbyExpr = pNewQueryInfo->groupbyExpr;
    memset(&pNewQueryInfo->groupbyExpr, 0, sizeof(SSqlGroupbyExpr));
    
    tscInitQueryInfo(pNewQueryInfo);
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pNewQueryInfo, 0);
    
    if (UTIL_TABLE_IS_SUPERTABLE(pTableMetaInfo)) { // return the tableId & tag
      SSchema s = {0};
      SColumnIndex index = {0};
  
      size_t numOfTags = taosArrayGetSize(pTableMetaInfo->tagColList);
      for(int32_t i = 0; i < numOfTags; ++i) {
        SColumn* c = taosArrayGetP(pTableMetaInfo->tagColList, i);
        index = (SColumnIndex) {.tableIndex = 0, .columnIndex = c->colIndex.columnIndex};
        
        SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
        s = pTagSchema[c->colIndex.columnIndex];
        
        int16_t bytes = 0;
        int16_t type = 0;
        int16_t inter = 0;
        
        getResultDataInfo(s.type, s.bytes, TSDB_FUNC_TID_TAG, 0, &type, &bytes, &inter, 0, 0);
        
        s.type = type;
        s.bytes = bytes;
        pSupporter->tagSize = s.bytes;
      }
  
      // set get tags query type
      TSDB_QUERY_SET_TYPE(pNewQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
      tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TID_TAG, &index, &s, TSDB_COL_TAG);
      size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
  
      tscTrace(
          "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, transfer to tid_tag query to retrieve (tableId, tags), "
          "exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
          pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, pNewQueryInfo->pTableMetaInfo[0]->name);
      
    } else {
      SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
      SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
      tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);

      // set the tags value for ts_comp function
      SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);

      int16_t tagColIndex = tscGetJoinTagColIndexByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->uid);

      pExpr->param->i64Key = tagColIndex;
      pExpr->numOfParams = 1;

      // add the filter tag column
      if (pSupporter->colList != NULL) {
        size_t s = taosArrayGetSize(pSupporter->colList);

        for (int32_t i = 0; i < s; ++i) {
          SColumn *pCol = taosArrayGetP(pSupporter->colList, i);

          if (pCol->numOfFilters > 0) {  // copy to the pNew->cmd.colList if it is filtered.
            SColumn *p = tscColumnClone(pCol);
            taosArrayPush(pNewQueryInfo->colList, &p);
          }
        }
      }

      size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);

      tscTrace(
          "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, transfer to ts_comp query to retrieve timestamps, "
          "exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
          pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, pNewQueryInfo->pTableMetaInfo[0]->name);
    }
  } else {
    assert(0);
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pNewQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;
  }

  return tscProcessSql(pNew);
}

int32_t tscHandleMasterJoinQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  assert((pQueryInfo->type & TSDB_QUERY_TYPE_SUBQUERY) == 0);
  
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = pQueryInfo->numOfTables;
  
  tscTrace("%p start launch subquery, total:%d", pSql, pQueryInfo->numOfTables);
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    SJoinSupporter *pSupporter = tscCreateJoinSupporter(pSql, pState, i);
    
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
  
  pSql->cmd.command = (pSql->numOfSubs <= 0)? TSDB_SQL_RETRIEVE_EMPTY_RESULT:TSDB_SQL_METRIC_JOIN_RETRIEVE;
  
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
    pCmd->command = TSDB_SQL_RETRIEVE_LOCALMERGE;  // enable the abort of kill super table function.
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
    tscQueueAsyncRes(pSql);
    return ret;
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
    
    pthread_mutexattr_t mutexattr;
    memset(&mutexattr, 0, sizeof(pthread_mutexattr_t));
    
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
  tscError("sub:%p failed to flush data to disk:reason:%s", tres, strerror(errno));
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
      
      tscTrace("%p sub:%p retrieve failed, code:%s, orderOfSub:%d, retry:%d", trsupport->pParentSqlObj, pSql,
          tstrerror(numOfRows), subqueryIndex, trsupport->numOfRetry);
      
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
      tscError("%p sub:%p retrieve failed,code:%s,orderOfSub:%d failed.no more retry,set global code:%d", pPObj, pSql,
               numOfRows, subqueryIndex, tstrerror(pState->code));
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
  pPObj->cmd.command = TSDB_SQL_RETRIEVE_LOCALMERGE;
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

void tscBuildResFromSubqueries(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscQueueAsyncRes(pSql);
    return;
  }
  
  while (1) {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
    size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
    
    if (pRes->tsrow == NULL) {
      pRes->tsrow = calloc(numOfExprs, POINTER_BYTES);
      pRes->length = calloc(numOfExprs, sizeof(int32_t));
    }
    
    bool success = false;
    
    int32_t numOfTableHasRes = 0;
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] != NULL) {
        numOfTableHasRes++;
      }
    }
    
    if (numOfTableHasRes >= 2) {  // do merge result
      success = (doSetResultRowData(pSql->pSubs[0], false) != NULL) && (doSetResultRowData(pSql->pSubs[1], false) != NULL);
    } else {  // only one subquery
      SSqlObj *pSub = pSql->pSubs[0];
      if (pSub == NULL) {
        pSub = pSql->pSubs[1];
      }
      
      success = (doSetResultRowData(pSub, false) != NULL);
    }
    
    if (success) {  // current row of final output has been built, return to app
      for (int32_t i = 0; i < numOfExprs; ++i) {
        SColumnIndex* pIndex = &pRes->pColumnIndex[i];
        SSqlRes *pRes1 = &pSql->pSubs[pIndex->tableIndex]->res;
        pRes->tsrow[i] = pRes1->tsrow[pIndex->columnIndex];
      }
      
      pRes->numOfTotalInCurrentClause++;
      break;
    } else {  // continue retrieve data from vnode
      if (!tscHashRemainDataInSubqueryResultSet(pSql)) {
        tscTrace("%p at least one subquery exhausted, free all other %d subqueries", pSql, pSql->numOfSubs - 1);
        SSubqueryState *pState = NULL;
        
        // free all sub sqlobj
        for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
          SSqlObj *pChildObj = pSql->pSubs[i];
          if (pChildObj == NULL) {
            continue;
          }
          
          SJoinSupporter *pSupporter = (SJoinSupporter *)pChildObj->param;
          pState = pSupporter->pState;
          
          tscDestroyJoinSupporter(pChildObj->param);
          taos_free_result(pChildObj);
        }
        
        free(pState);
        
        pRes->completed = true; // set query completed
        sem_post(&pSql->rspSem);
        return;
      }
      
      tscFetchDatablockFromSubquery(pSql);
      if (pRes->code != TSDB_CODE_SUCCESS) {
        return;
      }
    }
  }
  
  if (pSql->res.code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pSql->param, pSql, 0);
  } else {
    tscQueueAsyncRes(pSql);
  }
}

static void transferNcharData(SSqlObj *pSql, int32_t columnIndex, TAOS_FIELD *pField) {
  SSqlRes *pRes = &pSql->res;
  
  if (isNull(pRes->tsrow[columnIndex], pField->type)) {
    pRes->tsrow[columnIndex] = NULL;
  } else if (pField->type == TSDB_DATA_TYPE_NCHAR) {
    // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
    if (pRes->buffer[columnIndex] == NULL) {
      pRes->buffer[columnIndex] = malloc(pField->bytes + TSDB_NCHAR_SIZE);
    }
    
    /* string terminated char for binary data*/
    memset(pRes->buffer[columnIndex], 0, pField->bytes + TSDB_NCHAR_SIZE);
    
    if (taosUcs4ToMbs(pRes->tsrow[columnIndex], pField->bytes - VARSTR_HEADER_SIZE, pRes->buffer[columnIndex])) {
      pRes->tsrow[columnIndex] = pRes->buffer[columnIndex];
    } else {
      tscError("%p charset:%s to %s. val:%ls convert failed.", pSql, DEFAULT_UNICODE_ENCODEC, tsCharset, pRes->tsrow);
      pRes->tsrow[columnIndex] = NULL;
    }
  }
}

void **doSetResultRowData(SSqlObj *pSql, bool finalResult) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  
  assert(pRes->row >= 0 && pRes->row <= pRes->numOfRows);
  
  if(pCmd->command == TSDB_SQL_METRIC_JOIN_RETRIEVE) {
    if (pRes->completed) {
      tfree(pRes->tsrow);
    }

    return pRes->tsrow;
  }
  
  if (pRes->row >= pRes->numOfRows) {  // all the results has returned to invoker
    tfree(pRes->tsrow);
    return pRes->tsrow;
  }
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  for (int i = 0; i < tscNumOfFields(pQueryInfo); ++i) {
    SFieldSupInfo* pSup = tscFieldInfoGetSupp(&pQueryInfo->fieldsInfo, i);
    if (pSup->pSqlExpr != NULL) {
      tscGetResultColumnChr(pRes, &pQueryInfo->fieldsInfo, i);
    }
    
    // primary key column cannot be null in interval query, no need to check
    if (i == 0 && pQueryInfo->intervalTime > 0) {
      continue;
    }
    
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
    transferNcharData(pSql, i, pField);
    
    // calculate the result from several other columns
    if (pSup->pArithExprInfo != NULL) {
//      SArithmeticSupport *sas = (SArithmeticSupport *)calloc(1, sizeof(SArithmeticSupport));
//      sas->offset = 0;
//      sas-> = pQueryInfo->fieldsInfo.pExpr[i];
//
//      sas->numOfCols = sas->pExpr->binExprInfo.numOfCols;
//
//      if (pRes->buffer[i] == NULL) {
//        pRes->buffer[i] = malloc(tscFieldInfoGetField(pQueryInfo, i)->bytes);
//      }
//
//      for(int32_t k = 0; k < sas->numOfCols; ++k) {
//        int32_t columnIndex = sas->pExpr->binExprInfo.pReqColumns[k].colIdxInBuf;
//        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, columnIndex);
//
//        sas->elemSize[k] = pExpr->resBytes;
//        sas->data[k] = (pRes->data + pRes->numOfRows* pExpr->offset) + pRes->row*pExpr->resBytes;
//      }
//
//      tSQLBinaryExprCalcTraverse(sas->pExpr->binExprInfo.pBinExpr, 1, pRes->buffer[i], sas, TSQL_SO_ASC, getArithemicInputSrc);
//      pRes->tsrow[i] = pRes->buffer[i];
//
//      free(sas); //todo optimization
    }
  }
  
  pRes->row++;  // index increase one-step
  return pRes->tsrow;
}

static bool tscHashRemainDataInSubqueryResultSet(SSqlObj *pSql) {
  bool     hasData = true;
  SSqlCmd *pCmd = &pSql->cmd;
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    bool allSubqueryExhausted = true;
    
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] == NULL) {
        continue;
      }
      
      SSqlRes *pRes1 = &pSql->pSubs[i]->res;
      SSqlCmd *pCmd1 = &pSql->pSubs[i]->cmd;
      
      SQueryInfo *pQueryInfo1 = tscGetQueryInfoDetail(pCmd1, pCmd1->clauseIndex);
      assert(pQueryInfo1->numOfTables == 1);
      
      STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo1, 0);
      
      /*
       * if the global limitation is not reached, and current result has not exhausted, or next more vnodes are
       * available, goes on
       */
      if (pTableMetaInfo->vgroupIndex < pTableMetaInfo->vgroupList->numOfVgroups && pRes1->row < pRes1->numOfRows &&
          (!tscHasReachLimitation(pQueryInfo1, pRes1))) {
        allSubqueryExhausted = false;
        break;
      }
    }
    
    hasData = !allSubqueryExhausted;
  } else {  // otherwise, in case inner join, if any subquery exhausted, query completed.
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] == 0) {
        continue;
      }
      
      SSqlRes *   pRes1 = &pSql->pSubs[i]->res;
      SQueryInfo *pQueryInfo1 = tscGetQueryInfoDetail(&pSql->pSubs[i]->cmd, 0);
      
      if ((pRes1->row >= pRes1->numOfRows && tscHasReachLimitation(pQueryInfo1, pRes1) &&
          tscProjectionQueryOnTable(pQueryInfo1)) ||
          (pRes1->numOfRows == 0)) {
        hasData = false;
        break;
      }
    }
  }
  
  return hasData;
}


