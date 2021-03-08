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
#define _GNU_SOURCE

#include "os.h"

#include "texpr.h"
#include "qTsbuf.h"
#include "tcompare.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "qUtil.h"

typedef struct SInsertSupporter {
  SSqlObj*  pSql;
  int32_t   index;
} SInsertSupporter;

static void freeJoinSubqueryObj(SSqlObj* pSql);
static bool tscHasRemainDataInSubqueryResultSet(SSqlObj *pSql);

static int32_t tsCompare(int32_t order, int64_t left, int64_t right) {
  if (left == right) {
    return 0;
  }

  if (order == TSDB_ORDER_ASC) {
    return left < right? -1:1;
  } else {
    return left > right? -1:1;
  }
}

static void skipRemainValue(STSBuf* pTSBuf, tVariant* tag1) {
  while (tsBufNextPos(pTSBuf)) {
    STSElem el1 = tsBufGetElem(pTSBuf);

    int32_t res = tVariantCompare(el1.tag, tag1);
    if (res != 0) { // it is a record with new tag
      return;
    }
  }
}

static void subquerySetState(SSqlObj *pSql, SSubqueryState *subState, int idx, int8_t state) {
  assert(idx < subState->numOfSub);
  assert(subState->states);

  pthread_mutex_lock(&subState->mutex);
  
  tscDebug("subquery:%p,%d state set to %d", pSql, idx, state);
  
  subState->states[idx] = state;

  pthread_mutex_unlock(&subState->mutex);
}

static bool allSubqueryDone(SSqlObj *pParentSql) {
  bool done = true;
  SSubqueryState *subState = &pParentSql->subState;

  //lock in caller
  
  for (int i = 0; i < subState->numOfSub; i++) {
    if (0 == subState->states[i]) {
      tscDebug("%p subquery:%p,%d is NOT finished, total:%d", pParentSql, pParentSql->pSubs[i], i, subState->numOfSub);
      done = false;
      break;
    } else {
      tscDebug("%p subquery:%p,%d is finished, total:%d", pParentSql, pParentSql->pSubs[i], i, subState->numOfSub);
    }
  }

  return done;
}

static bool subAndCheckDone(SSqlObj *pSql, SSqlObj *pParentSql, int idx) {
  SSubqueryState *subState = &pParentSql->subState;

  assert(idx < subState->numOfSub);

  pthread_mutex_lock(&subState->mutex);

  bool done = allSubqueryDone(pParentSql);

  if (done) {
    tscDebug("%p subquery:%p,%d all subs already done", pParentSql, pSql, idx);
    
    pthread_mutex_unlock(&subState->mutex);
    
    return false;
  }
  
  tscDebug("%p subquery:%p,%d state set to 1", pParentSql, pSql, idx);
  
  subState->states[idx] = 1;

  done = allSubqueryDone(pParentSql);

  pthread_mutex_unlock(&subState->mutex);

  return done;
}



static int64_t doTSBlockIntersect(SSqlObj* pSql, SJoinSupporter* pSupporter1, SJoinSupporter* pSupporter2, STimeWindow * win) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);

  STSBuf* output1 = tsBufCreate(true, pQueryInfo->order.order);
  STSBuf* output2 = tsBufCreate(true, pQueryInfo->order.order);

  win->skey = INT64_MAX;
  win->ekey = INT64_MIN;

  SLimitVal* pLimit = &pQueryInfo->limit;
  int32_t    order = pQueryInfo->order.order;

  SQueryInfo* pSubQueryInfo1 = tscGetQueryInfoDetail(&pSql->pSubs[0]->cmd, 0);
  SQueryInfo* pSubQueryInfo2 = tscGetQueryInfoDetail(&pSql->pSubs[1]->cmd, 0);

  pSubQueryInfo1->tsBuf = output1;
  pSubQueryInfo2->tsBuf = output2;

  TSKEY st = taosGetTimestampUs();

  // no result generated, return directly
  if (pSupporter1->pTSBuf == NULL || pSupporter2->pTSBuf == NULL) {
    tscDebug("%p at least one ts-comp is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  tsBufResetPos(pSupporter1->pTSBuf);
  tsBufResetPos(pSupporter2->pTSBuf);

  if (!tsBufNextPos(pSupporter1->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscDebug("%p input1 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  if (!tsBufNextPos(pSupporter2->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscDebug("%p input2 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  int64_t numOfInput1 = 1;
  int64_t numOfInput2 = 1;

  while(1) {
    STSElem elem = tsBufGetElem(pSupporter1->pTSBuf);

    // no data in pSupporter1 anymore, jump out of loop
    if (!tsBufIsValidElem(&elem)) {
      break;
    }

    // find the data in supporter2 with the same tag value
    STSElem e2 = tsBufFindElemStartPosByTag(pSupporter2->pTSBuf, elem.tag);

    /**
     * there are elements in pSupporter2 with the same tag, continue
     */
    tVariant tag1 = {0};
    tVariantAssign(&tag1, elem.tag);

    if (tsBufIsValidElem(&e2)) {
      while (1) {
        STSElem elem1 = tsBufGetElem(pSupporter1->pTSBuf);
        STSElem elem2 = tsBufGetElem(pSupporter2->pTSBuf);

        // data with current are exhausted
        if (!tsBufIsValidElem(&elem1) || tVariantCompare(elem1.tag, &tag1) != 0) {
          break;
        }

        if (!tsBufIsValidElem(&elem2) || tVariantCompare(elem2.tag, &tag1) != 0) { // ignore all records with the same tag
          skipRemainValue(pSupporter1->pTSBuf, &tag1);
          break;
        }

        /*
         * in case of stable query, limit/offset is not applied here. the limit/offset is applied to the
         * final results which is acquired after the secondary merge of in the client.
         */
        int32_t re = tsCompare(order, elem1.ts, elem2.ts);
        if (re < 0) {
          tsBufNextPos(pSupporter1->pTSBuf);
          numOfInput1++;
        } else if (re > 0) {
          tsBufNextPos(pSupporter2->pTSBuf);
          numOfInput2++;
        } else {
          if (pLimit->offset == 0 || pQueryInfo->interval.interval > 0 || QUERY_IS_STABLE_QUERY(pQueryInfo->type)) {
            if (win->skey > elem1.ts) {
              win->skey = elem1.ts;
            }

            if (win->ekey < elem1.ts) {
              win->ekey = elem1.ts;
            }

            tsBufAppend(output1, elem1.id, elem1.tag, (const char*)&elem1.ts, sizeof(elem1.ts));
            tsBufAppend(output2, elem2.id, elem2.tag, (const char*)&elem2.ts, sizeof(elem2.ts));
          } else {
            pLimit->offset -= 1;//offset apply to projection?
          }

          tsBufNextPos(pSupporter1->pTSBuf);
          numOfInput1++;

          tsBufNextPos(pSupporter2->pTSBuf);
          numOfInput2++;
        }
      }
    } else {  // no data in pSupporter2, ignore current data in pSupporter2
      skipRemainValue(pSupporter1->pTSBuf, &tag1);
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

  tsBufDestroy(pSupporter1->pTSBuf);
  pSupporter1->pTSBuf = NULL;
  tsBufDestroy(pSupporter2->pTSBuf);
  pSupporter2->pTSBuf = NULL;
    
  TSKEY et = taosGetTimestampUs();
  tscDebug("%p input1:%" PRId64 ", input2:%" PRId64 ", final:%" PRId64 " in %d vnodes for secondary query after ts blocks "
           "intersecting, skey:%" PRId64 ", ekey:%" PRId64 ", numOfVnode:%d, elapsed time:%" PRId64 " us",
           pSql, numOfInput1, numOfInput2, output1->numOfTotal, output1->numOfGroups, win->skey, win->ekey,
           tsBufGetNumOfGroup(output1), et - st);

  return output1->numOfTotal;
}

// todo handle failed to create sub query
SJoinSupporter* tscCreateJoinSupporter(SSqlObj* pSql, int32_t index) {
  SJoinSupporter* pSupporter = calloc(1, sizeof(SJoinSupporter));
  if (pSupporter == NULL) {
    return NULL;
  }

  pSupporter->pObj = pSql;

  pSupporter->subqueryIndex = index;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
  
  memcpy(&pSupporter->interval, &pQueryInfo->interval, sizeof(pSupporter->interval));
  pSupporter->limit = pQueryInfo->limit;

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, pSql->cmd.clauseIndex, index);
  pSupporter->uid = pTableMetaInfo->pTableMeta->id.uid;
  assert (pSupporter->uid != 0);

  taosGetTmpfilePath("join-", pSupporter->path);

  // do NOT create file here to reduce crash generated file left issue
  pSupporter->f = NULL;

  return pSupporter;
}

static void tscDestroyJoinSupporter(SJoinSupporter* pSupporter) {
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

  if (pSupporter->pTSBuf != NULL) {
    tsBufDestroy(pSupporter->pTSBuf);
    pSupporter->pTSBuf = NULL;
  }

  unlink(pSupporter->path);
  
  if (pSupporter->f != NULL) {
    fclose(pSupporter->f);
    pSupporter->f = NULL;
  }


  if (pSupporter->pVgroupTables != NULL) {
    taosArrayDestroy(pSupporter->pVgroupTables);
    pSupporter->pVgroupTables = NULL;
  }

  tfree(pSupporter->pIdTagList);
  tscTagCondRelease(&pSupporter->tagCond);
  free(pSupporter);
}

/*
 * need the secondary query process
 * In case of count(ts)/count(*)/spread(ts) query, that are only applied to
 * primary timestamp column , the secondary query is not necessary
 *
 */
static UNUSED_FUNC bool needSecondaryQuery(SQueryInfo* pQueryInfo) {
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumn* base = taosArrayGet(pQueryInfo->colList, i);
    if (base->colIndex.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return true;
    }
  }

  return false;
}

static void filterVgroupTables(SQueryInfo* pQueryInfo, SArray* pVgroupTables) {
  int32_t  num = 0;
  int32_t* list = NULL;
  tsBufGetGroupIdList(pQueryInfo->tsBuf, &num, &list);

  // The virtual node, of which all tables are disqualified after the timestamp intersection,
  // is removed to avoid next stage query.
  // TODO: If tables from some vnodes are not qualified for next stage query, discard them.
  for (int32_t k = 0; k < taosArrayGetSize(pVgroupTables);) {
    SVgroupTableInfo* p = taosArrayGet(pVgroupTables, k);

    bool found = false;
    for (int32_t f = 0; f < num; ++f) {
      if (p->vgInfo.vgId == list[f]) {
        found = true;
        break;
      }
    }

    if (!found) {
      tscRemoveVgroupTableGroup(pVgroupTables, k);
    } else {
      k++;
    }
  }

  assert(taosArrayGetSize(pVgroupTables) > 0);
  TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_MULTITABLE_QUERY);

  tfree(list);
}

static SArray* buildVgroupTableByResult(SQueryInfo* pQueryInfo, SArray* pVgroupTables) {
  int32_t  num = 0;
  int32_t* list = NULL;
  tsBufGetGroupIdList(pQueryInfo->tsBuf, &num, &list);

  size_t numOfGroups = taosArrayGetSize(pVgroupTables);

  SArray* pNew = taosArrayInit(num, sizeof(SVgroupTableInfo));

  SVgroupTableInfo info;
  for (int32_t i = 0; i < num; ++i) {
    int32_t vnodeId = list[i];

    for (int32_t j = 0; j < numOfGroups; ++j) {
      SVgroupTableInfo* p1 = taosArrayGet(pVgroupTables, j);
      if (p1->vgInfo.vgId == vnodeId) {
        tscVgroupTableCopy(&info, p1);
        break;
      }
    }

    taosArrayPush(pNew, &info);
  }

  tfree(list);
  TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_MULTITABLE_QUERY);

  return pNew;
}

/*
 * launch secondary stage query to fetch the result that contains timestamp in set
 */
static int32_t tscLaunchRealSubqueries(SSqlObj* pSql) {
  int32_t         numOfSub = 0;
  SJoinSupporter* pSupporter = NULL;
  
  //If the columns are not involved in the final select clause, the corresponding query will not be issued.
  for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    pSupporter = pSql->pSubs[i]->param;
    if (taosArrayGetSize(pSupporter->exprList) > 0) {
      ++numOfSub;
    }
  }
  
  assert(numOfSub > 0);
  
  // scan all subquery, if one sub query has only ts, ignore it
  tscDebug("%p start to launch secondary subqueries, %d out of %d needs to query", pSql, numOfSub, pSql->subState.numOfSub);

  bool success = true;
  
  for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    SSqlObj *pPrevSub = pSql->pSubs[i];
    pSql->pSubs[i] = NULL;
    
    pSupporter = pPrevSub->param;
  
    if (taosArrayGetSize(pSupporter->exprList) == 0) {
      tscDebug("%p subIndex: %d, no need to launch query, ignore it", pSql, i);
    
      tscDestroyJoinSupporter(pSupporter);
      taos_free_result(pPrevSub);
    
      pSql->pSubs[i] = NULL;
      continue;
    }
  
    SQueryInfo *pSubQueryInfo = tscGetQueryInfoDetail(&pPrevSub->cmd, 0);
    STSBuf     *pTsBuf = pSubQueryInfo->tsBuf;
    pSubQueryInfo->tsBuf = NULL;
  
    // free result for async object will also free sqlObj
    assert(tscSqlExprNumOfExprs(pSubQueryInfo) == 1); // ts_comp query only requires one result columns
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
    pQueryInfo->tsBuf = pTsBuf;  // transfer the ownership of timestamp comp-z data to the new created object

    // set the second stage sub query for join process
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE);
    memcpy(&pQueryInfo->interval, &pSupporter->interval, sizeof(pQueryInfo->interval));

    tscTagCondCopy(&pQueryInfo->tagCond, &pSupporter->tagCond);

    pQueryInfo->colList = pSupporter->colList;
    pQueryInfo->exprList = pSupporter->exprList;
    pQueryInfo->fieldsInfo = pSupporter->fieldsInfo;
    pQueryInfo->groupbyExpr = pSupporter->groupInfo;

    assert(pNew->subState.numOfSub == 0 && pNew->cmd.numOfClause == 1 && pQueryInfo->numOfTables == 1);
  
    tscFieldInfoUpdateOffset(pQueryInfo);
  
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    pTableMetaInfo->pVgroupTables = pSupporter->pVgroupTables;

    pSupporter->exprList = NULL;
    pSupporter->colList = NULL;
    pSupporter->pVgroupTables = NULL;
    memset(&pSupporter->fieldsInfo, 0, sizeof(SFieldInfo));
    memset(&pSupporter->groupInfo, 0, sizeof(SSqlGroupbyExpr));

    /*
     * When handling the projection query, the offset value will be modified for table-table join, which is changed
     * during the timestamp intersection.
     */
    pSupporter->limit = pQueryInfo->limit;
    pQueryInfo->limit = pSupporter->limit;

    SColumnIndex index = {.tableIndex = 0, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
    SSchema* s = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, 0);

    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, 0);
    int16_t funcId = pExpr->functionId;

    // add the invisible timestamp column
    if ((pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) ||
        (funcId != TSDB_FUNC_TS && funcId != TSDB_FUNC_TS_DUMMY && funcId != TSDB_FUNC_PRJ)) {

      int16_t functionId = tscIsProjectionQuery(pQueryInfo)? TSDB_FUNC_PRJ : TSDB_FUNC_TS;

      tscAddFuncInSelectClause(pQueryInfo, 0, functionId, &index, s, TSDB_COL_NORMAL);
      tscPrintSelectClause(pNew, 0);
      tscFieldInfoUpdateOffset(pQueryInfo);

      pExpr = tscSqlExprGet(pQueryInfo, 0);
    }

    // set the join condition tag column info, todo extract method
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      assert(pQueryInfo->tagCond.joinInfo.hasJoin);
      int16_t colId = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->id.uid);

      // set the tag column id for executor to extract correct tag value
      pExpr->param[0] = (tVariant) {.i64 = colId, .nType = TSDB_DATA_TYPE_BIGINT, .nLen = sizeof(int64_t)};
      pExpr->numOfParams = 1;
    }

    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      assert(pTableMetaInfo->pVgroupTables != NULL);
      if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
        SArray* p = buildVgroupTableByResult(pQueryInfo, pTableMetaInfo->pVgroupTables);
        tscFreeVgroupTableInfo(pTableMetaInfo->pVgroupTables);
        pTableMetaInfo->pVgroupTables = p;
      } else {
        filterVgroupTables(pQueryInfo, pTableMetaInfo->pVgroupTables);
      }
    }

    subquerySetState(pPrevSub, &pSql->subState, i, 0);
    
    size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
    tscDebug("%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%" PRIzu ", colList:%" PRIzu ", fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pTableMetaInfo->vgroupIndex, pQueryInfo->type, taosArrayGetSize(pQueryInfo->exprList),
             numOfCols, pQueryInfo->fieldsInfo.numOfOutput, tNameGetTableName(&pTableMetaInfo->name));
  }
  
  //prepare the subqueries object failed, abort
  if (!success) {
    pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("%p failed to prepare subqueries objs for secondary phase query, numOfSub:%d, code:%d", pSql,
        pSql->subState.numOfSub, pSql->res.code);
    freeJoinSubqueryObj(pSql);
    
    return pSql->res.code;
  }
  
  for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    if (pSql->pSubs[i] == NULL) {
      continue;
    }

    tscDoQuery(pSql->pSubs[i]);
  }

  return TSDB_CODE_SUCCESS;
}

void freeJoinSubqueryObj(SSqlObj* pSql) {
  for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }
    
    SJoinSupporter* p = pSub->param;
    tscDestroyJoinSupporter(p);

    taos_free_result(pSub);
    pSql->pSubs[i] = NULL;
  }

  if (pSql->subState.states) {
    pthread_mutex_destroy(&pSql->subState.mutex);
  }
  
  tfree(pSql->subState.states);
  
  
  pSql->subState.numOfSub = 0;
}

static int32_t quitAllSubquery(SSqlObj* pSqlSub, SSqlObj* pSqlObj, SJoinSupporter* pSupporter) {
  if (subAndCheckDone(pSqlSub, pSqlObj, pSupporter->subqueryIndex)) {
    tscError("%p all subquery return and query failed, global code:%s", pSqlObj, tstrerror(pSqlObj->res.code));  
    freeJoinSubqueryObj(pSqlObj);
    return 0;
  }

  return 1;
  //tscDestroyJoinSupporter(pSupporter);
}

// update the query time range according to the join results on timestamp
static void updateQueryTimeRange(SQueryInfo* pQueryInfo, STimeWindow* win) {
  assert(pQueryInfo->window.skey <= win->skey && pQueryInfo->window.ekey >= win->ekey);
  pQueryInfo->window = *win;


}

int32_t tidTagsCompar(const void* p1, const void* p2) {
  const STidTags* t1 = (const STidTags*) (p1);
  const STidTags* t2 = (const STidTags*) (p2);
  
  if (t1->vgId != t2->vgId) {
    return (t1->vgId > t2->vgId) ? 1 : -1;
  }

  tstr* tag1 = (tstr*) t1->tag;
  tstr* tag2 = (tstr*) t2->tag;

  if (tag1->len != tag2->len) {
    return (tag1->len > tag2->len)? 1: -1;
  }

  return strncmp(tag1->data, tag2->data, tag1->len);
}

int32_t tagValCompar(const void* p1, const void* p2) {
  const STidTags* t1 = (const STidTags*) varDataVal(p1);
  const STidTags* t2 = (const STidTags*) varDataVal(p2);

  tstr* tag1 = (tstr*) t1->tag;
  tstr* tag2 = (tstr*) t2->tag;

  if (tag1->len != tag2->len) {
    return (tag1->len > tag2->len)? 1: -1;
  }

  return memcmp(tag1->data, tag2->data, tag1->len);
}

void tscBuildVgroupTableInfo(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo, SArray* tables) {
  SArray*   result = taosArrayInit(4, sizeof(SVgroupTableInfo));
  SArray*   vgTables = NULL;
  STidTags* prev = NULL;

  size_t numOfTables = taosArrayGetSize(tables);
  for (size_t i = 0; i < numOfTables; i++) {
    STidTags* tt = taosArrayGet(tables, i);

    if (prev == NULL || tt->vgId != prev->vgId) {
      SVgroupsInfo* pvg = pTableMetaInfo->vgroupList;

      SVgroupTableInfo info = {{0}};
      for (int32_t m = 0; m < pvg->numOfVgroups; ++m) {
        if (tt->vgId == pvg->vgroups[m].vgId) {
          tscSVgroupInfoCopy(&info.vgInfo, &pvg->vgroups[m]);
          break;
        }
      }
      assert(info.vgInfo.numOfEps != 0);

      vgTables = taosArrayInit(4, sizeof(STableIdInfo));
      info.itemList = vgTables;

      if (taosArrayGetSize(result) > 0) {
        SVgroupTableInfo* prevGroup = taosArrayGet(result, taosArrayGetSize(result) - 1);
        tscDebug("%p vgId:%d, tables:%"PRIzu, pSql, prevGroup->vgInfo.vgId, taosArrayGetSize(prevGroup->itemList));
      }

      taosArrayPush(result, &info);
    }

    STableIdInfo item = {.uid = tt->uid, .tid = tt->tid, .key = INT64_MIN};
    taosArrayPush(vgTables, &item);

    tscTrace("%p tid:%d, uid:%"PRIu64",vgId:%d added", pSql, tt->tid, tt->uid, tt->vgId);
    prev = tt;
  }

  pTableMetaInfo->vgroupIndex = 0;
  
  if (taosArrayGetSize(result) <= 0) {
    pTableMetaInfo->pVgroupTables = NULL;
    taosArrayDestroy(result);
  } else {
    pTableMetaInfo->pVgroupTables = result;

    SVgroupTableInfo* g = taosArrayGet(result, taosArrayGetSize(result) - 1);
    tscDebug("%p vgId:%d, tables:%"PRIzu, pSql, g->vgInfo.vgId, taosArrayGetSize(g->itemList));
  }
}

static void issueTsCompQuery(SSqlObj* pSql, SJoinSupporter* pSupporter, SSqlObj* pParent) {
  SSqlCmd* pCmd = &pSql->cmd;
  tscClearSubqueryInfo(pCmd);
  tscFreeSqlResult(pSql);

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  assert(pQueryInfo->numOfTables == 1);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  tscInitQueryInfo(pQueryInfo);

  TSDB_QUERY_CLEAR_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
  TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_MULTITABLE_QUERY);
  
  pCmd->command = TSDB_SQL_SELECT;
  pSql->fp = tscJoinQueryCallback;
  
  SSchema colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
  
  SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscAddFuncInSelectClause(pQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);
  
  // set the tags value for ts_comp function
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, 0);
    int16_t tagColId = tscGetJoinTagColIdByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->id.uid);
    pExpr->param->i64 = tagColId;
    pExpr->numOfParams = 1;
  }

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
  
  tscDebug(
      "%p subquery:%p tableIndex:%d, vgroupIndex:%d, numOfVgroups:%d, type:%d, ts_comp query to retrieve timestamps, "
      "numOfExpr:%" PRIzu ", colList:%" PRIzu ", numOfOutputFields:%d, name:%s",
      pParent, pSql, 0, pTableMetaInfo->vgroupIndex, pTableMetaInfo->vgroupList->numOfVgroups, pQueryInfo->type,
      tscSqlExprNumOfExprs(pQueryInfo), numOfCols, pQueryInfo->fieldsInfo.numOfOutput, tNameGetTableName(&pTableMetaInfo->name));
  
  tscProcessSql(pSql);
}

static bool checkForDuplicateTagVal(SSchema* pColSchema, SJoinSupporter* p1, SSqlObj* pPSqlObj) {
  for(int32_t i = 1; i < p1->num; ++i) {
    STidTags* prev = (STidTags*) varDataVal(p1->pIdTagList + (i - 1) * p1->tagSize);
    STidTags* p = (STidTags*) varDataVal(p1->pIdTagList + i * p1->tagSize);
    assert(prev->vgId >= 1 && p->vgId >= 1);

    if (doCompare(prev->tag, p->tag, pColSchema->type, pColSchema->bytes) == 0) {
      tscError("%p join tags have same value for different table, free all sub SqlObj and quit", pPSqlObj);
      pPSqlObj->res.code = TSDB_CODE_QRY_DUP_JOIN_KEY;
      return false;
    }
  }

  return true;
}

static int32_t getIntersectionOfTableTuple(SQueryInfo* pQueryInfo, SSqlObj* pParentSql, SArray** s1, SArray** s2) {
  SJoinSupporter* p1 = pParentSql->pSubs[0]->param;
  SJoinSupporter* p2 = pParentSql->pSubs[1]->param;

  tscDebug("%p all subquery retrieve <tid, tags> complete, do tags match, %d, %d", pParentSql, p1->num, p2->num);

  // sort according to the tag value
  qsort(p1->pIdTagList, p1->num, p1->tagSize, tagValCompar);
  qsort(p2->pIdTagList, p2->num, p2->tagSize, tagValCompar);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  int16_t tagColId = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->id.uid);

  SSchema* pColSchema = tscGetColumnSchemaById(pTableMetaInfo->pTableMeta, tagColId);

  // int16_t for padding
  int32_t size = p1->tagSize - sizeof(int16_t);
  *s1 = taosArrayInit(p1->num, size);
  *s2 = taosArrayInit(p2->num, size);

  if (!(checkForDuplicateTagVal(pColSchema, p1, pParentSql) && checkForDuplicateTagVal(pColSchema, p2, pParentSql))) {
    return TSDB_CODE_QRY_DUP_JOIN_KEY;
  }

  int32_t i = 0, j = 0;
  while(i < p1->num && j < p2->num) {
    STidTags* pp1 = (STidTags*) varDataVal(p1->pIdTagList + i * p1->tagSize);
    STidTags* pp2 = (STidTags*) varDataVal(p2->pIdTagList + j * p2->tagSize);
    assert(pp1->tid != 0 && pp2->tid != 0);

    int32_t ret = doCompare(pp1->tag, pp2->tag, pColSchema->type, pColSchema->bytes);
    if (ret == 0) {
      tscDebug("%p tag matched, vgId:%d, val:%d, tid:%d, uid:%"PRIu64", tid:%d, uid:%"PRIu64, pParentSql, pp1->vgId,
               *(int*) pp1->tag, pp1->tid, pp1->uid, pp2->tid, pp2->uid);

      taosArrayPush(*s1, pp1);
      taosArrayPush(*s2, pp2);
      j++;
      i++;
    } else if (ret > 0) {
      j++;
    } else {
      i++;
    }
  }

  // reorganize the tid-tag value according to both the vgroup id and tag values
  // sort according to the tag value
  size_t t1 = taosArrayGetSize(*s1);
  size_t t2 = taosArrayGetSize(*s2);

  qsort((*s1)->pData, t1, size, tidTagsCompar);
  qsort((*s2)->pData, t2, size, tidTagsCompar);

#if 0
  for(int32_t k = 0; k < t1; ++k) {
    STidTags* p =  (*s1)->pData + size * k;
    printf("%d, tag:%s\n", p->vgId, ((tstr*)(p->tag))->data);
  }

  for(int32_t k = 0; k < t1; ++k) {
    STidTags* p =  (*s2)->pData + size * k;
    printf("%d, tag:%s\n", p->vgId, ((tstr*)(p->tag))->data);
  }
#endif

  tscDebug("%p tags match complete, result: %"PRIzu", %"PRIzu, pParentSql, t1, t2);
  return TSDB_CODE_SUCCESS;
}

static void tidTagRetrieveCallback(void* param, TAOS_RES* tres, int32_t numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSqlObj* pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  // todo, the type may not include TSDB_QUERY_TYPE_TAG_FILTER_QUERY
  assert(TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY));

  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, numOfRows, pParentSql->res.code);
    if (quitAllSubquery(pSql, pParentSql, pSupporter)) {
      return;
    }

    tscAsyncResultOnError(pParentSql);

    return;
  }

  // check for the error code firstly
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    // todo retry if other subqueries are not failed

    assert(numOfRows < 0 && numOfRows == taos_errno(pSql));
    tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);

    pParentSql->res.code = numOfRows;
    if (quitAllSubquery(pSql, pParentSql, pSupporter)) {
      return;
    }

    tscAsyncResultOnError(pParentSql);
    return;
  }

  // keep the results in memory
  if (numOfRows > 0) {
    size_t validLen = (size_t)(pSupporter->tagSize * pRes->numOfRows);
    size_t length = pSupporter->totalLen + validLen;

    // todo handle memory error
    char* tmp = realloc(pSupporter->pIdTagList, length);
    if (tmp == NULL) {
      tscError("%p failed to malloc memory", pSql);

      pParentSql->res.code = TAOS_SYSTEM_ERROR(errno);
      if (quitAllSubquery(pSql, pParentSql, pSupporter)) {
        return;
      }

      tscAsyncResultOnError(pParentSql);
      return;
    }

    pSupporter->pIdTagList = tmp;

    memcpy(pSupporter->pIdTagList + pSupporter->totalLen, pRes->data, validLen);
    pSupporter->totalLen += (int32_t)validLen;
    pSupporter->num += (int32_t)pRes->numOfRows;

    // query not completed, continue to retrieve tid + tag tuples
    if (!pRes->completed) {
      taos_fetch_rows_a(tres, tidTagRetrieveCallback, param);
      return;
    }
  }

  // data in current vnode has all returned to client, try next vnode if exits
  // <tid + tag> tuples have been retrieved to client, try <tid + tag> tuples from the next vnode
  if (hasMoreVnodesToTry(pSql)) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

    int32_t totalVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
    pTableMetaInfo->vgroupIndex += 1;
    assert(pTableMetaInfo->vgroupIndex < totalVgroups);

    tscDebug("%p tid_tag from vgroup index:%d completed, try next vgroup:%d. total vgroups:%d. current numOfRes:%d",
             pSql, pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVgroups, pSupporter->num);

    pCmd->command = TSDB_SQL_SELECT;
    tscResetForNextRetrieve(&pSql->res);

    // set the callback function
    pSql->fp = tscJoinQueryCallback;
    tscProcessSql(pSql);
    return;
  }

  // no data exists in next vnode, mark the <tid, tags> query completed
  // only when there is no subquery exits any more, proceeds to get the intersect of the <tid, tags> tuple sets.
  if (!subAndCheckDone(pSql, pParentSql, pSupporter->subqueryIndex)) {
    tscDebug("%p tagRetrieve:%p,%d completed, total:%d", pParentSql, tres, pSupporter->subqueryIndex, pParentSql->subState.numOfSub);
    return;
  }  

  SArray *s1 = NULL, *s2 = NULL;
  int32_t code = getIntersectionOfTableTuple(pQueryInfo, pParentSql, &s1, &s2);
  if (code != TSDB_CODE_SUCCESS) {
    freeJoinSubqueryObj(pParentSql);
    pParentSql->res.code = code;
    tscAsyncResultOnError(pParentSql);

    taosArrayDestroy(s1);
    taosArrayDestroy(s2);
    return;
  }

  if (taosArrayGetSize(s1) == 0 || taosArrayGetSize(s2) == 0) {  // no results,return.
    assert(pParentSql->fp != tscJoinQueryCallback);

    tscDebug("%p tag intersect does not generated qualified tables for join, free all sub SqlObj and quit", pParentSql);
    freeJoinSubqueryObj(pParentSql);

    // set no result command
    pParentSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    assert(pParentSql->fp != tscJoinQueryCallback);

    (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
  } else {
    // proceed to for ts_comp query
    SSqlCmd* pSubCmd1 = &pParentSql->pSubs[0]->cmd;
    SSqlCmd* pSubCmd2 = &pParentSql->pSubs[1]->cmd;

    SQueryInfo*     pQueryInfo1 = tscGetQueryInfoDetail(pSubCmd1, 0);
    STableMetaInfo* pTableMetaInfo1 = tscGetMetaInfo(pQueryInfo1, 0);
    tscBuildVgroupTableInfo(pParentSql, pTableMetaInfo1, s1);

    SQueryInfo*     pQueryInfo2 = tscGetQueryInfoDetail(pSubCmd2, 0);
    STableMetaInfo* pTableMetaInfo2 = tscGetMetaInfo(pQueryInfo2, 0);
    tscBuildVgroupTableInfo(pParentSql, pTableMetaInfo2, s2);

    SSqlObj* psub1 = pParentSql->pSubs[0];
    ((SJoinSupporter*)psub1->param)->pVgroupTables =  tscVgroupTableInfoDup(pTableMetaInfo1->pVgroupTables);

    SSqlObj* psub2 = pParentSql->pSubs[1];
    ((SJoinSupporter*)psub2->param)->pVgroupTables =  tscVgroupTableInfoDup(pTableMetaInfo2->pVgroupTables);

    pParentSql->subState.numOfSub = 2;
    
    memset(pParentSql->subState.states, 0, sizeof(pParentSql->subState.states[0]) * pParentSql->subState.numOfSub);
    tscDebug("%p reset all sub states to 0", pParentSql);
    
    for (int32_t m = 0; m < pParentSql->subState.numOfSub; ++m) {
      SSqlObj* sub = pParentSql->pSubs[m];
      issueTsCompQuery(sub, sub->param, pParentSql);
    }
  }

  taosArrayDestroy(s1);
  taosArrayDestroy(s2);
}

static void tsCompRetrieveCallback(void* param, TAOS_RES* tres, int32_t numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSqlObj* pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  assert(!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE));

  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, numOfRows, pParentSql->res.code);
    if (quitAllSubquery(pSql, pParentSql, pSupporter)){
      return;
    }

    tscAsyncResultOnError(pParentSql);

    return;
  }

  // check for the error code firstly
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    // todo retry if other subqueries are not failed yet 
    assert(numOfRows < 0 && numOfRows == taos_errno(pSql));
    tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);

    pParentSql->res.code = numOfRows;
    if (quitAllSubquery(pSql, pParentSql, pSupporter)){
      return;
    }

    tscAsyncResultOnError(pParentSql);
    return;
  }

  if (numOfRows > 0) {  // write the compressed timestamp to disk file
    if(pSupporter->f == NULL) {
      pSupporter->f = fopen(pSupporter->path, "wb");

      if (pSupporter->f == NULL) {
        tscError("%p failed to create tmp file:%s, reason:%s", pSql, pSupporter->path, strerror(errno));
        
        pParentSql->res.code = TAOS_SYSTEM_ERROR(errno);

        if (quitAllSubquery(pSql, pParentSql, pSupporter)) {
          return;
        }
        
        tscAsyncResultOnError(pParentSql);

        return;
      }
    }
      
    fwrite(pRes->data, (size_t)pRes->numOfRows, 1, pSupporter->f);
    fclose(pSupporter->f);
    pSupporter->f = NULL;

    STSBuf* pBuf = tsBufCreateFromFile(pSupporter->path, true);
    if (pBuf == NULL) {  // in error process, close the fd
      tscError("%p invalid ts comp file from vnode, abort subquery, file size:%d", pSql, numOfRows);

      pParentSql->res.code = TAOS_SYSTEM_ERROR(errno);

      if (quitAllSubquery(pSql, pParentSql, pSupporter)){
        return;
      }
      
      tscAsyncResultOnError(pParentSql);

      return;
    }

    if (pSupporter->pTSBuf == NULL) {
      tscDebug("%p create tmp file for ts block:%s, size:%d bytes", pSql, pBuf->path, numOfRows);
      pSupporter->pTSBuf = pBuf;
    } else {
      assert(pQueryInfo->numOfTables == 1);  // for subquery, only one
      tsBufMerge(pSupporter->pTSBuf, pBuf);
      tsBufDestroy(pBuf);
    }

    // continue to retrieve ts-comp data from vnode
    if (!pRes->completed) {
      taosGetTmpfilePath("ts-join", pSupporter->path);
      pSupporter->f = fopen(pSupporter->path, "wb");
      pRes->row = pRes->numOfRows;

      taos_fetch_rows_a(tres, tsCompRetrieveCallback, param);
      return;
    }
  }

  if (hasMoreVnodesToTry(pSql)) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

    int32_t totalVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
    pTableMetaInfo->vgroupIndex += 1;
    assert(pTableMetaInfo->vgroupIndex < totalVgroups);

    tscDebug("%p results from vgroup index:%d completed, try next vgroup:%d. total vgroups:%d. current numOfRes:%" PRId64,
             pSql, pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVgroups,
             pRes->numOfClauseTotal);

    pCmd->command = TSDB_SQL_SELECT;
    tscResetForNextRetrieve(&pSql->res);

    assert(pSupporter->f == NULL);
    taosGetTmpfilePath("ts-join", pSupporter->path);
    
    // TODO check for failure
    pSupporter->f = fopen(pSupporter->path, "wb");
    pRes->row = pRes->numOfRows;

    // set the callback function
    pSql->fp = tscJoinQueryCallback;
    tscProcessSql(pSql);
    return;
  }

  if (!subAndCheckDone(pSql, pParentSql, pSupporter->subqueryIndex)) {
    return;
  }  

  tscDebug("%p all subquery retrieve ts complete, do ts block intersect", pParentSql);

  // proceeds to launched secondary query to retrieve final data
  SJoinSupporter* p1 = pParentSql->pSubs[0]->param;
  SJoinSupporter* p2 = pParentSql->pSubs[1]->param;

  STimeWindow win = TSWINDOW_INITIALIZER;
  int64_t num = doTSBlockIntersect(pParentSql, p1, p2, &win);
  if (num <= 0) {  // no result during ts intersect
    tscDebug("%p no results generated in ts intersection, free all sub SqlObj and quit", pParentSql);
    freeJoinSubqueryObj(pParentSql);

    // set no result command
    pParentSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
    return;
  }

  // launch the query the retrieve actual results from vnode along with the filtered timestamp
  SQueryInfo* pPQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, pParentSql->cmd.clauseIndex);
  updateQueryTimeRange(pPQueryInfo, &win);

  //update the vgroup that involved in real data query
  tscLaunchRealSubqueries(pParentSql);
}

static void joinRetrieveFinalResCallback(void* param, TAOS_RES* tres, int numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSqlObj* pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, numOfRows, pParentSql->res.code);
    if (quitAllSubquery(pSql, pParentSql, pSupporter)) {
      return;
    }
    
    tscAsyncResultOnError(pParentSql);

    return;
  }

  
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(numOfRows == taos_errno(pSql));

    pParentSql->res.code = numOfRows;
    tscError("%p retrieve failed, index:%d, code:%s", pSql, pSupporter->subqueryIndex, tstrerror(numOfRows));

    tscAsyncResultOnError(pParentSql);
    return;
  }

  if (numOfRows >= 0) {
    pRes->numOfTotal += pRes->numOfRows;
  }

  SSubqueryState* pState = &pParentSql->subState;
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && numOfRows == 0) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    assert(pQueryInfo->numOfTables == 1);

    // for projection query, need to try next vnode if current vnode is exhausted
    int32_t numOfVgroups = 0;  // TODO refactor
    if (pTableMetaInfo->pVgroupTables != NULL) {
      numOfVgroups = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
    } else {
      numOfVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
    }

    if ((++pTableMetaInfo->vgroupIndex) < numOfVgroups) {
      tscDebug("%p no result in current vnode anymore, try next vnode, vgIndex:%d", pSql, pTableMetaInfo->vgroupIndex);
      pSql->cmd.command = TSDB_SQL_SELECT;
      pSql->fp = tscJoinQueryCallback;

      tscProcessSql(pSql);
      return;
    } else {
      tscDebug("%p no result in current subquery anymore", pSql);
    }
  }

  if (!subAndCheckDone(pSql, pParentSql, pSupporter->subqueryIndex)) {
    tscDebug("%p sub:%p,%d completed, total:%d", pParentSql, tres, pSupporter->subqueryIndex, pState->numOfSub);
    return;
  }

  tscDebug("%p all %d secondary subqueries retrieval completed, code:%d", tres, pState->numOfSub, pParentSql->res.code);

  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    freeJoinSubqueryObj(pParentSql);
    pParentSql->res.completed = true;
  }

  // update the records for each subquery in parent sql object.
  bool stableQuery = tscIsTwoStageSTableQuery(pQueryInfo, 0);
  for (int32_t i = 0; i < pState->numOfSub; ++i) {
    if (pParentSql->pSubs[i] == NULL) {
      tscDebug("%p %p sub:%d not retrieve data", pParentSql, NULL, i);
      continue;
    }

    SSqlRes* pRes1 = &pParentSql->pSubs[i]->res;

    if (pRes1->row > 0 && pRes1->numOfRows > 0) {
      tscDebug("%p sub:%p index:%d numOfRows:%d total:%"PRId64 " (not retrieve)", pParentSql, pParentSql->pSubs[i], i,
               pRes1->numOfRows, pRes1->numOfTotal);
      assert(pRes1->row < pRes1->numOfRows);
    } else {
      if (!stableQuery) {
        pRes1->numOfClauseTotal += pRes1->numOfRows;
      }

      tscDebug("%p sub:%p index:%d numOfRows:%d total:%"PRId64, pParentSql, pParentSql->pSubs[i], i,
               pRes1->numOfRows, pRes1->numOfTotal);
    }
  }

  // data has retrieved to client, build the join results
  tscBuildResFromSubqueries(pParentSql);
}

void tscFetchDatablockForSubquery(SSqlObj* pSql) {
  assert(pSql->subState.numOfSub >= 1);
  
  int32_t numOfFetch = 0;
  bool    hasData = true;
  bool    reachLimit = false;

  // if the subquery is NULL, it does not involved in the final result generation
  for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }

    SSqlRes *pRes = &pSub->res;

    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSub->cmd, 0);

    if (!tscHasReachLimitation(pQueryInfo, pRes)) {
      if (pRes->row >= pRes->numOfRows) {
        // no data left in current result buffer
        hasData = false;

        // The current query is completed for the active vnode, try next vnode if exists
        // If it is completed, no need to fetch anymore.
        if (!pRes->completed) {
          numOfFetch++;
        }
      }
    } else {  // has reach the limitation, no data anymore
      if (pRes->row >= pRes->numOfRows) {
        reachLimit = true;
        hasData    = false;
        break;
      }
    }
  }

  // has data remains in client side, and continue to return data to app
  if (hasData) {
    tscBuildResFromSubqueries(pSql);
    return;
  }

  // If at least one subquery is completed in current vnode, try the next vnode in case of multi-vnode
  // super table projection query.
  if (reachLimit) {
    pSql->res.completed = true;
    freeJoinSubqueryObj(pSql);

    if (pSql->res.code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, 0);
    } else {
      tscAsyncResultOnError(pSql);
    }

    return;
  }

  if (numOfFetch <= 0) {
    bool tryNextVnode = false;

    bool orderedPrjQuery = false;
    for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      SSqlObj* pSub = pSql->pSubs[i];
      if (pSub == NULL) {
        continue;
      }

      SQueryInfo* p = tscGetQueryInfoDetail(&pSub->cmd, 0);
      orderedPrjQuery = tscNonOrderedProjectionQueryOnSTable(p, 0);
      if (orderedPrjQuery) {
        break;
      }
    }


    if (orderedPrjQuery) {
      for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
        SSqlObj* pSub = pSql->pSubs[i];
        if (pSub != NULL && pSub->res.row >= pSub->res.numOfRows && pSub->res.completed) {
          subquerySetState(pSub, &pSql->subState, i, 0);
        }
      }
    }
    

    for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      SSqlObj* pSub = pSql->pSubs[i];
      if (pSub == NULL) {
        continue;
      }

      SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSub->cmd, 0);

      if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && pSub->res.row >= pSub->res.numOfRows &&
          pSub->res.completed) {
        STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
        assert(pQueryInfo->numOfTables == 1);

        // for projection query, need to try next vnode if current vnode is exhausted
        int32_t numOfVgroups = 0;  // TODO refactor
        if (pTableMetaInfo->pVgroupTables != NULL) {
          numOfVgroups = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
        } else {
          numOfVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
        }

        if ((++pTableMetaInfo->vgroupIndex) < numOfVgroups) {
          tscDebug("%p no result in current vnode anymore, try next vnode, vgIndex:%d", pSub,
                   pTableMetaInfo->vgroupIndex);
          pSub->cmd.command = TSDB_SQL_SELECT;
          pSub->fp = tscJoinQueryCallback;

          tscProcessSql(pSub);
          tryNextVnode = true;
        } else {
          tscDebug("%p no result in current subquery anymore", pSub);
        }
      }
    }

    if (tryNextVnode) {
      return;
    }

    pSql->res.completed = true;
    freeJoinSubqueryObj(pSql);

    if (pSql->res.code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, 0);
    } else {
      tscAsyncResultOnError(pSql);
    }

    return;
  }

  // TODO multi-vnode retrieve for projection query with limitation has bugs, since the global limiation is not handled
  // retrieve data from current vnode.
  tscDebug("%p retrieve data from %d subqueries", pSql, numOfFetch);
  SJoinSupporter* pSupporter = NULL;

  for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    SSqlObj* pSql1 = pSql->pSubs[i];
    if (pSql1 == NULL) {
      continue;
    }

    SSqlRes* pRes1 = &pSql1->res;

    if (pRes1->row >= pRes1->numOfRows) {
      subquerySetState(pSql1, &pSql->subState, i, 0);
    }
  }

  for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
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
      tscDebug("%p subquery:%p retrieve data from vnode, subquery:%d, vgroupIndex:%d", pSql, pSql1,
               pSupporter->subqueryIndex, pTableMetaInfo->vgroupIndex);

      tscResetForNextRetrieve(pRes1);
      pSql1->fp = joinRetrieveFinalResCallback;

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


  // the column transfer support struct has been built
  if (pRes->pColumnIndex != NULL) {
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  int32_t numOfExprs = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
  pRes->pColumnIndex = calloc(1, sizeof(SColumnIndex) * numOfExprs);
  if (pRes->pColumnIndex == NULL) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return;
  }

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);

    int32_t tableIndexOfSub = -1;
    for (int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, j);
      if (pTableMetaInfo->pTableMeta->id.uid == pExpr->uid) {
        tableIndexOfSub = j;
        break;
      }
    }

    assert(tableIndexOfSub >= 0 && tableIndexOfSub < pQueryInfo->numOfTables);
    
    SSqlCmd* pSubCmd = &pSql->pSubs[tableIndexOfSub]->cmd;
    SQueryInfo* pSubQueryInfo = tscGetQueryInfoDetail(pSubCmd, 0);
    
    size_t numOfSubExpr = taosArrayGetSize(pSubQueryInfo->exprList);
    for (int32_t k = 0; k < numOfSubExpr; ++k) {
      SSqlExpr* pSubExpr = tscSqlExprGet(pSubQueryInfo, k);
      if (pExpr->functionId == pSubExpr->functionId && pExpr->colInfo.colId == pSubExpr->colInfo.colId) {
        pRes->pColumnIndex[i] = (SColumnIndex){.tableIndex = tableIndexOfSub, .columnIndex = k};
        break;
      }
    }
  }

  // restore the offset value for super table query in case of final result.
  tscRestoreFuncForSTableQuery(pQueryInfo);
  tscFieldInfoUpdateOffset(pQueryInfo);
}

void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code) {
  SSqlObj* pSql = (SSqlObj*)tres;

  SJoinSupporter* pSupporter = (SJoinSupporter*)param;
  SSqlObj* pParentSql = pSupporter->pObj;
  
  // There is only one subquery and table for each subquery.
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  assert(pQueryInfo->numOfTables == 1 && pSql->cmd.numOfClause == 1);

  // retrieve actual query results from vnode during the second stage join subquery
  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, code, pParentSql->res.code);
    if (quitAllSubquery(pSql, pParentSql, pSupporter)) {
      return;
    }

    tscAsyncResultOnError(pParentSql);

    return;
  }

  // TODO here retry is required, not directly returns to client
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(taos_errno(pSql) == code);

    tscError("%p abort query, code:%s, global code:%s", pSql, tstrerror(code), tstrerror(pParentSql->res.code));
    pParentSql->res.code = code;

    if (quitAllSubquery(pSql, pParentSql, pSupporter)) {
      return;
    }
    
    tscAsyncResultOnError(pParentSql);

    return;
  }

  // retrieve <tid, tag> tuples from vnode
  if (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY)) {
    pSql->fp = tidTagRetrieveCallback;
    pSql->cmd.command = TSDB_SQL_FETCH;
    tscProcessSql(pSql);
    return;
  }

  // retrieve ts_comp info from vnode
  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
    pSql->fp = tsCompRetrieveCallback;
    pSql->cmd.command = TSDB_SQL_FETCH;
    tscProcessSql(pSql);
    return;
  }

  // In case of consequence query from other vnode, do not wait for other query response here.
  if (!(pTableMetaInfo->vgroupIndex > 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0))) {
    if (!subAndCheckDone(pSql, pParentSql, pSupporter->subqueryIndex)) {
      return;
    }      
  }

  tscSetupOutputColumnIndex(pParentSql);

  /**
   * if the query is a continue query (vgroupIndex > 0 for projection query) for next vnode, do the retrieval of
   * data instead of returning to its invoker
   */
  if (pTableMetaInfo->vgroupIndex > 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    pSql->fp = joinRetrieveFinalResCallback;  // continue retrieve data
    pSql->cmd.command = TSDB_SQL_FETCH;
    
    tscProcessSql(pSql);
  } else {  // first retrieve from vnode during the secondary stage sub-query
    // set the command flag must be after the semaphore been correctly set.
    if (pParentSql->res.code == TSDB_CODE_SUCCESS) {
      (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
    } else {
      tscAsyncResultOnError(pParentSql);
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////
static void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code);

static SSqlObj *tscCreateSTableSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj);

int32_t tscCreateJoinSubquery(SSqlObj *pSql, int16_t tableIndex, SJoinSupporter *pSupporter) {
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  pSql->res.qhandle = 0x1;
  assert(pSql->res.numOfRows == 0);

  if (pSql->pSubs == NULL) {
    pSql->pSubs = calloc(pSql->subState.numOfSub, POINTER_BYTES);
    if (pSql->pSubs == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }
  
  SSqlObj *pNew = createSubqueryObj(pSql, tableIndex, tscJoinQueryCallback, pSupporter, TSDB_SQL_SELECT, NULL);
  if (pNew == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  
  pSql->pSubs[tableIndex] = pNew;
  
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
    if (tscTagCondCopy(&pSupporter->tagCond, &pNewQueryInfo->tagCond) != 0) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    pSupporter->groupInfo = pNewQueryInfo->groupbyExpr;
    memset(&pNewQueryInfo->groupbyExpr, 0, sizeof(SSqlGroupbyExpr));

    pNew->cmd.numOfCols = 0;
    pNewQueryInfo->interval.interval = 0;
    pSupporter->limit = pNewQueryInfo->limit;

    pNewQueryInfo->limit.limit = -1;
    pNewQueryInfo->limit.offset = 0;

    // backup the data and clear it in the sqlcmd object
    memset(&pNewQueryInfo->groupbyExpr, 0, sizeof(SSqlGroupbyExpr));
    
    tscInitQueryInfo(pNewQueryInfo);
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pNewQueryInfo, 0);
    
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) { // return the tableId & tag
      SColumnIndex colIndex = {0};

      STagCond* pTagCond = &pSupporter->tagCond;
      assert(pTagCond->joinInfo.hasJoin);

      int32_t tagColId = tscGetJoinTagColIdByUid(pTagCond, pTableMetaInfo->pTableMeta->id.uid);
      SSchema* s = tscGetColumnSchemaById(pTableMetaInfo->pTableMeta, tagColId);

      colIndex.columnIndex = tscGetTagColIndexById(pTableMetaInfo->pTableMeta, tagColId);

      int16_t bytes = 0;
      int16_t type  = 0;
      int32_t inter = 0;

      getResultDataInfo(s->type, s->bytes, TSDB_FUNC_TID_TAG, 0, &type, &bytes, &inter, 0, 0);

      SSchema s1 = {.colId = s->colId, .type = (uint8_t)type, .bytes = bytes};
      pSupporter->tagSize = s1.bytes;
      assert(isValidDataType(s1.type) && s1.bytes > 0);

      // set get tags query type
      TSDB_QUERY_SET_TYPE(pNewQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
      tscAddFuncInSelectClause(pNewQueryInfo, 0, TSDB_FUNC_TID_TAG, &colIndex, &s1, TSDB_COL_TAG);
      size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
  
      tscDebug(
          "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, transfer to tid_tag query to retrieve (tableId, tags), "
          "exprInfo:%" PRIzu ", colList:%" PRIzu ", fieldsInfo:%d, tagIndex:%d, name:%s",
          pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, colIndex.columnIndex, tNameGetTableName(&pNewQueryInfo->pTableMetaInfo[0]->name));
    } else {
      SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
      SColumnIndex colIndex = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
      tscAddFuncInSelectClause(pNewQueryInfo, 0, TSDB_FUNC_TS_COMP, &colIndex, &colSchema, TSDB_COL_NORMAL);

      // set the tags value for ts_comp function
      SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);

      if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        int16_t tagColId = tscGetJoinTagColIdByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->id.uid);
        pExpr->param->i64 = tagColId;
        pExpr->numOfParams = 1;
      }

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

      tscDebug(
          "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%u, transfer to ts_comp query to retrieve timestamps, "
          "exprInfo:%" PRIzu ", colList:%" PRIzu ", fieldsInfo:%d, name:%s",
          pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, tNameGetTableName(&pNewQueryInfo->pTableMetaInfo[0]->name));
    }
  } else {
    assert(0);
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pNewQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;
  }

  return TSDB_CODE_SUCCESS;
}

void tscHandleMasterJoinQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  assert((pQueryInfo->type & TSDB_QUERY_TYPE_SUBQUERY) == 0);

  int32_t code = TSDB_CODE_SUCCESS;
  pSql->subState.numOfSub = pQueryInfo->numOfTables;

  if (pSql->subState.states == NULL) {
    pSql->subState.states = calloc(pSql->subState.numOfSub, sizeof(*pSql->subState.states));
    if (pSql->subState.states == NULL) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
    
    pthread_mutex_init(&pSql->subState.mutex, NULL);
  }

  memset(pSql->subState.states, 0, sizeof(*pSql->subState.states) * pSql->subState.numOfSub);
  tscDebug("%p reset all sub states to 0", pSql);
  
  bool hasEmptySub = false;

  tscDebug("%p start subquery, total:%d", pSql, pQueryInfo->numOfTables);
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {

    SJoinSupporter *pSupporter = tscCreateJoinSupporter(pSql, i);
    
    if (pSupporter == NULL) {  // failed to create support struct, abort current query
      tscError("%p tableIndex:%d, failed to allocate join support object, abort further query", pSql, i);
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
    
    code = tscCreateJoinSubquery(pSql, i, pSupporter);
    if (code != TSDB_CODE_SUCCESS) {  // failed to create subquery object, quit query
      tscDestroyJoinSupporter(pSupporter);
      goto _error;
    }

    SSqlObj* pSub = pSql->pSubs[i];
    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSub->cmd, 0, 0);
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) && (pTableMetaInfo->vgroupList->numOfVgroups == 0)) {
      hasEmptySub = true;
      break;
    }
  }

  if (hasEmptySub) {  // at least one subquery is empty, do nothing and return
    freeJoinSubqueryObj(pSql);
    pSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    (*pSql->fp)(pSql->param, pSql, 0);
  } else {
    int fail = 0;
    for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      SSqlObj* pSub = pSql->pSubs[i];
      if (fail) {
        (*pSub->fp)(pSub->param, pSub, 0);
        continue;
      }
      
      if ((code = tscProcessSql(pSub)) != TSDB_CODE_SUCCESS) {
        pRes->code = code;
        (*pSub->fp)(pSub->param, pSub, 0);
        fail = 1;
      }
    }

    if(fail) {
      return;
    }

    pSql->cmd.command = TSDB_SQL_TABLE_JOIN_RETRIEVE;
  }

  return;

  _error:
  pRes->code = code;
  tscAsyncResultOnError(pSql);
}

static void doCleanupSubqueries(SSqlObj *pSql, int32_t numOfSubs) {
  assert(numOfSubs <= pSql->subState.numOfSub && numOfSubs >= 0);
  
  for(int32_t i = 0; i < numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    assert(pSub != NULL);
    
    SRetrieveSupport* pSupport = pSub->param;
    
    tfree(pSupport->localBuffer);
    tfree(pSupport);
    
    taos_free_result(pSub);
  }
}

void tscLockByThread(int64_t *lockedBy) {
  int64_t tid = taosGetSelfPthreadId();
  int     i = 0;
  while (atomic_val_compare_exchange_64(lockedBy, 0, tid) != 0) {
    if (++i % 100 == 0) {
      sched_yield();
    }
  }
}

void tscUnlockByThread(int64_t *lockedBy) {
  int64_t tid = taosGetSelfPthreadId();
  if (atomic_val_compare_exchange_64(lockedBy, tid, 0) != tid) {
    assert(false);
  }
}

typedef struct SFirstRoundQuerySup {
  SSqlObj  *pParent;
  int32_t   numOfRows;
  SArray   *pColsInfo;
  int32_t   tagLen;
  STColumn *pTagCols;
  SArray   *pResult;   // SArray<SInterResult>
  int64_t   interval;
  char*     buf;
  int32_t   bufLen;
} SFirstRoundQuerySup;

void doAppendData(SInterResult* pInterResult, TAOS_ROW row, int32_t numOfCols, SQueryInfo* pQueryInfo) {
  TSKEY key = INT64_MIN;
  for(int32_t i = 0; i < numOfCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (TSDB_COL_IS_TAG(pExpr->colInfo.flag) || pExpr->functionId == TSDB_FUNC_PRJ) {
      continue;
    }

    if (pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      key = *(TSKEY*) row[i];
      continue;
    }

    double v = 0;
    if (row[i] != NULL) {
      v = *(double*) row[i];
    } else {
      SET_DOUBLE_NULL(&v);
    }

    int32_t id = pExpr->colInfo.colId;
    int32_t numOfQueriedCols = (int32_t) taosArrayGetSize(pInterResult->pResult);

    SArray* p = NULL;
    for(int32_t j = 0; j < numOfQueriedCols; ++j) {
      SStddevInterResult* pColRes = taosArrayGet(pInterResult->pResult, j);
      if (pColRes->colId == id) {
        p = pColRes->pResult;
        break;
      }
    }

    //append a new column
    if (p == NULL) {
      SStddevInterResult t = {.colId = id, .pResult = taosArrayInit(10, sizeof(SResPair)),};
      taosArrayPush(pInterResult->pResult, &t);
      p = t.pResult;
    }

    SResPair pair = {.avg = v, .key = key};
    taosArrayPush(p, &pair);
  }
}

static void destroySup(SFirstRoundQuerySup* pSup) {
  taosArrayDestroyEx(pSup->pResult, freeInterResult);
  taosArrayDestroy(pSup->pColsInfo);
  tfree(pSup);
}

void tscFirstRoundRetrieveCallback(void* param, TAOS_RES* tres, int numOfRows) {
  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlRes* pRes = &pSql->res;

  SFirstRoundQuerySup* pSup = param;

  SSqlObj*     pParent = pSup->pParent;
  SQueryInfo*  pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  int32_t code = taos_errno(pSql);
  if (code != TSDB_CODE_SUCCESS) {
    destroySup(pSup);
    taos_free_result(pSql);
    pParent->res.code = code;
    tscAsyncResultOnError(pParent);
    return;
  }

  if (numOfRows > 0) {  // the number is not correct for group by column in super table query
    TAOS_ROW row = NULL;
    int32_t  numOfCols = taos_field_count(tres);

    if (pSup->tagLen == 0) {  // no tags, all rows belong to one group
      SInterResult interResult = {.tags = NULL, .pResult = taosArrayInit(4, sizeof(SStddevInterResult))};
      taosArrayPush(pSup->pResult, &interResult);

      while ((row = taos_fetch_row(tres)) != NULL) {
        doAppendData(&interResult, row, numOfCols, pQueryInfo);
        pSup->numOfRows += 1;
      }
    } else {  // tagLen > 0
      char* p = calloc(1, pSup->tagLen);

      while ((row = taos_fetch_row(tres)) != NULL) {
        int32_t* length = taos_fetch_lengths(tres);
        memset(p, 0, pSup->tagLen);

        int32_t offset = 0;
        for (int32_t i = 0; i < numOfCols && offset < pSup->tagLen; ++i) {
          SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);

          // tag or group by column
          if (TSDB_COL_IS_TAG(pExpr->colInfo.flag) || pExpr->functionId == TSDB_FUNC_PRJ) {
            memcpy(p + offset, row[i], length[i]);
            offset += pExpr->resBytes;
          }
        }

        assert(offset == pSup->tagLen);
        size_t size = taosArrayGetSize(pSup->pResult);

        if (size > 0) {
          SInterResult* pInterResult = taosArrayGetLast(pSup->pResult);
          if (memcmp(pInterResult->tags, p, pSup->tagLen) == 0) {  // belongs to the same group
            doAppendData(pInterResult, row, numOfCols, pQueryInfo);
          } else {
            char* tags = malloc( pSup->tagLen);
            memcpy(tags, p, pSup->tagLen);

            SInterResult interResult = {.tags = tags, .pResult = taosArrayInit(4, sizeof(SStddevInterResult))};
            taosArrayPush(pSup->pResult, &interResult);
            doAppendData(&interResult, row, numOfCols, pQueryInfo);
          }
        } else {
          char* tags = malloc(pSup->tagLen);
          memcpy(tags, p, pSup->tagLen);

          SInterResult interResult = {.tags = tags, .pResult = taosArrayInit(4, sizeof(SStddevInterResult))};
          taosArrayPush(pSup->pResult, &interResult);
          doAppendData(&interResult, row, numOfCols, pQueryInfo);
        }

        pSup->numOfRows += 1;
      }

      tfree(p);
    }
  }

  if (!pRes->completed) {
    taos_fetch_rows_a(tres, tscFirstRoundRetrieveCallback, param);
    return;
  }

  // set the parameters for the second round query process
  SSqlCmd    *pPCmd   = &pParent->cmd;
  SQueryInfo *pQueryInfo1 = tscGetQueryInfoDetail(pPCmd, 0);
  int32_t resRows = pSup->numOfRows;
  
  if (pSup->numOfRows > 0) {
    SBufferWriter bw = tbufInitWriter(NULL, false);
    interResToBinary(&bw, pSup->pResult, pSup->tagLen);

    pQueryInfo1->bufLen = (int32_t) tbufTell(&bw);
    pQueryInfo1->buf = tbufGetData(&bw, true);

    // set the serialized binary string as the parameter of arithmetic expression
    tbufCloseWriter(&bw);
  }

  taosArrayDestroyEx(pSup->pResult, freeInterResult);
  taosArrayDestroy(pSup->pColsInfo);
  tfree(pSup);

  taos_free_result(pSql);

  if (resRows == 0) {
    pParent->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    (*pParent->fp)(pParent->param, pParent, 0);
    return;
  }

  pQueryInfo1->round = 1;
  tscDoQuery(pParent);
}

void tscFirstRoundCallback(void* param, TAOS_RES* tres, int code) {
  SFirstRoundQuerySup* pSup = (SFirstRoundQuerySup*) param;

  SSqlObj* pSql = (SSqlObj*) tres;
  int32_t c = taos_errno(pSql);

  if (c != TSDB_CODE_SUCCESS) {
    SSqlObj* parent = pSup->pParent;

    destroySup(pSup);
    taos_free_result(pSql);
    parent->res.code = code;
    tscAsyncResultOnError(parent);
    return;
  }

  taos_fetch_rows_a(tres, tscFirstRoundRetrieveCallback, param);
}

int32_t tscHandleFirstRoundStableQuery(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMetaInfo* pTableMetaInfo1 = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  SFirstRoundQuerySup *pSup = calloc(1, sizeof(SFirstRoundQuerySup));

  pSup->pParent  = pSql;
  pSup->interval = pQueryInfo->interval.interval;
  pSup->pResult  = taosArrayInit(6, sizeof(SStddevInterResult));
  pSup->pColsInfo = taosArrayInit(6, sizeof(int16_t)); // result column id

  SSqlObj *pNew = createSubqueryObj(pSql, 0, tscFirstRoundCallback, pSup, TSDB_SQL_SELECT, NULL);
  SSqlCmd *pCmd = &pNew->cmd;

  tscClearSubqueryInfo(pCmd);
  tscFreeSqlResult(pSql);

  SQueryInfo* pNewQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  assert(pQueryInfo->numOfTables == 1);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pNewQueryInfo, 0);

  tscInitQueryInfo(pNewQueryInfo);
  pNewQueryInfo->groupbyExpr = pQueryInfo->groupbyExpr;
  if (pQueryInfo->groupbyExpr.columnInfo != NULL) {
    pNewQueryInfo->groupbyExpr.columnInfo = taosArrayDup(pQueryInfo->groupbyExpr.columnInfo);
    if (pNewQueryInfo->groupbyExpr.columnInfo == NULL) {
      terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
  }

  if (tscTagCondCopy(&pNewQueryInfo->tagCond, &pQueryInfo->tagCond) != 0) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  pNewQueryInfo->interval = pQueryInfo->interval;

  pCmd->command = TSDB_SQL_SELECT;
  pNew->fp = tscFirstRoundCallback;

  int32_t numOfExprs = (int32_t) tscSqlExprNumOfExprs(pQueryInfo);

  int32_t index = 0;
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_TS && pQueryInfo->interval.interval > 0) {
      taosArrayPush(pSup->pColsInfo, &pExpr->resColId);

      SColumnIndex colIndex = {.tableIndex = 0, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
      SSchema* schema = tscGetColumnSchemaById(pTableMetaInfo1->pTableMeta, pExpr->colInfo.colId);

      SSqlExpr* p = tscAddFuncInSelectClause(pNewQueryInfo, index++, TSDB_FUNC_TS, &colIndex, schema, TSDB_COL_NORMAL);
      p->resColId = pExpr->resColId;  // update the result column id
    } else if (pExpr->functionId == TSDB_FUNC_STDDEV_DST) {
      taosArrayPush(pSup->pColsInfo, &pExpr->resColId);

      SColumnIndex colIndex = {.tableIndex = 0, .columnIndex = pExpr->colInfo.colIndex};
      SSchema schema = {.type = TSDB_DATA_TYPE_DOUBLE, .bytes = sizeof(double)};
      tstrncpy(schema.name, pExpr->aliasName, tListLen(schema.name));

      SSqlExpr* p = tscAddFuncInSelectClause(pNewQueryInfo, index++, TSDB_FUNC_AVG, &colIndex, &schema, TSDB_COL_NORMAL);
      p->resColId = pExpr->resColId;  // update the result column id
    } else if (pExpr->functionId == TSDB_FUNC_TAG) {
      pSup->tagLen += pExpr->resBytes;
      SColumnIndex colIndex = {.tableIndex = 0, .columnIndex = pExpr->colInfo.colIndex};

      SSchema* schema = NULL;
      if (pExpr->colInfo.colId != TSDB_TBNAME_COLUMN_INDEX) {
        schema = tscGetColumnSchemaById(pTableMetaInfo1->pTableMeta, pExpr->colInfo.colId);
      } else {
        schema = tGetTbnameColumnSchema();
      }

      SSqlExpr* p = tscAddFuncInSelectClause(pNewQueryInfo, index++, TSDB_FUNC_TAG, &colIndex, schema, TSDB_COL_TAG);
      p->resColId = pExpr->resColId;
    } else if (pExpr->functionId == TSDB_FUNC_PRJ) {
      int32_t num = (int32_t) taosArrayGetSize(pNewQueryInfo->groupbyExpr.columnInfo);
      for(int32_t k = 0; k < num; ++k) {
        SColIndex* pIndex = taosArrayGet(pNewQueryInfo->groupbyExpr.columnInfo, k);
        if (pExpr->colInfo.colId == pIndex->colId) {
          pSup->tagLen += pExpr->resBytes;
          taosArrayPush(pSup->pColsInfo, &pExpr->resColId);

          SColumnIndex colIndex = {.tableIndex = 0, .columnIndex = pIndex->colIndex};
          SSchema* schema = tscGetColumnSchemaById(pTableMetaInfo1->pTableMeta, pExpr->colInfo.colId);

          //doLimitOutputNormalColOfGroupby
          SSqlExpr* p = tscAddFuncInSelectClause(pNewQueryInfo, index++, TSDB_FUNC_PRJ, &colIndex, schema, TSDB_COL_NORMAL);
          p->numOfParams = 1;
          p->param[0].i64 = 1;
          p->param[0].nType = TSDB_DATA_TYPE_INT;
          p->resColId = pExpr->resColId;  // update the result column id
        }
      }
    }
  }

  SColumnIndex columnIndex = {.tableIndex = 0, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscInsertPrimaryTsSourceColumn(pNewQueryInfo, &columnIndex);

  tscTansformFuncForSTableQuery(pNewQueryInfo);

  tscDebug(
      "%p first round subquery:%p tableIndex:%d, vgroupIndex:%d, numOfVgroups:%d, type:%d, query to retrieve timestamps, "
      "numOfExpr:%" PRIzu ", colList:%d, numOfOutputFields:%d, name:%s",
      pSql, pNew, 0, pTableMetaInfo->vgroupIndex, pTableMetaInfo->vgroupList->numOfVgroups, pNewQueryInfo->type,
      tscSqlExprNumOfExprs(pNewQueryInfo), index+1, pNewQueryInfo->fieldsInfo.numOfOutput, tNameGetTableName(&pTableMetaInfo->name));

  tscHandleMasterSTableQuery(pNew);
  return TSDB_CODE_SUCCESS;

  _error:
  destroySup(pSup);
  taos_free_result(pNew);
  pSql->res.code = terrno;
  tscAsyncResultOnError(pSql);
  return terrno;
}

int32_t tscHandleMasterSTableQuery(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  // pRes->code check only serves in launching metric sub-queries
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    pCmd->command = TSDB_SQL_RETRIEVE_LOCALMERGE;  // enable the abort of kill super table function.
    return pRes->code;
  }
  
  tExtMemBuffer   **pMemoryBuf = NULL;
  tOrderDescriptor *pDesc  = NULL;
  SColumnModel     *pModel = NULL;
  SColumnModel     *pFinalModel = NULL;

  pRes->qhandle = 0x1;  // hack the qhandle check
  
  const uint32_t nBufferSize = (1u << 16u);  // 64KB
  
  SQueryInfo     *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSubqueryState *pState = &pSql->subState;

  pState->numOfSub = 0;
  if (pTableMetaInfo->pVgroupTables == NULL) {
    pState->numOfSub = pTableMetaInfo->vgroupList->numOfVgroups;
  } else {
    pState->numOfSub = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
  }

  assert(pState->numOfSub > 0);
  
  int32_t ret = tscLocalReducerEnvCreate(pSql, &pMemoryBuf, &pDesc, &pModel, &pFinalModel, nBufferSize);
  if (ret != 0) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscAsyncResultOnError(pSql);
    tfree(pMemoryBuf);
    return ret;
  }

  tscDebug("%p retrieved query data from %d vnode(s)", pSql, pState->numOfSub);
  pSql->pSubs = calloc(pState->numOfSub, POINTER_BYTES);
  if (pSql->pSubs == NULL) {
    tfree(pSql->pSubs);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pFinalModel,pState->numOfSub);

    tscAsyncResultOnError(pSql);
    return ret;
  }

  if (pState->states == NULL) {
    pState->states = calloc(pState->numOfSub, sizeof(*pState->states));
    if (pState->states == NULL) {
      pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      tscAsyncResultOnError(pSql);
      tfree(pMemoryBuf);
      return ret;
    }

    pthread_mutex_init(&pState->mutex, NULL);
  }

  memset(pState->states, 0, sizeof(*pState->states) * pState->numOfSub);
  tscDebug("%p reset all sub states to 0", pSql);
  
  pRes->code = TSDB_CODE_SUCCESS;
  
  int32_t i = 0;
  for (; i < pState->numOfSub; ++i) {
    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    if (trs == NULL) {
      tscError("%p failed to malloc buffer for SRetrieveSupport, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      break;
    }
    
    trs->pExtMemBuffer = pMemoryBuf;
    trs->pOrderDescriptor = pDesc;

    trs->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
    if (trs->localBuffer == NULL) {
      tscError("%p failed to malloc buffer for local buffer, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      tfree(trs);
      break;
    }
    
    trs->subqueryIndex  = i;
    trs->pParentSql     = pSql;
    trs->pFinalColModel = pModel;
    trs->pFFColModel    = pFinalModel;

    SSqlObj *pNew = tscCreateSTableSubquery(pSql, trs, NULL);
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
      assert(pNewQueryInfo->tsBuf != NULL);
    }
    
    tscDebug("%p sub:%p create subquery success. orderOfSub:%d", pSql, pNew, trs->subqueryIndex);
  }
  
  if (i < pState->numOfSub) {
    tscError("%p failed to prepare subquery structure and launch subqueries", pSql);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pFinalModel, pState->numOfSub);
    doCleanupSubqueries(pSql, i);
    return pRes->code;   // free all allocated resource
  }
  
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pFinalModel, pState->numOfSub);
    doCleanupSubqueries(pSql, i);
    return pRes->code;
  }
  
  for(int32_t j = 0; j < pState->numOfSub; ++j) {
    SSqlObj* pSub = pSql->pSubs[j];
    SRetrieveSupport* pSupport = pSub->param;
    
    tscDebug("%p sub:%p launch subquery, orderOfSub:%d.", pSql, pSub, pSupport->subqueryIndex);
    tscProcessSql(pSub);
  }

  return TSDB_CODE_SUCCESS;
}

static void tscFreeRetrieveSup(SSqlObj *pSql) {
  SRetrieveSupport *trsupport = pSql->param;

  void* p = atomic_val_compare_exchange_ptr(&pSql->param, trsupport, 0);
  if (p == NULL) {
    tscDebug("%p retrieve supp already released", pSql);
    return;
  }

  tscDebug("%p start to free subquery supp obj:%p", pSql, trsupport);
  tfree(trsupport->localBuffer);
  tfree(trsupport);
}

static void tscRetrieveFromDnodeCallBack(void *param, TAOS_RES *tres, int numOfRows);
static void tscHandleSubqueryError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows);

static void tscAbortFurtherRetryRetrieval(SRetrieveSupport *trsupport, TAOS_RES *tres, int32_t code) {
// set no disk space error info
  tscError("sub:%p failed to flush data to disk, reason:%s", tres, tstrerror(code));
  SSqlObj* pParentSql = trsupport->pParentSql;

  pParentSql->res.code = code;
  trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
  tscHandleSubqueryError(trsupport, tres, pParentSql->res.code);
}

/*
 * current query failed, and the retry count is less than the available
 * count, retry query clear previous retrieved data, then launch a new sub query
 */
static int32_t tscReissueSubquery(SRetrieveSupport *oriTrs, SSqlObj *pSql, int32_t code, int32_t *sent) {
  *sent = 0;
  
  SRetrieveSupport *trsupport = malloc(sizeof(SRetrieveSupport));
  if (trsupport == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  memcpy(trsupport, oriTrs, sizeof(*trsupport));

  const uint32_t nBufferSize = (1u << 16u);  // 64KB
  trsupport->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
  if (trsupport->localBuffer == NULL) {
    tscError("%p failed to malloc buffer for local buffer, reason:%s", pSql, strerror(errno));
    tfree(trsupport);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  
  SSqlObj *pParentSql = trsupport->pParentSql;
  int32_t  subqueryIndex = trsupport->subqueryIndex;

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[0];

  tExtMemBufferClear(trsupport->pExtMemBuffer[subqueryIndex]);

  // clear local saved number of results
  trsupport->localBuffer->num = 0;
  tscError("%p sub:%p retrieve/query failed, code:%s, orderOfSub:%d, retry:%d", trsupport->pParentSql, pSql,
           tstrerror(code), subqueryIndex, trsupport->numOfRetry);

  SSqlObj *pNew = tscCreateSTableSubquery(trsupport->pParentSql, trsupport, pSql);
  if (pNew == NULL) {
    tscError("%p sub:%p failed to create new subquery due to error:%s, abort retry, vgId:%d, orderOfSub:%d",
             oriTrs->pParentSql, pSql, tstrerror(terrno), pVgroup->vgId, oriTrs->subqueryIndex);

    pParentSql->res.code = terrno;
    oriTrs->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;

    tfree(trsupport);
    return pParentSql->res.code;
  }

  int32_t ret = tscProcessSql(pNew);

  *sent = 1;
  
  // if failed to process sql, let following code handle the pSql
  if (ret == TSDB_CODE_SUCCESS) {
    tscFreeRetrieveSup(pSql);
    taos_free_result(pSql);
    return ret;
  } else {    
    pParentSql->pSubs[trsupport->subqueryIndex] = pSql;
    tscFreeRetrieveSup(pNew);
    taos_free_result(pNew);
    return ret;
  }
}

void tscHandleSubqueryError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows) {
  // it has been freed already
  if (pSql->param != trsupport || pSql->param == NULL) {
    return;
  }

  SSqlObj *pParentSql = trsupport->pParentSql;
  int32_t  subqueryIndex = trsupport->subqueryIndex;
  
  assert(pSql != NULL);

  SSubqueryState* pState = &pParentSql->subState;

  // retrieved in subquery failed. OR query cancelled in retrieve phase.
  if (taos_errno(pSql) == TSDB_CODE_SUCCESS && pParentSql->res.code != TSDB_CODE_SUCCESS) {

    /*
     * kill current sub-query connection, which may retrieve data from vnodes;
     * Here we get: pPObj->res.code == TSDB_CODE_TSC_QUERY_CANCELLED
     */
    pSql->res.numOfRows = 0;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;  // disable retry efforts
    tscDebug("%p query is cancelled, sub:%p, orderOfSub:%d abort retrieve, code:%s", pParentSql, pSql,
             subqueryIndex, tstrerror(pParentSql->res.code));
  }

  if (numOfRows >= 0) {  // current query is successful, but other sub query failed, still abort current query.
    tscDebug("%p sub:%p retrieve numOfRows:%d,orderOfSub:%d", pParentSql, pSql, numOfRows, subqueryIndex);
    tscError("%p sub:%p abort further retrieval due to other queries failure,orderOfSub:%d,code:%s", pParentSql, pSql,
             subqueryIndex, tstrerror(pParentSql->res.code));
  } else {
    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY && pParentSql->res.code == TSDB_CODE_SUCCESS) {
      int32_t sent = 0;
      
      tscReissueSubquery(trsupport, pSql, numOfRows, &sent);
      if (sent) {
        return;
      }
    } else {  // reach the maximum retry count, abort
      atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, numOfRows);
      tscError("%p sub:%p retrieve failed,code:%s,orderOfSub:%d failed.no more retry,set global code:%s", pParentSql, pSql,
               tstrerror(numOfRows), subqueryIndex, tstrerror(pParentSql->res.code));
    }
  }

  if (!subAndCheckDone(pSql, pParentSql, subqueryIndex)) {
    tscDebug("%p sub:%p,%d freed, not finished, total:%d", pParentSql, pSql, trsupport->subqueryIndex, pState->numOfSub);

    tscFreeRetrieveSup(pSql);
    return;
  }  
  
  // all subqueries are failed
  tscError("%p retrieve from %d vnode(s) completed,code:%s.FAILED.", pParentSql, pState->numOfSub,
      tstrerror(pParentSql->res.code));

  // release allocated resource
  tscLocalReducerEnvDestroy(trsupport->pExtMemBuffer, trsupport->pOrderDescriptor, trsupport->pFinalColModel, trsupport->pFFColModel,
                            pState->numOfSub);
  
  tscFreeRetrieveSup(pSql);

  // in case of second stage join subquery, invoke its callback function instead of regular QueueAsyncRes
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, 0);

  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
    (*pParentSql->fp)(pParentSql->param, pParentSql, pParentSql->res.code);
  } else {  // regular super table query
    if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
      tscAsyncResultOnError(pParentSql);
    }
  }
}

static void tscAllDataRetrievedFromDnode(SRetrieveSupport *trsupport, SSqlObj* pSql) {
  int32_t           idx = trsupport->subqueryIndex;
  SSqlObj *         pParentSql = trsupport->pParentSql;
  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;
  
  SSubqueryState* pState = &pParentSql->subState;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];
  
  // data in from current vnode is stored in cache and disk
  uint32_t numOfRowsFromSubquery = (uint32_t)(trsupport->pExtMemBuffer[idx]->numOfTotalElems + trsupport->localBuffer->num);
  SVgroupsInfo* vgroupsInfo = pTableMetaInfo->vgroupList;
  tscDebug("%p sub:%p all data retrieved from ep:%s, vgId:%d, numOfRows:%d, orderOfSub:%d", pParentSql, pSql,
           vgroupsInfo->vgroups[0].epAddr[0].fqdn, vgroupsInfo->vgroups[0].vgId, numOfRowsFromSubquery, idx);
  
  tColModelCompact(pDesc->pColumnModel, trsupport->localBuffer, pDesc->pColumnModel->capacity);

#ifdef _DEBUG_VIEW
  printf("%" PRIu64 " rows data flushed to disk:\n", trsupport->localBuffer->num);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, trsupport->localBuffer->data, trsupport->localBuffer->num,
                       trsupport->localBuffer->num, colInfo);
#endif
  
  if (tsTotalTmpDirGB != 0 && tsAvailTmpDirectorySpace < tsReservedTmpDirectorySpace) {
    tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pParentSql, pSql,
             tsAvailTmpDirectorySpace, tsReservedTmpDirectorySpace);
    tscAbortFurtherRetryRetrieval(trsupport, pSql, TSDB_CODE_TSC_NO_DISKSPACE);
    return;
  }
  
  // each result for a vnode is ordered as an independant list,
  // then used as an input of loser tree for disk-based merge
  int32_t code = tscFlushTmpBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pQueryInfo->groupbyExpr.orderType);
  if (code != 0) { // set no disk space error info, and abort retry
    tscAbortFurtherRetryRetrieval(trsupport, pSql, code);
    return;
  }
  
  if (!subAndCheckDone(pSql, pParentSql, idx)) {
    tscDebug("%p sub:%p orderOfSub:%d freed, not finished", pParentSql, pSql, trsupport->subqueryIndex);

    tscFreeRetrieveSup(pSql);
    return;
  }  
  
  // all sub-queries are returned, start to local merge process
  pDesc->pColumnModel->capacity = trsupport->pExtMemBuffer[idx]->numOfElemsPerPage;
  
  tscDebug("%p retrieve from %d vnodes completed.final NumOfRows:%" PRId64 ",start to build loser tree", pParentSql,
           pState->numOfSub, pState->numOfRetrievedRows);
  
  SQueryInfo *pPQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, 0);
  tscClearInterpInfo(pPQueryInfo);
  
  tscCreateLocalMerger(trsupport->pExtMemBuffer, pState->numOfSub, pDesc, trsupport->pFinalColModel, trsupport->pFFColModel, pParentSql);
  tscDebug("%p build loser tree completed", pParentSql);
  
  pParentSql->res.precision = pSql->res.precision;
  pParentSql->res.numOfRows = 0;
  pParentSql->res.row = 0;
  
  tscFreeRetrieveSup(pSql);

  // set the command flag must be after the semaphore been correctly set.
  pParentSql->cmd.command = TSDB_SQL_RETRIEVE_LOCALMERGE;
  if (pParentSql->res.code == TSDB_CODE_SUCCESS) {
    (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
  } else {
    tscAsyncResultOnError(pParentSql);
  }
}

static void tscRetrieveFromDnodeCallBack(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlObj *pSql = (SSqlObj *)tres;
  assert(pSql != NULL);

  // this query has been freed already
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;
  if (pSql->param == NULL || param == NULL) {
    tscDebug("%p already freed in dnodecallback", pSql);
    return;
  }

  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;
  int32_t           idx   = trsupport->subqueryIndex;
  SSqlObj *         pParentSql = trsupport->pParentSql;

  SSubqueryState* pState = &pParentSql->subState;
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SVgroupInfo  *pVgroup = &pTableMetaInfo->vgroupList->vgroups[0];

  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    tscDebug("%p query cancelled/failed, sub:%p, vgId:%d, orderOfSub:%d, code:%s, global code:%s",
             pParentSql, pSql, pVgroup->vgId, trsupport->subqueryIndex, tstrerror(numOfRows), tstrerror(pParentSql->res.code));

    tscHandleSubqueryError(param, tres, numOfRows);
    return;
  }

  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(numOfRows == taos_errno(pSql));

    if (numOfRows == TSDB_CODE_TSC_QUERY_CANCELLED) {
      trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    }

    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY) {
      tscError("%p sub:%p failed code:%s, retry:%d", pParentSql, pSql, tstrerror(numOfRows), trsupport->numOfRetry);

      int32_t sent = 0;
      
      tscReissueSubquery(trsupport, pSql, numOfRows, &sent);
      if (sent) {
        return;
      }
    } else {
      tscDebug("%p sub:%p reach the max retry times, set global code:%s", pParentSql, pSql, tstrerror(numOfRows));
      atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, numOfRows);  // set global code and abort
    }

    tscHandleSubqueryError(param, tres, numOfRows);
    return;
  }
  
  SSqlRes *   pRes = &pSql->res;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  if (numOfRows > 0) {
    assert(pRes->numOfRows == numOfRows);
    int64_t num = atomic_add_fetch_64(&pState->numOfRetrievedRows, numOfRows);
    
    tscDebug("%p sub:%p retrieve numOfRows:%d totalNumOfRows:%" PRIu64 " from ep:%s, orderOfSub:%d", pParentSql, pSql,
             pRes->numOfRows, pState->numOfRetrievedRows, pSql->epSet.fqdn[pSql->epSet.inUse], idx);

    if (num > tsMaxNumOfOrderedResults && tscIsProjectionQueryOnSTable(pQueryInfo, 0)) {
      tscError("%p sub:%p num of OrderedRes is too many, max allowed:%" PRId32 " , current:%" PRId64,
               pParentSql, pSql, tsMaxNumOfOrderedResults, num);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_TSC_SORTED_RES_TOO_MANY);
      return;
    }

#ifdef _DEBUG_VIEW
    printf("received data from vnode: %"PRIu64" rows\n", pRes->numOfRows);
    SSrcColumnInfo colInfo[256] = {0};

    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, pRes->data, pRes->numOfRows, pRes->numOfRows, colInfo);
#endif
    
    // no disk space for tmp directory
    if (tsTotalTmpDirGB != 0 && tsAvailTmpDirectorySpace < tsReservedTmpDirectorySpace) {
      tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pParentSql, pSql,
               tsAvailTmpDirectorySpace, tsReservedTmpDirectorySpace);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_TSC_NO_DISKSPACE);
      return;
    }
    
    int32_t ret = saveToBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pRes->data,
                               pRes->numOfRows, pQueryInfo->groupbyExpr.orderType);
    if (ret != 0) { // set no disk space error info, and abort retry
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_TSC_NO_DISKSPACE);
    } else if (pRes->completed) {
      tscAllDataRetrievedFromDnode(trsupport, pSql);
    } else { // continue fetch data from dnode
      taos_fetch_rows_a(tres, tscRetrieveFromDnodeCallBack, param);
    }
    
  } else { // all data has been retrieved to client
    tscAllDataRetrievedFromDnode(trsupport, pSql);
  }
}

static SSqlObj *tscCreateSTableSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj) {
  const int32_t table_index = 0;
  
  SSqlObj *pNew = createSubqueryObj(pSql, table_index, tscRetrieveDataRes, trsupport, TSDB_SQL_SELECT, prevSqlObj);
  if (pNew != NULL) {  // the sub query of two-stage super table query
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);

    pQueryInfo->type |= TSDB_QUERY_TYPE_STABLE_SUBQUERY;

    // clear the limit/offset info, since it should not be sent to vnode to be executed.
    pQueryInfo->limit.limit = -1;
    pQueryInfo->limit.offset = 0;

    assert(pQueryInfo->numOfTables == 1 && pNew->cmd.numOfClause == 1 && trsupport->subqueryIndex < pSql->subState.numOfSub);
    
    // launch subquery for each vnode, so the subquery index equals to the vgroupIndex.
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, table_index);
    pTableMetaInfo->vgroupIndex = trsupport->subqueryIndex;

    pSql->pSubs[trsupport->subqueryIndex] = pNew;
  }
  
  return pNew;
}

// todo there is are race condition in this function, while cancel is called by user.
void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code) {
  // the param may be null, since it may be done by other query threads. and the asyncOnError may enter in this
  // function while kill query by a user.
  if (param == NULL) {
    assert(code != TSDB_CODE_SUCCESS);
    return;
  }

  SRetrieveSupport *trsupport = (SRetrieveSupport *) param;
  
  SSqlObj*  pParentSql = trsupport->pParentSql;
  SSqlObj*  pSql = (SSqlObj *) tres;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  assert(pSql->cmd.numOfClause == 1 && pQueryInfo->numOfTables == 1);
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[trsupport->subqueryIndex];

  // stable query killed or other subquery failed, all query stopped
  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    tscError("%p query cancelled or failed, sub:%p, vgId:%d, orderOfSub:%d, code:%s, global code:%s",
        pParentSql, pSql, pVgroup->vgId, trsupport->subqueryIndex, tstrerror(code), tstrerror(pParentSql->res.code));

    tscHandleSubqueryError(param, tres, code);
    return;
  }
  
  /*
   * if a subquery on a vnode failed, all retrieve operations from vnode that occurs later
   * than this one are actually not necessary, we simply call the tscRetrieveFromDnodeCallBack
   * function to abort current and remain retrieve process.
   *
   * NOTE: thread safe is required.
   */
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(code == taos_errno(pSql));

    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY) {
      tscError("%p sub:%p failed code:%s, retry:%d", pParentSql, pSql, tstrerror(code), trsupport->numOfRetry);
      
      int32_t sent = 0;

      tscReissueSubquery(trsupport, pSql, code, &sent);
      if (sent) {
        return;
      }
    } else {
      tscError("%p sub:%p reach the max retry times, set global code:%s", pParentSql, pSql, tstrerror(code));
      atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, code);  // set global code and abort
    }

    tscHandleSubqueryError(param, tres, pParentSql->res.code);
    return;
  }

  tscDebug("%p sub:%p query complete, ep:%s, vgId:%d, orderOfSub:%d, retrieve data", trsupport->pParentSql, pSql,
             pVgroup->epAddr[0].fqdn, pVgroup->vgId, trsupport->subqueryIndex);

  if (pSql->res.qhandle == 0) { // qhandle is NULL, code is TSDB_CODE_SUCCESS means no results generated from this vnode
    tscRetrieveFromDnodeCallBack(param, pSql, 0);
  } else {
    taos_fetch_rows_a(tres, tscRetrieveFromDnodeCallBack, param);
  }
}

static bool needRetryInsert(SSqlObj* pParentObj, int32_t numOfSub) {
  if (pParentObj->retry > pParentObj->maxRetry) {
    tscError("%p max retry reached, abort the retry effort", pParentObj);
    return false;
  }

  for (int32_t i = 0; i < numOfSub; ++i) {
    int32_t code = pParentObj->pSubs[i]->res.code;
    if (code == TSDB_CODE_SUCCESS) {
      continue;
    }

    if (code != TSDB_CODE_TDB_TABLE_RECONFIGURE && code != TSDB_CODE_TDB_INVALID_TABLE_ID &&
        code != TSDB_CODE_VND_INVALID_VGROUP_ID && code != TSDB_CODE_RPC_NETWORK_UNAVAIL &&
        code != TSDB_CODE_APP_NOT_READY) {
      pParentObj->res.code = code;
      return false;
    }
  }

  return true;
}

static void doFreeInsertSupporter(SSqlObj* pSqlObj) {
  assert(pSqlObj != NULL && pSqlObj->subState.numOfSub > 0);

  for(int32_t i = 0; i < pSqlObj->subState.numOfSub; ++i) {
    SSqlObj* pSql = pSqlObj->pSubs[i];
    tfree(pSql->param);
  }
}

static void multiVnodeInsertFinalize(void* param, TAOS_RES* tres, int numOfRows) {
  SInsertSupporter *pSupporter = (SInsertSupporter *)param;
  SSqlObj* pParentObj = pSupporter->pSql;

  // record the total inserted rows
  if (numOfRows > 0) {
    atomic_add_fetch_32(&pParentObj->res.numOfRows, numOfRows);
  }

  if (taos_errno(tres) != TSDB_CODE_SUCCESS) {
    SSqlObj* pSql = (SSqlObj*) tres;
    assert(pSql != NULL && pSql->res.code == numOfRows);
    
    pParentObj->res.code = pSql->res.code;

    // set the flag in the parent sqlObj
    if (pSql->cmd.submitSchema) {
      pParentObj->cmd.submitSchema = 1;
    }
  }

  if (!subAndCheckDone(tres, pParentObj, pSupporter->index)) {
    tscDebug("%p insert:%p,%d completed, total:%d", pParentObj, tres, pSupporter->index, pParentObj->subState.numOfSub);
    return;
  }

  // restore user defined fp
  pParentObj->fp = pParentObj->fetchFp;
  int32_t numOfSub = pParentObj->subState.numOfSub;
  doFreeInsertSupporter(pParentObj);

  if (pParentObj->res.code == TSDB_CODE_SUCCESS) {
    tscDebug("%p Async insertion completed, total inserted:%d", pParentObj, pParentObj->res.numOfRows);

    // todo remove this parameter in async callback function definition.
    // all data has been sent to vnode, call user function
    int32_t v = (pParentObj->res.code != TSDB_CODE_SUCCESS) ? pParentObj->res.code : (int32_t)pParentObj->res.numOfRows;
    (*pParentObj->fp)(pParentObj->param, pParentObj, v);
  } else {
    if (!needRetryInsert(pParentObj, numOfSub)) {
      tscAsyncResultOnError(pParentObj);
      return;
    }

    int32_t numOfFailed = 0;
    for(int32_t i = 0; i < numOfSub; ++i) {
      SSqlObj* pSql = pParentObj->pSubs[i];
      if (pSql->res.code != TSDB_CODE_SUCCESS) {
        numOfFailed += 1;

        // clean up tableMeta in cache
        tscFreeQueryInfo(&pSql->cmd, false);
        SQueryInfo* pQueryInfo = tscGetQueryInfoDetailSafely(&pSql->cmd, 0);
        STableMetaInfo* pMasterTableMetaInfo = tscGetTableMetaInfoFromCmd(&pParentObj->cmd, pSql->cmd.clauseIndex, 0);
        tscAddTableMetaInfo(pQueryInfo, &pMasterTableMetaInfo->name, NULL, NULL, NULL, NULL);

        subquerySetState(pSql, &pParentObj->subState, i, 0);

        tscDebug("%p, failed sub:%d, %p", pParentObj, i, pSql);
      }
    }

    tscError("%p Async insertion completed, total inserted:%d rows, numOfFailed:%d, numOfTotal:%d", pParentObj,
             pParentObj->res.numOfRows, numOfFailed, numOfSub);

    tscDebug("%p cleanup %d tableMeta in hashTable", pParentObj, pParentObj->cmd.numOfTables);
    for(int32_t i = 0; i < pParentObj->cmd.numOfTables; ++i) {
      char name[TSDB_TABLE_FNAME_LEN] = {0};
      tNameExtractFullName(pParentObj->cmd.pTableNameList[i], name);
      taosHashRemove(tscTableMetaInfo, name, strnlen(name, TSDB_TABLE_FNAME_LEN));
    }

    pParentObj->cmd.parseFinished = false;

    tscResetSqlCmd(&pParentObj->cmd, false);

    // in case of insert, redo parsing the sql string and build new submit data block for two reasons:
    // 1. the table Id(tid & uid) may have been update, the submit block needs to be updated accordingly.
    // 2. vnode may need the schema information along with submit block to update its local table schema.
    tscDebug("%p re-parse sql to generate submit data, retry:%d", pParentObj, pParentObj->retry);
    pParentObj->retry++;

    int32_t code = tsParseSql(pParentObj, true);
    if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) return;

    if (code != TSDB_CODE_SUCCESS) {
      pParentObj->res.code = code;
      tscAsyncResultOnError(pParentObj);
      return;
    }

    tscDoQuery(pParentObj);
  }
}

/**
 * it is a subquery, so after parse the sql string, copy the submit block to payload of itself
 * @param pSql
 * @return
 */
int32_t tscHandleInsertRetry(SSqlObj* pParent, SSqlObj* pSql) {
  assert(pSql != NULL && pSql->param != NULL);
  SSqlRes* pRes = &pSql->res;

  SInsertSupporter* pSupporter = (SInsertSupporter*) pSql->param;
  assert(pSupporter->index < pSupporter->pSql->subState.numOfSub);

  STableDataBlocks* pTableDataBlock = taosArrayGetP(pParent->cmd.pDataBlocks, pSupporter->index);
  int32_t code = tscCopyDataBlockToPayload(pSql, pTableDataBlock);

  if ((pRes->code = code)!= TSDB_CODE_SUCCESS) {
    tscAsyncResultOnError(pSql);
    return code;  // here the pSql may have been released already.
  }

  return tscProcessSql(pSql);
}

int32_t tscHandleMultivnodeInsert(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  // it is the failure retry insert
  if (pSql->pSubs != NULL) {
    for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      SSqlObj* pSub = pSql->pSubs[i];
      SInsertSupporter* pSup = calloc(1, sizeof(SInsertSupporter));
      pSup->index = i;
      pSup->pSql = pSql;

      pSub->param = pSup;
      tscDebug("%p sub:%p launch sub insert, orderOfSub:%d", pSql, pSub, i);
      if (pSub->res.code != TSDB_CODE_SUCCESS) {
        tscHandleInsertRetry(pSql, pSub);
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  pSql->subState.numOfSub = (uint16_t)taosArrayGetSize(pCmd->pDataBlocks);
  assert(pSql->subState.numOfSub > 0);

  pRes->code = TSDB_CODE_SUCCESS;

  // the number of already initialized subqueries
  int32_t numOfSub = 0;

  if (pSql->subState.states == NULL) {
    pSql->subState.states = calloc(pSql->subState.numOfSub, sizeof(*pSql->subState.states));
    if (pSql->subState.states == NULL) {
      pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }

    pthread_mutex_init(&pSql->subState.mutex, NULL);
  }

  memset(pSql->subState.states, 0, sizeof(*pSql->subState.states) * pSql->subState.numOfSub);
  tscDebug("%p reset all sub states to 0", pSql);

  pSql->pSubs = calloc(pSql->subState.numOfSub, POINTER_BYTES);
  if (pSql->pSubs == NULL) {
    goto _error;
  }

  tscDebug("%p submit data to %d vnode(s)", pSql, pSql->subState.numOfSub);

  while(numOfSub < pSql->subState.numOfSub) {
    SInsertSupporter* pSupporter = calloc(1, sizeof(SInsertSupporter));
    if (pSupporter == NULL) {
      goto _error;
    }

    pSupporter->pSql   = pSql;
    pSupporter->index  = numOfSub;

    SSqlObj *pNew = createSimpleSubObj(pSql, multiVnodeInsertFinalize, pSupporter, TSDB_SQL_INSERT);
    if (pNew == NULL) {
      tscError("%p failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql, numOfSub, strerror(errno));
      goto _error;
    }
  
    /*
     * assign the callback function to fetchFp to make sure that the error process function can restore
     * the callback function (multiVnodeInsertFinalize) correctly.
     */
    pNew->fetchFp = pNew->fp;
    pSql->pSubs[numOfSub] = pNew;

    STableDataBlocks* pTableDataBlock = taosArrayGetP(pCmd->pDataBlocks, numOfSub);
    pRes->code = tscCopyDataBlockToPayload(pNew, pTableDataBlock);
    if (pRes->code == TSDB_CODE_SUCCESS) {
      tscDebug("%p sub:%p create subObj success. orderOfSub:%d", pSql, pNew, numOfSub);
      numOfSub++;
    } else {
      tscDebug("%p prepare submit data block failed in async insertion, vnodeIdx:%d, total:%d, code:%s", pSql, numOfSub,
               pSql->subState.numOfSub, tstrerror(pRes->code));
      goto _error;
    }
  }
  
  if (numOfSub < pSql->subState.numOfSub) {
    tscError("%p failed to prepare subObj structure and launch sub-insertion", pSql);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

  // use the local variable
  for (int32_t j = 0; j < numOfSub; ++j) {
    SSqlObj *pSub = pSql->pSubs[j];
    tscDebug("%p sub:%p launch sub insert, orderOfSub:%d", pSql, pSub, j);
    tscProcessSql(pSub);
  }

  return TSDB_CODE_SUCCESS;

  _error:
  return TSDB_CODE_TSC_OUT_OF_MEMORY;
}

static char* getResultBlockPosition(SSqlCmd* pCmd, SSqlRes* pRes, int32_t columnIndex, int16_t* bytes) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  SInternalField* pInfo = (SInternalField*) TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, columnIndex);
  assert(pInfo->pSqlExpr != NULL);

  *bytes = pInfo->pSqlExpr->resBytes;
  char* pData = pRes->data + pInfo->pSqlExpr->offset * pRes->numOfRows + pRes->row * (*bytes);

  return pData;
}

static void doBuildResFromSubqueries(SSqlObj* pSql) {
  SSqlRes* pRes = &pSql->res;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);

  int32_t numOfRes = INT32_MAX;
  for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }

    int32_t remain = (int32_t)(pSub->res.numOfRows - pSub->res.row);
    numOfRes = (int32_t)(MIN(numOfRes, remain));
  }

  if (numOfRes == 0) {  // no result any more, free all subquery objects
    freeJoinSubqueryObj(pSql);
    return;
  }

  int32_t rowSize = tscGetResRowLength(pQueryInfo->exprList);

  assert(numOfRes * rowSize > 0);
  char* tmp = realloc(pRes->pRsp, numOfRes * rowSize + sizeof(tFilePage));
  if (tmp == NULL) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return;
  } else {
    pRes->pRsp = tmp;
  }

  tFilePage* pFilePage = (tFilePage*) pRes->pRsp;
  pFilePage->num = numOfRes;

  pRes->data = pFilePage->data;
  char* data = pRes->data;

  int16_t bytes = 0;

  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SColumnIndex* pIndex = &pRes->pColumnIndex[i];
    SSqlRes*      pRes1 = &pSql->pSubs[pIndex->tableIndex]->res;
    SSqlCmd*      pCmd1 = &pSql->pSubs[pIndex->tableIndex]->cmd;

    char* pData = getResultBlockPosition(pCmd1, pRes1, pIndex->columnIndex, &bytes);
    memcpy(data, pData, bytes * numOfRes);

    data += bytes * numOfRes;
  }

  for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }

    pSub->res.row += numOfRes;
    assert(pSub->res.row <= pSub->res.numOfRows);
  }

  pRes->numOfRows = numOfRes;
  pRes->numOfClauseTotal += numOfRes;

  int32_t finalRowSize = 0;
  for(int32_t i = 0; i < tscNumOfFields(pQueryInfo); ++i) {
    TAOS_FIELD* pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
    finalRowSize += pField->bytes;
  }

  doArithmeticCalculate(pQueryInfo, pFilePage, rowSize, finalRowSize);

  pRes->data = pFilePage->data;
  tscSetResRawPtr(pRes, pQueryInfo);
}

void tscBuildResFromSubqueries(SSqlObj *pSql) {
  SSqlRes* pRes = &pSql->res;

  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscAsyncResultOnError(pSql);
    return;
  }

  if (pRes->tsrow == NULL) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
    pRes->numOfCols = (int16_t) tscSqlExprNumOfExprs(pQueryInfo);

    pRes->tsrow  = calloc(pRes->numOfCols, POINTER_BYTES);
    pRes->urow   = calloc(pRes->numOfCols, POINTER_BYTES);
    pRes->buffer = calloc(pRes->numOfCols, POINTER_BYTES);
    pRes->length = calloc(pRes->numOfCols, sizeof(int32_t));

    if (pRes->tsrow == NULL || pRes->buffer == NULL || pRes->length == NULL) {
      pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      tscAsyncResultOnError(pSql);
      return;
    }

    tscRestoreFuncForSTableQuery(pQueryInfo);
  }

  assert (pRes->row >= pRes->numOfRows);
  doBuildResFromSubqueries(pSql);
  if (pRes->code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pSql->param, pSql, pRes->numOfRows);
  } else {
    tscAsyncResultOnError(pSql);
  }
}

static UNUSED_FUNC void transferNcharData(SSqlObj *pSql, int32_t columnIndex, TAOS_FIELD *pField) {
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->tsrow[columnIndex] != NULL && pField->type == TSDB_DATA_TYPE_NCHAR) {
    // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
    if (pRes->buffer[columnIndex] == NULL) {
      pRes->buffer[columnIndex] = malloc(pField->bytes + TSDB_NCHAR_SIZE);
    }
    
    /* string terminated char for binary data*/
    memset(pRes->buffer[columnIndex], 0, pField->bytes + TSDB_NCHAR_SIZE);
    
    int32_t length = taosUcs4ToMbs(pRes->tsrow[columnIndex], pRes->length[columnIndex], pRes->buffer[columnIndex]);
    if ( length >= 0 ) {
      pRes->tsrow[columnIndex] = (unsigned char*)pRes->buffer[columnIndex];
      pRes->length[columnIndex] = length;
    } else {
      tscError("%p charset:%s to %s. val:%s convert failed.", pSql, DEFAULT_UNICODE_ENCODEC, tsCharset, (char*)pRes->tsrow[columnIndex]);
      pRes->tsrow[columnIndex] = NULL;
      pRes->length[columnIndex] = 0;
    }
  }
}

char *getArithmeticInputSrc(void *param, const char *name, int32_t colId) {
  SArithmeticSupport *pSupport = (SArithmeticSupport *) param;

  int32_t index = -1;
  SSqlExpr* pExpr = NULL;
  
  for (int32_t i = 0; i < pSupport->numOfCols; ++i) {
    pExpr = taosArrayGetP(pSupport->exprList, i);
    if (strncmp(name, pExpr->aliasName, sizeof(pExpr->aliasName) - 1) == 0) {
      index = i;
      break;
    }
  }

  assert(index >= 0 && index < pSupport->numOfCols);
  return pSupport->data[index] + pSupport->offset * pExpr->resBytes;
}

TAOS_ROW doSetResultRowData(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  assert(pRes->row >= 0 && pRes->row <= pRes->numOfRows);
  if (pRes->row >= pRes->numOfRows) {  // all the results has returned to invoker
    tfree(pRes->tsrow);
    return pRes->tsrow;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  size_t size = tscNumOfFields(pQueryInfo);
  for (int i = 0; i < size; ++i) {
    SInternalField* pInfo = (SInternalField*)TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, i);

    int32_t type  = pInfo->field.type;
    int32_t bytes = pInfo->field.bytes;

    if (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR) {
      pRes->tsrow[i] = isNull(pRes->urow[i], type) ? NULL : pRes->urow[i];
    } else {
      pRes->tsrow[i] = isNull(pRes->urow[i], type) ? NULL : varDataVal(pRes->urow[i]);
      pRes->length[i] = varDataLen(pRes->urow[i]);
    }

    ((char**) pRes->urow)[i] += bytes;
  }

  pRes->row++;  // index increase one-step
  return pRes->tsrow;
}

static UNUSED_FUNC bool tscHasRemainDataInSubqueryResultSet(SSqlObj *pSql) {
  bool     hasData = true;
  SSqlCmd *pCmd = &pSql->cmd;
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    bool allSubqueryExhausted = true;
    
    for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
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
    for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      if (pSql->pSubs[i] == 0) {
        continue;
      }
      
      SSqlRes *   pRes1 = &pSql->pSubs[i]->res;
      SQueryInfo *pQueryInfo1 = tscGetQueryInfoDetail(&pSql->pSubs[i]->cmd, 0);
      
      if ((pRes1->row >= pRes1->numOfRows && tscHasReachLimitation(pQueryInfo1, pRes1) &&
          tscIsProjectionQuery(pQueryInfo1)) || (pRes1->numOfRows == 0)) {
        hasData = false;
        break;
      }
    }
  }
  
  return hasData;
}
