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

#include "qAst.h"
#include "qTsbuf.h"
#include "tcompare.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tschemautil.h"
#include "tsclient.h"

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

            tsBufAppend(output1, elem1.vnode, elem1.tag, (const char*)&elem1.ts, sizeof(elem1.ts));
            tsBufAppend(output2, elem2.vnode, elem2.tag, (const char*)&elem2.ts, sizeof(elem2.ts));
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
  tsBufDestroy(pSupporter2->pTSBuf);

  TSKEY et = taosGetTimestampUs();
  tscDebug("%p input1:%" PRId64 ", input2:%" PRId64 ", final:%" PRId64 " in %d vnodes for secondary query after ts blocks "
           "intersecting, skey:%" PRId64 ", ekey:%" PRId64 ", numOfVnode:%d, elapsed time:%" PRId64 " us",
           pSql, numOfInput1, numOfInput2, output1->numOfTotal, output1->numOfVnodes, win->skey, win->ekey,
           tsBufGetNumOfVnodes(output1), et - st);

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
  pSupporter->f = fopen(pSupporter->path, "w");

  // todo handle error
  if (pSupporter->f == NULL) {
    tscError("%p failed to create tmp file:%s, reason:%s", pSql, pSupporter->path, strerror(errno));
  }

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

  if (pSupporter->f != NULL) {
    fclose(pSupporter->f);
    unlink(pSupporter->path);
    pSupporter->f = NULL;
  }

  if (pSupporter->pVgroupTables != NULL) {
    taosArrayDestroy(pSupporter->pVgroupTables);
    pSupporter->pVgroupTables = NULL;
  }

  taosTFree(pSupporter->pIdTagList);
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
  tsBufGetVnodeIdList(pQueryInfo->tsBuf, &num, &list);

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

  taosTFree(list);
}

static SArray* buildVgroupTableByResult(SQueryInfo* pQueryInfo, SArray* pVgroupTables) {
  int32_t  num = 0;
  int32_t* list = NULL;
  tsBufGetVnodeIdList(pQueryInfo->tsBuf, &num, &list);

  int32_t numOfGroups = taosArrayGetSize(pVgroupTables);

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

  taosTFree(list);
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

  //the subqueries that do not actually launch the secondary query to virtual node is set as completed.
  SSubqueryState* pState = &pSql->subState;
  pState->numOfRemain = numOfSub;

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

    if ((pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) ||
        (funcId != TSDB_FUNC_TS && funcId != TSDB_FUNC_TS_DUMMY && funcId != TSDB_FUNC_PRJ)) {

      int16_t functionId = tscIsProjectionQuery(pQueryInfo)? TSDB_FUNC_PRJ : TSDB_FUNC_TS;

      tscAddSpecialColumnForSelect(pQueryInfo, 0, functionId, &index, s, TSDB_COL_NORMAL);
      tscPrintSelectClause(pNew, 0);
      tscFieldInfoUpdateOffset(pQueryInfo);

      pExpr = tscSqlExprGet(pQueryInfo, 0);
    }

    // set the join condition tag column info, todo extract method
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      assert(pQueryInfo->tagCond.joinInfo.hasJoin);
      int16_t colId = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->id.uid);

      // set the tag column id for executor to extract correct tag value
      pExpr->param[0] = (tVariant) {.i64Key = colId, .nType = TSDB_DATA_TYPE_BIGINT, .nLen = sizeof(int64_t)};
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

    size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
    tscDebug("%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%" PRIzu ", colList:%" PRIzu ", fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pTableMetaInfo->vgroupIndex, pQueryInfo->type, taosArrayGetSize(pQueryInfo->exprList),
             numOfCols, pQueryInfo->fieldsInfo.numOfOutput, pTableMetaInfo->name);
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

    if (pSub->res.code == TSDB_CODE_SUCCESS) {
      taos_free_result(pSub);
    }
  }

  pSql->subState.numOfSub = 0;
}

static void quitAllSubquery(SSqlObj* pSqlObj, SJoinSupporter* pSupporter) {
  assert(pSqlObj->subState.numOfRemain > 0);

  if (atomic_sub_fetch_32(&pSqlObj->subState.numOfRemain, 1) <= 0) {
    tscError("%p all subquery return and query failed, global code:%d", pSqlObj, pSqlObj->res.code);
    freeJoinSubqueryObj(pSqlObj);
  }
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

  return strncmp(tag1->data, tag2->data, tag1->len);
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
          tscSCMVgroupInfoCopy(&info.vgInfo, &pvg->vgroups[m]);
          break;
        }
      }
      assert(info.vgInfo.numOfEps != 0);

      vgTables = taosArrayInit(4, sizeof(STableIdInfo));
      info.itemList = vgTables;

      if (taosArrayGetSize(result) > 0) {
        SVgroupTableInfo* prevGroup = taosArrayGet(result, taosArrayGetSize(result) - 1);
        tscDebug("%p vgId:%d, tables:%"PRId64, pSql, prevGroup->vgInfo.vgId, taosArrayGetSize(prevGroup->itemList));
      }

      taosArrayPush(result, &info);
    }

    STableIdInfo item = {.uid = tt->uid, .tid = tt->tid, .key = INT64_MIN};
    taosArrayPush(vgTables, &item);

    tscTrace("%p tid:%d, uid:%"PRIu64",vgId:%d added", pSql, tt->tid, tt->uid, tt->vgId);
    prev = tt;
  }

  pTableMetaInfo->pVgroupTables = result;
  pTableMetaInfo->vgroupIndex = 0;

  if (taosArrayGetSize(result) > 0) {
    SVgroupTableInfo* g = taosArrayGet(result, taosArrayGetSize(result) - 1);
    tscDebug("%p vgId:%d, tables:%"PRId64, pSql, g->vgInfo.vgId, taosArrayGetSize(g->itemList));
  }
}

static void issueTSCompQuery(SSqlObj* pSql, SJoinSupporter* pSupporter, SSqlObj* pParent) {
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
  tscAddSpecialColumnForSelect(pQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);
  
  // set the tags value for ts_comp function
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, 0);
    int16_t tagColId = tscGetJoinTagColIdByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->id.uid);
    pExpr->param->i64Key = tagColId;
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
      tscSqlExprNumOfExprs(pQueryInfo), numOfCols, pQueryInfo->fieldsInfo.numOfOutput, pTableMetaInfo->name);
  
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

  tscDebug("%p tags match complete, result: %"PRId64", %"PRId64, pParentSql, t1, t2);
  return TSDB_CODE_SUCCESS;
}

static void tidTagRetrieveCallback(void* param, TAOS_RES* tres, int32_t numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSqlObj* pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  assert(TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY));

  // check for the error code firstly
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    // todo retry if other subqueries are not failed

    assert(numOfRows < 0 && numOfRows == taos_errno(pSql));
    tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);

    pParentSql->res.code = numOfRows;
    quitAllSubquery(pParentSql, pSupporter);

    tscQueueAsyncRes(pParentSql);
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
      quitAllSubquery(pParentSql, pSupporter);

      tscQueueAsyncRes(pParentSql);
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
  if (atomic_sub_fetch_32(&pParentSql->subState.numOfRemain, 1) > 0) {
    return;
  }

  SArray *s1 = NULL, *s2 = NULL;
  int32_t code = getIntersectionOfTableTuple(pQueryInfo, pParentSql, &s1, &s2);
  if (code != TSDB_CODE_SUCCESS) {
    freeJoinSubqueryObj(pParentSql);
    pParentSql->res.code = code;
    tscQueueAsyncRes(pParentSql);

    taosArrayDestroy(s1);
    taosArrayDestroy(s2);
    return;
  }

  if (taosArrayGetSize(s1) == 0 || taosArrayGetSize(s2) == 0) {  // no results,return.
    tscDebug("%p tag intersect does not generated qualified tables for join, free all sub SqlObj and quit", pParentSql);
    freeJoinSubqueryObj(pParentSql);

    // set no result command
    pParentSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
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
    ((SJoinSupporter*)psub1->param)->pVgroupTables =  tscVgroupTableInfoClone(pTableMetaInfo1->pVgroupTables);

    SSqlObj* psub2 = pParentSql->pSubs[1];
    ((SJoinSupporter*)psub2->param)->pVgroupTables =  tscVgroupTableInfoClone(pTableMetaInfo2->pVgroupTables);

    pParentSql->subState.numOfSub = 2;
    pParentSql->subState.numOfRemain = pParentSql->subState.numOfSub;

    for (int32_t m = 0; m < pParentSql->subState.numOfSub; ++m) {
      SSqlObj* sub = pParentSql->pSubs[m];
      issueTSCompQuery(sub, sub->param, pParentSql);
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

  // check for the error code firstly
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    // todo retry if other subqueries are not failed yet 
    assert(numOfRows < 0 && numOfRows == taos_errno(pSql));
    tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);

    pParentSql->res.code = numOfRows;
    quitAllSubquery(pParentSql, pSupporter);

    tscQueueAsyncRes(pParentSql);
    return;
  }

  if (numOfRows > 0) {  // write the compressed timestamp to disk file
    fwrite(pRes->data, (size_t)pRes->numOfRows, 1, pSupporter->f);
    fclose(pSupporter->f);
    pSupporter->f = NULL;

    STSBuf* pBuf = tsBufCreateFromFile(pSupporter->path, true);
    if (pBuf == NULL) {  // in error process, close the fd
      tscError("%p invalid ts comp file from vnode, abort subquery, file size:%d", pSql, numOfRows);

      pParentSql->res.code = TAOS_SYSTEM_ERROR(errno);
      tscQueueAsyncRes(pParentSql);

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
      pSupporter->f = fopen(pSupporter->path, "w");
      pRes->row = (int32_t)pRes->numOfRows;

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
    pSupporter->f = fopen(pSupporter->path, "w");
    pRes->row = (int32_t)pRes->numOfRows;

    // set the callback function
    pSql->fp = tscJoinQueryCallback;
    tscProcessSql(pSql);
    return;
  }

  if (atomic_sub_fetch_32(&pParentSql->subState.numOfRemain, 1) > 0) {
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
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(numOfRows == taos_errno(pSql));

    pParentSql->res.code = numOfRows;
    tscError("%p retrieve failed, index:%d, code:%s", pSql, pSupporter->subqueryIndex, tstrerror(numOfRows));

    tscQueueAsyncRes(pParentSql);
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

  assert(pState->numOfRemain > 0);
  if (atomic_sub_fetch_32(&pState->numOfRemain, 1) > 0) {
    tscDebug("%p sub:%p completed, remain:%d, total:%d", pParentSql, tres, pState->numOfRemain, pState->numOfSub);
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
      tscDebug("%p sub:%p index:%d numOfRows:%"PRId64" total:%"PRId64 " (not retrieve)", pParentSql, pParentSql->pSubs[i], i,
               pRes1->numOfRows, pRes1->numOfTotal);
      assert(pRes1->row < pRes1->numOfRows);
    } else {
      if (!stableQuery) {
        pRes1->numOfClauseTotal += pRes1->numOfRows;
      }

      tscDebug("%p sub:%p index:%d numOfRows:%"PRId64" total:%"PRId64, pParentSql, pParentSql->pSubs[i], i,
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
      tscQueueAsyncRes(pSql);
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

    // get the number of subquery that need to retrieve the next vnode.
    if (orderedPrjQuery) {
      for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
        SSqlObj* pSub = pSql->pSubs[i];
        if (pSub != NULL && pSub->res.row >= pSub->res.numOfRows && pSub->res.completed) {
          pSql->subState.numOfRemain++;
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
      tscQueueAsyncRes(pSql);
    }

    return;
  }

  // TODO multi-vnode retrieve for projection query with limitation has bugs, since the global limiation is not handled
  // retrieve data from current vnode.
  tscDebug("%p retrieve data from %d subqueries", pSql, numOfFetch);
  SJoinSupporter* pSupporter = NULL;
  pSql->subState.numOfRemain = numOfFetch;

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
  tscRestoreSQLFuncForSTableQuery(pQueryInfo);
  tscFieldInfoUpdateOffset(pQueryInfo);
}

void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code) {
  SSqlObj* pSql = (SSqlObj*)tres;

  SJoinSupporter* pSupporter = (SJoinSupporter*)param;
  SSqlObj* pParentSql = pSupporter->pObj;

  // There is only one subquery and table for each subquery.
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  assert(pQueryInfo->numOfTables == 1 && pSql->cmd.numOfClause == 1);

  // retrieve actual query results from vnode during the second stage join subquery
  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, code, pParentSql->res.code);
    quitAllSubquery(pParentSql, pSupporter);
    tscQueueAsyncRes(pParentSql);

    return;
  }

  // TODO here retry is required, not directly returns to client
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(taos_errno(pSql) == code);

    tscError("%p abort query, code:%s, global code:%s", pSql, tstrerror(code), tstrerror(pParentSql->res.code));
    pParentSql->res.code = code;

    quitAllSubquery(pParentSql, pSupporter);
    tscQueueAsyncRes(pParentSql);

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


  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // In case of consequence query from other vnode, do not wait for other query response here.
  if (!(pTableMetaInfo->vgroupIndex > 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0))) {
    if (atomic_sub_fetch_32(&pParentSql->subState.numOfRemain, 1) > 0) {
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
      tscQueueAsyncRes(pParentSql);
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
  
  pSql->pSubs[pSql->subState.numOfRemain++] = pNew;
  assert(pSql->subState.numOfRemain <= pSql->subState.numOfSub);
  
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
      tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TID_TAG, &colIndex, &s1, TSDB_COL_TAG);
      size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
  
      tscDebug(
          "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, transfer to tid_tag query to retrieve (tableId, tags), "
          "exprInfo:%" PRIzu ", colList:%" PRIzu ", fieldsInfo:%d, tagIndex:%d, name:%s",
          pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, colIndex.columnIndex, pNewQueryInfo->pTableMetaInfo[0]->name);
    } else {
      SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
      SColumnIndex colIndex = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
      tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TS_COMP, &colIndex, &colSchema, TSDB_COL_NORMAL);

      // set the tags value for ts_comp function
      SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);

      if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        int16_t tagColId = tscGetJoinTagColIdByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->id.uid);
        pExpr->param->i64Key = tagColId;
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
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, pNewQueryInfo->pTableMetaInfo[0]->name);
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
    for (int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
      SSqlObj* pSub = pSql->pSubs[i];
      if ((code = tscProcessSql(pSub)) != TSDB_CODE_SUCCESS) {
        pSql->subState.numOfRemain = i - 1;  // the already sent request will continue and do not go to the error process routine
        break;
      }
    }

    pSql->cmd.command = TSDB_SQL_TABLE_JOIN_RETRIEVE;
  }

  return;

  _error:
  pRes->code = code;
  tscQueueAsyncRes(pSql);
}

static void doCleanupSubqueries(SSqlObj *pSql, int32_t numOfSubs) {
  assert(numOfSubs <= pSql->subState.numOfSub && numOfSubs >= 0);
  
  for(int32_t i = 0; i < numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    assert(pSub != NULL);
    
    SRetrieveSupport* pSupport = pSub->param;
    
    taosTFree(pSupport->localBuffer);
    taosTFree(pSupport);
    
    taos_free_result(pSub);
  }
}

int32_t tscHandleMasterSTableQuery(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  // pRes->code check only serves in launching metric sub-queries
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    pCmd->command = TSDB_SQL_RETRIEVE_LOCALMERGE;  // enable the abort of kill super table function.
    return pRes->code;
  }
  
  tExtMemBuffer **  pMemoryBuf = NULL;
  tOrderDescriptor *pDesc = NULL;
  SColumnModel *    pModel = NULL;
  
  pRes->qhandle = 0x1;  // hack the qhandle check
  
  const uint32_t nBufferSize = (1u << 16);  // 64KB
  
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
  
  int32_t ret = tscLocalReducerEnvCreate(pSql, &pMemoryBuf, &pDesc, &pModel, nBufferSize);
  if (ret != 0) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscQueueAsyncRes(pSql);
    taosTFree(pMemoryBuf);
    return ret;
  }
  
  pSql->pSubs = calloc(pState->numOfSub, POINTER_BYTES);

  tscDebug("%p retrieved query data from %d vnode(s)", pSql, pState->numOfSub);

  if (pSql->pSubs == NULL) {
    taosTFree(pSql->pSubs);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pState->numOfSub);

    tscQueueAsyncRes(pSql);
    return ret;
  }

  pState->numOfRemain = pState->numOfSub;
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
      taosTFree(trs);
      break;
    }
    
    trs->subqueryIndex  = i;
    trs->pParentSql     = pSql;
    trs->pFinalColModel = pModel;
    
    SSqlObj *pNew = tscCreateSTableSubquery(pSql, trs, NULL);
    if (pNew == NULL) {
      tscError("%p failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      taosTFree(trs->localBuffer);
      taosTFree(trs);
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
    
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pState->numOfSub);
    doCleanupSubqueries(pSql, i);
    return pRes->code;   // free all allocated resource
  }
  
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pState->numOfSub);
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
//  int32_t  index = trsupport->subqueryIndex;
//  SSqlObj *pParentSql = trsupport->pParentSql;

//  assert(pSql == pParentSql->pSubs[index]);
  taosTFree(trsupport->localBuffer);
  taosTFree(trsupport);
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
static int32_t tscReissueSubquery(SRetrieveSupport *trsupport, SSqlObj *pSql, int32_t code) {
  SSqlObj *pParentSql = trsupport->pParentSql;
  int32_t  subqueryIndex = trsupport->subqueryIndex;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SCMVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[0];

  tExtMemBufferClear(trsupport->pExtMemBuffer[subqueryIndex]);

  // clear local saved number of results
  trsupport->localBuffer->num = 0;
  tscError("%p sub:%p retrieve/query failed, code:%s, orderOfSub:%d, retry:%d", trsupport->pParentSql, pSql,
           tstrerror(code), subqueryIndex, trsupport->numOfRetry);

  SSqlObj *pNew = tscCreateSTableSubquery(trsupport->pParentSql, trsupport, pSql);
  if (pNew == NULL) {
    tscError("%p sub:%p failed to create new subquery due to error:%s, abort retry, vgId:%d, orderOfSub:%d",
             trsupport->pParentSql, pSql, tstrerror(terrno), pVgroup->vgId, trsupport->subqueryIndex);

    pParentSql->res.code = terrno;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;

    return pParentSql->res.code;
  }

  int32_t ret = tscProcessSql(pNew);

  // if failed to process sql, let following code handle the pSql
  if (ret == TSDB_CODE_SUCCESS) {
    taos_free_result(pSql);
    return ret;
  } else {
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
  assert(pState->numOfRemain <= pState->numOfSub && pState->numOfRemain >= 0);

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
      if (tscReissueSubquery(trsupport, pSql, numOfRows) == TSDB_CODE_SUCCESS) {
        return;
      }
    } else {  // reach the maximum retry count, abort
      atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, numOfRows);
      tscError("%p sub:%p retrieve failed,code:%s,orderOfSub:%d failed.no more retry,set global code:%s", pParentSql, pSql,
               tstrerror(numOfRows), subqueryIndex, tstrerror(pParentSql->res.code));
    }
  }

  int32_t remain = -1;
  if ((remain = atomic_sub_fetch_32(&pState->numOfRemain, 1)) > 0) {
    tscDebug("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pParentSql, pSql, trsupport->subqueryIndex,
        pState->numOfSub - remain);

    tscFreeRetrieveSup(pSql);
    return;
  }
  
  // all subqueries are failed
  tscError("%p retrieve from %d vnode(s) completed,code:%s.FAILED.", pParentSql, pState->numOfSub,
      tstrerror(pParentSql->res.code));

  // release allocated resource
  tscLocalReducerEnvDestroy(trsupport->pExtMemBuffer, trsupport->pOrderDescriptor, trsupport->pFinalColModel,
                            pState->numOfSub);
  
  tscFreeRetrieveSup(pSql);

  // in case of second stage join subquery, invoke its callback function instead of regular QueueAsyncRes
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, 0);

  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
    (*pParentSql->fp)(pParentSql->param, pParentSql, pParentSql->res.code);
  } else {  // regular super table query
    if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
      tscQueueAsyncRes(pParentSql);
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
  
  int32_t remain = -1;
  if ((remain = atomic_sub_fetch_32(&pParentSql->subState.numOfRemain, 1)) > 0) {
    tscDebug("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pParentSql, pSql, trsupport->subqueryIndex,
        pState->numOfSub - remain);

    tscFreeRetrieveSup(pSql);
    return;
  }
  
  // all sub-queries are returned, start to local merge process
  pDesc->pColumnModel->capacity = trsupport->pExtMemBuffer[idx]->numOfElemsPerPage;
  
  tscDebug("%p retrieve from %d vnodes completed.final NumOfRows:%" PRId64 ",start to build loser tree", pParentSql,
           pState->numOfSub, pState->numOfRetrievedRows);
  
  SQueryInfo *pPQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, 0);
  tscClearInterpInfo(pPQueryInfo);
  
  tscCreateLocalReducer(trsupport->pExtMemBuffer, pState->numOfSub, pDesc, trsupport->pFinalColModel, pParentSql);
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
    tscQueueAsyncRes(pParentSql);
  }
}

static void tscRetrieveFromDnodeCallBack(void *param, TAOS_RES *tres, int numOfRows) {
  SSqlObj *pSql = (SSqlObj *)tres;
  assert(pSql != NULL);

  // this query has been freed already
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;
  if (pSql->param == NULL || param == NULL) {
    tscDebug("%p already freed in dnodecallback", pSql);
    assert(pSql->res.code == TSDB_CODE_TSC_QUERY_CANCELLED);
    return;
  }

  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;
  int32_t           idx   = trsupport->subqueryIndex;
  SSqlObj *         pParentSql = trsupport->pParentSql;

  SSubqueryState* pState = &pParentSql->subState;
  assert(pState->numOfRemain <= pState->numOfSub && pState->numOfRemain >= 0);
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SCMVgroupInfo  *pVgroup = &pTableMetaInfo->vgroupList->vgroups[0];

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

      if (tscReissueSubquery(trsupport, pSql, numOfRows) == TSDB_CODE_SUCCESS) {
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
    
    tscDebug("%p sub:%p retrieve numOfRows:%" PRId64 " totalNumOfRows:%" PRIu64 " from ep:%s, orderOfSub:%d", pParentSql, pSql,
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
                               (int32_t)pRes->numOfRows, pQueryInfo->groupbyExpr.orderType);
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
    assert(pQueryInfo->numOfTables == 1 && pNew->cmd.numOfClause == 1 && trsupport->subqueryIndex < pSql->subState.numOfSub);
    
    // launch subquery for each vnode, so the subquery index equals to the vgroupIndex.
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, table_index);
    pTableMetaInfo->vgroupIndex = trsupport->subqueryIndex;
    
    pSql->pSubs[trsupport->subqueryIndex] = pNew;
  }
  
  return pNew;
}

void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *) param;
  
  SSqlObj*  pParentSql = trsupport->pParentSql;
  SSqlObj*  pSql = (SSqlObj *) tres;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  assert(pSql->cmd.numOfClause == 1 && pQueryInfo->numOfTables == 1);
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SCMVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[trsupport->subqueryIndex];

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
      if (tscReissueSubquery(trsupport, pSql, code) == TSDB_CODE_SUCCESS) {
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

static void multiVnodeInsertFinalize(void* param, TAOS_RES* tres, int numOfRows) {
  SInsertSupporter *pSupporter = (SInsertSupporter *)param;
  SSqlObj* pParentObj = pSupporter->pSql;

  // record the total inserted rows
  if (numOfRows > 0) {
    pParentObj->res.numOfRows += numOfRows;
  }

  if (taos_errno(tres) != TSDB_CODE_SUCCESS) {
    SSqlObj* pSql = (SSqlObj*) tres;
    assert(pSql != NULL && pSql->res.code == numOfRows);
    
    pParentObj->res.code = pSql->res.code;
  }

  taosTFree(pSupporter);

  if (atomic_sub_fetch_32(&pParentObj->subState.numOfRemain, 1) > 0) {
    return;
  }
  
  tscDebug("%p Async insertion completed, total inserted:%" PRId64, pParentObj, pParentObj->res.numOfRows);

  // restore user defined fp
  pParentObj->fp = pParentObj->fetchFp;

  // todo remove this parameter in async callback function definition.
  // all data has been sent to vnode, call user function
  int32_t v = (pParentObj->res.code != TSDB_CODE_SUCCESS) ? pParentObj->res.code : (int32_t)pParentObj->res.numOfRows;
  (*pParentObj->fp)(pParentObj->param, pParentObj, v);
}

/**
 * it is a subquery, so after parse the sql string, copy the submit block to payload of itself
 * @param pSql
 * @return
 */
int32_t tscHandleInsertRetry(SSqlObj* pSql) {
  assert(pSql != NULL && pSql->param != NULL);
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SInsertSupporter* pSupporter = (SInsertSupporter*) pSql->param;
  assert(pSupporter->index < pSupporter->pSql->subState.numOfSub);

  STableDataBlocks* pTableDataBlock = taosArrayGetP(pCmd->pDataBlocks, pSupporter->index);
  int32_t code = tscCopyDataBlockToPayload(pSql, pTableDataBlock);

  if ((pRes->code = code)!= TSDB_CODE_SUCCESS) {
    tscQueueAsyncRes(pSql);
    return code;  // here the pSql may have been released already.
  }

  return tscProcessSql(pSql);
}

int32_t tscHandleMultivnodeInsert(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  pSql->subState.numOfSub = (uint16_t)taosArrayGetSize(pCmd->pDataBlocks);
  assert(pSql->subState.numOfSub > 0);

  pRes->code = TSDB_CODE_SUCCESS;

  // the number of already initialized subqueries
  int32_t numOfSub = 0;

  pSql->subState.numOfRemain = pSql->subState.numOfSub;
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

  int32_t totalSize = tscGetResRowLength(pQueryInfo->exprList);

  assert(numOfRes * totalSize > 0);
  char* tmp = realloc(pRes->pRsp, numOfRes * totalSize);
  if (tmp == NULL) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return;
  } else {
    pRes->pRsp = tmp;
  }

  pRes->data = pRes->pRsp;

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
}

void tscBuildResFromSubqueries(SSqlObj *pSql) {
  SSqlRes* pRes = &pSql->res;

  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscQueueAsyncRes(pSql);
    return;
  }

  if (pRes->tsrow == NULL) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);

    size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
    pRes->numOfCols =  (int32_t)numOfExprs;

    pRes->tsrow  = calloc(numOfExprs, POINTER_BYTES);
    pRes->buffer = calloc(numOfExprs, POINTER_BYTES);
    pRes->length = calloc(numOfExprs, sizeof(int32_t));

    if (pRes->tsrow == NULL || pRes->buffer == NULL || pRes->length == NULL) {
      pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      tscQueueAsyncRes(pSql);
      return;
    }

    tscRestoreSQLFuncForSTableQuery(pQueryInfo);
  }

  while (1) {
    assert (pRes->row >= pRes->numOfRows);

    doBuildResFromSubqueries(pSql);
    tsem_post(&pSql->rspSem);
    return;
  }
}

static void transferNcharData(SSqlObj *pSql, int32_t columnIndex, TAOS_FIELD *pField) {
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

static char *getArithemicInputSrc(void *param, const char *name, int32_t colId) {
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

TAOS_ROW doSetResultRowData(SSqlObj *pSql, bool finalResult) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  assert(pRes->row >= 0 && pRes->row <= pRes->numOfRows);
  if (pRes->row >= pRes->numOfRows) {  // all the results has returned to invoker
    taosTFree(pRes->tsrow);
    return pRes->tsrow;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  size_t size = tscNumOfFields(pQueryInfo);
  for (int i = 0; i < size; ++i) {
    SInternalField* pSup = TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, i);
    if (pSup->pSqlExpr != NULL) {
      tscGetResultColumnChr(pRes, &pQueryInfo->fieldsInfo, i);
    }

    // primary key column cannot be null in interval query, no need to check
    if (i == 0 && pQueryInfo->interval.interval > 0) {
      continue;
    }

    TAOS_FIELD *pField = TARRAY_GET_ELEM(pQueryInfo->fieldsInfo.internalField, i);
    if (pRes->tsrow[i] != NULL && pField->type == TSDB_DATA_TYPE_NCHAR) {
      transferNcharData(pSql, i, pField);
    }

    // calculate the result from several other columns
    if (pSup->pArithExprInfo != NULL) {
      if (pRes->pArithSup == NULL) {
        pRes->pArithSup = (SArithmeticSupport*)calloc(1, sizeof(SArithmeticSupport));
      }

      pRes->pArithSup->offset     = 0;
      pRes->pArithSup->pArithExpr = pSup->pArithExprInfo;
      pRes->pArithSup->numOfCols  = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
      pRes->pArithSup->exprList   = pQueryInfo->exprList;
      pRes->pArithSup->data       = calloc(pRes->pArithSup->numOfCols, POINTER_BYTES);

      if (pRes->buffer[i] == NULL) {
        TAOS_FIELD* field = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
        pRes->buffer[i] = malloc(field->bytes);
      }

      for(int32_t k = 0; k < pRes->pArithSup->numOfCols; ++k) {
        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, k);
        pRes->pArithSup->data[k] = (pRes->data + pRes->numOfRows* pExpr->offset) + pRes->row*pExpr->resBytes;
      }

      tExprTreeCalcTraverse(pRes->pArithSup->pArithExpr->pExpr, 1, pRes->buffer[i], pRes->pArithSup,
          TSDB_ORDER_ASC, getArithemicInputSrc);
      pRes->tsrow[i] = (unsigned char*)pRes->buffer[i];
    }
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
