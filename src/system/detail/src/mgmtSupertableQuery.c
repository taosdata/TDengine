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

#define _DEFAULT_SOURCE
#include "os.h"

#include "mgmt.h"
#include "mgmtUtil.h"
#include "textbuffer.h"
#include "tschemautil.h"
#include "tsqlfunction.h"
#include "vnodeTagMgmt.h"

typedef struct SJoinSupporter {
  void**  val;
  void**  pTabObjs;
  int32_t size;

  int16_t type;
  int16_t colIndex;

  void**  qualMeterObj;
  int32_t qualSize;
} SJoinSupporter;

typedef struct SMeterNameFilterSupporter {
  SPatternCompareInfo info;
  char*               pattern;
} SMeterNameFilterSupporter;

static void tansformQueryResult(tQueryResultset* pRes);

static int32_t tabObjVGIDComparator(const void* pLeft, const void* pRight) {
  STabObj* p1 = *(STabObj**)pLeft;
  STabObj* p2 = *(STabObj**)pRight;

  int32_t ret = p1->gid.vgId - p2->gid.vgId;
  if (ret == 0) {
    return ret;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

// monotonic inc in memory address
static int32_t tabObjPointerComparator(const void* pLeft, const void* pRight) {
  int64_t ret = (*(STabObj**)(pLeft))->uid - (*(STabObj**)(pRight))->uid;
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t tabObjResultComparator(const void* p1, const void* p2, void* param) {
  tOrderDescriptor* pOrderDesc = (tOrderDescriptor*)param;

  STabObj* pNode1 = (STabObj*)p1;
  STabObj* pNode2 = (STabObj*)p2;

  for (int32_t i = 0; i < pOrderDesc->orderIdx.numOfCols; ++i) {
    int32_t colIdx = pOrderDesc->orderIdx.pData[i];

    char* f1 = NULL;
    char* f2 = NULL;

    SSchema schema = {0};

    if (colIdx == -1) {
      f1 = pNode1->meterId;
      f2 = pNode2->meterId;
      schema.type = TSDB_DATA_TYPE_BINARY;
      schema.bytes = TSDB_METER_ID_LEN;
    } else {
      f1 = mgmtMeterGetTag(pNode1, colIdx, NULL);
      f2 = mgmtMeterGetTag(pNode2, colIdx, &schema);
      
      SSchema* pSchema = getColumnModelSchema(pOrderDesc->pColumnModel, colIdx);
      assert(schema.type == pSchema->type);
    }

    int32_t ret = doCompare(f1, f2, schema.type, schema.bytes);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}

/**
 * update the tag order index according to the tags column index. The tags column index needs to be checked one-by-one,
 * since the normal columns may be passed to server for handling the group by on status column.
 *
 * @param pMetricMetaMsg
 * @param tableIndex
 * @param pOrderIndexInfo
 * @param numOfTags
 */
static void mgmtUpdateOrderTagColIndex(SMetricMetaMsg* pMetricMetaMsg, int32_t tableIndex, SColumnOrderInfo* pOrderIndexInfo,
     int32_t numOfTags) {
  SMetricMetaElemMsg* pElem = (SMetricMetaElemMsg*)((char*)pMetricMetaMsg + pMetricMetaMsg->metaElem[tableIndex]);
  SColIndexEx* groupColumnList = (SColIndexEx*)((char*)pMetricMetaMsg + pElem->groupbyTagColumnList);
  
  int32_t numOfGroupbyTags = 0;
  for (int32_t i = 0; i < pElem->numOfGroupCols; ++i) {
    if (groupColumnList[i].flag == TSDB_COL_TAG) {  // ignore this column if it is not a tag column.
      pOrderIndexInfo->pData[numOfGroupbyTags++] = groupColumnList[i].colIdx;
      
      assert(groupColumnList[i].colIdx < numOfTags);
    }
  }
  
  pOrderIndexInfo->numOfCols = numOfGroupbyTags;
}

// todo merge sort function with losertree used
void mgmtReorganizeMetersInMetricMeta(SMetricMetaMsg* pMetricMetaMsg, int32_t tableIndex, tQueryResultset* pRes) {
  if (pRes->num <= 0) {  // no result, no need to pagination
    return;
  }

  SMetricMetaElemMsg* pElem = (SMetricMetaElemMsg*)((char*)pMetricMetaMsg + pMetricMetaMsg->metaElem[tableIndex]);

  STabObj* pMetric = mgmtGetMeter(pElem->meterId);
  SSchema* pTagSchema = (SSchema*)(pMetric->schema + pMetric->numOfColumns * sizeof(SSchema));

  /*
   * To apply the group limitation and group offset, we should sort the result
   * list according to the order condition
   */
  tOrderDescriptor* descriptor =
      (tOrderDescriptor*)calloc(1, sizeof(tOrderDescriptor) + sizeof(int32_t) * pElem->numOfGroupCols);
  descriptor->pColumnModel = createColumnModel(pTagSchema, pMetric->numOfTags, 1);
  descriptor->orderIdx.numOfCols = pElem->numOfGroupCols;

  int32_t* startPos = NULL;
  int32_t  numOfSubset = 1;
  
  mgmtUpdateOrderTagColIndex(pMetricMetaMsg, tableIndex, &descriptor->orderIdx, pMetric->numOfTags);
  if (descriptor->orderIdx.numOfCols > 0) {
    tQSortEx(pRes->pRes, POINTER_BYTES, 0, pRes->num - 1, descriptor, tabObjResultComparator);
    startPos = calculateSubGroup(pRes->pRes, pRes->num, &numOfSubset, descriptor, tabObjResultComparator);
  } else {
    startPos = malloc(2 * sizeof(int32_t));

    startPos[0] = 0;
    startPos[1] = (int32_t)pRes->num;
  }

  /*
   * sort the result according to vgid to ensure meters with the same vgid is
   * continuous in the result list
   */
  qsort(pRes->pRes, (size_t)pRes->num, POINTER_BYTES, tabObjVGIDComparator);

  free(descriptor->pColumnModel);
  free(descriptor);
  free(startPos);
}

static void mgmtRetrieveByMeterName(tQueryResultset* pRes, char* str, STabObj* pMetric) {
  const char* sep = ",";
  char*       pToken = NULL;

  int32_t s = 4;  // initial size

  pRes->pRes = malloc(sizeof(char*) * s);
  pRes->num = 0;

  for (pToken = strsep(&str, sep); pToken != NULL; pToken = strsep(&str, sep)) {
    STabObj* pMeterObj = mgmtGetMeter(pToken);
    if (pMeterObj == NULL) {
      mWarn("metric:%s error in metric query expression, invalid meter id:%s", pMetric->meterId, pToken);
      continue;
    }

    if (pRes->num >= s) {
      s += (s >> 1);  // increase 50% size
      pRes->pRes = realloc(pRes->pRes, sizeof(char*) * s);
    }

    /* not a table created from metric, ignore */
    if (pMeterObj->meterType != TSDB_METER_MTABLE) {
      continue;
    }

    /*
     * queried meter not belongs to this metric, ignore, metric does not have
     * uid, so compare according to meterid
     */
    STabObj* parentMetric = mgmtGetMeter(pMeterObj->pTagData);
    if (strncasecmp(parentMetric->meterId, pMetric->meterId, TSDB_METER_ID_LEN) != 0 ||
        (parentMetric->uid != pMetric->uid)) {
      continue;
    }

    pRes->pRes[pRes->num++] = pMeterObj;
  }
}

static bool mgmtTablenameFilterCallback(tSkipListNode* pNode, void* param) {
  SMeterNameFilterSupporter* pSupporter = (SMeterNameFilterSupporter*)param;

  char name[TSDB_METER_ID_LEN] = {0};

  // pattern compare for meter name
  STabObj* pMeterObj = (STabObj*)pNode->pData;
  extractTableName(pMeterObj->meterId, name);

  return patternMatch(pSupporter->pattern, name, TSDB_METER_ID_LEN, &pSupporter->info) == TSDB_PATTERN_MATCH;
}

static void mgmtRetrieveFromLikeOptr(tQueryResultset* pRes, const char* str, STabObj* pMetric) {
  SPatternCompareInfo       info = PATTERN_COMPARE_INFO_INITIALIZER;
  SMeterNameFilterSupporter supporter = {info, (char*) str};

  pRes->num =
      tSkipListIterateList(pMetric->pSkipList, (tSkipListNode***)&pRes->pRes, mgmtTablenameFilterCallback, &supporter);
}

static void mgmtFilterByTableNameCond(tQueryResultset* pRes, char* condStr, int32_t len, STabObj* pMetric) {
  pRes->num = 0;
  if (len <= 0) {
    return;
  }

  char* str = calloc(1, (size_t)len + 1);
  memcpy(str, condStr, len);

  if (strncasecmp(condStr, QUERY_COND_REL_PREFIX_IN, QUERY_COND_REL_PREFIX_IN_LEN) == 0) {  // handle in expression
    mgmtRetrieveByMeterName(pRes, str + QUERY_COND_REL_PREFIX_IN_LEN, pMetric);
  } else {  // handle like expression
    assert(strncasecmp(str, QUERY_COND_REL_PREFIX_LIKE, QUERY_COND_REL_PREFIX_LIKE_LEN) == 0);
    mgmtRetrieveFromLikeOptr(pRes, str + QUERY_COND_REL_PREFIX_LIKE_LEN, pMetric);

    tansformQueryResult(pRes);
  }

  free(str);
}

UNUSED_FUNC static bool mgmtJoinFilterCallback(tSkipListNode* pNode, void* param) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSchema s = {0};
  char*   v = mgmtMeterGetTag((STabObj*)pNode->pData, pSupporter->colIndex, &s);

  for (int32_t i = 0; i < pSupporter->size; ++i) {
    int32_t ret = doCompare(v, pSupporter->val[i], pSupporter->type, s.bytes);
    if (ret == 0) {
      pSupporter->qualMeterObj[pSupporter->qualSize++] = pSupporter->pTabObjs[i];

      /*
       * Once a value is qualified according to the join condition, it is remove from the
       * candidate list, as well as its corresponding meter object.
       *
       * last one does not need to move forward
       */
      if (i < pSupporter->size - 1) {
        memmove(pSupporter->val[i], pSupporter->val[i + 1], pSupporter->size - (i + 1));
      }

      pSupporter->size -= 1;

      return true;
    }
  }

  return false;
}

static void orderResult(SMetricMetaMsg* pMetricMetaMsg, tQueryResultset* pRes, int16_t colIndex, int32_t tableIndex) {
  SMetricMetaElemMsg* pElem = (SMetricMetaElemMsg*)((char*)pMetricMetaMsg + pMetricMetaMsg->metaElem[tableIndex]);

  tOrderDescriptor* descriptor =
      (tOrderDescriptor*)calloc(1, sizeof(tOrderDescriptor) + sizeof(int32_t) * 1);  // only one column for join

  STabObj* pMetric = mgmtGetMeter(pElem->meterId);
  SSchema* pTagSchema = (SSchema*)(pMetric->schema + pMetric->numOfColumns * sizeof(SSchema));

  descriptor->pColumnModel = createColumnModel(pTagSchema, pMetric->numOfTags, 1);

  descriptor->orderIdx.pData[0] = colIndex;
  descriptor->orderIdx.numOfCols = 1;

  // sort results list
  tQSortEx(pRes->pRes, POINTER_BYTES, 0, pRes->num - 1, descriptor, tabObjResultComparator);

  free(descriptor->pColumnModel);
  free(descriptor);
}

// check for duplicate join tags
static int32_t mgmtCheckForDuplicateTagValue(tQueryResultset* pRes, int32_t index, int32_t tagCol) {
  SSchema s = {0};

  for (int32_t k = 1; k < pRes[index].num; ++k) {
    STabObj* pObj1 = pRes[index].pRes[k - 1];
    STabObj* pObj2 = pRes[index].pRes[k];

    char* val1 = mgmtMeterGetTag(pObj1, tagCol, &s);
    char* val2 = mgmtMeterGetTag(pObj2, tagCol, NULL);

    if (doCompare(val1, val2, s.type, s.bytes) == 0) {
      return TSDB_CODE_DUPLICATE_TAGS;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtDoJoin(SMetricMetaMsg* pMetricMetaMsg, tQueryResultset* pRes) {
  if (pMetricMetaMsg->numOfMeters == 1) {
    return TSDB_CODE_SUCCESS;
  }

  bool allEmpty = false;
  for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
    if (pRes[i].num == 0) {  // all results are empty if one of them is empty
      allEmpty = true;
      break;
    }
  }

  if (allEmpty) {
    for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
      pRes[i].num = 0;
      tfree(pRes[i].pRes);
    }

    return TSDB_CODE_SUCCESS;
  }

  char* cond = (char*)pMetricMetaMsg + pMetricMetaMsg->join;

  char left[TSDB_METER_ID_LEN + 1] = {0};
  strcpy(left, cond);
  int16_t leftTagColIndex = *(int16_t*)(cond + TSDB_METER_ID_LEN);

  char right[TSDB_METER_ID_LEN + 1] = {0};
  strcpy(right, cond + TSDB_METER_ID_LEN + sizeof(int16_t));
  int16_t rightTagColIndex = *(int16_t*)(cond + TSDB_METER_ID_LEN * 2 + sizeof(int16_t));

  STabObj* pLeftMetric = mgmtGetMeter(left);
  STabObj* pRightMetric = mgmtGetMeter(right);

  // decide the pRes belongs to
  int32_t leftIndex = 0;
  int32_t rightIndex = 0;

  for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
    STabObj* pObj = (STabObj*)pRes[i].pRes[0];
    STabObj* pMetric1 = mgmtGetMeter(pObj->pTagData);
    if (pMetric1 == pLeftMetric) {
      leftIndex = i;
    } else if (pMetric1 == pRightMetric) {
      rightIndex = i;
    }
  }

  orderResult(pMetricMetaMsg, &pRes[leftIndex], leftTagColIndex, leftIndex);
  orderResult(pMetricMetaMsg, &pRes[rightIndex], rightTagColIndex, rightIndex);

  int32_t i = 0;
  int32_t j = 0;

  SSchema s = {0};
  int32_t res = 0;

  // check for duplicated tag values
  int32_t ret1 = mgmtCheckForDuplicateTagValue(pRes, leftIndex, leftTagColIndex);
  int32_t ret2 = mgmtCheckForDuplicateTagValue(pRes, rightIndex, rightTagColIndex);
  if (ret1 != TSDB_CODE_SUCCESS || ret2 != TSDB_CODE_SUCCESS) {
    return ret1;
  }

  while (i < pRes[leftIndex].num && j < pRes[rightIndex].num) {
    STabObj* pLeftObj = pRes[leftIndex].pRes[i];
    STabObj* pRightObj = pRes[rightIndex].pRes[j];

    char* v1 = mgmtMeterGetTag(pLeftObj, leftTagColIndex, &s);
    char* v2 = mgmtMeterGetTag(pRightObj, rightTagColIndex, NULL);

    int32_t ret = doCompare(v1, v2, s.type, s.bytes);
    if (ret == 0) {  // qualified
      pRes[leftIndex].pRes[res] = pRes[leftIndex].pRes[i++];
      pRes[rightIndex].pRes[res] = pRes[rightIndex].pRes[j++];

      res++;
    } else if (ret < 0) {
      i++;
    } else {
      j++;
    }
  }

  pRes[leftIndex].num = res;
  pRes[rightIndex].num = res;

  return TSDB_CODE_SUCCESS;
}

/**
 * convert the result pointer to STabObj instead of tSkipListNode
 * @param pRes
 */
static void tansformQueryResult(tQueryResultset* pRes) {
  if (pRes == NULL || pRes->num == 0) {
    return;
  }

  for (int32_t i = 0; i < pRes->num; ++i) {
    pRes->pRes[i] = ((tSkipListNode*)(pRes->pRes[i]))->pData;
  }
}

static tQueryResultset* doNestedLoopIntersect(tQueryResultset* pRes1, tQueryResultset* pRes2) {
  int32_t num = 0;
  void**  pResult = pRes1->pRes;

  for (int32_t i = 0; i < pRes1->num; ++i) {
    for (int32_t j = 0; j < pRes2->num; ++j) {
      if (pRes1->pRes[i] == pRes2->pRes[j]) {
        pResult[num++] = pRes1->pRes[i];
        break;
      }
    }
  }

  tQueryResultClean(pRes2);

  memset(pRes1->pRes + num, 0, sizeof(void*) * (pRes1->num - num));
  pRes1->num = num;

  return pRes1;
}

static tQueryResultset* doSortIntersect(tQueryResultset* pRes1, tQueryResultset* pRes2) {
  size_t sizePtr = sizeof(void *);
  
  qsort(pRes1->pRes, pRes1->num, sizePtr, tabObjPointerComparator);
  qsort(pRes2->pRes, pRes2->num, sizePtr, tabObjPointerComparator);
  
  int32_t i = 0;
  int32_t j = 0;

  int32_t num = 0;
  while (i < pRes1->num && j < pRes2->num) {
    if (pRes1->pRes[i] == pRes2->pRes[j]) {
      j++;
      pRes1->pRes[num++] = pRes1->pRes[i++];
    } else if (pRes1->pRes[i] < pRes2->pRes[j]) {
      i++;
    } else {
      j++;
    }
  }

  tQueryResultClean(pRes2);

  memset(pRes1->pRes + num, 0, sizeof(void*) * (pRes1->num - num));
  pRes1->num = num;
  return pRes1;
}

static void queryResultIntersect(tQueryResultset* pFinalRes, tQueryResultset* pRes) {
  const int32_t NUM_OF_RES_THRESHOLD = 20;

  // for small result, use nested loop join
  if (pFinalRes->num <= NUM_OF_RES_THRESHOLD && pRes->num <= NUM_OF_RES_THRESHOLD) {
    doNestedLoopIntersect(pFinalRes, pRes);
  } else {  // for larger result, sort merge is employed
    doSortIntersect(pFinalRes, pRes);
  }
}

static void queryResultUnion(tQueryResultset* pFinalRes, tQueryResultset* pRes) {
  if (pRes->num == 0) {
    tQueryResultClean(pRes);
    return;
  }

  int32_t total = pFinalRes->num + pRes->num;
  void*   tmp = realloc(pFinalRes->pRes, total * POINTER_BYTES);
  if (tmp == NULL) {
    return;
  }
  pFinalRes->pRes = tmp;

  memcpy(&pFinalRes->pRes[pFinalRes->num], pRes->pRes, POINTER_BYTES * pRes->num);
  qsort(pFinalRes->pRes, total, POINTER_BYTES, tabObjPointerComparator);

  int32_t num = 1;
  for (int32_t i = 1; i < total; ++i) {
    if (pFinalRes->pRes[i] != pFinalRes->pRes[i - 1]) {
      pFinalRes->pRes[num++] = pFinalRes->pRes[i];
    }
  }

  if (num < total) {
    memset(&pFinalRes->pRes[num], 0, POINTER_BYTES * (total - num));
  }

  pFinalRes->num = num;

  tQueryResultClean(pRes);
}

static int32_t compareIntVal(const void* pLeft, const void* pRight) {
  DEFAULT_COMP(GET_INT64_VAL(pLeft), GET_INT64_VAL(pRight));
}

static int32_t compareIntDoubleVal(const void* pLeft, const void* pRight) {
  DEFAULT_COMP(GET_INT64_VAL(pLeft), GET_DOUBLE_VAL(pRight));
}

static int32_t compareDoubleVal(const void* pLeft, const void* pRight) {
  DEFAULT_COMP(GET_DOUBLE_VAL(pLeft), GET_DOUBLE_VAL(pRight));
}

static int32_t compareDoubleIntVal(const void* pLeft, const void* pRight) {
  double ret = (*(double*)pLeft) - (*(int64_t*)pRight);
  if (fabs(ret) < DBL_EPSILON) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareStrVal(const void* pLeft, const void* pRight) {
  int32_t ret = strcmp(pLeft, pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareWStrVal(const void* pLeft, const void* pRight) {
  int32_t ret = wcscmp(pLeft, pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};

  const char* pattern = pRight;
  const char* str = pLeft;

  int32_t ret = patternMatch(pattern, str, strlen(str), &pInfo);

  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

static int32_t compareWStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};

  const wchar_t* pattern = pRight;
  const wchar_t* str = pLeft;

  int32_t ret = WCSPatternMatch(pattern, str, twcslen(str), &pInfo);

  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

static __compar_fn_t getFilterComparator(int32_t type, int32_t filterType, int32_t optr) {
  __compar_fn_t comparator = NULL;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_BOOL: {
      if (filterType >= TSDB_DATA_TYPE_BOOL && filterType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareIntVal;
      } else if (filterType >= TSDB_DATA_TYPE_FLOAT && filterType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareIntDoubleVal;
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      if (filterType >= TSDB_DATA_TYPE_BOOL && filterType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareDoubleIntVal;
      } else if (filterType >= TSDB_DATA_TYPE_FLOAT && filterType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareDoubleVal;
      }
      break;
    }

    case TSDB_DATA_TYPE_BINARY: {
      assert(filterType == TSDB_DATA_TYPE_BINARY);

      if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        comparator = compareStrPatternComp;
      } else { /* normal relational comparator */
        comparator = compareStrVal;
      }

      break;
    }

    case TSDB_DATA_TYPE_NCHAR: {
      assert(filterType == TSDB_DATA_TYPE_NCHAR);

      if (optr == TSDB_RELATION_LIKE) {
        comparator = compareWStrPatternComp;
      } else {
        comparator = compareWStrVal;
      }

      break;
    }
    default:
      comparator = compareIntVal;
      break;
  }

  return comparator;
}

static void getTagColumnInfo(SSyntaxTreeFilterSupporter* pSupporter, SSchema* pSchema, int32_t* index,
                             int32_t* offset) {
  *index = 0;
  *offset = 0;

  // filter on table name(TBNAME)
  if (strcasecmp(pSchema->name, TSQL_TBNAME_L) == 0) {
    *index = TSDB_TBNAME_COLUMN_INDEX;
    *offset = TSDB_TBNAME_COLUMN_INDEX;
    return;
  }

  while ((*index) < pSupporter->numOfTags) {
    if (pSupporter->pTagSchema[*index].bytes == pSchema->bytes &&
        pSupporter->pTagSchema[*index].type == pSchema->type &&
        strcmp(pSupporter->pTagSchema[*index].name, pSchema->name) == 0) {
      break;
    } else {
      (*offset) += pSupporter->pTagSchema[(*index)++].bytes;
    }
  }
}

void filterPrepare(void* expr, void* param) {
  tSQLBinaryExpr *pExpr = (tSQLBinaryExpr*) expr;
  if (pExpr->info != NULL) {
    return;
  }

  int32_t i = 0, offset = 0;
  pExpr->info = calloc(1, sizeof(tQueryInfo));

  tQueryInfo*                 pInfo = pExpr->info;
  SSyntaxTreeFilterSupporter* pSupporter = (SSyntaxTreeFilterSupporter*)param;

  tVariant* pCond = pExpr->pRight->pVal;
  SSchema*  pSchema = pExpr->pLeft->pSchema;

  getTagColumnInfo(pSupporter, pSchema, &i, &offset);
  assert((i >= 0 && i < TSDB_MAX_TAGS) || (i == TSDB_TBNAME_COLUMN_INDEX));
  assert((offset >= 0 && offset < TSDB_MAX_TAGS_LEN) || (offset == TSDB_TBNAME_COLUMN_INDEX));

  pInfo->sch = *pSchema;
  pInfo->colIdx = i;
  pInfo->optr = pExpr->nSQLBinaryOptr;
  pInfo->offset = offset;
  pInfo->compare = getFilterComparator(pSchema->type, pCond->nType, pInfo->optr);

  tVariantAssign(&pInfo->q, pCond);
  tVariantTypeSetType(&pInfo->q, pInfo->sch.type);
}

void tSQLListTraverseDestroyInfo(void* param) {
  if (param == NULL) {
    return;
  }

  tQueryInfo* pInfo = (tQueryInfo*)param;
  tVariantDestroy(&(pInfo->q));
  free(param);
}

static int32_t mgmtFilterMeterByIndex(STabObj* pMetric, tQueryResultset* pRes, char* pCond, int32_t condLen) {
  SSchema* pTagSchema = (SSchema*)(pMetric->schema + pMetric->numOfColumns * sizeof(SSchema));

  tSQLBinaryExpr* pExpr = NULL;
  tSQLBinaryExprFromString(&pExpr, pTagSchema, pMetric->numOfTags, pCond, condLen);

  // failed to build expression, no result, return immediately
  if (pExpr == NULL) {
    mError("metric:%s, no result returned, error in super table query expression:%s", pMetric->meterId, pCond);

    return TSDB_CODE_OPS_NOT_SUPPORT;
  } else {  // query according to the binary expression
    SSyntaxTreeFilterSupporter s = {.pTagSchema = pTagSchema, .numOfTags = pMetric->numOfTags};
    SBinaryFilterSupp          supp = {.fp = (__result_filter_fn_t)tSkipListNodeFilterCallback,
                                       .setupInfoFn = (__do_filter_suppl_fn_t)filterPrepare,
                                       .pExtInfo = &s};

    tSQLBinaryExprTraverse(pExpr, pMetric->pSkipList, pRes, &supp);
    tSQLBinaryExprDestroy(&pExpr, tSQLListTraverseDestroyInfo);
  }

  tansformQueryResult(pRes);

  return TSDB_CODE_SUCCESS;
}

int mgmtRetrieveMetersFromMetric(SMetricMetaMsg* pMsg, int32_t tableIndex, tQueryResultset* pRes) {
  SMetricMetaElemMsg* pElem = (SMetricMetaElemMsg*)((char*)pMsg + pMsg->metaElem[tableIndex]);
  STabObj*            pMetric = mgmtGetMeter(pElem->meterId);
  char*               pCond = NULL;
  char*               tmpTableNameCond = NULL;

  // no table created in accordance with this metric.
  if (pMetric->pSkipList == NULL || pMetric->pSkipList->nSize == 0) {
    assert(pMetric->numOfMeters == 0);
    return TSDB_CODE_SUCCESS;
  }

  char*   pQueryCond = (char*)pMsg + pElem->cond;
  int32_t condLen = pElem->condLen;

  // transfer the unicode string to mbs binary expression
  if (condLen > 0) {
    pCond = calloc(1, (condLen + 1) * TSDB_NCHAR_SIZE);

    taosUcs4ToMbs(pQueryCond, condLen * TSDB_NCHAR_SIZE, pCond);
    condLen = strlen(pCond) + 1;
    mTrace("metric:%s len:%d, metric query condition:%s", pMetric->meterId, condLen, pCond);
  }

  char* tablenameCond = (char*)pMsg + pElem->tableCond;

  if (pElem->tableCondLen > 0) {
    tmpTableNameCond = calloc(1, pElem->tableCondLen + 1);
    strncpy(tmpTableNameCond, tablenameCond, pElem->tableCondLen);

    mTrace("metric:%s rel:%d, len:%d, table name cond:%s", pMetric->meterId, pElem->rel, pElem->tableCondLen,
           tmpTableNameCond);
  }

  if (pElem->tableCondLen > 0 || condLen > 0) {
    mgmtFilterByTableNameCond(pRes, tmpTableNameCond, pElem->tableCondLen, pMetric);

    bool noNextCal = (pRes->num == 0 && pElem->rel == TSDB_RELATION_AND);  // no need to calculate next result

    if (!noNextCal && condLen > 0) {
      tQueryResultset filterRes = {0};

      int32_t ret = mgmtFilterMeterByIndex(pMetric, &filterRes, pCond, condLen);
      if (ret != TSDB_CODE_SUCCESS) {
        tfree(pCond);
        tfree(tmpTableNameCond);

        return ret;
      }

      // union or intersect of two results
      assert(pElem->rel == TSDB_RELATION_AND || pElem->rel == TSDB_RELATION_OR);

      if (pElem->rel == TSDB_RELATION_AND) {
        if (filterRes.num == 0 || pRes->num == 0) {  // intersect two sets
          tQueryResultClean(pRes);
        } else {
          queryResultIntersect(pRes, &filterRes);
        }
      } else {  // union two sets
        queryResultUnion(pRes, &filterRes);
      }

      tQueryResultClean(&filterRes);
    }
  } else {
    mTrace("metric:%s retrieve all meter, no query condition", pMetric->meterId);
    pRes->num = tSkipListIterateList(pMetric->pSkipList, (tSkipListNode***)&pRes->pRes, NULL, NULL);
    tansformQueryResult(pRes);
  }

  tfree(pCond);
  tfree(tmpTableNameCond);

  mTrace("metric:%s numOfRes:%d", pMetric->meterId, pRes->num);
  return TSDB_CODE_SUCCESS;
}

// todo refactor!!!!!
static char* getTagValueFromMeter(STabObj* pMeter, int32_t offset, int32_t len, char* param) {
  if (offset == TSDB_TBNAME_COLUMN_INDEX) {
    extractTableName(pMeter->meterId, param);
  } else {
    char* tags = pMeter->pTagData + offset + TSDB_METER_ID_LEN;  // tag start position
    memcpy(param, tags, len);  // make sure the value is null-terminated string
  }
  
  return param;
}

bool tSkipListNodeFilterCallback(const void* pNode, void* param) {
  
  tQueryInfo* pInfo = (tQueryInfo*)param;
  STabObj*    pMeter = (STabObj*)(((tSkipListNode*)pNode)->pData);

  char   buf[TSDB_MAX_TAGS_LEN] = {0};
  
  char*  val = getTagValueFromMeter(pMeter, pInfo->offset, pInfo->sch.bytes, buf);
  int8_t type = pInfo->sch.type;

  int32_t ret = 0;
  if (pInfo->q.nType == TSDB_DATA_TYPE_BINARY || pInfo->q.nType == TSDB_DATA_TYPE_NCHAR) {
    ret = pInfo->compare(val, pInfo->q.pz);
  } else {
    tVariant t = {0};
    tVariantCreateFromBinary(&t, val, (uint32_t) pInfo->sch.bytes, type);

    ret = pInfo->compare(&t.i64Key, &pInfo->q.i64Key);
  }

  switch (pInfo->optr) {
    case TSDB_RELATION_EQUAL: {
      return ret == 0;
    }
    case TSDB_RELATION_NOT_EQUAL: {
      return ret != 0;
    }
    case TSDB_RELATION_LARGE_EQUAL: {
      return ret >= 0;
    }
    case TSDB_RELATION_LARGE: {
      return ret > 0;
    }
    case TSDB_RELATION_LESS_EQUAL: {
      return ret <= 0;
    }
    case TSDB_RELATION_LESS: {
      return ret < 0;
    }
    case TSDB_RELATION_LIKE: {
      return ret == 0;
    }

    default:
      assert(false);
  }
  return true;
}
