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

#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"
#include "tdatablock.h"

#define COPY_SCALAR_FIELD(fldname)     \
  do {                                 \
    (pDst)->fldname = (pSrc)->fldname; \
  } while (0)

#define COPY_CHAR_ARRAY_FIELD(fldname)        \
  do {                                        \
    strcpy((pDst)->fldname, (pSrc)->fldname); \
  } while (0)

#define COPY_OBJECT_FIELD(fldname, size)                  \
  do {                                                    \
    memcpy(&((pDst)->fldname), &((pSrc)->fldname), size); \
  } while (0)

#define COPY_CHAR_POINT_FIELD(fldname)             \
  do {                                             \
    if (NULL == (pSrc)->fldname) {                 \
      break;                                       \
    }                                              \
    (pDst)->fldname = taosStrdup((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                 \
      return TSDB_CODE_OUT_OF_MEMORY;              \
    }                                              \
  } while (0)

#define CLONE_NODE_FIELD(fldname)                      \
  do {                                                 \
    if (NULL == (pSrc)->fldname) {                     \
      break;                                           \
    }                                                  \
    (pDst)->fldname = nodesCloneNode((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                     \
      return TSDB_CODE_OUT_OF_MEMORY;                  \
    }                                                  \
  } while (0)

#define CLONE_NODE_FIELD_EX(fldname, nodePtrType)                           \
  do {                                                                      \
    if (NULL == (pSrc)->fldname) {                                          \
      break;                                                                \
    }                                                                       \
    (pDst)->fldname = (nodePtrType)nodesCloneNode((SNode*)(pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                                          \
      return TSDB_CODE_OUT_OF_MEMORY;                                       \
    }                                                                       \
  } while (0)

#define CLONE_NODE_LIST_FIELD(fldname)                 \
  do {                                                 \
    if (NULL == (pSrc)->fldname) {                     \
      break;                                           \
    }                                                  \
    (pDst)->fldname = nodesCloneList((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                     \
      return TSDB_CODE_OUT_OF_MEMORY;                  \
    }                                                  \
  } while (0)

#define CLONE_OBJECT_FIELD(fldname, cloneFunc)    \
  do {                                            \
    if (NULL == (pSrc)->fldname) {                \
      break;                                      \
    }                                             \
    (pDst)->fldname = cloneFunc((pSrc)->fldname); \
    if (NULL == (pDst)->fldname) {                \
      return TSDB_CODE_OUT_OF_MEMORY;             \
    }                                             \
  } while (0)

#define COPY_BASE_OBJECT_FIELD(fldname, copyFunc)                                \
  do {                                                                           \
    if (TSDB_CODE_SUCCESS != copyFunc(&((pSrc)->fldname), &((pDst)->fldname))) { \
      return TSDB_CODE_OUT_OF_MEMORY;                                            \
    }                                                                            \
  } while (0)

static int32_t exprNodeCopy(const SExprNode* pSrc, SExprNode* pDst) {
  COPY_OBJECT_FIELD(resType, sizeof(SDataType));
  COPY_CHAR_ARRAY_FIELD(aliasName);
  COPY_CHAR_ARRAY_FIELD(userAlias);
  COPY_SCALAR_FIELD(orderAlias);
  return TSDB_CODE_SUCCESS;
}

static int32_t columnNodeCopy(const SColumnNode* pSrc, SColumnNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  COPY_SCALAR_FIELD(tableId);
  COPY_SCALAR_FIELD(tableType);
  COPY_SCALAR_FIELD(colId);
  COPY_SCALAR_FIELD(projIdx);
  COPY_SCALAR_FIELD(colType);
  COPY_SCALAR_FIELD(hasIndex);
  COPY_CHAR_ARRAY_FIELD(dbName);
  COPY_CHAR_ARRAY_FIELD(tableName);
  COPY_CHAR_ARRAY_FIELD(tableAlias);
  COPY_CHAR_ARRAY_FIELD(colName);
  COPY_SCALAR_FIELD(dataBlockId);
  COPY_SCALAR_FIELD(slotId);
  return TSDB_CODE_SUCCESS;
}

static int32_t valueNodeCopy(const SValueNode* pSrc, SValueNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  COPY_CHAR_POINT_FIELD(literal);
  COPY_SCALAR_FIELD(isDuration);
  COPY_SCALAR_FIELD(translate);
  COPY_SCALAR_FIELD(notReserved);
  COPY_SCALAR_FIELD(isNull);
  COPY_SCALAR_FIELD(placeholderNo);
  COPY_SCALAR_FIELD(typeData);
  COPY_SCALAR_FIELD(unit);
  if (!pSrc->translate || pSrc->isNull) {
    return TSDB_CODE_SUCCESS;
  }
  switch (pSrc->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
      COPY_SCALAR_FIELD(datum.b);
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      COPY_SCALAR_FIELD(datum.i);
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      COPY_SCALAR_FIELD(datum.d);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      COPY_SCALAR_FIELD(datum.u);
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY: {
      int32_t len = pSrc->node.resType.bytes + 1;
      pDst->datum.p = taosMemoryCalloc(1, len);
      if (NULL == pDst->datum.p) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      memcpy(pDst->datum.p, pSrc->datum.p, len);
      break;
    }
    case TSDB_DATA_TYPE_JSON: {
      int32_t len = getJsonValueLen(pSrc->datum.p);
      pDst->datum.p = taosMemoryCalloc(1, len);
      if (NULL == pDst->datum.p) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      memcpy(pDst->datum.p, pSrc->datum.p, len);
      break;
    }
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t operatorNodeCopy(const SOperatorNode* pSrc, SOperatorNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  COPY_SCALAR_FIELD(opType);
  CLONE_NODE_FIELD(pLeft);
  CLONE_NODE_FIELD(pRight);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicConditionNodeCopy(const SLogicConditionNode* pSrc, SLogicConditionNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  COPY_SCALAR_FIELD(condType);
  CLONE_NODE_LIST_FIELD(pParameterList);
  return TSDB_CODE_SUCCESS;
}

static int32_t functionNodeCopy(const SFunctionNode* pSrc, SFunctionNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  COPY_CHAR_ARRAY_FIELD(functionName);
  COPY_SCALAR_FIELD(funcId);
  COPY_SCALAR_FIELD(funcType);
  CLONE_NODE_LIST_FIELD(pParameterList);
  COPY_SCALAR_FIELD(udfBufSize);
  return TSDB_CODE_SUCCESS;
}

static int32_t tableNodeCopy(const STableNode* pSrc, STableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  COPY_CHAR_ARRAY_FIELD(dbName);
  COPY_CHAR_ARRAY_FIELD(tableName);
  COPY_CHAR_ARRAY_FIELD(tableAlias);
  COPY_SCALAR_FIELD(precision);
  COPY_SCALAR_FIELD(singleTable);
  return TSDB_CODE_SUCCESS;
}

static STableMeta* tableMetaClone(const STableMeta* pSrc) {
  int32_t     len = TABLE_META_SIZE(pSrc);
  STableMeta* pDst = taosMemoryMalloc(len);
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, len);
  return pDst;
}

static SVgroupsInfo* vgroupsInfoClone(const SVgroupsInfo* pSrc) {
  int32_t       len = VGROUPS_INFO_SIZE(pSrc);
  SVgroupsInfo* pDst = taosMemoryMalloc(len);
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, len);
  return pDst;
}

static SArray* functParamClone(const SArray* pSrc) {
  int32_t       len = sizeof(SArray) + pSrc->capacity * pSrc->elemSize;

  SArray* pDst = taosArrayInit(pSrc->capacity, pSrc->elemSize);
  if (NULL == pDst) {
    return NULL;
  }
  for (int i = 0; i < TARRAY_SIZE(pSrc); ++i) {
    SFunctParam* pFunctParam = taosArrayGet(pSrc, i);
    SFunctParam* pNewFunctParam = (SFunctParam*)taosArrayPush(pDst, pFunctParam);

    if (NULL == pNewFunctParam) {
      return NULL;
    }
    pNewFunctParam->type = pFunctParam->type;
    pNewFunctParam->pCol = taosMemoryCalloc(1, sizeof(SColumn));
    memcpy(pNewFunctParam->pCol, pFunctParam->pCol, sizeof(SColumn));
  }

  return pDst;
}


static int32_t realTableNodeCopy(const SRealTableNode* pSrc, SRealTableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(table, tableNodeCopy);
  CLONE_OBJECT_FIELD(pMeta, tableMetaClone);
  CLONE_OBJECT_FIELD(pVgroupList, vgroupsInfoClone);
  COPY_CHAR_ARRAY_FIELD(qualDbName);
  COPY_SCALAR_FIELD(ratio);
  return TSDB_CODE_SUCCESS;
}

static int32_t tempTableNodeCopy(const STempTableNode* pSrc, STempTableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(table, tableNodeCopy);
  CLONE_NODE_FIELD(pSubquery);
  return TSDB_CODE_SUCCESS;
}

static int32_t joinTableNodeCopy(const SJoinTableNode* pSrc, SJoinTableNode* pDst) {
  COPY_BASE_OBJECT_FIELD(table, tableNodeCopy);
  COPY_SCALAR_FIELD(joinType);
  COPY_SCALAR_FIELD(hasSubQuery);
  COPY_SCALAR_FIELD(isLowLevelJoin);
  CLONE_NODE_FIELD(pLeft);
  CLONE_NODE_FIELD(pRight);
  CLONE_NODE_FIELD(pOnCond);
  return TSDB_CODE_SUCCESS;
}

static int32_t targetNodeCopy(const STargetNode* pSrc, STargetNode* pDst) {
  COPY_SCALAR_FIELD(dataBlockId);
  COPY_SCALAR_FIELD(slotId);
  CLONE_NODE_FIELD(pExpr);
  return TSDB_CODE_SUCCESS;
}

static int32_t groupingSetNodeCopy(const SGroupingSetNode* pSrc, SGroupingSetNode* pDst) {
  COPY_SCALAR_FIELD(groupingSetType);
  CLONE_NODE_LIST_FIELD(pParameterList);
  return TSDB_CODE_SUCCESS;
}

static int32_t orderByExprNodeCopy(const SOrderByExprNode* pSrc, SOrderByExprNode* pDst) {
  CLONE_NODE_FIELD(pExpr);
  COPY_SCALAR_FIELD(order);
  COPY_SCALAR_FIELD(nullOrder);
  return TSDB_CODE_SUCCESS;
}

static int32_t limitNodeCopy(const SLimitNode* pSrc, SLimitNode* pDst) {
  COPY_SCALAR_FIELD(limit);
  COPY_SCALAR_FIELD(offset);
  return TSDB_CODE_SUCCESS;
}

static int32_t stateWindowNodeCopy(const SStateWindowNode* pSrc, SStateWindowNode* pDst) {
  CLONE_NODE_FIELD(pCol);
  CLONE_NODE_FIELD(pExpr);
  return TSDB_CODE_SUCCESS;
}

static int32_t eventWindowNodeCopy(const SEventWindowNode* pSrc, SEventWindowNode* pDst) {
  CLONE_NODE_FIELD(pCol);
  CLONE_NODE_FIELD(pStartCond);
  CLONE_NODE_FIELD(pEndCond);
  return TSDB_CODE_SUCCESS;
}

static int32_t countWindowNodeCopy(const SCountWindowNode* pSrc, SCountWindowNode* pDst) {
  CLONE_NODE_FIELD(pCol);
  COPY_SCALAR_FIELD(windowCount);
  COPY_SCALAR_FIELD(windowSliding);
  return TSDB_CODE_SUCCESS;
}

static int32_t sessionWindowNodeCopy(const SSessionWindowNode* pSrc, SSessionWindowNode* pDst) {
  CLONE_NODE_FIELD_EX(pCol, SColumnNode*);
  CLONE_NODE_FIELD_EX(pGap, SValueNode*);
  return TSDB_CODE_SUCCESS;
}

static int32_t intervalWindowNodeCopy(const SIntervalWindowNode* pSrc, SIntervalWindowNode* pDst) {
  CLONE_NODE_FIELD(pCol);
  CLONE_NODE_FIELD(pInterval);
  CLONE_NODE_FIELD(pOffset);
  CLONE_NODE_FIELD(pSliding);
  CLONE_NODE_FIELD(pFill);
  return TSDB_CODE_SUCCESS;
}

static int32_t nodeListNodeCopy(const SNodeListNode* pSrc, SNodeListNode* pDst) {
  COPY_OBJECT_FIELD(node.resType, sizeof(SDataType));
  CLONE_NODE_LIST_FIELD(pNodeList);
  return TSDB_CODE_SUCCESS;
}

static int32_t fillNodeCopy(const SFillNode* pSrc, SFillNode* pDst) {
  COPY_SCALAR_FIELD(mode);
  CLONE_NODE_FIELD(pValues);
  CLONE_NODE_FIELD(pWStartTs);
  COPY_OBJECT_FIELD(timeRange, sizeof(STimeWindow));
  return TSDB_CODE_SUCCESS;
}

static int32_t whenThenNodeCopy(const SWhenThenNode* pSrc, SWhenThenNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  CLONE_NODE_FIELD(pWhen);
  CLONE_NODE_FIELD(pThen);
  return TSDB_CODE_SUCCESS;
}

static int32_t caseWhenNodeCopy(const SCaseWhenNode* pSrc, SCaseWhenNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, exprNodeCopy);
  CLONE_NODE_FIELD(pCase);
  CLONE_NODE_FIELD(pElse);
  CLONE_NODE_LIST_FIELD(pWhenThenList);
  return TSDB_CODE_SUCCESS;
}

static int32_t copyHintValue(const SHintNode* pSrc, SHintNode* pDst) {
  if (NULL == pSrc->value) {
    pDst->value = NULL;
    return TSDB_CODE_SUCCESS;
  }
  switch (pSrc->option) {
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t hintNodeCopy(const SHintNode* pSrc, SHintNode* pDst) {
  COPY_SCALAR_FIELD(type);
  COPY_SCALAR_FIELD(option);
  return copyHintValue(pSrc, pDst);
}

static int32_t logicNodeCopy(const SLogicNode* pSrc, SLogicNode* pDst) {
  CLONE_NODE_LIST_FIELD(pTargets);
  CLONE_NODE_FIELD(pConditions);
  CLONE_NODE_LIST_FIELD(pChildren);
  COPY_SCALAR_FIELD(optimizedFlag);
  COPY_SCALAR_FIELD(precision);
  CLONE_NODE_FIELD(pLimit);
  CLONE_NODE_FIELD(pSlimit);
  COPY_SCALAR_FIELD(requireDataOrder);
  COPY_SCALAR_FIELD(resultDataOrder);
  COPY_SCALAR_FIELD(groupAction);
  COPY_SCALAR_FIELD(inputTsOrder);
  COPY_SCALAR_FIELD(outputTsOrder);
  COPY_SCALAR_FIELD(dynamicOp);
  COPY_SCALAR_FIELD(forceCreateNonBlockingOptr);
  CLONE_NODE_LIST_FIELD(pHint);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicScanCopy(const SScanLogicNode* pSrc, SScanLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pScanCols);
  CLONE_NODE_LIST_FIELD(pScanPseudoCols);
  COPY_SCALAR_FIELD(tableType);
  COPY_SCALAR_FIELD(tableId);
  COPY_SCALAR_FIELD(stableId);
  CLONE_OBJECT_FIELD(pVgroupList, vgroupsInfoClone);
  COPY_SCALAR_FIELD(scanType);
  COPY_OBJECT_FIELD(scanSeq[0], sizeof(uint8_t) * 2);
  COPY_OBJECT_FIELD(scanRange, sizeof(STimeWindow));
  COPY_OBJECT_FIELD(tableName, sizeof(SName));
  COPY_SCALAR_FIELD(showRewrite);
  COPY_SCALAR_FIELD(ratio);
  CLONE_NODE_LIST_FIELD(pDynamicScanFuncs);
  COPY_SCALAR_FIELD(dataRequired);
  COPY_SCALAR_FIELD(interval);
  COPY_SCALAR_FIELD(offset);
  COPY_SCALAR_FIELD(sliding);
  COPY_SCALAR_FIELD(intervalUnit);
  COPY_SCALAR_FIELD(slidingUnit);
  CLONE_NODE_FIELD(pTagCond);
  CLONE_NODE_FIELD(pTagIndexCond);
  COPY_SCALAR_FIELD(triggerType);
  COPY_SCALAR_FIELD(watermark);
  COPY_SCALAR_FIELD(deleteMark);
  COPY_SCALAR_FIELD(igExpired);
  COPY_SCALAR_FIELD(igCheckUpdate);
  CLONE_NODE_LIST_FIELD(pGroupTags);
  COPY_SCALAR_FIELD(groupSort);
  CLONE_NODE_LIST_FIELD(pTags);
  CLONE_NODE_FIELD(pSubtable);
  COPY_SCALAR_FIELD(cacheLastMode);
  COPY_SCALAR_FIELD(igLastNull);
  COPY_SCALAR_FIELD(groupOrderScan);
  COPY_SCALAR_FIELD(onlyMetaCtbIdx);
  COPY_SCALAR_FIELD(filesetDelimited);
  COPY_SCALAR_FIELD(isCountByTag);
  CLONE_OBJECT_FIELD(pFuncTypes, functParamClone);
  COPY_SCALAR_FIELD(paraTablesSort);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicJoinCopy(const SJoinLogicNode* pSrc, SJoinLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(joinType);
  COPY_SCALAR_FIELD(joinAlgo);
  CLONE_NODE_FIELD(pPrimKeyEqCond);
  CLONE_NODE_FIELD(pColEqCond);
  CLONE_NODE_FIELD(pTagEqCond);
  CLONE_NODE_FIELD(pTagOnCond);
  CLONE_NODE_FIELD(pOtherOnCond);
  COPY_SCALAR_FIELD(isSingleTableJoin);
  COPY_SCALAR_FIELD(hasSubQuery);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicAggCopy(const SAggLogicNode* pSrc, SAggLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pGroupKeys);
  CLONE_NODE_LIST_FIELD(pAggFuncs);
  COPY_SCALAR_FIELD(hasLastRow);
  COPY_SCALAR_FIELD(hasLast);
  COPY_SCALAR_FIELD(hasTimeLineFunc);
  COPY_SCALAR_FIELD(onlyHasKeepOrderFunc);
  COPY_SCALAR_FIELD(hasGroupKeyOptimized);
  COPY_SCALAR_FIELD(isGroupTb);
  COPY_SCALAR_FIELD(isPartTb);
  COPY_SCALAR_FIELD(hasGroup);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicProjectCopy(const SProjectLogicNode* pSrc, SProjectLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pProjections);
  COPY_CHAR_ARRAY_FIELD(stmtName);
  COPY_SCALAR_FIELD(ignoreGroupId);
  COPY_SCALAR_FIELD(inputIgnoreGroup);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicVnodeModifCopy(const SVnodeModifyLogicNode* pSrc, SVnodeModifyLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(modifyType);
  COPY_SCALAR_FIELD(msgType);
  CLONE_NODE_FIELD(pAffectedRows);
  CLONE_NODE_FIELD(pStartTs);
  CLONE_NODE_FIELD(pEndTs);
  COPY_SCALAR_FIELD(tableId);
  COPY_SCALAR_FIELD(stableId);
  COPY_SCALAR_FIELD(tableType);
  COPY_CHAR_ARRAY_FIELD(tableName);
  COPY_CHAR_ARRAY_FIELD(tsColName);
  COPY_OBJECT_FIELD(deleteTimeRange, sizeof(STimeWindow));
  CLONE_OBJECT_FIELD(pVgroupList, vgroupsInfoClone);
  CLONE_NODE_LIST_FIELD(pInsertCols);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicExchangeCopy(const SExchangeLogicNode* pSrc, SExchangeLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(srcStartGroupId);
  COPY_SCALAR_FIELD(srcEndGroupId);
  COPY_SCALAR_FIELD(seqRecvData);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicMergeCopy(const SMergeLogicNode* pSrc, SMergeLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pMergeKeys);
  CLONE_NODE_LIST_FIELD(pInputs);
  COPY_SCALAR_FIELD(numOfChannels);
  COPY_SCALAR_FIELD(srcGroupId);
  COPY_SCALAR_FIELD(colsMerge);
  COPY_SCALAR_FIELD(needSort);
  COPY_SCALAR_FIELD(groupSort);
  COPY_SCALAR_FIELD(ignoreGroupId);
  COPY_SCALAR_FIELD(inputWithGroupId);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicWindowCopy(const SWindowLogicNode* pSrc, SWindowLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(winType);
  CLONE_NODE_LIST_FIELD(pFuncs);
  COPY_SCALAR_FIELD(interval);
  COPY_SCALAR_FIELD(offset);
  COPY_SCALAR_FIELD(sliding);
  COPY_SCALAR_FIELD(intervalUnit);
  COPY_SCALAR_FIELD(slidingUnit);
  COPY_SCALAR_FIELD(sessionGap);
  CLONE_NODE_FIELD(pTspk);
  CLONE_NODE_FIELD(pTsEnd);
  CLONE_NODE_FIELD(pStateExpr);
  CLONE_NODE_FIELD(pStartCond);
  CLONE_NODE_FIELD(pEndCond);
  COPY_SCALAR_FIELD(triggerType);
  COPY_SCALAR_FIELD(watermark);
  COPY_SCALAR_FIELD(deleteMark);
  COPY_SCALAR_FIELD(igExpired);
  COPY_SCALAR_FIELD(igCheckUpdate);
  COPY_SCALAR_FIELD(windowAlgo);
  COPY_SCALAR_FIELD(windowCount);
  COPY_SCALAR_FIELD(windowSliding);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicFillCopy(const SFillLogicNode* pSrc, SFillLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(mode);
  CLONE_NODE_LIST_FIELD(pFillExprs);
  CLONE_NODE_LIST_FIELD(pNotFillExprs);
  CLONE_NODE_FIELD(pWStartTs);
  CLONE_NODE_FIELD(pValues);
  COPY_OBJECT_FIELD(timeRange, sizeof(STimeWindow));
  return TSDB_CODE_SUCCESS;
}

static int32_t logicSortCopy(const SSortLogicNode* pSrc, SSortLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pSortKeys);
  COPY_SCALAR_FIELD(groupSort);
  COPY_SCALAR_FIELD(calcGroupId);
  COPY_SCALAR_FIELD(excludePkCol);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicPartitionCopy(const SPartitionLogicNode* pSrc, SPartitionLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pPartitionKeys);
  CLONE_NODE_LIST_FIELD(pTags);
  CLONE_NODE_FIELD(pSubtable);
  CLONE_NODE_LIST_FIELD(pAggFuncs);
  COPY_SCALAR_FIELD(needBlockOutputTsOrder);
  COPY_SCALAR_FIELD(pkTsColId);
  COPY_SCALAR_FIELD(pkTsColTbId);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicIndefRowsFuncCopy(const SIndefRowsFuncLogicNode* pSrc, SIndefRowsFuncLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pFuncs);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicInterpFuncCopy(const SInterpFuncLogicNode* pSrc, SInterpFuncLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  CLONE_NODE_LIST_FIELD(pFuncs);
  COPY_OBJECT_FIELD(timeRange, sizeof(STimeWindow));
  COPY_SCALAR_FIELD(interval);
  COPY_SCALAR_FIELD(fillMode);
  CLONE_NODE_FIELD(pFillValues);
  CLONE_NODE_FIELD(pTimeSeries);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicGroupCacheCopy(const SGroupCacheLogicNode* pSrc, SGroupCacheLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(grpColsMayBeNull);
  COPY_SCALAR_FIELD(grpByUid);
  COPY_SCALAR_FIELD(globalGrp);
  COPY_SCALAR_FIELD(batchFetch);
  CLONE_NODE_LIST_FIELD(pGroupCols);
  return TSDB_CODE_SUCCESS;
}

static int32_t logicDynQueryCtrlCopy(const SDynQueryCtrlLogicNode* pSrc, SDynQueryCtrlLogicNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, logicNodeCopy);
  COPY_SCALAR_FIELD(qType);
  COPY_SCALAR_FIELD(stbJoin.batchFetch);
  CLONE_NODE_LIST_FIELD(stbJoin.pVgList);
  CLONE_NODE_LIST_FIELD(stbJoin.pUidList);
  COPY_OBJECT_FIELD(stbJoin.srcScan, sizeof(pDst->stbJoin.srcScan));
  return TSDB_CODE_SUCCESS;
}

static int32_t logicSubplanCopy(const SLogicSubplan* pSrc, SLogicSubplan* pDst) {
  COPY_OBJECT_FIELD(id, sizeof(SSubplanId));
  CLONE_NODE_FIELD_EX(pNode, SLogicNode*);
  COPY_SCALAR_FIELD(subplanType);
  COPY_SCALAR_FIELD(level);
  COPY_SCALAR_FIELD(splitFlag);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiNodeCopy(const SPhysiNode* pSrc, SPhysiNode* pDst) {
  CLONE_NODE_FIELD_EX(pOutputDataBlockDesc, SDataBlockDescNode*);
  CLONE_NODE_FIELD(pConditions);
  CLONE_NODE_LIST_FIELD(pChildren);
  COPY_SCALAR_FIELD(inputTsOrder);
  COPY_SCALAR_FIELD(outputTsOrder);
  COPY_SCALAR_FIELD(dynamicOp);
  COPY_SCALAR_FIELD(forceCreateNonBlockingOptr);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiScanCopy(const SScanPhysiNode* pSrc, SScanPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, physiNodeCopy);
  CLONE_NODE_LIST_FIELD(pScanCols);
  CLONE_NODE_LIST_FIELD(pScanPseudoCols);
  COPY_SCALAR_FIELD(uid);
  COPY_SCALAR_FIELD(suid);
  COPY_SCALAR_FIELD(tableType);
  COPY_OBJECT_FIELD(tableName, sizeof(SName));
  COPY_SCALAR_FIELD(groupOrderScan);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiTagScanCopy(const STagScanPhysiNode* pSrc, STagScanPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(scan, physiScanCopy);
  COPY_SCALAR_FIELD(onlyMetaCtbIdx);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiTableScanCopy(const STableScanPhysiNode* pSrc, STableScanPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(scan, physiScanCopy);
  COPY_OBJECT_FIELD(scanSeq[0], sizeof(uint8_t) * 2);
  COPY_OBJECT_FIELD(scanRange, sizeof(STimeWindow));
  COPY_SCALAR_FIELD(ratio);
  COPY_SCALAR_FIELD(dataRequired);
  CLONE_NODE_LIST_FIELD(pDynamicScanFuncs);
  CLONE_NODE_LIST_FIELD(pGroupTags);
  COPY_SCALAR_FIELD(interval);
  COPY_SCALAR_FIELD(offset);
  COPY_SCALAR_FIELD(sliding);
  COPY_SCALAR_FIELD(intervalUnit);
  COPY_SCALAR_FIELD(slidingUnit);
  COPY_SCALAR_FIELD(triggerType);
  COPY_SCALAR_FIELD(watermark);
  COPY_SCALAR_FIELD(igExpired);
  COPY_SCALAR_FIELD(filesetDelimited);
  COPY_SCALAR_FIELD(needCountEmptyTable);
  COPY_SCALAR_FIELD(paraTablesSort);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiSysTableScanCopy(const SSystemTableScanPhysiNode* pSrc, SSystemTableScanPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(scan, physiScanCopy);
  COPY_OBJECT_FIELD(mgmtEpSet, sizeof(SEpSet));
  COPY_SCALAR_FIELD(showRewrite);
  COPY_SCALAR_FIELD(accountId);
  COPY_SCALAR_FIELD(sysInfo);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiWindowCopy(const SWindowPhysiNode* pSrc, SWindowPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, physiNodeCopy);
  CLONE_NODE_LIST_FIELD(pExprs);
  CLONE_NODE_LIST_FIELD(pFuncs);
  CLONE_NODE_FIELD(pTspk);
  CLONE_NODE_FIELD(pTsEnd);
  COPY_SCALAR_FIELD(triggerType);
  COPY_SCALAR_FIELD(watermark);
  COPY_SCALAR_FIELD(igExpired);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiIntervalCopy(const SIntervalPhysiNode* pSrc, SIntervalPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(window, physiWindowCopy);
  COPY_SCALAR_FIELD(interval);
  COPY_SCALAR_FIELD(offset);
  COPY_SCALAR_FIELD(sliding);
  COPY_SCALAR_FIELD(intervalUnit);
  COPY_SCALAR_FIELD(slidingUnit);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiSessionCopy(const SSessionWinodwPhysiNode* pSrc, SSessionWinodwPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(window, physiWindowCopy);
  COPY_SCALAR_FIELD(gap);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiPartitionCopy(const SPartitionPhysiNode* pSrc, SPartitionPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, physiNodeCopy);
  CLONE_NODE_LIST_FIELD(pExprs);
  CLONE_NODE_LIST_FIELD(pPartitionKeys);
  CLONE_NODE_LIST_FIELD(pTargets);
  COPY_SCALAR_FIELD(needBlockOutputTsOrder);
  COPY_SCALAR_FIELD(tsSlotId);
  return TSDB_CODE_SUCCESS;
}

static int32_t physiProjectCopy(const SProjectPhysiNode* pSrc, SProjectPhysiNode* pDst) {
  COPY_BASE_OBJECT_FIELD(node, physiNodeCopy);
  CLONE_NODE_LIST_FIELD(pProjections);
  COPY_SCALAR_FIELD(mergeDataBlock);
  COPY_SCALAR_FIELD(ignoreGroupId);
  COPY_SCALAR_FIELD(inputIgnoreGroup);
  return TSDB_CODE_SUCCESS;
}

static int32_t dataBlockDescCopy(const SDataBlockDescNode* pSrc, SDataBlockDescNode* pDst) {
  COPY_SCALAR_FIELD(dataBlockId);
  CLONE_NODE_LIST_FIELD(pSlots);
  COPY_SCALAR_FIELD(totalRowSize);
  COPY_SCALAR_FIELD(outputRowSize);
  COPY_SCALAR_FIELD(precision);
  return TSDB_CODE_SUCCESS;
}

static int32_t slotDescCopy(const SSlotDescNode* pSrc, SSlotDescNode* pDst) {
  COPY_SCALAR_FIELD(slotId);
  COPY_OBJECT_FIELD(dataType, sizeof(SDataType));
  COPY_SCALAR_FIELD(reserve);
  COPY_SCALAR_FIELD(output);
  COPY_SCALAR_FIELD(tag);
  return TSDB_CODE_SUCCESS;
}

static int32_t downstreamSourceCopy(const SDownstreamSourceNode* pSrc, SDownstreamSourceNode* pDst) {
  COPY_OBJECT_FIELD(addr, sizeof(SQueryNodeAddr));
  COPY_SCALAR_FIELD(taskId);
  COPY_SCALAR_FIELD(schedId);
  COPY_SCALAR_FIELD(execId);
  COPY_SCALAR_FIELD(fetchMsgType);
  COPY_SCALAR_FIELD(localExec);
  return TSDB_CODE_SUCCESS;
}

static int32_t selectStmtCopy(const SSelectStmt* pSrc, SSelectStmt* pDst) {
  COPY_SCALAR_FIELD(isDistinct);
  CLONE_NODE_LIST_FIELD(pProjectionList);
  CLONE_NODE_FIELD(pFromTable);
  CLONE_NODE_FIELD(pWhere);
  CLONE_NODE_LIST_FIELD(pPartitionByList);
  CLONE_NODE_FIELD(pWindow);
  CLONE_NODE_LIST_FIELD(pGroupByList);
  CLONE_NODE_FIELD(pHaving);
  CLONE_NODE_LIST_FIELD(pOrderByList);
  CLONE_NODE_FIELD_EX(pLimit, SLimitNode*);
  CLONE_NODE_FIELD_EX(pLimit, SLimitNode*);
  COPY_CHAR_ARRAY_FIELD(stmtName);
  COPY_SCALAR_FIELD(precision);
  COPY_SCALAR_FIELD(isEmptyResult);
  COPY_SCALAR_FIELD(timeLineResMode);
  COPY_SCALAR_FIELD(hasAggFuncs);
  COPY_SCALAR_FIELD(hasRepeatScanFuncs);
  CLONE_NODE_LIST_FIELD(pHint);
  return TSDB_CODE_SUCCESS;
}

static int32_t setOperatorCopy(const SSetOperator* pSrc, SSetOperator* pDst) {
  COPY_SCALAR_FIELD(opType);
  CLONE_NODE_LIST_FIELD(pProjectionList);
  CLONE_NODE_FIELD(pLeft);
  CLONE_NODE_FIELD(pRight);
  CLONE_NODE_LIST_FIELD(pOrderByList);
  CLONE_NODE_FIELD(pLimit);
  COPY_CHAR_ARRAY_FIELD(stmtName);
  COPY_SCALAR_FIELD(precision);
  COPY_SCALAR_FIELD(timeLineResMode);
  return TSDB_CODE_SUCCESS;
}

SNode* nodesCloneNode(const SNode* pNode) {
  if (NULL == pNode) {
    return NULL;
  }

  SNode* pDst = nodesMakeNode(nodeType(pNode));
  if (NULL == pDst) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
      code = columnNodeCopy((const SColumnNode*)pNode, (SColumnNode*)pDst);
      break;
    case QUERY_NODE_VALUE:
      code = valueNodeCopy((const SValueNode*)pNode, (SValueNode*)pDst);
      break;
    case QUERY_NODE_OPERATOR:
      code = operatorNodeCopy((const SOperatorNode*)pNode, (SOperatorNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_CONDITION:
      code = logicConditionNodeCopy((const SLogicConditionNode*)pNode, (SLogicConditionNode*)pDst);
      break;
    case QUERY_NODE_FUNCTION:
      code = functionNodeCopy((const SFunctionNode*)pNode, (SFunctionNode*)pDst);
      break;
    case QUERY_NODE_REAL_TABLE:
      code = realTableNodeCopy((const SRealTableNode*)pNode, (SRealTableNode*)pDst);
      break;
    case QUERY_NODE_TEMP_TABLE:
      code = tempTableNodeCopy((const STempTableNode*)pNode, (STempTableNode*)pDst);
      break;
    case QUERY_NODE_JOIN_TABLE:
      code = joinTableNodeCopy((const SJoinTableNode*)pNode, (SJoinTableNode*)pDst);
      break;
    case QUERY_NODE_GROUPING_SET:
      code = groupingSetNodeCopy((const SGroupingSetNode*)pNode, (SGroupingSetNode*)pDst);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      code = orderByExprNodeCopy((const SOrderByExprNode*)pNode, (SOrderByExprNode*)pDst);
      break;
    case QUERY_NODE_LIMIT:
      code = limitNodeCopy((const SLimitNode*)pNode, (SLimitNode*)pDst);
      break;
    case QUERY_NODE_STATE_WINDOW:
      code = stateWindowNodeCopy((const SStateWindowNode*)pNode, (SStateWindowNode*)pDst);
      break;
    case QUERY_NODE_EVENT_WINDOW:
      code = eventWindowNodeCopy((const SEventWindowNode*)pNode, (SEventWindowNode*)pDst);
      break;
    case QUERY_NODE_COUNT_WINDOW:
      code = countWindowNodeCopy((const SCountWindowNode*)pNode, (SCountWindowNode*)pDst);
      break;
    case QUERY_NODE_SESSION_WINDOW:
      code = sessionWindowNodeCopy((const SSessionWindowNode*)pNode, (SSessionWindowNode*)pDst);
      break;
    case QUERY_NODE_INTERVAL_WINDOW:
      code = intervalWindowNodeCopy((const SIntervalWindowNode*)pNode, (SIntervalWindowNode*)pDst);
      break;
    case QUERY_NODE_NODE_LIST:
      code = nodeListNodeCopy((const SNodeListNode*)pNode, (SNodeListNode*)pDst);
      break;
    case QUERY_NODE_FILL:
      code = fillNodeCopy((const SFillNode*)pNode, (SFillNode*)pDst);
      break;
    case QUERY_NODE_TARGET:
      code = targetNodeCopy((const STargetNode*)pNode, (STargetNode*)pDst);
      break;
    case QUERY_NODE_DATABLOCK_DESC:
      code = dataBlockDescCopy((const SDataBlockDescNode*)pNode, (SDataBlockDescNode*)pDst);
      break;
    case QUERY_NODE_SLOT_DESC:
      code = slotDescCopy((const SSlotDescNode*)pNode, (SSlotDescNode*)pDst);
      break;
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      code = downstreamSourceCopy((const SDownstreamSourceNode*)pNode, (SDownstreamSourceNode*)pDst);
      break;
    case QUERY_NODE_LEFT_VALUE:
      code = TSDB_CODE_SUCCESS;
      break;
    case QUERY_NODE_WHEN_THEN:
      code = whenThenNodeCopy((const SWhenThenNode*)pNode, (SWhenThenNode*)pDst);
      break;
    case QUERY_NODE_CASE_WHEN:
      code = caseWhenNodeCopy((const SCaseWhenNode*)pNode, (SCaseWhenNode*)pDst);
      break;
    case QUERY_NODE_HINT:
      code = hintNodeCopy((const SHintNode*)pNode, (SHintNode*)pDst);
      break;
    case QUERY_NODE_SET_OPERATOR:
      code = setOperatorCopy((const SSetOperator*)pNode, (SSetOperator*)pDst);
      break;
    case QUERY_NODE_SELECT_STMT:
      code = selectStmtCopy((const SSelectStmt*)pNode, (SSelectStmt*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = logicScanCopy((const SScanLogicNode*)pNode, (SScanLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = logicJoinCopy((const SJoinLogicNode*)pNode, (SJoinLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = logicAggCopy((const SAggLogicNode*)pNode, (SAggLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      code = logicProjectCopy((const SProjectLogicNode*)pNode, (SProjectLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY:
      code = logicVnodeModifCopy((const SVnodeModifyLogicNode*)pNode, (SVnodeModifyLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      code = logicExchangeCopy((const SExchangeLogicNode*)pNode, (SExchangeLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_MERGE:
      code = logicMergeCopy((const SMergeLogicNode*)pNode, (SMergeLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      code = logicWindowCopy((const SWindowLogicNode*)pNode, (SWindowLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_FILL:
      code = logicFillCopy((const SFillLogicNode*)pNode, (SFillLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
      code = logicSortCopy((const SSortLogicNode*)pNode, (SSortLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      code = logicPartitionCopy((const SPartitionLogicNode*)pNode, (SPartitionLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      code = logicIndefRowsFuncCopy((const SIndefRowsFuncLogicNode*)pNode, (SIndefRowsFuncLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      code = logicInterpFuncCopy((const SInterpFuncLogicNode*)pNode, (SInterpFuncLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_GROUP_CACHE:
      code = logicGroupCacheCopy((const SGroupCacheLogicNode*)pNode, (SGroupCacheLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      code = logicDynQueryCtrlCopy((const SDynQueryCtrlLogicNode*)pNode, (SDynQueryCtrlLogicNode*)pDst);
      break;
    case QUERY_NODE_LOGIC_SUBPLAN:
      code = logicSubplanCopy((const SLogicSubplan*)pNode, (SLogicSubplan*)pDst);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      code = physiTagScanCopy((const STagScanPhysiNode*)pNode, (STagScanPhysiNode*)pDst);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      code = physiTableScanCopy((const STableScanPhysiNode*)pNode, (STableScanPhysiNode*)pDst);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
      code = physiSysTableScanCopy((const SSystemTableScanPhysiNode*)pNode, (SSystemTableScanPhysiNode*)pDst);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
      code = physiIntervalCopy((const SIntervalPhysiNode*)pNode, (SIntervalPhysiNode*)pDst);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      code = physiSessionCopy((const SSessionWinodwPhysiNode*)pNode, (SSessionWinodwPhysiNode*)pDst);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
      code = physiPartitionCopy((const SPartitionPhysiNode*)pNode, (SPartitionPhysiNode*)pDst);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      code = physiProjectCopy((const SProjectPhysiNode*)pNode, (SProjectPhysiNode*)pDst);
      break;
    default:
      break;
  }

  if (TSDB_CODE_SUCCESS != code) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    nodesDestroyNode(pDst);
    nodesError("nodesCloneNode failed node = %s", nodesNodeName(nodeType(pNode)));
    return NULL;
  }
  return pDst;
}

SNodeList* nodesCloneList(const SNodeList* pList) {
  if (NULL == pList) {
    return NULL;
  }

  SNodeList* pDst = NULL;
  SNode*     pNode;
  FOREACH(pNode, pList) {
    int32_t code = nodesListMakeStrictAppend(&pDst, nodesCloneNode(pNode));
    if (TSDB_CODE_SUCCESS != code) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      nodesDestroyList(pDst);
      return NULL;
    }
  }
  return pDst;
}
