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

#include "cmdnodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"
#include "thash.h"

static SNode* makeNode(ENodeType type, size_t size) {
  SNode* p = taosMemoryCalloc(1, size);
  if (NULL == p) {
    return NULL;
  }
  setNodeType(p, type);
  return p;
}

SNode* nodesMakeNode(ENodeType type) {
  switch (type) {
    case QUERY_NODE_COLUMN:
      return makeNode(type, sizeof(SColumnNode));
    case QUERY_NODE_VALUE:
      return makeNode(type, sizeof(SValueNode));
    case QUERY_NODE_OPERATOR:
      return makeNode(type, sizeof(SOperatorNode));
    case QUERY_NODE_LOGIC_CONDITION:
      return makeNode(type, sizeof(SLogicConditionNode));
    case QUERY_NODE_FUNCTION:
      return makeNode(type, sizeof(SFunctionNode));
    case QUERY_NODE_REAL_TABLE:
      return makeNode(type, sizeof(SRealTableNode));
    case QUERY_NODE_TEMP_TABLE:
      return makeNode(type, sizeof(STempTableNode));
    case QUERY_NODE_JOIN_TABLE:
      return makeNode(type, sizeof(SJoinTableNode));
    case QUERY_NODE_GROUPING_SET:
      return makeNode(type, sizeof(SGroupingSetNode));
    case QUERY_NODE_ORDER_BY_EXPR:
      return makeNode(type, sizeof(SOrderByExprNode));
    case QUERY_NODE_LIMIT:
      return makeNode(type, sizeof(SLimitNode));
    case QUERY_NODE_STATE_WINDOW:
      return makeNode(type, sizeof(SStateWindowNode));
    case QUERY_NODE_SESSION_WINDOW:
      return makeNode(type, sizeof(SSessionWindowNode));
    case QUERY_NODE_INTERVAL_WINDOW:
      return makeNode(type, sizeof(SIntervalWindowNode));
    case QUERY_NODE_NODE_LIST:
      return makeNode(type, sizeof(SNodeListNode));
    case QUERY_NODE_FILL:
      return makeNode(type, sizeof(SFillNode));
    case QUERY_NODE_RAW_EXPR:
      return makeNode(type, sizeof(SRawExprNode));
    case QUERY_NODE_TARGET:
      return makeNode(type, sizeof(STargetNode));
    case QUERY_NODE_DATABLOCK_DESC:
      return makeNode(type, sizeof(SDataBlockDescNode));
    case QUERY_NODE_SLOT_DESC:
      return makeNode(type, sizeof(SSlotDescNode));
    case QUERY_NODE_COLUMN_DEF:
      return makeNode(type, sizeof(SColumnDefNode));
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      return makeNode(type, sizeof(SDownstreamSourceNode));
    case QUERY_NODE_DATABASE_OPTIONS:
      return makeNode(type, sizeof(SDatabaseOptions));
    case QUERY_NODE_TABLE_OPTIONS:
      return makeNode(type, sizeof(STableOptions));
    case QUERY_NODE_INDEX_OPTIONS:
      return makeNode(type, sizeof(SIndexOptions));
    case QUERY_NODE_EXPLAIN_OPTIONS:
      return makeNode(type, sizeof(SExplainOptions));
    case QUERY_NODE_STREAM_OPTIONS:
      return makeNode(type, sizeof(SStreamOptions));
    case QUERY_NODE_LEFT_VALUE:
      return makeNode(type, sizeof(SLeftValueNode));
    case QUERY_NODE_COLUMN_REF:
      return makeNode(type, sizeof(SColumnDefNode));
    case QUERY_NODE_SET_OPERATOR:
      return makeNode(type, sizeof(SSetOperator));
    case QUERY_NODE_SELECT_STMT:
      return makeNode(type, sizeof(SSelectStmt));
    case QUERY_NODE_VNODE_MODIF_STMT:
      return makeNode(type, sizeof(SVnodeModifOpStmt));
    case QUERY_NODE_CREATE_DATABASE_STMT:
      return makeNode(type, sizeof(SCreateDatabaseStmt));
    case QUERY_NODE_DROP_DATABASE_STMT:
      return makeNode(type, sizeof(SDropDatabaseStmt));
    case QUERY_NODE_ALTER_DATABASE_STMT:
      return makeNode(type, sizeof(SAlterDatabaseStmt));
    case QUERY_NODE_CREATE_TABLE_STMT:
      return makeNode(type, sizeof(SCreateTableStmt));
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE:
      return makeNode(type, sizeof(SCreateSubTableClause));
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      return makeNode(type, sizeof(SCreateMultiTableStmt));
    case QUERY_NODE_DROP_TABLE_CLAUSE:
      return makeNode(type, sizeof(SDropTableClause));
    case QUERY_NODE_DROP_TABLE_STMT:
      return makeNode(type, sizeof(SDropTableStmt));
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      return makeNode(type, sizeof(SDropSuperTableStmt));
    case QUERY_NODE_ALTER_TABLE_STMT:
      return makeNode(type, sizeof(SAlterTableStmt));
    case QUERY_NODE_CREATE_USER_STMT:
      return makeNode(type, sizeof(SCreateUserStmt));
    case QUERY_NODE_ALTER_USER_STMT:
      return makeNode(type, sizeof(SAlterUserStmt));
    case QUERY_NODE_DROP_USER_STMT:
      return makeNode(type, sizeof(SDropUserStmt));
    case QUERY_NODE_USE_DATABASE_STMT:
      return makeNode(type, sizeof(SUseDatabaseStmt));
    case QUERY_NODE_CREATE_DNODE_STMT:
      return makeNode(type, sizeof(SCreateDnodeStmt));
    case QUERY_NODE_DROP_DNODE_STMT:
      return makeNode(type, sizeof(SDropDnodeStmt));
    case QUERY_NODE_ALTER_DNODE_STMT:
      return makeNode(type, sizeof(SAlterDnodeStmt));
    case QUERY_NODE_CREATE_INDEX_STMT:
      return makeNode(type, sizeof(SCreateIndexStmt));
    case QUERY_NODE_DROP_INDEX_STMT:
      return makeNode(type, sizeof(SDropIndexStmt));
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_CREATE_BNODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
      return makeNode(type, sizeof(SCreateComponentNodeStmt));
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_DROP_BNODE_STMT:
    case QUERY_NODE_DROP_SNODE_STMT:
    case QUERY_NODE_DROP_MNODE_STMT:
      return makeNode(type, sizeof(SDropComponentNodeStmt));
    case QUERY_NODE_CREATE_TOPIC_STMT:
      return makeNode(type, sizeof(SCreateTopicStmt));
    case QUERY_NODE_DROP_TOPIC_STMT:
      return makeNode(type, sizeof(SDropTopicStmt));
    case QUERY_NODE_DROP_CGROUP_STMT:
      return makeNode(type, sizeof(SDropCGroupStmt));
    case QUERY_NODE_ALTER_LOCAL_STMT:
      return makeNode(type, sizeof(SAlterLocalStmt));
    case QUERY_NODE_EXPLAIN_STMT:
      return makeNode(type, sizeof(SExplainStmt));
    case QUERY_NODE_DESCRIBE_STMT:
      return makeNode(type, sizeof(SDescribeStmt));
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
      return makeNode(type, sizeof(SNode));
    case QUERY_NODE_COMPACT_STMT:
      break;
    case QUERY_NODE_CREATE_FUNCTION_STMT:
      return makeNode(type, sizeof(SCreateFunctionStmt));
    case QUERY_NODE_DROP_FUNCTION_STMT:
      return makeNode(type, sizeof(SDropFunctionStmt));
    case QUERY_NODE_CREATE_STREAM_STMT:
      return makeNode(type, sizeof(SCreateStreamStmt));
    case QUERY_NODE_DROP_STREAM_STMT:
      return makeNode(type, sizeof(SDropStreamStmt));
    case QUERY_NODE_BALANCE_VGROUP_STMT:
      return makeNode(type, sizeof(SBalanceVgroupStmt));
    case QUERY_NODE_MERGE_VGROUP_STMT:
      return makeNode(type, sizeof(SMergeVgroupStmt));
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
      return makeNode(type, sizeof(SRedistributeVgroupStmt));
    case QUERY_NODE_SPLIT_VGROUP_STMT:
      return makeNode(type, sizeof(SSplitVgroupStmt));
    case QUERY_NODE_SYNCDB_STMT:
      break;
    case QUERY_NODE_GRANT_STMT:
      return makeNode(type, sizeof(SGrantStmt));
    case QUERY_NODE_REVOKE_STMT:
      return makeNode(type, sizeof(SRevokeStmt));
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_LICENCE_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_CONSUMERS_STMT:
    case QUERY_NODE_SHOW_SUBSCRIBES_STMT:
    case QUERY_NODE_SHOW_SMAS_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_APPS_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_VARIABLE_STMT:
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
      return makeNode(type, sizeof(SShowStmt));
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return makeNode(type, sizeof(SShowCreateDatabaseStmt));
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return makeNode(type, sizeof(SShowCreateTableStmt));
    case QUERY_NODE_KILL_QUERY_STMT:
      return makeNode(type, sizeof(SKillQueryStmt));
    case QUERY_NODE_KILL_TRANSACTION_STMT:
    case QUERY_NODE_KILL_CONNECTION_STMT:
      return makeNode(type, sizeof(SKillStmt));
    case QUERY_NODE_DELETE_STMT:
      return makeNode(type, sizeof(SDeleteStmt));
    case QUERY_NODE_QUERY:
      return makeNode(type, sizeof(SQuery));
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return makeNode(type, sizeof(SScanLogicNode));
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return makeNode(type, sizeof(SJoinLogicNode));
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return makeNode(type, sizeof(SAggLogicNode));
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return makeNode(type, sizeof(SProjectLogicNode));
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY:
      return makeNode(type, sizeof(SVnodeModifyLogicNode));
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      return makeNode(type, sizeof(SExchangeLogicNode));
    case QUERY_NODE_LOGIC_PLAN_MERGE:
      return makeNode(type, sizeof(SMergeLogicNode));
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return makeNode(type, sizeof(SWindowLogicNode));
    case QUERY_NODE_LOGIC_PLAN_FILL:
      return makeNode(type, sizeof(SFillLogicNode));
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return makeNode(type, sizeof(SSortLogicNode));
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      return makeNode(type, sizeof(SPartitionLogicNode));
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      return makeNode(type, sizeof(SIndefRowsFuncLogicNode));
    case QUERY_NODE_LOGIC_SUBPLAN:
      return makeNode(type, sizeof(SLogicSubplan));
    case QUERY_NODE_LOGIC_PLAN:
      return makeNode(type, sizeof(SQueryLogicPlan));
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      return makeNode(type, sizeof(STagScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      return makeNode(type, sizeof(STableScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
      return makeNode(type, sizeof(STableSeqScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
      return makeNode(type, sizeof(STableMergeScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      return makeNode(type, sizeof(SStreamScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
      return makeNode(type, sizeof(SSystemTableScanPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return makeNode(type, sizeof(SProjectPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
      return makeNode(type, sizeof(SJoinPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG:
      return makeNode(type, sizeof(SAggPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      return makeNode(type, sizeof(SExchangePhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_MERGE:
      return makeNode(type, sizeof(SMergePhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
      return makeNode(type, sizeof(SSortPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
      return makeNode(type, sizeof(SIntervalPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL:
      return makeNode(type, sizeof(SMergeIntervalPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
      return makeNode(type, sizeof(SStreamIntervalPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
      return makeNode(type, sizeof(SStreamFinalIntervalPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
      return makeNode(type, sizeof(SStreamSemiIntervalPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
      return makeNode(type, sizeof(SFillPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
      return makeNode(type, sizeof(SSessionWinodwPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
      return makeNode(type, sizeof(SStreamSessionWinodwPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
      return makeNode(type, sizeof(SStreamSemiSessionWinodwPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      return makeNode(type, sizeof(SStreamFinalSessionWinodwPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
      return makeNode(type, sizeof(SStateWinodwPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE:
      return makeNode(type, sizeof(SStreamStateWinodwPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
      return makeNode(type, sizeof(SPartitionPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC:
      return makeNode(type, sizeof(SIndefRowsFuncPhysiNode));
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      return makeNode(type, sizeof(SDataDispatcherNode));
    case QUERY_NODE_PHYSICAL_PLAN_INSERT:
      return makeNode(type, sizeof(SDataInserterNode));
    case QUERY_NODE_PHYSICAL_PLAN_DELETE:
      return makeNode(type, sizeof(SDataDeleterNode));
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      return makeNode(type, sizeof(SSubplan));
    case QUERY_NODE_PHYSICAL_PLAN:
      return makeNode(type, sizeof(SQueryPlan));
    default:
      break;
  }
  nodesError("nodesMakeNode unknown node = %s", nodesNodeName(type));
  return NULL;
}

static void destroyVgDataBlockArray(SArray* pArray) {
  size_t size = taosArrayGetSize(pArray);
  for (size_t i = 0; i < size; ++i) {
    SVgDataBlocks* pVg = taosArrayGetP(pArray, i);
    taosMemoryFreeClear(pVg->pData);
    taosMemoryFreeClear(pVg);
  }
  taosArrayDestroy(pArray);
}

static void destroyLogicNode(SLogicNode* pNode) {
  nodesDestroyList(pNode->pChildren);
  nodesDestroyNode(pNode->pConditions);
  nodesDestroyList(pNode->pTargets);
}

static void destroyPhysiNode(SPhysiNode* pNode) {
  nodesDestroyList(pNode->pChildren);
  nodesDestroyNode(pNode->pConditions);
  nodesDestroyNode((SNode*)pNode->pOutputDataBlockDesc);
}

static void destroyWinodwPhysiNode(SWinodwPhysiNode* pNode) {
  destroyPhysiNode((SPhysiNode*)pNode);
  nodesDestroyList(pNode->pExprs);
  nodesDestroyList(pNode->pFuncs);
  nodesDestroyNode(pNode->pTspk);
}

static void destroyScanPhysiNode(SScanPhysiNode* pNode) {
  destroyPhysiNode((SPhysiNode*)pNode);
  nodesDestroyList(pNode->pScanCols);
}

static void destroyDataSinkNode(SDataSinkNode* pNode) { nodesDestroyNode((SNode*)pNode->pInputDataBlockDesc); }

void nodesDestroyNode(SNode* pNode) {
  if (NULL == pNode) {
    return;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:  // pProjectRef is weak reference, no need to release
      break;
    case QUERY_NODE_VALUE: {
      SValueNode* pValue = (SValueNode*)pNode;
      taosMemoryFreeClear(pValue->literal);
      if (IS_VAR_DATA_TYPE(pValue->node.resType.type)) {
        taosMemoryFreeClear(pValue->datum.p);
      }
      break;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)pNode;
      nodesDestroyNode(pOp->pLeft);
      nodesDestroyNode(pOp->pRight);
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
      nodesDestroyList(((SLogicConditionNode*)pNode)->pParameterList);
      break;
    case QUERY_NODE_FUNCTION:
      nodesDestroyList(((SFunctionNode*)pNode)->pParameterList);
      break;
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pReal = (SRealTableNode*)pNode;
      taosMemoryFreeClear(pReal->pMeta);
      taosMemoryFreeClear(pReal->pVgroupList);
      break;
    }
    case QUERY_NODE_TEMP_TABLE:
      nodesDestroyNode(((STempTableNode*)pNode)->pSubquery);
      break;
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoin = (SJoinTableNode*)pNode;
      nodesDestroyNode(pJoin->pLeft);
      nodesDestroyNode(pJoin->pRight);
      nodesDestroyNode(pJoin->pOnCond);
      break;
    }
    case QUERY_NODE_GROUPING_SET:
      nodesDestroyList(((SGroupingSetNode*)pNode)->pParameterList);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      nodesDestroyNode(((SOrderByExprNode*)pNode)->pExpr);
      break;
    case QUERY_NODE_LIMIT:  // no pointer field
      break;
    case QUERY_NODE_STATE_WINDOW:
      nodesDestroyNode(((SStateWindowNode*)pNode)->pExpr);
      break;
    case QUERY_NODE_SESSION_WINDOW: {
      SSessionWindowNode* pSession = (SSessionWindowNode*)pNode;
      nodesDestroyNode((SNode*)pSession->pCol);
      nodesDestroyNode((SNode*)pSession->pGap);
      break;
    }
    case QUERY_NODE_INTERVAL_WINDOW: {
      SIntervalWindowNode* pJoin = (SIntervalWindowNode*)pNode;
      nodesDestroyNode(pJoin->pCol);
      nodesDestroyNode(pJoin->pInterval);
      nodesDestroyNode(pJoin->pOffset);
      nodesDestroyNode(pJoin->pSliding);
      nodesDestroyNode(pJoin->pFill);
      break;
    }
    case QUERY_NODE_NODE_LIST:
      nodesDestroyList(((SNodeListNode*)pNode)->pNodeList);
      break;
    case QUERY_NODE_FILL: {
      SFillNode* pFill = (SFillNode*)pNode;
      nodesDestroyNode(pFill->pValues);
      nodesDestroyNode(pFill->pWStartTs);
      break;
    }
    case QUERY_NODE_RAW_EXPR:
      nodesDestroyNode(((SRawExprNode*)pNode)->pNode);
      break;
    case QUERY_NODE_TARGET:
      nodesDestroyNode(((STargetNode*)pNode)->pExpr);
      break;
    case QUERY_NODE_DATABLOCK_DESC:
      nodesDestroyList(((SDataBlockDescNode*)pNode)->pSlots);
      break;
    case QUERY_NODE_SLOT_DESC:          // no pointer field
    case QUERY_NODE_COLUMN_DEF:         // no pointer field
    case QUERY_NODE_DOWNSTREAM_SOURCE:  // no pointer field
      break;
    case QUERY_NODE_DATABASE_OPTIONS: {
      SDatabaseOptions* pOptions = (SDatabaseOptions*)pNode;
      nodesDestroyNode((SNode*)pOptions->pDaysPerFile);
      nodesDestroyList(pOptions->pKeep);
      nodesDestroyList(pOptions->pRetentions);
      break;
    }
    case QUERY_NODE_TABLE_OPTIONS: {
      STableOptions* pOptions = (STableOptions*)pNode;
      nodesDestroyList(pOptions->pSma);
      nodesDestroyList(pOptions->pRollupFuncs);
      break;
    }
    case QUERY_NODE_INDEX_OPTIONS: {
      SIndexOptions* pOptions = (SIndexOptions*)pNode;
      nodesDestroyList(pOptions->pFuncs);
      nodesDestroyNode(pOptions->pInterval);
      nodesDestroyNode(pOptions->pOffset);
      nodesDestroyNode(pOptions->pSliding);
      break;
    }
    case QUERY_NODE_EXPLAIN_OPTIONS:  // no pointer field
      break;
    case QUERY_NODE_STREAM_OPTIONS:
      nodesDestroyNode(((SStreamOptions*)pNode)->pWatermark);
      break;
    case QUERY_NODE_LEFT_VALUE:  // no pointer field
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pStmt = (SSetOperator*)pNode;
      nodesDestroyNode(pStmt->pLeft);
      nodesDestroyNode(pStmt->pRight);
      nodesDestroyList(pStmt->pOrderByList);
      nodesDestroyNode(pStmt->pLimit);
      break;
    }
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pStmt = (SSelectStmt*)pNode;
      nodesDestroyList(pStmt->pProjectionList);
      nodesDestroyNode(pStmt->pFromTable);
      nodesDestroyNode(pStmt->pWhere);
      nodesDestroyList(pStmt->pPartitionByList);
      nodesDestroyNode(pStmt->pWindow);
      nodesDestroyList(pStmt->pGroupByList);
      nodesDestroyNode(pStmt->pHaving);
      nodesDestroyList(pStmt->pOrderByList);
      nodesDestroyNode((SNode*)pStmt->pLimit);
      nodesDestroyNode((SNode*)pStmt->pSlimit);
      break;
    }
    case QUERY_NODE_VNODE_MODIF_STMT:
      destroyVgDataBlockArray(((SVnodeModifOpStmt*)pNode)->pDataBlocks);
      break;
    case QUERY_NODE_CREATE_DATABASE_STMT:
      nodesDestroyNode((SNode*)((SCreateDatabaseStmt*)pNode)->pOptions);
      break;
    case QUERY_NODE_DROP_DATABASE_STMT:  // no pointer field
      break;
    case QUERY_NODE_ALTER_DATABASE_STMT:
      nodesDestroyNode((SNode*)((SAlterDatabaseStmt*)pNode)->pOptions);
      break;
    case QUERY_NODE_CREATE_TABLE_STMT: {
      SCreateTableStmt* pStmt = (SCreateTableStmt*)pNode;
      nodesDestroyList(pStmt->pCols);
      nodesDestroyList(pStmt->pTags);
      nodesDestroyNode((SNode*)pStmt->pOptions);
      break;
    }
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE: {
      SCreateSubTableClause* pStmt = (SCreateSubTableClause*)pNode;
      nodesDestroyList(pStmt->pSpecificTags);
      nodesDestroyList(pStmt->pValsOfTags);
      break;
    }
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      nodesDestroyList(((SCreateMultiTableStmt*)pNode)->pSubTables);
      break;
    case QUERY_NODE_DROP_TABLE_CLAUSE:  // no pointer field
      break;
    case QUERY_NODE_DROP_TABLE_STMT:
      nodesDestroyList(((SDropTableStmt*)pNode)->pTables);
      break;
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:  // no pointer field
      break;
    case QUERY_NODE_ALTER_TABLE_STMT: {
      SAlterTableStmt* pStmt = (SAlterTableStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyNode((SNode*)pStmt->pVal);
      break;
    }
    case QUERY_NODE_CREATE_USER_STMT:   // no pointer field
    case QUERY_NODE_ALTER_USER_STMT:    // no pointer field
    case QUERY_NODE_DROP_USER_STMT:     // no pointer field
    case QUERY_NODE_USE_DATABASE_STMT:  // no pointer field
    case QUERY_NODE_CREATE_DNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_DNODE_STMT:    // no pointer field
    case QUERY_NODE_ALTER_DNODE_STMT:   // no pointer field
      break;
    case QUERY_NODE_CREATE_INDEX_STMT: {
      SCreateIndexStmt* pStmt = (SCreateIndexStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyList(pStmt->pCols);
      break;
    }
    case QUERY_NODE_DROP_INDEX_STMT:    // no pointer field
    case QUERY_NODE_CREATE_QNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_QNODE_STMT:    // no pointer field
    case QUERY_NODE_CREATE_BNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_BNODE_STMT:    // no pointer field
    case QUERY_NODE_CREATE_SNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_SNODE_STMT:    // no pointer field
    case QUERY_NODE_CREATE_MNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_MNODE_STMT:    // no pointer field
      break;
    case QUERY_NODE_CREATE_TOPIC_STMT:
      nodesDestroyNode(((SCreateTopicStmt*)pNode)->pQuery);
      break;
    case QUERY_NODE_DROP_TOPIC_STMT:   // no pointer field
    case QUERY_NODE_DROP_CGROUP_STMT:  // no pointer field
    case QUERY_NODE_ALTER_LOCAL_STMT:  // no pointer field
      break;
    case QUERY_NODE_EXPLAIN_STMT: {
      SExplainStmt* pStmt = (SExplainStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyNode(pStmt->pQuery);
      break;
    }
    case QUERY_NODE_DESCRIBE_STMT:
      taosMemoryFree(((SDescribeStmt*)pNode)->pMeta);
      break;
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:  // no pointer field
    case QUERY_NODE_COMPACT_STMT:            // no pointer field
    case QUERY_NODE_CREATE_FUNCTION_STMT:    // no pointer field
    case QUERY_NODE_DROP_FUNCTION_STMT:      // no pointer field
      break;
    case QUERY_NODE_CREATE_STREAM_STMT: {
      SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyNode(pStmt->pQuery);
      break;
    }
    case QUERY_NODE_DROP_STREAM_STMT:     // no pointer field
    case QUERY_NODE_BALANCE_VGROUP_STMT:  // no pointer field
    case QUERY_NODE_MERGE_VGROUP_STMT:    // no pointer field
      break;
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
      nodesDestroyList(((SRedistributeVgroupStmt*)pNode)->pDnodes);
      break;
    case QUERY_NODE_SPLIT_VGROUP_STMT:  // no pointer field
    case QUERY_NODE_SYNCDB_STMT:        // no pointer field
    case QUERY_NODE_GRANT_STMT:         // no pointer field
    case QUERY_NODE_REVOKE_STMT:        // no pointer field
      break;
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_LICENCE_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_CONSUMERS_STMT:
    case QUERY_NODE_SHOW_SUBSCRIBES_STMT:
    case QUERY_NODE_SHOW_SMAS_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_APPS_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_VARIABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT: {
      SShowStmt* pStmt = (SShowStmt*)pNode;
      nodesDestroyNode(pStmt->pDbName);
      nodesDestroyNode(pStmt->pTbNamePattern);
      break;
    }
    case QUERY_NODE_KILL_CONNECTION_STMT:   // no pointer field
    case QUERY_NODE_KILL_QUERY_STMT:        // no pointer field
    case QUERY_NODE_KILL_TRANSACTION_STMT:  // no pointer field
      break;
    case QUERY_NODE_DELETE_STMT: {
      SDeleteStmt* pStmt = (SDeleteStmt*)pNode;
      nodesDestroyNode(pStmt->pFromTable);
      nodesDestroyNode(pStmt->pWhere);
      nodesDestroyNode(pStmt->pCountFunc);
      nodesDestroyNode(pStmt->pTagIndexCond);
      break;
    }
    case QUERY_NODE_QUERY: {
      SQuery* pQuery = (SQuery*)pNode;
      nodesDestroyNode(pQuery->pRoot);
      taosMemoryFreeClear(pQuery->pResSchema);
      if (NULL != pQuery->pCmdMsg) {
        taosMemoryFreeClear(pQuery->pCmdMsg->pMsg);
        taosMemoryFreeClear(pQuery->pCmdMsg);
      }
      taosArrayDestroy(pQuery->pDbList);
      taosArrayDestroy(pQuery->pTableList);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      SScanLogicNode* pLogicNode = (SScanLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pScanCols);
      taosMemoryFreeClear(pLogicNode->pVgroupList);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      SJoinLogicNode* pLogicNode = (SJoinLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyNode(pLogicNode->pOnConditions);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      SAggLogicNode* pLogicNode = (SAggLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pAggFuncs);
      nodesDestroyList(pLogicNode->pGroupKeys);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PROJECT: {
      SProjectLogicNode* pLogicNode = (SProjectLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pProjections);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY: {
      SVnodeModifyLogicNode* pLogicNode = (SVnodeModifyLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      destroyVgDataBlockArray(pLogicNode->pDataBlocks);
      // pVgDataBlocks is weak reference
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      destroyLogicNode((SLogicNode*)pNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_MERGE: {
      SMergeLogicNode* pLogicNode = (SMergeLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pMergeKeys);
      nodesDestroyList(pLogicNode->pInputs);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pLogicNode = (SWindowLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pFuncs);
      nodesDestroyNode(pLogicNode->pTspk);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_FILL: {
      SFillLogicNode* pLogicNode = (SFillLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyNode(pLogicNode->pWStartTs);
      nodesDestroyNode(pLogicNode->pValues);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SORT: {
      SSortLogicNode* pLogicNode = (SSortLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pSortKeys);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      SPartitionLogicNode* pLogicNode = (SPartitionLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pPartitionKeys);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC: {
      SIndefRowsFuncLogicNode* pLogicNode = (SIndefRowsFuncLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pVectorFuncs);
      break;
    }
    case QUERY_NODE_LOGIC_SUBPLAN: {
      SLogicSubplan* pSubplan = (SLogicSubplan*)pNode;
      nodesDestroyList(pSubplan->pChildren);
      nodesDestroyNode((SNode*)pSubplan->pNode);
      nodesClearList(pSubplan->pParents);
      taosMemoryFreeClear(pSubplan->pVgroupList);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN:
      nodesDestroyList(((SQueryLogicPlan*)pNode)->pTopSubplans);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
      destroyScanPhysiNode((SScanPhysiNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT: {
      SProjectPhysiNode* pPhyNode = (SProjectPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pProjections);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN: {
      SJoinPhysiNode* pPhyNode = (SJoinPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyNode(pPhyNode->pOnConditions);
      nodesDestroyList(pPhyNode->pTargets);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG: {
      SAggPhysiNode* pPhyNode = (SAggPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pAggFuncs);
      nodesDestroyList(pPhyNode->pGroupKeys);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE: {
      SExchangePhysiNode* pPhyNode = (SExchangePhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pSrcEndPoints);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE: {
      SMergePhysiNode* pPhyNode = (SMergePhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pMergeKeys);
      nodesDestroyList(pPhyNode->pTargets);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT: {
      SSortPhysiNode* pPhyNode = (SSortPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pSortKeys);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
      destroyWinodwPhysiNode((SWinodwPhysiNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL: {
      SFillPhysiNode* pPhyNode = (SFillPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyNode(pPhyNode->pWStartTs);
      nodesDestroyNode(pPhyNode->pValues);
      nodesDestroyList(pPhyNode->pTargets);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      destroyWinodwPhysiNode((SWinodwPhysiNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE: {
      SStateWinodwPhysiNode* pPhyNode = (SStateWinodwPhysiNode*)pNode;
      destroyWinodwPhysiNode((SWinodwPhysiNode*)pPhyNode);
      nodesDestroyNode(pPhyNode->pStateKey);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION: {
      SPartitionPhysiNode* pPhyNode = (SPartitionPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pPartitionKeys);
      nodesDestroyList(pPhyNode->pTargets);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC: {
      SIndefRowsFuncPhysiNode* pPhyNode = (SIndefRowsFuncPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pVectorFuncs);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      destroyDataSinkNode((SDataSinkNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_INSERT: {
      SDataInserterNode* pSink = (SDataInserterNode*)pNode;
      destroyDataSinkNode((SDataSinkNode*)pSink);
      taosMemoryFreeClear(pSink->pData);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DELETE: {
      SDataDeleterNode* pSink = (SDataDeleterNode*)pNode;
      destroyDataSinkNode((SDataSinkNode*)pSink);
      nodesDestroyNode(pSink->pAffectedRows);
      break;
    }
    case QUERY_NODE_PHYSICAL_SUBPLAN: {
      SSubplan* pSubplan = (SSubplan*)pNode;
      nodesDestroyList(pSubplan->pChildren);
      nodesDestroyNode((SNode*)pSubplan->pNode);
      nodesDestroyNode((SNode*)pSubplan->pDataSink);
      nodesClearList(pSubplan->pParents);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN: {
      SQueryPlan* pPlan = (SQueryPlan*)pNode;
      if (NULL != pPlan->pSubplans) {
        // only need to destroy the top-level subplans, because they will recurse to all the subplans below
        bool   first = true;
        SNode* pElement = NULL;
        FOREACH(pElement, pPlan->pSubplans) {
          if (first) {
            first = false;
            nodesDestroyNode(pElement);
          } else {
            nodesClearList(((SNodeListNode*)pElement)->pNodeList);
            taosMemoryFreeClear(pElement);
          }
        }
        nodesClearList(pPlan->pSubplans);
      }
      break;
    }
    default:
      break;
  }
  taosMemoryFreeClear(pNode);
  return;
}

SNodeList* nodesMakeList() {
  SNodeList* p = taosMemoryCalloc(1, sizeof(SNodeList));
  if (NULL == p) {
    return NULL;
  }
  return p;
}

int32_t nodesListAppend(SNodeList* pList, SNode* pNode) {
  if (NULL == pList || NULL == pNode) {
    return TSDB_CODE_FAILED;
  }
  SListCell* p = taosMemoryCalloc(1, sizeof(SListCell));
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  p->pNode = pNode;
  if (NULL == pList->pHead) {
    pList->pHead = p;
  }
  if (NULL != pList->pTail) {
    pList->pTail->pNext = p;
  }
  p->pPrev = pList->pTail;
  pList->pTail = p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}

int32_t nodesListStrictAppend(SNodeList* pList, SNode* pNode) {
  if (NULL == pNode) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = nodesListAppend(pList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
  }
  return code;
}

int32_t nodesListMakeAppend(SNodeList** pList, SNode* pNode) {
  if (NULL == *pList) {
    *pList = nodesMakeList();
    if (NULL == *pList) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return nodesListAppend(*pList, pNode);
}

int32_t nodesListMakeStrictAppend(SNodeList** pList, SNode* pNode) {
  if (NULL == *pList) {
    *pList = nodesMakeList();
    if (NULL == *pList) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return nodesListStrictAppend(*pList, pNode);
}

int32_t nodesListAppendList(SNodeList* pTarget, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pSrc) {
    return TSDB_CODE_FAILED;
  }

  if (NULL == pTarget->pHead) {
    pTarget->pHead = pSrc->pHead;
  } else {
    pTarget->pTail->pNext = pSrc->pHead;
    if (NULL != pSrc->pHead) {
      pSrc->pHead->pPrev = pTarget->pTail;
    }
  }
  pTarget->pTail = pSrc->pTail;
  pTarget->length += pSrc->length;
  taosMemoryFreeClear(pSrc);

  return TSDB_CODE_SUCCESS;
}

int32_t nodesListStrictAppendList(SNodeList* pTarget, SNodeList* pSrc) {
  if (NULL == pSrc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = nodesListAppendList(pTarget, pSrc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pSrc);
  }
  return code;
}

int32_t nodesListPushFront(SNodeList* pList, SNode* pNode) {
  if (NULL == pList || NULL == pNode) {
    return TSDB_CODE_FAILED;
  }
  SListCell* p = taosMemoryCalloc(1, sizeof(SListCell));
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  p->pNode = pNode;
  if (NULL != pList->pHead) {
    pList->pHead->pPrev = p;
    p->pNext = pList->pHead;
  }
  pList->pHead = p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}

SListCell* nodesListErase(SNodeList* pList, SListCell* pCell) {
  if (NULL == pCell->pPrev) {
    pList->pHead = pCell->pNext;
  } else {
    pCell->pPrev->pNext = pCell->pNext;
  }
  if (NULL == pCell->pNext) {
    pList->pTail = pCell->pPrev;
  } else {
    pCell->pNext->pPrev = pCell->pPrev;
  }
  SListCell* pNext = pCell->pNext;
  nodesDestroyNode(pCell->pNode);
  taosMemoryFreeClear(pCell);
  --(pList->length);
  return pNext;
}

void nodesListInsertList(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pPos || NULL == pSrc) {
    return;
  }

  if (NULL == pPos->pPrev) {
    pTarget->pHead = pSrc->pHead;
  } else {
    pPos->pPrev->pNext = pSrc->pHead;
  }
  pSrc->pHead->pPrev = pPos->pPrev;
  pSrc->pTail->pNext = pPos;
  pPos->pPrev = pSrc->pTail;

  pTarget->length += pSrc->length;
  taosMemoryFreeClear(pSrc);
}

SNode* nodesListGetNode(SNodeList* pList, int32_t index) {
  SNode* node;
  FOREACH(node, pList) {
    if (0 == index--) {
      return node;
    }
  }
  return NULL;
}

void nodesDestroyList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    pNext = nodesListErase(pList, pNext);
  }
  taosMemoryFreeClear(pList);
}

void nodesClearList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    SListCell* tmp = pNext;
    pNext = pNext->pNext;
    taosMemoryFreeClear(tmp);
  }
  taosMemoryFreeClear(pList);
}

void* nodesGetValueFromNode(SValueNode* pNode) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return (void*)&pNode->typeData;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      return (void*)pNode->datum.p;
    default:
      break;
  }

  return NULL;
}

int32_t nodesSetValueNodeValue(SValueNode* pNode, void* value) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
      pNode->datum.b = *(bool*)value;
      *(bool*)&pNode->typeData = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pNode->datum.i = *(int8_t*)value;
      *(int8_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pNode->datum.i = *(int16_t*)value;
      *(int16_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_INT:
      pNode->datum.i = *(int32_t*)value;
      *(int32_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pNode->datum.i = *(int64_t*)value;
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      pNode->datum.i = *(int64_t*)value;
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      pNode->datum.u = *(int8_t*)value;
      *(int8_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      pNode->datum.u = *(int16_t*)value;
      *(int16_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UINT:
      pNode->datum.u = *(int32_t*)value;
      *(int32_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      pNode->datum.u = *(uint64_t*)value;
      *(uint64_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pNode->datum.d = *(float*)value;
      *(float*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pNode->datum.d = *(double*)value;
      *(double*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      pNode->datum.p = (char*)value;
      break;
    default:
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

char* nodesGetStrValueFromNode(SValueNode* pNode) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%s", pNode->datum.b ? "true" : "false");
      return buf;
    }
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%" PRId64, pNode->datum.i);
      return buf;
    }
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%" PRIu64, pNode->datum.u);
      return buf;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%e", pNode->datum.d);
      return buf;
    }
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY: {
      int32_t bufSize = varDataLen(pNode->datum.p) + 2 + 1;
      void*   buf = taosMemoryMalloc(bufSize);
      if (NULL == buf) {
        return NULL;
      }

      snprintf(buf, bufSize, "'%s'", varDataVal(pNode->datum.p));
      return buf;
    }
    default:
      break;
  }

  return NULL;
}

bool nodesIsExprNode(const SNode* pNode) {
  ENodeType type = nodeType(pNode);
  return (QUERY_NODE_COLUMN == type || QUERY_NODE_VALUE == type || QUERY_NODE_OPERATOR == type ||
          QUERY_NODE_FUNCTION == type || QUERY_NODE_LOGIC_CONDITION == type);
}

bool nodesIsUnaryOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_MINUS:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsArithmeticOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
    case OP_TYPE_MULTI:
    case OP_TYPE_DIV:
    case OP_TYPE_REM:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsComparisonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
    case OP_TYPE_JSON_CONTAINS:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsJsonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_JSON_GET_VALUE:
    case OP_TYPE_JSON_CONTAINS:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsRegularOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
      return true;
    default:
      break;
  }
  return false;
}

typedef struct SCollectColumnsCxt {
  int32_t         errCode;
  const char*     pTableAlias;
  ECollectColType collectType;
  SNodeList*      pCols;
  SHashObj*       pColHash;
} SCollectColumnsCxt;

static EDealRes doCollect(SCollectColumnsCxt* pCxt, SColumnNode* pCol, SNode* pNode) {
  char    name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  int32_t len = 0;
  if ('\0' == pCol->tableAlias[0]) {
    len = sprintf(name, "%s", pCol->colName);
  }
  len = sprintf(name, "%s.%s", pCol->tableAlias, pCol->colName);
  if (NULL == taosHashGet(pCxt->pColHash, name, len)) {
    pCxt->errCode = taosHashPut(pCxt->pColHash, name, len, NULL, 0);
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pCxt->errCode = nodesListStrictAppend(pCxt->pCols, nodesCloneNode(pNode));
    }
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

static bool isCollectType(ECollectColType collectType, EColumnType colType) {
  return COLLECT_COL_TYPE_ALL == collectType
             ? true
             : (COLLECT_COL_TYPE_TAG == collectType ? COLUMN_TYPE_TAG == colType : COLUMN_TYPE_TAG != colType);
}

static EDealRes collectColumns(SNode* pNode, void* pContext) {
  SCollectColumnsCxt* pCxt = (SCollectColumnsCxt*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (isCollectType(pCxt->collectType, pCol->colType) &&
        (NULL == pCxt->pTableAlias || 0 == strcmp(pCxt->pTableAlias, pCol->tableAlias))) {
      return doCollect(pCxt, pCol, pNode);
    }
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectColumns(SSelectStmt* pSelect, ESqlClause clause, const char* pTableAlias, ECollectColType type,
                            SNodeList** pCols) {
  if (NULL == pSelect || NULL == pCols) {
    return TSDB_CODE_FAILED;
  }

  SCollectColumnsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pTableAlias = pTableAlias,
      .collectType = type,
      .pCols = (NULL == *pCols ? nodesMakeList() : *pCols),
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pCols || NULL == cxt.pColHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pCols = NULL;
  nodesWalkSelectStmt(pSelect, clause, collectColumns, &cxt);
  taosHashCleanup(cxt.pColHash);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pCols);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pCols) > 0) {
    *pCols = cxt.pCols;
  } else {
    nodesDestroyList(cxt.pCols);
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SCollectFuncsCxt {
  int32_t         errCode;
  FFuncClassifier classifier;
  SNodeList*      pFuncs;
} SCollectFuncsCxt;

static EDealRes collectFuncs(SNode* pNode, void* pContext) {
  SCollectFuncsCxt* pCxt = (SCollectFuncsCxt*)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && pCxt->classifier(((SFunctionNode*)pNode)->funcId)) {
    pCxt->errCode = nodesListStrictAppend(pCxt->pFuncs, nodesCloneNode(pNode));
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectFuncs(SSelectStmt* pSelect, ESqlClause clause, FFuncClassifier classifier, SNodeList** pFuncs) {
  if (NULL == pSelect || NULL == pFuncs) {
    return TSDB_CODE_FAILED;
  }

  SCollectFuncsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS, .classifier = classifier, .pFuncs = (NULL == *pFuncs ? nodesMakeList() : *pFuncs)};
  if (NULL == cxt.pFuncs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pFuncs = NULL;
  nodesWalkSelectStmt(pSelect, clause, collectFuncs, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pFuncs);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pFuncs) > 0) {
    *pFuncs = cxt.pFuncs;
  } else {
    nodesDestroyList(cxt.pFuncs);
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SCollectSpecialNodesCxt {
  int32_t    errCode;
  ENodeType  type;
  SNodeList* pNodes;
} SCollectSpecialNodesCxt;

static EDealRes collectSpecialNodes(SNode* pNode, void* pContext) {
  SCollectSpecialNodesCxt* pCxt = (SCollectSpecialNodesCxt*)pContext;
  if (pCxt->type == nodeType(pNode)) {
    pCxt->errCode = nodesListStrictAppend(pCxt->pNodes, nodesCloneNode(pNode));
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectSpecialNodes(SSelectStmt* pSelect, ESqlClause clause, ENodeType type, SNodeList** pNodes) {
  if (NULL == pSelect || NULL == pNodes) {
    return TSDB_CODE_FAILED;
  }

  SCollectSpecialNodesCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS, .type = type, .pNodes = (NULL == *pNodes ? nodesMakeList() : *pNodes)};
  if (NULL == cxt.pNodes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pNodes = NULL;
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_GROUP_BY, collectSpecialNodes, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pNodes);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pNodes) > 0) {
    *pNodes = cxt.pNodes;
  } else {
    nodesDestroyList(cxt.pNodes);
  }

  return TSDB_CODE_SUCCESS;
}

static EDealRes hasColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

bool nodesExprHasColumn(SNode* pNode) {
  bool hasCol = false;
  nodesWalkExprPostOrder(pNode, hasColumn, &hasCol);
  return hasCol;
}

bool nodesExprsHasColumn(SNodeList* pList) {
  bool hasCol = false;
  nodesWalkExprsPostOrder(pList, hasColumn, &hasCol);
  return hasCol;
}

char* nodesGetFillModeString(EFillMode mode) {
  switch (mode) {
    case FILL_MODE_NONE:
      return "none";
    case FILL_MODE_VALUE:
      return "value";
    case FILL_MODE_PREV:
      return "prev";
    case FILL_MODE_NULL:
      return "null";
    case FILL_MODE_LINEAR:
      return "linear";
    case FILL_MODE_NEXT:
      return "next";
    default:
      return "unknown";
  }
}

char* nodesGetNameFromColumnNode(SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_COLUMN != pNode->type) {
    return "NULL";
  }

  return ((SColumnNode*)pNode)->colName;
}

int32_t nodesGetOutputNumFromSlotList(SNodeList* pSlots) {
  if (NULL == pSlots || pSlots->length <= 0) {
    return 0;
  }

  SNode*  pNode = NULL;
  int32_t num = 0;
  FOREACH(pNode, pSlots) {
    if (QUERY_NODE_SLOT_DESC != pNode->type) {
      continue;
    }

    SSlotDescNode* descNode = (SSlotDescNode*)pNode;
    if (descNode->output) {
      ++num;
    }
  }

  return num;
}

void nodesValueNodeToVariant(const SValueNode* pNode, SVariant* pVal) {
  pVal->nType = pNode->node.resType.type;
  pVal->nLen = pNode->node.resType.bytes;
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      pVal->i = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      pVal->i = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      pVal->u = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      pVal->d = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      pVal->pz = taosMemoryMalloc(pVal->nLen + VARSTR_HEADER_SIZE + 1);
      memcpy(pVal->pz, pNode->datum.p, pVal->nLen + VARSTR_HEADER_SIZE);
      pVal->pz[pVal->nLen + VARSTR_HEADER_SIZE] = 0;
      break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }
}

int32_t nodesMergeConds(SNode** pDst, SNodeList** pSrc) {
  if (NULL == *pSrc) {
    return TSDB_CODE_SUCCESS;
  }

  if (1 == LIST_LENGTH(*pSrc)) {
    *pDst = nodesListGetNode(*pSrc, 0);
    nodesClearList(*pSrc);
  } else {
    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (NULL == pLogicCond) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
    pLogicCond->pParameterList = *pSrc;
    *pDst = (SNode*)pLogicCond;
  }
  *pSrc = NULL;

  return TSDB_CODE_SUCCESS;
}

typedef struct SClassifyConditionCxt {
  bool hasPrimaryKey;
  bool hasTagIndexCol;
  bool hasOtherCol;
} SClassifyConditionCxt;

static EDealRes classifyConditionImpl(SNode* pNode, void* pContext) {
  SClassifyConditionCxt* pCxt = (SClassifyConditionCxt*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
      pCxt->hasPrimaryKey = true;
    } else if (pCol->hasIndex) {
      pCxt->hasTagIndexCol = true;
    } else {
      pCxt->hasOtherCol = true;
    }
    return *((bool*)pContext) ? DEAL_RES_CONTINUE : DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

typedef enum EConditionType { COND_TYPE_PRIMARY_KEY = 1, COND_TYPE_TAG_INDEX, COND_TYPE_NORMAL } EConditionType;

static EConditionType classifyCondition(SNode* pNode) {
  SClassifyConditionCxt cxt = {.hasPrimaryKey = false, .hasTagIndexCol = false, .hasOtherCol = false};
  nodesWalkExpr(pNode, classifyConditionImpl, &cxt);
  return cxt.hasOtherCol ? COND_TYPE_NORMAL
                         : (cxt.hasPrimaryKey && cxt.hasTagIndexCol
                                ? COND_TYPE_NORMAL
                                : (cxt.hasPrimaryKey ? COND_TYPE_PRIMARY_KEY : COND_TYPE_TAG_INDEX));
}

static int32_t partitionLogicCond(SNode** pCondition, SNode** pPrimaryKeyCond, SNode** pTagCond, SNode** pOtherCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(*pCondition);

  int32_t code = TSDB_CODE_SUCCESS;

  SNodeList* pPrimaryKeyConds = NULL;
  SNodeList* pTagConds = NULL;
  SNodeList* pOtherConds = NULL;
  SNode*     pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    switch (classifyCondition(pCond)) {
      case COND_TYPE_PRIMARY_KEY:
        if (NULL != pPrimaryKeyCond) {
          code = nodesListMakeAppend(&pPrimaryKeyConds, nodesCloneNode(pCond));
        }
        break;
      case COND_TYPE_TAG_INDEX:
        if (NULL != pTagCond) {
          code = nodesListMakeAppend(&pTagConds, nodesCloneNode(pCond));
        }
        break;
      case COND_TYPE_NORMAL:
      default:
        if (NULL != pOtherCond) {
          code = nodesListMakeAppend(&pOtherConds, nodesCloneNode(pCond));
        }
        break;
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  SNode* pTempPrimaryKeyCond = NULL;
  SNode* pTempTagCond = NULL;
  SNode* pTempOtherCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempPrimaryKeyCond, &pPrimaryKeyConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempTagCond, &pTagConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempOtherCond, &pOtherConds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pPrimaryKeyCond) {
      *pPrimaryKeyCond = pTempPrimaryKeyCond;
    }
    if (NULL != pTagCond) {
      *pTagCond = pTempTagCond;
    }
    if (NULL != pOtherCond) {
      *pOtherCond = pTempOtherCond;
    }
    nodesDestroyNode(*pCondition);
    *pCondition = NULL;
  } else {
    nodesDestroyList(pPrimaryKeyConds);
    nodesDestroyList(pTagConds);
    nodesDestroyList(pOtherConds);
    nodesDestroyNode(pTempPrimaryKeyCond);
    nodesDestroyNode(pTempTagCond);
    nodesDestroyNode(pTempOtherCond);
  }

  return code;
}

int32_t nodesPartitionCond(SNode** pCondition, SNode** pPrimaryKeyCond, SNode** pTagCond, SNode** pOtherCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pCondition) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)*pCondition)->condType) {
    return partitionLogicCond(pCondition, pPrimaryKeyCond, pTagCond, pOtherCond);
  }

  switch (classifyCondition(*pCondition)) {
    case COND_TYPE_PRIMARY_KEY:
      if (NULL != pPrimaryKeyCond) {
        *pPrimaryKeyCond = *pCondition;
      }
      break;
    case COND_TYPE_TAG_INDEX:
      if (NULL != pTagCond) {
        *pTagCond = *pCondition;
      }
      break;
    case COND_TYPE_NORMAL:
    default:
      if (NULL != pOtherCond) {
        *pOtherCond = *pCondition;
      }
      break;
  }
  *pCondition = NULL;

  return TSDB_CODE_SUCCESS;
}
