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

#include "queryInt.h"
#include "query.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"

void qFreeExplainRes(SExplainResNode *res) {

}

char *qFillModeString(EFillMode mode) {
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

char *qGetNameFromColumnNode(SNode *pNode) {
  if (NULL == pNode || QUERY_NODE_COLUMN != pNode->type) {
    return "NULL";
  }
  
  return ((SColumnNode *)pNode)->colName;
}

int32_t qMakeExplainResChildrenInfo(SPhysiNode *pNode, void *pExecInfo, SNodeList **pChildren) {
  int32_t tlen = 0;
  SNodeList *pPhysiChildren = NULL;
  
  switch (pNode->type) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN: {
      STagScanPhysiNode *pTagScanNode = (STagScanPhysiNode *)pNode;
      pPhysiChildren = pTagScanNode->node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:{
      STableScanPhysiNode *pTblScanNode = (STableScanPhysiNode *)pNode;
      pPhysiChildren = pTblScanNode->scan.node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:{
      SSystemTableScanPhysiNode *pSTblScanNode = (SSystemTableScanPhysiNode *)pNode;
      pPhysiChildren = pSTblScanNode->scan.node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:{
      SProjectPhysiNode *pPrjNode = (SProjectPhysiNode *)pNode;
      pPhysiChildren = pPrjNode->node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:{
      SJoinPhysiNode *pJoinNode = (SJoinPhysiNode *)pNode;
      pPhysiChildren = pJoinNode->node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_AGG:{
      SAggPhysiNode *pAggNode = (SAggPhysiNode *)pNode;
      pPhysiChildren = pAggNode->node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:{
      SExchangePhysiNode *pExchNode = (SExchangePhysiNode *)pNode;
      pPhysiChildren = pExchNode->node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT:{
      SSortPhysiNode *pSortNode = (SSortPhysiNode *)pNode;
      pPhysiChildren = pSortNode->node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INTERVAL:{
      SIntervalPhysiNode *pIntNode = (SIntervalPhysiNode *)pNode;
      pPhysiChildren = pIntNode->window.node.pChildren;
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW:{
      SSessionWinodwPhysiNode *pSessNode = (SSessionWinodwPhysiNode *)pNode;
      pPhysiChildren = pSessNode->window.node.pChildren;
      break;
    }
    default:
      qError("not supported physical node type %d", pNode->type);
      QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (pPhysiChildren) {
    *pChildren = nodesMakeList();
    if (NULL == *pChildren) {
      qError("nodesMakeList failed");
      QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  SNode* node = NULL;
  SExplainResNode *pResNode = NULL;
  FOREACH(node, pPhysiChildren) {
    QRY_ERR_RET(qMakeExplainResNode((SPhysiNode *)node, pExecInfo, &pResNode));
    QRY_ERR_RET(nodesListAppend(*pChildren, pResNode));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qMakeExplainResNode(SPhysiNode *pNode, void *pExecInfo, SExplainResNode **pRes) {
  if (NULL == pNode) {
    *pRes = NULL;
    qError("physical node is NULL");
    return TSDB_CODE_QRY_APP_ERROR;
  }

  SExplainResNode *res = taosMemoryCalloc(1, sizeof(SExplainResNode));
  if (NULL == res) {
    qError("calloc SPhysiNodeExplainRes failed");
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  int32_t code = 0;
  res->pNode = pNode;
  res->pExecInfo = pExecInfo;
  QRY_ERR_JRET(qMakeExplainResChildrenInfo(pNode, pExecInfo, &res->pChildren));
  
  *pRes = res;

  return TSDB_CODE_SUCCESS;

_return:

  qFreeExplainRes(res);
  
  QRY_RET(code);
}

int32_t qMakeTaskExplainResTree(struct SSubplan *plan, void *pExecTree, SExplainResNode **pRes) {
  char *tbuf = taosMemoryMalloc(QUERY_EXPLAIN_MAX_RES_LEN);
  if (NULL == tbuf) {
    qError("malloc size %d failed", QUERY_EXPLAIN_MAX_RES_LEN);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  void *pExecInfo = NULL; // TODO
  int32_t code = qMakeExplainResNode(plan->pNode, pExecInfo, pRes);

  taosMemoryFree(tbuf);

  QRY_RET(code);
}

int32_t qExplainBufAppendExecInfo(void *pExecInfo, char *tbuf, int32_t tlen) {

}

int32_t qExplainResAppendRow(SArray *pRows, char *tbuf, int32_t len, int32_t level) {
  SQueryExplainRowInfo row = {0};
  row.buf = strdup(tbuf);
  if (NULL == row.buf) {
    qError("strdup %s failed", tbuf);
    QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  row.level = level;
  row.len = len;

  if (taosArrayPush(pRows, &row)) {
    qError("taosArrayPush row to explain res rows failed");
    taosMemoryFree(row.buf);
    QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qExplainResNodeToRowsImpl(SExplainResNode *pResNode, SArray *pRows, char *tbuf, int32_t level) {
  int32_t tlen = 0;
  SPhysiNode* pNode = pResNode->pNode;
  if (NULL == pNode) {
    qError("pyhsical node in explain res node is NULL");
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  switch (pNode->type) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN: {
      STagScanPhysiNode *pTagScanNode = (STagScanPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_TAG_SCAN_FORMAT, pTagScanNode->tableName.tname, pTagScanNode->pScanCols->length);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf));
      }
      QUERY_EXPLAIN_APPEND(EXPLAIN_LOOPS_FORMAT, pTagScanNode->count);
      if (pTagScanNode->reverse) {
        QUERY_EXPLAIN_APPEND(EXPLAIN_REVERSE_FORMAT, pTagScanNode->reverse);
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));
      
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pTagScanNode->order));
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:{
      STableScanPhysiNode *pTblScanNode = (STableScanPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_TBL_SCAN_FORMAT, pTblScanNode->scan.tableName.tname, pTblScanNode->scan.pScanCols->length);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QUERY_EXPLAIN_APPEND(EXPLAIN_LOOPS_FORMAT, pTblScanNode->scan.count);
      if (pTblScanNode->scan.reverse) {
        QUERY_EXPLAIN_APPEND(EXPLAIN_REVERSE_FORMAT, pTblScanNode->scan.reverse);
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));
      
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pTblScanNode->scan.order));
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_TIMERANGE_FORMAT, pTblScanNode->scanRange.skey, pTblScanNode->scanRange.ekey);
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));

      if (pTblScanNode->pScanConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);
        QRY_ERR_RET(nodesNodeToSQL(pTblScanNode->pScanConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));        
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:{
      SSystemTableScanPhysiNode *pSTblScanNode = (SSystemTableScanPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_SYSTBL_SCAN_FORMAT, pSTblScanNode->scan.tableName.tname, pSTblScanNode->scan.pScanCols->length);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QUERY_EXPLAIN_APPEND(EXPLAIN_LOOPS_FORMAT, pSTblScanNode->scan.count);
      if (pSTblScanNode->scan.reverse) {
        QUERY_EXPLAIN_APPEND(EXPLAIN_REVERSE_FORMAT, pSTblScanNode->scan.reverse);
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));
      
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pSTblScanNode->scan.order));
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));      
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:{
      SProjectPhysiNode *pPrjNode = (SProjectPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_PROJECTION_FORMAT, pPrjNode->pProjections->length, pPrjNode->node.pOutputDataBlockDesc->resultRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));

      if (pPrjNode->node.pConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pPrjNode->node.pConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));        
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:{
      SJoinPhysiNode *pJoinNode = (SJoinPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_JOIN_FORMAT, EXPLAIN_JOIN_STRING(pJoinNode->joinType), pJoinNode->pTargets->length, pJoinNode->node.pOutputDataBlockDesc->resultRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));

      if (pJoinNode->node.pConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pJoinNode->node.pConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));                
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_ON_CONDITIONS_FORMAT);      
      QRY_ERR_RET(nodesNodeToSQL(pJoinNode->pOnConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));              
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_AGG:{
      SAggPhysiNode *pAggNode = (SAggPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_AGG_FORMAT, pAggNode->pAggFuncs->length, pAggNode->pGroupKeys->length, pAggNode->node.pOutputDataBlockDesc->resultRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));

      if (pAggNode->node.pConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pAggNode->node.pConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));                
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:{
      SExchangePhysiNode *pExchNode = (SExchangePhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_EXCHANGE_FORMAT, pExchNode->pSrcEndPoints->length, pExchNode->node.pOutputDataBlockDesc->resultRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));

      if (pExchNode->node.pConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pExchNode->node.pConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));                
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT:{
      SSortPhysiNode *pSortNode = (SSortPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_SORT_FORMAT, pSortNode->pSortKeys->length, pSortNode->node.pOutputDataBlockDesc->resultRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));

      if (pSortNode->node.pConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pSortNode->node.pConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));                
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INTERVAL:{
      SIntervalPhysiNode *pIntNode = (SIntervalPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_INTERVAL_FORMAT, qGetNameFromColumnNode(pIntNode->pTspk), pIntNode->window.pFuncs->length, 
        pIntNode->interval, pIntNode->intervalUnit, pIntNode->offset, pIntNode->intervalUnit, pIntNode->sliding, pIntNode->slidingUnit, pIntNode->window.node.pOutputDataBlockDesc->resultRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));

      if (pIntNode->pFill) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILL_FORMAT, qFillModeString(pIntNode->pFill->mode));      
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }

      if (pIntNode->window.node.pConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));                
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW:{
      SSessionWinodwPhysiNode *pIntNode = (SSessionWinodwPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_SESSION_FORMAT, pIntNode->gap, pIntNode->window.pFuncs->length, pIntNode->window.node.pOutputDataBlockDesc->resultRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, tlen));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));

      if (pIntNode->window.node.pConditions) {
        QUERY_EXPLAIN_NEWLINE(EXPLAIN_FILTER_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf, QUERY_EXPLAIN_MAX_RES_LEN, &tlen));                
        QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level + 1));
      }
      break;
    }
    default:
      qError("not supported physical node type %d", pNode->type);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qExplainResNodeToRows(SExplainResNode *pResNode, SArray *pRsp, char *tbuf, int32_t level) {
  if (NULL == pResNode) {
    qError("explain res node is NULL");
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  int32_t code = 0;
  QRY_ERR_RET(qExplainResNodeToRowsImpl(pResNode, pRsp, tbuf, level));

  SNode* pNode = NULL;
  FOREACH(pNode, pResNode->pChildren) {
    QRY_ERR_RET(qExplainResNodeToRows((SExplainResNode *)pNode, pRsp, tbuf, level + 1));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainRowsToRsp(SArray *rows, SRetrieveTableRsp **pRsp) {
  int32_t rspSize = sizeof(SRetrieveTableRsp) + ;
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, rspSize);
  if (NULL == rsp) {
    qError("malloc SRetrieveTableRsp failed");
    QRY_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
}

int32_t qMakeTaskExplainResRows(SExplainResNode *pResNode, SRetrieveTableRsp **pRsp) {
  if (NULL == pResNode) {
    qError("explain res node is NULL");
    QRY_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  int32_t code = 0;
  char *tbuf = taosMemoryMalloc(QUERY_EXPLAIN_MAX_RES_LEN);
  if (NULL == tbuf) {
    qError("malloc size %d failed", QUERY_EXPLAIN_MAX_RES_LEN);
    QRY_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SArray *rows = taosArrayInit(10, sizeof(SQueryExplainRowInfo));
  if (NULL == rows) {
    qError("taosArrayInit SQueryExplainRowInfo failed");
    QRY_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  
  QRY_ERR_JRET(qExplainResNodeToRows(pResNode, rows, tbuf, 0));

  QRY_ERR_JRET(qExplainRowsToRsp(rows, pRsp));  

_return:

  taosMemoryFree(tbuf);
  taosArrayDestroy(rows);

  QRY_RET(code);
}


#pragma GCC diagnostic pop
