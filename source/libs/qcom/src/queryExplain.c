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
#include "plannodes.h"

int32_t qGenerateExplainResNode(SPhysiNode *pNode, void *pExecInfo, SExplainResNode **pRes);


void qFreeExplainResTree(SExplainResNode *res) {
  if (NULL == res) {
    return;
  }

  taosMemoryFreeClear(res->pExecInfo);

  SNode* node = NULL;
  FOREACH(node, res->pChildren) {
    qFreeExplainResTree((SExplainResNode *)node);
  }  
  nodesClearList(res->pChildren);
  
  taosMemoryFreeClear(res);
}

void qFreeExplainCtx(void *ctx) {
  if (NULL == ctx) {
    return;
  }

  SExplainCtx *pCtx = (SExplainCtx *)ctx;
  int32_t rowSize = taosArrayGetSize(pCtx->rows);
  for (int32_t i = 0; i < rowSize; ++i) {
    SQueryExplainRowInfo *row = taosArrayGet(pCtx->rows, i);
    taosMemoryFreeClear(row->buf);
  }

  taosHashCleanup(pCtx->groupHash);
  taosArrayDestroy(pCtx->rows);
  taosMemoryFree(pCtx);
}

int32_t qInitExplainCtx(void **pCtx, SHashObj *groupHash, bool verbose) {
  int32_t code = 0;
  SExplainCtx *ctx = taosMemoryCalloc(1, sizeof(SExplainCtx));
  if (NULL == ctx) {
    qError("calloc SExplainCtx failed");
    QRY_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  
  SArray *rows = taosArrayInit(10, sizeof(SQueryExplainRowInfo));
  if (NULL == rows) {
    qError("taosArrayInit SQueryExplainRowInfo failed");
    QRY_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  char *tbuf = taosMemoryMalloc(TSDB_EXPLAIN_RESULT_ROW_SIZE);
  if (NULL == tbuf) {
    qError("malloc size %d failed", TSDB_EXPLAIN_RESULT_ROW_SIZE);
    QRY_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  ctx->verbose = verbose;
  ctx->tbuf = tbuf;
  ctx->rows = rows;
  ctx->groupHash = groupHash;
  
  *pCtx = ctx;

  return TSDB_CODE_SUCCESS;

_return:

  taosArrayDestroy(rows);
  taosHashCleanup(groupHash);
  taosMemoryFree(ctx);

  QRY_RET(code);
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

int32_t qGenerateExplainResChildren(SPhysiNode *pNode, void *pExecInfo, SNodeList **pChildren) {
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
    QRY_ERR_RET(qGenerateExplainResNode((SPhysiNode *)node, pExecInfo, &pResNode));
    QRY_ERR_RET(nodesListAppend(*pChildren, pResNode));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qGenerateExplainResNode(SPhysiNode *pNode, void *pExecInfo, SExplainResNode **pRes) {
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
  QRY_ERR_JRET(qGenerateExplainResChildren(pNode, pExecInfo, &res->pChildren));
  
  *pRes = res;

  return TSDB_CODE_SUCCESS;

_return:

  qFreeExplainResTree(res);
  
  QRY_RET(code);
}

int32_t qExplainBufAppendExecInfo(void *pExecInfo, char *tbuf, int32_t *len) {
  int32_t tlen = *len;
  
  EXPLAIN_ROW_APPEND("(exec info here)");
  
  *len = tlen;
  
  return TSDB_CODE_SUCCESS;
}

int32_t qExplainResAppendRow(SExplainCtx *ctx, char *tbuf, int32_t len, int32_t level) {
  SQueryExplainRowInfo row = {0};
  row.buf = taosMemoryMalloc(len);
  if (NULL == row.buf) {
    qError("taosMemoryMalloc %d failed", len);
    QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  memcpy(row.buf, tbuf, len);
  row.level = level;
  row.len = len;
  ctx->totalSize += len;

  if (NULL == taosArrayPush(ctx->rows, &row)) {
    qError("taosArrayPush row to explain res rows failed");
    taosMemoryFree(row.buf);
    QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qExplainResNodeToRowsImpl(SExplainResNode *pResNode, SExplainCtx *ctx, int32_t level) {
  int32_t tlen = 0;
  bool isVerboseLine = false;
  char *tbuf = ctx->tbuf;
  bool verbose = ctx->verbose;
  SPhysiNode* pNode = pResNode->pNode;
  if (NULL == pNode) {
    qError("pyhsical node in explain res node is NULL");
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  switch (pNode->type) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN: {
      STagScanPhysiNode *pTagScanNode = (STagScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_TAG_SCAN_FORMAT, pTagScanNode->tableName.tname, pTagScanNode->pScanCols->length);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_LOOPS_FORMAT, pTagScanNode->count);
      if (pTagScanNode->reverse) {
        EXPLAIN_ROW_APPEND(EXPLAIN_REVERSE_FORMAT, pTagScanNode->reverse);
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pTagScanNode->order));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:{
      STableScanPhysiNode *pTblScanNode = (STableScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_TBL_SCAN_FORMAT, pTblScanNode->scan.tableName.tname, pTblScanNode->scan.pScanCols->length);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_LOOPS_FORMAT, pTblScanNode->scan.count);
      if (pTblScanNode->scan.reverse) {
        EXPLAIN_ROW_APPEND(EXPLAIN_REVERSE_FORMAT, pTblScanNode->scan.reverse);
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {      
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pTblScanNode->scan.order));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIMERANGE_FORMAT, pTblScanNode->scanRange.skey, pTblScanNode->scanRange.ekey);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pTblScanNode->scan.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pTblScanNode->scan.node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));        
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:{
      SSystemTableScanPhysiNode *pSTblScanNode = (SSystemTableScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SYSTBL_SCAN_FORMAT, pSTblScanNode->scan.tableName.tname, pSTblScanNode->scan.pScanCols->length);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_LOOPS_FORMAT, pSTblScanNode->scan.count);
      if (pSTblScanNode->scan.reverse) {
        EXPLAIN_ROW_APPEND(EXPLAIN_REVERSE_FORMAT, pSTblScanNode->scan.reverse);
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {            
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pSTblScanNode->scan.order));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));      
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:{
      SProjectPhysiNode *pPrjNode = (SProjectPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_PROJECTION_FORMAT, pPrjNode->pProjections->length, pPrjNode->node.pOutputDataBlockDesc->outputRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        if (pPrjNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pPrjNode->node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));        
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:{
      SJoinPhysiNode *pJoinNode = (SJoinPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_JOIN_FORMAT, EXPLAIN_JOIN_STRING(pJoinNode->joinType), pJoinNode->pTargets->length, pJoinNode->node.pOutputDataBlockDesc->outputRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        if (pJoinNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pJoinNode->node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));                
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
        
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ON_CONDITIONS_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pJoinNode->pOnConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));              
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_AGG:{
      SAggPhysiNode *pAggNode = (SAggPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_AGG_FORMAT, pAggNode->pAggFuncs->length);
      if (pAggNode->pGroupKeys) {
        EXPLAIN_ROW_APPEND(EXPLAIN_GROUPS_FORMAT, pAggNode->pGroupKeys->length);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pAggNode->node.pOutputDataBlockDesc->outputRowSize);
      
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        if (pAggNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pAggNode->node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));                
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:{
      SExchangePhysiNode *pExchNode = (SExchangePhysiNode *)pNode;
      SExplainGroup *group = taosHashGet(ctx->groupHash, &pExchNode->srcGroupId, sizeof(pExchNode->srcGroupId));
      if (NULL == group) {
        qError("exchange src group %d not in groupHash", pExchNode->srcGroupId);
        QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      EXPLAIN_ROW_NEW(level, EXPLAIN_EXCHANGE_FORMAT, group->nodeNum, pExchNode->node.pOutputDataBlockDesc->outputRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        if (pExchNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pExchNode->node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));                
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }

      QRY_ERR_RET(qAppendTaskExplainResRows(ctx, pExchNode->srcGroupId, level + 1));
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT:{
      SSortPhysiNode *pSortNode = (SSortPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SORT_FORMAT, pSortNode->pSortKeys->length, pSortNode->node.pOutputDataBlockDesc->outputRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        if (pSortNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pSortNode->node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));                
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INTERVAL:{
      SIntervalPhysiNode *pIntNode = (SIntervalPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_INTERVAL_FORMAT, qGetNameFromColumnNode(pIntNode->pTspk), pIntNode->window.pFuncs->length, 
        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->interval, pIntNode->intervalUnit, pIntNode->precision), pIntNode->intervalUnit, 
        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->offset, pIntNode->intervalUnit, pIntNode->precision), pIntNode->intervalUnit, 
        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->sliding, pIntNode->slidingUnit, pIntNode->precision), pIntNode->slidingUnit, 
        pIntNode->window.node.pOutputDataBlockDesc->outputRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        if (pIntNode->pFill) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILL_FORMAT, qFillModeString(pIntNode->pFill->mode));      
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        if (pIntNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));                
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW:{
      SSessionWinodwPhysiNode *pIntNode = (SSessionWinodwPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SESSION_FORMAT, pIntNode->gap, pIntNode->window.pFuncs->length, pIntNode->window.node.pOutputDataBlockDesc->outputRowSize);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
      }
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        if (pIntNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));                
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    default:
      qError("not supported physical node type %d", pNode->type);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qExplainResNodeToRows(SExplainResNode *pResNode, SExplainCtx *ctx, int32_t level) {
  if (NULL == pResNode) {
    qError("explain res node is NULL");
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  int32_t code = 0;
  QRY_ERR_RET(qExplainResNodeToRowsImpl(pResNode, ctx, level));

  SNode* pNode = NULL;
  FOREACH(pNode, pResNode->pChildren) {
    QRY_ERR_RET(qExplainResNodeToRows((SExplainResNode *)pNode, ctx, level + 1));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qAppendTaskExplainResRows(void *pCtx, int32_t groupId, int32_t level) {
  SExplainResNode *node = NULL;
  int32_t code = 0;
  SExplainCtx *ctx = (SExplainCtx *)pCtx;

  SExplainGroup *group = taosHashGet(ctx->groupHash, &groupId, sizeof(groupId));
  if (NULL == group) {
    qError("group %d not in groupHash", groupId);
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  QRY_ERR_RET(qGenerateExplainResNode(group->plan->pNode, group->execInfo, &node));

  QRY_ERR_JRET(qExplainResNodeToRows(node, ctx, level));

_return:

  qFreeExplainResTree(node);
  
  QRY_RET(code);
}


int32_t qGetExplainRspFromCtx(void *ctx, SRetrieveTableRsp **pRsp) {
  SExplainCtx *pCtx = (SExplainCtx *)ctx;
  int32_t rowNum = taosArrayGetSize(pCtx->rows);
  if (rowNum <= 0) {
    qError("empty explain res rows");
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  int32_t colNum = 1;
  int32_t rspSize = sizeof(SRetrieveTableRsp) + sizeof(int32_t) * colNum + sizeof(int32_t) * rowNum + pCtx->totalSize;
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, rspSize);
  if (NULL == rsp) {
    qError("malloc SRetrieveTableRsp failed, size:%d", rspSize);
    QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  rsp->completed = 1;
  rsp->numOfRows = htonl(rowNum);

  *(int32_t *)rsp->data = htonl(pCtx->totalSize);

  int32_t *offset = (int32_t *)((char *)rsp->data + sizeof(int32_t));
  char *data = (char *)(offset + rowNum);
  int32_t tOffset = 0;
  
  for (int32_t i = 0; i < rowNum; ++i) {
    SQueryExplainRowInfo *row = taosArrayGet(pCtx->rows, i);
    *offset = tOffset;
    tOffset += row->len;

    memcpy(data, row->buf, row->len);
    
    ++offset;
    data += row->len;
  }

  *pRsp = rsp;

  return TSDB_CODE_SUCCESS;
}



