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

#include "query.h"
#include "plannodes.h"
#include "commandInt.h"

int32_t qExplainGenerateResNode(SPhysiNode *pNode, SExplainGroup *group, SExplainResNode **pRes);
int32_t qExplainAppendGroupResRows(void *pCtx, int32_t groupId, int32_t level);


void qExplainFreeResNode(SExplainResNode *resNode) {
  if (NULL == resNode) {
    return;
  }

  taosMemoryFreeClear(resNode->pExecInfo);

  SNode* node = NULL;
  FOREACH(node, resNode->pChildren) {
    qExplainFreeResNode((SExplainResNode *)node);
  }  
  nodesClearList(resNode->pChildren);
  
  taosMemoryFreeClear(resNode);
}

void qExplainFreeCtx(SExplainCtx *pCtx) {
  if (NULL == pCtx) {
    return;
  }

  int32_t rowSize = taosArrayGetSize(pCtx->rows);
  for (int32_t i = 0; i < rowSize; ++i) {
    SQueryExplainRowInfo *row = taosArrayGet(pCtx->rows, i);
    taosMemoryFreeClear(row->buf);
  }

  if (EXPLAIN_MODE_ANALYZE == pCtx->mode && pCtx->groupHash) {
    void *pIter = taosHashIterate(pCtx->groupHash, NULL);
    while (pIter) {
      SExplainGroup *group = (SExplainGroup *)pIter;
      if (NULL == group->nodeExecInfo) {
        continue;
      }
      
      int32_t num = taosArrayGetSize(group->nodeExecInfo);
      for (int32_t i = 0; i < num; ++i) {
        SExplainRsp *rsp = taosArrayGet(group->nodeExecInfo, i);
        taosMemoryFreeClear(rsp->subplanInfo);
      }
    
      pIter = taosHashIterate(pCtx->groupHash, pIter);
    }
  }
  
  taosHashCleanup(pCtx->groupHash);
  taosArrayDestroy(pCtx->rows);
  taosMemoryFree(pCtx);
}

int32_t qExplainInitCtx(SExplainCtx **pCtx, SHashObj *groupHash, bool verbose, double ratio, EExplainMode mode) {
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

  ctx->mode = mode;
  ctx->verbose = verbose;
  ctx->ratio = ratio;
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

int32_t qExplainGenerateResChildren(SPhysiNode *pNode, SExplainGroup *group, SNodeList **pChildren) {
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
    QRY_ERR_RET(qExplainGenerateResNode((SPhysiNode *)node, group, &pResNode));
    QRY_ERR_RET(nodesListAppend(*pChildren, pResNode));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainGenerateResNodeExecInfo(SArray **pExecInfo, SExplainGroup *group) {
  *pExecInfo = taosArrayInit(group->nodeNum, sizeof(SExplainExecInfo));
  if (NULL == (*pExecInfo)) {
    qError("taosArrayInit %d explainExecInfo failed", group->nodeNum);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  SExplainRsp *rsp = NULL;  
  for (int32_t i = 0; i < group->nodeNum; ++i) {
    rsp = taosArrayGet(group->nodeExecInfo, i);
    if (group->physiPlanExecIdx >= rsp->numOfPlans) {
      qError("physiPlanIdx %d exceed plan num %d", group->physiPlanExecIdx, rsp->numOfPlans);
      return TSDB_CODE_QRY_APP_ERROR;
    }
    
    taosArrayPush(*pExecInfo, rsp->subplanInfo + group->physiPlanExecIdx);
  }

  ++group->physiPlanExecIdx;

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainGenerateResNode(SPhysiNode *pNode, SExplainGroup *group, SExplainResNode **pResNode) {
  if (NULL == pNode) {
    *pResNode = NULL;
    qError("physical node is NULL");
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  SExplainResNode *resNode = taosMemoryCalloc(1, sizeof(SExplainResNode));
  if (NULL == resNode) {
    qError("calloc SPhysiNodeExplainRes failed");
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  int32_t code = 0;
  resNode->pNode = pNode;
  
  if (group->nodeExecInfo) {
    QRY_ERR_JRET(qExplainGenerateResNodeExecInfo(&resNode->pExecInfo, group));
  }
  
  QRY_ERR_JRET(qExplainGenerateResChildren(pNode, group, &resNode->pChildren));

  ++group->physiPlanNum;
  
  *pResNode = resNode;

  return TSDB_CODE_SUCCESS;

_return:

  qExplainFreeResNode(resNode);
  
  QRY_RET(code);
}

int32_t qExplainBufAppendExecInfo(SArray *pExecInfo, char *tbuf, int32_t *len) {
  int32_t tlen = *len;
  int32_t nodeNum = taosArrayGetSize(pExecInfo);
  SExplainExecInfo maxExecInfo = {0};
  
  for (int32_t i = 0; i < nodeNum; ++i) {
    SExplainExecInfo *execInfo = taosArrayGet(pExecInfo, i);
    if (execInfo->startupCost > maxExecInfo.startupCost) {
      maxExecInfo.startupCost = execInfo->startupCost;
    }
    if (execInfo->totalCost > maxExecInfo.totalCost) {
      maxExecInfo.totalCost = execInfo->totalCost;
    }
    if (execInfo->numOfRows > maxExecInfo.numOfRows) {
      maxExecInfo.numOfRows = execInfo->numOfRows;
    }
  }
  
  EXPLAIN_ROW_APPEND(EXPLAIN_EXECINFO_FORMAT, maxExecInfo.startupCost, maxExecInfo.totalCost, maxExecInfo.numOfRows);

  *len = tlen;
  
  return TSDB_CODE_SUCCESS;
}

int32_t qExplainBufAppendVerboseExecInfo(SArray *pExecInfo, char *tbuf, int32_t *len) {
  int32_t tlen = 0;
  bool gotVerbose = false;
  int32_t nodeNum = taosArrayGetSize(pExecInfo);
  SExplainExecInfo maxExecInfo = {0};
  
  for (int32_t i = 0; i < nodeNum; ++i) {
    SExplainExecInfo *execInfo = taosArrayGet(pExecInfo, i);
    if (execInfo->verboseInfo) {
      gotVerbose = true;
    }
  }

  if (gotVerbose) {
    EXPLAIN_ROW_APPEND("exec verbose info");
  }

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
  ctx->dataSize += len;

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
      EXPLAIN_ROW_NEW(level, EXPLAIN_TAG_SCAN_FORMAT, pTagScanNode->tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pTagScanNode->pScanCols->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTagScanNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LOOPS_FORMAT, pTagScanNode->count);
      if (pTagScanNode->reverse) {
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_REVERSE_FORMAT, pTagScanNode->reverse);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pTagScanNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTagScanNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pTagScanNode->order));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pResNode->pExecInfo) {
          QRY_ERR_RET(qExplainBufAppendVerboseExecInfo(pResNode->pExecInfo, tbuf, &tlen));
          if (tlen) {
            EXPLAIN_ROW_END();
            QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
          }
        }      
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:{
      STableScanPhysiNode *pTblScanNode = (STableScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_TBL_SCAN_FORMAT, pTblScanNode->scan.tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pTblScanNode->scan.pScanCols->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTblScanNode->scan.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LOOPS_FORMAT, pTblScanNode->scan.count);
      if (pTblScanNode->scan.reverse) {
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_REVERSE_FORMAT, pTblScanNode->scan.reverse);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);      
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {      
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pTblScanNode->scan.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTblScanNode->scan.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pTblScanNode->scan.order));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIMERANGE_FORMAT, pTblScanNode->scanRange.skey, pTblScanNode->scanRange.ekey);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pTblScanNode->scan.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pTblScanNode->scan.node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:{
      SSystemTableScanPhysiNode *pSTblScanNode = (SSystemTableScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SYSTBL_SCAN_FORMAT, pSTblScanNode->scan.tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pSTblScanNode->scan.pScanCols->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSTblScanNode->scan.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LOOPS_FORMAT, pSTblScanNode->scan.count);
      if (pSTblScanNode->scan.reverse) {
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_REVERSE_FORMAT, pSTblScanNode->scan.reverse);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);      
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {            
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pSTblScanNode->scan.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSTblScanNode->scan.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pSTblScanNode->scan.order));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));   

        if (pSTblScanNode->scan.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pSTblScanNode->scan.node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
        
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:{
      SProjectPhysiNode *pPrjNode = (SProjectPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_PROJECTION_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pPrjNode->pProjections->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pPrjNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);      
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pPrjNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pPrjNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      
        if (pPrjNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pPrjNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:{
      SJoinPhysiNode *pJoinNode = (SJoinPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_JOIN_FORMAT, EXPLAIN_JOIN_STRING(pJoinNode->joinType));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pJoinNode->pTargets->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pJoinNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);      
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pJoinNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pJoinNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pJoinNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pJoinNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
        
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ON_CONDITIONS_FORMAT);      
        QRY_ERR_RET(nodesNodeToSQL(pJoinNode->pOnConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_AGG:{
      SAggPhysiNode *pAggNode = (SAggPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_AGG_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pAggNode->pAggFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pAggNode->node.pOutputDataBlockDesc->totalRowSize);
      if (pAggNode->pGroupKeys) {
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_GROUPS_FORMAT, pAggNode->pGroupKeys->length);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);      
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pAggNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pAggNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pAggNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pAggNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
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
      
      EXPLAIN_ROW_NEW(level, EXPLAIN_EXCHANGE_FORMAT, group->nodeNum);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pExchNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);      
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pExchNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pExchNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pExchNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pExchNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }

      QRY_ERR_RET(qExplainAppendGroupResRows(ctx, pExchNode->srcGroupId, level + 1));
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT:{
      SSortPhysiNode *pSortNode = (SSortPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SORT_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pSortNode->pSortKeys->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSortNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pSortNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSortNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pSortNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pSortNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INTERVAL:{
      SIntervalPhysiNode *pIntNode = (SIntervalPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_INTERVAL_FORMAT, nodesGetNameFromColumnNode(pIntNode->pTspk));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pIntNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIntNode->window.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pIntNode->window.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIntNode->window.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIME_WINDOWS_FORMAT, INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->interval, pIntNode->intervalUnit, pIntNode->precision),  
              pIntNode->intervalUnit, pIntNode->offset, getPrecisionUnit(pIntNode->precision), 
              INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->sliding, pIntNode->slidingUnit, pIntNode->precision), pIntNode->slidingUnit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        
        if (pIntNode->pFill) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILL_FORMAT, getFillModeString(pIntNode->pFill->mode));      
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        if (pIntNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW:{
      SSessionWinodwPhysiNode *pSessNode = (SSessionWinodwPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SESSION_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }      
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pSessNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSessNode->window.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));


      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pSessNode->window.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSessNode->window.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_WINDOW_FORMAT, pSessNode->gap);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pSessNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);      
          QRY_ERR_RET(nodesNodeToSQL(pSessNode->window.node.pConditions, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen)); 
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

int32_t qExplainAppendGroupResRows(void *pCtx, int32_t groupId, int32_t level) {
  SExplainResNode *node = NULL;
  int32_t code = 0;
  SExplainCtx *ctx = (SExplainCtx *)pCtx;

  SExplainGroup *group = taosHashGet(ctx->groupHash, &groupId, sizeof(groupId));
  if (NULL == group) {
    qError("group %d not in groupHash", groupId);
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  QRY_ERR_RET(qExplainGenerateResNode(group->plan->pNode, group, &node));

  if ((EXPLAIN_MODE_ANALYZE == ctx->mode) && (group->physiPlanNum != group->physiPlanExecNum)) {
    qError("physiPlanNum %d mismatch with physiExecNum %d in group %d", group->physiPlanNum, group->physiPlanExecNum, groupId);
    QRY_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  QRY_ERR_JRET(qExplainResNodeToRows(node, ctx, level));

_return:

  qExplainFreeResNode(node);
  
  QRY_RET(code);
}


int32_t qExplainGetRspFromCtx(void *ctx, SRetrieveTableRsp **pRsp) {
  SExplainCtx *pCtx = (SExplainCtx *)ctx;
  int32_t rowNum = taosArrayGetSize(pCtx->rows);
  if (rowNum <= 0) {
    qError("empty explain res rows");
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  int32_t colNum = 1;
  int32_t rspSize = sizeof(SRetrieveTableRsp) + sizeof(int32_t) * colNum + sizeof(int32_t) * rowNum + pCtx->dataSize;
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, rspSize);
  if (NULL == rsp) {
    qError("malloc SRetrieveTableRsp failed, size:%d", rspSize);
    QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  rsp->completed = 1;
  rsp->numOfRows = htonl(rowNum);

  *(int32_t *)rsp->data = htonl(pCtx->dataSize);

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


int32_t qExplainPrepareCtx(SQueryPlan *pDag, SExplainCtx **pCtx) {
  int32_t code = 0;
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  SExplainGroup *pGroup = NULL;
  SExplainCtx *ctx = NULL;

  if (pDag->numOfSubplans <= 0) {
    qError("invalid subplan num:%d", pDag->numOfSubplans);
    QRY_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  if (levelNum <= 0) {
    qError("invalid level num:%d", levelNum);
    QRY_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SHashObj *groupHash = taosHashInit(EXPLAIN_MAX_GROUP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == groupHash) {
    qError("groupHash %d failed", EXPLAIN_MAX_GROUP_NUM);
    QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  QRY_ERR_JRET(qExplainInitCtx(&ctx, groupHash, pDag->explainInfo.verbose, pDag->explainInfo.ratio, pDag->explainInfo.mode));

  for (int32_t i = 0; i < levelNum; ++i) {
    plans = (SNodeListNode *)nodesListGetNode(pDag->pSubplans, i);
    if (NULL == plans) {
      qError("empty level plan, level:%d", i);
      QRY_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    taskNum = (int32_t)LIST_LENGTH(plans->pNodeList);
    if (taskNum <= 0) {
      qError("invalid level plan number:%d, level:%d", taskNum, i);
      QRY_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    SSubplan *plan = NULL;
    for (int32_t n = 0; n < taskNum; ++n) {
      plan = (SSubplan *)nodesListGetNode(plans->pNodeList, n);
      pGroup = taosHashGet(groupHash, &plan->id.groupId, sizeof(plan->id.groupId));
      if (pGroup) {
        ++pGroup->nodeNum;
        continue;
      }

      SExplainGroup group = {0};
      group.nodeNum = 1;
      group.plan = plan;
      
      if (0 != taosHashPut(groupHash, &plan->id.groupId, sizeof(plan->id.groupId), &group, sizeof(group))) {
        qError("taosHashPut to explainGroupHash failed, taskIdx:%d", n);
        QRY_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
    }

    if (0 == i) {
      if (taskNum > 1) {
        qError("invalid taskNum %d for level 0", taskNum);
        QRY_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      ctx->rootGroupId = plan->id.groupId;
    }

    qDebug("level %d group handled, taskNum:%d", i, taskNum);
  }

  *pCtx = ctx;

  return TSDB_CODE_SUCCESS;
  
_return:

  qExplainFreeCtx(ctx);

  QRY_RET(code);
}


int32_t qExplainGenerateRsp(SExplainCtx *pCtx, SRetrieveTableRsp **pRsp) {
  QRY_ERR_RET(qExplainAppendGroupResRows(pCtx, pCtx->rootGroupId, 0));
  
  QRY_ERR_RET(qExplainGetRspFromCtx(pCtx, pRsp));

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainUpdateExecInfo(SExplainCtx        *pCtx, SExplainRsp *pRspMsg, int32_t groupId, SRetrieveTableRsp **pRsp) {
  SExplainResNode *node = NULL;
  int32_t code = 0;
  bool groupDone = false;
  SExplainCtx *ctx = (SExplainCtx *)pCtx;

  SExplainGroup *group = taosHashGet(ctx->groupHash, &groupId, sizeof(groupId));
  if (NULL == group) {
    qError("group %d not in groupHash", groupId);
    taosMemoryFreeClear(pRspMsg->subplanInfo);
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  taosWLockLatch(&group->lock);
  if (NULL == group->nodeExecInfo) {
    group->nodeExecInfo = taosArrayInit(group->nodeNum, sizeof(SExplainRsp));
    if (NULL == group->nodeExecInfo) {
      qError("taosArrayInit %d explainExecInfo failed", group->nodeNum);
      taosMemoryFreeClear(pRspMsg->subplanInfo);
      taosWUnLockLatch(&group->lock);
      
      QRY_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    group->physiPlanExecNum = pRspMsg->numOfPlans;
  } else if (taosArrayGetSize(group->nodeExecInfo) >= group->nodeNum) {
    qError("group execInfo already full, size:%d, nodeNum:%d", (int32_t)taosArrayGetSize(group->nodeExecInfo), group->nodeNum);
    taosMemoryFreeClear(pRspMsg->subplanInfo);
    taosWUnLockLatch(&group->lock);
    
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (group->physiPlanExecNum != pRspMsg->numOfPlans) {
    qError("physiPlanExecNum %d mismatch with others %d in group %d", pRspMsg->numOfPlans, group->physiPlanExecNum, groupId);
    taosMemoryFreeClear(pRspMsg->subplanInfo);
    taosWUnLockLatch(&group->lock);
    
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  taosArrayPush(group->nodeExecInfo, pRspMsg);
  groupDone = (taosArrayGetSize(group->nodeExecInfo) >= group->nodeNum);
  
  taosWUnLockLatch(&group->lock);

  if (groupDone && (taosHashGetSize(pCtx->groupHash) == atomic_add_fetch_32(&pCtx->groupDoneNum, 1))) {
    if (atomic_load_8((int8_t *)&pCtx->execDone)) {
      if (0 == taosWTryLockLatch(&pCtx->lock)) {
        QRY_ERR_RET(qExplainGenerateRsp(pCtx, pRsp));
        // LEAVE LOCK THERE
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qExecStaticExplain(SQueryPlan *pDag, SRetrieveTableRsp **pRsp) {
  int32_t code = 0;
  SExplainCtx *pCtx = NULL;

  QRY_ERR_RET(qExplainPrepareCtx(pDag, &pCtx));
  
  QRY_ERR_JRET(qExplainGenerateRsp(pCtx, pRsp));
  
_return:

  qExplainFreeCtx(pCtx);

  QRY_RET(code);
}

int32_t qExecExplainBegin(SQueryPlan *pDag, SExplainCtx **pCtx, int32_t startTs) {
  QRY_ERR_RET(qExplainPrepareCtx(pDag, pCtx));
  
  (*pCtx)->reqStartTs = startTs;
  (*pCtx)->jobStartTs = taosGetTimestampMs();

  return TSDB_CODE_SUCCESS;
}

int32_t qExecExplainEnd(SExplainCtx *pCtx, SRetrieveTableRsp **pRsp) {
  int32_t code = 0;
  pCtx->jobDoneTs = taosGetTimestampMs();
  
  atomic_store_8((int8_t *)&pCtx->execDone, true);

  if (taosHashGetSize(pCtx->groupHash) == atomic_load_32(&pCtx->groupDoneNum)) {
    if (0 == taosWTryLockLatch(&pCtx->lock)) {
      QRY_ERR_RET(qExplainGenerateRsp(pCtx, pRsp));
      // LEAVE LOCK THERE
    }
  }

  return TSDB_CODE_SUCCESS;
}



