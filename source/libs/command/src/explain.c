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

// clang-format off
#include "commandInt.h"
#include "plannodes.h"
#include "query.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "systable.h"
#include "functionMgt.h"

int32_t qExplainGenerateResNode(SPhysiNode *pNode, SExplainGroup *group, SExplainResNode **pRes);
int32_t qExplainAppendGroupResRows(void *pCtx, int32_t groupId, int32_t level, bool singleChannel);

char *qExplainGetDynQryCtrlType(EDynQueryType type) {
  switch (type) {
    case DYN_QTYPE_STB_HASH:
      return "STable Join";
    default:
      break;
  }

  return "unknown task";
}

void qExplainFreeResNode(SExplainResNode *resNode) {
  if (NULL == resNode) {
    return;
  }

  taosArrayDestroy(resNode->pExecInfo);

  SNode *node = NULL;
  FOREACH(node, resNode->pChildren) { qExplainFreeResNode((SExplainResNode *)node); }
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
      if (group->nodeExecInfo) {
        int32_t num = taosArrayGetSize(group->nodeExecInfo);
        for (int32_t i = 0; i < num; ++i) {
          SExplainRsp *rsp = taosArrayGet(group->nodeExecInfo, i);
          tFreeSExplainRsp(rsp);
        }
        taosArrayDestroy(group->nodeExecInfo);
      }

      pIter = taosHashIterate(pCtx->groupHash, pIter);
    }
  }

  taosHashCleanup(pCtx->groupHash);
  taosArrayDestroy(pCtx->rows);
  taosMemoryFreeClear(pCtx->tbuf);
  taosMemoryFree(pCtx);
}

int32_t qExplainInitCtx(SExplainCtx **pCtx, SHashObj *groupHash, bool verbose, double ratio, EExplainMode mode) {
  int32_t      code = 0;
  SExplainCtx *ctx = taosMemoryCalloc(1, sizeof(SExplainCtx));
  if (NULL == ctx) {
    qError("calloc SExplainCtx failed");
    QRY_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SArray *rows = taosArrayInit(10, sizeof(SQueryExplainRowInfo));
  if (NULL == rows) {
    qError("taosArrayInit SQueryExplainRowInfo failed");
    QRY_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  char *tbuf = taosMemoryMalloc(TSDB_EXPLAIN_RESULT_ROW_SIZE);
  if (NULL == tbuf) {
    qError("malloc size %d failed", TSDB_EXPLAIN_RESULT_ROW_SIZE);
    QRY_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
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
  int32_t    tlen = 0;
  SNodeList *pPhysiChildren = pNode->pChildren;

  if (pPhysiChildren) {
    *pChildren = nodesMakeList();
    if (NULL == *pChildren) {
      qError("nodesMakeList failed");
      QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SNode           *node = NULL;
  SExplainResNode *pResNode = NULL;
  FOREACH(node, pPhysiChildren) {
    QRY_ERR_RET(qExplainGenerateResNode((SPhysiNode *)node, group, &pResNode));
    QRY_ERR_RET(nodesListAppend(*pChildren, (SNode *)pResNode));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainGenerateResNodeExecInfo(SPhysiNode *pNode, SArray **pExecInfo, SExplainGroup *group) {
  *pExecInfo = taosArrayInit(group->nodeNum, sizeof(SExplainExecInfo));
  if (NULL == (*pExecInfo)) {
    qError("taosArrayInit %d explainExecInfo failed", group->nodeNum);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SExplainRsp *rsp = NULL;
  if (group->singleChannel) {
    if (0 == group->physiPlanExecIdx) {
      group->nodeIdx = 0;
    }
    
    rsp = taosArrayGet(group->nodeExecInfo, group->nodeIdx++);
    if (group->physiPlanExecIdx >= rsp->numOfPlans) {
      qError("physiPlanIdx %d exceed plan num %d", group->physiPlanExecIdx, rsp->numOfPlans);
      return TSDB_CODE_APP_ERROR;
    }
    
    taosArrayPush(*pExecInfo, rsp->subplanInfo + group->physiPlanExecIdx);
  } else {
    for (int32_t i = 0; i < group->nodeNum; ++i) {
      rsp = taosArrayGet(group->nodeExecInfo, i);
      if (group->physiPlanExecIdx >= rsp->numOfPlans) {
        qError("physiPlanIdx %d exceed plan num %d", group->physiPlanExecIdx, rsp->numOfPlans);
        return TSDB_CODE_APP_ERROR;
      }

      taosArrayPush(*pExecInfo, rsp->subplanInfo + group->physiPlanExecIdx);
    }
  }

  ++group->physiPlanExecIdx;

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainGenerateResNode(SPhysiNode *pNode, SExplainGroup *group, SExplainResNode **pResNode) {
  if (NULL == pNode) {
    *pResNode = NULL;
    qError("physical node is NULL");
    return TSDB_CODE_APP_ERROR;
  }

  SExplainResNode *resNode = taosMemoryCalloc(1, sizeof(SExplainResNode));
  if (NULL == resNode) {
    qError("calloc SPhysiNodeExplainRes failed");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = 0;
  resNode->pNode = pNode;

  if (group->nodeExecInfo) {
    QRY_ERR_JRET(qExplainGenerateResNodeExecInfo(pNode, &resNode->pExecInfo, group));
  }

  QRY_ERR_JRET(qExplainGenerateResChildren(pNode, group, &resNode->pChildren));

  *pResNode = resNode;

  return TSDB_CODE_SUCCESS;

_return:

  qExplainFreeResNode(resNode);

  QRY_RET(code);
}

int32_t qExplainBufAppendExecInfo(SArray *pExecInfo, char *tbuf, int32_t *len) {
  int32_t          tlen = *len;
  int32_t          nodeNum = taosArrayGetSize(pExecInfo);
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
  int32_t          tlen = 0;
  bool             gotVerbose = false;
  int32_t          nodeNum = taosArrayGetSize(pExecInfo);
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
    QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  memcpy(row.buf, tbuf, len);
  row.level = level;
  row.len = len;
  ctx->dataSize += row.len;

  if (NULL == taosArrayPush(ctx->rows, &row)) {
    qError("taosArrayPush row to explain res rows failed");
    taosMemoryFree(row.buf);
    QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

static uint8_t qExplainGetIntervalPrecision(SIntervalPhysiNode *pIntNode) {
  return ((SColumnNode *)pIntNode->window.pTspk)->node.resType.precision;
}

static char* qExplainGetScanMode(STableScanPhysiNode* pScan) {
  bool isGroupByTbname = false;
  bool isGroupByTag = false;
  bool seq = false;
  bool groupOrder = false;
  if (pScan->pGroupTags && LIST_LENGTH(pScan->pGroupTags) == 1) {
    SNode* p = nodesListGetNode(pScan->pGroupTags, 0);
    if (QUERY_NODE_FUNCTION == nodeType(p) && (strcmp(((struct SFunctionNode*)p)->functionName, "tbname") == 0)) {
      isGroupByTbname = true;
    }
  }

  isGroupByTag = (NULL != pScan->pGroupTags) && !isGroupByTbname;
  if ((((!isGroupByTag) || isGroupByTbname) && pScan->groupSort) || (isGroupByTag && (pScan->groupSort || pScan->scan.groupOrderScan))) {
    return "seq_grp_order";
  } 

  if ((isGroupByTbname && (pScan->groupSort || pScan->scan.groupOrderScan)) || (isGroupByTag && (pScan->groupSort || pScan->scan.groupOrderScan))) {
    return "grp_order";
  }

  return "ts_order";
}

static char* qExplainGetScanDataLoad(STableScanPhysiNode* pScan) {
  switch (pScan->dataRequired) {
    case FUNC_DATA_REQUIRED_DATA_LOAD:
      return "data";
    case FUNC_DATA_REQUIRED_SMA_LOAD:
      return "sma";
    case FUNC_DATA_REQUIRED_NOT_LOAD:
      return "no";
    default:
      break;
  }

  return "unknown";
}

int32_t qExplainResNodeToRowsImpl(SExplainResNode *pResNode, SExplainCtx *ctx, int32_t level) {
  int32_t     tlen = 0;
  bool        isVerboseLine = false;
  char       *tbuf = ctx->tbuf;
  bool        verbose = ctx->verbose;
  SPhysiNode *pNode = pResNode->pNode;
  if (NULL == pNode) {
    qError("pyhsical node in explain res node is NULL");
    return TSDB_CODE_APP_ERROR;
  }

  switch (pNode->type) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN: {
      STagScanPhysiNode *pTagScanNode = (STagScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_TAG_SCAN_FORMAT, pTagScanNode->scan.tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      if (pTagScanNode->scan.pScanPseudoCols) {
        EXPLAIN_ROW_APPEND(EXPLAIN_PSEUDO_COLUMNS_FORMAT, pTagScanNode->scan.pScanPseudoCols->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTagScanNode->scan.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pTagScanNode->scan.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTagScanNode->scan.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pTagScanNode->scan.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pTagScanNode->scan.node.pSlimit);
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
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN: {
      STableScanPhysiNode *pTblScanNode = (STableScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level,
                      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == pNode->type ? EXPLAIN_TBL_MERGE_SCAN_FORMAT
                                                                               : EXPLAIN_TBL_SCAN_FORMAT,
                      pTblScanNode->scan.tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }

      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pTblScanNode->scan.pScanCols->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      if (pTblScanNode->scan.pScanPseudoCols) {
        EXPLAIN_ROW_APPEND(EXPLAIN_PSEUDO_COLUMNS_FORMAT, pTblScanNode->scan.pScanPseudoCols->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTblScanNode->scan.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_SCAN_ORDER_FORMAT, pTblScanNode->scanSeq[0], pTblScanNode->scanSeq[1]);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_SCAN_MODE_FORMAT, qExplainGetScanMode(pTblScanNode));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_SCAN_DATA_LOAD_FORMAT, qExplainGetScanDataLoad(pTblScanNode));
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      // basic analyze output
      if (EXPLAIN_MODE_ANALYZE == ctx->mode) {
        EXPLAIN_ROW_NEW(level + 1, "I/O: ");

        int32_t                      nodeNum = taosArrayGetSize(pResNode->pExecInfo);
        struct STableScanAnalyzeInfo info = {0};

        int32_t maxIndex = 0;
        int32_t totalRows = 0;
        for (int32_t i = 0; i < nodeNum; ++i) {
          SExplainExecInfo      *execInfo = taosArrayGet(pResNode->pExecInfo, i);
          STableScanAnalyzeInfo *pScanInfo = (STableScanAnalyzeInfo *)execInfo->verboseInfo;

          info.totalBlocks += pScanInfo->totalBlocks;
          info.loadBlocks += pScanInfo->loadBlocks;
          info.totalRows += pScanInfo->totalRows;
          info.skipBlocks += pScanInfo->skipBlocks;
          info.filterTime += pScanInfo->filterTime;
          info.loadBlockStatis += pScanInfo->loadBlockStatis;
          info.totalCheckedRows += pScanInfo->totalCheckedRows;
          info.filterOutBlocks += pScanInfo->filterOutBlocks;

          if (pScanInfo->totalRows > totalRows) {
            totalRows = pScanInfo->totalRows;
            maxIndex = i;
          }
        }

        EXPLAIN_ROW_APPEND("total_blocks=%.1f", ((double)info.totalBlocks) / nodeNum);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);

        EXPLAIN_ROW_APPEND("load_blocks=%.1f", ((double)info.loadBlocks) / nodeNum);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);

        EXPLAIN_ROW_APPEND("load_block_SMAs=%.1f", ((double)info.loadBlockStatis) / nodeNum);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);

        EXPLAIN_ROW_APPEND("total_rows=%.1f", ((double)info.totalRows) / nodeNum);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);

        EXPLAIN_ROW_APPEND("check_rows=%.1f", ((double)info.totalCheckedRows) / nodeNum);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_END();

        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        // Rows out: Avg 4166.7 rows x 24 workers. Max 4187 rows (seg7) with 0.220 ms to first row, 1.738 ms to end,
        // start offset by 1.470 ms.
        SExplainExecInfo      *execInfo = taosArrayGet(pResNode->pExecInfo, maxIndex);
        STableScanAnalyzeInfo *p1 = (STableScanAnalyzeInfo *)execInfo->verboseInfo;

        EXPLAIN_ROW_NEW(level + 1, " ");
        EXPLAIN_ROW_APPEND("max_row_task=%d, total_rows:%" PRId64 ", ep:%s (cost=%.3f..%.3f)", maxIndex, p1->totalRows,
                           "tbd", execInfo->startupCost, execInfo->totalCost);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_END();

        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pTblScanNode->scan.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pTblScanNode->scan.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pTblScanNode->scan.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pTblScanNode->scan.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIMERANGE_FORMAT, pTblScanNode->scanRange.skey,
                        pTblScanNode->scanRange.ekey);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (NULL != pTblScanNode->pGroupTags) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_PARTITION_KETS_FORMAT);
          EXPLAIN_ROW_APPEND(EXPLAIN_PARTITIONS_FORMAT, pTblScanNode->pGroupTags->length);
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        if (pTblScanNode->scan.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pTblScanNode->scan.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN: {
      SSystemTableScanPhysiNode *pSTblScanNode = (SSystemTableScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SYSTBL_SCAN_FORMAT, pSTblScanNode->scan.tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pSTblScanNode->scan.pScanCols->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      if (pSTblScanNode->scan.pScanPseudoCols) {
        EXPLAIN_ROW_APPEND(EXPLAIN_PSEUDO_COLUMNS_FORMAT, pSTblScanNode->scan.pScanPseudoCols->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSTblScanNode->scan.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pSTblScanNode->scan.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSTblScanNode->scan.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pSTblScanNode->scan.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pSTblScanNode->scan.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pSTblScanNode->scan.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pSTblScanNode->scan.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }       
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT: {
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
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pPrjNode->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pPrjNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pPrjNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pPrjNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pPrjNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_IGNORE_GROUPID_FORMAT, pPrjNode->ignoreGroupId ? "true" : "false");
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_MERGEBLOCKS_FORMAT, pPrjNode->mergeDataBlock? "True":"False");
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));  

        if (pPrjNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pPrjNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }      
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN: {
      SSortMergeJoinPhysiNode *pJoinNode = (SSortMergeJoinPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_JOIN_FORMAT, EXPLAIN_JOIN_STRING(pJoinNode->joinType));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pJoinNode->pTargets->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pJoinNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pJoinNode->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pJoinNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pJoinNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pJoinNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pJoinNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pJoinNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pJoinNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ON_CONDITIONS_FORMAT);
        QRY_ERR_RET(
            nodesNodeToSQL(pJoinNode->pPrimKeyCond, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
        if (pJoinNode->pOtherOnCond) {
          EXPLAIN_ROW_APPEND(" AND ");
          QRY_ERR_RET(
              nodesNodeToSQL(pJoinNode->pOtherOnCond, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
        }
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));        
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG: {
      SAggPhysiNode *pAggNode = (SAggPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_AGG_FORMAT, (pAggNode->pGroupKeys ? "GroupAggragate" : "Aggragate"));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      if (pAggNode->pAggFuncs) {
        EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pAggNode->pAggFuncs->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pAggNode->node.pOutputDataBlockDesc->totalRowSize);
      if (pAggNode->pGroupKeys) {
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_GROUPS_FORMAT, pAggNode->pGroupKeys->length);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pAggNode->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pAggNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pAggNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pAggNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pAggNode->node.pSlimit);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_PLAN_BLOCKING, !pAggNode->node.forceCreateNonBlockingOptr);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pAggNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pAggNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_MERGEBLOCKS_FORMAT, pAggNode->mergeDataBlock? "True":"False");
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC: {
      SIndefRowsFuncPhysiNode *pIndefNode = (SIndefRowsFuncPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_INDEF_ROWS_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      if (pIndefNode->pFuncs) {
        EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pIndefNode->pFuncs->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIndefNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pIndefNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIndefNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pIndefNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pIndefNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pIndefNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pIndefNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }      
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE: {
      SExchangePhysiNode *pExchNode = (SExchangePhysiNode *)pNode;
      int32_t nodeNum = 0;
      for (int32_t i = pExchNode->srcStartGroupId; i <= pExchNode->srcEndGroupId; ++i) {
        SExplainGroup      *group = taosHashGet(ctx->groupHash, &pExchNode->srcStartGroupId, sizeof(pExchNode->srcStartGroupId));
        if (NULL == group) {
          qError("exchange src group %d not in groupHash", pExchNode->srcStartGroupId);
          QRY_ERR_RET(TSDB_CODE_APP_ERROR);
        }
        
        nodeNum += group->nodeNum;
      }

      EXPLAIN_ROW_NEW(level, EXPLAIN_EXCHANGE_FORMAT, pExchNode->singleChannel ? 1 : nodeNum);
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
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pExchNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pExchNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pExchNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pExchNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pExchNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pExchNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }      
      }

      for (int32_t i = pExchNode->srcStartGroupId; i <= pExchNode->srcEndGroupId; ++i) {
        QRY_ERR_RET(qExplainAppendGroupResRows(ctx, i, level + 1, pExchNode->singleChannel));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT: {
      SSortPhysiNode *pSortNode = (SSortPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_SORT_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pSortNode->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_OUTPUT_ORDER_TYPE_FORMAT, EXPLAIN_ORDER_STRING(pSortNode->node.outputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }

      SDataBlockDescNode *pDescNode = pSortNode->node.pOutputDataBlockDesc;
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pDescNode->pSlots));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pDescNode->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (EXPLAIN_MODE_ANALYZE == ctx->mode) {
        // sort key
        EXPLAIN_ROW_NEW(level + 1, "Sort Key: ");
        if (pResNode->pExecInfo) {
          for (int32_t i = 0; i < LIST_LENGTH(pSortNode->pSortKeys); ++i) {
            SOrderByExprNode *ptn = (SOrderByExprNode *)nodesListGetNode(pSortNode->pSortKeys, i);
            EXPLAIN_ROW_APPEND("%s ", nodesGetNameFromColumnNode(ptn->pExpr));
          }
        }

        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

        // sort method
        EXPLAIN_ROW_NEW(level + 1, "Sort Method: ");

        int32_t           nodeNum = taosArrayGetSize(pResNode->pExecInfo);
        SExplainExecInfo *execInfo = taosArrayGet(pResNode->pExecInfo, 0);
        SSortExecInfo    *pExecInfo = (SSortExecInfo *)execInfo->verboseInfo;
        EXPLAIN_ROW_APPEND("%s", pExecInfo->sortMethod == SORT_QSORT_T ? "quicksort" : "merge sort");
        if (pExecInfo->sortBuffer > 1024 * 1024) {
          EXPLAIN_ROW_APPEND("  Buffers:%.2f Mb", pExecInfo->sortBuffer / (1024 * 1024.0));
        } else if (pExecInfo->sortBuffer > 1024) {
          EXPLAIN_ROW_APPEND("  Buffers:%.2f Kb", pExecInfo->sortBuffer / (1024.0));
        } else {
          EXPLAIN_ROW_APPEND("  Buffers:%d b", pExecInfo->sortBuffer);
        }

        EXPLAIN_ROW_APPEND("  loops:%d", pExecInfo->loops);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));
      }

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pSortNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSortNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pSortNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pSortNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pSortNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pSortNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }        
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL: {
      SIntervalPhysiNode *pIntNode = (SIntervalPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_INTERVAL_FORMAT, nodesGetNameFromColumnNode(pIntNode->window.pTspk));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pIntNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIntNode->window.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pIntNode->window.node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_OUTPUT_ORDER_TYPE_FORMAT, EXPLAIN_ORDER_STRING(pIntNode->window.node.outputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pIntNode->window.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIntNode->window.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pIntNode->window.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pIntNode->window.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        uint8_t precision = qExplainGetIntervalPrecision(pIntNode);
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIME_WINDOWS_FORMAT,
                        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->interval, pIntNode->intervalUnit, precision),
                        pIntNode->intervalUnit, pIntNode->offset, getPrecisionUnit(precision),
                        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->sliding, pIntNode->slidingUnit, precision),
                        pIntNode->slidingUnit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pIntNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_MERGEBLOCKS_FORMAT, pIntNode->window.mergeDataBlock? "True":"False");
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL: {
      SMergeAlignedIntervalPhysiNode *pIntNode = (SMergeAlignedIntervalPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_MERGE_ALIGNED_INTERVAL_FORMAT, nodesGetNameFromColumnNode(pIntNode->window.pTspk));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pIntNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIntNode->window.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pIntNode->window.node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_OUTPUT_ORDER_TYPE_FORMAT, EXPLAIN_ORDER_STRING(pIntNode->window.node.outputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pIntNode->window.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIntNode->window.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pIntNode->window.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pIntNode->window.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        uint8_t precision = qExplainGetIntervalPrecision(pIntNode);
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIME_WINDOWS_FORMAT,
                        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->interval, pIntNode->intervalUnit, precision),
                        pIntNode->intervalUnit, pIntNode->offset, getPrecisionUnit(precision),
                        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->sliding, pIntNode->slidingUnit, precision),
                        pIntNode->slidingUnit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pIntNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_MERGEBLOCKS_FORMAT, pIntNode->window.mergeDataBlock? "True":"False");
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_FILL: {
      SFillPhysiNode *pFillNode = (SFillPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_FILL_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_MODE_FORMAT, nodesGetFillModeString(pFillNode->mode));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pFillNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pFillNode->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pFillNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pFillNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pFillNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pFillNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        if (pFillNode->pValues) {
          SNodeListNode *pValues = (SNodeListNode *)pFillNode->pValues;
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILL_VALUE_FORMAT);
          SNode  *tNode = NULL;
          int32_t i = 0;
          FOREACH(tNode, pValues->pNodeList) {
            if (i) {
              EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
            }
            SValueNode *tValue = (SValueNode *)tNode;
            char       *value = nodesGetStrValueFromNode(tValue);
            EXPLAIN_ROW_APPEND(EXPLAIN_STRING_TYPE_FORMAT, value);
            taosMemoryFree(value);
            ++i;
          }

          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIMERANGE_FORMAT, pFillNode->timeRange.skey, pFillNode->timeRange.ekey);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pFillNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pFillNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }       
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION: {
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
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pSessNode->window.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSessNode->window.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pSessNode->window.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pSessNode->window.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_WINDOW_FORMAT, pSessNode->gap);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pSessNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pSessNode->window.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }      
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE: {
      SStateWinodwPhysiNode *pStateNode = (SStateWinodwPhysiNode *)pNode;

      EXPLAIN_ROW_NEW(level, EXPLAIN_STATE_WINDOW_FORMAT,
                      nodesGetNameFromColumnNode(pStateNode->pStateKey));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }

      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pStateNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pStateNode->window.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pStateNode->window.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pStateNode->window.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pStateNode->window.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pStateNode->window.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pStateNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pStateNode->window.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }     
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION: {
      SPartitionPhysiNode *pPartNode = (SPartitionPhysiNode *)pNode;

      SNode *p = nodesListGetNode(pPartNode->pPartitionKeys, 0);
      EXPLAIN_ROW_NEW(level, EXPLAIN_PARITION_FORMAT, nodesGetNameFromColumnNode(p));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pPartNode->node.pOutputDataBlockDesc->totalRowSize);

      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pPartNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pPartNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pPartNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pPartNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_PARTITION_KETS_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_PARTITIONS_FORMAT, pPartNode->pPartitionKeys->length);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pPartNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pPartNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }        
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE: {
      SMergePhysiNode *pMergeNode = (SMergePhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_MERGE_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }

      SDataBlockDescNode *pDescNode = pMergeNode->node.pOutputDataBlockDesc;
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pDescNode->pSlots));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pDescNode->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pMergeNode->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_OUTPUT_ORDER_TYPE_FORMAT, EXPLAIN_ORDER_STRING(pMergeNode->node.outputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_MERGE_MODE_FORMAT, EXPLAIN_MERGE_MODE_STRING(pMergeNode->type));
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (EXPLAIN_MODE_ANALYZE == ctx->mode) {
        if (MERGE_TYPE_SORT == pMergeNode->type) {
          // sort method
          EXPLAIN_ROW_NEW(level + 1, "Sort Method: ");

          int32_t           nodeNum = taosArrayGetSize(pResNode->pExecInfo);
          SExplainExecInfo *execInfo = taosArrayGet(pResNode->pExecInfo, 0);
          SSortExecInfo    *pExecInfo = (SSortExecInfo *)execInfo->verboseInfo;
          EXPLAIN_ROW_APPEND("%s", pExecInfo->sortMethod == SORT_QSORT_T ? "quicksort" : "merge sort");
          if (pExecInfo->sortBuffer > 1024 * 1024) {
            EXPLAIN_ROW_APPEND("  Buffers:%.2f Mb", pExecInfo->sortBuffer / (1024 * 1024.0));
          } else if (pExecInfo->sortBuffer > 1024) {
            EXPLAIN_ROW_APPEND("  Buffers:%.2f Kb", pExecInfo->sortBuffer / (1024.0));
          } else {
            EXPLAIN_ROW_APPEND("  Buffers:%d b", pExecInfo->sortBuffer);
          }

          EXPLAIN_ROW_APPEND("  loops:%d", pExecInfo->loops);
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));
        }
      }

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pMergeNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pMergeNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pMergeNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pMergeNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (MERGE_TYPE_SORT == pMergeNode->type) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
          EXPLAIN_ROW_APPEND(EXPLAIN_IGNORE_GROUPID_FORMAT, pMergeNode->ignoreGroupId ? "true" : "false");
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_MERGE_KEYS_FORMAT);
          if (pMergeNode->groupSort) {
            EXPLAIN_ROW_APPEND(EXPLAIN_STRING_TYPE_FORMAT, "_group_id asc");
            if (LIST_LENGTH(pMergeNode->pMergeKeys) > 0) {
              EXPLAIN_ROW_APPEND(EXPLAIN_COMMA_FORMAT);
            }
          }
          for (int32_t i = 0; i < LIST_LENGTH(pMergeNode->pMergeKeys); ++i) {
            SOrderByExprNode *ptn = (SOrderByExprNode *)nodesListGetNode(pMergeNode->pMergeKeys, i);
            EXPLAIN_ROW_APPEND(EXPLAIN_STRING_TYPE_FORMAT, nodesGetNameFromColumnNode(ptn->pExpr));
            EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
            EXPLAIN_ROW_APPEND(EXPLAIN_STRING_TYPE_FORMAT, EXPLAIN_ORDER_STRING(ptn->order));
            if (i != LIST_LENGTH(pMergeNode->pMergeKeys) - 1) {
              EXPLAIN_ROW_APPEND(EXPLAIN_COMMA_FORMAT);
            }
          }
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        if (pMergeNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pMergeNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }     
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN: {
      SBlockDistScanPhysiNode *pDistScanNode = (SBlockDistScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_DISTBLK_SCAN_FORMAT, pDistScanNode->tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pDistScanNode->pScanCols->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      if (pDistScanNode->pScanPseudoCols) {
        EXPLAIN_ROW_APPEND(EXPLAIN_PSEUDO_COLUMNS_FORMAT, pDistScanNode->pScanPseudoCols->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pDistScanNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pDistScanNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pDistScanNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pDistScanNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pDistScanNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pDistScanNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pDistScanNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }       
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN: {
      SLastRowScanPhysiNode *pLastRowNode = (SLastRowScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_LASTROW_SCAN_FORMAT, pLastRowNode->scan.tableName.tname);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pLastRowNode->scan.pScanCols->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      if (pLastRowNode->scan.pScanPseudoCols) {
        EXPLAIN_ROW_APPEND(EXPLAIN_PSEUDO_COLUMNS_FORMAT, pLastRowNode->scan.pScanPseudoCols->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pLastRowNode->scan.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pLastRowNode->scan.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pLastRowNode->scan.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pLastRowNode->scan.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pLastRowNode->scan.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pLastRowNode->scan.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pLastRowNode->scan.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }        
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN: {
      STableCountScanPhysiNode *pLastRowNode = (STableCountScanPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_TABLE_COUNT_SCAN_FORMAT,
                      ('\0' != pLastRowNode->scan.tableName.tname[0] ? pLastRowNode->scan.tableName.tname : TSDB_INS_TABLE_TABLES));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, LIST_LENGTH(pLastRowNode->scan.pScanCols));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      if (pLastRowNode->scan.pScanPseudoCols) {
        EXPLAIN_ROW_APPEND(EXPLAIN_PSEUDO_COLUMNS_FORMAT, pLastRowNode->scan.pScanPseudoCols->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pLastRowNode->scan.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pLastRowNode->scan.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pLastRowNode->scan.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pLastRowNode->scan.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pLastRowNode->scan.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pLastRowNode->scan.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pLastRowNode->scan.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }        
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT: {
      SGroupSortPhysiNode *pSortNode = (SGroupSortPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_GROUP_SORT_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }

      SDataBlockDescNode *pDescNode = pSortNode->node.pOutputDataBlockDesc;
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, nodesGetOutputNumFromSlotList(pDescNode->pSlots));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pDescNode->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (EXPLAIN_MODE_ANALYZE == ctx->mode) {
        // sort key
        EXPLAIN_ROW_NEW(level + 1, "Sort Key: ");
        if (pResNode->pExecInfo) {
          for (int32_t i = 0; i < LIST_LENGTH(pSortNode->pSortKeys); ++i) {
            SOrderByExprNode *ptn = (SOrderByExprNode *)nodesListGetNode(pSortNode->pSortKeys, i);
            EXPLAIN_ROW_APPEND("%s ", nodesGetNameFromColumnNode(ptn->pExpr));
          }
        }

        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

        // sort method
        EXPLAIN_ROW_NEW(level + 1, "Sort Method: ");

        int32_t           nodeNum = taosArrayGetSize(pResNode->pExecInfo);
        SExplainExecInfo *execInfo = taosArrayGet(pResNode->pExecInfo, 0);
        SSortExecInfo    *pExecInfo = (SSortExecInfo *)execInfo->verboseInfo;
        EXPLAIN_ROW_APPEND("%s", pExecInfo->sortMethod == SORT_QSORT_T ? "quicksort" : "merge sort");
        if (pExecInfo->sortBuffer > 1024 * 1024) {
          EXPLAIN_ROW_APPEND("  Buffers:%.2f Mb", pExecInfo->sortBuffer / (1024 * 1024.0));
        } else if (pExecInfo->sortBuffer > 1024) {
          EXPLAIN_ROW_APPEND("  Buffers:%.2f Kb", pExecInfo->sortBuffer / (1024.0));
        } else {
          EXPLAIN_ROW_APPEND("  Buffers:%d b", pExecInfo->sortBuffer);
        }

        EXPLAIN_ROW_APPEND("  loops:%d", pExecInfo->loops);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));
      }

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pSortNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pSortNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pSortNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pSortNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pSortNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pSortNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }    
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL: {
      SMergeIntervalPhysiNode *pIntNode = (SMergeIntervalPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_MERGE_INTERVAL_FORMAT, nodesGetNameFromColumnNode(pIntNode->window.pTspk));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pIntNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pIntNode->window.node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pIntNode->window.node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pIntNode->window.node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pIntNode->window.node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        uint8_t precision = qExplainGetIntervalPrecision(pIntNode);
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIME_WINDOWS_FORMAT,
                        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->interval, pIntNode->intervalUnit, precision),
                        pIntNode->intervalUnit, pIntNode->offset, getPrecisionUnit(precision),
                        INVERAL_TIME_FROM_PRECISION_TO_UNIT(pIntNode->sliding, pIntNode->slidingUnit, precision),
                        pIntNode->slidingUnit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pIntNode->window.node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pIntNode->window.node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }  
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC: {
      SInterpFuncPhysiNode *pInterpNode = (SInterpFuncPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_INTERP_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      if (pInterpNode->pFuncs) {
        EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pInterpNode->pFuncs->length);
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }

      EXPLAIN_ROW_APPEND(EXPLAIN_MODE_FORMAT, nodesGetFillModeString(pInterpNode->fillMode));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);

      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pInterpNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pInterpNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pInterpNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pInterpNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        if (pInterpNode->pFillValues) {
          SNodeListNode *pValues = (SNodeListNode *)pInterpNode->pFillValues;
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILL_VALUE_FORMAT);
          SNode  *tNode = NULL;
          int32_t i = 0;
          FOREACH(tNode, pValues->pNodeList) {
            if (i) {
              EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
            }
            SValueNode *tValue = (SValueNode *)tNode;
            char       *value = nodesGetStrValueFromNode(tValue);
            EXPLAIN_ROW_APPEND(EXPLAIN_STRING_TYPE_FORMAT, value);
            taosMemoryFree(value);
            ++i;
          }

          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_INTERVAL_VALUE_FORMAT, pInterpNode->interval, pInterpNode->intervalUnit);
        EXPLAIN_ROW_END();

        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_TIMERANGE_FORMAT, pInterpNode->timeRange.skey, pInterpNode->timeRange.ekey);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pInterpNode->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pInterpNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }      
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT: {
      SEventWinodwPhysiNode *pEventNode = (SEventWinodwPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_EVENT_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pEventNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pEventNode->window.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_EVENT_START_FORMAT);
        QRY_ERR_RET(nodesNodeToSQL(pEventNode->pStartCond, tbuf + VARSTR_HEADER_SIZE,
                                    TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_EVENT_END_FORMAT);
        QRY_ERR_RET(nodesNodeToSQL(pEventNode->pEndCond, tbuf + VARSTR_HEADER_SIZE,
                                    TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN:{
      SHashJoinPhysiNode *pJoinNode = (SHashJoinPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_JOIN_FORMAT, EXPLAIN_JOIN_STRING(pJoinNode->joinType));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT, pJoinNode->pTargets->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pJoinNode->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pJoinNode->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pJoinNode->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pJoinNode->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pJoinNode->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pJoinNode->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pJoinNode->node.pConditions || pJoinNode->pFilterConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          if (pJoinNode->node.pConditions) {
            QRY_ERR_RET(nodesNodeToSQL(pJoinNode->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                       TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          }
          if (pJoinNode->pFilterConditions) {
            if (pJoinNode->node.pConditions) {
              EXPLAIN_ROW_APPEND(" AND ");
            }
            QRY_ERR_RET(nodesNodeToSQL(pJoinNode->pFilterConditions, tbuf + VARSTR_HEADER_SIZE,
                                       TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          }
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }

        bool conditionsGot = false;
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_ON_CONDITIONS_FORMAT);
        if (pJoinNode->pPrimKeyCond) {
          QRY_ERR_RET(
              nodesNodeToSQL(pJoinNode->pPrimKeyCond, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          conditionsGot = true;  
        }
        if (pJoinNode->pColEqCond) {
          if (conditionsGot) {
            EXPLAIN_ROW_APPEND(" AND ");
          }
          QRY_ERR_RET(
              nodesNodeToSQL(pJoinNode->pColEqCond, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          conditionsGot = true;  
        }
        if (pJoinNode->pTagEqCond) {
          if (conditionsGot) {
            EXPLAIN_ROW_APPEND(" AND ");
          }
          QRY_ERR_RET(
              nodesNodeToSQL(pJoinNode->pTagEqCond, tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          conditionsGot = true;  
        }
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));        
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE:{
      SGroupCachePhysiNode *pGroupCache = (SGroupCachePhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_GROUP_CACHE_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_GLOBAL_GROUP_FORMAT, pGroupCache->globalGrp);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_GROUP_BY_UID_FORMAT, pGroupCache->grpByUid);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_BATCH_SCAN_FORMAT, pGroupCache->batchFetch);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pGroupCache->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pGroupCache->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pGroupCache->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pGroupCache->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pGroupCache->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pGroupCache->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pGroupCache->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pGroupCache->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL:{
      SDynQueryCtrlPhysiNode *pDyn = (SDynQueryCtrlPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_DYN_QRY_CTRL_FORMAT, qExplainGetDynQryCtrlType(pDyn->qType));
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_BATCH_SCAN_FORMAT, pDyn->stbJoin.batchFetch);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_VGROUP_SLOT_FORMAT, pDyn->stbJoin.vgSlot[0], pDyn->stbJoin.vgSlot[1]);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_UID_SLOT_FORMAT, pDyn->stbJoin.uidSlot[0], pDyn->stbJoin.uidSlot[1]);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_SRC_SCAN_FORMAT, pDyn->stbJoin.srcScan[0], pDyn->stbJoin.srcScan[1]);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pDyn->node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_INPUT_ORDER_FORMAT, EXPLAIN_ORDER_STRING(pDyn->node.inputTsOrder));
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_OUTPUT_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_COLUMNS_FORMAT,
                           nodesGetOutputNumFromSlotList(pDyn->node.pOutputDataBlockDesc->pSlots));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
        EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pDyn->node.pOutputDataBlockDesc->outputRowSize);
        EXPLAIN_ROW_APPEND_LIMIT(pDyn->node.pLimit);
        EXPLAIN_ROW_APPEND_SLIMIT(pDyn->node.pSlimit);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));

        if (pDyn->node.pConditions) {
          EXPLAIN_ROW_NEW(level + 1, EXPLAIN_FILTER_FORMAT);
          QRY_ERR_RET(nodesNodeToSQL(pDyn->node.pConditions, tbuf + VARSTR_HEADER_SIZE,
                                     TSDB_EXPLAIN_RESULT_ROW_SIZE, &tlen));
          EXPLAIN_ROW_END();
          QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        }
      }
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT: {
      SCountWinodwPhysiNode *pCountNode = (SCountWinodwPhysiNode *)pNode;
      EXPLAIN_ROW_NEW(level, EXPLAIN_COUNT_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_LEFT_PARENTHESIS_FORMAT);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainBufAppendExecInfo(pResNode->pExecInfo, tbuf, &tlen));
        EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      }
      EXPLAIN_ROW_APPEND(EXPLAIN_FUNCTIONS_FORMAT, pCountNode->window.pFuncs->length);
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);
      EXPLAIN_ROW_APPEND(EXPLAIN_WIDTH_FORMAT, pCountNode->window.node.pOutputDataBlockDesc->totalRowSize);
      EXPLAIN_ROW_APPEND(EXPLAIN_RIGHT_PARENTHESIS_FORMAT);
      EXPLAIN_ROW_END();
      QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level));

      if (verbose) {
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_COUNT_NUM_FORMAT, pCountNode->windowCount);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
        EXPLAIN_ROW_NEW(level + 1, EXPLAIN_COUNT_SLIDING_FORMAT, pCountNode->windowSliding);
        EXPLAIN_ROW_END();
        QRY_ERR_RET(qExplainResAppendRow(ctx, tbuf, tlen, level + 1));
      }
      break;
    }
    default:
      qError("not supported physical node type %d", pNode->type);
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainResNodeToRows(SExplainResNode *pResNode, SExplainCtx *ctx, int32_t level) {
  if (NULL == pResNode) {
    qError("explain res node is NULL");
    QRY_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  int32_t code = 0;
  QRY_ERR_RET(qExplainResNodeToRowsImpl(pResNode, ctx, level));

  SNode *pNode = NULL;
  FOREACH(pNode, pResNode->pChildren) { QRY_ERR_RET(qExplainResNodeToRows((SExplainResNode *)pNode, ctx, level + 1)); }

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainAppendGroupResRows(void *pCtx, int32_t groupId, int32_t level, bool singleChannel) {
  SExplainResNode *node = NULL;
  int32_t          code = 0;
  SExplainCtx     *ctx = (SExplainCtx *)pCtx;

  SExplainGroup *group = taosHashGet(ctx->groupHash, &groupId, sizeof(groupId));
  if (NULL == group) {
    qError("group %d not in groupHash", groupId);
    QRY_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  group->singleChannel = singleChannel;
  group->physiPlanExecIdx = 0;

  QRY_ERR_RET(qExplainGenerateResNode(group->plan->pNode, group, &node));

  QRY_ERR_JRET(qExplainResNodeToRows(node, ctx, level));

_return:

  qExplainFreeResNode(node);

  QRY_RET(code);
}

int32_t qExplainGetRspFromCtx(void *ctx, SRetrieveTableRsp **pRsp) {
  SExplainCtx *pCtx = (SExplainCtx *)ctx;
  int32_t      rowNum = taosArrayGetSize(pCtx->rows);
  if (rowNum <= 0) {
    qError("empty explain res rows");
    QRY_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  SSDataBlock    *pBlock = createDataBlock();
  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, TSDB_EXPLAIN_RESULT_ROW_SIZE, 1);
  blockDataAppendColInfo(pBlock, &infoData);
  blockDataEnsureCapacity(pBlock, rowNum);

  SColumnInfoData *pInfoData = taosArrayGet(pBlock->pDataBlock, 0);

  for (int32_t i = 0; i < rowNum; ++i) {
    SQueryExplainRowInfo *row = taosArrayGet(pCtx->rows, i);
    colDataSetVal(pInfoData, i, row->buf, false);
  }

  pBlock->info.rows = rowNum;

  int32_t rspSize = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);

  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, rspSize);
  if (NULL == rsp) {
    qError("malloc SRetrieveTableRsp failed, size:%d", rspSize);
    blockDataDestroy(pBlock);
    QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  rsp->completed = 1;
  rsp->numOfRows = htobe64((int64_t)rowNum);

  int32_t len = blockEncode(pBlock, rsp->data, taosArrayGetSize(pBlock->pDataBlock));

  rsp->compLen = htonl(len);

  blockDataDestroy(pBlock);

  *pRsp = rsp;
  return TSDB_CODE_SUCCESS;
}

int32_t qExplainPrepareCtx(SQueryPlan *pDag, SExplainCtx **pCtx) {
  int32_t        code = 0;
  SNodeListNode *plans = NULL;
  int32_t        taskNum = 0;
  SExplainGroup *pGroup = NULL;
  SExplainCtx   *ctx = NULL;

  if (pDag->numOfSubplans <= 0) {
    qError("invalid subplan num:%d", pDag->numOfSubplans);
    QRY_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t levelNum = (int32_t)LIST_LENGTH(pDag->pSubplans);
  if (levelNum <= 0) {
    qError("invalid level num:%d", levelNum);
    QRY_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SHashObj *groupHash =
      taosHashInit(EXPLAIN_MAX_GROUP_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == groupHash) {
    qError("groupHash %d failed", EXPLAIN_MAX_GROUP_NUM);
    QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  QRY_ERR_JRET(
      qExplainInitCtx(&ctx, groupHash, pDag->explainInfo.verbose, pDag->explainInfo.ratio, pDag->explainInfo.mode));

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
        QRY_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
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

int32_t qExplainAppendPlanRows(SExplainCtx *pCtx) {
  if (EXPLAIN_MODE_ANALYZE != pCtx->mode) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t tlen = 0;
  char   *tbuf = pCtx->tbuf;

  EXPLAIN_SUM_ROW_NEW(EXPLAIN_RATIO_TIME_FORMAT, pCtx->ratio);
  EXPLAIN_SUM_ROW_END();
  QRY_ERR_RET(qExplainResAppendRow(pCtx, tbuf, tlen, 0));

  EXPLAIN_SUM_ROW_NEW(EXPLAIN_PLANNING_TIME_FORMAT, (double)(pCtx->jobStartTs - pCtx->reqStartTs) / 1000.0);
  EXPLAIN_SUM_ROW_END();
  QRY_ERR_RET(qExplainResAppendRow(pCtx, tbuf, tlen, 0));

  EXPLAIN_SUM_ROW_NEW(EXPLAIN_EXEC_TIME_FORMAT, (double)(pCtx->jobDoneTs - pCtx->jobStartTs) / 1000.0);
  EXPLAIN_SUM_ROW_END();
  QRY_ERR_RET(qExplainResAppendRow(pCtx, tbuf, tlen, 0));

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainGenerateRsp(SExplainCtx *pCtx, SRetrieveTableRsp **pRsp) {
  QRY_ERR_RET(qExplainAppendGroupResRows(pCtx, pCtx->rootGroupId, 0, false));
  QRY_ERR_RET(qExplainAppendPlanRows(pCtx));
  QRY_ERR_RET(qExplainGetRspFromCtx(pCtx, pRsp));

  return TSDB_CODE_SUCCESS;
}

int32_t qExplainUpdateExecInfo(SExplainCtx *pCtx, SExplainRsp *pRspMsg, int32_t groupId, SRetrieveTableRsp **pRsp) {
  SExplainResNode *node = NULL;
  int32_t          code = 0;
  bool             groupDone = false;
  SExplainCtx     *ctx = (SExplainCtx *)pCtx;

  SExplainGroup *group = taosHashGet(ctx->groupHash, &groupId, sizeof(groupId));
  if (NULL == group) {
    qError("group %d not in groupHash", groupId);
    tFreeSExplainRsp(pRspMsg);
    QRY_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  taosWLockLatch(&group->lock);
  if (NULL == group->nodeExecInfo) {
    group->nodeExecInfo = taosArrayInit(group->nodeNum, sizeof(SExplainRsp));
    if (NULL == group->nodeExecInfo) {
      qError("taosArrayInit %d explainExecInfo failed", group->nodeNum);
      tFreeSExplainRsp(pRspMsg);
      taosWUnLockLatch(&group->lock);

      QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    group->physiPlanExecNum = pRspMsg->numOfPlans;
  } else if (taosArrayGetSize(group->nodeExecInfo) >= group->nodeNum) {
    qError("group execInfo already full, size:%d, nodeNum:%d", (int32_t)taosArrayGetSize(group->nodeExecInfo),
           group->nodeNum);
    tFreeSExplainRsp(pRspMsg);
    taosWUnLockLatch(&group->lock);

    QRY_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  if (group->physiPlanExecNum != pRspMsg->numOfPlans) {
    qError("physiPlanExecNum %d mismatch with others %d in group %d", pRspMsg->numOfPlans, group->physiPlanExecNum,
           groupId);
    tFreeSExplainRsp(pRspMsg);
    taosWUnLockLatch(&group->lock);

    QRY_ERR_RET(TSDB_CODE_APP_ERROR);
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
  int32_t      code = 0;
  SExplainCtx *pCtx = NULL;

  QRY_ERR_RET(qExplainPrepareCtx(pDag, &pCtx));
  QRY_ERR_JRET(qExplainGenerateRsp(pCtx, pRsp));

_return:
  qExplainFreeCtx(pCtx);
  QRY_RET(code);
}

int32_t qExecExplainBegin(SQueryPlan *pDag, SExplainCtx **pCtx, int64_t startTs) {
  QRY_ERR_RET(qExplainPrepareCtx(pDag, pCtx));

  (*pCtx)->reqStartTs = startTs;
  (*pCtx)->jobStartTs = taosGetTimestampUs();

  return TSDB_CODE_SUCCESS;
}

int32_t qExecExplainEnd(SExplainCtx *pCtx, SRetrieveTableRsp **pRsp) {
  int32_t code = 0;
  pCtx->jobDoneTs = taosGetTimestampUs();

  atomic_store_8((int8_t *)&pCtx->execDone, true);

  if (taosHashGetSize(pCtx->groupHash) == atomic_load_32(&pCtx->groupDoneNum)) {
    if (0 == taosWTryLockLatch(&pCtx->lock)) {
      QRY_ERR_RET(qExplainGenerateRsp(pCtx, pRsp));
      // LEAVE LOCK THERE
    }
  }

  return TSDB_CODE_SUCCESS;
}
