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

void qFreeExplainRes(SPhysiNodeExplainRes *res) {

}

int32_t qMakeExplainResChildrenInfo(SPhysiNode *pNode, void *pExecInfo, SNodeList **pChildren) {

}

int32_t qMakeExplainResNode(SPhysiNode *pNode, void *pExecInfo, SPhysiNodeExplainResNode **pRes) {
  if (NULL == pNode) {
    *pRes = NULL;
    qError("physical node is NULL");
    return TSDB_CODE_QRY_APP_ERROR;
  }

  SPhysiNodeExplainResNode *res = calloc(1, sizeof(SPhysiNodeExplainResNode));
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

int32_t qMakeTaskExplainResTree(struct SSubplan *plan, void *pExecTree, SPhysiNodeExplainResNode **pRes) {
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

int32_t qExplainResNodeAppendExecInfo(void *pExecInfo, char *tbuf) {

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


int32_t qExplainResNodeToRowsImpl(SPhysiNodeExplainResNode *pResNode, SArray *pRows, char *tbuf, int32_t level) {
  int32_t tlen = 0;
  SPhysiNode* pNode = pResNode->pNode;
  if (NULL == pNode) {
    qError("pyhsical node in explain res node is NULL");
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  switch (pNode->type) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN: {
      STagScanPhysiNode *pTagScanNode = (STagScanPhysiNode *)pNode;
      QUERY_EXPLAIN_NEWLINE(EXPLAIN_TAG_SCAN_FORMAT, pTagScanNode->tableName.tname);
      if (pResNode->pExecInfo) {
        QRY_ERR_RET(qExplainResNodeAppendExecInfo(pResNode->pExecInfo, tbuf));
      }
      QRY_ERR_RET(qExplainResAppendRow(pRows, tbuf, tlen, level));
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
    case QUERY_NODE_PHYSICAL_PLAN_JOIN:
    case QUERY_NODE_PHYSICAL_PLAN_AGG:
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW:
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
    case QUERY_NODE_PHYSICAL_PLAN_INSERT:
    default:
      qError("not supported physical node type %d", pNode->type);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qExplainResNodeToRows(SPhysiNodeExplainResNode *pResNode, SArray *pRsp, char *tbuf, int32_t level) {
  if (NULL == pResNode) {
    qError("explain res node is NULL");
    QRY_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  int32_t code = 0;
  QRY_ERR_RET(qExplainResNodeToRowsImpl(pResNode, pRsp, tbuf, level));

  SNode* pNode = NULL;
  FOREACH(pNode, pResNode->pChildren) {
    QRY_ERR_RET(qExplainResNodeToRows((SPhysiNodeExplainResNode *)pNode, pRsp, tbuf, level + 1));
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qMakeTaskExplainResRows(SPhysiNodeExplainResNode *pResNode, SRetrieveTableRsp **pRsp) {
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

  int32_t rspSize = sizeof(SRetrieveTableRsp) + ;
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, rspSize);
  if (NULL == rsp) {
    qError("malloc SRetrieveTableRsp failed");
    QRY_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  QRY_ERR_JRET(qExplainRowsToRsp(rows, rsp));  

  *pRsp = rsp;
  rsp = NULL;

_return:

  taosMemoryFree(tbuf);
  taosMemoryFree(rsp);
  taosArrayDestroy(rows);

  QRY_RET(code);
}


#pragma GCC diagnostic pop
