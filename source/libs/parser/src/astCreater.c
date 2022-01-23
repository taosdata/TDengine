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

#include "ttoken.h"
#include "astCreateFuncs.h"

extern void* NewParseAlloc(FMalloc);
extern void NewParse(void*, int, SToken*, void*);
extern void NewParseFree(void*, FFree);

SNodeList* addNodeToList(SAstCreaterContext* pCxt, SNodeList* pList, SNode* pNode) {

}

SNode* createColumnNode(SAstCreaterContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pColumnName) {

}

SNodeList* createNodeList(SAstCreaterContext* pCxt, SNode* pNode) {

}

SNode* createOrderByExprNode(SAstCreaterContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {

}

SNode* createSelectStmt(SAstCreaterContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable) {

}

SNode* createSetOperator(SAstCreaterContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {

}

SNode* createShowStmt(SAstCreaterContext* pCxt, EShowStmtType type) {

}

SNode* setProjectionAlias(SAstCreaterContext* pCxt, SNode* pNode, SToken* pAlias) {

}

SNode* doParse(SParseContext* pParseCxt) {
  SAstCreaterContext cxt = { .pQueryCxt = pParseCxt, .valid = true, .pRootNode = NULL, .mallocFunc = malloc, .freeFunc = free};
  void *pParser = NewParseAlloc(cxt.mallocFunc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};

    if (cxt.pQueryCxt->pSql[i] == 0) {
      NewParse(pParser, 0, &t0, &cxt);
      goto abort_parse;
    }

    t0.n = tGetToken((char *)&cxt.pQueryCxt->pSql[i], &t0.type);
    t0.z = (char *)(cxt.pQueryCxt->pSql + i);
    i += t0.n;

    switch (t0.type) {
      case TK_SPACE:
      case TK_COMMENT: {
        break;
      }
      case TK_SEMI: {
        NewParse(pParser, 0, &t0, &cxt);
        goto abort_parse;
      }

      case TK_QUESTION:
      case TK_ILLEGAL: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unrecognized token: \"%s\"", t0.z);
        goto abort_parse;
      }

      case TK_HEX:
      case TK_OCT:
      case TK_BIN: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unsupported token: \"%s\"", t0.z);
        goto abort_parse;
      }

      default:
        NewParse(pParser, t0.type, &t0, &cxt);
        if (!cxt.valid) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  NewParseFree(pParser, cxt.freeFunc);
  return cxt.pRootNode;
}
