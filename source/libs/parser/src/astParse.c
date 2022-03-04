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

#include "parserInt.h"

#include "astCreateFuncs.h"
#include "ttoken.h"

typedef void* (*FMalloc)(size_t);
typedef void (*FFree)(void*);

extern void* NewParseAlloc(FMalloc);
extern void NewParse(void*, int, SToken, void*);
extern void NewParseFree(void*, FFree);
extern void NewParseTrace(FILE*, char*);

static void setQuery(SAstCreateContext* pCxt, SQuery* pQuery) {
  pQuery->pRoot = pCxt->pRootNode;
  ENodeType type = nodeType(pCxt->pRootNode);
  if (QUERY_NODE_SELECT_STMT == type) {
    pQuery->haveResultSet = true;
    pQuery->directRpc = false;
  } else if (QUERY_NODE_CREATE_TABLE_STMT == type) {
    pQuery->haveResultSet = false;
    pQuery->directRpc = false;
  } else {
    pQuery->haveResultSet = false;
    pQuery->directRpc = true;
  }
  pQuery->msgType = (QUERY_NODE_CREATE_TABLE_STMT == type ? TDMT_VND_CREATE_TABLE : TDMT_VND_QUERY);
}

int32_t doParse(SParseContext* pParseCxt, SQuery** pQuery) {
  SAstCreateContext cxt;
  initAstCreateContext(pParseCxt, &cxt);
  void *pParser = NewParseAlloc(malloc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    if (cxt.pQueryCxt->pSql[i] == 0) {
      NewParse(pParser, 0, t0, &cxt);
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
        NewParse(pParser, 0, t0, &cxt);
        goto abort_parse;
      }
      case TK_QUESTION:
      case TK_ILLEGAL: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unrecognized token: \"%s\"", t0.z);
        cxt.valid = false;
        goto abort_parse;
      }
      case TK_HEX:
      case TK_OCT:
      case TK_BIN: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unsupported token: \"%s\"", t0.z);
        cxt.valid = false;
        goto abort_parse;
      }
      default:
        NewParse(pParser, t0.type, t0, &cxt);
        // NewParseTrace(stdout, "");
        if (!cxt.valid) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  NewParseFree(pParser, free);
  if (cxt.valid) {
    *pQuery = calloc(1, sizeof(SQuery));
    if (NULL == *pQuery) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    setQuery(&cxt, *pQuery);
  }
  return cxt.valid ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}
