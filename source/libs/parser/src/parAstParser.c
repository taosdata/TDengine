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

#include "parInt.h"

#include "parAst.h"
#include "parToken.h"

typedef void* (*FMalloc)(size_t);
typedef void (*FFree)(void*);

extern void* ParseAlloc(FMalloc);
extern void Parse(void*, int, SToken, void*);
extern void ParseFree(void*, FFree);
extern void ParseTrace(FILE*, char*);

int32_t doParse(SParseContext* pParseCxt, SQuery** pQuery) {
  SAstCreateContext cxt;
  initAstCreateContext(pParseCxt, &cxt);
  void *pParser = ParseAlloc((FMalloc)taosMemoryMalloc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    if (cxt.pQueryCxt->pSql[i] == 0) {
      Parse(pParser, 0, t0, &cxt);
      goto abort_parse;
    }
    t0.n = tGetToken((char *)&cxt.pQueryCxt->pSql[i], &t0.type);
    t0.z = (char *)(cxt.pQueryCxt->pSql + i);
    i += t0.n;

    switch (t0.type) {
      case TK_NK_SPACE:
      case TK_NK_COMMENT: {
        break;
      }
      case TK_NK_SEMI: {
        Parse(pParser, 0, t0, &cxt);
        goto abort_parse;
      }
      case TK_NK_QUESTION:
      case TK_NK_ILLEGAL: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unrecognized token: \"%s\"", t0.z);
        cxt.valid = false;
        goto abort_parse;
      }
      case TK_NK_HEX:
      case TK_NK_OCT:
      case TK_NK_BIN: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unsupported token: \"%s\"", t0.z);
        cxt.valid = false;
        goto abort_parse;
      }
      default:
        Parse(pParser, t0.type, t0, &cxt);
        // ParseTrace(stdout, "");
        if (!cxt.valid) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  ParseFree(pParser, (FFree)taosMemoryFree);
  if (cxt.valid) {
    *pQuery = taosMemoryCalloc(1, sizeof(SQuery));
    if (NULL == *pQuery) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    (*pQuery)->pRoot = cxt.pRootNode;
  }
  return cxt.valid ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}
