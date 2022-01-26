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

#include "parserImpl.h"

#include "ttoken.h"
#include "astCreateContext.h"

typedef void* (*FMalloc)(size_t);
typedef void (*FFree)(void*);

extern void* NewParseAlloc(FMalloc);
extern void NewParse(void*, int, SToken, void*);
extern void NewParseFree(void*, FFree);

uint32_t toNewTokenId(uint32_t tokenId) {
  switch (tokenId) {
    case TK_UNION:
      return NEW_TK_UNION;
    case TK_ALL:
      return NEW_TK_ALL;
    case TK_MINUS:
      return NEW_TK_NK_MINUS;
    case TK_PLUS:
      return NEW_TK_NK_PLUS;
    case TK_STAR:
      return NEW_TK_NK_STAR;
    case TK_SLASH:
      return NEW_TK_NK_SLASH;
    case TK_SHOW:
      return NEW_TK_SHOW;
    case TK_DATABASES:
      return NEW_TK_DATABASES;
    case TK_ID:
      return NEW_TK_NK_ID;
    case TK_LP:
      return NEW_TK_NK_LP;
    case TK_RP:
      return NEW_TK_NK_RP;
    case TK_COMMA:
      return NEW_TK_NK_COMMA;
    case TK_DOT:
      return NEW_TK_NK_DOT;
    case TK_SELECT:
      return NEW_TK_SELECT;
    case TK_DISTINCT:
      return NEW_TK_DISTINCT;
    case TK_AS:
      return NEW_TK_AS;
    case TK_FROM:
      return NEW_TK_FROM;
    case TK_ORDER:
      return NEW_TK_ORDER;
    case TK_BY:
      return NEW_TK_BY;
    case TK_ASC:
      return NEW_TK_ASC;
    case TK_DESC:
      return NEW_TK_DESC;
  }
  return tokenId;
}

uint32_t getToken(const char* z, uint32_t* tokenId) {
  uint32_t n = tGetToken(z, tokenId);
  *tokenId = toNewTokenId(*tokenId);
  return n;
}

int32_t doParse(SParseContext* pParseCxt, SQuery* pQuery) {
  SAstCreateContext cxt = { .pQueryCxt = pParseCxt, .valid = true, .pRootNode = NULL };
  void *pParser = NewParseAlloc(malloc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    printf("===========================\n");
    if (cxt.pQueryCxt->pSql[i] == 0) {
      NewParse(pParser, 0, t0, &cxt);
      goto abort_parse;
    }
    printf("input: [%s]\n", cxt.pQueryCxt->pSql + i);
    t0.n = getToken((char *)&cxt.pQueryCxt->pSql[i], &t0.type);
    t0.z = (char *)(cxt.pQueryCxt->pSql + i);
    printf("token %p : %d %d [%s]\n", &t0, t0.type, t0.n, t0.z);
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
        if (!cxt.valid) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  NewParseFree(pParser, free);
  pQuery->pRoot = cxt.pRootNode;
  return cxt.valid ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}
