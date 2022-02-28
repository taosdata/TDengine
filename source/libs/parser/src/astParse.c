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

static uint32_t toNewTokenId(uint32_t tokenId) {
  switch (tokenId) {
    case TK_OR:
      return NEW_TK_OR;
    case TK_AND:
      return NEW_TK_AND;
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
    case TK_REM:
      return NEW_TK_NK_REM;
    case TK_SHOW:
      return NEW_TK_SHOW;
    case TK_DATABASES:
      return NEW_TK_DATABASES;
    case TK_INTEGER:
      return NEW_TK_NK_INTEGER;
    case TK_FLOAT:
      return NEW_TK_NK_FLOAT;
    case TK_STRING:
      return NEW_TK_NK_STRING;
    case TK_BOOL:
      return NEW_TK_NK_BOOL;
    case TK_TIMESTAMP:
      return NEW_TK_TIMESTAMP;
    case TK_VARIABLE:
      return NEW_TK_NK_VARIABLE;
    case TK_COMMA:
      return NEW_TK_NK_COMMA;
    case TK_ID:
      return NEW_TK_NK_ID;
    case TK_LP:
      return NEW_TK_NK_LP;
    case TK_RP:
      return NEW_TK_NK_RP;
    case TK_DOT:
      return NEW_TK_NK_DOT;
    case TK_BETWEEN:
      return NEW_TK_BETWEEN;
    case TK_NOT:
      return NEW_TK_NOT;
    case TK_IS:
      return NEW_TK_IS;
    case TK_NULL:
      return NEW_TK_NULL;
    case TK_LT:
      return NEW_TK_NK_LT;
    case TK_GT:
      return NEW_TK_NK_GT;
    case TK_LE:
      return NEW_TK_NK_LE;
    case TK_GE:
      return NEW_TK_NK_GE;
    case TK_NE:
      return NEW_TK_NK_NE;
    case TK_EQ:
      return NEW_TK_NK_EQ;
    case TK_LIKE:
      return NEW_TK_LIKE;
    case TK_MATCH:
      return NEW_TK_MATCH;
    case TK_NMATCH:
      return NEW_TK_NMATCH;
    case TK_IN:
      return NEW_TK_IN;
    case TK_SELECT:
      return NEW_TK_SELECT;
    case TK_DISTINCT:
      return NEW_TK_DISTINCT;
    case TK_WHERE:
      return NEW_TK_WHERE;
    case TK_AS:
      return NEW_TK_AS;
    case TK_FROM:
      return NEW_TK_FROM;
    case TK_JOIN:
      return NEW_TK_JOIN;
    // case TK_PARTITION:
    //   return NEW_TK_PARTITION;
    case TK_SESSION:
      return NEW_TK_SESSION;
    case TK_STATE_WINDOW:
      return NEW_TK_STATE_WINDOW;
    case TK_INTERVAL:
      return NEW_TK_INTERVAL;
    case TK_SLIDING:
      return NEW_TK_SLIDING;
    case TK_FILL:
      return NEW_TK_FILL;
    // case TK_VALUE:
    //   return NEW_TK_VALUE;
    case TK_NONE:
      return NEW_TK_NONE;
    case TK_PREV:
      return NEW_TK_PREV;
    case TK_LINEAR:
      return NEW_TK_LINEAR;
    // case TK_NEXT:
    //   return NEW_TK_NEXT;
    case TK_GROUP:
      return NEW_TK_GROUP;
    case TK_HAVING:
      return NEW_TK_HAVING;
    case TK_ORDER:
      return NEW_TK_ORDER;
    case TK_BY:
      return NEW_TK_BY;
    case TK_ASC:
      return NEW_TK_ASC;
    case TK_DESC:
      return NEW_TK_DESC;
    case TK_SLIMIT:
      return NEW_TK_SLIMIT;
    case TK_SOFFSET:
      return NEW_TK_SOFFSET;
    case TK_LIMIT:
      return NEW_TK_LIMIT;
    case TK_OFFSET:
      return NEW_TK_OFFSET;
    case TK_SPACE:
    case NEW_TK_ON:
    case NEW_TK_INNER:
      break;
    default:
      printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!tokenId = %d\n", tokenId);
  }
  return tokenId;
}

static uint32_t getToken(const char* z, uint32_t* tokenId) {
  uint32_t n = tGetToken(z, tokenId);
  *tokenId = toNewTokenId(*tokenId);
  return n;
}

static bool isCmd(const SNode* pRootNode) {
  if (NULL == pRootNode) {
    return true;
  }
  switch (nodeType(pRootNode)) {
    case QUERY_NODE_SELECT_STMT:
      return false;
    default:
      break;
  }
  return true;
}

int32_t doParse(SParseContext* pParseCxt, SQuery** pQuery) {
  SAstCreateContext cxt = { .pQueryCxt = pParseCxt, .notSupport = false, .valid = true, .pRootNode = NULL};
  void *pParser = NewParseAlloc(malloc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    if (cxt.pQueryCxt->pSql[i] == 0) {
      NewParse(pParser, 0, t0, &cxt);
      goto abort_parse;
    }
    t0.n = getToken((char *)&cxt.pQueryCxt->pSql[i], &t0.type);
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
    (*pQuery)->isCmd = isCmd(cxt.pRootNode);
    (*pQuery)->pRoot = cxt.pRootNode;
  }
  return cxt.valid ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}
