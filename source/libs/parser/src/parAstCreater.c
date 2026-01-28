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
#ifndef TD_ASTRA
#include <uv.h>
#endif

#include <regex.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "nodes.h"
#include "parAst.h"
#include "parUtil.h"
#include "tglobal.h"
#include "ttime.h"
#include "cmdnodes.h"
#include "osMemory.h"
#include "osString.h"
#include "parToken.h"
#include "tdef.h"
#include "tmsg.h"
#include "ttokendef.h"

#define CHECK_MAKE_NODE(p) \
  do {                     \
    if (NULL == (p)) {     \
      goto _err;           \
    }                      \
  } while (0)

#define CHECK_OUT_OF_MEM(p)   \
  do {                        \
    if (NULL == (p)) {        \
      pCxt->errCode = terrno; \
      goto _err;              \
    }                         \
  } while (0)

#define CHECK_PARSER_STATUS(pCxt)             \
  do {                                        \
    if (TSDB_CODE_SUCCESS != pCxt->errCode) { \
      goto _err;                              \
    }                                         \
  } while (0)

#define CHECK_NAME(p) \
  do {                \
    if (!p) {         \
      goto _err;      \
    }                 \
  } while (0)

#define COPY_STRING_FORM_ID_TOKEN(buf, pToken) strncpy(buf, (pToken)->z, TMIN((pToken)->n, sizeof(buf) - 1))
#define TRIM_STRING_FORM_ID_TOKEN(buf, pToken)                      \
  do {                                                              \
    if (pToken->z[0] == '`') {                                      \
      (void)trimString(pToken->z, pToken->n, buf, sizeof(buf) - 1); \
    } else {                                                        \
      COPY_STRING_FORM_ID_TOKEN(buf, pToken);                       \
    }                                                               \
  } while (0)
#define COPY_STRING_FORM_STR_TOKEN(buf, pToken)                              \
  do {                                                                       \
    if ((pToken)->n > 2) {                                                   \
      strncpy(buf, (pToken)->z + 1, TMIN((pToken)->n - 2, sizeof(buf) - 1)); \
    }                                                                        \
  } while (0)

#define COPY_COW_STR_FROM_ID_TOKEN(cow, pToken)                  \
  do {                                                           \
    if (pToken->z[0] == '`') {                                   \
      (cow) = xCreateCowStr((pToken)->n - 2, (pToken)->z + 1, true); \
    } else {                                                     \
      (cow) = xCreateCowStr((pToken)->n, (pToken)->z, true); \
    }                                                            \
  } while (0)
#define COPY_COW_STR_FROM_STR_TOKEN(cow, pToken)                     \
  do {                                                               \
    if ((pToken)->n > 2) {                                           \
      (cow) = xCreateCowStr((pToken)->n - 2, (pToken)->z + 1, true); \
    }                                                                \
  } while (0)
SToken nil_token = {.type = TK_NK_NIL, .n = 0, .z = NULL};

void initAstCreateContext(SParseContext* pParseCxt, SAstCreateContext* pCxt) {
  memset(pCxt, 0, sizeof(SAstCreateContext));
  pCxt->pQueryCxt = pParseCxt;
  pCxt->msgBuf.buf = pParseCxt->pMsg;
  pCxt->msgBuf.len = pParseCxt->msgLen;
  pCxt->notSupport = false;
  pCxt->pRootNode = NULL;
  pCxt->placeholderNo = 0;
  pCxt->pPlaceholderValues = NULL;
  pCxt->errCode = TSDB_CODE_SUCCESS;
}

static void trimEscape(SAstCreateContext* pCxt, SToken* pName, bool trimStar) {
  // todo need to deal with `ioo``ii` -> ioo`ii: done
  if (NULL != pName && pName->n > 1 && TS_ESCAPE_CHAR == pName->z[0]) {
    if (!pCxt->pQueryCxt->hasDupQuoteChar) {
      pName->z += 1;
      pName->n -= 2;
      // * is forbidden as an identifier name
      if (pName->z[0] == '*' && trimStar && pName->n == 1) {
        pName->z[0] = '\0';
        pName->n = 0;
      }
    } else {
      int32_t i = 1, j = 0;
      for (; i < pName->n - 1; ++i) {
        if ((pName->z[i] == TS_ESCAPE_CHAR) && (pName->z[i + 1] == TS_ESCAPE_CHAR)) {
          pName->z[j++] = TS_ESCAPE_CHAR;
          ++i;
        } else {
          pName->z[j++] = pName->z[i];
        }
      }
      pName->n = j;
    }
  }
}

static bool checkUserName(SAstCreateContext* pCxt, SToken* pUserName) {
  if (NULL == pUserName) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else {
    if (pUserName->n >= TSDB_USER_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    }
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    trimEscape(pCxt, pUserName, true);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}



static bool isValidSimplePassword(const char* password) {
  for (char c = *password; c != 0; c = *(++password)) {
    if (c == ' ' || c == '\'' || c == '\"' || c == '`' || c == '\\') {
      return false;
    }
  }
  return true;
}



static bool isValidPassword(SAstCreateContext* pCxt, const char* password, bool imported) {
  if (imported) {
    return strlen(password) == TSDB_PASSWORD_LEN;
  }

  if (tsEnableStrongPassword) {
    return taosIsComplexString(password);
  }

  return isValidSimplePassword(password);
}



static int32_t parsePort(SAstCreateContext* pCxt, const char* p, int32_t* pPort) {
  *pPort = taosStr2Int32(p, NULL, 10);
  if (*pPort >= UINT16_MAX || *pPort <= 0) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PORT);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t parseEndpoint(SAstCreateContext* pCxt, const SToken* pEp, char* pFqdn, int32_t* pPort) {
  if (pEp->n >= (NULL == pPort ? (TSDB_FQDN_LEN + 1 + 5) : TSDB_FQDN_LEN)) {  // format 'fqdn:port' or 'fqdn'
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
  }

  char ep[TSDB_FQDN_LEN + 1 + 5] = {0};
  COPY_STRING_FORM_ID_TOKEN(ep, pEp);
  (void)strdequote(ep);
  (void)strtrim(ep);
  if (NULL == pPort) {
    tstrncpy(pFqdn, ep, TSDB_FQDN_LEN);
    return TSDB_CODE_SUCCESS;
  }
  char* pColon = strrchr(ep, ':');
  if (NULL == pColon) {
    *pPort = tsServerPort;
    tstrncpy(pFqdn, ep, TSDB_FQDN_LEN);
    return TSDB_CODE_SUCCESS;
  }
  strncpy(pFqdn, ep, pColon - ep);
  return parsePort(pCxt, pColon + 1, pPort);
}

static bool checkAndSplitEndpoint(SAstCreateContext* pCxt, const SToken* pEp, const SToken* pPortToken, char* pFqdn,
                                  int32_t* pPort) {
  if (NULL == pEp) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return false;
  }

  if (NULL != pPortToken) {
    pCxt->errCode = parsePort(pCxt, pPortToken->z, pPort);
  }

  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode = parseEndpoint(pCxt, pEp, pFqdn, (NULL != pPortToken ? NULL : pPort));
  }

  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkObjName(SAstCreateContext* pCxt, SToken* pObjName, bool need) {
  if (NULL == pObjName || TK_NK_NIL == pObjName->type) {
    if (need) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME);
    }
  } else {
    trimEscape(pCxt, pObjName, true);
    if (pObjName->n >= TSDB_OBJ_NAME_LEN || pObjName->n == 0) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pObjName->z);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkDbName(SAstCreateContext* pCxt, SToken* pDbName, bool demandDb) {
  if (NULL == pDbName) {
    if (demandDb && NULL == pCxt->pQueryCxt->db) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DB_NOT_SPECIFIED);
    }
  } else {
    trimEscape(pCxt, pDbName, true);
    if (pDbName->n >= TSDB_DB_NAME_LEN || (demandDb && (pDbName->n == 0))) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pDbName->z);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkTableName(SAstCreateContext* pCxt, SToken* pTableName) {
  trimEscape(pCxt, pTableName, true);
  if (NULL != pTableName && pTableName->type != TK_NK_NIL &&
      (pTableName->n >= TSDB_TABLE_NAME_LEN || pTableName->n == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTableName->z);
    return false;
  }
  return true;
}

static bool checkColumnName(SAstCreateContext* pCxt, SToken* pColumnName) {
  trimEscape(pCxt, pColumnName, true);
  if (NULL != pColumnName && pColumnName->type != TK_NK_NIL &&
      (pColumnName->n >= TSDB_COL_NAME_LEN || pColumnName->n == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pColumnName->z);
    return false;
  }
  return true;
}

static bool checkIndexName(SAstCreateContext* pCxt, SToken* pIndexName) {
  trimEscape(pCxt, pIndexName, true);
  if (NULL != pIndexName && (pIndexName->n >= TSDB_INDEX_NAME_LEN || pIndexName->n == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pIndexName->z);
    return false;
  }
  return true;
}

static bool checkTopicName(SAstCreateContext* pCxt, SToken* pTopicName) {
  trimEscape(pCxt, pTopicName, true);
  if (pTopicName->n >= TSDB_TOPIC_NAME_LEN || pTopicName->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTopicName->z);
    return false;
  }
  return true;
}

static bool checkCGroupName(SAstCreateContext* pCxt, SToken* pCGroup) {
  trimEscape(pCxt, pCGroup, true);
  if (pCGroup->n >= TSDB_CGROUP_LEN || pCGroup->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pCGroup->z);
    return false;
  }
  return true;
}

static bool checkViewName(SAstCreateContext* pCxt, SToken* pViewName) {
  trimEscape(pCxt, pViewName, true);
  if (pViewName->n >= TSDB_VIEW_NAME_LEN || pViewName->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pViewName->z);
    return false;
  }
  return true;
}

static bool checkStreamName(SAstCreateContext* pCxt, SToken* pStreamName) {
  trimEscape(pCxt, pStreamName, true);
  if (pStreamName->n >= TSDB_STREAM_NAME_LEN || pStreamName->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pStreamName->z);
    return false;
  }
  return true;
}

static bool checkComment(SAstCreateContext* pCxt, const SToken* pCommentToken, bool demand) {
  if (NULL == pCommentToken) {
    pCxt->errCode = demand ? TSDB_CODE_PAR_SYNTAX_ERROR : TSDB_CODE_SUCCESS;
  } else if (pCommentToken->n >= (TSDB_TB_COMMENT_LEN + 2)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_COMMENT_TOO_LONG);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkRsmaName(SAstCreateContext* pCxt, SToken* pRsmaToken) {
  trimEscape(pCxt, pRsmaToken, true);
  if (NULL == pRsmaToken) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else if (pRsmaToken->n >= TSDB_TABLE_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_TSMA_NAME_TOO_LONG);
  } else if (pRsmaToken->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pRsmaToken->z);
  }
  return pCxt->errCode == TSDB_CODE_SUCCESS;
}

static bool checkTsmaName(SAstCreateContext* pCxt, SToken* pTsmaToken) {
  trimEscape(pCxt, pTsmaToken, true);
  if (NULL == pTsmaToken) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else if (pTsmaToken->n >= TSDB_TABLE_NAME_LEN - strlen(TSMA_RES_STB_POSTFIX)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_TSMA_NAME_TOO_LONG);
  } else if (pTsmaToken->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTsmaToken->z);
  }
  return pCxt->errCode == TSDB_CODE_SUCCESS;
}

static bool checkMountPath(SAstCreateContext* pCxt, SToken* pMountPath) {
  trimEscape(pCxt, pMountPath, true);
  if (pMountPath->n >= TSDB_MOUNT_PATH_LEN || pMountPath->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pMountPath->z);
    return false;
  }
  return true;
}

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* target = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_RAW_EXPR, (SNode**)&target);
  CHECK_MAKE_NODE(target);
  target->p = pToken->z;
  target->n = pToken->n;
  target->pNode = pNode;
  return (SNode*)target;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* target = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_RAW_EXPR, (SNode**)&target);
  CHECK_MAKE_NODE(target);
  target->p = pStart->z;
  target->n = (pEnd->z + pEnd->n) - pStart->z;
  target->pNode = pNode;
  return (SNode*)target;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SNode* setRawExprNodeIsPseudoColumn(SAstCreateContext* pCxt, SNode* pNode, bool isPseudoColumn) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pNode || QUERY_NODE_RAW_EXPR != nodeType(pNode)) {
    return pNode;
  }
  ((SRawExprNode*)pNode)->isPseudoColumn = isPseudoColumn;
  return pNode;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* pRawExpr = (SRawExprNode*)pNode;
  SNode*        pRealizedExpr = pRawExpr->pNode;
  if (nodesIsExprNode(pRealizedExpr)) {
    SExprNode* pExpr = (SExprNode*)pRealizedExpr;
    if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
      tstrncpy(pExpr->aliasName, ((SColumnNode*)pExpr)->colName, TSDB_COL_NAME_LEN);
      tstrncpy(pExpr->userAlias, ((SColumnNode*)pExpr)->colName, TSDB_COL_NAME_LEN);
    } else if (pRawExpr->isPseudoColumn) {
      // all pseudo column are translate to function with same name
      tstrncpy(pExpr->aliasName, ((SFunctionNode*)pExpr)->functionName, TSDB_COL_NAME_LEN);
      if (strcmp(((SFunctionNode*)pExpr)->functionName, "_placeholder_column") == 0) {
        SValueNode* pColId = (SValueNode*)nodesListGetNode(((SFunctionNode*)pExpr)->pParameterList, 0);
        snprintf(pExpr->userAlias, sizeof(pExpr->userAlias), "%%%%%s", pColId->literal);
      } else if (strcmp(((SFunctionNode*)pExpr)->functionName, "_placeholder_tbname") == 0) {
        tstrncpy(pExpr->userAlias, "%%tbname", TSDB_COL_NAME_LEN);
      } else {
        tstrncpy(pExpr->userAlias, ((SFunctionNode*)pExpr)->functionName, TSDB_COL_NAME_LEN);
      }
    } else {
      int32_t len = TMIN(sizeof(pExpr->aliasName) - 1, pRawExpr->n);

      // See TS-3398.
      // Len of pRawExpr->p could be larger than len of aliasName[TSDB_COL_NAME_LEN].
      // If aliasName is truncated, hash value of aliasName could be the same.
      uint64_t hashVal = MurmurHash3_64(pRawExpr->p, pRawExpr->n);
      snprintf(pExpr->aliasName, TSDB_COL_NAME_LEN, "%" PRIu64, hashVal);
      strncpy(pExpr->userAlias, pRawExpr->p, len);
      pExpr->userAlias[len] = 0;
    }
  }
  pRawExpr->pNode = NULL;
  nodesDestroyNode(pNode);
  return pRealizedExpr;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_RAW_EXPR != nodeType(pNode)) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return nil_token;
  }
  SRawExprNode* target = (SRawExprNode*)pNode;
  SToken        t = {.type = 0, .z = target->p, .n = target->n};
  return t;
}

SNodeList* createColsFuncParamNodeList(SAstCreateContext* pCxt, SNode* pNode, SNodeList* pNodeList, SToken* pAlias) {
  SRawExprNode* pRawExpr = (SRawExprNode*)pNode;
  SNode*        pFuncNode = pRawExpr->pNode;
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pNode || QUERY_NODE_RAW_EXPR != nodeType(pNode)) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  }
  CHECK_PARSER_STATUS(pCxt);
  if (pFuncNode->type != QUERY_NODE_FUNCTION) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  }
  CHECK_PARSER_STATUS(pCxt);
  SNodeList* list = NULL;
  pCxt->errCode = nodesMakeList(&list);
  CHECK_MAKE_NODE(list);
  pCxt->errCode = nodesListAppend(list, pFuncNode);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListAppendList(list, pNodeList);
  CHECK_PARSER_STATUS(pCxt);
  return list;

_err:
  nodesDestroyNode(pFuncNode);
  nodesDestroyList(pNodeList);
  return NULL;
}

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SNodeList* list = NULL;
  pCxt->errCode = nodesMakeList(&list);
  CHECK_MAKE_NODE(list);
  pCxt->errCode = nodesListAppend(list, pNode);
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    nodesDestroyList(list);
    return NULL;
  }
  return list;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListAppend(pList, pNode);
  return pList;
_err:
  nodesDestroyNode(pNode);
  nodesDestroyList(pList);
  return NULL;
}

SPrivSetArgs privArgsAdd(SAstCreateContext* pCxt, SPrivSetArgs arg1, SPrivSetArgs arg2) {
  CHECK_PARSER_STATUS(pCxt);
  SPrivSetArgs merged = arg1;
  merged.nPrivArgs += arg2.nPrivArgs;
  if (merged.nPrivArgs > TSDB_PRIV_MAX_INPUT_ARGS) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                "Invalid privilege types: exceed max privilege number:%d", TSDB_PRIV_MAX_INPUT_ARGS);
    CHECK_PARSER_STATUS(pCxt);
  }
  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    if (arg2.privSet.set[i]) {
      merged.privSet.set[i] |= arg2.privSet.set[i];
    }
  }
  if (merged.selectCols) {
    if (arg2.selectCols) {
      pCxt->errCode = nodesListAppendList((SNodeList*)merged.selectCols, (SNodeList*)arg2.selectCols);
      CHECK_PARSER_STATUS(pCxt);
    }
  } else if (arg2.selectCols) {
    merged.selectCols = arg2.selectCols;
  }
  if (LIST_LENGTH((SNodeList*)merged.selectCols) > TSDB_MAX_COLUMNS) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                "Invalid privilege columns: SELECT exceed max columns number:%d", TSDB_MAX_COLUMNS);
    CHECK_PARSER_STATUS(pCxt);
  }
  if (merged.insertCols) {
    if (arg2.insertCols) {
      pCxt->errCode = nodesListAppendList((SNodeList*)merged.insertCols, (SNodeList*)arg2.insertCols);
      CHECK_PARSER_STATUS(pCxt);
    }
  } else if (arg2.insertCols) {
    merged.insertCols = arg2.insertCols;
  }
  if (LIST_LENGTH((SNodeList*)merged.insertCols) > TSDB_MAX_COLUMNS) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                "Invalid privilege columns: INSERT exceed max columns number:%d", TSDB_MAX_COLUMNS);
    CHECK_PARSER_STATUS(pCxt);
  }
  if (merged.updateCols) {
    if (arg2.updateCols) {
      pCxt->errCode = nodesListAppendList((SNodeList*)merged.updateCols, (SNodeList*)arg2.updateCols);
      CHECK_PARSER_STATUS(pCxt);
    }
  } else if (arg2.updateCols) {
    merged.updateCols = arg2.updateCols;
  }
  if (LIST_LENGTH((SNodeList*)merged.updateCols) > TSDB_MAX_COLUMNS) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                "Invalid privilege columns: UPDATE exceed max columns number:%d", TSDB_MAX_COLUMNS);
    CHECK_PARSER_STATUS(pCxt);
  }
_err:
  return merged;
}

SPrivSetArgs privArgsSetType(SAstCreateContext* pCxt, EPrivType type) {
  CHECK_PARSER_STATUS(pCxt);
  SPrivSetArgs args = {.nPrivArgs = 1};
  privAddType(&args.privSet, type);
_err:
  return args;
}

SPrivSetArgs privArgsSetCols(SAstCreateContext* pCxt, SNodeList* selectCols, SNodeList* insertCols,
                             SNodeList* updateCols) {
  CHECK_PARSER_STATUS(pCxt);
  SPrivSetArgs args = {.nPrivArgs = 1, .selectCols = selectCols, .insertCols = insertCols, .updateCols = updateCols};
_err:
  return args;
}

SNode* createColumnNode(SAstCreateContext* pCxt, SToken* pTableAlias, SToken* pColumnName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTableName(pCxt, pTableAlias) || !checkColumnName(pCxt, pColumnName)) {
    return NULL;
  }
  SColumnNode* col = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&col);
  CHECK_MAKE_NODE(col);
  if (NULL != pTableAlias) {
    COPY_STRING_FORM_ID_TOKEN(col->tableAlias, pTableAlias);
  }
  COPY_STRING_FORM_ID_TOKEN(col->colName, pColumnName);
  return (SNode*)col;
_err:
  return NULL;
}

/**
 * @param type: 1 with mask; 0 without mask
 */
SNode* createColumnNodeExt(SAstCreateContext* pCxt, SToken* pTableAlias, SToken* pColumnName, int8_t type) {
  SNode* result = createColumnNode(pCxt, pTableAlias, pColumnName);
  if (result != NULL) {
    if (type == 1) ((SColumnNode*)result)->hasMask = 1;
  }
  return result;
}

SNode* createPlaceHolderColumnNode(SAstCreateContext* pCxt, SNode* pColId) {
  CHECK_PARSER_STATUS(pCxt);
  SFunctionNode* pFunc = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  CHECK_PARSER_STATUS(pCxt);
  tstrncpy(pFunc->functionName, "_placeholder_column", TSDB_FUNC_NAME_LEN);
  ((SValueNode*)pColId)->notReserved = true;
  pCxt->errCode = nodesListMakeAppend(&pFunc->pParameterList, pColId);
  CHECK_PARSER_STATUS(pCxt);
  pFunc->tz = pCxt->pQueryCxt->timezone;
  pFunc->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  return (SNode*)pFunc;
_err:
  return NULL;
}

static void copyValueTrimEscape(char* buf, int32_t bufLen, const SToken* pToken, bool trim) {
  int32_t len = TMIN(pToken->n, bufLen - 1);
  if (trim && (pToken->z[0] == TS_ESCAPE_CHAR)) {
    int32_t i = 1, j = 0;
    for (; i < len - 1; ++i) {
      buf[j++] = pToken->z[i];
      if (pToken->z[i] == TS_ESCAPE_CHAR) {
        if (pToken->z[i + 1] == TS_ESCAPE_CHAR) ++i;
      }
    }
    buf[j] = 0;
  } else {
    tstrncpy(buf, pToken->z, len + 1);
  }
}

SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&val);
  CHECK_MAKE_NODE(val);
  if (!(val->literal = taosMemoryMalloc(pLiteral->n + 1))) {
    pCxt->errCode = terrno;
    nodesDestroyNode((SNode*)val);
    return NULL;
  }
  copyValueTrimEscape(val->literal, pLiteral->n + 1, pLiteral,
                      pCxt->pQueryCxt->hasDupQuoteChar && (TK_NK_ID == pLiteral->type));
  if (TK_NK_STRING == pLiteral->type) {
    (void)trimString(pLiteral->z, pLiteral->n, val->literal, pLiteral->n);
  }
  val->node.resType.type = dataType;
  val->node.resType.bytes = IS_VAR_DATA_TYPE(dataType) ? strlen(val->literal) : tDataTypes[dataType].bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
  val->translate = false;
  val->tz = pCxt->pQueryCxt->timezone;
  val->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  return (SNode*)val;
_err:
  return NULL;
}

SNode* createRawValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&val);
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, pCxt->errCode, "");
    goto _exit;
  }
  if (pLiteral) {
    val->literal = taosStrndup(pLiteral->z, pLiteral->n);
    if (!val->literal) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, terrno, "Out of memory");
      goto _exit;
    }
  } else if (pNode) {
    SRawExprNode* pRawExpr = (SRawExprNode*)pNode;
    if (!nodesIsExprNode(pRawExpr->pNode)) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pRawExpr->p);
      goto _exit;
    }
    val->literal = taosStrndup(pRawExpr->p, pRawExpr->n);
    if (!val->literal) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, terrno, "Out of memory");
      goto _exit;
    }
  } else {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTERNAL_ERROR, "Invalid parameters");
    goto _exit;
  }
  if (!val->literal) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY, "Out of memory");
    goto _exit;
  }

  val->node.resType.type = dataType;
  val->node.resType.bytes = IS_VAR_DATA_TYPE(dataType) ? strlen(val->literal) : tDataTypes[dataType].bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
_exit:
  nodesDestroyNode(pNode);
  if (pCxt->errCode != 0) {
    nodesDestroyNode((SNode*)val);
    return NULL;
  }
  return (SNode*)val;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SNode* createRawValueNodeExt(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral, SNode* pLeft,
                             SNode* pRight) {
  SValueNode* val = NULL;
  CHECK_PARSER_STATUS(pCxt);

  pCxt->errCode = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&val);
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, pCxt->errCode, "");
    goto _exit;
  }
  if (pLiteral) {
    if (!(val->literal = taosStrndup(pLiteral->z, pLiteral->n))) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, terrno, "Out of memory");
      goto _exit;
    }
  } else {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTERNAL_ERROR, "Invalid parameters");
    goto _exit;
  }

  val->node.resType.type = dataType;
  val->node.resType.bytes = IS_VAR_DATA_TYPE(dataType) ? strlen(val->literal) : tDataTypes[dataType].bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
_exit:
  nodesDestroyNode(pLeft);
  nodesDestroyNode(pRight);
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)val;
_err:
  nodesDestroyNode((SNode*)val);
  nodesDestroyNode(pLeft);
  nodesDestroyNode(pRight);
  return NULL;
}

static bool hasHint(SNodeList* pHintList, EHintOption hint) {
  if (!pHintList) return false;
  SNode* pNode;
  FOREACH(pNode, pHintList) {
    SHintNode* pHint = (SHintNode*)pNode;
    if (pHint->option == hint) {
      return true;
    }
  }
  return false;
}

bool addHintNodeToList(SAstCreateContext* pCxt, SNodeList** ppHintList, EHintOption opt, SToken* paramList,
                       int32_t paramNum) {
  void* value = NULL;
  switch (opt) {
    case HINT_SKIP_TSMA:
    case HINT_BATCH_SCAN:
    case HINT_NO_BATCH_SCAN: {
      if (paramNum > 0) {
        return true;
      }
      break;
    }
    case HINT_SORT_FOR_GROUP:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_PARTITION_FIRST)) return true;
      break;
    case HINT_PARTITION_FIRST:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_SORT_FOR_GROUP)) return true;
      break;
    case HINT_PARA_TABLES_SORT:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_PARA_TABLES_SORT)) return true;
      break;
    case HINT_SMALLDATA_TS_SORT:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_SMALLDATA_TS_SORT)) return true;
      break;
    case HINT_HASH_JOIN:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_HASH_JOIN)) return true;
      break;
    case HINT_WIN_OPTIMIZE_BATCH:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_WIN_OPTIMIZE_BATCH)) return true;
      break;
    case HINT_WIN_OPTIMIZE_SINGLE:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_WIN_OPTIMIZE_SINGLE)) return true;
      break;
    default:
      return true;
  }

  SHintNode* hint = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_HINT, (SNode**)&hint);
  if (!hint) {
    return true;
  }
  hint->option = opt;
  hint->value = value;

  if (NULL == *ppHintList) {
    pCxt->errCode = nodesMakeList(ppHintList);
    if (!*ppHintList) {
      nodesDestroyNode((SNode*)hint);
      return true;
    }
  }

  pCxt->errCode = nodesListStrictAppend(*ppHintList, (SNode*)hint);
  if (pCxt->errCode) {
    return true;
  }

  return false;
}

SNodeList* createHintNodeList(SAstCreateContext* pCxt, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pLiteral || pLiteral->n <= 5) {
    return NULL;
  }
  SNodeList* pHintList = NULL;
  char*      hint = taosStrndup(pLiteral->z + 3, pLiteral->n - 5);
  if (!hint) return NULL;
  int32_t     i = 0;
  bool        quit = false;
  bool        inParamList = false;
  bool        lastComma = false;
  EHintOption opt = 0;
  int32_t     paramNum = 0;
  SToken      paramList[10];
  while (!quit) {
    SToken t0 = {0};
    if (hint[i] == 0) {
      break;
    }
    t0.n = tGetToken(&hint[i], &t0.type, NULL);
    t0.z = hint + i;
    i += t0.n;

    switch (t0.type) {
      case TK_BATCH_SCAN:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_BATCH_SCAN;
        break;
      case TK_NO_BATCH_SCAN:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_NO_BATCH_SCAN;
        break;
      case TK_SORT_FOR_GROUP:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_SORT_FOR_GROUP;
        break;
      case TK_PARTITION_FIRST:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_PARTITION_FIRST;
        break;
      case TK_PARA_TABLES_SORT:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_PARA_TABLES_SORT;
        break;
      case TK_SMALLDATA_TS_SORT:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_SMALLDATA_TS_SORT;
        break;
      case TK_HASH_JOIN:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_HASH_JOIN;
        break;
      case TK_SKIP_TSMA:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_SKIP_TSMA;
        break;
      case TK_WIN_OPTIMIZE_BATCH:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_WIN_OPTIMIZE_BATCH;
        break;
      case TK_WIN_OPTIMIZE_SINGLE:
        lastComma = false;
        if (0 != opt || inParamList) {
          quit = true;
          break;
        }
        opt = HINT_WIN_OPTIMIZE_SINGLE;
        break;
      case TK_NK_LP:
        lastComma = false;
        if (0 == opt || inParamList) {
          quit = true;
        }
        inParamList = true;
        break;
      case TK_NK_RP:
        lastComma = false;
        if (0 == opt || !inParamList) {
          quit = true;
        } else {
          quit = addHintNodeToList(pCxt, &pHintList, opt, paramList, paramNum);
          inParamList = false;
          paramNum = 0;
          opt = 0;
        }
        break;
      case TK_NK_ID:
        lastComma = false;
        if (0 == opt || !inParamList) {
          quit = true;
        } else {
          paramList[paramNum++] = t0;
        }
        break;
      case TK_NK_COMMA:
        if (lastComma) {
          quit = true;
        }
        lastComma = true;
        break;
      case TK_NK_SPACE:
        break;
      default:
        lastComma = false;
        quit = true;
        break;
    }
  }

  taosMemoryFree(hint);
  return pHintList;
_err:
  return NULL;
}

SNode* createIdentifierValueNode(SAstCreateContext* pCxt, SToken* pLiteral) {
  trimEscape(pCxt, pLiteral, false);
  return createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, pLiteral);
}

SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&val);
  CHECK_MAKE_NODE(val);
  if (pLiteral->type == TK_NK_STRING) {
    // like '100s' or "100d"
    // check format: ^[0-9]+[smwbauhdny]$'
    if (pLiteral->n < 4) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
      return NULL;
    }
    char unit = pLiteral->z[pLiteral->n - 2];
    switch (unit) {
      case 'a':
      case 'b':
      case 'd':
      case 'h':
      case 'm':
      case 's':
      case 'u':
      case 'w':
      case 'y':
      case 'n':
        break;
      default:
        pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
        return NULL;
    }
    for (uint32_t i = 1; i < pLiteral->n - 2; ++i) {
      if (!isdigit(pLiteral->z[i])) {
        pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
        return NULL;
      }
    }
    val->literal = taosStrndup(pLiteral->z + 1, pLiteral->n - 2);
  } else {
    val->literal = taosStrndup(pLiteral->z, pLiteral->n);
  }
  if (!val->literal) {
    nodesDestroyNode((SNode*)val);
    pCxt->errCode = terrno;
    return NULL;
  }
  val->flag |= VALUE_FLAG_IS_DURATION;
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  val->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
_err:
  return NULL;
}

SNode* createTimeOffsetValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&val);
  CHECK_MAKE_NODE(val);
  if (pLiteral->type == TK_NK_STRING) {
    // like '100s' or "100d"
    // check format: ^[0-9]+[smwbauhdny]$'
    if (pLiteral->n < 4) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
      return NULL;
    }
    char unit = pLiteral->z[pLiteral->n - 2];
    switch (unit) {
      case 'a':
      case 'b':
      case 'd':
      case 'h':
      case 'm':
      case 's':
      case 'u':
      case 'w':
        break;
      default:
        pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
        return NULL;
    }
    for (uint32_t i = 1; i < pLiteral->n - 2; ++i) {
      if (!isdigit(pLiteral->z[i])) {
        pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
        return NULL;
      }
    }
    val->literal = taosStrndup(pLiteral->z + 1, pLiteral->n - 2);
  } else {
    val->literal = taosStrndup(pLiteral->z, pLiteral->n);
  }
  if (!val->literal) {
    nodesDestroyNode((SNode*)val);
    pCxt->errCode = terrno;
    return NULL;
  }
  val->flag |= VALUE_FLAG_IS_TIME_OFFSET;
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  val->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
_err:
  return NULL;
}

SNode* createDefaultDatabaseCondValue(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pCxt->pQueryCxt->db) {
    return NULL;
  }

  SValueNode* val = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&val);
  CHECK_MAKE_NODE(val);
  val->literal = taosStrdup(pCxt->pQueryCxt->db);
  if (!val->literal) {
    pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
    nodesDestroyNode((SNode*)val);
    return NULL;
  }
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BINARY;
  val->node.resType.bytes = strlen(val->literal);
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
_err:
  return NULL;
}

SNode* createPlaceholderValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pCxt->pQueryCxt->pStmtCb) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
    return NULL;
  }
  SValueNode* val = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&val);
  CHECK_MAKE_NODE(val);
  val->literal = taosStrndup(pLiteral->z, pLiteral->n);
  if (!val->literal) {
    pCxt->errCode = terrno;
    nodesDestroyNode((SNode*)val);
    return NULL;
  }
  val->placeholderNo = ++pCxt->placeholderNo;
  if (NULL == pCxt->pPlaceholderValues) {
    pCxt->pPlaceholderValues = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    if (NULL == pCxt->pPlaceholderValues) {
      nodesDestroyNode((SNode*)val);
      return NULL;
    }
  }
  if (NULL == taosArrayPush(pCxt->pPlaceholderValues, &val)) {
    pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
    nodesDestroyNode((SNode*)val);
    taosArrayDestroy(pCxt->pPlaceholderValues);
    return NULL;
  }
  return (SNode*)val;
_err:
  return NULL;
}

SNode* createDurationPlaceholderValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  SNode* pNode = createPlaceholderValueNode(pCxt, pLiteral);
  if (pNode != NULL) {
    SValueNode* val = (SValueNode*)pNode;
    val->flag |= VALUE_FLAG_IS_DURATION;
    val->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    val->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
  return pNode;
}

static int32_t addParamToLogicConditionNode(SLogicConditionNode* pCond, SNode* pParam) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pParam) && pCond->condType == ((SLogicConditionNode*)pParam)->condType &&
      ((SLogicConditionNode*)pParam)->condType != LOGIC_COND_TYPE_NOT) {
    int32_t code = nodesListAppendList(pCond->pParameterList, ((SLogicConditionNode*)pParam)->pParameterList);
    ((SLogicConditionNode*)pParam)->pParameterList = NULL;
    nodesDestroyNode(pParam);
    return code;
  } else {
    return nodesListAppend(pCond->pParameterList, pParam);
  }
}

SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2) {
  CHECK_PARSER_STATUS(pCxt);
  SLogicConditionNode* cond = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&cond);
  CHECK_MAKE_NODE(cond);
  cond->condType = type;
  cond->pParameterList = NULL;
  pCxt->errCode = nodesMakeList(&cond->pParameterList);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode = addParamToLogicConditionNode(cond, pParam1);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode && NULL != pParam2) {
    pCxt->errCode = addParamToLogicConditionNode(cond, pParam2);
  }
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    nodesDestroyNode((SNode*)cond);
    return NULL;
  }
  return (SNode*)cond;
_err:
  nodesDestroyNode(pParam1);
  nodesDestroyNode(pParam2);
  return NULL;
}

static uint8_t getMinusDataType(uint8_t orgType) {
  switch (orgType) {
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return TSDB_DATA_TYPE_BIGINT;
    default:
      break;
  }
  return orgType;
}

SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  if (OP_TYPE_MINUS == type && QUERY_NODE_VALUE == nodeType(pLeft)) {
    SValueNode* pVal = (SValueNode*)pLeft;
    char*       pNewLiteral = taosMemoryCalloc(1, strlen(pVal->literal) + 2);
    if (!pNewLiteral) {
      pCxt->errCode = terrno;
      goto _err;
    }
    if ('+' == pVal->literal[0]) {
      snprintf(pNewLiteral, strlen(pVal->literal) + 2, "-%s", pVal->literal + 1);
    } else if ('-' == pVal->literal[0]) {
      snprintf(pNewLiteral, strlen(pVal->literal) + 2, "%s", pVal->literal + 1);
    } else {
      snprintf(pNewLiteral, strlen(pVal->literal) + 2, "-%s", pVal->literal);
    }
    taosMemoryFree(pVal->literal);
    pVal->literal = pNewLiteral;
    pVal->node.resType.type = getMinusDataType(pVal->node.resType.type);
    return pLeft;
  }
  if (pLeft && QUERY_NODE_VALUE == nodeType(pLeft)) {
    SValueNode* pVal = (SValueNode*)pLeft;
    pVal->tz = pCxt->pQueryCxt->timezone;
  }
  if (pRight && QUERY_NODE_VALUE == nodeType(pRight)) {
    SValueNode* pVal = (SValueNode*)pRight;
    pVal->tz = pCxt->pQueryCxt->timezone;
  }

  SOperatorNode* op = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&op);
  CHECK_MAKE_NODE(op);
  op->opType = type;
  op->pLeft = pLeft;
  op->pRight = pRight;
  op->tz = pCxt->pQueryCxt->timezone;
  op->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  return (SNode*)op;
_err:
  nodesDestroyNode(pLeft);
  nodesDestroyNode(pRight);
  return NULL;
}

SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  SNode *pNew = NULL, *pGE = NULL, *pLE = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesCloneNode(pExpr, &pNew);
  CHECK_PARSER_STATUS(pCxt);
  pGE = createOperatorNode(pCxt, OP_TYPE_GREATER_EQUAL, pExpr, pLeft);
  CHECK_PARSER_STATUS(pCxt);
  pLE = createOperatorNode(pCxt, OP_TYPE_LOWER_EQUAL, pNew, pRight);
  CHECK_PARSER_STATUS(pCxt);
  SNode* pRet = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, pGE, pLE);
  CHECK_PARSER_STATUS(pCxt);
  return pRet;
_err:
  nodesDestroyNode(pNew);
  nodesDestroyNode(pGE);
  nodesDestroyNode(pLE);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pLeft);
  nodesDestroyNode(pRight);
  return NULL;
}

SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  SNode *pNew = NULL, *pLT = NULL, *pGT = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesCloneNode(pExpr, &pNew);
  CHECK_PARSER_STATUS(pCxt);
  pLT = createOperatorNode(pCxt, OP_TYPE_LOWER_THAN, pExpr, pLeft);
  CHECK_PARSER_STATUS(pCxt);
  pGT = createOperatorNode(pCxt, OP_TYPE_GREATER_THAN, pNew, pRight);
  CHECK_PARSER_STATUS(pCxt);
  SNode* pRet = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, pLT, pGT);
  CHECK_PARSER_STATUS(pCxt);
  return pRet;
_err:
  nodesDestroyNode(pNew);
  nodesDestroyNode(pGT);
  nodesDestroyNode(pLT);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pLeft);
  nodesDestroyNode(pRight);
  return NULL;
}

static SNode* createPrimaryKeyCol(SAstCreateContext* pCxt, const SToken* pFuncName) {
  CHECK_PARSER_STATUS(pCxt);
  SColumnNode* pCol = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  CHECK_MAKE_NODE(pCol);
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  if (NULL == pFuncName) {
    tstrncpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME, TSDB_COL_NAME_LEN);
  } else {
    strncpy(pCol->colName, pFuncName->z, pFuncName->n);
  }
  pCol->isPrimTs = true;
  return (SNode*)pCol;
_err:
  return NULL;
}

SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList) {
  CHECK_PARSER_STATUS(pCxt);
  if (0 == strncasecmp("_rowts", pFuncName->z, pFuncName->n) || 0 == strncasecmp("_c0", pFuncName->z, pFuncName->n)) {
    return createPrimaryKeyCol(pCxt, pFuncName);
  }
  SFunctionNode* func = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  COPY_STRING_FORM_ID_TOKEN(func->functionName, pFuncName);
  func->pParameterList = pParameterList;
  func->tz = pCxt->pQueryCxt->timezone;
  func->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  return (SNode*)func;
_err:
  nodesDestroyList(pParameterList);
  return NULL;
}

SNode* createPHTbnameFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList) {
  CHECK_PARSER_STATUS(pCxt);
  SFunctionNode* func = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  tstrncpy(func->functionName, "_placeholder_tbname", TSDB_FUNC_NAME_LEN);
  func->pParameterList = pParameterList;
  func->tz = pCxt->pQueryCxt->timezone;
  func->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  return (SNode*)func;
_err:
  nodesDestroyList(pParameterList);
  return NULL;
}

SNode* createCastFunctionNode(SAstCreateContext* pCxt, SNode* pExpr, SDataType dt) {
  SFunctionNode* func = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  tstrncpy(func->functionName, "cast", TSDB_FUNC_NAME_LEN);
  func->node.resType = dt;
  if (TSDB_DATA_TYPE_VARCHAR == dt.type || TSDB_DATA_TYPE_GEOMETRY == dt.type || TSDB_DATA_TYPE_VARBINARY == dt.type) {
    func->node.resType.bytes = func->node.resType.bytes + VARSTR_HEADER_SIZE;
  } else if (TSDB_DATA_TYPE_NCHAR == dt.type) {
    func->node.resType.bytes = func->node.resType.bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  }
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr);
  CHECK_PARSER_STATUS(pCxt);
  func->tz = pCxt->pQueryCxt->timezone;
  func->charsetCxt = pCxt->pQueryCxt->charsetCxt;

  return (SNode*)func;
_err:
  nodesDestroyNode((SNode*)func);
  nodesDestroyNode(pExpr);
  return NULL;
}

SNode* createPositionFunctionNode(SAstCreateContext* pCxt, SNode* pExpr, SNode* pExpr2) {
  SFunctionNode* func = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  tstrncpy(func->functionName, "position", TSDB_FUNC_NAME_LEN);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)func;
_err:
  nodesDestroyNode((SNode*)func);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pExpr2);
  return NULL;
}

SNode* createTrimFunctionNode(SAstCreateContext* pCxt, SNode* pExpr, ETrimType type) {
  SFunctionNode* func = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  tstrncpy(func->functionName, "trim", TSDB_FUNC_NAME_LEN);
  func->trimType = type;
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr);
  CHECK_PARSER_STATUS(pCxt);
  func->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  return (SNode*)func;
_err:
  nodesDestroyNode((SNode*)func);
  nodesDestroyNode(pExpr);
  return NULL;
}

SNode* createTrimFunctionNodeExt(SAstCreateContext* pCxt, SNode* pExpr, SNode* pExpr2, ETrimType type) {
  SFunctionNode* func = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  tstrncpy(func->functionName, "trim", TSDB_FUNC_NAME_LEN);
  func->trimType = type;
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  func->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  return (SNode*)func;
_err:
  nodesDestroyNode((SNode*)func);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pExpr2);
  return NULL;
}

SNode* createSubstrFunctionNode(SAstCreateContext* pCxt, SNode* pExpr, SNode* pExpr2) {
  SFunctionNode* func = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  tstrncpy(func->functionName, "substr", TSDB_FUNC_NAME_LEN);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)func;
_err:
  nodesDestroyNode((SNode*)func);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pExpr2);
  return NULL;
}

SNode* createSubstrFunctionNodeExt(SAstCreateContext* pCxt, SNode* pExpr, SNode* pExpr2, SNode* pExpr3) {
  SFunctionNode* func = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&func);
  CHECK_MAKE_NODE(func);
  tstrncpy(func->functionName, "substr", TSDB_FUNC_NAME_LEN);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListMakeAppend(&func->pParameterList, pExpr3);
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)func;
_err:
  nodesDestroyNode((SNode*)func);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pExpr2);
  nodesDestroyNode(pExpr3);
  return NULL;
}

SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList) {
  SNodeListNode* list = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&list);
  CHECK_MAKE_NODE(list);
  list->pNodeList = pList;
  return (SNode*)list;
_err:
  nodesDestroyList(pList);
  return NULL;
}

SNode* createNodeListNodeEx(SAstCreateContext* pCxt, SNode* p1, SNode* p2) {
  SNodeListNode* list = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&list);
  CHECK_MAKE_NODE(list);
  pCxt->errCode = nodesListMakeStrictAppend(&list->pNodeList, p1);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListStrictAppend(list->pNodeList, p2);
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)list;
_err:
  nodesDestroyNode((SNode*)list);
  nodesDestroyNode(p1);
  nodesDestroyNode(p2);
  return NULL;
}

SNode* createRealTableNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pTableAlias) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, true));
  CHECK_NAME(checkTableName(pCxt, pTableName));
  CHECK_NAME(checkTableName(pCxt, pTableAlias));
  SRealTableNode* realTable = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&realTable);
  CHECK_MAKE_NODE(realTable);
  if (NULL != pDbName) {
    COPY_STRING_FORM_ID_TOKEN(realTable->table.dbName, pDbName);
  } else {
    snprintf(realTable->table.dbName, sizeof(realTable->table.dbName), "%s", pCxt->pQueryCxt->db);
  }
  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    COPY_STRING_FORM_ID_TOKEN(realTable->table.tableAlias, pTableAlias);
  } else {
    COPY_STRING_FORM_ID_TOKEN(realTable->table.tableAlias, pTableName);
  }
  COPY_STRING_FORM_ID_TOKEN(realTable->table.tableName, pTableName);
  return (SNode*)realTable;
_err:
  return NULL;
}

SNode* createPlaceHolderTableNode(SAstCreateContext* pCxt, EStreamPlaceholder type, SToken* pTableAlias) {
  CHECK_PARSER_STATUS(pCxt);

  SPlaceHolderTableNode* phTable = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_PLACE_HOLDER_TABLE, (SNode**)&phTable);
  CHECK_MAKE_NODE(phTable);

  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    COPY_STRING_FORM_ID_TOKEN(phTable->table.tableAlias, pTableAlias);
  }

  phTable->placeholderType = type;
  return (SNode*)phTable;
_err:
  return NULL;
}

SNode* createStreamNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pStreamName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, true));
  CHECK_NAME(checkStreamName(pCxt, pStreamName));
  SStreamNode* pStream = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STREAM, (SNode**)&pStream);
  CHECK_MAKE_NODE(pStream);
  if (NULL != pDbName) {
    COPY_STRING_FORM_ID_TOKEN(pStream->dbName, pDbName);
  } else {
    snprintf(pStream->dbName, sizeof(pStream->dbName), "%s", pCxt->pQueryCxt->db);
  }
  COPY_STRING_FORM_ID_TOKEN(pStream->streamName, pStreamName);
  return (SNode*)pStream;
_err:
  return NULL;
}

SNode* createRecalcRange(SAstCreateContext* pCxt, SNode* pStart, SNode* pEnd) {
  SStreamCalcRangeNode* pRange = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STREAM_CALC_RANGE, (SNode**)&pRange);
  CHECK_MAKE_NODE(pRange);
  if (NULL == pStart && NULL == pEnd) {
    pRange->calcAll = true;
  } else {
    pRange->calcAll = false;
    pRange->pStart = pStart;
    pRange->pEnd = pEnd;
  }

  return (SNode*)pRange;
_err:
  nodesDestroyNode((SNode*)pRange);
  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  return NULL;
}

SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, SToken* pTableAlias) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTableName(pCxt, pTableAlias)) {
    return NULL;
  }
  STempTableNode* tempTable = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_TEMP_TABLE, (SNode**)&tempTable);
  CHECK_MAKE_NODE(tempTable);
  tempTable->pSubquery = pSubquery;
  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    COPY_STRING_FORM_ID_TOKEN(tempTable->table.tableAlias, pTableAlias);
  } else {
    taosRandStr(tempTable->table.tableAlias, 32);
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(pSubquery)) {
    tstrncpy(((SSelectStmt*)pSubquery)->stmtName, tempTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
    ((SSelectStmt*)pSubquery)->isSubquery = true;
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pSubquery)) {
    tstrncpy(((SSetOperator*)pSubquery)->stmtName, tempTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  }
  return (SNode*)tempTable;
_err:
  nodesDestroyNode(pSubquery);
  return NULL;
}

SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, EJoinSubType stype, SNode* pLeft, SNode* pRight,
                           SNode* pJoinCond) {
  CHECK_PARSER_STATUS(pCxt);
  SJoinTableNode* joinTable = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_JOIN_TABLE, (SNode**)&joinTable);
  CHECK_MAKE_NODE(joinTable);
  joinTable->joinType = type;
  joinTable->subType = stype;
  joinTable->pLeft = pLeft;
  joinTable->pRight = pRight;
  joinTable->pOnCond = pJoinCond;
  return (SNode*)joinTable;
_err:
  nodesDestroyNode(pLeft);
  nodesDestroyNode(pRight);
  nodesDestroyNode(pJoinCond);
  return NULL;
}

SNode* createViewNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pViewName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, true));
  CHECK_NAME(checkViewName(pCxt, pViewName));
  SViewNode* pView = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_VIEW, (SNode**)&pView);
  CHECK_MAKE_NODE(pView);
  if (NULL != pDbName) {
    COPY_STRING_FORM_ID_TOKEN(pView->table.dbName, pDbName);
  } else {
    snprintf(pView->table.dbName, sizeof(pView->table.dbName), "%s", pCxt->pQueryCxt->db);
  }
  COPY_STRING_FORM_ID_TOKEN(pView->table.tableName, pViewName);
  return (SNode*)pView;
_err:
  return NULL;
}

SNode* createLimitNode(SAstCreateContext* pCxt, SNode* pLimit, SNode* pOffset) {
  CHECK_PARSER_STATUS(pCxt);
  SLimitNode* limitNode = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_LIMIT, (SNode**)&limitNode);
  CHECK_MAKE_NODE(limitNode);
  limitNode->limit = (SValueNode*)pLimit;
  if (NULL != pOffset) {
    limitNode->offset = (SValueNode*)pOffset;
  }
  return (SNode*)limitNode;
_err:
  return NULL;
}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {
  CHECK_PARSER_STATUS(pCxt);
  SOrderByExprNode* orderByExpr = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&orderByExpr);
  CHECK_MAKE_NODE(orderByExpr);
  orderByExpr->pExpr = pExpr;
  orderByExpr->order = order;
  if (NULL_ORDER_DEFAULT == nullOrder) {
    nullOrder = (ORDER_ASC == order ? NULL_ORDER_FIRST : NULL_ORDER_LAST);
  }
  orderByExpr->nullOrder = nullOrder;
  return (SNode*)orderByExpr;
_err:
  nodesDestroyNode(pExpr);
  return NULL;
}

SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, SNode* pGap) {
  CHECK_PARSER_STATUS(pCxt);
  SSessionWindowNode* session = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SESSION_WINDOW, (SNode**)&session);
  CHECK_MAKE_NODE(session);
  session->pCol = (SColumnNode*)pCol;
  session->pGap = (SValueNode*)pGap;
  return (SNode*)session;
_err:
  nodesDestroyNode(pCol);
  nodesDestroyNode(pGap);
  return NULL;
}

SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pExpr, SNodeList* pOptions, SNode* pTrueForLimit) {
  SStateWindowNode* state = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STATE_WINDOW, (SNode**)&state);
  CHECK_MAKE_NODE(state);
  state->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(state->pCol);
  state->pExpr = pExpr;
  state->pTrueForLimit = pTrueForLimit;
  if (pOptions != NULL) {
    if (pOptions->length >= 1) {
      pCxt->errCode = nodesCloneNode(nodesListGetNode(pOptions, 0), &state->pExtend);
      CHECK_MAKE_NODE(state->pExtend);
    }
    if (pOptions->length == 2) {
      pCxt->errCode = nodesCloneNode(nodesListGetNode(pOptions, 1), &state->pZeroth);
      CHECK_MAKE_NODE(state->pZeroth);
    }
    nodesDestroyList(pOptions);
  }
  return (SNode*)state;
_err:
  nodesDestroyNode((SNode*)state);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pTrueForLimit);
  nodesDestroyList(pOptions);
  return NULL;
}

SNode* createEventWindowNode(SAstCreateContext* pCxt, SNode* pStartCond, SNode* pEndCond, SNode* pTrueForLimit) {
  SEventWindowNode* pEvent = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_EVENT_WINDOW, (SNode**)&pEvent);
  CHECK_MAKE_NODE(pEvent);
  pEvent->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(pEvent->pCol);
  pEvent->pStartCond = pStartCond;
  pEvent->pEndCond = pEndCond;
  pEvent->pTrueForLimit = pTrueForLimit;
  return (SNode*)pEvent;
_err:
  nodesDestroyNode((SNode*)pEvent);
  nodesDestroyNode(pStartCond);
  nodesDestroyNode(pEndCond);
  nodesDestroyNode(pTrueForLimit);
  return NULL;
}

SNode* createCountWindowNode(SAstCreateContext* pCxt, const SToken* pCountToken, const SToken* pSlidingToken,
                             SNodeList* pColList) {
  SCountWindowNode* pCount = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COUNT_WINDOW, (SNode**)&pCount);
  CHECK_MAKE_NODE(pCount);
  pCount->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(pCount->pCol);
  pCount->windowCount = taosStr2Int64(pCountToken->z, NULL, 10);
  if (pSlidingToken == NULL) {
    pCount->windowSliding = taosStr2Int64(pSlidingToken->z, NULL, 10);
  } else {
    pCount->windowSliding = taosStr2Int64(pCountToken->z, NULL, 10);
  }
  pCount->pColList = pColList;
  return (SNode*)pCount;
_err:
  nodesDestroyNode((SNode*)pCount);
  return NULL;
}

SNode* createCountWindowNodeFromArgs(SAstCreateContext* pCxt, SNode* arg) {
  SCountWindowArgs* args = (SCountWindowArgs*)arg;
  SCountWindowNode* pCount = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COUNT_WINDOW, (SNode**)&pCount);
  CHECK_MAKE_NODE(pCount);
  pCount->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(pCount->pCol);
  pCount->windowCount = args->count;
  pCount->windowSliding = args->sliding;
  pCount->pColList = args->pColList;
  args->pColList = NULL;
  nodesDestroyNode(arg);
  return (SNode*)pCount;
_err:
  nodesDestroyNode((SNode*)pCount);
  return NULL;
}

SNode* createCountWindowArgs(SAstCreateContext* pCxt, const SToken* countToken, const SToken* slidingToken,
                             SNodeList* colList) {
  CHECK_PARSER_STATUS(pCxt);

  SCountWindowArgs* args = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COUNT_WINDOW_ARGS, (SNode**)&args);
  CHECK_MAKE_NODE(args);
  args->count = taosStr2Int64(countToken->z, NULL, 10);
  if (slidingToken && slidingToken->type == TK_NK_INTEGER) {
    args->sliding = taosStr2Int64(slidingToken->z, NULL, 10);
  } else {
    args->sliding = taosStr2Int64(countToken->z, NULL, 10);
  }
  args->pColList = colList;
  return (SNode*)args;
_err:
  return NULL;
}

SNode* createAnomalyWindowNode(SAstCreateContext* pCxt, SNode* pExpr, const SToken* pFuncOpt) {
  SAnomalyWindowNode* pAnomaly = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ANOMALY_WINDOW, (SNode**)&pAnomaly);
  CHECK_MAKE_NODE(pAnomaly);
  pAnomaly->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(pAnomaly->pCol);
  pAnomaly->pExpr = pExpr;
  if (pFuncOpt == NULL) {
    tstrncpy(pAnomaly->anomalyOpt, "algo=iqr", TSDB_ANALYTIC_ALGO_OPTION_LEN);
  } else {
    (void)trimString(pFuncOpt->z, pFuncOpt->n, pAnomaly->anomalyOpt, sizeof(pAnomaly->anomalyOpt));
  }
  return (SNode*)pAnomaly;
_err:
  nodesDestroyNode((SNode*)pAnomaly);
  return NULL;
}

SNode* createIntervalWindowNodeExt(SAstCreateContext* pCxt, SNode* pInter, SNode* pSliding) {
  SIntervalWindowNode* pInterval = NULL;
  CHECK_PARSER_STATUS(pCxt);
  if (pInter) {
    pInterval = (SIntervalWindowNode*)pInter;
  } else {
    pCxt->errCode = nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW, (SNode**)&pInterval);
    CHECK_MAKE_NODE(pInterval);
  }
  pInterval->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(pInterval->pCol);
  pInterval->pSliding = ((SSlidingWindowNode*)pSliding)->pSlidingVal;
  pInterval->pSOffset = ((SSlidingWindowNode*)pSliding)->pOffset;
  return (SNode*)pInterval;
_err:
  nodesDestroyNode((SNode*)pInter);
  nodesDestroyNode((SNode*)pInterval);
  nodesDestroyNode((SNode*)pSliding);
  return NULL;
}

SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding,
                                SNode* pFill) {
  SIntervalWindowNode* interval = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW, (SNode**)&interval);
  CHECK_MAKE_NODE(interval);
  interval->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(interval->pCol);
  interval->pInterval = pInterval;
  interval->pOffset = pOffset;
  interval->pSliding = pSliding;
  interval->pFill = pFill;
  TAOS_SET_OBJ_ALIGNED(&interval->timeRange, TSWINDOW_INITIALIZER);
  interval->timezone = pCxt->pQueryCxt->timezone;
  return (SNode*)interval;
_err:
  nodesDestroyNode((SNode*)interval);
  nodesDestroyNode(pInterval);
  nodesDestroyNode(pOffset);
  nodesDestroyNode(pSliding);
  nodesDestroyNode(pFill);
  return NULL;
}

SNode* createPeriodWindowNode(SAstCreateContext* pCxt, SNode* pPeriodTime, SNode* pOffset) {
  SPeriodWindowNode* pPeriod = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_PERIOD_WINDOW, (SNode**)&pPeriod);
  CHECK_MAKE_NODE(pPeriod);
  pPeriod->pOffset = pOffset;
  pPeriod->pPeroid = pPeriodTime;
  return (SNode*)pPeriod;
_err:
  nodesDestroyNode((SNode*)pOffset);
  nodesDestroyNode((SNode*)pPeriodTime);
  nodesDestroyNode((SNode*)pPeriod);
  return NULL;
}

SNode* createWindowOffsetNode(SAstCreateContext* pCxt, SNode* pStartOffset, SNode* pEndOffset) {
  SWindowOffsetNode* winOffset = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_WINDOW_OFFSET, (SNode**)&winOffset);
  CHECK_MAKE_NODE(winOffset);
  winOffset->pStartOffset = pStartOffset;
  winOffset->pEndOffset = pEndOffset;
  return (SNode*)winOffset;
_err:
  nodesDestroyNode((SNode*)winOffset);
  nodesDestroyNode(pStartOffset);
  nodesDestroyNode(pEndOffset);
  return NULL;
}

SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues) {
  SFillNode* fill = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FILL, (SNode**)&fill);
  CHECK_MAKE_NODE(fill);
  fill->mode = mode;
  fill->pValues = pValues;
  fill->pWStartTs = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&(fill->pWStartTs));
  CHECK_MAKE_NODE(fill->pWStartTs);
  tstrncpy(((SFunctionNode*)fill->pWStartTs)->functionName, "_wstart", TSDB_FUNC_NAME_LEN);
  return (SNode*)fill;
_err:
  nodesDestroyNode((SNode*)fill);
  nodesDestroyNode(pValues);
  return NULL;
}

SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode) {
  SGroupingSetNode* groupingSet = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&groupingSet);
  CHECK_MAKE_NODE(groupingSet);
  groupingSet->groupingSetType = GP_TYPE_NORMAL;
  groupingSet->pParameterList = NULL;
  pCxt->errCode = nodesListMakeAppend(&groupingSet->pParameterList, pNode);
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)groupingSet;
_err:
  nodesDestroyNode((SNode*)groupingSet);
  nodesDestroyNode(pNode);
  return NULL;
}

SNode* createInterpTimeRange(SAstCreateContext* pCxt, SNode* pStart, SNode* pEnd, SNode* pInterval) {
  CHECK_PARSER_STATUS(pCxt);
  if (isSubQueryNode(pStart) || isSubQueryNode(pEnd) || isSubQueryNode(pInterval)) {
    pCxt->errCode = TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_USAGE;
    CHECK_PARSER_STATUS(pCxt);
  }

  if (NULL == pInterval) {
    if (pEnd && nodeType(pEnd) == QUERY_NODE_VALUE && ((SValueNode*)pEnd)->flag & VALUE_FLAG_IS_DURATION) {
      return createInterpTimeAround(pCxt, pStart, NULL, pEnd);
    }
    return createBetweenAnd(pCxt, createPrimaryKeyCol(pCxt, NULL), pStart, pEnd);
  }

  return createInterpTimeAround(pCxt, pStart, pEnd, pInterval);

_err:

  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  nodesDestroyNode(pInterval);
  return NULL;
}

SNode* createInterpTimePoint(SAstCreateContext* pCxt, SNode* pPoint) {
  CHECK_PARSER_STATUS(pCxt);
  if (isSubQueryNode(pPoint)) {
    pCxt->errCode = TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_USAGE;
    CHECK_PARSER_STATUS(pCxt);
  }

  return createOperatorNode(pCxt, OP_TYPE_EQUAL, createPrimaryKeyCol(pCxt, NULL), pPoint);
_err:
  nodesDestroyNode(pPoint);
  return NULL;
}

SNode* createInterpTimeAround(SAstCreateContext* pCxt, SNode* pStart, SNode* pEnd, SNode* pInterval) {
  CHECK_PARSER_STATUS(pCxt);
  SRangeAroundNode* pAround = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_RANGE_AROUND, (SNode**)&pAround);
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pEnd) {
    pAround->pRange = createInterpTimePoint(pCxt, pStart);
  } else {
    pAround->pRange = createBetweenAnd(pCxt, createPrimaryKeyCol(pCxt, NULL), pStart, pEnd);
  }
  pAround->pInterval = pInterval;
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)pAround;
_err:
  return NULL;
}

SNode* createWhenThenNode(SAstCreateContext* pCxt, SNode* pWhen, SNode* pThen) {
  CHECK_PARSER_STATUS(pCxt);
  SWhenThenNode* pWhenThen = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_WHEN_THEN, (SNode**)&pWhenThen);
  CHECK_MAKE_NODE(pWhenThen);
  pWhenThen->pWhen = pWhen;
  pWhenThen->pThen = pThen;
  return (SNode*)pWhenThen;
_err:
  nodesDestroyNode(pWhen);
  nodesDestroyNode(pThen);
  return NULL;
}

static int32_t debugPrintNode(SNode* pNode) {
  char*   pStr = NULL;
  int32_t code = nodesNodeToString(pNode, false, &pStr, NULL);
  if (TSDB_CODE_SUCCESS == code) {
    (void)printf("%s\n", pStr);
    taosMemoryFree(pStr);
  }
  return code;
}

SNode* createCaseWhenNode(SAstCreateContext* pCxt, SNode* pCase, SNodeList* pWhenThenList, SNode* pElse) {
  CHECK_PARSER_STATUS(pCxt);
  SCaseWhenNode* pCaseWhen = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CASE_WHEN, (SNode**)&pCaseWhen);
  CHECK_MAKE_NODE(pCaseWhen);
  pCaseWhen->pCase = pCase;
  pCaseWhen->pWhenThenList = pWhenThenList;
  pCaseWhen->pElse = pElse;
  pCaseWhen->tz = pCxt->pQueryCxt->timezone;
  pCaseWhen->charsetCxt = pCxt->pQueryCxt->charsetCxt;
  // debugPrintNode((SNode*)pCaseWhen);
  return (SNode*)pCaseWhen;
_err:
  nodesDestroyNode(pCase);
  nodesDestroyList(pWhenThenList);
  nodesDestroyNode(pElse);
  return NULL;
}

SNode* createNullIfNode(SAstCreateContext* pCxt, SNode* pExpr1, SNode* pExpr2) {
  SNode *    pCase = NULL, *pEqual = NULL, *pThen = NULL;
  SNode *    pWhenThenNode = NULL, *pElse = NULL;
  SNodeList* pWhenThenList = NULL;
  SNode*     pCaseWhen = NULL;

  CHECK_PARSER_STATUS(pCxt);
  pEqual = createOperatorNode(pCxt, OP_TYPE_EQUAL, pExpr1, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  SToken nullToken = {
      .n = 4,
      .type = TK_NULL,
      .z = "null",
  };
  pThen = createValueNode(pCxt, TSDB_DATA_TYPE_NULL, &nullToken);
  CHECK_PARSER_STATUS(pCxt);
  pWhenThenNode = createWhenThenNode(pCxt, pEqual, pThen);
  CHECK_PARSER_STATUS(pCxt);
  pWhenThenList = createNodeList(pCxt, pWhenThenNode);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesCloneNode(pExpr1, &pElse);
  CHECK_PARSER_STATUS(pCxt);
  pCaseWhen = createCaseWhenNode(pCxt, pCase, pWhenThenList, pElse);
  CHECK_PARSER_STATUS(pCxt);
  // debugPrintNode((SNode*)pCaseWhen);
  return (SNode*)pCaseWhen;
_err:
  nodesDestroyNode(pCase);
  nodesDestroyNode(pEqual);
  nodesDestroyNode(pThen);
  nodesDestroyNode(pWhenThenNode);
  nodesDestroyNode(pElse);
  nodesDestroyList(pWhenThenList);
  return NULL;
}

SNode* createIfNode(SAstCreateContext* pCxt, SNode* pExpr1, SNode* pExpr2, SNode* pExpr3) {
  SNode*     pCase = NULL;
  SNode*     pWhenThenNode = NULL;
  SNodeList* pWhenThenList = NULL;
  SNode*     pCaseWhen = NULL;

  CHECK_PARSER_STATUS(pCxt);
  pWhenThenNode = createWhenThenNode(pCxt, pExpr1, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  pWhenThenList = createNodeList(pCxt, pWhenThenNode);
  CHECK_PARSER_STATUS(pCxt);
  pCaseWhen = createCaseWhenNode(pCxt, pCase, pWhenThenList, pExpr3);
  CHECK_PARSER_STATUS(pCxt);
  // debugPrintNode((SNode*)pCaseWhen);
  return (SNode*)pCaseWhen;
_err:
  nodesDestroyNode(pCase);
  nodesDestroyNode(pWhenThenNode);
  nodesDestroyList(pWhenThenList);
  return NULL;
}

SNode* createNvlNode(SAstCreateContext* pCxt, SNode* pExpr1, SNode* pExpr2) {
  SNode *    pThen = NULL, *pEqual = NULL;
  SNode*     pWhenThenNode = NULL;
  SNodeList* pWhenThenList = NULL;
  SNode*     pCaseWhen = NULL;

  CHECK_PARSER_STATUS(pCxt);
  pEqual = createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, pExpr1, NULL);
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesCloneNode(pExpr1, &pThen);
  CHECK_PARSER_STATUS(pCxt);
  pWhenThenNode = createWhenThenNode(pCxt, pEqual, pThen);
  CHECK_PARSER_STATUS(pCxt);
  pWhenThenList = createNodeList(pCxt, pWhenThenNode);
  CHECK_PARSER_STATUS(pCxt);
  pCaseWhen = createCaseWhenNode(pCxt, NULL, pWhenThenList, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  // debugPrintNode((SNode*)pCaseWhen);
  return (SNode*)pCaseWhen;
_err:
  nodesDestroyNode(pEqual);
  nodesDestroyNode(pThen);
  nodesDestroyNode(pWhenThenNode);
  nodesDestroyList(pWhenThenList);
  return NULL;
}

SNode* createNvl2Node(SAstCreateContext* pCxt, SNode* pExpr1, SNode* pExpr2, SNode* pExpr3) {
  SNode *    pEqual = NULL, *pWhenThenNode = NULL;
  SNodeList* pWhenThenList = NULL;
  SNode*     pCaseWhen = NULL;

  CHECK_PARSER_STATUS(pCxt);
  pEqual = createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, pExpr1, NULL);
  CHECK_PARSER_STATUS(pCxt);
  pWhenThenNode = createWhenThenNode(pCxt, pEqual, pExpr2);
  CHECK_PARSER_STATUS(pCxt);
  pWhenThenList = createNodeList(pCxt, pWhenThenNode);
  CHECK_PARSER_STATUS(pCxt);
  pCaseWhen = createCaseWhenNode(pCxt, NULL, pWhenThenList, pExpr3);
  CHECK_PARSER_STATUS(pCxt);
  // debugPrintNode((SNode*)pCaseWhen);
  return (SNode*)pCaseWhen;
_err:
  nodesDestroyNode(pEqual);
  nodesDestroyNode(pWhenThenNode);
  nodesDestroyList(pWhenThenList);
  return NULL;
}

SNode* createCoalesceNode(SAstCreateContext* pCxt, SNodeList* pParamList) {
  int32_t    sizeParam = LIST_LENGTH(pParamList);
  SNode *    pNotNullCond = NULL, *pWhenThenNode = NULL, *pExpr = NULL;
  SNodeList* pWhenThenList = NULL;
  SNode *    pCaseWhen = NULL, *pThen = NULL;

  CHECK_PARSER_STATUS(pCxt);

  for (int i = 0; i < sizeParam; ++i) {
    pExpr = nodesListGetNode(pParamList, i);

    pCxt->errCode = nodesCloneNode(pExpr, &pThen);
    CHECK_PARSER_STATUS(pCxt);

    pNotNullCond = createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, pExpr, NULL);
    CHECK_PARSER_STATUS(pCxt);

    pWhenThenNode = createWhenThenNode(pCxt, pNotNullCond, pThen);
    CHECK_PARSER_STATUS(pCxt);

    if (!pWhenThenList) {
      pWhenThenList = createNodeList(pCxt, pWhenThenNode);
    } else {
      pCxt->errCode = nodesListAppend(pWhenThenList, pWhenThenNode);
    }
    CHECK_PARSER_STATUS(pCxt);
  }

  pCaseWhen = createCaseWhenNode(pCxt, NULL, pWhenThenList, NULL);
  CHECK_PARSER_STATUS(pCxt);
  // debugPrintNode((SNode*)pCaseWhen);
  return (SNode*)pCaseWhen;
_err:
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pNotNullCond);
  nodesDestroyNode(pThen);
  nodesDestroyNode(pWhenThenNode);
  nodesDestroyList(pWhenThenList);
  return NULL;
}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, SToken* pAlias) {
  CHECK_PARSER_STATUS(pCxt);
  trimEscape(pCxt, pAlias, false);
  SExprNode* pExpr = (SExprNode*)pNode;
  int32_t    len = TMIN(sizeof(pExpr->aliasName) - 1, pAlias->n);
  strncpy(pExpr->aliasName, pAlias->z, len);
  pExpr->aliasName[len] = '\0';
  strncpy(pExpr->userAlias, pAlias->z, len);
  pExpr->userAlias[len] = '\0';
  pExpr->asAlias = true;
  return pNode;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWhere = pWhere;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pWhere);
  return NULL;
}

SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pPartitionByList = pPartitionByList;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyList(pPartitionByList);
  return NULL;
}

SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWindow = pWindow;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pWindow);
  return NULL;
}

SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pGroupByList = pGroupByList;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyList(pGroupByList);
  return NULL;
}

SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pHaving = pHaving;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pHaving);
  return NULL;
}

SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pOrderByList) {
    return pStmt;
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pOrderByList = pOrderByList;
  } else {
    ((SSetOperator*)pStmt)->pOrderByList = pOrderByList;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyList(pOrderByList);
  return NULL;
}

SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pSlimit) {
    return pStmt;
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pSlimit = (SLimitNode*)pSlimit;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pSlimit);
  return NULL;
}

SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pLimit) {
    return pStmt;
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pLimit = (SLimitNode*)pLimit;
  } else {
    ((SSetOperator*)pStmt)->pLimit = pLimit;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pLimit);
  return NULL;
}

SNode* addRangeClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pRange) {
  CHECK_PARSER_STATUS(pCxt);
  SSelectStmt* pSelect = (SSelectStmt*)pStmt;
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    if (pRange && nodeType(pRange) == QUERY_NODE_RANGE_AROUND) {
      pSelect->pRangeAround = pRange;
      SRangeAroundNode* pAround = (SRangeAroundNode*)pRange;
      TSWAP(pSelect->pRange, pAround->pRange);
    } else {
      pSelect->pRange = pRange;
    }
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pRange);
  return NULL;
}

SNode* addEveryClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pEvery) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pEvery = pEvery;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pEvery);
  return NULL;
}

SNode* addFillClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pFill) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt) && NULL != pFill) {
    SFillNode* pFillClause = (SFillNode*)pFill;
    nodesDestroyNode(pFillClause->pWStartTs);
    pFillClause->pWStartTs = createPrimaryKeyCol(pCxt, NULL);
    CHECK_MAKE_NODE(pFillClause->pWStartTs);
    ((SSelectStmt*)pStmt)->pFill = (SNode*)pFillClause;
  }
  return pStmt;
_err:
  nodesDestroyNode(pStmt);
  nodesDestroyNode(pFill);
  return NULL;
}

SNode* addJLimitClause(SAstCreateContext* pCxt, SNode* pJoin, SNode* pJLimit) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pJLimit) {
    return pJoin;
  }
  SJoinTableNode* pJoinNode = (SJoinTableNode*)pJoin;
  pJoinNode->pJLimit = pJLimit;

  return pJoin;
_err:
  nodesDestroyNode(pJoin);
  nodesDestroyNode(pJLimit);
  return NULL;
}

SNode* addWindowOffsetClause(SAstCreateContext* pCxt, SNode* pJoin, SNode* pWinOffset) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pWinOffset) {
    return pJoin;
  }
  SJoinTableNode* pJoinNode = (SJoinTableNode*)pJoin;
  pJoinNode->pWindowOffset = pWinOffset;

  return pJoin;
_err:
  nodesDestroyNode(pJoin);
  nodesDestroyNode(pWinOffset);
  return NULL;
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable,
                        SNodeList* pHint) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* select = NULL;
  pCxt->errCode = createSelectStmtImpl(isDistinct, pProjectionList, pTable, pHint, &select);
  CHECK_MAKE_NODE(select);
  return select;
_err:
  nodesDestroyList(pProjectionList);
  nodesDestroyNode(pTable);
  nodesDestroyList(pHint);
  return NULL;
}

SNode* setSelectStmtTagMode(SAstCreateContext* pCxt, SNode* pStmt, bool bSelectTags) {
  if (pStmt && QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    if (pCxt->pQueryCxt->biMode) {
      ((SSelectStmt*)pStmt)->tagScan = true;
    } else {
      ((SSelectStmt*)pStmt)->tagScan = bSelectTags;
    }
  }
  return pStmt;
}

static void setSubquery(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->isSubquery = true;
  }
}

SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  SSetOperator* setOp = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SET_OPERATOR, (SNode**)&setOp);
  CHECK_MAKE_NODE(setOp);
  setOp->opType = type;
  setOp->pLeft = pLeft;
  setSubquery(setOp->pLeft);
  setOp->pRight = pRight;
  setSubquery(setOp->pRight);
  snprintf(setOp->stmtName, TSDB_TABLE_NAME_LEN, "%p", setOp);
  return (SNode*)setOp;
_err:
  nodesDestroyNode(pLeft);
  nodesDestroyNode(pRight);
  return NULL;
}

static void updateWalOptionsDefault(SDatabaseOptions* pOptions) {
  if (!pOptions->walRetentionPeriodIsSet) {
    pOptions->walRetentionPeriod =
        pOptions->replica > 1 ? TSDB_REPS_DEF_DB_WAL_RET_PERIOD : TSDB_REP_DEF_DB_WAL_RET_PERIOD;
  }
  if (!pOptions->walRetentionSizeIsSet) {
    pOptions->walRetentionSize = pOptions->replica > 1 ? TSDB_REPS_DEF_DB_WAL_RET_SIZE : TSDB_REP_DEF_DB_WAL_RET_SIZE;
  }
  if (!pOptions->walRollPeriodIsSet) {
    pOptions->walRollPeriod =
        pOptions->replica > 1 ? TSDB_REPS_DEF_DB_WAL_ROLL_PERIOD : TSDB_REP_DEF_DB_WAL_ROLL_PERIOD;
  }
}

SNode* createDefaultDatabaseOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SDatabaseOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->buffer = TSDB_DEFAULT_BUFFER_PER_VNODE;
  pOptions->cacheModel = TSDB_DEFAULT_CACHE_MODEL;
  pOptions->cacheLastSize = TSDB_DEFAULT_CACHE_SIZE;
  pOptions->compressionLevel = TSDB_DEFAULT_COMP_LEVEL;
  pOptions->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  pOptions->fsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
  pOptions->maxRowsPerBlock = TSDB_DEFAULT_MAXROWS_FBLOCK;
  pOptions->minRowsPerBlock = TSDB_DEFAULT_MINROWS_FBLOCK;
  pOptions->keep[0] = TSDB_DEFAULT_KEEP;
  pOptions->keep[1] = TSDB_DEFAULT_KEEP;
  pOptions->keep[2] = TSDB_DEFAULT_KEEP;
  pOptions->pages = TSDB_DEFAULT_PAGES_PER_VNODE;
  pOptions->pagesize = TSDB_DEFAULT_PAGESIZE_PER_VNODE;
  pOptions->tsdbPageSize = TSDB_DEFAULT_TSDB_PAGESIZE;
  pOptions->precision = TSDB_DEFAULT_PRECISION;
  pOptions->replica = TSDB_DEFAULT_DB_REPLICA;
  pOptions->strict = TSDB_DEFAULT_DB_STRICT;
  pOptions->walLevel = TSDB_DEFAULT_WAL_LEVEL;
  pOptions->numOfVgroups = TSDB_DEFAULT_VN_PER_DB;
  pOptions->singleStable = TSDB_DEFAULT_DB_SINGLE_STABLE;
  pOptions->schemaless = TSDB_DEFAULT_DB_SCHEMALESS;
  updateWalOptionsDefault(pOptions);
  pOptions->walSegmentSize = TSDB_DEFAULT_DB_WAL_SEGMENT_SIZE;
  pOptions->sstTrigger = TSDB_DEFAULT_SST_TRIGGER;
  pOptions->tablePrefix = TSDB_DEFAULT_HASH_PREFIX;
  pOptions->tableSuffix = TSDB_DEFAULT_HASH_SUFFIX;
  pOptions->ssChunkSize = TSDB_DEFAULT_SS_CHUNK_SIZE;
  pOptions->ssKeepLocal = TSDB_DEFAULT_SS_KEEP_LOCAL;
  pOptions->ssCompact = TSDB_DEFAULT_SS_COMPACT;
  pOptions->withArbitrator = TSDB_DEFAULT_DB_WITH_ARBITRATOR;
  pOptions->encryptAlgorithm = TSDB_DEFAULT_ENCRYPT_ALGO;
  pOptions->dnodeListStr[0] = 0;
  pOptions->compactInterval = TSDB_DEFAULT_COMPACT_INTERVAL;
  pOptions->compactStartTime = TSDB_DEFAULT_COMPACT_START_TIME;
  pOptions->compactEndTime = TSDB_DEFAULT_COMPACT_END_TIME;
  pOptions->compactTimeOffset = TSDB_DEFAULT_COMPACT_TIME_OFFSET;
  pOptions->encryptAlgorithmStr[0] = 0;
  pOptions->isAudit = 0;
  return (SNode*)pOptions;
_err:
  return NULL;
}

SNode* createAlterDatabaseOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SDatabaseOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->buffer = -1;
  pOptions->cacheModel = -1;
  pOptions->cacheLastSize = -1;
  pOptions->compressionLevel = -1;
  pOptions->daysPerFile = -1;
  pOptions->fsyncPeriod = -1;
  pOptions->maxRowsPerBlock = -1;
  pOptions->minRowsPerBlock = -1;
  pOptions->keep[0] = -1;
  pOptions->keep[1] = -1;
  pOptions->keep[2] = -1;
  pOptions->pages = -1;
  pOptions->pagesize = -1;
  pOptions->tsdbPageSize = -1;
  pOptions->precision = -1;
  pOptions->replica = -1;
  pOptions->strict = -1;
  pOptions->walLevel = -1;
  pOptions->numOfVgroups = -1;
  pOptions->singleStable = -1;
  pOptions->schemaless = -1;
  pOptions->walRetentionPeriod = -2;  // -1 is a valid value
  pOptions->walRetentionSize = -2;    // -1 is a valid value
  pOptions->walRollPeriod = -1;
  pOptions->walSegmentSize = -1;
  pOptions->sstTrigger = -1;
  pOptions->tablePrefix = -1;
  pOptions->tableSuffix = -1;
  pOptions->ssChunkSize = -1;
  pOptions->ssKeepLocal = -1;
  pOptions->ssCompact = -1;
  pOptions->withArbitrator = -1;
  pOptions->encryptAlgorithm = TSDB_DEFAULT_ENCRYPT_ALGO;
  pOptions->dnodeListStr[0] = 0;
  pOptions->compactInterval = -1;
  pOptions->compactStartTime = -1;
  pOptions->compactEndTime = -1;
  pOptions->compactTimeOffset = -1;
  pOptions->encryptAlgorithmStr[0] = 0;
  pOptions->isAudit = 0;
  return (SNode*)pOptions;
_err:
  return NULL;
}

static SNode* setDatabaseOptionImpl(SAstCreateContext* pCxt, SNode* pOptions, EDatabaseOptionType type, void* pVal,
                                    bool alter) {
  CHECK_PARSER_STATUS(pCxt);
  SDatabaseOptions* pDbOptions = (SDatabaseOptions*)pOptions;
  switch (type) {
    case DB_OPTION_BUFFER:
      pDbOptions->buffer = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_CACHEMODEL:
      COPY_STRING_FORM_STR_TOKEN(pDbOptions->cacheModelStr, (SToken*)pVal);
      break;
    case DB_OPTION_CACHESIZE:
      pDbOptions->cacheLastSize = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_COMP:
      pDbOptions->compressionLevel = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_DAYS: {
      SToken* pToken = pVal;
      if (TK_NK_INTEGER == pToken->type) {
        pDbOptions->daysPerFile = taosStr2Int32(pToken->z, NULL, 10) * 1440;
      } else {
        pDbOptions->pDaysPerFile = (SValueNode*)createDurationValueNode(pCxt, pToken);
      }
      break;
    }
    case DB_OPTION_FSYNC:
      pDbOptions->fsyncPeriod = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_MAXROWS:
      pDbOptions->maxRowsPerBlock = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_MINROWS:
      pDbOptions->minRowsPerBlock = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_KEEP:
      pDbOptions->pKeep = pVal;
      break;
    case DB_OPTION_PAGES:
      pDbOptions->pages = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_PAGESIZE:
      pDbOptions->pagesize = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_TSDB_PAGESIZE:
      pDbOptions->tsdbPageSize = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_PRECISION:
      COPY_STRING_FORM_STR_TOKEN(pDbOptions->precisionStr, (SToken*)pVal);
      break;
    case DB_OPTION_REPLICA:
      pDbOptions->replica = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      pDbOptions->withArbitrator = (pDbOptions->replica == 2);
      if (!alter) {
        updateWalOptionsDefault(pDbOptions);
      }
      break;
    case DB_OPTION_STRICT:
      COPY_STRING_FORM_STR_TOKEN(pDbOptions->strictStr, (SToken*)pVal);
      break;
    case DB_OPTION_WAL:
      pDbOptions->walLevel = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_VGROUPS:
      pDbOptions->numOfVgroups = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_SINGLE_STABLE:
      pDbOptions->singleStable = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_RETENTIONS:
      pDbOptions->pRetentions = pVal;
      break;
    case DB_OPTION_WAL_RETENTION_PERIOD:
      pDbOptions->walRetentionPeriod = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      pDbOptions->walRetentionPeriodIsSet = true;
      break;
    case DB_OPTION_WAL_RETENTION_SIZE:
      pDbOptions->walRetentionSize = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      pDbOptions->walRetentionSizeIsSet = true;
      break;
    case DB_OPTION_WAL_ROLL_PERIOD:
      pDbOptions->walRollPeriod = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      pDbOptions->walRollPeriodIsSet = true;
      break;
    case DB_OPTION_WAL_SEGMENT_SIZE:
      pDbOptions->walSegmentSize = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_STT_TRIGGER:
      pDbOptions->sstTrigger = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_TABLE_PREFIX: {
      SValueNode* pNode = (SValueNode*)pVal;
      if (TSDB_DATA_TYPE_BIGINT == pNode->node.resType.type || TSDB_DATA_TYPE_UBIGINT == pNode->node.resType.type) {
        pDbOptions->tablePrefix = taosStr2Int32(pNode->literal, NULL, 10);
      } else {
        snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid table_prefix data type");
        pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
      }
      nodesDestroyNode((SNode*)pNode);
      break;
    }
    case DB_OPTION_TABLE_SUFFIX: {
      SValueNode* pNode = (SValueNode*)pVal;
      if (TSDB_DATA_TYPE_BIGINT == pNode->node.resType.type || TSDB_DATA_TYPE_UBIGINT == pNode->node.resType.type) {
        pDbOptions->tableSuffix = taosStr2Int32(pNode->literal, NULL, 10);
      } else {
        snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid table_suffix data type");
        pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
      }
      nodesDestroyNode((SNode*)pNode);
      break;
    }
    case DB_OPTION_SS_CHUNKPAGES:
      pDbOptions->ssChunkSize = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_SS_KEEPLOCAL: {
      SToken* pToken = pVal;
      if (TK_NK_INTEGER == pToken->type) {
        pDbOptions->ssKeepLocal = taosStr2Int32(pToken->z, NULL, 10) * 1440;
      } else {
        pDbOptions->ssKeepLocalStr = (SValueNode*)createDurationValueNode(pCxt, pToken);
      }
      break;
    }
    case DB_OPTION_SS_COMPACT:
      pDbOptions->ssCompact = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_KEEP_TIME_OFFSET:
      if (TK_NK_INTEGER == ((SToken*)pVal)->type) {
        pDbOptions->keepTimeOffset = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      } else {
        pDbOptions->pKeepTimeOffsetNode = (SValueNode*)createDurationValueNode(pCxt, (SToken*)pVal);
      }
      break;
    case DB_OPTION_ENCRYPT_ALGORITHM:
      COPY_STRING_FORM_STR_TOKEN(pDbOptions->encryptAlgorithmStr, (SToken*)pVal);
      if (strlen(pDbOptions->encryptAlgorithmStr) == 0) pDbOptions->encryptAlgorithm = -1;
      break;
    case DB_OPTION_DNODES:
      if (((SToken*)pVal)->n >= TSDB_DNODE_LIST_LEN) {
        snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "the dnode list is too long (should less than %d)",
                 TSDB_DNODE_LIST_LEN);
        pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
      } else {
        COPY_STRING_FORM_STR_TOKEN(pDbOptions->dnodeListStr, (SToken*)pVal);
      }
      break;
    case DB_OPTION_COMPACT_INTERVAL:
      if (TK_NK_INTEGER == ((SToken*)pVal)->type) {
        pDbOptions->compactInterval = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      } else {
        pDbOptions->pCompactIntervalNode = (SValueNode*)createDurationValueNode(pCxt, (SToken*)pVal);
      }
      break;
    case DB_OPTION_COMPACT_TIME_RANGE:
      pDbOptions->pCompactTimeRangeList = pVal;
      break;
    case DB_OPTION_COMPACT_TIME_OFFSET:
      if (TK_NK_INTEGER == ((SToken*)pVal)->type) {
        pDbOptions->compactTimeOffset = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      } else {
        pDbOptions->pCompactTimeOffsetNode = (SValueNode*)createDurationValueNode(pCxt, (SToken*)pVal);
      }
      break;
    case DB_OPTION_IS_AUDIT:
      pDbOptions->isAudit = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    default:
      break;
  }
  return pOptions;
_err:
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* setDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, EDatabaseOptionType type, void* pVal) {
  return setDatabaseOptionImpl(pCxt, pOptions, type, pVal, false);
}

SNode* setAlterDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption) {
  CHECK_PARSER_STATUS(pCxt);
  switch (pAlterOption->type) {
    case DB_OPTION_KEEP:
    case DB_OPTION_RETENTIONS:
    case DB_OPTION_COMPACT_TIME_RANGE:
      return setDatabaseOptionImpl(pCxt, pOptions, pAlterOption->type, pAlterOption->pList, true);
    default:
      break;
  }
  return setDatabaseOptionImpl(pCxt, pOptions, pAlterOption->type, &pAlterOption->val, true);
_err:
  nodesDestroyNode(pOptions);
  nodesDestroyList(pAlterOption->pList);
  return NULL;
}

SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pDbName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SCreateDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* createDropDatabaseStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pDbName, bool force) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SDropDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->ignoreNotExists = ignoreNotExists;
  pStmt->force = force;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAlterDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SAlterDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* createFlushDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SFlushDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FLUSH_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createTrimDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, int32_t maxSpeed) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  STrimDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_TRIM_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->maxSpeed = maxSpeed;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createTrimDbWalStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  STrimDbWalStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_TRIM_DATABASE_WAL_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createSsMigrateDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SSsMigrateDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SSMIGRATE_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCompactStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pStart, SNode* pEnd, bool metaOnly,
                         bool force) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SCompactDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COMPACT_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  pStmt->metaOnly = metaOnly;
  pStmt->force = force;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  return NULL;
}

SNode* createCreateMountStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pMountName, SToken* pDnodeId,
                             SToken* pMountPath) {
#ifdef USE_MOUNT
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pMountName, false));
  CHECK_NAME(checkMountPath(pCxt, pMountPath));
  SCreateMountStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_MOUNT_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->mountName, pMountName);
  COPY_STRING_FORM_STR_TOKEN(pStmt->mountPath, pMountPath);
  pStmt->ignoreExists = ignoreExists;
  if (TK_NK_INTEGER == pDnodeId->type) {
    pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  } else {
    goto _err;
  }
  return (SNode*)pStmt;
_err:
  nodesDestroyNode((SNode*)pStmt);
  return NULL;
#else
  pCxt->errCode = TSDB_CODE_OPS_NOT_SUPPORT;
  return NULL;
#endif
}

SNode* createDropMountStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pMountName) {
#ifdef USE_MOUNT
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pMountName, false));
  SDropMountStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_MOUNT_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->mountName, pMountName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
_err:
  return NULL;
#else
  pCxt->errCode = TSDB_CODE_OPS_NOT_SUPPORT;
  return NULL;
#endif
}

SNode* createCompactVgroupsStmt(SAstCreateContext* pCxt, SNode* pDbName, SNodeList* vgidList, SNode* pStart,
                                SNode* pEnd, bool metaOnly, bool force) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pDbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    CHECK_PARSER_STATUS(pCxt);
  }
  SCompactVgroupsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COMPACT_VGROUPS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->vgidList = vgidList;
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  pStmt->metaOnly = metaOnly;
  pStmt->force = force;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pDbName);
  nodesDestroyList(vgidList);
  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  return NULL;
}

SNode* createDefaultTableOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STableOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_TABLE_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->maxDelay1 = -1;
  pOptions->maxDelay2 = -1;
  pOptions->watermark1 = TSDB_DEFAULT_ROLLUP_WATERMARK;
  pOptions->watermark2 = TSDB_DEFAULT_ROLLUP_WATERMARK;
  pOptions->ttl = TSDB_DEFAULT_TABLE_TTL;
  pOptions->keep = -1;
  pOptions->virtualStb = false;
  pOptions->commentNull = true;  // mark null
  return (SNode*)pOptions;
_err:
  return NULL;
}

SNode* createAlterTableOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STableOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_TABLE_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->ttl = -1;
  pOptions->commentNull = true;  // mark null
  pOptions->keep = -1;
  return (SNode*)pOptions;
_err:
  return NULL;
}

SNode* setTableOption(SAstCreateContext* pCxt, SNode* pOptions, ETableOptionType type, void* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  switch (type) {
    case TABLE_OPTION_COMMENT:
      if (checkComment(pCxt, (SToken*)pVal, true)) {
        ((STableOptions*)pOptions)->commentNull = false;
        COPY_STRING_FORM_STR_TOKEN(((STableOptions*)pOptions)->comment, (SToken*)pVal);
      }
      break;
    case TABLE_OPTION_MAXDELAY:
      ((STableOptions*)pOptions)->pMaxDelay = pVal;
      break;
    case TABLE_OPTION_WATERMARK:
      ((STableOptions*)pOptions)->pWatermark = pVal;
      break;
    case TABLE_OPTION_ROLLUP:
      ((STableOptions*)pOptions)->pRollupFuncs = pVal;
      break;
    case TABLE_OPTION_TTL: {
      int64_t ttl = taosStr2Int64(((SToken*)pVal)->z, NULL, 10);
      if (ttl > INT32_MAX) {
        pCxt->errCode = TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
      } else {
        // ttl can not be smaller than 0, because there is a limitation in sql.y (TTL NK_INTEGER)
        ((STableOptions*)pOptions)->ttl = ttl;
      }
      break;
    }
    case TABLE_OPTION_SMA:
      ((STableOptions*)pOptions)->pSma = pVal;
      break;
    case TABLE_OPTION_DELETE_MARK:
      ((STableOptions*)pOptions)->pDeleteMark = pVal;
      break;
    case TABLE_OPTION_KEEP:
      if (TK_NK_INTEGER == ((SToken*)pVal)->type) {
        ((STableOptions*)pOptions)->keep = taosStr2Int32(((SToken*)pVal)->z, NULL, 10) * 1440;
      } else {
        ((STableOptions*)pOptions)->pKeepNode = (SValueNode*)createDurationValueNode(pCxt, (SToken*)pVal);
      }
      break;
    case TABLE_OPTION_VIRTUAL: {
      int64_t virtualStb = taosStr2Int64(((SToken*)pVal)->z, NULL, 10);
      if (virtualStb != 0 && virtualStb != 1) {
        pCxt->errCode = TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
      } else {
        ((STableOptions*)pOptions)->virtualStb = virtualStb;
      }
      break;
    }
    default:
      break;
  }
  return pOptions;
_err:
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* createDefaultColumnOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SColumnOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->commentNull = true;
  pOptions->bPrimaryKey = false;
  pOptions->hasRef = false;
  return (SNode*)pOptions;
_err:
  return NULL;
}

EColumnOptionType getColumnOptionType(const char* optionType) {
  if (0 == strcasecmp(optionType, "ENCODE")) {
    return COLUMN_OPTION_ENCODE;
  } else if (0 == strcasecmp(optionType, "COMPRESS")) {
    return COLUMN_OPTION_COMPRESS;
  } else if (0 == strcasecmp(optionType, "LEVEL")) {
    return COLUMN_OPTION_LEVEL;
  }
  return 0;
}

SNode* setColumnReference(SAstCreateContext* pCxt, SNode* pOptions, SNode* pRef) {
  CHECK_PARSER_STATUS(pCxt);

  ((SColumnOptions*)pOptions)->hasRef = true;
  tstrncpy(((SColumnOptions*)pOptions)->refDb, ((SColumnRefNode*)pRef)->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(((SColumnOptions*)pOptions)->refTable, ((SColumnRefNode*)pRef)->refTableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(((SColumnOptions*)pOptions)->refColumn, ((SColumnRefNode*)pRef)->refColName, TSDB_COL_NAME_LEN);
  return pOptions;
_err:
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* setColumnOptionsPK(SAstCreateContext* pCxt, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  ((SColumnOptions*)pOptions)->bPrimaryKey = true;
  return pOptions;
_err:
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* setColumnOptions(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal1, void* pVal2) {
  CHECK_PARSER_STATUS(pCxt);
  char optionType[TSDB_CL_OPTION_LEN];

  memset(optionType, 0, TSDB_CL_OPTION_LEN);
  strncpy(optionType, pVal1->z, TMIN(pVal1->n, TSDB_CL_OPTION_LEN));
  if (0 == strlen(optionType)) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return pOptions;
  }
  EColumnOptionType type = getColumnOptionType(optionType);
  switch (type) {
    case COLUMN_OPTION_ENCODE:
      memset(((SColumnOptions*)pOptions)->encode, 0, TSDB_CL_COMPRESS_OPTION_LEN);
      COPY_STRING_FORM_STR_TOKEN(((SColumnOptions*)pOptions)->encode, (SToken*)pVal2);
      if (0 == strlen(((SColumnOptions*)pOptions)->encode)) {
        pCxt->errCode = TSDB_CODE_TSC_ENCODE_PARAM_ERROR;
      }
      break;
    case COLUMN_OPTION_COMPRESS:
      memset(((SColumnOptions*)pOptions)->compress, 0, TSDB_CL_COMPRESS_OPTION_LEN);
      COPY_STRING_FORM_STR_TOKEN(((SColumnOptions*)pOptions)->compress, (SToken*)pVal2);
      if (0 == strlen(((SColumnOptions*)pOptions)->compress)) {
        pCxt->errCode = TSDB_CODE_TSC_COMPRESS_PARAM_ERROR;
      }
      break;
    case COLUMN_OPTION_LEVEL:
      memset(((SColumnOptions*)pOptions)->compressLevel, 0, TSDB_CL_COMPRESS_OPTION_LEN);
      COPY_STRING_FORM_STR_TOKEN(((SColumnOptions*)pOptions)->compressLevel, (SToken*)pVal2);
      if (0 == strlen(((SColumnOptions*)pOptions)->compressLevel)) {
        pCxt->errCode = TSDB_CODE_TSC_COMPRESS_LEVEL_ERROR;
      }
      break;
    default:
      pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
      break;
  }
  return pOptions;
_err:
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* createColumnRefNodeByNode(SAstCreateContext* pCxt, SToken* pColName, SNode* pRef) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));

  SColumnRefNode* pCol = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN_REF, (SNode**)&pCol);
  CHECK_MAKE_NODE(pCol);
  if (pColName) {
    COPY_STRING_FORM_ID_TOKEN(pCol->colName, pColName);
  }
  tstrncpy(pCol->refDbName, ((SColumnRefNode*)pRef)->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pCol->refTableName, ((SColumnRefNode*)pRef)->refTableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->refColName, ((SColumnRefNode*)pRef)->refColName, TSDB_COL_NAME_LEN);
  return (SNode*)pCol;
_err:
  return NULL;
}

STokenTriplet* createTokenTriplet(SAstCreateContext* pCxt, SToken pName) {
  CHECK_PARSER_STATUS(pCxt);

  STokenTriplet* pTokenTri = taosMemoryMalloc(sizeof(STokenTriplet));
  CHECK_OUT_OF_MEM(pTokenTri);
  pTokenTri->name[0] = pName;
  pTokenTri->numOfName = 1;

  return pTokenTri;
_err:
  return NULL;
}

STokenTriplet* setColumnName(SAstCreateContext* pCxt, STokenTriplet* pTokenTri, SToken pName) {
  CHECK_PARSER_STATUS(pCxt);

  if (pTokenTri->numOfName >= 3) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    goto _err;
  }

  pTokenTri->name[pTokenTri->numOfName] = pName;
  pTokenTri->numOfName++;
  return pTokenTri;
_err:
  return NULL;
}

SNode* createColumnRefNodeByName(SAstCreateContext* pCxt, STokenTriplet* pTokenTri) {
  SColumnRefNode* pCol = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN_REF, (SNode**)&pCol);
  CHECK_MAKE_NODE(pCol);

  switch (pTokenTri->numOfName) {
    case 2: {
      CHECK_NAME(checkTableName(pCxt, &pTokenTri->name[0]));
      CHECK_NAME(checkColumnName(pCxt, &pTokenTri->name[1]));
      snprintf(pCol->refDbName, TSDB_DB_NAME_LEN, "%s", pCxt->pQueryCxt->db);
      COPY_STRING_FORM_ID_TOKEN(pCol->refTableName, &pTokenTri->name[0]);
      COPY_STRING_FORM_ID_TOKEN(pCol->refColName, &pTokenTri->name[1]);
      break;
    }
    case 3: {
      CHECK_NAME(checkDbName(pCxt, &pTokenTri->name[0], true));
      CHECK_NAME(checkTableName(pCxt, &pTokenTri->name[1]));
      CHECK_NAME(checkColumnName(pCxt, &pTokenTri->name[2]));
      COPY_STRING_FORM_ID_TOKEN(pCol->refDbName, &pTokenTri->name[0]);
      COPY_STRING_FORM_ID_TOKEN(pCol->refTableName, &pTokenTri->name[1]);
      COPY_STRING_FORM_ID_TOKEN(pCol->refColName, &pTokenTri->name[2]);
      break;
    }
    default: {
      pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
      goto _err;
    }
  }

  taosMemFree(pTokenTri);
  return (SNode*)pCol;
_err:
  taosMemFree(pTokenTri);
  nodesDestroyNode((SNode*)pCol);
  return NULL;
}

SNode* createColumnDefNode(SAstCreateContext* pCxt, SToken* pColName, SDataType dataType, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));
  if (IS_VAR_DATA_TYPE(dataType.type) && dataType.bytes == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
    CHECK_PARSER_STATUS(pCxt);
  }
  SColumnDefNode* pCol = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN_DEF, (SNode**)&pCol);
  CHECK_MAKE_NODE(pCol);
  COPY_STRING_FORM_ID_TOKEN(pCol->colName, pColName);
  pCol->dataType = dataType;
  pCol->pOptions = pNode;
  pCol->sma = true;
  return (SNode*)pCol;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SDataType createDataType(uint8_t type) {
  SDataType dt = {.type = type, .precision = 0, .scale = 0, .bytes = tDataTypes[type].bytes};
  return dt;
}

SDataType createVarLenDataType(uint8_t type, const SToken* pLen) {
  int32_t len = TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE;
  if (type == TSDB_DATA_TYPE_NCHAR) len /= TSDB_NCHAR_SIZE;
  if (pLen) len = taosStr2Int32(pLen->z, NULL, 10);
  SDataType dt = {.type = type, .precision = 0, .scale = 0, .bytes = len};
  return dt;
}

SDataType createDecimalDataType(uint8_t type, const SToken* pPrecisionToken, const SToken* pScaleToken) {
  SDataType dt = {0};
  dt.precision = taosStr2UInt8(pPrecisionToken->z, NULL, 10);
  dt.scale = pScaleToken ? taosStr2Int32(pScaleToken->z, NULL, 10) : 0;
  dt.type = decimalTypeFromPrecision(dt.precision);
  dt.bytes = tDataTypes[dt.type].bytes;
  return dt;
}

SNode* createCreateVTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNodeList* pCols) {
  SCreateVTableStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pCols = pCols;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  nodesDestroyList(pCols);
  return NULL;
}

SNode* createCreateVSubTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable,
                                 SNodeList* pSpecificColRefs, SNodeList* pColRefs, SNode* pUseRealTable,
                                 SNodeList* pSpecificTags, SNodeList* pValsOfTags) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateVSubTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_VIRTUAL_SUBTABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  strcpy(pStmt->useDbName, ((SRealTableNode*)pUseRealTable)->table.dbName);
  strcpy(pStmt->useTableName, ((SRealTableNode*)pUseRealTable)->table.tableName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pSpecificTags = pSpecificTags;
  pStmt->pValsOfTags = pValsOfTags;
  pStmt->pSpecificColRefs = pSpecificColRefs;
  pStmt->pColRefs = pColRefs;
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pUseRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pUseRealTable);
  nodesDestroyList(pSpecificTags);
  nodesDestroyList(pValsOfTags);
  nodesDestroyList(pSpecificColRefs);
  nodesDestroyList(pColRefs);
  return NULL;
}

SNode* createCreateTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNodeList* pCols,
                             SNodeList* pTags, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pCols = pCols;
  pStmt->pTags = pTags;
  pStmt->pOptions = (STableOptions*)pOptions;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  nodesDestroyList(pCols);
  nodesDestroyList(pTags);
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* createCreateSubTableClause(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable,
                                  SNodeList* pSpecificTags, SNodeList* pValsOfTags, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateSubTableClause* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_SUBTABLE_CLAUSE, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pStmt->useDbName, ((SRealTableNode*)pUseRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->useTableName, ((SRealTableNode*)pUseRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pSpecificTags = pSpecificTags;
  pStmt->pValsOfTags = pValsOfTags;
  pStmt->pOptions = (STableOptions*)pOptions;
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pUseRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pOptions);
  nodesDestroyNode(pUseRealTable);
  nodesDestroyList(pSpecificTags);
  nodesDestroyList(pValsOfTags);
  return NULL;
}

SNode* createCreateSubTableFromFileClause(SAstCreateContext* pCxt, bool ignoreExists, SNode* pUseRealTable,
                                          SNodeList* pSpecificTags, const SToken* pFilePath) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateSubTableFromFileClause* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->useDbName, ((SRealTableNode*)pUseRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->useTableName, ((SRealTableNode*)pUseRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pSpecificTags = pSpecificTags;
  if (TK_NK_STRING == pFilePath->type) {
    (void)trimString(pFilePath->z, pFilePath->n, pStmt->filePath, PATH_MAX);
  } else {
    strncpy(pStmt->filePath, pFilePath->z, pFilePath->n);
  }

  nodesDestroyNode(pUseRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pUseRealTable);
  nodesDestroyList(pSpecificTags);
  return NULL;
}

SNode* createCreateMultiTableStmt(SAstCreateContext* pCxt, SNodeList* pSubTables) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateMultiTablesStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_MULTI_TABLES_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pSubTables = pSubTables;
  return (SNode*)pStmt;
_err:
  nodesDestroyList(pSubTables);
  return NULL;
}

SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTableClause* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_TABLE_CLAUSE, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createDropTableStmt(SAstCreateContext* pCxt, bool withOpt, SNodeList* pTables) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pTables = pTables;
  pStmt->withOpt = withOpt;
  return (SNode*)pStmt;
_err:
  nodesDestroyList(pTables);
  return NULL;
}

SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool withOpt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropSuperTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_SUPER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  pStmt->ignoreNotExists = ignoreNotExists;
  pStmt->withOpt = withOpt;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createDropVirtualTableStmt(SAstCreateContext* pCxt, bool withOpt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropVirtualTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_VIRTUAL_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  pStmt->ignoreNotExists = ignoreNotExists;
  pStmt->withOpt = withOpt;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

static SNode* createAlterTableStmtFinalize(SNode* pRealTable, SAlterTableStmt* pStmt) {
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createAlterTableModifyOptions(SAstCreateContext* pCxt, SNode* pRealTable, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_OPTIONS;
  pStmt->pOptions = (STableOptions*)pOptions;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pOptions);
  return NULL;
}

SNode* createAlterTableAddModifyCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName,
                                    SDataType dataType) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  pStmt->dataType = dataType;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterTableAddModifyColOptions2(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType,
                                            SToken* pColName, SDataType dataType, SNode* pOptions) {
  SAlterTableStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  pStmt->dataType = dataType;
  pStmt->pColOptions = (SColumnOptions*)pOptions;

  if (pOptions != NULL) {
    SColumnOptions* pOption = (SColumnOptions*)pOptions;
    if (pOption->hasRef) {
      if (!pOption->commentNull || pOption->bPrimaryKey || 0 != strcmp(pOption->compress, "") ||
          0 != strcmp(pOption->encode, "") || 0 != strcmp(pOption->compressLevel, "")) {
        pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
      }
      pStmt->alterType = TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF;
      tstrncpy(pStmt->refDbName, pOption->refDb, TSDB_DB_NAME_LEN);
      tstrncpy(pStmt->refTableName, pOption->refTable, TSDB_TABLE_NAME_LEN);
      tstrncpy(pStmt->refColName, pOption->refColumn, TSDB_COL_NAME_LEN);
      CHECK_PARSER_STATUS(pCxt);
    } else if (pOption->bPrimaryKey == false && pOption->commentNull == true) {
      if (strlen(pOption->compress) != 0 || strlen(pOption->compressLevel) || strlen(pOption->encode) != 0) {
        pStmt->alterType = TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION;
      } else {
        // pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
        //                                         "not support alter column with option except compress");
        // return NULL;
      }
    } else {
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "not support alter column with option except compress");
      CHECK_PARSER_STATUS(pCxt);
    }
  }
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pOptions);
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterTableAddModifyColOptions(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType,
                                           SToken* pColName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  pStmt->pColOptions = (SColumnOptions*)pOptions;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pOptions);
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterTableDropCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterTableRenameCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pOldColName,
                                 SToken* pNewColName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pOldColName));
  CHECK_NAME(checkColumnName(pCxt, pNewColName));
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pOldColName);
  COPY_STRING_FORM_ID_TOKEN(pStmt->newColName, pNewColName);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterTableAlterColRef(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName,
                                   SNode* pRef) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  tstrncpy(pStmt->refDbName, ((SColumnRefNode*)pRef)->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->refTableName, ((SColumnRefNode*)pRef)->refTableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pStmt->refColName, ((SColumnRefNode*)pRef)->refColName, TSDB_COL_NAME_LEN);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterTableRemoveColRef(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName,
                                    const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pColName));
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterSingleTagColumnNode(SAstCreateContext* pCtx, SToken* pTagName, SNode* pVal) {
  CHECK_PARSER_STATUS(pCtx);
  SAlterTableStmt* pStmt = NULL;
  pCtx->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
  CHECK_NAME(checkColumnName(pCtx, pTagName));
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pTagName);
  pStmt->pVal = (SValueNode*)pVal;
  pStmt->pNodeListTagValue = NULL;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAlterTableSetTag(SAstCreateContext* pCxt, SNode* pRealTable, SToken* pTagName, SNode* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkColumnName(pCxt, pTagName));
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pTagName);
  pStmt->pVal = (SValueNode*)pVal;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  nodesDestroyNode(pVal);
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterTableSetMultiTagValue(SAstCreateContext* pCxt, SNode* pRealTable, SNodeList* pList) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, (SNode**)&pStmt);

  CHECK_MAKE_NODE(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL;
  pStmt->pNodeListTagValue = pList;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
_err:
  return NULL;
}

SNode* setAlterSuperTableType(SNode* pStmt) {
  if (!pStmt) return NULL;
  setNodeType(pStmt, QUERY_NODE_ALTER_SUPER_TABLE_STMT);
  return pStmt;
}

SNode* setAlterVirtualTableType(SNode* pStmt) {
  if (!pStmt) return NULL;
  setNodeType(pStmt, QUERY_NODE_ALTER_VIRTUAL_TABLE_STMT);
  return pStmt;
}

SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SUseDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_USE_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
_err:
  return NULL;
}

static bool needDbShowStmt(ENodeType type) {
  return QUERY_NODE_SHOW_TABLES_STMT == type || QUERY_NODE_SHOW_STABLES_STMT == type ||
         QUERY_NODE_SHOW_VGROUPS_STMT == type || QUERY_NODE_SHOW_INDEXES_STMT == type ||
         QUERY_NODE_SHOW_TAGS_STMT == type || QUERY_NODE_SHOW_TABLE_TAGS_STMT == type ||
         QUERY_NODE_SHOW_VIEWS_STMT == type || QUERY_NODE_SHOW_TSMAS_STMT == type ||
         QUERY_NODE_SHOW_USAGE_STMT == type || QUERY_NODE_SHOW_VTABLES_STMT == type ||
         QUERY_NODE_SHOW_STREAMS_STMT == type;
}

SNode* createShowStmtWithLike(SAstCreateContext* pCxt, ENodeType type, SNode* pLikePattern) {
  CHECK_PARSER_STATUS(pCxt);
  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->withFull = false;
  pStmt->pTbName = pLikePattern;
  if (pLikePattern) {
    pStmt->tableCondType = OP_TYPE_LIKE;
  }
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pLikePattern);
  return NULL;
}

SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->withFull = false;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowXnodeStmtWithCond(SAstCreateContext* pCxt, ENodeType type, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pWhere = pWhere;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowXNodeResourcesWhereStmt(SAstCreateContext* pCxt, EXnodeResourceType resourceType, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  SShowStmt* pStmt = NULL;
  ENodeType  type;
  switch (resourceType) {
    case XNODE_TASK:
      type = QUERY_NODE_SHOW_XNODE_TASKS_STMT;
      break;
    case XNODE_JOB:
      type = QUERY_NODE_SHOW_XNODE_JOBS_STMT;
      break;
    case XNODE_AGENT:
      type = QUERY_NODE_SHOW_XNODE_AGENTS_STMT;
      break;
    default:
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "Xnode not support show xnode resource type");
      goto _err;
  }

  pStmt = (SShowStmt*)createShowXnodeStmtWithCond(pCxt, type, pWhere);
  CHECK_MAKE_NODE(pStmt);

  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowStmtWithFull(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->withFull = true;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowCompactsStmt(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCompactsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowSsMigratesStmt(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowSsMigratesStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowTokensStmt(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowTokensStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* setShowKind(SAstCreateContext* pCxt, SNode* pStmt, EShowKind showKind) {
  if (pStmt == NULL) {
    return NULL;
  }
  SShowStmt* pShow = (SShowStmt*)pStmt;
  pShow->showKind = showKind;
  return pStmt;
}

SNode* createShowStmtWithCond(SAstCreateContext* pCxt, ENodeType type, SNode* pDbName, SNode* pTbName,
                              EOperatorType tableCondType) {
  CHECK_PARSER_STATUS(pCxt);
  if (needDbShowStmt(type) && NULL == pDbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    CHECK_PARSER_STATUS(pCxt);
  }
  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->pTbName = pTbName;
  pStmt->tableCondType = tableCondType;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pDbName);
  nodesDestroyNode(pTbName);
  return NULL;
}

SNode* createShowTablesStmt(SAstCreateContext* pCxt, SShowTablesOption option, SNode* pTbName,
                            EOperatorType tableCondType) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pDbName = NULL;
  if (option.dbName.type == TK_NK_NIL) {
    pDbName = createDefaultDatabaseCondValue(pCxt);
  } else {
    pDbName = createIdentifierValueNode(pCxt, &option.dbName);
  }

  if (option.kind != SHOW_KIND_TABLES_NORMAL && option.kind != SHOW_KIND_TABLES_CHILD && option.kind != SHOW_KIND_ALL) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return NULL;
  }

  SNode* pStmt = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TABLES_STMT, pDbName, pTbName, tableCondType);
  CHECK_PARSER_STATUS(pCxt);
  (void)setShowKind(pCxt, pStmt, option.kind);
  return pStmt;
_err:
  nodesDestroyNode(pTbName);
  return NULL;
}

SNode* createShowVTablesStmt(SAstCreateContext* pCxt, SShowTablesOption option, SNode* pTbName,
                             EOperatorType tableCondType) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pDbName = NULL;
  if (option.dbName.type == TK_NK_NIL) {
    pDbName = createDefaultDatabaseCondValue(pCxt);
  } else {
    pDbName = createIdentifierValueNode(pCxt, &option.dbName);
  }

  if (option.kind != SHOW_KIND_TABLES_NORMAL && option.kind != SHOW_KIND_TABLES_CHILD && option.kind != SHOW_KIND_ALL) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return NULL;
  }

  SNode* pStmt = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_VTABLES_STMT, pDbName, pTbName, tableCondType);
  CHECK_PARSER_STATUS(pCxt);
  (void)setShowKind(pCxt, pStmt, option.kind);
  return pStmt;
_err:
  nodesDestroyNode(pTbName);
  return NULL;
}

SNode* createShowSTablesStmt(SAstCreateContext* pCxt, SShowTablesOption option, SNode* pTbName,
                             EOperatorType tableCondType) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pDbName = NULL;
  if (option.dbName.type == TK_NK_NIL) {
    pDbName = createDefaultDatabaseCondValue(pCxt);
  } else {
    pDbName = createIdentifierValueNode(pCxt, &option.dbName);
  }

  if (option.kind != SHOW_KIND_TABLES_NORMAL && option.kind != SHOW_KIND_TABLES_VIRTUAL &&
      option.kind != SHOW_KIND_ALL) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return NULL;
  }

  SNode* pStmt = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_STABLES_STMT, pDbName, pTbName, tableCondType);
  CHECK_PARSER_STATUS(pCxt);
  (void)setShowKind(pCxt, pStmt, option.kind);
  return pStmt;
_err:
  nodesDestroyNode(pTbName);
  return NULL;
}

SNode* createShowCreateDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, true));
  SShowCreateDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_CREATE_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowAliveStmt(SAstCreateContext* pCxt, SNode* pNode, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SToken  dbToken = {0};
  SToken* pDbToken = NULL;

  if (pNode) {
    SValueNode* pDbName = (SValueNode*)pNode;
    if (pDbName->literal) {
      dbToken.z = pDbName->literal;
      dbToken.n = strlen(pDbName->literal);
      pDbToken = &dbToken;
    }
  }

  if (pDbToken) {
    CHECK_NAME(checkDbName(pCxt, pDbToken, true));
  }

  SShowAliveStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_PARSER_STATUS(pCxt);

  if (pDbToken) {
    COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbToken);
  }
  if (pNode) {
    nodesDestroyNode(pNode);
  }

  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pNode);
  return NULL;
}

SNode* createShowCreateTableStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCreateTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createShowCreateVTableStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCreateTableStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createShowCreateViewStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCreateViewStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->viewName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createShowTableDistributedStmt(SAstCreateContext* pCxt, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowTableDistributedStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createShowDnodeVariablesStmt(SAstCreateContext* pCxt, SNode* pDnodeId, SNode* pLikePattern) {
  CHECK_PARSER_STATUS(pCxt);
  SShowDnodeVariablesStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_DNODE_VARIABLES_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pDnodeId = pDnodeId;
  pStmt->pLikePattern = pLikePattern;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pDnodeId);
  nodesDestroyNode(pLikePattern);
  return NULL;
}

SNode* createShowVnodesStmt(SAstCreateContext* pCxt, SNode* pDnodeId, SNode* pDnodeEndpoint) {
  CHECK_PARSER_STATUS(pCxt);
  SShowVnodesStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_VNODES_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pDnodeId = pDnodeId;
  pStmt->pDnodeEndpoint = pDnodeEndpoint;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pDnodeId);
  nodesDestroyNode(pDnodeEndpoint);
  return NULL;
}

SNode* createShowTableTagsStmt(SAstCreateContext* pCxt, SNode* pTbName, SNode* pDbName, SNodeList* pTags) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pDbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    CHECK_PARSER_STATUS(pCxt);
  }
  SShowTableTagsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_TABLE_TAGS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->pTbName = pTbName;
  pStmt->pTags = pTags;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pTbName);
  nodesDestroyNode(pDbName);
  nodesDestroyList(pTags);
  return NULL;
}

SNode* createShowCompactDetailsStmt(SAstCreateContext* pCxt, SNode* pCompactId) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCompactDetailsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_COMPACT_DETAILS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pId = pCompactId;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pCompactId);
  return NULL;
}

SNode* createShowRetentionDetailsStmt(SAstCreateContext* pCxt, SNode* pId) {
  CHECK_PARSER_STATUS(pCxt);
  SShowRetentionDetailsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_RETENTION_DETAILS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pId = pId;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pId);
  return NULL;
}

SNode* createShowTransactionDetailsStmt(SAstCreateContext* pCxt, SNode* pTransactionIdNode) {
  CHECK_PARSER_STATUS(pCxt);
  SShowTransactionDetailsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_TRANSACTION_DETAILS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pTransactionId = pTransactionIdNode;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pTransactionIdNode);
  return NULL;
}



static bool parseIp(const char* strIp, SIpRange* pIpRange) {
  if (strchr(strIp, ':') == NULL) {
    struct in_addr ip4;
    if (inet_pton(AF_INET, strIp, &ip4) == 1) {
      pIpRange->type = 0;
      memcpy(&pIpRange->ipV4.ip, &ip4.s_addr, sizeof(ip4.s_addr));
      return true;
    }
  } else {
    struct in6_addr ip6;
    if (inet_pton(AF_INET6, strIp, &ip6) == 1) {
      pIpRange->type = 1;
      memcpy(&pIpRange->ipV6.addr[0], ip6.s6_addr, 8);
      memcpy(&pIpRange->ipV6.addr[1], ip6.s6_addr + 8, 8);
      return true;
    }
  }

  return false;
}



SIpRangeNode* parseIpRange(SAstCreateContext* pCxt, const SToken* token) {
  CHECK_PARSER_STATUS(pCxt);

#ifdef TD_ASTRA
  return NULL;
#else

  SIpRangeNode* node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_IP_RANGE, (SNode**)&node);
  if (node == NULL) {
    goto _err;
  }

  char buf[64];
  if (token->n >= sizeof(buf)) {
    code = TSDB_CODE_PAR_INVALID_IP_RANGE;
    goto _err;
  }
  memcpy(buf, token->z, token->n);
  buf[token->n] = '\0';
  (void)strdequote(buf);

  char* slash = strchr(buf, '/');
  if (slash) {
    *slash = '\0';
  }

  if (!parseIp(buf, &node->range)) {
    code = TSDB_CODE_PAR_INVALID_IP_RANGE;
    goto _err;
  }

  int32_t mask = 0;
  if (!slash) {
    mask = node->range.type == 0 ? 32 : 128;
  } else if (taosStr2int32(slash + 1, &mask) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_PAR_INVALID_IP_RANGE;
    goto _err;
  }

  code = tIpRangeSetMask(&node->range, mask);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  return node;

_err:
  pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, code);
  nodesDestroyNode((SNode*)node);
  return NULL;

#endif
}



SDateTimeRangeNode* parseDateTimeRange(SAstCreateContext* pCxt, const SToken* token) {
  CHECK_PARSER_STATUS(pCxt);

  SDateTimeRangeNode* node = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_DATE_TIME_RANGE, (SNode**)&node);
  if (code != TSDB_CODE_SUCCESS) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, code);
    goto _err;
  }

  char buf[128];
  if (token->n >= sizeof(buf)) {
    code = TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG;
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "Date time range string is too long");
    goto _err;
  }
  memcpy(buf, token->z, token->n);
  buf[token->n] = '\0';
  (void)strdequote(buf);

  code = TSDB_CODE_PAR_INVALID_OPTION_VALUE;
  int32_t year = 0, month = 0, day = 0, hour = 0, minute = 0, duration = 0;
  if (buf[0] >= '1' && buf[0] <= '9') {
    // format: YYYY-MM-DD HH:MM duration
    int ret = sscanf(buf, "%d-%d-%d %d:%d %d", &year, &month, &day, &hour, &minute, &duration);
    if (ret != 6) {
      goto _err;
    }
    if (month < 1 || month > 12) {
      goto _err;
    }
  } else {
    // format: WEEKDAY HH:MM duration
    char weekday[4];
    int ret = sscanf(buf, "%3s %d:%d %d", weekday, &hour, &minute, &duration);
    if (ret != 4) {
      goto _err;
    }
    day = taosParseShortWeekday(weekday);
    if (day < 0 || day > 6) {
      goto _err;
    }
    month = -1;
  }

  node->range.year = (int16_t)year;
  node->range.month = (int8_t)month;
  node->range.day = (int8_t)day;
  node->range.hour = (int8_t)hour;
  node->range.minute = (int8_t)minute;
  node->range.duration = duration;
  if (!isValidDateTimeRange(&node->range)) {
    goto _err;
  }

  return node;

_err:
  if (code == TSDB_CODE_PAR_INVALID_OPTION_VALUE) { // other error types have been set above
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "Invalid date time range");
  }
  nodesDestroyNode((SNode*)node);
  return NULL;
}


SUserOptions* createDefaultUserOptions(SAstCreateContext* pCxt) {
  SUserOptions* pOptions = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_USER_OPTIONS, (SNode**)&pOptions);
  if (pOptions == NULL) {
    pCxt->errCode = code;
    return NULL;
  }

  pOptions->enable = 1;
  pOptions->sysinfo = 1;
  pOptions->createdb = 0;
  pOptions->isImport = 0;
  pOptions->changepass = 2;
  pOptions->sessionPerUser = TSDB_USER_SESSION_PER_USER_DEFAULT;
  pOptions->connectTime = TSDB_USER_CONNECT_TIME_DEFAULT;
  pOptions->connectIdleTime = TSDB_USER_CONNECT_IDLE_TIME_DEFAULT;
  pOptions->callPerSession = TSDB_USER_CALL_PER_SESSION_DEFAULT;
  pOptions->vnodePerCall = TSDB_USER_VNODE_PER_CALL_DEFAULT;
  pOptions->failedLoginAttempts = TSDB_USER_FAILED_LOGIN_ATTEMPTS_DEFAULT;
  pOptions->passwordLifeTime = TSDB_USER_PASSWORD_LIFE_TIME_DEFAULT;
  pOptions->passwordReuseTime = TSDB_USER_PASSWORD_REUSE_TIME_DEFAULT;
  pOptions->passwordReuseMax = TSDB_USER_PASSWORD_REUSE_MAX_DEFAULT;
  pOptions->passwordLockTime = TSDB_USER_PASSWORD_LOCK_TIME_DEFAULT;
  pOptions->passwordGraceTime = TSDB_USER_PASSWORD_GRACE_TIME_DEFAULT;
  pOptions->inactiveAccountTime = TSDB_USER_INACTIVE_ACCOUNT_TIME_DEFAULT;
  pOptions->allowTokenNum = TSDB_USER_ALLOW_TOKEN_NUM_DEFAULT;

  return pOptions;
}



SUserOptions* mergeUserOptions(SAstCreateContext* pCxt, SUserOptions* a, SUserOptions* b) {
  if (a == NULL && b == NULL) {
      return createDefaultUserOptions(pCxt);
  }
  if (b == NULL) {
    return a;
  }
  if (a == NULL) {
    return b;
  }

  if (b->hasPassword) {
    if (a->hasPassword) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "PASS");
    } else {
      a->hasPassword = true;
      tstrncpy(a->password, b->password, sizeof(a->password));
    }
  }

  if (b->hasTotpseed) {
    if (a->hasTotpseed) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "TOTPSEED");
    } else {
      a->hasTotpseed = true;
      tstrncpy(a->totpseed, b->totpseed, sizeof(a->totpseed));
    }
  }

  if (b->hasEnable) {
    if (a->hasEnable) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "ENABLE/ACCOUNT LOCK/ACCOUNT UNLOCK");
    } else {
      a->hasEnable = true;
      a->enable = b->enable;
    }
  }

  if (b->hasSysinfo) {
    if (a->hasSysinfo) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "SYSINFO");
    } else {
      a->hasSysinfo = true;
      a->sysinfo = b->sysinfo;
    }
  }

  if (b->hasCreatedb) {
    if (a->hasCreatedb) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "CREATEDB");
    } else {
      a->hasCreatedb = true;
      a->createdb = b->createdb;
    }
  }

  if (b->hasChangepass) {
    if (a->hasChangepass) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "CHANGEPASS");
    } else {
      a->hasChangepass = true;
      a->changepass = b->changepass;
    }
  }

  if (b->hasSessionPerUser) {
    if (a->hasSessionPerUser) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "SESSION_PER_USER");
    } else {
      a->hasSessionPerUser = true;
      a->sessionPerUser = b->sessionPerUser;
    }
  }

  if (b->hasConnectTime) {
    if (a->hasConnectTime) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "CONNECT_TIME");
    } else {
      a->hasConnectTime = true;
      a->connectTime = b->connectTime;
    }
  }

  if (b->hasConnectIdleTime) {
    if (a->hasConnectIdleTime) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "CONNECT_IDLE_TIME");
    } else {
      a->hasConnectIdleTime = true;
      a->connectIdleTime = b->connectIdleTime;
    }
  }

  if (b->hasCallPerSession) {
    if (a->hasCallPerSession) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "CALLS_PER_SESSION");
    } else {
      a->hasCallPerSession = true;
      a->callPerSession = b->callPerSession;
    }
  }

  if (b->hasVnodePerCall) {
    if (a->hasVnodePerCall) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "VNODES_PER_CALL");
    } else {
      a->hasVnodePerCall = true;
      a->vnodePerCall = b->vnodePerCall;
    }
  }

  if (b->hasFailedLoginAttempts) {
    if (a->hasFailedLoginAttempts) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "FAILED_LOGIN_ATTEMPTS");
    } else {
      a->hasFailedLoginAttempts = true;
      a->failedLoginAttempts = b->failedLoginAttempts;
    }
  }

  if (b->hasPasswordLifeTime) {
    if (a->hasPasswordLifeTime) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "PASSWORD_LIFE_TIME");
    } else {
      a->hasPasswordLifeTime = true;
      a->passwordLifeTime = b->passwordLifeTime;
    }
  }

  if (b->hasPasswordReuseTime) {
    if (a->hasPasswordReuseTime) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "PASSWORD_REUSE_TIME");
    } else {
      a->hasPasswordReuseTime = true;
      a->passwordReuseTime = b->passwordReuseTime;
    }
  }

  if (b->hasPasswordReuseMax) {
    if (a->hasPasswordReuseMax) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "PASSWORD_REUSE_MAX");
    } else {
      a->hasPasswordReuseMax = true;
      a->passwordReuseMax = b->passwordReuseMax;
    }
  }

  if (b->hasPasswordLockTime) {
    if (a->hasPasswordLockTime) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "PASSWORD_LOCK_TIME");
    } else {
      a->hasPasswordLockTime = true;
      a->passwordLockTime = b->passwordLockTime;
    }
  }

  if (b->hasPasswordGraceTime) {
    if (a->hasPasswordGraceTime) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "PASSWORD_GRACE_TIME");
    } else {
      a->hasPasswordGraceTime = true;
      a->passwordGraceTime = b->passwordGraceTime;
    }
  }

  if (b->hasInactiveAccountTime) {
    if (a->hasInactiveAccountTime) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "INACTIVE_ACCOUNT_TIME");
    } else {
      a->hasInactiveAccountTime = true;
      a->inactiveAccountTime = b->inactiveAccountTime;
    }
  }

  if (b->hasAllowTokenNum) {
    if (a->hasAllowTokenNum) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "ALLOW_TOKEN_NUM");
    } else {
      a->hasAllowTokenNum = true;
      a->allowTokenNum = b->allowTokenNum;
    }
  }

  if (b->pIpRanges != NULL) {
    if (a->pIpRanges == NULL) {
      a->pIpRanges = b->pIpRanges;
    } else {
      int32_t code = nodesListAppendList(a->pIpRanges, b->pIpRanges);
      if (code != TSDB_CODE_SUCCESS) {
        pCxt->errCode = code;
      }
    }
    b->pIpRanges = NULL;
  }

  if (b->pDropIpRanges != NULL) {
    if (a->pDropIpRanges == NULL) {
      a->pDropIpRanges = b->pDropIpRanges;
    } else {
      int32_t code = nodesListAppendList(a->pDropIpRanges, b->pDropIpRanges);
      if (code != TSDB_CODE_SUCCESS) {
        pCxt->errCode = code;
      }
    }
    b->pDropIpRanges = NULL;
  }

  if (b->pTimeRanges != NULL) {
    if (a->pTimeRanges == NULL) {
      a->pTimeRanges = b->pTimeRanges;
    } else {
      int32_t code = nodesListAppendList(a->pTimeRanges, b->pTimeRanges);
      if (code != TSDB_CODE_SUCCESS) {
        pCxt->errCode = code;
      }
    }
    b->pTimeRanges = NULL;
  }

  if (b->pDropTimeRanges != NULL) {
    if (a->pDropTimeRanges == NULL) {
      a->pDropTimeRanges = b->pDropTimeRanges;
    } else {
      int32_t code = nodesListAppendList(a->pDropTimeRanges, b->pDropTimeRanges);
      if (code != TSDB_CODE_SUCCESS) {
        pCxt->errCode = code;
      }
    }
    b->pDropTimeRanges = NULL;
  }

  nodesDestroyNode((SNode*)b);
  return a;
}



void setUserOptionsTotpseed(SAstCreateContext* pCxt, SUserOptions* pUserOptions, const SToken* pTotpseed) {
  pUserOptions->hasTotpseed = true;

  if (pTotpseed == NULL) { // clear TOTP secret
    memset(pUserOptions->totpseed, 0, sizeof(pUserOptions->totpseed));
    return;
  }

  if (pTotpseed->n >= sizeof(pUserOptions->totpseed) * 2) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG, "TOTPSEED", sizeof(pUserOptions->totpseed));
    return;
  }

  char buf[sizeof(pUserOptions->totpseed) * 2 + 1];
  memcpy(buf, pTotpseed->z, pTotpseed->n);
  buf[pTotpseed->n] = 0;
  (void)strdequote(buf);
  size_t len = strtrim(buf);

  if (len >= sizeof(pUserOptions->totpseed)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG, "TOTPSEED", sizeof(pUserOptions->totpseed));
  } else if (len < TSDB_USER_TOTPSEED_MIN_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_SHORT, "TOTPSEED", TSDB_USER_TOTPSEED_MIN_LEN);
  } else {
    tstrncpy(pUserOptions->totpseed, buf, sizeof(pUserOptions->totpseed));
  }
}



void setUserOptionsPassword(SAstCreateContext* pCxt, SUserOptions* pUserOptions, const SToken* pPassword) {
  pUserOptions->hasPassword = true;

  if (pPassword->n >= sizeof(pUserOptions->password) * 2) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    return;
  }

  char buf[sizeof(pUserOptions->password) * 2 + 1];
  memcpy(buf, pPassword->z, pPassword->n);
  buf[pPassword->n] = 0;
  (void)strdequote(buf);
  size_t len = strtrim(buf);

  if (len >= sizeof(pUserOptions->password)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
  } else if (len < TSDB_PASSWORD_MIN_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY);
  } else {
    tstrncpy(pUserOptions->password, buf, sizeof(pUserOptions->password));
  }
}



static bool isValidUserOptions(SAstCreateContext* pCxt, const SUserOptions* opts) {
  if (opts->hasEnable && (opts->enable < 0 || opts->enable > 1)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "ENABLE");
    return false;
  }

  if (opts->hasSysinfo && (opts->sysinfo < 0 || opts->sysinfo > 1)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "SYSINFO");
    return false;
  }

  if (opts->hasIsImport && (opts->isImport < 0 || opts->isImport > 1)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "IS_IMPORT");
    return false;
  }

  if (opts->hasCreatedb && (opts->createdb < 0 || opts->createdb > 1)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "CREATEDB");
    return false;
  }

  if (opts->hasTotpseed && opts->totpseed[0] != 0 && !taosIsComplexString(opts->totpseed)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "TOTPSEED");
    return false;
  }

  if (opts->hasPassword && !isValidPassword(pCxt, opts->password, opts->hasIsImport && opts->isImport)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PASSWD);
    return false;
  }

  if (opts->hasChangepass && (opts->changepass < 0 || opts->changepass > 2)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "CHANGEPASS");
    return false;
  }

  if (opts->hasSessionPerUser && (opts->sessionPerUser < -1 || opts->sessionPerUser == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "SESSION_PER_USER");
    return false;
  }

  if (opts->hasConnectTime && (opts->connectTime < -1 || opts->connectTime == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "CONNECT_TIME");
    return false;
  }

  if (opts->hasConnectIdleTime && (opts->connectIdleTime < -1 || opts->connectIdleTime == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "CONNECT_IDLE_TIME");
    return false;
  }

  if (opts->hasCallPerSession && (opts->callPerSession < -1 || opts->callPerSession == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "CALLS_PER_SESSION");
    return false;
  }

  if (opts->hasVnodePerCall && (opts->vnodePerCall < -1 || opts->vnodePerCall == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "VNODES_PER_CALL");
    return false;
  }

  if (opts->hasFailedLoginAttempts && (opts->failedLoginAttempts < -1 || opts->failedLoginAttempts == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "FAILED_LOGIN_ATTEMPTS");
    return false;
  }

  if (opts->hasPasswordLockTime && (opts->passwordLockTime < -1 || opts->passwordLockTime == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "PASSWORD_LOCK_TIME");
    return false;
  }

  if (opts->hasPasswordLifeTime && (opts->passwordLifeTime < -1 || opts->passwordLifeTime == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "PASSWORD_LIFE_TIME");
    return false;
  }

  if (opts->hasPasswordGraceTime && (opts->passwordGraceTime < -1)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "PASSWORD_GRACE_TIME");
    return false;
  }

  if (opts->hasPasswordReuseTime && (opts->passwordReuseTime < 0 || opts->passwordReuseTime > TSDB_USER_PASSWORD_REUSE_TIME_MAX)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "PASSWORD_REUSE_TIME");
    return false;
  }

  if (opts->hasPasswordReuseMax && (opts->passwordReuseMax < 0 || opts->passwordReuseMax > TSDB_USER_PASSWORD_REUSE_MAX_MAX)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "PASSWORD_REUSE_MAX");
    return false;
  }

  if (opts->hasInactiveAccountTime && (opts->inactiveAccountTime < -1 || opts->inactiveAccountTime == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "INACTIVE_ACCOUNT_TIME");
    return false;
  }

  if (opts->hasAllowTokenNum && opts->allowTokenNum < -1) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "ALLOW_TOKEN_NUM");
    return false;
  }

  // ip ranges and date time ranges has been validated during parsing

  return true;
}



SNode* createCreateUserStmt(SAstCreateContext* pCxt, SToken* pUserName, SUserOptions* opts, bool ignoreExists) {
  SCreateUserStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pUserName));

  if (!isValidUserOptions(pCxt, opts)) {
    goto _err;
  }

  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_USER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  tstrncpy(pStmt->password, opts->password, sizeof(pStmt->password));
  tstrncpy(pStmt->totpseed, opts->totpseed, sizeof(pStmt->totpseed));

  pStmt->ignoreExists = ignoreExists;
  pStmt->sysinfo = opts->sysinfo;
  pStmt->createDb = opts->createdb;
  pStmt->isImport = opts->isImport;
  pStmt->changepass = opts->changepass;
  pStmt->enable = opts->enable;

  pStmt->sessionPerUser = opts->sessionPerUser;
  pStmt->connectTime = opts->connectTime;
  pStmt->connectIdleTime = opts->connectIdleTime;
  pStmt->callPerSession = opts->callPerSession;
  pStmt->vnodePerCall = opts->vnodePerCall;
  pStmt->failedLoginAttempts = opts->failedLoginAttempts;
  pStmt->passwordLifeTime = opts->passwordLifeTime;
  pStmt->passwordReuseTime = opts->passwordReuseTime;
  pStmt->passwordReuseMax = opts->passwordReuseMax;
  pStmt->passwordLockTime = opts->passwordLockTime;
  pStmt->passwordGraceTime = opts->passwordGraceTime;
  pStmt->inactiveAccountTime = opts->inactiveAccountTime;
  pStmt->allowTokenNum = opts->allowTokenNum;

  pStmt->numIpRanges = LIST_LENGTH(opts->pIpRanges);
  pStmt->pIpRanges = taosMemoryMalloc(pStmt->numIpRanges * sizeof(SIpRange));
  CHECK_OUT_OF_MEM(pStmt->pIpRanges);
  int i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, opts->pIpRanges) {
    SIpRangeNode* node = (SIpRangeNode*)(pNode);
    pStmt->pIpRanges[i++] = node->range;
  }

  pStmt->numTimeRanges = LIST_LENGTH(opts->pTimeRanges);
  pStmt->pTimeRanges = taosMemoryMalloc(pStmt->numTimeRanges * sizeof(SDateTimeRange));
  CHECK_OUT_OF_MEM(pStmt->pTimeRanges);
  i = 0;
  pNode = NULL;
  FOREACH(pNode, opts->pTimeRanges) {
    SDateTimeRangeNode* node = (SDateTimeRangeNode*)(pNode);
    pStmt->pTimeRanges[i++] = node->range;
  }

  nodesDestroyNode((SNode*)opts);
  return (SNode*)pStmt;

_err:
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyNode((SNode*)opts);
  return NULL;
}



SNode* createAlterUserStmt(SAstCreateContext* pCxt, SToken* pUserName, SUserOptions* pUserOptions) {
  SAlterUserStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pUserName));
  if (!isValidUserOptions(pCxt, pUserOptions)) {
    goto _err;
  }

  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_USER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->pUserOptions = pUserOptions;
  return (SNode*)pStmt;

_err:
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyNode((SNode*)pUserOptions);
  return NULL;
}



SNode* createDropUserStmt(SAstCreateContext* pCxt, SToken* pUserName, bool ignoreNotExists) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pUserName));
  SDropUserStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_USER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
_err:
  return NULL;
}

static bool checkRoleName(SAstCreateContext* pCxt, SToken* pName, bool checkSysName) {
  if (NULL == pName) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else {
    if (pName->n >= TSDB_ROLE_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    }
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    trimEscape(pCxt, pName, true);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    if (checkSysName && taosStrncasecmp(pName->z, "sys", 3) == 0) {  // system reserved role name prefix
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "Cannot create/drop/alter roles with reserved prefix 'sys'");
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}


STokenOptions* createDefaultTokenOptions(SAstCreateContext* pCxt) {
  STokenOptions* pOptions = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_TOKEN_OPTIONS, (SNode**)&pOptions);
  if (pOptions == NULL) {
    pCxt->errCode = code;
    return NULL;
  }

  pOptions->enable = 1;
  pOptions->ttl = 0;
  return pOptions;
}



STokenOptions* mergeTokenOptions(SAstCreateContext* pCxt, STokenOptions* a, STokenOptions* b) {
  if (a == NULL && b == NULL) {
      return createDefaultTokenOptions(pCxt);
  }
  if (b == NULL) {
    return a;
  }
  if (a == NULL) {
    return b;
  }

  if (b->hasEnable) {
    if (a->hasEnable) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "ENABLE");
    } else {
      a->hasEnable = true;
      a->enable = b->enable;
    }
  }

  if (b->hasTtl) {
    if (a->hasTtl) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "TTL");
    } else {
      a->hasTtl = true;
      a->ttl = b->ttl;
    }
  }

  if (b->hasProvider) {
    if (a->hasProvider) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "PROVIDER");
    } else {
      a->hasProvider = true;
      tstrncpy(a->provider, b->provider, sizeof(a->provider));
    }
  }

  if (b->hasExtraInfo) {
    if (a->hasExtraInfo) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_DUPLICATED, "EXTRA_INFO");
    } else {
      a->hasExtraInfo = true;
      tstrncpy(a->extraInfo, b->extraInfo, sizeof(a->extraInfo));
    }
  }
  nodesDestroyNode((SNode*)b);
  return a;
}



void setTokenOptionsProvider(SAstCreateContext* pCxt, STokenOptions* pTokenOptions, const SToken* pProvider) {
  pTokenOptions->hasProvider = true;

  if (pProvider->n >= sizeof(pTokenOptions->provider) * 2) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG, "PROVIDER", sizeof(pTokenOptions->provider));
    return;
  }

  char buf[sizeof(pTokenOptions->provider) * 2 + 1];
  memcpy(buf, pProvider->z, pProvider->n);
  buf[pProvider->n] = 0;
  (void)strdequote(buf);
  size_t len = strtrim(buf);

  if (len >= sizeof(pTokenOptions->provider)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG, "PROVIDER", sizeof(pTokenOptions->provider));
  } else {
    tstrncpy(pTokenOptions->provider, buf, sizeof(pTokenOptions->provider));
  }
}



void setTokenOptionsExtraInfo(SAstCreateContext* pCxt, STokenOptions* pTokenOptions, const SToken* pExtraInfo) {
  pTokenOptions->hasExtraInfo = true;

  if (pExtraInfo->n >= sizeof(pTokenOptions->extraInfo) * 2) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG, "EXTRA_INFO", sizeof(pTokenOptions->extraInfo));
    return;
  }

  char buf[sizeof(pTokenOptions->extraInfo) * 2 + 1];
  memcpy(buf, pExtraInfo->z, pExtraInfo->n);
  buf[pExtraInfo->n] = 0;
  (void)strdequote(buf);
  size_t len = strtrim(buf);

  if (len >= sizeof(pTokenOptions->extraInfo)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG, "EXTRA_INFO", sizeof(pTokenOptions->extraInfo));
  } else {
    tstrncpy(pTokenOptions->extraInfo, buf, sizeof(pTokenOptions->extraInfo));
  }
}



static bool isValidTokenOptions(SAstCreateContext* pCxt, const STokenOptions* opts) {
  if (opts->hasEnable && (opts->enable < 0 || opts->enable > 1)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "ENABLE");
    return false;
  }

  if (opts->hasTtl && (opts->ttl < 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_VALUE, "TTL");
    return false;
  }

  return true;
}



static bool checkTokenName(SAstCreateContext* pCxt, SToken* pTokenName) {
  if (NULL == pTokenName) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else {
    if (pTokenName->n >= TSDB_TOKEN_NAME_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OPTION_VALUE_TOO_LONG, "TOKEN_NAME", TSDB_TOKEN_NAME_LEN);
    }
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    trimEscape(pCxt, pTokenName, true);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

SNode* createCreateRoleStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkRoleName(pCxt, pName, true));
  SCreateRoleStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_ROLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->name, pName);
  pStmt->ignoreExists = ignoreExists;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropRoleStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkRoleName(pCxt, pName, true));
  SDropRoleStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_ROLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->name, pName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
_err:
  return NULL;
}

/**
 * used by user and role
 */
SNode* createAlterRoleStmt(SAstCreateContext* pCxt, SToken* pName, int8_t alterType, void* pAlterInfo) {
  SAlterRoleStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pName));
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_ROLE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->name, pName);
  pStmt->alterType = alterType;
  switch (alterType) {
    case TSDB_ALTER_ROLE_LOCK: {
      SToken* pVal = pAlterInfo;
      pStmt->lock = taosStr2Int8(pVal->z, NULL, 10);
      break;
    }
    default:
      break;
  }
  return (SNode*)pStmt;
_err:
  nodesDestroyNode((SNode*)pStmt);
  return NULL;
}


SNode* createCreateTokenStmt(SAstCreateContext* pCxt, SToken* pTokenName, SToken* pUserName, STokenOptions* opts, bool ignoreExists) {
  SCreateTokenStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTokenName(pCxt, pTokenName));
  CHECK_NAME(checkUserName(pCxt, pUserName));

  if (opts == NULL) {
    opts = createDefaultTokenOptions(pCxt);
  } else if (!isValidTokenOptions(pCxt, opts)) {
    goto _err;
  }

  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TOKEN_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  COPY_STRING_FORM_ID_TOKEN(pStmt->name, pTokenName);
  COPY_STRING_FORM_ID_TOKEN(pStmt->user, pUserName);
  pStmt->enable = opts->enable;
  pStmt->ignoreExists = ignoreExists;
  pStmt->ttl = opts->ttl;
  tstrncpy(pStmt->provider, opts->provider, sizeof(pStmt->provider));
  tstrncpy(pStmt->extraInfo, opts->extraInfo, sizeof(pStmt->extraInfo));
  nodesDestroyNode((SNode*)opts);
  return (SNode*)pStmt;

_err:
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyNode((SNode*)opts);
  return NULL;
}



SNode* createAlterTokenStmt(SAstCreateContext* pCxt, SToken* pTokenName, STokenOptions* opts) {
  SAlterTokenStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTokenName(pCxt, pTokenName));
  if (!isValidTokenOptions(pCxt, opts)) {
    goto _err;
  }

  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_TOKEN_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  COPY_STRING_FORM_ID_TOKEN(pStmt->name, pTokenName);
  pStmt->pTokenOptions = opts;
  return (SNode*)pStmt;

_err:
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyNode((SNode*)opts);
  return NULL;
}



SNode* createDropTokenStmt(SAstCreateContext* pCxt, SToken* pTokenName, bool ignoreNotExists) {
  SDropTokenStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTokenName(pCxt, pTokenName));

  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_TOKEN_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  COPY_STRING_FORM_ID_TOKEN(pStmt->name, pTokenName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;

_err:
  nodesDestroyNode((SNode*)pStmt);
  return NULL;
}

SNode* createCreateTotpSecretStmt(SAstCreateContext* pCxt, SToken* pUserName) {
  SCreateTotpSecretStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pUserName));

  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TOTP_SECRET_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  COPY_STRING_FORM_ID_TOKEN(pStmt->user, pUserName);
  return (SNode*)pStmt;

_err:
  nodesDestroyNode((SNode*)pStmt);
  return NULL;
}


SNode* createDropTotpSecretStmt(SAstCreateContext* pCxt, SToken* pUserName) {
  SDropTotpSecretStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pUserName));

  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_TOTP_SECRET_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  COPY_STRING_FORM_ID_TOKEN(pStmt->user, pUserName);
  return (SNode*)pStmt;

_err:
  nodesDestroyNode((SNode*)pStmt);
  return NULL;
}


SNode* createDropEncryptAlgrStmt(SAstCreateContext* pCxt, SToken* algorithmId) {
  CHECK_PARSER_STATUS(pCxt);
  if (algorithmId->n >= TSDB_ENCRYPT_ALGR_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ALGR_ID_TOO_LONG);
    goto _err;
  }
  SDropEncryptAlgrStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_ENCRYPT_ALGR_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  (void)trimString(algorithmId->z, algorithmId->n, pStmt->algorithmId, sizeof(pStmt->algorithmId));
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateDnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_DNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (!checkAndSplitEndpoint(pCxt, pFqdn, pPort, pStmt->fqdn, &pStmt->port)) {
    nodesDestroyNode((SNode*)pStmt);
    return NULL;
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, bool force, bool unsafe) {
  CHECK_PARSER_STATUS(pCxt);
  SDropDnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_DNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (TK_NK_INTEGER == pDnode->type) {
    pStmt->dnodeId = taosStr2Int32(pDnode->z, NULL, 10);
  } else {
    if (!checkAndSplitEndpoint(pCxt, pDnode, NULL, pStmt->fqdn, &pStmt->port)) {
      nodesDestroyNode((SNode*)pStmt);
      return NULL;
    }
  }
  pStmt->force = force;
  pStmt->unsafe = unsafe;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAlterDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, const SToken* pConfig,
                            const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterDnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_DNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (NULL != pDnode) {
    pStmt->dnodeId = taosStr2Int32(pDnode->z, NULL, 10);
  } else {
    pStmt->dnodeId = -1;
  }
  (void)trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    (void)trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCreateAlgrStmt(SAstCreateContext* pCxt, SToken* algorithmId, const SToken* name, const SToken* desc,
                            const SToken* type, const SToken* osslAlgrName) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateEncryptAlgrStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_ENCRYPT_ALGORITHMS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (algorithmId->n >= TSDB_ENCRYPT_ALGR_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ALGR_ID_TOO_LONG);
    goto _err;
  }
  if (name->n >= TSDB_ENCRYPT_ALGR_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ALGR_NAME_TOO_LONG);
    goto _err;
  }
  if (desc->n >= TSDB_ENCRYPT_ALGR_DESC_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ALGR_DESC_TOO_LONG);
    goto _err;
  }
  if (type->n >= TSDB_ENCRYPT_ALGR_TYPE_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ALGR_TYPE_TOO_LONG);
    goto _err;
  }
  if (osslAlgrName->n >= TSDB_ENCRYPT_ALGR_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ALGR_OSSL_NAME_TOO_LONG);
    goto _err;
  }
  (void)trimString(algorithmId->z, algorithmId->n, pStmt->algorithmId, sizeof(pStmt->algorithmId));
  (void)trimString(name->z, name->n, pStmt->name, sizeof(pStmt->name));
  (void)trimString(desc->z, desc->n, pStmt->desc, sizeof(pStmt->desc));
  (void)trimString(type->z, type->n, pStmt->algrType, sizeof(pStmt->algrType));
  (void)trimString(osslAlgrName->z, osslAlgrName->n, pStmt->osslAlgrName, sizeof(pStmt->osslAlgrName));
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCreateAnodeStmt(SAstCreateContext* pCxt, const SToken* pUrl) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateAnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_ANODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  (void)trimString(pUrl->z, pUrl->n, pStmt->url, sizeof(pStmt->url));
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropAnodeStmt(SAstCreateContext* pCxt, const SToken* pAnode) {
  CHECK_PARSER_STATUS(pCxt);
  SUpdateAnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_ANODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (NULL != pAnode) {
    pStmt->anodeId = taosStr2Int32(pAnode->z, NULL, 10);
  } else {
    pStmt->anodeId = -1;
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createUpdateAnodeStmt(SAstCreateContext* pCxt, const SToken* pAnode, bool updateAll) {
  CHECK_PARSER_STATUS(pCxt);
  SUpdateAnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_UPDATE_ANODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (NULL != pAnode) {
    pStmt->anodeId = taosStr2Int32(pAnode->z, NULL, 10);
  } else {
    pStmt->anodeId = -1;
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCreateBnodeStmt(SAstCreateContext* pCxt, const SToken* pDnodeId, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateBnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_BNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);

  pStmt->pOptions = (SBnodeOptions*)pOptions;

  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropBnodeStmt(SAstCreateContext* pCxt, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SUpdateBnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_BNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);

  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDefaultBnodeOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SBnodeOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_BNODE_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);

  tstrncpy(pOptions->protoStr, TSDB_BNODE_OPT_PROTO_DFT_STR, TSDB_BNODE_OPT_PROTO_STR_LEN);
  pOptions->proto = TSDB_BNODE_OPT_PROTO_DEFAULT;

  return (SNode*)pOptions;
_err:
  return NULL;
}

static SNode* setBnodeOptionImpl(SAstCreateContext* pCxt, SNode* pBodeOptions, EBnodeOptionType type, void* pVal,
                                 bool alter) {
  CHECK_PARSER_STATUS(pCxt);
  SBnodeOptions* pOptions = (SBnodeOptions*)pBodeOptions;
  switch (type) {
    case BNODE_OPTION_PROTOCOL:
      COPY_STRING_FORM_STR_TOKEN(pOptions->protoStr, (SToken*)pVal);
      break;
    default:
      break;
  }

  return pBodeOptions;
_err:
  nodesDestroyNode(pBodeOptions);
  return NULL;
}

SNode* setBnodeOption(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pOption, void* pVal) {
  if (0 == strncasecmp(pOption->z, "protocol", 8)) {
    return setBnodeOptionImpl(pCxt, pOptions, BNODE_OPTION_PROTOCOL, pVal, false);
  } else {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return pOptions;
  }
}

SNode* createCreateXnodeWithUserPassStmt(SAstCreateContext* pCxt, const SToken* pUrl, SToken* pUser,
                                         const SToken* pPass) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateXnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_XNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  (void)trimString(pUrl->z, pUrl->n, pStmt->url, sizeof(pStmt->url));

  if (pUser != NULL) {
    CHECK_NAME(checkUserName(pCxt, pUser));
    COPY_STRING_FORM_ID_TOKEN(pStmt->user, pUser);
  }
  if (pPass != NULL) {
    if (pPass->n <= 2) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode password should not be empty");
      goto _err;
    }
    strncpy(pStmt->pass, pPass->z + 1, pPass->n - 2);
    pStmt->pass[sizeof(pStmt->pass) - 1] = '\0';
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}
SNode* createCreateXnodeStmt(SAstCreateContext* pCxt, const SToken* pUrl) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateXnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_XNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  (void)trimString(pUrl->z, pUrl->n, pStmt->url, sizeof(pStmt->url));
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropXnodeStmt(SAstCreateContext* pCxt, const SToken* pXnode, bool force) {
  if (NULL == pXnode) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "xnode id should not be NULL or empty");
    goto _err;
  }
  CHECK_PARSER_STATUS(pCxt);
  SDropXnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_XNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->force = force;
  if (pXnode->type == TK_NK_STRING) {
    if (pXnode->n <= 2) {
      pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "xnode url should not be all be NULL");
        goto _err;
    }
    COPY_STRING_FORM_STR_TOKEN(pStmt->url, pXnode);
  } else if(pXnode->type == TK_NK_INTEGER) {
    pStmt->xnodeId = taosStr2Int32(pXnode->z, NULL, 10);
  } else {
    pCxt->errCode =
      generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "xnode id or url should not be all be NULL");
  }

  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDrainXnodeStmt(SAstCreateContext* pCxt, const SToken* pXnode) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pXnode) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "xnode id should not be NULL or empty");
    goto _err;
  }
  if (pXnode->type != TK_NK_INTEGER) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "xnode id should be an integer");
    goto _err;
  }

  SDrainXnodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DRAIN_XNODE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->xnodeId = taosStr2Int32(pXnode->z, NULL, 10);
  if (pStmt->xnodeId <= 0) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "xnode id should be greater than 0");
    goto _err;
  }

  return (SNode*)pStmt;
_err:
  return NULL;
}

// SNode* createUpdateXnodeStmt(SAstCreateContext* pCxt, const SToken* pXnode, bool updateAll) {
//   CHECK_PARSER_STATUS(pCxt);
//   SUpdateXnodeStmt* pStmt = NULL;
//   pCxt->errCode = nodesMakeNode(QUERY_NODE_UPDATE_XNODE_STMT, (SNode**)&pStmt);
//   CHECK_MAKE_NODE(pStmt);
//   if (NULL != pXnode) {
//     pStmt->xnodeId = taosStr2Int32(pXnode->z, NULL, 10);
//   } else {
//     pStmt->xnodeId = -1;
//   }
//   return (SNode*)pStmt;
// _err:
//   return NULL;
// }

EXnodeResourceType setXnodeResourceType(SAstCreateContext* pCxt, const SToken* pResourceId) {
  CHECK_PARSER_STATUS(pCxt);
  const size_t TASK_LEN = 4;
  const size_t TASKS_LEN = 5;
  const size_t AGENT_LEN = 5;
  const size_t AGENTS_LEN = 6;
  const size_t JOB_LEN = 3;
  const size_t JOBS_LEN = 4;

  if (pResourceId->z[0] == '`') {
    if (strncmp(pResourceId->z + 1, "task", TASK_LEN) == 0 || strncmp(pResourceId->z + 1, "tasks", TASKS_LEN) == 0) {
      return XNODE_TASK;
    }
    if (strncmp(pResourceId->z + 1, "agent", AGENT_LEN) == 0 ||
        strncmp(pResourceId->z + 1, "agents", AGENTS_LEN) == 0) {
      return XNODE_AGENT;
    }
    if (strncmp(pResourceId->z + 1, "job", JOB_LEN) == 0 || strncmp(pResourceId->z + 1, "jobs", JOBS_LEN) == 0) {
      return XNODE_JOB;
    }

    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode resource type (task/agent) at: %s", pResourceId->z);
    goto _err;
  }
  if (strncmp(pResourceId->z, "task", TASK_LEN) == 0 || strncmp(pResourceId->z, "tasks", TASKS_LEN) == 0) {
    return XNODE_TASK;
  }
  if (strncmp(pResourceId->z, "agent", AGENT_LEN) == 0 || strncmp(pResourceId->z, "agents", AGENTS_LEN) == 0) {
    return XNODE_AGENT;
  }
  if (strncmp(pResourceId->z, "job", JOB_LEN) == 0 || strncmp(pResourceId->z, "jobs", JOBS_LEN) == 0) {
    return XNODE_JOB;
  }
  pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                          "Invalid xnode resource type (task/agent/job) at: %s", pResourceId->z);
  goto _err;

_err:
  return XNODE_UNKNOWN;
}
SNode* createXnodeSourceAsDsn(SAstCreateContext* pCxt, const SToken* pToken) {
  SXTaskSource* pSource = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_XNODE_TASK_SOURCE_OPT, (SNode**)&pSource);
  CHECK_MAKE_NODE(pSource);
  if (pToken == NULL || pToken->n <= 0) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "xnode source dsn should not be NULL or empty");
    goto _err;
  }
  if (pToken->n > TSDB_XNODE_TASK_SOURCE_LEN) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode source dsn length: %d, max length: %d", pToken->n,
                                            TSDB_XNODE_TASK_SOURCE_LEN);
    goto _err;
  }
  pSource->source.type = XNODE_TASK_SOURCE_DSN;
  COPY_COW_STR_FROM_STR_TOKEN(pSource->source.cstr, pToken);
  return (SNode*)pSource;
_err:
  return NULL;
}
SNode* createXnodeSourceAsDatabase(SAstCreateContext* pCxt, const SToken* pToken) {
  SXTaskSource* pSource = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_XNODE_TASK_SOURCE_OPT, (SNode**)&pSource);
  CHECK_MAKE_NODE(pSource);
  if (pToken == NULL || pToken->n <= 0) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "xnode source database should not be NULL or empty");
    goto _err;
  }
  if (pToken->n > TSDB_XNODE_TASK_SOURCE_LEN) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode source database length: %d, max length: %d", pToken->n,
                                            TSDB_XNODE_TASK_SOURCE_LEN);
    goto _err;
  }
  pSource->source.type = XNODE_TASK_SOURCE_DATABASE;
  COPY_COW_STR_FROM_ID_TOKEN(pSource->source.cstr, pToken);
  return (SNode*)pSource;
_err:
  return NULL;
}
SNode* createXnodeSourceAsTopic(SAstCreateContext* pCxt, const SToken* pToken) {
  SXTaskSource* pSource = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_XNODE_TASK_SOURCE_OPT, (SNode**)&pSource);
  CHECK_MAKE_NODE(pSource);
  if (pToken == NULL || pToken->n <= 0) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "xnode source dsn should not be NULL or empty");
    goto _err;
  }
  if (pToken->n > TSDB_TOPIC_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode source topic length: %d, max length: %d", pToken->n,
                                            TSDB_TOPIC_NAME_LEN);
    goto _err;
  }
  pSource->source.type = XNODE_TASK_SOURCE_TOPIC;
  COPY_COW_STR_FROM_STR_TOKEN(pSource->source.cstr, pToken);
  return (SNode*)pSource;
_err:
  return NULL;
}
SNode* createXnodeSinkAsDsn(SAstCreateContext* pCxt, const SToken* pToken) {
  SXTaskSink* pSink = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_XNODE_TASK_SINK_OPT, (SNode**)&pSink);
  CHECK_MAKE_NODE(pSink);
  if (pToken == NULL || pToken->n <= 0) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "xnode sink dsn should not be NULL or empty");
    goto _err;
  }
  if (pToken->n > TSDB_XNODE_TASK_SINK_LEN) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode sink dsn length: %d, max length: %d", pToken->n,
                                            TSDB_XNODE_TASK_SINK_LEN);
    goto _err;
  }
  pSink->sink.type = XNODE_TASK_SINK_DSN;
  COPY_COW_STR_FROM_STR_TOKEN(pSink->sink.cstr, pToken);
  return (SNode*)pSink;
_err:
  return NULL;
}
SNode* createXnodeSinkAsDatabase(SAstCreateContext* pCxt, const SToken* pToken) {
  SXTaskSink* pSink = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_XNODE_TASK_SINK_OPT, (SNode**)&pSink);
  CHECK_MAKE_NODE(pSink);
  if (pToken == NULL || pToken->n <= 0) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode sink database should not be NULL or empty");
    goto _err;
  }
  if (pToken->n > TSDB_XNODE_TASK_SINK_LEN) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode sink database length: %d, max length: %d", pToken->n,
                                            TSDB_XNODE_TASK_SINK_LEN);
    goto _err;
  }
  pSink->sink.type = XNODE_TASK_SINK_DATABASE;
  if (pToken->type == TK_NK_STRING) {
    COPY_COW_STR_FROM_STR_TOKEN(pSink->sink.cstr, pToken);
  } else if (pToken->type == TK_NK_ID) {
    COPY_COW_STR_FROM_ID_TOKEN(pSink->sink.cstr, pToken);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode sink database type: %d", pToken->type);
    goto _err;
  }

  return (SNode*)pSink;
_err:
  return NULL;
}

SNode* createXnodeTaskWithOptionsDirectly(SAstCreateContext* pCxt, const SToken* pResourceName, SNode* pSource,
                                          SNode* pSink, SNode* pNode) {
  SNode* pStmt = NULL;
  if (pResourceName == NULL) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode task name should not be NULL");
    goto _err;
  }
  if (pSource == NULL || pSink == NULL) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode task source and sink should not be NULL");
    goto _err;
  }
  if (nodeType(pSource) != QUERY_NODE_XNODE_TASK_SOURCE_OPT || nodeType(pSink) != QUERY_NODE_XNODE_TASK_SINK_OPT) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode task source and sink should be valid nodes");
    goto _err;
  }
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_XNODE_TASK_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  SCreateXnodeTaskStmt* pTaskStmt = (SCreateXnodeTaskStmt*)pStmt;
  if (pResourceName->type == TK_NK_STRING) {
    COPY_STRING_FORM_STR_TOKEN(pTaskStmt->name, pResourceName);
  } else if (pResourceName->type == TK_NK_ID) {
    COPY_STRING_FORM_STR_TOKEN(pTaskStmt->name, pResourceName);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Invalid xnode name type: %d",
                                            pResourceName->type);
    goto _err;
  }

  if (pSource != NULL) {
    SXTaskSource* source = (SXTaskSource*)(pSource);
    pTaskStmt->source = source;
  }
  if (pSink != NULL) {
    SXTaskSink* sink = (SXTaskSink*)(pSink);
    pTaskStmt->sink = sink;
  }
  if (pNode != NULL) {
    if (nodeType(pNode) == QUERY_NODE_XNODE_TASK_OPTIONS) {
      SXnodeTaskOptions* options = (SXnodeTaskOptions*)(pNode);
      pTaskStmt->options = options;
    }
  }
  return (SNode*)pTaskStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  if (pNode != NULL) {
    nodesDestroyNode(pNode);
  }
  return NULL;
}

SNode* createXnodeAgentWithOptionsDirectly(SAstCreateContext* pCxt, const SToken* pResourceName, SNode* pOptions) {
  SNode* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_XNODE_AGENT_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  SCreateXnodeAgentStmt* pAgentStmt = (SCreateXnodeAgentStmt*)pStmt;

  if (pOptions != NULL) {
    if (nodeType(pOptions) == QUERY_NODE_XNODE_TASK_OPTIONS) {
      SXnodeTaskOptions* options = (SXnodeTaskOptions*)(pOptions);
      pAgentStmt->options = options;
    }
  }

  if (pResourceName->type == TK_NK_STRING && pResourceName->n > 2) {
    COPY_STRING_FORM_STR_TOKEN(pAgentStmt->name, pResourceName);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode agent name type: %d", pResourceName->type);
    goto _err;
  }

  return (SNode*)pAgentStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}

SNode* createXnodeTaskWithOptions(SAstCreateContext* pCxt, EXnodeResourceType resourceType, const SToken* pResourceName,
                                  SNode* pSource, SNode* pSink, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);

  switch (resourceType) {
    case XNODE_TASK: {
      return createXnodeTaskWithOptionsDirectly(pCxt, pResourceName, pSource, pSink, pNode);
    }
    case XNODE_AGENT: {
      return createXnodeAgentWithOptionsDirectly(pCxt, pResourceName, pNode);
      break;
    }
    default:
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "Invalid xnode resource type: %d", resourceType);
      goto _err;
  }
_err:
  return NULL;
}

SNode* createStartXnodeTaskStmt(SAstCreateContext* pCxt, const EXnodeResourceType resourceType, SToken* pIdOrName) {
  SNode* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  if (resourceType != XNODE_TASK) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode resource type: %d", resourceType);
    goto _err;
  }
  if (pIdOrName == NULL || (pIdOrName != NULL && pIdOrName->type != TK_NK_INTEGER && pIdOrName->type != TK_NK_STRING)) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode task id or name should be an integer or string");
    goto _err;
  }

  pCxt->errCode = nodesMakeNode(QUERY_NODE_START_XNODE_TASK_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  SStartXnodeTaskStmt* pTaskStmt = (SStartXnodeTaskStmt*)pStmt;
  if (pIdOrName->type == TK_NK_INTEGER) {
    pTaskStmt->tid = taosStr2Int32(pIdOrName->z, NULL, 10);
    if (pTaskStmt->tid <= 0) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Task id should be greater than 0");
      goto _err;
    }
  } else {
    if (pIdOrName->n > TSDB_XNODE_RESOURCE_NAME_LEN + 2) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                  "Xnode task name should be less than %d characters", TSDB_XNODE_RESOURCE_NAME_LEN);
      goto _err;
    }
    char buf[TSDB_XNODE_RESOURCE_NAME_LEN + 1] = {0};
    COPY_STRING_FORM_STR_TOKEN(buf, pIdOrName);
    pTaskStmt->name = xCreateCowStr(strlen(buf), buf, true);
  }

  return (SNode*)pTaskStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}

SNode* createStopXnodeTaskStmt(SAstCreateContext* pCxt, const EXnodeResourceType resourceType, SToken* pIdOrName) {
  SNode* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  if (resourceType != XNODE_TASK) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Only support stop task, invalid resource type: %d", resourceType);
    goto _err;
  }
  if (pIdOrName != NULL && pIdOrName->type != TK_NK_INTEGER && pIdOrName->type != TK_NK_STRING) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode task id or name should be an integer or string");
    goto _err;
  }
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STOP_XNODE_TASK_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  SStopXnodeTaskStmt* pTaskStmt = (SStopXnodeTaskStmt*)pStmt;
  if (pIdOrName->type == TK_NK_INTEGER) {
    pTaskStmt->tid = taosStr2Int32(pIdOrName->z, NULL, 10);
    if (pTaskStmt->tid <= 0) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Task id should be greater than 0");
      goto _err;
    }
  } else {
    if (pIdOrName->n > TSDB_XNODE_RESOURCE_NAME_LEN + 2) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                  "Xnode task name should be less than %d characters", TSDB_XNODE_RESOURCE_NAME_LEN);
      goto _err;
    }
    char buf[TSDB_XNODE_RESOURCE_NAME_LEN + 1] = {0};
    COPY_STRING_FORM_STR_TOKEN(buf, pIdOrName);
    pTaskStmt->name = xCreateCowStr(strlen(buf), buf, true);
  }

  return (SNode*)pTaskStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}

SNode* rebalanceXnodeJobWithOptionsDirectly(SAstCreateContext* pCxt, const SToken* pResourceId, SNode* pNode) {
  SNode* pStmt = NULL;
  if (pResourceId == NULL) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode job id should not be NULL");
    goto _err;
  }
  if (pNode == NULL) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode job options should not be NULL");
    goto _err;
  }
  if (pResourceId->type != TK_NK_INTEGER) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode job id should be an integer");
    goto _err;
  }
  pCxt->errCode = nodesMakeNode(QUERY_NODE_REBALANCE_XNODE_JOB_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  SRebalanceXnodeJobStmt* pJobStmt = (SRebalanceXnodeJobStmt*)pStmt;
  char                    buf[TSDB_XNODE_RESOURCE_ID_LEN] = {0};
  COPY_STRING_FORM_ID_TOKEN(buf, pResourceId);
  pJobStmt->jid = atoi(buf);

  if (nodeType(pNode) == QUERY_NODE_XNODE_TASK_OPTIONS) {
    SXnodeTaskOptions* options = (SXnodeTaskOptions*)(pNode);
    // printXnodeTaskOptions(&options->opts);
    pJobStmt->options = options;
  }
  return (SNode*)pJobStmt;
_err:
  return NULL;
}

SNode* createRebalanceXnodeJobStmt(SAstCreateContext* pCxt, EXnodeResourceType resourceType, const SToken* resourceId,
                                   SNode* pNodeOptions) {
  CHECK_PARSER_STATUS(pCxt);

  switch (resourceType) {
    case XNODE_JOB: {
      return rebalanceXnodeJobWithOptionsDirectly(pCxt, resourceId, pNodeOptions);
    }
    default:
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                  "Invalid xnode resource type: %d, rebalance only support job", resourceType);
      goto _err;
  }
_err:
  return NULL;
}

SNode* rebalanceXnodeJobWhereDirectly(SAstCreateContext* pCxt, SNode* pWhere) {
  int32_t code = 0;
  SNode*  pStmt = NULL;

  pCxt->errCode = nodesMakeNode(QUERY_NODE_REBALANCE_XNODE_JOB_WHERE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  SRebalanceXnodeJobWhereStmt* pJobStmt = (SRebalanceXnodeJobWhereStmt*)pStmt;
  pJobStmt->pWhere = pWhere;

  return (SNode*)pJobStmt;
_err:
  return NULL;
}

SNode* createRebalanceXnodeJobWhereStmt(SAstCreateContext* pCxt, EXnodeResourceType resourceType, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);

  switch (resourceType) {
    case XNODE_JOB: {
      return rebalanceXnodeJobWhereDirectly(pCxt, pWhere);
    }
    default:
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                  "Invalid xnode resource type: %d, rebalance only support job", resourceType);
      goto _err;
  }
_err:
  return NULL;
}

SNode* updateXnodeTaskWithOptionsDirectly(SAstCreateContext* pCxt, const SToken* pResIdOrName, SNode* pSource,
                                          SNode* pSink, SNode* pNode) {
  SNode* pStmt = NULL;

  if ((pSource != NULL && nodeType(pSource) != QUERY_NODE_XNODE_TASK_SOURCE_OPT) ||
      (pSink != NULL && nodeType(pSink) != QUERY_NODE_XNODE_TASK_SINK_OPT)) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode task source and sink should be valid nodes");
    goto _err;
  }
  if (pSource == NULL && pSink == NULL && pNode == NULL) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode task source, sink, and with options can't all be NULL");
    goto _err;
  }

  pCxt->errCode = nodesMakeNode(QUERY_NODE_UPDATE_XNODE_TASK_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  SUpdateXnodeTaskStmt* pTaskStmt = (SUpdateXnodeTaskStmt*)pStmt;
  if (pResIdOrName->type == TK_NK_INTEGER) {
    pTaskStmt->tid = taosStr2Int32(pResIdOrName->z, NULL, 10);
  } else {
    if (pResIdOrName->n <= 2) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode task name can't be empty string");
      goto _err;
    }
    char buf[TSDB_XNODE_TASK_NAME_LEN] = {0};
    COPY_STRING_FORM_STR_TOKEN(buf, pResIdOrName);
    pTaskStmt->name = xCreateCowStr(strlen(buf), buf, true);
  }

  if (pSource != NULL) {
    SXTaskSource* source = (SXTaskSource*)(pSource);
    pTaskStmt->source = source;
  }
  if (pSink != NULL) {
    SXTaskSink* sink = (SXTaskSink*)(pSink);
    pTaskStmt->sink = sink;
  }
  if (pNode != NULL) {
    if (nodeType(pNode) == QUERY_NODE_XNODE_TASK_OPTIONS) {
      SXnodeTaskOptions* options = (SXnodeTaskOptions*)(pNode);
      pTaskStmt->options = options;
    }
  }
  return (SNode*)pTaskStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}

SNode* alterXnodeJobWithOptionsDirectly(SAstCreateContext* pCxt, const SToken* pResourceName, SNode* pNode) {
  SNode* pStmt = NULL;
  if (pResourceName == NULL) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode job id should not be NULL");
    goto _err;
  }
  if (pNode == NULL) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode job options should not be NULL");
    goto _err;
  }
  if (pResourceName->type != TK_NK_INTEGER) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode job id should be integer");
    goto _err;
  }
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_XNODE_JOB_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  SAlterXnodeJobStmt* pJobStmt = (SAlterXnodeJobStmt*)pStmt;
  char                buf[TSDB_XNODE_RESOURCE_ID_LEN] = {0};
  COPY_STRING_FORM_ID_TOKEN(buf, pResourceName);
  pJobStmt->jid = atoi(buf);

  if (nodeType(pNode) == QUERY_NODE_XNODE_TASK_OPTIONS) {
    SXnodeTaskOptions* options = (SXnodeTaskOptions*)(pNode);
    pJobStmt->options = options;
  }
  return (SNode*)pJobStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}

SNode* alterXnodeAgentWithOptionsDirectly(SAstCreateContext* pCxt, const SToken* pResIdOrName, SNode* pNode) {
  SNode* pStmt = NULL;
  if (NULL == pNode) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode alter agent options can't be null");
    goto _err;
  }

  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_XNODE_AGENT_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  SAlterXnodeAgentStmt* pAgentStmt = (SAlterXnodeAgentStmt*)pStmt;
  if (pResIdOrName->type == TK_NK_INTEGER) {
    pAgentStmt->id = taosStr2Int32(pResIdOrName->z, NULL, 10);
  } else {
    if (pResIdOrName->n <= 2) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode alter agent name can't be empty string");
      goto _err;
    }
    char buf[TSDB_XNODE_AGENT_NAME_LEN] = {0};
    COPY_STRING_FORM_STR_TOKEN(buf, pResIdOrName);
    pAgentStmt->name = xCreateCowStr(strlen(buf), buf, true);
  }

  if (nodeType(pNode) == QUERY_NODE_XNODE_TASK_OPTIONS) {
    SXnodeTaskOptions* options = (SXnodeTaskOptions*)(pNode);
    pAgentStmt->options = options;
  }

  return (SNode*)pAgentStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}

SNode* alterXnodeTaskWithOptions(SAstCreateContext* pCxt, EXnodeResourceType resourceType, const SToken* pResIdOrName,
                                 SNode* pSource, SNode* pSink, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);

  switch (resourceType) {
    case XNODE_TASK: {
      return updateXnodeTaskWithOptionsDirectly(pCxt, pResIdOrName, pSource, pSink, pNode);
    }
    case XNODE_AGENT: {
      return alterXnodeAgentWithOptionsDirectly(pCxt, pResIdOrName, pNode);
    }
    case XNODE_JOB: {
      return alterXnodeJobWithOptionsDirectly(pCxt, pResIdOrName, pNode);
    }
    default:
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "Invalid xnode resource type: %d", resourceType);
      goto _err;
  }
_err:
  return NULL;
}

SNode* dropXnodeResource(SAstCreateContext* pCxt, EXnodeResourceType resourceType, SToken* pResourceName) {
  SNode* pStmt = NULL;
  char   buf[TSDB_XNODE_TASK_NAME_LEN + 1] = {0};

  CHECK_PARSER_STATUS(pCxt);
  if (pResourceName == NULL || pResourceName->n <= 0) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Xnode resource name should not be NULL or empty");
    goto _err;
  }
  if (pResourceName->n > TSDB_XNODE_RESOURCE_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "Invalid xnode resource name length: %d, max length: %d", pResourceName->n,
                                            TSDB_XNODE_RESOURCE_NAME_LEN);
    goto _err;
  }
  switch (resourceType) {
    case XNODE_TASK:
      pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_XNODE_TASK_STMT, (SNode**)&pStmt);
      CHECK_MAKE_NODE(pStmt);
      SDropXnodeTaskStmt* pTaskStmt = (SDropXnodeTaskStmt*)pStmt;

      if (pResourceName->type == TK_NK_STRING) {
        if (pResourceName->n > TSDB_XNODE_TASK_NAME_LEN + 2) {
          pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                  "Invalid xnode task name length: %d, max length: %d",
                                                  pResourceName->n, TSDB_XNODE_TASK_NAME_LEN);
          goto _err;
        }
        COPY_STRING_FORM_STR_TOKEN(buf, pResourceName);
        pTaskStmt->name = taosStrndupi(buf, sizeof(buf));
      } else if (pResourceName->type == TK_NK_ID) {
        COPY_STRING_FORM_ID_TOKEN(buf, pResourceName);
        pTaskStmt->name = taosStrndupi(buf, sizeof(buf));
      } else if (pResourceName->type == TK_NK_INTEGER) {
        pTaskStmt->id = taosStr2Int32(pResourceName->z, NULL, 10);
      } else {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "Invalid xnode job id type: %d", pResourceName->type);
        goto _err;
      }
      break;
    case XNODE_AGENT:
      pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_XNODE_AGENT_STMT, (SNode**)&pStmt);
      CHECK_MAKE_NODE(pStmt);
      SDropXnodeAgentStmt* pDropAgent = (SDropXnodeAgentStmt*)pStmt;

      if (pResourceName->type == TK_NK_STRING) {
        if (pResourceName->n > TSDB_XNODE_TASK_NAME_LEN + 2) {
          pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                  "Invalid xnode task name length: %d, max length: %d",
                                                  pResourceName->n, TSDB_XNODE_TASK_NAME_LEN);
          goto _err;
        }
        COPY_STRING_FORM_STR_TOKEN(buf, pResourceName);
        pDropAgent->name = taosStrndupi(buf, sizeof(buf));
      } else if (pResourceName->type == TK_NK_ID) {
        COPY_STRING_FORM_ID_TOKEN(buf, pResourceName);
        pDropAgent->name = taosStrndupi(buf, sizeof(buf));
      } else if (pResourceName->type == TK_NK_INTEGER) {
        pDropAgent->id = taosStr2Int32(pResourceName->z, NULL, 10);
      } else {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "Invalid xnode agent id type: %d", pResourceName->type);
        goto _err;
      }
      break;
    case XNODE_JOB:
      pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_XNODE_JOB_STMT, (SNode**)&pStmt);
      CHECK_MAKE_NODE(pStmt);
      SDropXnodeJobStmt* pJobStmt = (SDropXnodeJobStmt*)pStmt;

      if (pResourceName->type == TK_NK_STRING) {
        pJobStmt->jid = taosStr2Int32(pResourceName->z, NULL, 10);
      } else if (pResourceName->type == TK_NK_INTEGER) {
        pJobStmt->jid = taosStr2Int32(pResourceName->z, NULL, 10);
      } else {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "Invalid xnode job id type: %d", pResourceName->type);
        goto _err;
      }
      break;
    default:
      break;
  }
  return (SNode*)pStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}
// SNode* dropXnodeResourceOn(SAstCreateContext* pCxt, EXnodeResourceType resourceType, SToken* pResource, SNode*
// pWhere) {
//   char resourceId[TSDB_XNODE_RESOURCE_ID_LEN + 1];
//   SShowStmt* pStmt = NULL;

//   CHECK_PARSER_STATUS(pCxt);
//   if (pResource == NULL || pResource->n <= 0) {
//     pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
//                                             "xnode resource name should not be NULL or empty");
//     goto _err;
//   }
//   if (pResource->n > TSDB_XNODE_RESOURCE_NAME_LEN) {
//     pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
//                                             "Invalid xnode resource name length: %d, max length: %d", pResource->n,
//                                             TSDB_XNODE_RESOURCE_NAME_LEN);
//     goto _err;
//   }
//   switch (resourceType) {
//     case XNODE_TASK:
//       // pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_XNODE_TASK_STMT, (SNode**)&pStmt);
//       pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_XNODE_TASKS_STMT, (SNode**)&pStmt);
//       CHECK_MAKE_NODE(pStmt);
//       break;
//     case XNODE_AGENT:
//       pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_XNODE_AGENT_STMT, (SNode**)&pStmt);
//       CHECK_MAKE_NODE(pStmt);
//       break;
//     case XNODE_JOB:
//       pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_XNODE_JOBS_STMT, (SNode**)&pStmt);
//       CHECK_MAKE_NODE(pStmt);
//       break;
//     default:
//       break;
//   }
//   return (SNode*)pStmt;
// _err:
//   return NULL;
// }

SNode* dropXnodeResourceWhere(SAstCreateContext* pCxt, EXnodeResourceType resourceType, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  SDropXnodeJobStmt* pStmt = NULL;
  switch (resourceType) {
    case XNODE_TASK:
    case XNODE_AGENT:
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "Xnode only drop xnode job where ... support");
      break;
    case XNODE_JOB:
      pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_XNODE_JOB_STMT, (SNode**)&pStmt);
      CHECK_MAKE_NODE(pStmt);
      pStmt->pWhere = pWhere;
      break;
    default:
      break;
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDefaultXnodeTaskOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SXnodeTaskOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_XNODE_TASK_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  return (SNode*)pOptions;
_err:
  return NULL;
}

static char   TRIGGER[8] = "trigger";
static SToken TRIGGER_TOKEN = {
    .n = 7,
    .type = TK_NK_ID,
    .z = TRIGGER,
};
SToken* createTriggerToken() { return &TRIGGER_TOKEN; }

SNode*  setXnodeTaskOption(SAstCreateContext* pCxt, SNode* pTaskOptions, SToken* pKey, SToken* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  if (pTaskOptions == NULL) {
    pTaskOptions = createDefaultXnodeTaskOptions(pCxt);
    if (pTaskOptions == NULL) {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Xnode task options should not be NULL");
      goto _err;
    }
  }
  SXnodeTaskOptions* pOptions = (SXnodeTaskOptions*)pTaskOptions;
  char               key[TSDB_COL_NAME_LEN] = {0};
  if (pKey == NULL) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Option name for xnode should not be empty");
    goto _err;
  }
  TRIM_STRING_FORM_ID_TOKEN(key, pKey);

  if (strlen(key) == 0) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Option name for xnode should not be empty");
    goto _err;
  }
  char via[TSDB_COL_NAME_LEN] = {0};
  char buf[TSDB_XNODE_TASK_OPTIONS_MAX_NUM] = {0};
  if (strcmp(key, "trigger") == 0) {
    if (pVal->type == TK_NK_STRING) {
      (void)trimString(pVal->z, pVal->n, pOptions->trigger, sizeof(pOptions->trigger));
      pOptions->triggerLen = pVal->n == 2 ? 1 : pVal->n - 2;
    } else {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Option trigger must be string");
      goto _err;
    }
  } else if (strcmp(key, "parser") == 0 || strcmp(key, "transform") == 0) {
    if (pVal->type == TK_NK_STRING) {
      (void)trimString(pVal->z, pVal->n, pOptions->parser, sizeof(pOptions->parser));
      pOptions->parserLen = pVal->n == 2 ? 1 : pVal->n - 2;
    } else {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Option parser must be string");
      goto _err;
    }
  } else if (strcmp(key, "health") == 0) {
    if (pVal->type == TK_NK_STRING) {
      (void)trimString(pVal->z, pVal->n, pOptions->health, sizeof(pOptions->health));
      pOptions->healthLen = pVal->n == 2 ? 1 : pVal->n - 2;
    } else {
      pCxt->errCode =
          generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Option health must be string");
      goto _err;
    }
  } else if (strcmp(key, "via") == 0) {
    switch (pVal->type) {
      case TK_NK_STRING:
        (void)trimString(pVal->z, pVal->n, via, sizeof(via));
        pOptions->via = taosStr2Int32(via, NULL, 10);
        if (pOptions->via <= 0) {
          pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                   "Invalid xnode task option via: %s", pVal->z);
          goto _err;
        }
        break;
      case TK_NK_INTEGER:
        pOptions->via = taosStr2Int32(pVal->z, NULL, 10);
        break;
      default:
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "Invalid xnode task option: %s", key);
    }
  } else {
    if (pOptions->optionsNum < TSDB_XNODE_TASK_OPTIONS_MAX_NUM) {
      char* pKeyVal = NULL;
      if (pVal != NULL) {
        parserDebug("key value length expected: %d, actual: %d\n", pKey->n + pVal->n + 2, pKey->n + pVal->n);
        pKeyVal = taosMemoryMalloc(pKey->n + pVal->n + 2);
        memset(pKeyVal, 0, pKey->n + pVal->n + 2);

        CHECK_OUT_OF_MEM(pKeyVal);
        size_t pos = strlen(key);
        memcpy(pKeyVal, key, pos);
        pKeyVal[pos] = '=';  // Add '=' after the key
        pos++;

        if (pVal->type == TK_NK_STRING) {
          (void)trimString(pVal->z, pVal->n, pKeyVal + pos, pVal->n + 1);
        } else {
          strncpy(pKeyVal + pos, pVal->z, TMIN(pVal->n, pKey->n + pVal->n + 2 - pos - 1));
          pKeyVal[pos + pVal->n] = '\0';
        }
      } else {
        size_t keyLen = strlen(key);
        pKeyVal = taosMemoryMalloc(keyLen + 1);
        memset(pKeyVal, 0, keyLen + 1);
        CHECK_OUT_OF_MEM(pKeyVal);
        memcpy(pKeyVal, key, keyLen);
      }
      pOptions->options[pOptions->optionsNum] = pKeyVal;
      pOptions->optionsNum++;
    } else {
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                               "reaches max options number(%d) %s", pOptions->optionsNum, key);
      goto _err;
    }
  }
_err:
  return pTaskOptions;
}

SNode* createXnodeTaskJobWithOptions(SAstCreateContext* pCxt, EXnodeResourceType resourceType, const SToken* pTidToken,
                                     SNode* pNodeOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = NULL;

  switch (resourceType) {
    case XNODE_JOB: {
      if (pTidToken == NULL || pTidToken->n <= 0) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "Xnode job task id should not be NULL or empty");
        goto _err;
      }
      if (pNodeOptions == NULL || nodeType(pNodeOptions) != QUERY_NODE_XNODE_TASK_OPTIONS) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "Xnode job options should not be NULL or empty");
        goto _err;
      }
      pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_XNODE_JOB_STMT, &pStmt);
      CHECK_MAKE_NODE(pStmt);
      SCreateXnodeJobStmt* pJobStmt = (SCreateXnodeJobStmt*)pStmt;
      pJobStmt->options = (SXnodeTaskOptions*)pNodeOptions;
      pJobStmt->tid = pTidToken->type == TK_NK_STRING ? atoi(pTidToken->z) : taosStr2Int32(pTidToken->z, NULL, 10);
      break;
    }
    default:
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "Invalid xnode resource type: %d with ON clause", resourceType);
      goto _err;
  }
  return pStmt;
_err:
  if (pStmt != NULL) {
    nodesDestroyNode(pStmt);
  }
  return NULL;
}

SNode* createEncryptKeyStmt(SAstCreateContext* pCxt, const SToken* pValue) {
  SToken config;
  config.type = TK_NK_STRING;
  config.z = "\"encrypt_key\"";
  config.n = strlen(config.z);
  return createAlterDnodeStmt(pCxt, NULL, &config, pValue);
}

SNode* createAlterEncryptKeyStmt(SAstCreateContext* pCxt, int8_t keyType, const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterEncryptKeyStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_ENCRYPT_KEY_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->keyType = keyType;
  if (NULL != pValue) {
    (void)trimString(pValue->z, pValue->n, pStmt->newKey, sizeof(pStmt->newKey));
  }

  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAlterKeyExpirationStmt(SAstCreateContext* pCxt, const SToken* pDays, const SToken* pStrategy) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterKeyExpirationStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_KEY_EXPIRATION_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  if (NULL != pDays) {
    pStmt->days = taosStr2Int32(pDays->z, NULL, 10);
  }
  if (NULL != pStrategy) {
    (void)trimString(pStrategy->z, pStrategy->n, pStmt->strategy, sizeof(pStmt->strategy));
  }

  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createRealTableNodeForIndexName(SAstCreateContext* pCxt, SToken* pDbName, SToken* pIndexName) {
  if (!checkIndexName(pCxt, pIndexName)) {
    return NULL;
  }
  return createRealTableNode(pCxt, pDbName, pIndexName, NULL);
}

SNode* createCreateIndexStmt(SAstCreateContext* pCxt, EIndexType type, bool ignoreExists, SNode* pIndexName,
                             SNode* pRealTable, SNodeList* pCols, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateIndexStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_INDEX_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->indexType = type;
  pStmt->ignoreExists = ignoreExists;

  SRealTableNode* pFullTable = (SRealTableNode*)pRealTable;
  if (strlen(pFullTable->table.dbName) == 0) {
    // no db specified,
    if (pCxt->pQueryCxt->db == NULL) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DB_NOT_SPECIFIED);
      CHECK_PARSER_STATUS(pCxt);
    } else {
      snprintf(pStmt->indexDbName, sizeof(pStmt->indexDbName), "%s", pCxt->pQueryCxt->db);
    }
  } else {
    snprintf(pStmt->indexDbName, sizeof(pStmt->indexDbName), "%s", pFullTable->table.dbName);
  }
  snprintf(pStmt->indexName, sizeof(pStmt->indexName), "%s", ((SColumnNode*)pIndexName)->colName);
  snprintf(pStmt->dbName, sizeof(pStmt->dbName), "%s", ((SRealTableNode*)pRealTable)->table.dbName);
  snprintf(pStmt->tableName, sizeof(pStmt->tableName), "%s", ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pIndexName);
  nodesDestroyNode(pRealTable);
  pStmt->pCols = pCols;
  pStmt->pOptions = (SIndexOptions*)pOptions;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pIndexName);
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pOptions);
  nodesDestroyList(pCols);
  return NULL;
}

SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset, SNode* pSliding,
                         SNode* pStreamOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SIndexOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_INDEX_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->pFuncs = pFuncs;
  pOptions->pInterval = pInterval;
  pOptions->pOffset = pOffset;
  pOptions->pSliding = pSliding;
  pOptions->pStreamOptions = pStreamOptions;
  return (SNode*)pOptions;
_err:
  nodesDestroyNode(pInterval);
  nodesDestroyNode(pOffset);
  nodesDestroyNode(pSliding);
  nodesDestroyNode(pStreamOptions);
  return NULL;
}

SNode* createDropIndexStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pIndexName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropIndexStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_INDEX_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  snprintf(pStmt->indexDbName, sizeof(pStmt->indexDbName), "%s", ((SRealTableNode*)pIndexName)->table.dbName);
  snprintf(pStmt->indexName, sizeof(pStmt->indexName), "%s", ((SRealTableNode*)pIndexName)->table.tableName);
  nodesDestroyNode(pIndexName);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pIndexName);
  return NULL;
}

SNode* createCreateComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateComponentNodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SDropComponentNodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createRestoreComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SRestoreComponentNodeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCreateTopicStmtUseQuery(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SNode* pQuery, bool reload) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTopicName(pCxt, pTopicName));
  SCreateTopicStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pQuery = pQuery;
  pStmt->reload = reload;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pQuery);
  return NULL;
}

SNode* createCreateTopicStmtUseDb(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SToken* pSubDbName,
                                  int8_t withMeta) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTopicName(pCxt, pTopicName));
  CHECK_NAME(checkDbName(pCxt, pSubDbName, true));
  SCreateTopicStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreExists = ignoreExists;
  COPY_STRING_FORM_ID_TOKEN(pStmt->subDbName, pSubDbName);
  pStmt->withMeta = withMeta;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCreateTopicStmtUseTable(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SNode* pRealTable,
                                     int8_t withMeta, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTopicName(pCxt, pTopicName));
  SCreateTopicStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->withMeta = withMeta;
  pStmt->pWhere = pWhere;

  tstrncpy(pStmt->subDbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->subSTbName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pWhere);
  return NULL;
}

SNode* createDropTopicStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pTopicName, bool force) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTopicName(pCxt, pTopicName));
  SDropTopicStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_TOPIC_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreNotExists = ignoreNotExists;
  pStmt->force = force;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropCGroupStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pCGroupId, SToken* pTopicName,
                            bool force) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTopicName(pCxt, pTopicName));
  CHECK_NAME(checkCGroupName(pCxt, pCGroupId));
  SDropCGroupStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_CGROUP_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  pStmt->force = force;
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  COPY_STRING_FORM_ID_TOKEN(pStmt->cgroup, pCGroupId);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAlterClusterStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterClusterStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_CLUSTER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  (void)trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    (void)trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAlterLocalStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterLocalStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_LOCAL_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  (void)trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    (void)trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDefaultExplainOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SExplainOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_EXPLAIN_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->verbose = TSDB_DEFAULT_EXPLAIN_VERBOSE;
  pOptions->ratio = TSDB_DEFAULT_EXPLAIN_RATIO;
  return (SNode*)pOptions;
_err:
  return NULL;
}

SNode* setExplainVerbose(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  ((SExplainOptions*)pOptions)->verbose = (0 == strncasecmp(pVal->z, "true", pVal->n));
  return pOptions;
_err:
  return NULL;
}

SNode* setExplainRatio(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  ((SExplainOptions*)pOptions)->ratio = taosStr2Double(pVal->z, NULL);
  return pOptions;
_err:
  return NULL;
}

SNode* createExplainStmt(SAstCreateContext* pCxt, bool analyze, SNode* pOptions, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  SExplainStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_EXPLAIN_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->analyze = analyze;
  pStmt->pOptions = (SExplainOptions*)pOptions;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pOptions);
  nodesDestroyNode(pQuery);
  return NULL;
}

SNode* createDescribeStmt(SAstCreateContext* pCxt, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDescribeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DESCRIBE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createResetQueryCacheStmt(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_RESET_QUERY_CACHE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return pStmt;
_err:
  return NULL;
}

static int32_t convertUdfLanguageType(SAstCreateContext* pCxt, const SToken* pLanguageToken, int8_t* pLanguage) {
  if (TK_NK_NIL == pLanguageToken->type || 0 == strncasecmp(pLanguageToken->z + 1, "c", pLanguageToken->n - 2)) {
    *pLanguage = TSDB_FUNC_SCRIPT_BIN_LIB;
  } else if (0 == strncasecmp(pLanguageToken->z + 1, "python", pLanguageToken->n - 2)) {
    *pLanguage = TSDB_FUNC_SCRIPT_PYTHON;
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                            "udf programming language supports c and python");
  }
  return pCxt->errCode;
}

SNode* createCreateFunctionStmt(SAstCreateContext* pCxt, bool ignoreExists, bool aggFunc, const SToken* pFuncName,
                                const SToken* pLibPath, SDataType dataType, int32_t bufSize, const SToken* pLanguage,
                                bool orReplace) {
  CHECK_PARSER_STATUS(pCxt);
  if (pLibPath->n <= 2) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    CHECK_PARSER_STATUS(pCxt);
  }
  int8_t language = 0;
  pCxt->errCode = convertUdfLanguageType(pCxt, pLanguage, &language);
  CHECK_PARSER_STATUS(pCxt);
  SCreateFunctionStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_FUNCTION_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->orReplace = orReplace;
  pStmt->ignoreExists = ignoreExists;
  COPY_STRING_FORM_ID_TOKEN(pStmt->funcName, pFuncName);
  pStmt->isAgg = aggFunc;
  COPY_STRING_FORM_STR_TOKEN(pStmt->libraryPath, pLibPath);
  pStmt->outputDt = dataType;
  pStmt->bufSize = bufSize;
  pStmt->language = language;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createDropFunctionStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pFuncName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropFunctionStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_FUNCTION_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  COPY_STRING_FORM_ID_TOKEN(pStmt->funcName, pFuncName);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createCreateViewStmt(SAstCreateContext* pCxt, bool orReplace, SNode* pView, const SToken* pAs, SNode* pQuery) {
  SCreateViewStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_VIEW_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  int32_t i = pAs->n;
  while (isspace(*(pAs->z + i))) {
    ++i;
  }
  pStmt->pQuerySql = tstrdup(pAs->z + i);
  CHECK_OUT_OF_MEM(pStmt->pQuerySql);
  tstrncpy(pStmt->dbName, ((SViewNode*)pView)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->viewName, ((SViewNode*)pView)->table.tableName, TSDB_VIEW_NAME_LEN);
  nodesDestroyNode(pView);
  pStmt->orReplace = orReplace;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pView);
  nodesDestroyNode(pQuery);
  nodesDestroyNode((SNode*)pStmt);
  return NULL;
}

SNode* createDropViewStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pView) {
  CHECK_PARSER_STATUS(pCxt);
  SDropViewStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_VIEW_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  tstrncpy(pStmt->dbName, ((SViewNode*)pView)->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pStmt->viewName, ((SViewNode*)pView)->table.tableName, TSDB_VIEW_NAME_LEN);
  nodesDestroyNode(pView);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pView);
  return NULL;
}

SNode* createStreamOutTableNode(SAstCreateContext* pCxt, SNode* pIntoTable, SNode* pOutputSubTable, SNodeList* pColList,
                                SNodeList* pTagList) {
  SStreamOutTableNode* pOutTable = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STREAM_OUT_TABLE, (SNode**)&pOutTable);
  CHECK_MAKE_NODE(pOutTable);
  pOutTable->pOutTable = pIntoTable;
  pOutTable->pSubtable = pOutputSubTable;
  pOutTable->pCols = pColList;
  pOutTable->pTags = pTagList;
  return (SNode*)pOutTable;

_err:
  nodesDestroyNode((SNode*)pOutTable);
  nodesDestroyNode(pIntoTable);
  nodesDestroyNode(pOutputSubTable);
  nodesDestroyList(pColList);
  nodesDestroyList(pTagList);
  return NULL;
}

SNode* createStreamTriggerNode(SAstCreateContext* pCxt, SNode* pTriggerWindow, SNode* pTriggerTable,
                               SNodeList* pPartitionList, SNode* pOptions, SNode* pNotification) {
  SStreamTriggerNode* pTrigger = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STREAM_TRIGGER, (SNode**)&pTrigger);
  CHECK_MAKE_NODE(pTrigger);

  pTrigger->pOptions = pOptions;
  pTrigger->pNotify = pNotification;
  pTrigger->pTrigerTable = pTriggerTable;
  pTrigger->pPartitionList = pPartitionList;
  pTrigger->pTriggerWindow = pTriggerWindow;
  return (SNode*)pTrigger;

_err:
  nodesDestroyNode((SNode*)pTrigger);
  nodesDestroyNode(pTriggerWindow);
  nodesDestroyNode(pTriggerTable);
  nodesDestroyNode(pOptions);
  nodesDestroyNode(pNotification);
  nodesDestroyList(pPartitionList);
  return NULL;
}

SNode* createSlidingWindowNode(SAstCreateContext* pCxt, SNode* pSlidingVal, SNode* pOffset) {
  SSlidingWindowNode* pSliding = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SLIDING_WINDOW, (SNode**)&pSliding);
  CHECK_MAKE_NODE(pSliding);
  pSliding->pSlidingVal = pSlidingVal;
  pSliding->pOffset = pOffset;
  return (SNode*)pSliding;
_err:
  nodesDestroyNode(pSlidingVal);
  nodesDestroyNode(pOffset);
  nodesDestroyNode((SNode*)pSliding);
  return NULL;
}

SNode* createStreamTriggerOptions(SAstCreateContext* pCxt) {
  SStreamTriggerOptions* pOptions = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STREAM_TRIGGER_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->pPreFilter = NULL;
  pOptions->pWaterMark = NULL;
  pOptions->pMaxDelay = NULL;
  pOptions->pExpiredTime = NULL;
  pOptions->pFillHisStartTime = NULL;
  pOptions->pEventType = EVENT_NONE;
  pOptions->calcNotifyOnly = false;
  pOptions->deleteOutputTable = false;
  pOptions->deleteRecalc = false;
  pOptions->fillHistory = false;
  pOptions->fillHistoryFirst = false;
  pOptions->lowLatencyCalc = false;
  pOptions->forceOutput = false;
  pOptions->ignoreDisorder = false;
  pOptions->ignoreNoDataTrigger = false;
  return (SNode*)pOptions;
_err:
  nodesDestroyNode((SNode*)pOptions);
  return NULL;
}

SNode* createStreamTagDefNode(SAstCreateContext* pCxt, SToken* pTagName, SDataType dataType, SNode* tagExpression) {
  SStreamTagDefNode* pTagDef = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STREAM_TAG_DEF, (SNode**)&pTagDef);
  CHECK_MAKE_NODE(pTagDef);
  COPY_STRING_FORM_ID_TOKEN(pTagDef->tagName, pTagName);
  int32_t nameLen = strdequote(pTagDef->tagName);
  pTagDef->tagName[nameLen] = '\0';
  pTagDef->dataType = dataType;
  pTagDef->pTagExpr = tagExpression;
  return (SNode*)pTagDef;
_err:
  nodesDestroyNode(tagExpression);
  nodesDestroyNode((SNode*)pTagDef);
  return NULL;
}

SNode* setStreamTriggerOptions(SAstCreateContext* pCxt, SNode* pOptions, SStreamTriggerOption* pOptionUnit) {
  CHECK_PARSER_STATUS(pCxt);
  SStreamTriggerOptions* pStreamOptions = (SStreamTriggerOptions*)pOptions;
  switch (pOptionUnit->type) {
    case STREAM_TRIGGER_OPTION_CALC_NOTIFY_ONLY:
      if (pStreamOptions->calcNotifyOnly) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "CALC_NOTIFY_ONLY specified multiple times");
        goto _err;
      }
      pStreamOptions->calcNotifyOnly = true;
      break;
    case STREAM_TRIGGER_OPTION_DELETE_OUTPUT_TABLE:
      if (pStreamOptions->deleteOutputTable) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "DELETE_OUTPUT_TABLE specified multiple times");
        goto _err;
      }
      pStreamOptions->deleteOutputTable = true;
      break;
    case STREAM_TRIGGER_OPTION_DELETE_RECALC:
      if (pStreamOptions->deleteRecalc) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "DELETE_RECALC specified multiple times");
        goto _err;
      }
      pStreamOptions->deleteRecalc = true;
      break;
    case STREAM_TRIGGER_OPTION_EXPIRED_TIME:
      if (pStreamOptions->pExpiredTime != NULL) {
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "EXPIRED_TIME specified multiple times");
        goto _err;
      }
      pStreamOptions->pExpiredTime = pOptionUnit->pNode;
      break;
    case STREAM_TRIGGER_OPTION_FORCE_OUTPUT:
      if (pStreamOptions->forceOutput) {
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "FORCE_OUTPUT specified multiple times");
        goto _err;
      }
      pStreamOptions->forceOutput = true;
      break;
    case STREAM_TRIGGER_OPTION_FILL_HISTORY:
      if (pStreamOptions->fillHistoryFirst) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "FILL_HISTORY_FIRST and FILL_HISTORY cannot be used at the same time");
        goto _err;
      }
      if (pStreamOptions->pFillHisStartTime != NULL) {
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "FILL_HISTORY specified multiple times");
        goto _err;
      }
      pStreamOptions->fillHistory = true;
      if (pOptionUnit->pNode == NULL) {
        pCxt->errCode = nodesMakeValueNodeFromInt64(INT64_MIN, &pStreamOptions->pFillHisStartTime);
        CHECK_MAKE_NODE(pStreamOptions->pFillHisStartTime);
      } else {
        pStreamOptions->pFillHisStartTime = pOptionUnit->pNode;
      }
      break;
    case STREAM_TRIGGER_OPTION_FILL_HISTORY_FIRST:
      if (pStreamOptions->fillHistory) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "FILL_HISTORY_FIRST and FILL_HISTORY cannot be used at the same time");
        goto _err;
      }
      if (pStreamOptions->pFillHisStartTime != NULL) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "FILL_HISTORY_FIRST specified multiple times");
        goto _err;
      }
      pStreamOptions->fillHistoryFirst = true;
      if (pOptionUnit->pNode == NULL) {
        pCxt->errCode = nodesMakeValueNodeFromInt64(INT64_MIN, &pStreamOptions->pFillHisStartTime);
        CHECK_MAKE_NODE(pStreamOptions->pFillHisStartTime);
      } else {
        pStreamOptions->pFillHisStartTime = pOptionUnit->pNode;
      }
      break;
    case STREAM_TRIGGER_OPTION_IGNORE_DISORDER:
      if (pStreamOptions->ignoreDisorder) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "IGNORE_DISORDER specified multiple times");
        goto _err;
      }
      pStreamOptions->ignoreDisorder = true;
      break;
    case STREAM_TRIGGER_OPTION_LOW_LATENCY_CALC:
      if (pStreamOptions->lowLatencyCalc) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "LOW_LATENCY_CALC specified multiple times");
        goto _err;
      }
      pStreamOptions->lowLatencyCalc = true;
      break;
    case STREAM_TRIGGER_OPTION_MAX_DELAY:
      if (pStreamOptions->pMaxDelay != NULL) {
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "MAX_DELAY specified multiple times");
        goto _err;
      }
      pStreamOptions->pMaxDelay = pOptionUnit->pNode;
      break;
    case STREAM_TRIGGER_OPTION_WATERMARK:
      if (pStreamOptions->pWaterMark != NULL) {
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "WATERMARK specified multiple times");
        goto _err;
      }
      pStreamOptions->pWaterMark = pOptionUnit->pNode;
      break;
    case STREAM_TRIGGER_OPTION_PRE_FILTER:
      if (pStreamOptions->pPreFilter != NULL) {
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "PRE_FILTER specified multiple times");
        goto _err;
      }
      pStreamOptions->pPreFilter = pOptionUnit->pNode;
      break;
    case STREAM_TRIGGER_OPTION_EVENT_TYPE:
      if (pStreamOptions->pEventType != EVENT_NONE) {
        pCxt->errCode =
            generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "EVENT_TYPE specified multiple times");
        goto _err;
      }
      pStreamOptions->pEventType = pOptionUnit->flag;
      break;
    case STREAM_TRIGGER_OPTION_IGNORE_NODATA_TRIGGER:
      if (pStreamOptions->ignoreNoDataTrigger) {
        pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                                "IGNORE_NODATA_TRIGGER specified multiple times");
        goto _err;
      }
      pStreamOptions->ignoreNoDataTrigger = true;
      break;
    default:
      break;
  }
  return pOptions;
_err:
  nodesDestroyNode(pOptionUnit->pNode);
  nodesDestroyNode(pOptions);
  return NULL;
}

static bool validateNotifyUrl(const char* url) {
  const char* prefix[] = {"ws://", "wss://"};
  const char* host = NULL;

  if (!url || *url == '\0') return false;

  for (int32_t i = 0; i < ARRAY_SIZE(prefix); ++i) {
    if (taosStrncasecmp(url, prefix[i], strlen(prefix[i])) == 0) {
      host = url + strlen(prefix[i]);
      break;
    }
  }

  return (host != NULL) && (*host != '\0') && (*host != '/');
}

SNode* createStreamNotifyOptions(SAstCreateContext* pCxt, SNodeList* pAddrUrls, int64_t eventType, SNode* pWhere,
                                 int64_t notifyType) {
  SNode* pNode = NULL;
  CHECK_PARSER_STATUS(pCxt);

  if (LIST_LENGTH(pAddrUrls) == 0) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "notification address cannot be empty");
    goto _err;
  }

  FOREACH(pNode, pAddrUrls) {
    char* url = ((SValueNode*)pNode)->literal;
    if (strlen(url) >= TSDB_STREAM_NOTIFY_URL_LEN) {
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "notification address \"%s\" exceed maximum length %d", url,
                                              TSDB_STREAM_NOTIFY_URL_LEN);
      goto _err;
    }
    if (!validateNotifyUrl(url)) {
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR,
                                              "invalid notification address \"%s\"", url);
      goto _err;
    }
  }

  SStreamNotifyOptions* pNotifyOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STREAM_NOTIFY_OPTIONS, (SNode**)&pNotifyOptions);
  CHECK_MAKE_NODE(pNotifyOptions);
  pNotifyOptions->pAddrUrls = pAddrUrls;
  pNotifyOptions->pWhere = pWhere;
  pNotifyOptions->eventType = eventType;
  pNotifyOptions->notifyType = notifyType;
  return (SNode*)pNotifyOptions;
_err:
  nodesDestroyList(pAddrUrls);
  return NULL;
}

SNode* createCreateStreamStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pStream, SNode* pTrigger,
                              SNode* pOutTable, SNode* pQuery) {
  SCreateStreamStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_STREAM_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  if (pOutTable && ((SStreamOutTableNode*)pOutTable)->pOutTable) {
    tstrncpy(pStmt->targetDbName, ((SRealTableNode*)((SStreamOutTableNode*)pOutTable)->pOutTable)->table.dbName,
             TSDB_DB_NAME_LEN);
    tstrncpy(pStmt->targetTabName, ((SRealTableNode*)((SStreamOutTableNode*)pOutTable)->pOutTable)->table.tableName,
             TSDB_TABLE_NAME_LEN);
  }

  if (pStream) {
    tstrncpy(pStmt->streamDbName, ((SStreamNode*)pStream)->dbName, TSDB_DB_NAME_LEN);
    tstrncpy(pStmt->streamName, ((SStreamNode*)pStream)->streamName, TSDB_STREAM_NAME_LEN);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "stream name cannot be empty");
    goto _err;
  }
  nodesDestroyNode(pStream);

  pStmt->ignoreExists = ignoreExists;
  pStmt->pTrigger = pTrigger;
  pStmt->pQuery = pQuery;
  pStmt->pTags = pOutTable ? ((SStreamOutTableNode*)pOutTable)->pTags : NULL;
  pStmt->pSubtable = pOutTable ? ((SStreamOutTableNode*)pOutTable)->pSubtable : NULL;
  pStmt->pCols = pOutTable ? ((SStreamOutTableNode*)pOutTable)->pCols : NULL;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pOutTable);
  nodesDestroyNode(pQuery);
  nodesDestroyNode(pTrigger);
  nodesDestroyNode(pQuery);
  return NULL;
}

SNode* createDropStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNodeList* pStreamList) {
  CHECK_PARSER_STATUS(pCxt);
  SDropStreamStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_STREAM_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  if (pStreamList) {
    pStmt->pStreamList = pStreamList;
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "stream name cannot be empty");
    goto _err;
  }

  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
_err:
  nodesDestroyList(pStreamList);
  return NULL;
}

SNode* createPauseStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pStream) {
  CHECK_PARSER_STATUS(pCxt);
  SPauseStreamStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_PAUSE_STREAM_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (pStream) {
    tstrncpy(pStmt->streamDbName, ((SStreamNode*)pStream)->dbName, TSDB_DB_NAME_LEN);
    tstrncpy(pStmt->streamName, ((SStreamNode*)pStream)->streamName, TSDB_STREAM_NAME_LEN);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "stream name cannot be empty");
    goto _err;
  }
  nodesDestroyNode(pStream);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createResumeStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, bool ignoreUntreated, SNode* pStream) {
  CHECK_PARSER_STATUS(pCxt);
  SResumeStreamStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_RESUME_STREAM_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (pStream) {
    tstrncpy(pStmt->streamDbName, ((SStreamNode*)pStream)->dbName, TSDB_DB_NAME_LEN);
    tstrncpy(pStmt->streamName, ((SStreamNode*)pStream)->streamName, TSDB_STREAM_NAME_LEN);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "stream name cannot be empty");
    goto _err;
  }
  nodesDestroyNode(pStream);
  pStmt->ignoreNotExists = ignoreNotExists;
  pStmt->ignoreUntreated = ignoreUntreated;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createRecalcStreamStmt(SAstCreateContext* pCxt, SNode* pStream, SNode* pRange) {
  CHECK_PARSER_STATUS(pCxt);
  SRecalcStreamStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_RECALCULATE_STREAM_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (pStream) {
    tstrncpy(pStmt->streamDbName, ((SStreamNode*)pStream)->dbName, TSDB_DB_NAME_LEN);
    tstrncpy(pStmt->streamName, ((SStreamNode*)pStream)->streamName, TSDB_STREAM_NAME_LEN);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "stream name cannot be empty");
    goto _err;
  }
  pStmt->pRange = pRange;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createKillStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pId) {
  CHECK_PARSER_STATUS(pCxt);
  SKillStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->targetId = taosStr2Int32(pId->z, NULL, 10);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createKillQueryStmt(SAstCreateContext* pCxt, const SToken* pQueryId) {
  CHECK_PARSER_STATUS(pCxt);
  SKillQueryStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_KILL_QUERY_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  (void)trimString(pQueryId->z, pQueryId->n, pStmt->queryId, sizeof(pStmt->queryId) - 1);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createBalanceVgroupStmt(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SBalanceVgroupStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_BALANCE_VGROUP_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAssignLeaderStmt(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SAssignLeaderStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ASSIGN_LEADER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createBalanceVgroupLeaderStmt(SAstCreateContext* pCxt, const SToken* pVgId) {
  CHECK_PARSER_STATUS(pCxt);
  SBalanceVgroupLeaderStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_BALANCE_VGROUP_LEADER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (NULL != pVgId && NULL != pVgId->z) {
    pStmt->vgId = taosStr2Int32(pVgId->z, NULL, 10);
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createBalanceVgroupLeaderDBNameStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  SBalanceVgroupLeaderStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (NULL != pDbName) {
    COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createSetVgroupKeepVersionStmt(SAstCreateContext* pCxt, const SToken* pVgId, const SToken* pKeepVersion) {
  CHECK_PARSER_STATUS(pCxt);
  SSetVgroupKeepVersionStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SET_VGROUP_KEEP_VERSION_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  if (NULL != pVgId && NULL != pVgId->z) {
    pStmt->vgId = taosStr2Int32(pVgId->z, NULL, 10);
  }
  if (NULL != pKeepVersion && NULL != pKeepVersion->z) {
    pStmt->keepVersion = taosStr2Int64(pKeepVersion->z, NULL, 10);
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createMergeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId1, const SToken* pVgId2) {
  CHECK_PARSER_STATUS(pCxt);
  SMergeVgroupStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_MERGE_VGROUP_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->vgId1 = taosStr2Int32(pVgId1->z, NULL, 10);
  pStmt->vgId2 = taosStr2Int32(pVgId2->z, NULL, 10);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createRedistributeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId, SNodeList* pDnodes) {
  CHECK_PARSER_STATUS(pCxt);
  SRedistributeVgroupStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_REDISTRIBUTE_VGROUP_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->vgId = taosStr2Int32(pVgId->z, NULL, 10);
  pStmt->pDnodes = pDnodes;
  return (SNode*)pStmt;
_err:
  nodesDestroyList(pDnodes);
  return NULL;
}

SNode* createSplitVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId, bool force) {
  CHECK_PARSER_STATUS(pCxt);
  SSplitVgroupStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SPLIT_VGROUP_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->vgId = taosStr2Int32(pVgId->z, NULL, 10);
  pStmt->force = force;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createSyncdbStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SYNCDB_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return pStmt;
_err:
  return NULL;
}

SNode* createGrantStmt(SAstCreateContext* pCxt, void* resouces, SPrivLevelArgs* pPrivLevel, SToken* pPrincipal,
                       SNode* pCond, int8_t optrType) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkRoleName(pCxt, pPrincipal, false));
  SGrantStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_GRANT_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->optrType = optrType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->principal, pPrincipal);
  switch (optrType) {
    case TSDB_ALTER_ROLE_LOCK:
      break;
    case TSDB_ALTER_ROLE_PRIVILEGES: {
      CHECK_NAME(checkObjName(pCxt, &pPrivLevel->first, false));
      CHECK_NAME(checkTableName(pCxt, &pPrivLevel->second));
      pStmt->privileges = *(SPrivSetArgs*)resouces;
      if (TK_NK_NIL != pPrivLevel->first.type) {
        COPY_STRING_FORM_ID_TOKEN(pStmt->objName, &pPrivLevel->first);
      }
      if (TK_NK_NIL != pPrivLevel->second.type) {
        COPY_STRING_FORM_ID_TOKEN(pStmt->tabName, &pPrivLevel->second);
      }
      pStmt->privileges.objType = pPrivLevel->objType;
      pStmt->pCond = pCond;
      break;
    }
    case TSDB_ALTER_ROLE_ROLE: {
      SToken* pRole = (SToken*)resouces;
      CHECK_NAME(checkRoleName(pCxt, pRole, false));
      COPY_STRING_FORM_ID_TOKEN(pStmt->roleName, pRole);
      break;
    }
    default:
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "unsupported grant type");
      goto _err;
  }
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pCond);
  return NULL;
}

SNode* createRevokeStmt(SAstCreateContext* pCxt, void* resouces, SPrivLevelArgs* pPrivLevel, SToken* pPrincipal,
                        SNode* pCond, int8_t optrType) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pPrincipal));
  SRevokeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_REVOKE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->optrType = optrType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->principal, pPrincipal);
  if (optrType == TSDB_ALTER_ROLE_PRIVILEGES) {
    CHECK_NAME(checkDbName(pCxt, &pPrivLevel->first, false));
    CHECK_NAME(checkTableName(pCxt, &pPrivLevel->second));
    pStmt->privileges = *(SPrivSetArgs*)resouces;
    COPY_STRING_FORM_ID_TOKEN(pStmt->objName, &pPrivLevel->first);
    if (TK_NK_NIL != pPrivLevel->second.type) {
      COPY_STRING_FORM_ID_TOKEN(pStmt->tabName, &pPrivLevel->second);
    }
    pStmt->privileges.objType = pPrivLevel->objType;
    pStmt->pCond = pCond;
  } else if (optrType == TSDB_ALTER_ROLE_ROLE) {
    SToken* pRole = (SToken*)resouces;
    CHECK_NAME(checkRoleName(pCxt, pRole, false));
    COPY_STRING_FORM_ID_TOKEN(pStmt->roleName, pRole);
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "unsupported revoke type");
    goto _err;
  }

  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pCond);
  return NULL;
}

SNode* createFuncForDelete(SAstCreateContext* pCxt, const char* pFuncName) {
  SFunctionNode* pFunc = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  CHECK_MAKE_NODE(pFunc);
  snprintf(pFunc->functionName, sizeof(pFunc->functionName), "%s", pFuncName);
  SNode* pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(pCol);
  pCxt->errCode = nodesListMakeStrictAppend(&pFunc->pParameterList, pCol);
  CHECK_PARSER_STATUS(pCxt);
  return (SNode*)pFunc;
_err:
  nodesDestroyNode((SNode*)pFunc);
  return NULL;
}

SNode* createDeleteStmt(SAstCreateContext* pCxt, SNode* pTable, SNode* pWhere) {
  SDeleteStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DELETE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pFromTable = pTable;
  pStmt->pWhere = pWhere;
  pStmt->pCountFunc = createFuncForDelete(pCxt, "count");
  pStmt->pFirstFunc = createFuncForDelete(pCxt, "first");
  pStmt->pLastFunc = createFuncForDelete(pCxt, "last");
  CHECK_MAKE_NODE(pStmt->pCountFunc);
  CHECK_MAKE_NODE(pStmt->pFirstFunc);
  CHECK_MAKE_NODE(pStmt->pLastFunc);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyNode(pTable);
  nodesDestroyNode(pWhere);
  return NULL;
}

SNode* createInsertStmt(SAstCreateContext* pCxt, SNode* pTable, SNodeList* pCols, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  SInsertStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_INSERT_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pTable = pTable;
  pStmt->pCols = pCols;
  pStmt->pQuery = pQuery;
  if (QUERY_NODE_SELECT_STMT == nodeType(pQuery)) {
    tstrncpy(((SSelectStmt*)pQuery)->stmtName, ((STableNode*)pTable)->tableAlias, TSDB_TABLE_NAME_LEN);
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pQuery)) {
    tstrncpy(((SSetOperator*)pQuery)->stmtName, ((STableNode*)pTable)->tableAlias, TSDB_TABLE_NAME_LEN);
  }
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pTable);
  nodesDestroyNode(pQuery);
  nodesDestroyList(pCols);
  return NULL;
}

SNode* createCreateRsmaStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* rsmaName, SNode* pRealTable,
                            SNodeList* pFuncs, SNodeList* pIntervals) {
  SCreateRsmaStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkRsmaName(pCxt, rsmaName));
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_RSMA_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->ignoreExists = ignoreExists;
  pStmt->pFuncs = pFuncs;
  pStmt->pIntervals = pIntervals;
  COPY_STRING_FORM_ID_TOKEN(pStmt->rsmaName, rsmaName);

  SRealTableNode* pTable = (SRealTableNode*)pRealTable;
  memcpy(pStmt->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
  memcpy(pStmt->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);

  return (SNode*)pStmt;
_err:
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyList(pFuncs);
  nodesDestroyNode(pRealTable);
  nodesDestroyList(pIntervals);
  return NULL;
}

SNode* createDropRsmaStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropRsmaStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_RSMA_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->ignoreNotExists = ignoreNotExists;
  SRealTableNode* pTableNode = (SRealTableNode*)pRealTable;

  memcpy(pStmt->rsmaName, pTableNode->table.tableName, TSDB_TABLE_NAME_LEN);
  memcpy(pStmt->dbName, pTableNode->table.dbName, TSDB_DB_NAME_LEN);

  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createAlterRsmaStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRsma, int8_t alterType,
                           void* alterInfo) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterRsmaStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_RSMA_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  SRealTableNode* pTableNode = (SRealTableNode*)pRsma;

  memcpy(pStmt->rsmaName, pTableNode->table.tableName, TSDB_TABLE_NAME_LEN);
  memcpy(pStmt->dbName, pTableNode->table.dbName, TSDB_DB_NAME_LEN);
  nodesDestroyNode(pRsma);

  pStmt->alterType = alterType;
  switch (alterType) {
    case TSDB_ALTER_RSMA_FUNCTION: {
      pStmt->pFuncs = (SNodeList*)alterInfo;
      break;
    }
    default:
      break;
  }
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowCreateRsmaStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCreateRsmaStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  tstrncpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName, sizeof(pStmt->dbName));
  tstrncpy(pStmt->rsmaName, ((SRealTableNode*)pRealTable)->table.tableName, sizeof(pStmt->rsmaName));
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createRollupStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pStart, SNode* pEnd) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SRollupDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ROLLUP_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  return NULL;
}

SNode* createRollupVgroupsStmt(SAstCreateContext* pCxt, SNode* pDbName, SNodeList* vgidList, SNode* pStart,
                                SNode* pEnd) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pDbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    CHECK_PARSER_STATUS(pCxt);
  }
  SRollupVgroupsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ROLLUP_VGROUPS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->vgidList = vgidList;
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pDbName);
  nodesDestroyList(vgidList);
  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  return NULL;
}

SNode* createCreateTSMAStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* tsmaName, SNode* pOptions,
                            SNode* pRealTable, SNode* pInterval) {
  SCreateTSMAStmt* pStmt = NULL;

  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTsmaName(pCxt, tsmaName));
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TSMA_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->ignoreExists = ignoreExists;
  if (!pOptions) {
    // recursive tsma
    pStmt->pOptions = NULL;
    pCxt->errCode = nodesMakeNode(QUERY_NODE_TSMA_OPTIONS, (SNode**)&pStmt->pOptions);
    CHECK_MAKE_NODE(pStmt->pOptions);
    pStmt->pOptions->recursiveTsma = true;
  } else {
    pStmt->pOptions = (STSMAOptions*)pOptions;
  }
  pStmt->pOptions->pInterval = pInterval;
  COPY_STRING_FORM_ID_TOKEN(pStmt->tsmaName, tsmaName);

  SRealTableNode* pTable = (SRealTableNode*)pRealTable;
  memcpy(pStmt->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
  memcpy(pStmt->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
  memcpy(pStmt->originalTbName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
  nodesDestroyNode(pRealTable);

  return (SNode*)pStmt;
_err:
  nodesDestroyNode((SNode*)pStmt);
  nodesDestroyNode(pOptions);
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pInterval);
  return NULL;
}

SNode* createTSMAOptions(SAstCreateContext* pCxt, SNodeList* pFuncs) {
  CHECK_PARSER_STATUS(pCxt);
  STSMAOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_TSMA_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  pOptions->pFuncs = pFuncs;
  return (SNode*)pOptions;
_err:
  nodesDestroyList(pFuncs);
  return NULL;
}

SNode* createDefaultTSMAOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STSMAOptions* pOptions = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_TSMA_OPTIONS, (SNode**)&pOptions);
  CHECK_MAKE_NODE(pOptions);
  return (SNode*)pOptions;
_err:
  return NULL;
}

SNode* createDropTSMAStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTSMAStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_TSMA_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->ignoreNotExists = ignoreNotExists;
  SRealTableNode* pTableNode = (SRealTableNode*)pRealTable;

  memcpy(pStmt->tsmaName, pTableNode->table.tableName, TSDB_TABLE_NAME_LEN);
  memcpy(pStmt->dbName, pTableNode->table.dbName, TSDB_DB_NAME_LEN);

  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pRealTable);
  return NULL;
}

SNode* createShowTSMASStmt(SAstCreateContext* pCxt, SNode* dbName) {
  CHECK_PARSER_STATUS(pCxt);

  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_TSMAS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->pDbName = dbName;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(dbName);
  return NULL;
}
SNode* createShowDiskUsageStmt(SAstCreateContext* pCxt, SNode* dbName, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == dbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    CHECK_PARSER_STATUS(pCxt);
  }

  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);

  pStmt->pDbName = dbName;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(dbName);
  return NULL;
}

SNode* createShowStreamsStmt(SAstCreateContext* pCxt, SNode* pDbName, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);

  if (needDbShowStmt(type) && NULL == pDbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    CHECK_PARSER_STATUS(pCxt);
  }

  SShowStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->withFull = false;
  pStmt->pDbName = pDbName;

  return (SNode*)pStmt;

_err:
  nodesDestroyNode(pDbName);
  return NULL;
}

SNode* createScanStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pStart, SNode* pEnd) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SScanDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SCAN_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  return NULL;
}

SNode* createScanVgroupsStmt(SAstCreateContext* pCxt, SNode* pDbName, SNodeList* vgidList, SNode* pStart, SNode* pEnd) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pDbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    CHECK_PARSER_STATUS(pCxt);
  }
  SScanVgroupsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SCAN_VGROUPS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->vgidList = vgidList;
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pDbName);
  nodesDestroyList(vgidList);
  nodesDestroyNode(pStart);
  nodesDestroyNode(pEnd);
  return NULL;
}

SNode* createShowScansStmt(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowScansStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(type, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createShowScanDetailsStmt(SAstCreateContext* pCxt, SNode* pScanIdNode) {
  CHECK_PARSER_STATUS(pCxt);
  SShowScanDetailsStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_SHOW_SCAN_DETAILS_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->pScanId = pScanIdNode;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pScanIdNode);
  return NULL;
}

SNode* createAlterAllDnodeTLSStmt(SAstCreateContext* pCxt, SToken* alterName) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterDnodeStmt* pStmt = NULL;
  static char*     tls = "TLS";
  if (NULL == alterName || alterName->n <= 0) {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "alter name is empty");
    goto _err;
  }

  if (alterName->n == strlen(tls) && taosStrncasecmp(alterName->z, tls, alterName->n) == 0) {
    pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_DNODES_RELOAD_TLS_STMT, (SNode**)&pStmt);

    memcpy(pStmt->config, "reload", strlen("reload"));
    memcpy(pStmt->value, "tls", strlen("tls"));
    pStmt->dnodeId = -1;
  } else {
    pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "alter is not supported");
    goto _err;
  }

  CHECK_MAKE_NODE(pStmt);

  return (SNode*)pStmt;
_err:
  return NULL;
}
