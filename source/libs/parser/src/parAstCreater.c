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
#include <regex.h>
#ifndef TD_ASTRA
#include <uv.h>
#endif

#include "nodes.h"
#include "parAst.h"
#include "parUtil.h"
#include "tglobal.h"
#include "ttime.h"

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
#define COPY_STRING_FORM_STR_TOKEN(buf, pToken)                              \
  do {                                                                       \
    if ((pToken)->n > 2) {                                                   \
      strncpy(buf, (pToken)->z + 1, TMIN((pToken)->n - 2, sizeof(buf) - 1)); \
    }                                                                        \
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

static void trimEscape(SAstCreateContext* pCxt, SToken* pName) {
  // todo need to deal with `ioo``ii` -> ioo`ii: done
  if (NULL != pName && pName->n > 1 && TS_ESCAPE_CHAR == pName->z[0]) {
    if (!pCxt->pQueryCxt->hasDupQuoteChar) {
      pName->z += 1;
      pName->n -= 2;
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
    trimEscape(pCxt, pUserName);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool invalidPassword(const char* pPassword) {
  regex_t regex;

  if (regcomp(&regex, "[ '\"`\\]", REG_EXTENDED | REG_ICASE) != 0) {
    return false;
  }

  /* Execute regular expression */
  int32_t res = regexec(&regex, pPassword, 0, NULL, 0);
  regfree(&regex);
  return 0 == res;
}

static bool invalidStrongPassword(const char* pPassword) {
  if (strcmp(pPassword, "taosdata") == 0) {
    return false;
  }

  bool charTypes[4] = {0};
  for (int32_t i = 0; i < strlen(pPassword); ++i) {
    if (taosIsBigChar(pPassword[i])) {
      charTypes[0] = true;
    } else if (taosIsSmallChar(pPassword[i])) {
      charTypes[1] = true;
    } else if (taosIsNumberChar(pPassword[i])) {
      charTypes[2] = true;
    } else if (taosIsSpecialChar(pPassword[i])) {
      charTypes[3] = true;
    } else {
      return true;
    }
  }

  int32_t numOfTypes = 0;
  for (int32_t i = 0; i < 4; ++i) {
    numOfTypes += charTypes[i];
  }

  if (numOfTypes < 3) {
    return true;
  }

  return false;
}

static bool checkPassword(SAstCreateContext* pCxt, const SToken* pPasswordToken, char* pPassword) {
  if (NULL == pPasswordToken) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else if (pPasswordToken->n >= (TSDB_USET_PASSWORD_LONGLEN + 2)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
  } else {
    strncpy(pPassword, pPasswordToken->z, pPasswordToken->n);
    (void)strdequote(pPassword);
    if (strtrim(pPassword) < TSDB_PASSWORD_MIN_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY);
    } else {
      if (tsEnableStrongPassword) {
        if (invalidStrongPassword(pPassword)) {
          pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PASSWD);
        }
      } else {
        if (invalidPassword(pPassword)) {
          pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PASSWD);
        }
      }
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkImportPassword(SAstCreateContext* pCxt, const SToken* pPasswordToken, char* pPassword) {
  if (NULL == pPasswordToken) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else if (pPasswordToken->n > (32 + 2)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
  } else {
    strncpy(pPassword, pPasswordToken->z, pPasswordToken->n);
    (void)strdequote(pPassword);
    if (strtrim(pPassword) < TSDB_PASSWORD_LEN - 1) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
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

static bool checkDbName(SAstCreateContext* pCxt, SToken* pDbName, bool demandDb) {
  if (NULL == pDbName) {
    if (demandDb && NULL == pCxt->pQueryCxt->db) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DB_NOT_SPECIFIED);
    }
  } else {
    trimEscape(pCxt, pDbName);
    if (pDbName->n >= TSDB_DB_NAME_LEN || pDbName->n == 0) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pDbName->z);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkTableName(SAstCreateContext* pCxt, SToken* pTableName) {
  trimEscape(pCxt, pTableName);
  if (NULL != pTableName && pTableName->type != TK_NK_NIL &&
      (pTableName->n >= TSDB_TABLE_NAME_LEN || pTableName->n == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTableName->z);
    return false;
  }
  return true;
}

static bool checkColumnName(SAstCreateContext* pCxt, SToken* pColumnName) {
  trimEscape(pCxt, pColumnName);
  if (NULL != pColumnName && pColumnName->type != TK_NK_NIL &&
      (pColumnName->n >= TSDB_COL_NAME_LEN || pColumnName->n == 0)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pColumnName->z);
    return false;
  }
  return true;
}

static bool checkIndexName(SAstCreateContext* pCxt, SToken* pIndexName) {
  trimEscape(pCxt, pIndexName);
  if (NULL != pIndexName && pIndexName->n >= TSDB_INDEX_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pIndexName->z);
    return false;
  }
  return true;
}

static bool checkTopicName(SAstCreateContext* pCxt, SToken* pTopicName) {
  trimEscape(pCxt, pTopicName);
  if (pTopicName->n >= TSDB_TOPIC_NAME_LEN || pTopicName->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTopicName->z);
    return false;
  }
  return true;
}

static bool checkCGroupName(SAstCreateContext* pCxt, SToken* pCGroup) {
  trimEscape(pCxt, pCGroup);
  if (pCGroup->n >= TSDB_CGROUP_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pCGroup->z);
    return false;
  }
  return true;
}

static bool checkViewName(SAstCreateContext* pCxt, SToken* pViewName) {
  trimEscape(pCxt, pViewName);
  if (pViewName->n >= TSDB_VIEW_NAME_LEN || pViewName->n == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pViewName->z);
    return false;
  }
  return true;
}

static bool checkStreamName(SAstCreateContext* pCxt, SToken* pStreamName) {
  trimEscape(pCxt, pStreamName);
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
  trimEscape(pCxt, pRsmaToken);
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
  trimEscape(pCxt, pTsmaToken);
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
  trimEscape(pCxt, pMountPath);
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
  if (TK_NK_ID != pLiteral->type && TK_TIMEZONE != pLiteral->type &&
      (IS_VAR_DATA_TYPE(dataType) || TSDB_DATA_TYPE_TIMESTAMP == dataType)) {
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
  trimEscape(pCxt, pLiteral);
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
    taosRandStr(tempTable->table.tableAlias, 8);
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

SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pExpr, SNode* pExtend, SNode* pTrueForLimit) {
  SStateWindowNode* state = NULL;
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesMakeNode(QUERY_NODE_STATE_WINDOW, (SNode**)&state);
  CHECK_MAKE_NODE(state);
  state->pCol = createPrimaryKeyCol(pCxt, NULL);
  CHECK_MAKE_NODE(state->pCol);
  state->pExpr = pExpr;
  state->pTrueForLimit = pTrueForLimit;
  state->pExtend = pExtend;
  return (SNode*)state;
_err:
  nodesDestroyNode((SNode*)state);
  nodesDestroyNode(pExpr);
  nodesDestroyNode(pTrueForLimit);
  nodesDestroyNode(pExtend);
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
  return NULL;
}

SNode* createInterpTimePoint(SAstCreateContext* pCxt, SNode* pPoint) {
  CHECK_PARSER_STATUS(pCxt);
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
  trimEscape(pCxt, pAlias);
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
  pOptions->encryptAlgorithm = -1;
  pOptions->dnodeListStr[0] = 0;
  pOptions->compactInterval = -1;
  pOptions->compactStartTime = -1;
  pOptions->compactEndTime = -1;
  pOptions->compactTimeOffset = -1;
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
      pDbOptions->encryptAlgorithm = TSDB_DEFAULT_ENCRYPT_ALGO;
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

SNode* createS3MigrateDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
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

SNode* createCompactStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pStart, SNode* pEnd, bool metaOnly) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, pDbName, false));
  SCompactDatabaseStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_COMPACT_DATABASE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  pStmt->metaOnly = metaOnly;
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
                                SNode* pEnd, bool metaOnly) {
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

static int32_t getIpRangeFromStr(char* ipRange, SIpRange* pIpRange) {
  int32_t code = 0;

  int8_t isIp6 = ((strchr(ipRange, ':')) != NULL ? 1 : 0);
  if (isIp6) {
    struct in6_addr ip6;
    if (inet_pton(AF_INET6, ipRange, &ip6) == 1) {
      pIpRange->type = 1;
      memcpy(&pIpRange->ipV6.addr[0], ip6.s6_addr, 8);
      memcpy(&pIpRange->ipV6.addr[1], ip6.s6_addr + 8, 8);

    } else {
      return TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
  } else {
    struct in_addr ip4;
    if (inet_pton(AF_INET, ipRange, &ip4) == 1) {
      pIpRange->type = 0;
      memcpy(&pIpRange->ipV4.ip, &ip4.s_addr, sizeof(ip4.s_addr));
    } else {
      return TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
  }

  return code;
}
static int32_t getIpRangeFromWhitelistItem(char* ipRange, SIpRange* pIpRange) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   ipCopy = NULL;
  int32_t mask = 0;
#ifndef TD_ASTRA

  ipCopy = taosStrdup(ipRange);
  if (ipCopy == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  char* slash = strchr(ipCopy, '/');
  if (slash) {
    *slash = '\0';
    code = taosStr2int32(slash + 1, &mask);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  code = getIpRangeFromStr(ipCopy, pIpRange);
  TAOS_CHECK_GOTO(code, &lino, _error);

  if (!slash) {
    mask = pIpRange->type == 0 ? 32 : 128;
  }
  code = tIpRangeSetMask(pIpRange, mask);
  TAOS_CHECK_GOTO(code, &lino, _error);

#endif
_error:
  taosMemoryFreeClear(ipCopy);
  return code;
}

static int32_t fillIpRangesFromWhiteList(SAstCreateContext* pCxt, SNodeList* pIpRangesNodeList, SIpRange* pIpRanges) {
  int32_t i = 0;
  int32_t code = 0;

  SNode* pNode = NULL;
  FOREACH(pNode, pIpRangesNodeList) {
    if (QUERY_NODE_VALUE != nodeType(pNode)) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IP_RANGE);
      return TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
    SValueNode* pValNode = (SValueNode*)(pNode);
    code = getIpRangeFromWhitelistItem(pValNode->literal, pIpRanges + i);
    ++i;
    if (code != TSDB_CODE_SUCCESS) {
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "Invalid IP range %s", pValNode->literal);
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

SNode* addCreateUserStmtWhiteList(SAstCreateContext* pCxt, SNode* pCreateUserStmt, SNodeList* pIpRangesNodeList) {
  if (NULL == pCreateUserStmt) {
    if (pIpRangesNodeList != NULL) {
      nodesDestroyList(pIpRangesNodeList);
    }
    return NULL;
  }

  if (NULL == pIpRangesNodeList) {
    return pCreateUserStmt;
  }

  ((SCreateUserStmt*)pCreateUserStmt)->pNodeListIpRanges = pIpRangesNodeList;
  SCreateUserStmt* pCreateUser = (SCreateUserStmt*)pCreateUserStmt;
  pCreateUser->numIpRanges = LIST_LENGTH(pIpRangesNodeList);
  pCreateUser->pIpRanges = taosMemoryMalloc(pCreateUser->numIpRanges * sizeof(SIpRange));
  CHECK_OUT_OF_MEM(pCreateUser->pIpRanges);

  pCxt->errCode = fillIpRangesFromWhiteList(pCxt, pIpRangesNodeList, pCreateUser->pIpRanges);
  CHECK_PARSER_STATUS(pCxt);

  return pCreateUserStmt;
_err:
  nodesDestroyNode(pCreateUserStmt);
  nodesDestroyList(pIpRangesNodeList);
  return NULL;
}

SNode* createCreateUserStmt(SAstCreateContext* pCxt, SToken* pUserName, const SToken* pPassword, int8_t sysinfo,
                            int8_t createDb, int8_t is_import) {
  CHECK_PARSER_STATUS(pCxt);
  char password[TSDB_USET_PASSWORD_LONGLEN + 3] = {0};
  CHECK_NAME(checkUserName(pCxt, pUserName));
  if (is_import == 0) {
    CHECK_NAME(checkPassword(pCxt, pPassword, password));
  } else {
    CHECK_NAME(checkImportPassword(pCxt, pPassword, password));
  }
  SCreateUserStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_USER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  tstrncpy(pStmt->password, password, TSDB_USET_PASSWORD_LONGLEN);
  pStmt->sysinfo = sysinfo;
  pStmt->createDb = createDb;
  pStmt->isImport = is_import;
  return (SNode*)pStmt;
_err:
  return NULL;
}

SNode* createAlterUserStmt(SAstCreateContext* pCxt, SToken* pUserName, int8_t alterType, void* pAlterInfo) {
  SAlterUserStmt* pStmt = NULL;
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pUserName));
  pCxt->errCode = nodesMakeNode(QUERY_NODE_ALTER_USER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->alterType = alterType;
  switch (alterType) {
    case TSDB_ALTER_USER_PASSWD: {
      char    password[TSDB_USET_PASSWORD_LONGLEN] = {0};
      SToken* pVal = pAlterInfo;
      CHECK_NAME(checkPassword(pCxt, pVal, password));
      tstrncpy(pStmt->password, password, TSDB_USET_PASSWORD_LONGLEN);
      break;
    }
    case TSDB_ALTER_USER_ENABLE: {
      SToken* pVal = pAlterInfo;
      pStmt->enable = taosStr2Int8(pVal->z, NULL, 10);
      break;
    }
    case TSDB_ALTER_USER_SYSINFO: {
      SToken* pVal = pAlterInfo;
      pStmt->sysinfo = taosStr2Int8(pVal->z, NULL, 10);
      break;
    }
    case TSDB_ALTER_USER_CREATEDB: {
      SToken* pVal = pAlterInfo;
      pStmt->createdb = taosStr2Int8(pVal->z, NULL, 10);
      break;
    }
    case TSDB_ALTER_USER_ADD_WHITE_LIST:
    case TSDB_ALTER_USER_DROP_WHITE_LIST: {
      SNodeList* pIpRangesNodeList = pAlterInfo;
      pStmt->pNodeListIpRanges = pIpRangesNodeList;
      pStmt->numIpRanges = LIST_LENGTH(pIpRangesNodeList);
      pStmt->pIpRanges = taosMemoryMalloc(pStmt->numIpRanges * sizeof(SIpRange));
      CHECK_OUT_OF_MEM(pStmt->pIpRanges);

      pCxt->errCode = fillIpRangesFromWhiteList(pCxt, pIpRangesNodeList, pStmt->pIpRanges);
      CHECK_PARSER_STATUS(pCxt);
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

SNode* createDropUserStmt(SAstCreateContext* pCxt, SToken* pUserName) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkUserName(pCxt, pUserName));
  SDropUserStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_USER_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
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

SNode* createEncryptKeyStmt(SAstCreateContext* pCxt, const SToken* pValue) {
  SToken config;
  config.type = TK_NK_STRING;
  config.z = "\"encrypt_key\"";
  config.n = strlen(config.z);
  return createAlterDnodeStmt(pCxt, NULL, &config, pValue);
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

SNode* createCreateTopicStmtUseQuery(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkTopicName(pCxt, pTopicName));
  SCreateTopicStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pQuery = pQuery;
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

SNode* createDropStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pStream) {
  CHECK_PARSER_STATUS(pCxt);
  SDropStreamStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_DROP_STREAM_STMT, (SNode**)&pStmt);
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

SNode* createGrantStmt(SAstCreateContext* pCxt, int64_t privileges, STokenPair* pPrivLevel, SToken* pUserName,
                       SNode* pTagCond) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, &pPrivLevel->first, false));
  CHECK_NAME(checkUserName(pCxt, pUserName));
  CHECK_NAME(checkTableName(pCxt, &pPrivLevel->second));
  SGrantStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_GRANT_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->privileges = privileges;
  COPY_STRING_FORM_ID_TOKEN(pStmt->objName, &pPrivLevel->first);
  if (TK_NK_NIL != pPrivLevel->second.type && TK_NK_STAR != pPrivLevel->second.type) {
    COPY_STRING_FORM_ID_TOKEN(pStmt->tabName, &pPrivLevel->second);
  }
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->pTagCond = pTagCond;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pTagCond);
  return NULL;
}

SNode* createRevokeStmt(SAstCreateContext* pCxt, int64_t privileges, STokenPair* pPrivLevel, SToken* pUserName,
                        SNode* pTagCond) {
  CHECK_PARSER_STATUS(pCxt);
  CHECK_NAME(checkDbName(pCxt, &pPrivLevel->first, false));
  CHECK_NAME(checkUserName(pCxt, pUserName));
  CHECK_NAME(checkTableName(pCxt, &pPrivLevel->second));
  SRevokeStmt* pStmt = NULL;
  pCxt->errCode = nodesMakeNode(QUERY_NODE_REVOKE_STMT, (SNode**)&pStmt);
  CHECK_MAKE_NODE(pStmt);
  pStmt->privileges = privileges;
  COPY_STRING_FORM_ID_TOKEN(pStmt->objName, &pPrivLevel->first);
  if (TK_NK_NIL != pPrivLevel->second.type && TK_NK_STAR != pPrivLevel->second.type) {
    COPY_STRING_FORM_ID_TOKEN(pStmt->tabName, &pPrivLevel->second);
  }
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->pTagCond = pTagCond;
  return (SNode*)pStmt;
_err:
  nodesDestroyNode(pTagCond);
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