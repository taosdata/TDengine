
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
#include <uv.h>

#include "parAst.h"
#include "parUtil.h"
#include "tglobal.h"
#include "ttime.h"

#define CHECK_OUT_OF_MEM(p)                                                      \
  do {                                                                           \
    if (NULL == (p)) {                                                           \
      pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;                                   \
      snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "Out of memory"); \
      return NULL;                                                               \
    }                                                                            \
  } while (0)

#define CHECK_PARSER_STATUS(pCxt)             \
  do {                                        \
    if (TSDB_CODE_SUCCESS != pCxt->errCode) { \
      return NULL;                            \
    }                                         \
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

static void trimEscape(SToken* pName) {
  // todo need to deal with `ioo``ii` -> ioo`ii
  if (NULL != pName && pName->n > 1 && '`' == pName->z[0]) {
    pName->z += 1;
    pName->n -= 2;
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
    trimEscape(pUserName);
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

static bool checkPassword(SAstCreateContext* pCxt, const SToken* pPasswordToken, char* pPassword) {
  if (NULL == pPasswordToken) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else if (pPasswordToken->n >= (TSDB_USET_PASSWORD_LEN + 2)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
  } else {
    strncpy(pPassword, pPasswordToken->z, pPasswordToken->n);
    strdequote(pPassword);
    if (strtrim(pPassword) <= 0) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_PASSWD_EMPTY);
    } else if (invalidPassword(pPassword)) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PASSWD);
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
  strdequote(ep);
  strtrim(ep);
  if (NULL == pPort) {
    strcpy(pFqdn, ep);
    return TSDB_CODE_SUCCESS;
  }
  char* pColon = strchr(ep, ':');
  if (NULL == pColon) {
    *pPort = tsServerPort;
    strcpy(pFqdn, ep);
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
    trimEscape(pDbName);
    if (pDbName->n >= TSDB_DB_NAME_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pDbName->z);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkTableName(SAstCreateContext* pCxt, SToken* pTableName) {
  trimEscape(pTableName);
  if (NULL != pTableName && pTableName->n >= TSDB_TABLE_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTableName->z);
    return false;
  }
  return true;
}

static bool checkColumnName(SAstCreateContext* pCxt, SToken* pColumnName) {
  trimEscape(pColumnName);
  if (NULL != pColumnName && pColumnName->n >= TSDB_COL_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pColumnName->z);
    return false;
  }
  return true;
}

static bool checkIndexName(SAstCreateContext* pCxt, SToken* pIndexName) {
  trimEscape(pIndexName);
  if (NULL != pIndexName && pIndexName->n >= TSDB_INDEX_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pIndexName->z);
    return false;
  }
  return true;
}

static bool checkTopicName(SAstCreateContext* pCxt, SToken* pTopicName) {
  trimEscape(pTopicName);
  if (pTopicName->n >= TSDB_TOPIC_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTopicName->z);
    return false;
  }
  return true;
}

static bool checkCGroupName(SAstCreateContext* pCxt, SToken* pCGroup) {
  trimEscape(pCGroup);
  if (pCGroup->n >= TSDB_CGROUP_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pCGroup->z);
    return false;
  }
  return true;
}

static bool checkViewName(SAstCreateContext* pCxt, SToken* pViewName) {
  trimEscape(pViewName);
  if (pViewName->n >= TSDB_VIEW_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pViewName->z);
    return false;
  }
  return true;
}


static bool checkStreamName(SAstCreateContext* pCxt, SToken* pStreamName) {
  trimEscape(pStreamName);
  if (pStreamName->n >= TSDB_STREAM_NAME_LEN) {
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

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pToken->z;
  target->n = pToken->n;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pStart->z;
  target->n = (pEnd->z + pEnd->n) - pStart->z;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* setRawExprNodeIsPseudoColumn(SAstCreateContext* pCxt, SNode* pNode, bool isPseudoColumn) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pNode || QUERY_NODE_RAW_EXPR != nodeType(pNode)) {
    return pNode;
  }
  ((SRawExprNode*)pNode)->isPseudoColumn = isPseudoColumn;
  return pNode;
}

SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* pRawExpr = (SRawExprNode*)pNode;
  SNode*        pRealizedExpr = pRawExpr->pNode;
  if (nodesIsExprNode(pRealizedExpr)) {
    SExprNode* pExpr = (SExprNode*)pRealizedExpr;
    if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
      strcpy(pExpr->aliasName, ((SColumnNode*)pExpr)->colName);
      strcpy(pExpr->userAlias, ((SColumnNode*)pExpr)->colName);
    } else if (pRawExpr->isPseudoColumn) {
      // all pseudo column are translate to function with same name
      strcpy(pExpr->userAlias, ((SFunctionNode*)pExpr)->functionName);
      strcpy(pExpr->aliasName, ((SFunctionNode*)pExpr)->functionName);     
    } else {
      int32_t len = TMIN(sizeof(pExpr->aliasName) - 1, pRawExpr->n);

      // See TS-3398.
      // Len of pRawExpr->p could be larger than len of aliasName[TSDB_COL_NAME_LEN].
      // If aliasName is truncated, hash value of aliasName could be the same.
      T_MD5_CTX ctx;
      tMD5Init(&ctx);
      tMD5Update(&ctx, (uint8_t*)pRawExpr->p, pRawExpr->n);
      tMD5Final(&ctx);
      char* p = pExpr->aliasName;
      for (uint8_t i = 0; i < tListLen(ctx.digest); ++i) {
        sprintf(p, "%02x", ctx.digest[i]);
        p += 2;
      }
      strncpy(pExpr->userAlias, pRawExpr->p, len);
      pExpr->userAlias[len] = '\0';
    }
  }
  pRawExpr->pNode = NULL;
  nodesDestroyNode(pNode);
  return pRealizedExpr;
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

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SNodeList* list = nodesMakeList();
  CHECK_OUT_OF_MEM(list);
  pCxt->errCode = nodesListAppend(list, pNode);
  return list;
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListAppend(pList, pNode);
  return pList;
}

SNode* createColumnNode(SAstCreateContext* pCxt, SToken* pTableAlias, SToken* pColumnName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTableName(pCxt, pTableAlias) || !checkColumnName(pCxt, pColumnName)) {
    return NULL;
  }
  SColumnNode* col = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_OUT_OF_MEM(col);
  if (NULL != pTableAlias) {
    COPY_STRING_FORM_ID_TOKEN(col->tableAlias, pTableAlias);
  }
  COPY_STRING_FORM_ID_TOKEN(col->colName, pColumnName);
  return (SNode*)col;
}

SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  if (TK_NK_ID != pLiteral->type && TK_TIMEZONE != pLiteral->type &&
      (IS_VAR_DATA_TYPE(dataType) || TSDB_DATA_TYPE_TIMESTAMP == dataType)) {
    trimString(pLiteral->z, pLiteral->n, val->literal, pLiteral->n);
  }
  CHECK_OUT_OF_MEM(val->literal);
  val->node.resType.type = dataType;
  val->node.resType.bytes = IS_VAR_DATA_TYPE(dataType) ? strlen(val->literal) : tDataTypes[dataType].bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
  val->isDuration = false;
  val->translate = false;
  return (SNode*)val;
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
    case HINT_BATCH_SCAN:
    case HINT_NO_BATCH_SCAN: {
      if (paramNum > 0) {
        return true;
      }
      break;
    }
    case HINT_SORT_FOR_GROUP:
      if (paramNum > 0) return true;
      if (hasHint(*ppHintList, HINT_PARTITION_FIRST)) return true;
      break;
    case HINT_PARTITION_FIRST:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_SORT_FOR_GROUP)) return true;
      break;
    case HINT_PARA_TABLES_SORT:
      if (paramNum > 0 || hasHint(*ppHintList, HINT_PARA_TABLES_SORT)) return true;
      break;
    default:
      return true;
  }

  SHintNode* hint = (SHintNode*)nodesMakeNode(QUERY_NODE_HINT);
  CHECK_OUT_OF_MEM(hint);
  hint->option = opt;
  hint->value = value;

  if (NULL == *ppHintList) {
    *ppHintList = nodesMakeList();
    CHECK_OUT_OF_MEM(*ppHintList);
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
  SNodeList*  pHintList = NULL;
  char*       hint = strndup(pLiteral->z + 3, pLiteral->n - 5);
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
    t0.n = tGetToken(&hint[i], &t0.type);
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
}

SNode* createIdentifierValueNode(SAstCreateContext* pCxt, SToken* pLiteral) {
  trimEscape(pLiteral);
  return createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, pLiteral);
}

SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
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
    val->literal = strndup(pLiteral->z + 1, pLiteral->n - 2);
  } else {
    val->literal = strndup(pLiteral->z, pLiteral->n);
  }
  CHECK_OUT_OF_MEM(val->literal);
  val->isDuration = true;
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  val->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
}

SNode* createDefaultDatabaseCondValue(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pCxt->pQueryCxt->db) {
    return NULL;
  }

  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = taosStrdup(pCxt->pQueryCxt->db);
  CHECK_OUT_OF_MEM(val->literal);
  val->isDuration = false;
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BINARY;
  val->node.resType.bytes = strlen(val->literal);
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
}

SNode* createPlaceholderValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pCxt->pQueryCxt->pStmtCb) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, pLiteral->z);
    return NULL;
  }
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  CHECK_OUT_OF_MEM(val->literal);
  val->placeholderNo = ++pCxt->placeholderNo;
  if (NULL == pCxt->pPlaceholderValues) {
    pCxt->pPlaceholderValues = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    if (NULL == pCxt->pPlaceholderValues) {
      nodesDestroyNode((SNode*)val);
      return NULL;
    }
  }
  taosArrayPush(pCxt->pPlaceholderValues, &val);
  return (SNode*)val;
}

static int32_t addParamToLogicConditionNode(SLogicConditionNode* pCond, SNode* pParam) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pParam) && pCond->condType == ((SLogicConditionNode*)pParam)->condType) {
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
  SLogicConditionNode* cond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  CHECK_OUT_OF_MEM(cond);
  cond->condType = type;
  cond->pParameterList = nodesMakeList();
  int32_t code = addParamToLogicConditionNode(cond, pParam1);
  if (TSDB_CODE_SUCCESS == code && NULL != pParam2) {
    code = addParamToLogicConditionNode(cond, pParam2);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)cond);
    return NULL;
  }
  return (SNode*)cond;
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
    CHECK_OUT_OF_MEM(pNewLiteral);
    if ('+' == pVal->literal[0]) {
      sprintf(pNewLiteral, "-%s", pVal->literal + 1);
    } else if ('-' == pVal->literal[0]) {
      sprintf(pNewLiteral, "%s", pVal->literal + 1);
    } else {
      sprintf(pNewLiteral, "-%s", pVal->literal);
    }
    taosMemoryFree(pVal->literal);
    pVal->literal = pNewLiteral;
    pVal->node.resType.type = getMinusDataType(pVal->node.resType.type);
    return pLeft;
  }
  SOperatorNode* op = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  CHECK_OUT_OF_MEM(op);
  op->opType = type;
  op->pLeft = pLeft;
  op->pRight = pRight;
  return (SNode*)op;
}

SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND,
                                  createOperatorNode(pCxt, OP_TYPE_GREATER_EQUAL, pExpr, pLeft),
                                  createOperatorNode(pCxt, OP_TYPE_LOWER_EQUAL, nodesCloneNode(pExpr), pRight));
}

SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, createOperatorNode(pCxt, OP_TYPE_LOWER_THAN, pExpr, pLeft),
                                  createOperatorNode(pCxt, OP_TYPE_GREATER_THAN, nodesCloneNode(pExpr), pRight));
}

static SNode* createPrimaryKeyCol(SAstCreateContext* pCxt, const SToken* pFuncName) {
  CHECK_PARSER_STATUS(pCxt);
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_OUT_OF_MEM(pCol);
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  if (NULL == pFuncName) {
    strcpy(pCol->colName, ROWTS_PSEUDO_COLUMN_NAME);
  } else {
    strncpy(pCol->colName, pFuncName->z, pFuncName->n);
  }
  return (SNode*)pCol;
}

SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList) {
  CHECK_PARSER_STATUS(pCxt);
  if (0 == strncasecmp("_rowts", pFuncName->z, pFuncName->n) || 0 == strncasecmp("_c0", pFuncName->z, pFuncName->n)) {
    return createPrimaryKeyCol(pCxt, pFuncName);
  }
  SFunctionNode* func = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(func);
  COPY_STRING_FORM_ID_TOKEN(func->functionName, pFuncName);
  func->pParameterList = pParameterList;
  return (SNode*)func;
}

SNode* createCastFunctionNode(SAstCreateContext* pCxt, SNode* pExpr, SDataType dt) {
  CHECK_PARSER_STATUS(pCxt);
  SFunctionNode* func = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(func);
  strcpy(func->functionName, "cast");
  func->node.resType = dt;
  if (TSDB_DATA_TYPE_VARCHAR == dt.type || TSDB_DATA_TYPE_GEOMETRY == dt.type || TSDB_DATA_TYPE_VARBINARY == dt.type) {
    func->node.resType.bytes = func->node.resType.bytes + VARSTR_HEADER_SIZE;
  } else if (TSDB_DATA_TYPE_NCHAR == dt.type) {
    func->node.resType.bytes = func->node.resType.bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  }
  nodesListMakeAppend(&func->pParameterList, pExpr);
  return (SNode*)func;
}

SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList) {
  CHECK_PARSER_STATUS(pCxt);
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = pList;
  return (SNode*)list;
}

SNode* createNodeListNodeEx(SAstCreateContext* pCxt, SNode* p1, SNode* p2) {
  CHECK_PARSER_STATUS(pCxt);
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = nodesMakeList();
  CHECK_OUT_OF_MEM(list->pNodeList);
  nodesListAppend(list->pNodeList, p1);
  nodesListAppend(list->pNodeList, p2);
  return (SNode*)list;
}

SNode* createRealTableNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pTableAlias) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, true) || !checkTableName(pCxt, pTableName) || !checkTableName(pCxt, pTableAlias)) {
    return NULL;
  }
  SRealTableNode* realTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  CHECK_OUT_OF_MEM(realTable);
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
}

SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias) {
  CHECK_PARSER_STATUS(pCxt);
  STempTableNode* tempTable = (STempTableNode*)nodesMakeNode(QUERY_NODE_TEMP_TABLE);
  CHECK_OUT_OF_MEM(tempTable);
  tempTable->pSubquery = pSubquery;
  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    COPY_STRING_FORM_ID_TOKEN(tempTable->table.tableAlias, pTableAlias);
  } else {
    taosRandStr(tempTable->table.tableAlias, 8);
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(pSubquery)) {
    strcpy(((SSelectStmt*)pSubquery)->stmtName, tempTable->table.tableAlias);
    ((SSelectStmt*)pSubquery)->isSubquery = true;
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pSubquery)) {
    strcpy(((SSetOperator*)pSubquery)->stmtName, tempTable->table.tableAlias);
  }
  return (SNode*)tempTable;
}

SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond) {
  CHECK_PARSER_STATUS(pCxt);
  SJoinTableNode* joinTable = (SJoinTableNode*)nodesMakeNode(QUERY_NODE_JOIN_TABLE);
  CHECK_OUT_OF_MEM(joinTable);
  joinTable->joinType = type;
  joinTable->pLeft = pLeft;
  joinTable->pRight = pRight;
  joinTable->pOnCond = pJoinCond;
  return (SNode*)joinTable;
}

SNode* createViewNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pViewName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, true) || !checkViewName(pCxt, pViewName)) {
    return NULL;
  }
  SViewNode* pView = (SViewNode*)nodesMakeNode(QUERY_NODE_VIEW);
  CHECK_OUT_OF_MEM(pView);
  if (NULL != pDbName) {
    COPY_STRING_FORM_ID_TOKEN(pView->table.dbName, pDbName);
  } else {
    snprintf(pView->table.dbName, sizeof(pView->table.dbName), "%s", pCxt->pQueryCxt->db);
  }
  COPY_STRING_FORM_ID_TOKEN(pView->table.tableName, pViewName);
  return (SNode*)pView;
}


SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset) {
  CHECK_PARSER_STATUS(pCxt);
  SLimitNode* limitNode = (SLimitNode*)nodesMakeNode(QUERY_NODE_LIMIT);
  CHECK_OUT_OF_MEM(limitNode);
  limitNode->limit = taosStr2Int64(pLimit->z, NULL, 10);
  if (NULL != pOffset) {
    limitNode->offset = taosStr2Int64(pOffset->z, NULL, 10);
  }
  return (SNode*)limitNode;
}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {
  CHECK_PARSER_STATUS(pCxt);
  SOrderByExprNode* orderByExpr = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  CHECK_OUT_OF_MEM(orderByExpr);
  orderByExpr->pExpr = pExpr;
  orderByExpr->order = order;
  if (NULL_ORDER_DEFAULT == nullOrder) {
    nullOrder = (ORDER_ASC == order ? NULL_ORDER_FIRST : NULL_ORDER_LAST);
  }
  orderByExpr->nullOrder = nullOrder;
  return (SNode*)orderByExpr;
}

SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, SNode* pGap) {
  CHECK_PARSER_STATUS(pCxt);
  SSessionWindowNode* session = (SSessionWindowNode*)nodesMakeNode(QUERY_NODE_SESSION_WINDOW);
  CHECK_OUT_OF_MEM(session);
  session->pCol = (SColumnNode*)pCol;
  session->pGap = (SValueNode*)pGap;
  return (SNode*)session;
}

SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pExpr) {
  CHECK_PARSER_STATUS(pCxt);
  SStateWindowNode* state = (SStateWindowNode*)nodesMakeNode(QUERY_NODE_STATE_WINDOW);
  CHECK_OUT_OF_MEM(state);
  state->pCol = createPrimaryKeyCol(pCxt, NULL);
  if (NULL == state->pCol) {
    nodesDestroyNode((SNode*)state);
    CHECK_OUT_OF_MEM(NULL);
  }
  state->pExpr = pExpr;
  return (SNode*)state;
}

SNode* createEventWindowNode(SAstCreateContext* pCxt, SNode* pStartCond, SNode* pEndCond) {
  CHECK_PARSER_STATUS(pCxt);
  SEventWindowNode* pEvent = (SEventWindowNode*)nodesMakeNode(QUERY_NODE_EVENT_WINDOW);
  CHECK_OUT_OF_MEM(pEvent);
  pEvent->pCol = createPrimaryKeyCol(pCxt, NULL);
  if (NULL == pEvent->pCol) {
    nodesDestroyNode((SNode*)pEvent);
    CHECK_OUT_OF_MEM(NULL);
  }
  pEvent->pStartCond = pStartCond;
  pEvent->pEndCond = pEndCond;
  return (SNode*)pEvent;
}

SNode* createCountWindowNode(SAstCreateContext* pCxt, const SToken* pCountToken, const SToken* pSlidingToken) {
  CHECK_PARSER_STATUS(pCxt);
  SCountWindowNode* pCount = (SCountWindowNode*)nodesMakeNode(QUERY_NODE_COUNT_WINDOW);
  CHECK_OUT_OF_MEM(pCount);
  pCount->pCol = createPrimaryKeyCol(pCxt, NULL);
  if (NULL == pCount->pCol) {
    nodesDestroyNode((SNode*)pCount);
    CHECK_OUT_OF_MEM(NULL);
  }
  pCount->windowCount = taosStr2Int64(pCountToken->z, NULL, 10);
  pCount->windowSliding = taosStr2Int64(pSlidingToken->z, NULL, 10);
  return (SNode*)pCount;
}

SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding,
                                SNode* pFill) {
  CHECK_PARSER_STATUS(pCxt);
  SIntervalWindowNode* interval = (SIntervalWindowNode*)nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  CHECK_OUT_OF_MEM(interval);
  interval->pCol = createPrimaryKeyCol(pCxt, NULL);
  if (NULL == interval->pCol) {
    nodesDestroyNode((SNode*)interval);
    CHECK_OUT_OF_MEM(NULL);
  }
  interval->pInterval = pInterval;
  interval->pOffset = pOffset;
  interval->pSliding = pSliding;
  interval->pFill = pFill;
  return (SNode*)interval;
}

SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues) {
  CHECK_PARSER_STATUS(pCxt);
  SFillNode* fill = (SFillNode*)nodesMakeNode(QUERY_NODE_FILL);
  CHECK_OUT_OF_MEM(fill);
  fill->mode = mode;
  fill->pValues = pValues;
  fill->pWStartTs = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == fill->pWStartTs) {
    nodesDestroyNode((SNode*)fill);
    CHECK_OUT_OF_MEM(NULL);
  }
  strcpy(((SFunctionNode*)fill->pWStartTs)->functionName, "_wstart");
  return (SNode*)fill;
}

SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SGroupingSetNode* groupingSet = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
  CHECK_OUT_OF_MEM(groupingSet);
  groupingSet->groupingSetType = GP_TYPE_NORMAL;
  groupingSet->pParameterList = nodesMakeList();
  nodesListAppend(groupingSet->pParameterList, pNode);
  return (SNode*)groupingSet;
}

SNode* createInterpTimeRange(SAstCreateContext* pCxt, SNode* pStart, SNode* pEnd) {
  CHECK_PARSER_STATUS(pCxt);
  return createBetweenAnd(pCxt, createPrimaryKeyCol(pCxt, NULL), pStart, pEnd);
}

SNode* createInterpTimePoint(SAstCreateContext* pCxt, SNode* pPoint) {
  CHECK_PARSER_STATUS(pCxt);
  return createOperatorNode(pCxt, OP_TYPE_EQUAL, createPrimaryKeyCol(pCxt, NULL), pPoint);
}

SNode* createWhenThenNode(SAstCreateContext* pCxt, SNode* pWhen, SNode* pThen) {
  CHECK_PARSER_STATUS(pCxt);
  SWhenThenNode* pWhenThen = (SWhenThenNode*)nodesMakeNode(QUERY_NODE_WHEN_THEN);
  CHECK_OUT_OF_MEM(pWhenThen);
  pWhenThen->pWhen = pWhen;
  pWhenThen->pThen = pThen;
  return (SNode*)pWhenThen;
}

SNode* createCaseWhenNode(SAstCreateContext* pCxt, SNode* pCase, SNodeList* pWhenThenList, SNode* pElse) {
  CHECK_PARSER_STATUS(pCxt);
  SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)nodesMakeNode(QUERY_NODE_CASE_WHEN);
  CHECK_OUT_OF_MEM(pCaseWhen);
  pCaseWhen->pCase = pCase;
  pCaseWhen->pWhenThenList = pWhenThenList;
  pCaseWhen->pElse = pElse;
  return (SNode*)pCaseWhen;
}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, SToken* pAlias) {
  CHECK_PARSER_STATUS(pCxt);
  trimEscape(pAlias);
  SExprNode* pExpr = (SExprNode*)pNode;
  int32_t    len = TMIN(sizeof(pExpr->aliasName) - 1, pAlias->n);
  strncpy(pExpr->aliasName, pAlias->z, len);
  pExpr->aliasName[len] = '\0';
  strncpy(pExpr->userAlias, pAlias->z, len);
  pExpr->userAlias[len] = '\0';
  pExpr->asAlias = true;
  return pNode;
}

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWhere = pWhere;
  }
  return pStmt;
}

SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pPartitionByList = pPartitionByList;
  }
  return pStmt;
}

SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWindow = pWindow;
  }
  return pStmt;
}

SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pGroupByList = pGroupByList;
  }
  return pStmt;
}

SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pHaving = pHaving;
  }
  return pStmt;
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
}

SNode* addRangeClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pRange) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pRange = pRange;
  }
  return pStmt;
}

SNode* addEveryClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pEvery) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pEvery = pEvery;
  }
  return pStmt;
}

SNode* addFillClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pFill) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt) && NULL != pFill) {
    SFillNode* pFillClause = (SFillNode*)pFill;
    nodesDestroyNode(pFillClause->pWStartTs);
    pFillClause->pWStartTs = createPrimaryKeyCol(pCxt, NULL);
    ((SSelectStmt*)pStmt)->pFill = (SNode*)pFillClause;
  }
  return pStmt;
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable,
                        SNodeList* pHint) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* select = createSelectStmtImpl(isDistinct, pProjectionList, pTable, pHint);
  CHECK_OUT_OF_MEM(select);
  return select;
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
  SSetOperator* setOp = (SSetOperator*)nodesMakeNode(QUERY_NODE_SET_OPERATOR);
  CHECK_OUT_OF_MEM(setOp);
  setOp->opType = type;
  setOp->pLeft = pLeft;
  setSubquery(setOp->pLeft);
  setOp->pRight = pRight;
  setSubquery(setOp->pRight);
  sprintf(setOp->stmtName, "%p", setOp);
  return (SNode*)setOp;
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
  SDatabaseOptions* pOptions = (SDatabaseOptions*)nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
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
  return (SNode*)pOptions;
}

SNode* createAlterDatabaseOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SDatabaseOptions* pOptions = (SDatabaseOptions*)nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
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
  return (SNode*)pOptions;
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
    case DB_OPTION_KEEP_TIME_OFFSET: {
      pDbOptions->keepTimeOffset = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    }
    default:
      break;
  }
  return pOptions;
}

SNode* setDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, EDatabaseOptionType type, void* pVal) {
  return setDatabaseOptionImpl(pCxt, pOptions, type, pVal, false);
}

SNode* setAlterDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption) {
  CHECK_PARSER_STATUS(pCxt);
  switch (pAlterOption->type) {
    case DB_OPTION_KEEP:
    case DB_OPTION_RETENTIONS:
      return setDatabaseOptionImpl(pCxt, pOptions, pAlterOption->type, pAlterOption->pList, true);
    default:
      break;
  }
  return setDatabaseOptionImpl(pCxt, pOptions, pAlterOption->type, &pAlterOption->val, true);
}

SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pDbName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SCreateDatabaseStmt* pStmt = (SCreateDatabaseStmt*)nodesMakeNode(QUERY_NODE_CREATE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createDropDatabaseStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SDropDatabaseStmt* pStmt = (SDropDatabaseStmt*)nodesMakeNode(QUERY_NODE_DROP_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createAlterDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SAlterDatabaseStmt* pStmt = (SAlterDatabaseStmt*)nodesMakeNode(QUERY_NODE_ALTER_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createFlushDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SFlushDatabaseStmt* pStmt = (SFlushDatabaseStmt*)nodesMakeNode(QUERY_NODE_FLUSH_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
}

SNode* createTrimDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, int32_t maxSpeed) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  STrimDatabaseStmt* pStmt = (STrimDatabaseStmt*)nodesMakeNode(QUERY_NODE_TRIM_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->maxSpeed = maxSpeed;
  return (SNode*)pStmt;
}

SNode* createCompactStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pStart, SNode* pEnd) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SCompactDatabaseStmt* pStmt = (SCompactDatabaseStmt*)nodesMakeNode(QUERY_NODE_COMPACT_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  pStmt->pStart = pStart;
  pStmt->pEnd = pEnd;
  return (SNode*)pStmt;
}

SNode* createDefaultTableOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STableOptions* pOptions = (STableOptions*)nodesMakeNode(QUERY_NODE_TABLE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->maxDelay1 = -1;
  pOptions->maxDelay2 = -1;
  pOptions->watermark1 = TSDB_DEFAULT_ROLLUP_WATERMARK;
  pOptions->watermark2 = TSDB_DEFAULT_ROLLUP_WATERMARK;
  pOptions->ttl = TSDB_DEFAULT_TABLE_TTL;
  pOptions->commentNull = true;  // mark null
  return (SNode*)pOptions;
}

SNode* createAlterTableOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STableOptions* pOptions = (STableOptions*)nodesMakeNode(QUERY_NODE_TABLE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->ttl = -1;
  pOptions->commentNull = true;  // mark null
  return (SNode*)pOptions;
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
        ttl = INT32_MAX;
      }
      // ttl can not be smaller than 0, because there is a limitation in sql.y (TTL NK_INTEGER)
      ((STableOptions*)pOptions)->ttl = ttl;
      break;
    }
    case TABLE_OPTION_SMA:
      ((STableOptions*)pOptions)->pSma = pVal;
      break;
    case TABLE_OPTION_DELETE_MARK:
      ((STableOptions*)pOptions)->pDeleteMark = pVal;
      break;
    default:
      break;
  }
  return pOptions;
}

SNode* createColumnDefNode(SAstCreateContext* pCxt, SToken* pColName, SDataType dataType, const SToken* pComment) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pColName) || !checkComment(pCxt, pComment, false)) {
    return NULL;
  }
  if (IS_VAR_DATA_TYPE(dataType.type) && dataType.bytes == 0) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
    return NULL;
  }
  SColumnDefNode* pCol = (SColumnDefNode*)nodesMakeNode(QUERY_NODE_COLUMN_DEF);
  CHECK_OUT_OF_MEM(pCol);
  COPY_STRING_FORM_ID_TOKEN(pCol->colName, pColName);
  pCol->dataType = dataType;
  if (NULL != pComment) {
    trimString(pComment->z, pComment->n, pCol->comments, sizeof(pCol->comments));
  }
  pCol->sma = true;
  return (SNode*)pCol;
}

SDataType createDataType(uint8_t type) {
  SDataType dt = {.type = type, .precision = 0, .scale = 0, .bytes = tDataTypes[type].bytes};
  return dt;
}

SDataType createVarLenDataType(uint8_t type, const SToken* pLen) {
  SDataType dt = {.type = type, .precision = 0, .scale = 0, .bytes = taosStr2Int32(pLen->z, NULL, 10)};
  return dt;
}

SNode* createCreateTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNodeList* pCols,
                             SNodeList* pTags, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateTableStmt* pStmt = (SCreateTableStmt*)nodesMakeNode(QUERY_NODE_CREATE_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pCols = pCols;
  pStmt->pTags = pTags;
  pStmt->pOptions = (STableOptions*)pOptions;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createCreateSubTableClause(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable,
                                  SNodeList* pSpecificTags, SNodeList* pValsOfTags, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateSubTableClause* pStmt = (SCreateSubTableClause*)nodesMakeNode(QUERY_NODE_CREATE_SUBTABLE_CLAUSE);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  strcpy(pStmt->useDbName, ((SRealTableNode*)pUseRealTable)->table.dbName);
  strcpy(pStmt->useTableName, ((SRealTableNode*)pUseRealTable)->table.tableName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pSpecificTags = pSpecificTags;
  pStmt->pValsOfTags = pValsOfTags;
  pStmt->pOptions = (STableOptions*)pOptions;
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pUseRealTable);
  return (SNode*)pStmt;
}

SNode* createCreateMultiTableStmt(SAstCreateContext* pCxt, SNodeList* pSubTables) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateMultiTablesStmt* pStmt = (SCreateMultiTablesStmt*)nodesMakeNode(QUERY_NODE_CREATE_MULTI_TABLES_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pSubTables = pSubTables;
  return (SNode*)pStmt;
}

SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTableClause* pStmt = (SDropTableClause*)nodesMakeNode(QUERY_NODE_DROP_TABLE_CLAUSE);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createDropTableStmt(SAstCreateContext* pCxt, SNodeList* pTables) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTableStmt* pStmt = (SDropTableStmt*)nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pTables = pTables;
  return (SNode*)pStmt;
}

SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropSuperTableStmt* pStmt = (SDropSuperTableStmt*)nodesMakeNode(QUERY_NODE_DROP_SUPER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

static SNode* createAlterTableStmtFinalize(SNode* pRealTable, SAlterTableStmt* pStmt) {
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createAlterTableModifyOptions(SAstCreateContext* pCxt, SNode* pRealTable, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterTableStmt* pStmt = (SAlterTableStmt*)nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_OPTIONS;
  pStmt->pOptions = (STableOptions*)pOptions;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableAddModifyCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName,
                                    SDataType dataType) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pColName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = (SAlterTableStmt*)nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  pStmt->dataType = dataType;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableDropCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pColName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = (SAlterTableStmt*)nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pColName);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableRenameCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pOldColName,
                                 SToken* pNewColName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pOldColName) || !checkColumnName(pCxt, pNewColName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = (SAlterTableStmt*)nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pOldColName);
  COPY_STRING_FORM_ID_TOKEN(pStmt->newColName, pNewColName);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableSetTag(SAstCreateContext* pCxt, SNode* pRealTable, SToken* pTagName, SNode* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pTagName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = (SAlterTableStmt*)nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
  COPY_STRING_FORM_ID_TOKEN(pStmt->colName, pTagName);
  pStmt->pVal = (SValueNode*)pVal;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* setAlterSuperTableType(SNode* pStmt) {
  setNodeType(pStmt, QUERY_NODE_ALTER_SUPER_TABLE_STMT);
  return pStmt;
}

SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SUseDatabaseStmt* pStmt = (SUseDatabaseStmt*)nodesMakeNode(QUERY_NODE_USE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
}

static bool needDbShowStmt(ENodeType type) {
  return QUERY_NODE_SHOW_TABLES_STMT == type || QUERY_NODE_SHOW_STABLES_STMT == type ||
         QUERY_NODE_SHOW_VGROUPS_STMT == type || QUERY_NODE_SHOW_INDEXES_STMT == type ||
         QUERY_NODE_SHOW_TAGS_STMT == type || QUERY_NODE_SHOW_TABLE_TAGS_STMT == type ||
         QUERY_NODE_SHOW_VIEWS_STMT == type;
}

SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowStmt* pStmt = (SShowStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  return (SNode*)pStmt;
}

SNode* createShowCompactsStmt(SAstCreateContext* pCxt, ENodeType type) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCompactsStmt* pStmt = (SShowCompactsStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  return (SNode*)pStmt;
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
    return NULL;
  }
  SShowStmt* pStmt = (SShowStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->pTbName = pTbName;
  pStmt->tableCondType = tableCondType;
  return (SNode*)pStmt;
}

SNode* createShowTablesStmt(SAstCreateContext* pCxt, SShowTablesOption option, SNode* pTbName, EOperatorType tableCondType) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pDbName = NULL;
  if (option.dbName.type == TK_NK_NIL) {
    pDbName = createDefaultDatabaseCondValue(pCxt);
  } else {
    pDbName = createIdentifierValueNode(pCxt, &option.dbName);
  }
  SNode* pStmt = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TABLES_STMT, pDbName, pTbName, tableCondType);
  setShowKind(pCxt, pStmt, option.kind);
  return pStmt;
}

SNode* createShowCreateDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, true)) {
    return NULL;
  }
  SShowCreateDatabaseStmt* pStmt = (SShowCreateDatabaseStmt*)nodesMakeNode(QUERY_NODE_SHOW_CREATE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbName);
  return (SNode*)pStmt;
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

  if (pDbToken && !checkDbName(pCxt, pDbToken, true)) {
    nodesDestroyNode(pNode);
    return NULL;
  }

  SShowAliveStmt* pStmt = (SShowAliveStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);

  if (pDbToken) {
    COPY_STRING_FORM_ID_TOKEN(pStmt->dbName, pDbToken);
  }
  if (pNode) {
    nodesDestroyNode(pNode);
  }

  return (SNode*)pStmt;
}

SNode* createShowCreateTableStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCreateTableStmt* pStmt = (SShowCreateTableStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createShowCreateViewStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCreateViewStmt* pStmt = (SShowCreateViewStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->viewName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}


SNode* createShowTableDistributedStmt(SAstCreateContext* pCxt, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SShowTableDistributedStmt* pStmt = (SShowTableDistributedStmt*)nodesMakeNode(QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createShowDnodeVariablesStmt(SAstCreateContext* pCxt, SNode* pDnodeId, SNode* pLikePattern) {
  CHECK_PARSER_STATUS(pCxt);
  SShowDnodeVariablesStmt* pStmt = (SShowDnodeVariablesStmt*)nodesMakeNode(QUERY_NODE_SHOW_DNODE_VARIABLES_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pDnodeId = pDnodeId;
  pStmt->pLikePattern = pLikePattern;
  return (SNode*)pStmt;
}

SNode* createShowVnodesStmt(SAstCreateContext* pCxt, SNode* pDnodeId, SNode* pDnodeEndpoint) {
  CHECK_PARSER_STATUS(pCxt);
  SShowVnodesStmt* pStmt = (SShowVnodesStmt*)nodesMakeNode(QUERY_NODE_SHOW_VNODES_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pDnodeId = pDnodeId;
  pStmt->pDnodeEndpoint = pDnodeEndpoint;
  return (SNode*)pStmt;
}

SNode* createShowTableTagsStmt(SAstCreateContext* pCxt, SNode* pTbName, SNode* pDbName, SNodeList* pTags) {
  CHECK_PARSER_STATUS(pCxt);
  if (NULL == pDbName) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "database not specified");
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return NULL;
  }
  SShowTableTagsStmt* pStmt = (SShowTableTagsStmt*)nodesMakeNode(QUERY_NODE_SHOW_TABLE_TAGS_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->pTbName = pTbName;
  pStmt->pTags = pTags;
  return (SNode*)pStmt;
}

SNode* createShowCompactDetailsStmt(SAstCreateContext* pCxt, SNode* pCompactId) {
  CHECK_PARSER_STATUS(pCxt);
  SShowCompactDetailsStmt* pStmt = (SShowCompactDetailsStmt*)nodesMakeNode(QUERY_NODE_SHOW_COMPACT_DETAILS_STMT);
  pStmt->pCompactId = pCompactId;
  return (SNode*)pStmt;
}

static int32_t getIpV4RangeFromWhitelistItem(char* ipRange, SIpV4Range* pIpRange) {
  int32_t code = TSDB_CODE_SUCCESS;
  char*   ipCopy = taosStrdup(ipRange);
  char*   slash = strchr(ipCopy, '/');
  if (slash) {
    *slash = '\0';
    struct in_addr addr;
    if (uv_inet_pton(AF_INET, ipCopy, &addr) == 0) {
      int prefix = atoi(slash + 1);
      if (prefix < 0 || prefix > 32) {
        code = TSDB_CODE_PAR_INVALID_IP_RANGE;
      } else {
        pIpRange->ip = addr.s_addr;
        pIpRange->mask = prefix;
        code = TSDB_CODE_SUCCESS;
      }
    } else {
      code = TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
  } else {
    struct in_addr addr;
    if (uv_inet_pton(AF_INET, ipCopy, &addr) == 0) {
      pIpRange->ip = addr.s_addr;
      pIpRange->mask = 32;
      code = TSDB_CODE_SUCCESS;
    } else {
      code = TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
  }

  taosMemoryFreeClear(ipCopy);
  return code;
}

static int32_t fillIpRangesFromWhiteList(SAstCreateContext* pCxt, SNodeList* pIpRangesNodeList, SIpV4Range* pIpRanges) {
  int32_t i = 0;
  int32_t code = 0;

  SNode* pNode = NULL;
  FOREACH(pNode, pIpRangesNodeList) {
    if (QUERY_NODE_VALUE != nodeType(pNode)) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IP_RANGE);
      return TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
    SValueNode* pValNode = (SValueNode*)(pNode);
    code = getIpV4RangeFromWhitelistItem(pValNode->literal, pIpRanges + i);
    ++i;
    if (code != TSDB_CODE_SUCCESS) {
      pCxt->errCode = generateSyntaxErrMsgExt(&pCxt->msgBuf, code, "Invalid IP range %s", pValNode->literal);
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

SNode* addCreateUserStmtWhiteList(SAstCreateContext* pCxt, SNode* pCreateUserStmt, SNodeList* pIpRangesNodeList) {
  if (NULL == pCreateUserStmt || NULL == pIpRangesNodeList) {
    return pCreateUserStmt;
  }

  ((SCreateUserStmt*)pCreateUserStmt)->pNodeListIpRanges = pIpRangesNodeList;
  SCreateUserStmt* pCreateUser = (SCreateUserStmt*)pCreateUserStmt;
  pCreateUser->numIpRanges = LIST_LENGTH(pIpRangesNodeList);
  pCreateUser->pIpRanges = taosMemoryMalloc(pCreateUser->numIpRanges * sizeof(SIpV4Range));
  if (NULL == pCreateUser->pIpRanges) {
    pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
    nodesDestroyNode(pCreateUserStmt);
    return NULL;
  }

  int32_t code = fillIpRangesFromWhiteList(pCxt, pIpRangesNodeList, pCreateUser->pIpRanges);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pCreateUserStmt);
    return NULL;
  }
  return pCreateUserStmt;
}

SNode* createCreateUserStmt(SAstCreateContext* pCxt, SToken* pUserName, const SToken* pPassword, int8_t sysinfo) {
  CHECK_PARSER_STATUS(pCxt);
  char password[TSDB_USET_PASSWORD_LEN + 3] = {0};
  if (!checkUserName(pCxt, pUserName) || !checkPassword(pCxt, pPassword, password)) {
    return NULL;
  }
  SCreateUserStmt* pStmt = (SCreateUserStmt*)nodesMakeNode(QUERY_NODE_CREATE_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  strcpy(pStmt->password, password);
  pStmt->sysinfo = sysinfo;
  return (SNode*)pStmt;
}

SNode* createAlterUserStmt(SAstCreateContext* pCxt, SToken* pUserName, int8_t alterType, void* pAlterInfo) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SAlterUserStmt* pStmt = (SAlterUserStmt*)nodesMakeNode(QUERY_NODE_ALTER_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->alterType = alterType;
  switch (alterType) {
    case TSDB_ALTER_USER_PASSWD: {
      char    password[TSDB_USET_PASSWORD_LEN] = {0};
      SToken* pVal = pAlterInfo;
      if (!checkPassword(pCxt, pVal, password)) {
        nodesDestroyNode((SNode*)pStmt);
        return NULL;
      }
      strcpy(pStmt->password, password);
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
    case TSDB_ALTER_USER_ADD_WHITE_LIST:
    case TSDB_ALTER_USER_DROP_WHITE_LIST: {
      SNodeList* pIpRangesNodeList = pAlterInfo;
      pStmt->pNodeListIpRanges = pIpRangesNodeList;
      pStmt->numIpRanges = LIST_LENGTH(pIpRangesNodeList);
      pStmt->pIpRanges = taosMemoryMalloc(pStmt->numIpRanges * sizeof(SIpV4Range));
      if (NULL == pStmt->pIpRanges) {
        pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
        nodesDestroyNode((SNode*)pStmt);
        return NULL;
      }

      int32_t code = fillIpRangesFromWhiteList(pCxt, pIpRangesNodeList, pStmt->pIpRanges);
      if (TSDB_CODE_SUCCESS != code) {
        nodesDestroyNode((SNode*)pStmt);
        return NULL;
      }
      break;
    }
    default:
      break;
  }
  return (SNode*)pStmt;
}

SNode* createDropUserStmt(SAstCreateContext* pCxt, SToken* pUserName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SDropUserStmt* pStmt = (SDropUserStmt*)nodesMakeNode(QUERY_NODE_DROP_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  return (SNode*)pStmt;
}

SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateDnodeStmt* pStmt = (SCreateDnodeStmt*)nodesMakeNode(QUERY_NODE_CREATE_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (!checkAndSplitEndpoint(pCxt, pFqdn, pPort, pStmt->fqdn, &pStmt->port)) {
    nodesDestroyNode((SNode*)pStmt);
    return NULL;
  }
  return (SNode*)pStmt;
}

SNode* createDropDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, bool force, bool unsafe) {
  CHECK_PARSER_STATUS(pCxt);
  SDropDnodeStmt* pStmt = (SDropDnodeStmt*)nodesMakeNode(QUERY_NODE_DROP_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
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
}

SNode* createAlterDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, const SToken* pConfig,
                            const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterDnodeStmt* pStmt = (SAlterDnodeStmt*)nodesMakeNode(QUERY_NODE_ALTER_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (NULL != pDnode) {
    pStmt->dnodeId = taosStr2Int32(pDnode->z, NULL, 10);
  } else {
    pStmt->dnodeId = -1;
  }
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
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
  SCreateIndexStmt* pStmt = (SCreateIndexStmt*)nodesMakeNode(QUERY_NODE_CREATE_INDEX_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->indexType = type;
  pStmt->ignoreExists = ignoreExists;

  SRealTableNode* pFullTable = (SRealTableNode*)pRealTable;
  if (strlen(pFullTable->table.dbName) == 0) {
    // no db specified,
    if (pCxt->pQueryCxt->db == NULL) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DB_NOT_SPECIFIED);
      nodesDestroyNode(pIndexName);
      nodesDestroyNode(pRealTable);
      nodesDestroyNode(pOptions);
      return NULL;
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
}

SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset, SNode* pSliding,
                         SNode* pStreamOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SIndexOptions* pOptions = (SIndexOptions*)nodesMakeNode(QUERY_NODE_INDEX_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->pFuncs = pFuncs;
  pOptions->pInterval = pInterval;
  pOptions->pOffset = pOffset;
  pOptions->pSliding = pSliding;
  pOptions->pStreamOptions = pStreamOptions;
  return (SNode*)pOptions;
}

SNode* createDropIndexStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pIndexName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropIndexStmt* pStmt = (SDropIndexStmt*)nodesMakeNode(QUERY_NODE_DROP_INDEX_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  snprintf(pStmt->indexDbName, sizeof(pStmt->indexDbName), "%s", ((SRealTableNode*)pIndexName)->table.dbName);
  snprintf(pStmt->indexName, sizeof(pStmt->indexName), "%s", ((SRealTableNode*)pIndexName)->table.tableName);
  nodesDestroyNode(pIndexName);
  return (SNode*)pStmt;
}

SNode* createCreateComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateComponentNodeStmt* pStmt = (SCreateComponentNodeStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  return (SNode*)pStmt;
}

SNode* createDropComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SDropComponentNodeStmt* pStmt = (SDropComponentNodeStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  return (SNode*)pStmt;
}

SNode* createRestoreComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SRestoreComponentNodeStmt* pStmt = (SRestoreComponentNodeStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  return (SNode*)pStmt;
}

SNode* createCreateTopicStmtUseQuery(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTopicName(pCxt, pTopicName)) {
    return NULL;
  }
  SCreateTopicStmt* pStmt = (SCreateTopicStmt*)nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
}

SNode* createCreateTopicStmtUseDb(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SToken* pSubDbName,
                                  int8_t withMeta) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTopicName(pCxt, pTopicName) || !checkDbName(pCxt, pSubDbName, true)) {
    return NULL;
  }
  SCreateTopicStmt* pStmt = (SCreateTopicStmt*)nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreExists = ignoreExists;
  COPY_STRING_FORM_ID_TOKEN(pStmt->subDbName, pSubDbName);
  pStmt->withMeta = withMeta;
  return (SNode*)pStmt;
}

SNode* createCreateTopicStmtUseTable(SAstCreateContext* pCxt, bool ignoreExists, SToken* pTopicName, SNode* pRealTable,
                                     int8_t withMeta, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTopicName(pCxt, pTopicName)) {
    return NULL;
  }
  SCreateTopicStmt* pStmt = (SCreateTopicStmt*)nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->withMeta = withMeta;
  pStmt->pWhere = pWhere;

  strcpy(pStmt->subDbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->subSTbName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createDropTopicStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pTopicName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTopicName(pCxt, pTopicName)) {
    return NULL;
  }
  SDropTopicStmt* pStmt = (SDropTopicStmt*)nodesMakeNode(QUERY_NODE_DROP_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createDropCGroupStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pCGroupId, SToken* pTopicName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkTopicName(pCxt, pTopicName)) {
    return NULL;
  }
  if (!checkCGroupName(pCxt, pCGroupId)) {
    return NULL;
  }
  SDropCGroupStmt* pStmt = (SDropCGroupStmt*)nodesMakeNode(QUERY_NODE_DROP_CGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  COPY_STRING_FORM_ID_TOKEN(pStmt->topicName, pTopicName);
  COPY_STRING_FORM_ID_TOKEN(pStmt->cgroup, pCGroupId);
  return (SNode*)pStmt;
}

SNode* createAlterClusterStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterClusterStmt* pStmt = (SAlterClusterStmt*)nodesMakeNode(QUERY_NODE_ALTER_CLUSTER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}

SNode* createAlterLocalStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterLocalStmt* pStmt = (SAlterLocalStmt*)nodesMakeNode(QUERY_NODE_ALTER_LOCAL_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}

SNode* createDefaultExplainOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SExplainOptions* pOptions = (SExplainOptions*)nodesMakeNode(QUERY_NODE_EXPLAIN_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->verbose = TSDB_DEFAULT_EXPLAIN_VERBOSE;
  pOptions->ratio = TSDB_DEFAULT_EXPLAIN_RATIO;
  return (SNode*)pOptions;
}

SNode* setExplainVerbose(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  ((SExplainOptions*)pOptions)->verbose = (0 == strncasecmp(pVal->z, "true", pVal->n));
  return pOptions;
}

SNode* setExplainRatio(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  ((SExplainOptions*)pOptions)->ratio = taosStr2Double(pVal->z, NULL);
  return pOptions;
}

SNode* createExplainStmt(SAstCreateContext* pCxt, bool analyze, SNode* pOptions, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  SExplainStmt* pStmt = (SExplainStmt*)nodesMakeNode(QUERY_NODE_EXPLAIN_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->analyze = analyze;
  pStmt->pOptions = (SExplainOptions*)pOptions;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
}

SNode* createDescribeStmt(SAstCreateContext* pCxt, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDescribeStmt* pStmt = (SDescribeStmt*)nodesMakeNode(QUERY_NODE_DESCRIBE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createResetQueryCacheStmt(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_RESET_QUERY_CACHE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
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
    return NULL;
  }
  int8_t language = 0;
  if (TSDB_CODE_SUCCESS != convertUdfLanguageType(pCxt, pLanguage, &language)) {
    return NULL;
  }
  SCreateFunctionStmt* pStmt = (SCreateFunctionStmt*)nodesMakeNode(QUERY_NODE_CREATE_FUNCTION_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->orReplace = orReplace;
  pStmt->ignoreExists = ignoreExists;
  COPY_STRING_FORM_ID_TOKEN(pStmt->funcName, pFuncName);
  pStmt->isAgg = aggFunc;
  COPY_STRING_FORM_STR_TOKEN(pStmt->libraryPath, pLibPath);
  pStmt->outputDt = dataType;
  pStmt->bufSize = bufSize;
  pStmt->language = language;
  return (SNode*)pStmt;
}

SNode* createDropFunctionStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pFuncName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropFunctionStmt* pStmt = (SDropFunctionStmt*)nodesMakeNode(QUERY_NODE_DROP_FUNCTION_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  COPY_STRING_FORM_ID_TOKEN(pStmt->funcName, pFuncName);
  return (SNode*)pStmt;
}

SNode* createCreateViewStmt(SAstCreateContext* pCxt, bool orReplace, SNode* pView, const SToken* pAs, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateViewStmt* pStmt = (SCreateViewStmt*)nodesMakeNode(QUERY_NODE_CREATE_VIEW_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  int32_t i = pAs->n;
  while (isspace(*(pAs->z + i))) {
    ++i;
  }
  pStmt->pQuerySql = tstrdup(pAs->z + i);
  CHECK_OUT_OF_MEM(pStmt->pQuerySql);
  strcpy(pStmt->dbName, ((SViewNode*)pView)->table.dbName);
  strcpy(pStmt->viewName, ((SViewNode*)pView)->table.tableName);
  nodesDestroyNode(pView);
  pStmt->orReplace = orReplace;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
}

SNode* createDropViewStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pView) {
  CHECK_PARSER_STATUS(pCxt);
  SDropViewStmt* pStmt = (SDropViewStmt*)nodesMakeNode(QUERY_NODE_DROP_VIEW_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  strcpy(pStmt->dbName, ((SViewNode*)pView)->table.dbName);
  strcpy(pStmt->viewName, ((SViewNode*)pView)->table.tableName);
  nodesDestroyNode(pView);
  return (SNode*)pStmt;
}

SNode* createStreamOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SStreamOptions* pOptions = (SStreamOptions*)nodesMakeNode(QUERY_NODE_STREAM_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->triggerType = STREAM_TRIGGER_WINDOW_CLOSE;
  pOptions->fillHistory = STREAM_DEFAULT_FILL_HISTORY;
  pOptions->ignoreExpired = STREAM_DEFAULT_IGNORE_EXPIRED;
  pOptions->ignoreUpdate = STREAM_DEFAULT_IGNORE_UPDATE;
  return (SNode*)pOptions;
}

static int8_t getTriggerType(uint32_t tokenType) {
  switch (tokenType) {
    case TK_AT_ONCE:
      return STREAM_TRIGGER_AT_ONCE;
    case TK_WINDOW_CLOSE:
      return STREAM_TRIGGER_WINDOW_CLOSE;
    case TK_MAX_DELAY:
      return STREAM_TRIGGER_MAX_DELAY;
    default:
      break;
  }
  return STREAM_TRIGGER_WINDOW_CLOSE;
}

SNode* setStreamOptions(SAstCreateContext* pCxt, SNode* pOptions, EStreamOptionsSetFlag setflag, SToken* pToken,
                        SNode* pNode) {
  SStreamOptions* pStreamOptions = (SStreamOptions*)pOptions;
  if (BIT_FLAG_TEST_MASK(setflag, pStreamOptions->setFlag)) {
    pCxt->errCode =
        generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, "stream options each item is only set once");
    return pOptions;
  }

  switch (setflag) {
    case SOPT_TRIGGER_TYPE_SET:
      pStreamOptions->triggerType = getTriggerType(pToken->type);
      if (STREAM_TRIGGER_MAX_DELAY == pStreamOptions->triggerType) {
        pStreamOptions->pDelay = pNode;
      }
      break;
    case SOPT_WATERMARK_SET:
      pStreamOptions->pWatermark = pNode;
      break;
    case SOPT_DELETE_MARK_SET:
      pStreamOptions->pDeleteMark = pNode;
      break;
    case SOPT_FILL_HISTORY_SET:
      pStreamOptions->fillHistory = taosStr2Int8(pToken->z, NULL, 10);
      break;
    case SOPT_IGNORE_EXPIRED_SET:
      pStreamOptions->ignoreExpired = taosStr2Int8(pToken->z, NULL, 10);
      break;
    case SOPT_IGNORE_UPDATE_SET:
      pStreamOptions->ignoreUpdate = taosStr2Int8(pToken->z, NULL, 10);
      break;
    default:
      break;
  }
  BIT_FLAG_SET_MASK(pStreamOptions->setFlag, setflag);

  return pOptions;
}

SNode* createCreateStreamStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pStreamName, SNode* pRealTable,
                              SNode* pOptions, SNodeList* pTags, SNode* pSubtable, SNode* pQuery, SNodeList* pCols) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkStreamName(pCxt, pStreamName)) {
    return NULL;
  }
  SCreateStreamStmt* pStmt = (SCreateStreamStmt*)nodesMakeNode(QUERY_NODE_CREATE_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->streamName, pStreamName);
  strcpy(pStmt->targetDbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->targetTabName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pOptions = (SStreamOptions*)pOptions;
  pStmt->pQuery = pQuery;
  pStmt->pTags = pTags;
  pStmt->pSubtable = pSubtable;
  pStmt->pCols = pCols;
  return (SNode*)pStmt;
}

SNode* createDropStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pStreamName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkStreamName(pCxt, pStreamName)) {
    return NULL;
  }
  SDropStreamStmt* pStmt = (SDropStreamStmt*)nodesMakeNode(QUERY_NODE_DROP_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->streamName, pStreamName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createPauseStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pStreamName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkStreamName(pCxt, pStreamName)) {
    return NULL;
  }
  SPauseStreamStmt* pStmt = (SPauseStreamStmt*)nodesMakeNode(QUERY_NODE_PAUSE_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->streamName, pStreamName);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createResumeStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, bool ignoreUntreated,
                              SToken* pStreamName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkStreamName(pCxt, pStreamName)) {
    return NULL;
  }
  SResumeStreamStmt* pStmt = (SResumeStreamStmt*)nodesMakeNode(QUERY_NODE_RESUME_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_ID_TOKEN(pStmt->streamName, pStreamName);
  pStmt->ignoreNotExists = ignoreNotExists;
  pStmt->ignoreUntreated = ignoreUntreated;
  return (SNode*)pStmt;
}

SNode* createKillStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pId) {
  CHECK_PARSER_STATUS(pCxt);
  SKillStmt* pStmt = (SKillStmt*)nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->targetId = taosStr2Int32(pId->z, NULL, 10);
  return (SNode*)pStmt;
}

SNode* createKillQueryStmt(SAstCreateContext* pCxt, const SToken* pQueryId) {
  CHECK_PARSER_STATUS(pCxt);
  SKillQueryStmt* pStmt = (SKillQueryStmt*)nodesMakeNode(QUERY_NODE_KILL_QUERY_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  trimString(pQueryId->z, pQueryId->n, pStmt->queryId, sizeof(pStmt->queryId) - 1);
  return (SNode*)pStmt;
}

SNode* createBalanceVgroupStmt(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SBalanceVgroupStmt* pStmt = (SBalanceVgroupStmt*)nodesMakeNode(QUERY_NODE_BALANCE_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return (SNode*)pStmt;
}

SNode* createBalanceVgroupLeaderStmt(SAstCreateContext* pCxt, const SToken* pVgId) {
  CHECK_PARSER_STATUS(pCxt);
  SBalanceVgroupLeaderStmt* pStmt = (SBalanceVgroupLeaderStmt*)nodesMakeNode(QUERY_NODE_BALANCE_VGROUP_LEADER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (NULL != pVgId && NULL != pVgId->z) {
    pStmt->vgId = taosStr2Int32(pVgId->z, NULL, 10);
  }
  return (SNode*)pStmt;
}

SNode* createMergeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId1, const SToken* pVgId2) {
  CHECK_PARSER_STATUS(pCxt);
  SMergeVgroupStmt* pStmt = (SMergeVgroupStmt*)nodesMakeNode(QUERY_NODE_MERGE_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->vgId1 = taosStr2Int32(pVgId1->z, NULL, 10);
  pStmt->vgId2 = taosStr2Int32(pVgId2->z, NULL, 10);
  return (SNode*)pStmt;
}

SNode* createRedistributeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId, SNodeList* pDnodes) {
  CHECK_PARSER_STATUS(pCxt);
  SRedistributeVgroupStmt* pStmt = (SRedistributeVgroupStmt*)nodesMakeNode(QUERY_NODE_REDISTRIBUTE_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->vgId = taosStr2Int32(pVgId->z, NULL, 10);
  pStmt->pDnodes = pDnodes;
  return (SNode*)pStmt;
}

SNode* createSplitVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId) {
  CHECK_PARSER_STATUS(pCxt);
  SSplitVgroupStmt* pStmt = (SSplitVgroupStmt*)nodesMakeNode(QUERY_NODE_SPLIT_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->vgId = taosStr2Int32(pVgId->z, NULL, 10);
  return (SNode*)pStmt;
}

SNode* createSyncdbStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_SYNCDB_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createGrantStmt(SAstCreateContext* pCxt, int64_t privileges, STokenPair* pPrivLevel, SToken* pUserName,
                       SNode* pTagCond) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, &pPrivLevel->first, false) || !checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SGrantStmt* pStmt = (SGrantStmt*)nodesMakeNode(QUERY_NODE_GRANT_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->privileges = privileges;
  COPY_STRING_FORM_ID_TOKEN(pStmt->objName, &pPrivLevel->first);
  if (TK_NK_NIL != pPrivLevel->second.type && TK_NK_STAR != pPrivLevel->second.type) {
    COPY_STRING_FORM_ID_TOKEN(pStmt->tabName, &pPrivLevel->second);
  }
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->pTagCond = pTagCond;
  return (SNode*)pStmt;
}

SNode* createRevokeStmt(SAstCreateContext* pCxt, int64_t privileges, STokenPair* pPrivLevel, SToken* pUserName,
                        SNode* pTagCond) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, &pPrivLevel->first, false) || !checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SRevokeStmt* pStmt = (SRevokeStmt*)nodesMakeNode(QUERY_NODE_REVOKE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->privileges = privileges;
  COPY_STRING_FORM_ID_TOKEN(pStmt->objName, &pPrivLevel->first);
  if (TK_NK_NIL != pPrivLevel->second.type && TK_NK_STAR != pPrivLevel->second.type) {
    COPY_STRING_FORM_ID_TOKEN(pStmt->tabName, &pPrivLevel->second);
  }
  COPY_STRING_FORM_ID_TOKEN(pStmt->userName, pUserName);
  pStmt->pTagCond = pTagCond;
  return (SNode*)pStmt;
}

SNode* createFuncForDelete(SAstCreateContext* pCxt, const char* pFuncName) {
  SFunctionNode* pFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(pFunc);
  snprintf(pFunc->functionName, sizeof(pFunc->functionName), "%s", pFuncName);
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pFunc->pParameterList, createPrimaryKeyCol(pCxt, NULL))) {
    nodesDestroyNode((SNode*)pFunc);
    CHECK_OUT_OF_MEM(NULL);
  }
  return (SNode*)pFunc;
}

SNode* createDeleteStmt(SAstCreateContext* pCxt, SNode* pTable, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  SDeleteStmt* pStmt = (SDeleteStmt*)nodesMakeNode(QUERY_NODE_DELETE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pFromTable = pTable;
  pStmt->pWhere = pWhere;
  pStmt->pCountFunc = createFuncForDelete(pCxt, "count");
  pStmt->pFirstFunc = createFuncForDelete(pCxt, "first");
  pStmt->pLastFunc = createFuncForDelete(pCxt, "last");
  if (NULL == pStmt->pCountFunc || NULL == pStmt->pFirstFunc || NULL == pStmt->pLastFunc) {
    nodesDestroyNode((SNode*)pStmt);
    CHECK_OUT_OF_MEM(NULL);
  }
  return (SNode*)pStmt;
}

SNode* createInsertStmt(SAstCreateContext* pCxt, SNode* pTable, SNodeList* pCols, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  SInsertStmt* pStmt = (SInsertStmt*)nodesMakeNode(QUERY_NODE_INSERT_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pTable = pTable;
  pStmt->pCols = pCols;
  pStmt->pQuery = pQuery;
  if (QUERY_NODE_SELECT_STMT == nodeType(pQuery)) {
    strcpy(((SSelectStmt*)pQuery)->stmtName, ((STableNode*)pTable)->tableAlias);
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pQuery)) {
    strcpy(((SSetOperator*)pQuery)->stmtName, ((STableNode*)pTable)->tableAlias);
  }
  return (SNode*)pStmt;
}
