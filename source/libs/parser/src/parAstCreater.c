
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

#include "parAst.h"
#include "parUtil.h"

#define CHECK_OUT_OF_MEM(p) \
  do { \
    if (NULL == (p)) { \
      pCxt->valid = false; \
      return NULL; \
    } \
  } while (0)

#define CHECK_RAW_EXPR_NODE(node) \
  do { \
    if (NULL == (node) || QUERY_NODE_RAW_EXPR != nodeType(node)) { \
      pCxt->valid = false; \
      return NULL; \
    } \
  } while (0)

SToken nil_token = { .type = TK_NK_NIL, .n = 0, .z = NULL };

typedef SDatabaseOptions* (*FSetDatabaseOption)(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal);
static FSetDatabaseOption setDbOptionFuncs[DB_OPTION_MAX];

typedef STableOptions* (*FSetTableOption)(SAstCreateContext* pCxt, STableOptions* pOptions, const SToken* pVal);
static FSetTableOption setTableOptionFuncs[TABLE_OPTION_MAX];

static SDatabaseOptions* setDbBlocks(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_TOTAL_BLOCKS || val > TSDB_MAX_TOTAL_BLOCKS) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option totalBlocks: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_TOTAL_BLOCKS, TSDB_MAX_TOTAL_BLOCKS);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->numOfBlocks = val;
  return pOptions;
}

static SDatabaseOptions* setDbCache(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_CACHE_BLOCK_SIZE || val > TSDB_MAX_CACHE_BLOCK_SIZE) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option cacheBlockSize: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MAX_CACHE_BLOCK_SIZE);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->cacheBlockSize = val;
  return pOptions;
}

static SDatabaseOptions* setDbCacheLast(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DB_CACHE_LAST_ROW || val > TSDB_MAX_DB_CACHE_LAST_ROW) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option cacheLast: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_DB_CACHE_LAST_ROW, TSDB_MAX_DB_CACHE_LAST_ROW);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->cachelast = val;
  return pOptions;
}

static SDatabaseOptions* setDbComp(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_COMP_LEVEL || val > TSDB_MAX_COMP_LEVEL) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option compression: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->compressionLevel = val;
  return pOptions;
}

static SDatabaseOptions* setDbDays(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DAYS_PER_FILE || val > TSDB_MAX_DAYS_PER_FILE) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option daysPerFile: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->daysPerFile = val;
  return pOptions;
}

static SDatabaseOptions* setDbFsync(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_FSYNC_PERIOD || val > TSDB_MAX_FSYNC_PERIOD) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option fsyncPeriod: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->fsyncPeriod = val;
  return pOptions;
}

static SDatabaseOptions* setDbMaxRows(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_MAX_ROW_FBLOCK || val > TSDB_MAX_MAX_ROW_FBLOCK) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option maxRowsPerBlock: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_MAX_ROW_FBLOCK, TSDB_MAX_MAX_ROW_FBLOCK);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->maxRowsPerBlock = val;
  return pOptions;
}

static SDatabaseOptions* setDbMinRows(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_MIN_ROW_FBLOCK || val > TSDB_MAX_MIN_ROW_FBLOCK) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option minRowsPerBlock: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_MIN_ROW_FBLOCK, TSDB_MAX_MIN_ROW_FBLOCK);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->minRowsPerBlock = val;
  return pOptions;
}

static SDatabaseOptions* setDbKeep(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_KEEP || val > TSDB_MAX_KEEP) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option keep: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_KEEP, TSDB_MAX_KEEP);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->keep = val;
  return pOptions;
}

static SDatabaseOptions* setDbPrecision(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  char val[10] = {0};
  trimString(pVal->z, pVal->n, val, sizeof(val));
  if (0 == strcmp(val, TSDB_TIME_PRECISION_MILLI_STR)) {
    pOptions->precision = TSDB_TIME_PRECISION_MILLI;
  } else if (0 == strcmp(val, TSDB_TIME_PRECISION_MICRO_STR)) {
    pOptions->precision = TSDB_TIME_PRECISION_MICRO;
  } else if (0 == strcmp(val, TSDB_TIME_PRECISION_NANO_STR)) {
    pOptions->precision = TSDB_TIME_PRECISION_NANO;
  } else {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid db option precision: %s", val);
    pCxt->valid = false;
  }
  return pOptions;
}

static SDatabaseOptions* setDbQuorum(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DB_QUORUM_OPTION || val > TSDB_MAX_DB_QUORUM_OPTION) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option quorum: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_DB_QUORUM_OPTION, TSDB_MAX_DB_QUORUM_OPTION);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->quorum = val;
  return pOptions;
}

static SDatabaseOptions* setDbReplica(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DB_REPLICA_OPTION || val > TSDB_MAX_DB_REPLICA_OPTION) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option replications: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_DB_REPLICA_OPTION, TSDB_MAX_DB_REPLICA_OPTION);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->replica = val;
  return pOptions;
}

static SDatabaseOptions* setDbTtl(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DB_TTL_OPTION) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option ttl: %"PRId64", should be greater than or equal to %d", val, TSDB_MIN_DB_TTL_OPTION);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->ttl = val;
  return pOptions;
}

static SDatabaseOptions* setDbWal(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_WAL_LEVEL || val > TSDB_MAX_WAL_LEVEL) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid db option walLevel: %"PRId64", only 1-2 allowed", val);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->walLevel = val;
  return pOptions;
}

static SDatabaseOptions* setDbVgroups(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_VNODES_PER_DB || val > TSDB_MAX_VNODES_PER_DB) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid db option vgroups: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_VNODES_PER_DB, TSDB_MAX_VNODES_PER_DB);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->numOfVgroups = val;
  return pOptions;
}

static SDatabaseOptions* setDbSingleStable(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DB_SINGLE_STABLE_OPTION || val > TSDB_MAX_DB_SINGLE_STABLE_OPTION) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid db option singleStable: %"PRId64", only 0-1 allowed", val);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->singleStable = val;
  return pOptions;
}

static SDatabaseOptions* setDbStreamMode(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DB_STREAM_MODE_OPTION || val > TSDB_MAX_DB_STREAM_MODE_OPTION) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid db option streamMode: %"PRId64", only 0-1 allowed", val);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->streamMode = val;
  return pOptions;
}

static SDatabaseOptions* setDbRetentions(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  // todo
  return pOptions;
}

static SDatabaseOptions* setDbFileFactor(SAstCreateContext* pCxt, SDatabaseOptions* pOptions, const SToken* pVal) {
  // todo
  return pOptions;
}

static void initSetDatabaseOptionFp() {
  setDbOptionFuncs[DB_OPTION_BLOCKS] = setDbBlocks;
  setDbOptionFuncs[DB_OPTION_CACHE] = setDbCache;
  setDbOptionFuncs[DB_OPTION_CACHELAST] = setDbCacheLast;
  setDbOptionFuncs[DB_OPTION_COMP] = setDbComp;
  setDbOptionFuncs[DB_OPTION_DAYS] = setDbDays;
  setDbOptionFuncs[DB_OPTION_FSYNC] = setDbFsync;
  setDbOptionFuncs[DB_OPTION_MAXROWS] = setDbMaxRows;
  setDbOptionFuncs[DB_OPTION_MINROWS] = setDbMinRows;
  setDbOptionFuncs[DB_OPTION_KEEP] = setDbKeep;
  setDbOptionFuncs[DB_OPTION_PRECISION] = setDbPrecision;
  setDbOptionFuncs[DB_OPTION_QUORUM] = setDbQuorum;
  setDbOptionFuncs[DB_OPTION_REPLICA] = setDbReplica;
  setDbOptionFuncs[DB_OPTION_TTL] = setDbTtl;
  setDbOptionFuncs[DB_OPTION_WAL] = setDbWal;
  setDbOptionFuncs[DB_OPTION_VGROUPS] = setDbVgroups;
  setDbOptionFuncs[DB_OPTION_SINGLE_STABLE] = setDbSingleStable;
  setDbOptionFuncs[DB_OPTION_STREAM_MODE] = setDbStreamMode;
  setDbOptionFuncs[DB_OPTION_RETENTIONS] = setDbRetentions;
  setDbOptionFuncs[DB_OPTION_FILE_FACTOR] = setDbFileFactor;
}

static STableOptions* setTableKeep(SAstCreateContext* pCxt, STableOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_KEEP || val > TSDB_MAX_KEEP) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid table option keep: %"PRId64" valid range: [%d, %d]", val, TSDB_MIN_KEEP, TSDB_MAX_KEEP);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->keep = val;
  return pOptions;
}

static STableOptions* setTableTtl(SAstCreateContext* pCxt, STableOptions* pOptions, const SToken* pVal) {
  int64_t val = strtol(pVal->z, NULL, 10);
  if (val < TSDB_MIN_DB_TTL_OPTION) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid table option ttl: %"PRId64", should be greater than or equal to %d", val, TSDB_MIN_DB_TTL_OPTION);
    pCxt->valid = false;
    return pOptions;
  }
  pOptions->ttl = val;
  return pOptions;
}

static STableOptions* setTableComment(SAstCreateContext* pCxt, STableOptions* pOptions, const SToken* pVal) {
  if (pVal->n >= sizeof(pOptions->comments)) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid table option comment, length cannot exceed %d", (int32_t)(sizeof(pOptions->comments) - 1));
    pCxt->valid = false;
    return pOptions;
  }
  trimString(pVal->z, pVal->n, pOptions->comments, sizeof(pOptions->comments));
  return pOptions;
}

static void initSetTableOptionFp() {
  setTableOptionFuncs[TABLE_OPTION_KEEP] = setTableKeep;
  setTableOptionFuncs[TABLE_OPTION_TTL] = setTableTtl;
  setTableOptionFuncs[TABLE_OPTION_COMMENT] = setTableComment;
}

void initAstCreateContext(SParseContext* pParseCxt, SAstCreateContext* pCxt) {
  pCxt->pQueryCxt = pParseCxt;
  pCxt->msgBuf.buf = pParseCxt->pMsg;
  pCxt->msgBuf.len = pParseCxt->msgLen;
  pCxt->notSupport = false;
  pCxt->valid = true;
  pCxt->pRootNode = NULL;
  initSetDatabaseOptionFp();
  initSetTableOptionFp();
}

static bool checkUserName(SAstCreateContext* pCxt, const SToken* pUserName) {
  if (NULL == pUserName) {
    return false;
  }
  if (pUserName->n >= TSDB_USER_LEN) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    pCxt->valid = false;
  }
  return pCxt->valid;
}

static bool checkPassword(SAstCreateContext* pCxt, const SToken* pPasswordToken, char* pPassword) {
  if (NULL == pPasswordToken) {
    return false;
  }
  if (pPasswordToken->n >= (TSDB_USET_PASSWORD_LEN - 2)) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    pCxt->valid = false;
    return false;
  }
  strncpy(pPassword, pPasswordToken->z, pPasswordToken->n);
  strdequote(pPassword);
  if (strtrim(pPassword) <= 0) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_PASSWD_EMPTY);
    pCxt->valid = false;
  }
  return pCxt->valid;
}

static bool checkAndSplitEndpoint(SAstCreateContext* pCxt, const SToken* pEp, char* pFqdn, int32_t* pPort) {
  if (NULL == pEp) {
    return false;
  }
  if (pEp->n >= TSDB_FQDN_LEN + 2 + 6) { // format 'fqdn:port'
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    pCxt->valid = false;
  }
  char ep[TSDB_FQDN_LEN + 2 + 6];
  strncpy(ep, pEp->z, pEp->n);
  strdequote(ep);
  strtrim(ep);
  char* pColon = strchr(ep, ':');
  if (NULL == pColon) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ENDPOINT);
    pCxt->valid = false;
  }
  strncpy(pFqdn, ep, pColon - ep);
  *pPort = strtol(pColon + 1, NULL, 10);
  if (*pPort >= UINT16_MAX || *pPort <= 0) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PORT);
    pCxt->valid = false;
  }
  return pCxt->valid;
}

static bool checkFqdn(SAstCreateContext* pCxt, const SToken* pFqdn) {
  if (NULL == pFqdn) {
    return false;
  }
  if (pFqdn->n >= TSDB_FQDN_LEN) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    pCxt->valid = false;
  }
  return pCxt->valid;
}

static bool checkPort(SAstCreateContext* pCxt, const SToken* pPortToken, int32_t* pPort) {
  if (NULL == pPortToken) {
    return false;
  }
  *pPort = strtol(pPortToken->z, NULL, 10);
  if (*pPort >= UINT16_MAX || *pPort <= 0) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PORT);
    pCxt->valid = false;
  }
  return pCxt->valid;
}

static bool checkDbName(SAstCreateContext* pCxt, const SToken* pDbName) {
  if (NULL == pDbName) {
    return true;
  }
  pCxt->valid = pDbName->n < TSDB_DB_NAME_LEN ? true : false;
  return pCxt->valid;
}

static bool checkTableName(SAstCreateContext* pCxt, const SToken* pTableName) {
  if (NULL == pTableName) {
    return true;
  }
  pCxt->valid = pTableName->n < TSDB_TABLE_NAME_LEN ? true : false;
  return pCxt->valid;
}

static bool checkColumnName(SAstCreateContext* pCxt, const SToken* pColumnName) {
  if (NULL == pColumnName) {
    return true;
  }
  pCxt->valid = pColumnName->n < TSDB_COL_NAME_LEN ? true : false;
  return pCxt->valid;
}

static bool checkIndexName(SAstCreateContext* pCxt, const SToken* pIndexName) {
  if (NULL == pIndexName) {
    return false;
  }
  pCxt->valid = pIndexName->n < TSDB_INDEX_NAME_LEN ? true : false;
  return pCxt->valid;
}

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode) {
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pToken->z;
  target->n = pToken->n;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode) {
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pStart->z;
  target->n = (pEnd->z + pEnd->n) - pStart->z;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_RAW_EXPR_NODE(pNode);
  SNode* tmp = ((SRawExprNode*)pNode)->pNode;
  tfree(pNode);
  return tmp;
}

SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_RAW_EXPR != nodeType(pNode)) {
    pCxt->valid = false;
    return nil_token;
  }
  SRawExprNode* target = (SRawExprNode*)pNode;
  SToken t = { .type = 0, .z = target->p, .n = target->n};
  return t;
}

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {
  SNodeList* list = nodesMakeList();
  CHECK_OUT_OF_MEM(list);
  if (TSDB_CODE_SUCCESS != nodesListAppend(list, pNode)) {
    pCxt->valid = false;
  }
  return list;
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  if (TSDB_CODE_SUCCESS != nodesListAppend(pList, pNode)) {
    pCxt->valid = false;
  }
  return pList;
}

SNode* createColumnNode(SAstCreateContext* pCxt, const SToken* pTableAlias, const SToken* pColumnName) {
  if (!checkTableName(pCxt, pTableAlias) || !checkColumnName(pCxt, pColumnName)) {
    return NULL;
  }
  SColumnNode* col = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_OUT_OF_MEM(col);
  if (NULL != pTableAlias) {
    strncpy(col->tableAlias, pTableAlias->z, pTableAlias->n);
  }
  strncpy(col->colName, pColumnName->z, pColumnName->n);
  return (SNode*)col;
}

SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  CHECK_OUT_OF_MEM(val->literal);
  val->node.resType.type = dataType;
  val->node.resType.bytes = tDataTypes[dataType].bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
  val->isDuration = false;
  val->translate = false;
  return (SNode*)val;
}

SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  CHECK_OUT_OF_MEM(val->literal);
  val->isDuration = true;
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  val->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
}

SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2) {
  SLogicConditionNode* cond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  CHECK_OUT_OF_MEM(cond);
  cond->condType = type;
  cond->pParameterList = nodesMakeList();
  nodesListAppend(cond->pParameterList, pParam1);
  nodesListAppend(cond->pParameterList, pParam2);
  return (SNode*)cond;
}

SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight) {
  SOperatorNode* op = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  CHECK_OUT_OF_MEM(op);
  op->opType = type;
  op->pLeft = pLeft;
  op->pRight = pRight;
  return (SNode*)op;
}

SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND,
      createOperatorNode(pCxt, OP_TYPE_GREATER_EQUAL, pExpr, pLeft), createOperatorNode(pCxt, OP_TYPE_LOWER_EQUAL, nodesCloneNode(pExpr), pRight));
}

SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR,
      createOperatorNode(pCxt, OP_TYPE_LOWER_THAN, pExpr, pLeft), createOperatorNode(pCxt, OP_TYPE_GREATER_THAN, nodesCloneNode(pExpr), pRight));
}

SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList) {
  SFunctionNode* func = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(func);
  strncpy(func->functionName, pFuncName->z, pFuncName->n);
  func->pParameterList = pParameterList;
  return (SNode*)func;
}

SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList) {
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = pList;
  return (SNode*)list;
}

SNode* createRealTableNode(SAstCreateContext* pCxt, const SToken* pDbName, const SToken* pTableName, const SToken* pTableAlias) {
  if (!checkDbName(pCxt, pDbName) || !checkTableName(pCxt, pTableName)) {
    return NULL;
  }
  SRealTableNode* realTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  CHECK_OUT_OF_MEM(realTable);
  if (NULL != pDbName) {
    strncpy(realTable->table.dbName, pDbName->z, pDbName->n);
  } else {
    strcpy(realTable->table.dbName, pCxt->pQueryCxt->db);
  }
  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    strncpy(realTable->table.tableAlias, pTableAlias->z, pTableAlias->n);
  } else {
    strncpy(realTable->table.tableAlias, pTableName->z, pTableName->n);
  }
  strncpy(realTable->table.tableName, pTableName->z, pTableName->n);
  return (SNode*)realTable;
}

SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias) {
  STempTableNode* tempTable = (STempTableNode*)nodesMakeNode(QUERY_NODE_TEMP_TABLE);
  CHECK_OUT_OF_MEM(tempTable);
  tempTable->pSubquery = pSubquery;
  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    strncpy(tempTable->table.tableAlias, pTableAlias->z, pTableAlias->n);
  }
  return (SNode*)tempTable;
}

SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond) {
  SJoinTableNode* joinTable = (SJoinTableNode*)nodesMakeNode(QUERY_NODE_JOIN_TABLE);
  CHECK_OUT_OF_MEM(joinTable);
  joinTable->joinType = type;
  joinTable->pLeft = pLeft;
  joinTable->pRight = pRight;
  joinTable->pOnCond = pJoinCond;
  return (SNode*)joinTable;
}

SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset) {
  SLimitNode* limitNode = (SLimitNode*)nodesMakeNode(QUERY_NODE_LIMIT);
  CHECK_OUT_OF_MEM(limitNode);
  // limitNode->limit = limit;
  // limitNode->offset = offset;
  return (SNode*)limitNode;
}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {
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

SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, const SToken* pVal) {
  SSessionWindowNode* session = (SSessionWindowNode*)nodesMakeNode(QUERY_NODE_SESSION_WINDOW);
  CHECK_OUT_OF_MEM(session);
  session->pCol = pCol;
  // session->gap = getInteger(pVal);
  return (SNode*)session;
}

SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pCol) {
  SStateWindowNode* state = (SStateWindowNode*)nodesMakeNode(QUERY_NODE_STATE_WINDOW);
  CHECK_OUT_OF_MEM(state);
  state->pCol = pCol;
  return (SNode*)state;
}

SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding, SNode* pFill) {
  SIntervalWindowNode* interval = (SIntervalWindowNode*)nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  CHECK_OUT_OF_MEM(interval);
  interval->pInterval = pInterval;
  interval->pOffset = pOffset;
  interval->pSliding = pSliding;
  interval->pFill = pFill;
  return (SNode*)interval;
}

SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues) {
  SFillNode* fill = (SFillNode*)nodesMakeNode(QUERY_NODE_FILL);
  CHECK_OUT_OF_MEM(fill);
  fill->mode = mode;
  fill->pValues = pValues;
  return (SNode*)fill;
}

SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode) {
  SGroupingSetNode* groupingSet = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
  CHECK_OUT_OF_MEM(groupingSet);
  groupingSet->groupingSetType = GP_TYPE_NORMAL;
  groupingSet->pParameterList = nodesMakeList();
  nodesListAppend(groupingSet->pParameterList, pNode);
  return (SNode*)groupingSet;
}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias) {
  if (NULL == pNode || !pCxt->valid) {
    return pNode;
  }
  uint32_t maxLen = sizeof(((SExprNode*)pNode)->aliasName);
  strncpy(((SExprNode*)pNode)->aliasName, pAlias->z, pAlias->n > maxLen ? maxLen : pAlias->n);
  return pNode;
}

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWhere = pWhere;
  }
  return pStmt;
}

SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pPartitionByList = pPartitionByList;
  }
  return pStmt;
}

SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWindow = pWindow;
  }
  return pStmt;
}

SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pGroupByList = pGroupByList;
  }
  return pStmt;
}

SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pHaving = pHaving;
  }
  return pStmt;
}

SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pOrderByList = pOrderByList;
  }
  return pStmt;
}

SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pSlimit = pSlimit;
  }
  return pStmt;
}

SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pLimit = pLimit;
  }
  return pStmt;
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable) {
  SSelectStmt* select = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  CHECK_OUT_OF_MEM(select);
  select->isDistinct = isDistinct;
  select->pProjectionList = pProjectionList;
  select->pFromTable = pTable;
  return (SNode*)select;
}

SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {
  SSetOperator* setOp = (SSetOperator*)nodesMakeNode(QUERY_NODE_SET_OPERATOR);
  CHECK_OUT_OF_MEM(setOp);
  setOp->opType = type;
  setOp->pLeft = pLeft;
  setOp->pRight = pRight;
  return (SNode*)setOp;
}

SNode* createDefaultDatabaseOptions(SAstCreateContext* pCxt) {
  SDatabaseOptions* pOptions = nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->numOfBlocks = TSDB_DEFAULT_TOTAL_BLOCKS;
  pOptions->cacheBlockSize = TSDB_DEFAULT_CACHE_BLOCK_SIZE;
  pOptions->cachelast = TSDB_DEFAULT_CACHE_LAST_ROW;
  pOptions->compressionLevel = TSDB_DEFAULT_COMP_LEVEL;
  pOptions->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  pOptions->fsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
  pOptions->maxRowsPerBlock = TSDB_DEFAULT_MAX_ROW_FBLOCK;
  pOptions->minRowsPerBlock = TSDB_DEFAULT_MIN_ROW_FBLOCK;
  pOptions->keep = TSDB_DEFAULT_KEEP;
  pOptions->precision = TSDB_TIME_PRECISION_MILLI;
  pOptions->quorum = TSDB_DEFAULT_DB_QUORUM_OPTION;
  pOptions->replica = TSDB_DEFAULT_DB_REPLICA_OPTION;
  pOptions->ttl = TSDB_DEFAULT_DB_TTL_OPTION;
  pOptions->walLevel = TSDB_DEFAULT_WAL_LEVEL;
  pOptions->numOfVgroups = TSDB_DEFAULT_VN_PER_DB;
  pOptions->singleStable = TSDB_DEFAULT_DB_SINGLE_STABLE_OPTION;
  pOptions->streamMode = TSDB_DEFAULT_DB_STREAM_MODE_OPTION;
  return (SNode*)pOptions;
}

SNode* createDefaultAlterDatabaseOptions(SAstCreateContext* pCxt) {
  SDatabaseOptions* pOptions = nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->numOfBlocks = -1;
  pOptions->cacheBlockSize = -1;
  pOptions->cachelast = -1;
  pOptions->compressionLevel = -1;
  pOptions->daysPerFile = -1;
  pOptions->fsyncPeriod = -1;
  pOptions->maxRowsPerBlock = -1;
  pOptions->minRowsPerBlock = -1;
  pOptions->keep = -1;
  pOptions->precision = -1;
  pOptions->quorum = -1;
  pOptions->replica = -1;
  pOptions->ttl = -1;
  pOptions->walLevel = -1;
  pOptions->numOfVgroups = -1;
  pOptions->singleStable = -1;
  pOptions->streamMode = -1;
  return (SNode*)pOptions;
}

SNode* setDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, EDatabaseOptionType type, const SToken* pVal) {
  return (SNode*)setDbOptionFuncs[type](pCxt, (SDatabaseOptions*)pOptions, pVal);
}

SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, const SToken* pDbName, SNode* pOptions) {
  if (!checkDbName(pCxt, pDbName)) {
    return NULL;
  }
  SCreateDatabaseStmt* pStmt = (SCreateDatabaseStmt*)nodesMakeNode(QUERY_NODE_CREATE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createDropDatabaseStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pDbName) {
  if (!checkDbName(pCxt, pDbName)) {
    return NULL;
  }
  SDropDatabaseStmt* pStmt = (SDropDatabaseStmt*)nodesMakeNode(QUERY_NODE_DROP_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createAlterDatabaseStmt(SAstCreateContext* pCxt, const SToken* pDbName, SNode* pOptions) {
  if (!checkDbName(pCxt, pDbName)) {
    return NULL;
  }
  SAlterDatabaseStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createDefaultTableOptions(SAstCreateContext* pCxt) {
  STableOptions* pOptions = nodesMakeNode(QUERY_NODE_TABLE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->keep = TSDB_DEFAULT_KEEP;
  pOptions->ttl = TSDB_DEFAULT_DB_TTL_OPTION;
  return (SNode*)pOptions;
}

SNode* createDefaultAlterTableOptions(SAstCreateContext* pCxt) {
  STableOptions* pOptions = nodesMakeNode(QUERY_NODE_TABLE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->keep = -1;
  pOptions->ttl = -1;
  return (SNode*)pOptions;
}

SNode* setTableOption(SAstCreateContext* pCxt, SNode* pOptions, ETableOptionType type, const SToken* pVal) {
  return (SNode*)setTableOptionFuncs[type](pCxt, (STableOptions*)pOptions, pVal);
}

SNode* setTableSmaOption(SAstCreateContext* pCxt, SNode* pOptions, SNodeList* pSma) {
  ((STableOptions*)pOptions)->pSma = pSma;
  return pOptions;
}

SNode* setTableRollupOption(SAstCreateContext* pCxt, SNode* pOptions, SNodeList* pFuncs) {
  // todo
  return pOptions;
}

SNode* createColumnDefNode(SAstCreateContext* pCxt, const SToken* pColName, SDataType dataType, const SToken* pComment) {
  SColumnDefNode* pCol = (SColumnDefNode*)nodesMakeNode(QUERY_NODE_COLUMN_DEF);
  CHECK_OUT_OF_MEM(pCol);
  strncpy(pCol->colName, pColName->z, pColName->n);
  pCol->dataType = dataType;
  if (NULL != pComment) {
    trimString(pComment->z, pComment->n, pCol->comments, sizeof(pCol->comments));
  }
  return (SNode*)pCol;
}

SDataType createDataType(uint8_t type) {
  SDataType dt = { .type = type, .precision = 0, .scale = 0, .bytes = tDataTypes[type].bytes };
  return dt;
}

SDataType createVarLenDataType(uint8_t type, const SToken* pLen) {
  SDataType dt = { .type = type, .precision = 0, .scale = 0, .bytes = strtol(pLen->z, NULL, 10) };
  return dt;
}

SNode* createCreateTableStmt(SAstCreateContext* pCxt,
    bool ignoreExists, SNode* pRealTable, SNodeList* pCols, SNodeList* pTags, SNode* pOptions) {
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

SNode* createCreateSubTableClause(SAstCreateContext* pCxt,
    bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable, SNodeList* pSpecificTags, SNodeList* pValsOfTags) {
  SCreateSubTableClause* pStmt = nodesMakeNode(QUERY_NODE_CREATE_SUBTABLE_CLAUSE);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  strcpy(pStmt->useDbName, ((SRealTableNode*)pUseRealTable)->table.dbName);
  strcpy(pStmt->useTableName, ((SRealTableNode*)pUseRealTable)->table.tableName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pSpecificTags = pSpecificTags;
  pStmt->pValsOfTags = pValsOfTags;
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pUseRealTable);
  return (SNode*)pStmt;
}

SNode* createCreateMultiTableStmt(SAstCreateContext* pCxt, SNodeList* pSubTables) {
  SCreateMultiTableStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_MULTI_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pSubTables = pSubTables;
  return (SNode*)pStmt;
}

SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  SDropTableClause* pStmt = nodesMakeNode(QUERY_NODE_DROP_TABLE_CLAUSE);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createDropTableStmt(SAstCreateContext* pCxt, SNodeList* pTables) {
  SDropTableStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pTables = pTables;
  return (SNode*)pStmt;
}

SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  SDropSuperTableStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_SUPER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createAlterTableOption(SAstCreateContext* pCxt, SNode* pRealTable, SNode* pOptions) {
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_OPTIONS;
  pStmt->pOptions = (STableOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createAlterTableAddModifyCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pColName, SDataType dataType) {
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pColName->z, pColName->n);
  pStmt->dataType = dataType;
  return (SNode*)pStmt;
}

SNode* createAlterTableDropCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pColName) {
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pColName->z, pColName->n);
  return (SNode*)pStmt;
}

SNode* createAlterTableRenameCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pOldColName, const SToken* pNewColName) {
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pOldColName->z, pOldColName->n);
  strncpy(pStmt->newColName, pNewColName->z, pNewColName->n);
  return (SNode*)pStmt;
}

SNode* createAlterTableSetTag(SAstCreateContext* pCxt, SNode* pRealTable, const SToken* pTagName, SNode* pVal) {
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
  strncpy(pStmt->colName, pTagName->z, pTagName->n);
  pStmt->pVal = (SValueNode*)pVal;
  return (SNode*)pStmt;
}

SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  SUseDatabaseStmt* pStmt = (SUseDatabaseStmt*)nodesMakeNode(QUERY_NODE_USE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  return (SNode*)pStmt;
}

SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDbName) {
  if (!checkDbName(pCxt, pDbName)) {
    return NULL;
  }
  SShowStmt* pStmt = nodesMakeNode(type);;
  CHECK_OUT_OF_MEM(pStmt);
  if (NULL != pDbName) {
    strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  } else if (NULL != pCxt->pQueryCxt->db) {
    strcpy(pStmt->dbName, pCxt->pQueryCxt->db);
  }
  return (SNode*)pStmt;
}

SNode* createCreateUserStmt(SAstCreateContext* pCxt, const SToken* pUserName, const SToken* pPassword) {
  char password[TSDB_USET_PASSWORD_LEN] = {0};
  if (!checkUserName(pCxt, pUserName) || !checkPassword(pCxt, pPassword, password)) {
    return NULL;
  }
  SCreateUserStmt* pStmt = (SCreateUserStmt*)nodesMakeNode(QUERY_NODE_CREATE_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->useName, pUserName->z, pUserName->n);
  strcpy(pStmt->password, password);
  return (SNode*)pStmt;
}

SNode* createAlterUserStmt(SAstCreateContext* pCxt, const SToken* pUserName, int8_t alterType, const SToken* pVal) {
  if (!checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SAlterUserStmt* pStmt = (SAlterUserStmt*)nodesMakeNode(QUERY_NODE_ALTER_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->useName, pUserName->z, pUserName->n);
  if (TSDB_ALTER_USER_PASSWD == alterType) {
    char password[TSDB_USET_PASSWORD_LEN] = {0};
    if (!checkPassword(pCxt, pVal, password)) {
      nodesDestroyNode(pStmt);
      return NULL;
    }
    strcpy(pStmt->password, password);
  }
  pStmt->alterType = alterType;
  return (SNode*)pStmt;
}

SNode* createDropUserStmt(SAstCreateContext* pCxt, const SToken* pUserName) {
  if (!checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SDropUserStmt* pStmt = (SDropUserStmt*)nodesMakeNode(QUERY_NODE_DROP_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->useName, pUserName->z, pUserName->n);
  return (SNode*)pStmt;
}

SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort) {
  int32_t port = 0;
  char fqdn[TSDB_FQDN_LEN] = {0};
  if (NULL == pPort) {
    if (!checkAndSplitEndpoint(pCxt, pFqdn, fqdn, &port)) {
      return NULL;
    }
  } else if (!checkFqdn(pCxt, pFqdn) || !checkPort(pCxt, pPort, &port)) {
    return NULL;
  }
  SCreateDnodeStmt* pStmt = (SCreateDnodeStmt*)nodesMakeNode(QUERY_NODE_CREATE_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (NULL == pPort) {
    strcpy(pStmt->fqdn, fqdn);
  } else {
    strncpy(pStmt->fqdn, pFqdn->z, pFqdn->n);
  }
  pStmt->port = port;
  return (SNode*)pStmt;
}

SNode* createDropDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode) {
  SDropDnodeStmt* pStmt = (SDropDnodeStmt*)nodesMakeNode(QUERY_NODE_DROP_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (TK_NK_INTEGER == pDnode->type) {
    pStmt->dnodeId = strtol(pDnode->z, NULL, 10);
  } else {
    if (!checkAndSplitEndpoint(pCxt, pDnode, pStmt->fqdn, &pStmt->port)) {
      nodesDestroyNode(pStmt);
      return NULL;
    }
  }
  return (SNode*)pStmt;
}

SNode* createAlterDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, const SToken* pConfig, const SToken* pValue) {
  SAlterDnodeStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = strtol(pDnode->z, NULL, 10);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}

SNode* createCreateIndexStmt(SAstCreateContext* pCxt, EIndexType type, const SToken* pIndexName, const SToken* pTableName, SNodeList* pCols, SNode* pOptions) {
  if (!checkIndexName(pCxt, pIndexName) || !checkTableName(pCxt, pTableName)) {
    return NULL;
  }
  SCreateIndexStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_INDEX_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->indexType = type;
  strncpy(pStmt->indexName, pIndexName->z, pIndexName->n);
  strncpy(pStmt->tableName, pTableName->z, pTableName->n);
  pStmt->pCols = pCols;
  pStmt->pOptions = (SIndexOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset, SNode* pSliding) {
  SIndexOptions* pOptions = nodesMakeNode(QUERY_NODE_INDEX_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->pFuncs = pFuncs;
  pOptions->pInterval = pInterval;
  pOptions->pOffset = pOffset;
  pOptions->pSliding = pSliding;
  return (SNode*)pOptions;
}

SNode* createDropIndexStmt(SAstCreateContext* pCxt, const SToken* pIndexName, const SToken* pTableName) {
  if (!checkIndexName(pCxt, pIndexName) || !checkTableName(pCxt, pTableName)) {
    return NULL;
  }
  SDropIndexStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_INDEX_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->indexName, pIndexName->z, pIndexName->n);
  strncpy(pStmt->tableName, pTableName->z, pTableName->n);
  return (SNode*)pStmt;
}

SNode* createCreateQnodeStmt(SAstCreateContext* pCxt, const SToken* pDnodeId) {
  SCreateQnodeStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_QNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = strtol(pDnodeId->z, NULL, 10);;
  return (SNode*)pStmt;
}

SNode* createDropQnodeStmt(SAstCreateContext* pCxt, const SToken* pDnodeId) {
  SDropQnodeStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_QNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = strtol(pDnodeId->z, NULL, 10);;
  return (SNode*)pStmt;
}

SNode* createCreateTopicStmt(SAstCreateContext* pCxt, bool ignoreExists, const SToken* pTopicName, SNode* pQuery, const SToken* pSubscribeDbName) {
  SCreateTopicStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->topicName, pTopicName->z, pTopicName->n);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pQuery = pQuery;
  if (NULL != pSubscribeDbName) {
    strncpy(pStmt->subscribeDbName, pSubscribeDbName->z, pSubscribeDbName->n);
  }
  return (SNode*)pStmt;
}

SNode* createDropTopicStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pTopicName) {
  SDropTopicStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->topicName, pTopicName->z, pTopicName->n);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createAlterLocalStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue) {
  SAlterLocalStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_LOCAL_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}
