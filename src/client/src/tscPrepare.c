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
#include "os.h"

#include "taos.h"
#include "tsclient.h"
#include "tscUtil.h"
#include "ttimer.h"
#include "taosmsg.h"
#include "tstrbuild.h"
#include "tscLog.h"
#include "tscSubquery.h"

int tsParseInsertSql(SSqlObj *pSql);
int taos_query_imp(STscObj* pObj, SSqlObj* pSql);

////////////////////////////////////////////////////////////////////////////////
// functions for normal statement preparation

typedef struct SNormalStmtPart {
  bool  isParam;
  uint32_t len;
  char* str;
} SNormalStmtPart;

typedef struct SNormalStmt {
  uint16_t         sizeParts;
  uint16_t         numParts;
  uint16_t         numParams;
  char* sql;
  SNormalStmtPart* parts;
  tVariant*        params;
} SNormalStmt;

//typedef struct SInsertStmt {
//
//} SInsertStmt;

typedef struct STscStmt {
  bool isInsert;
  STscObj* taos;
  SSqlObj* pSql;
  SNormalStmt normal;
} STscStmt;


static int normalStmtAddPart(SNormalStmt* stmt, bool isParam, char* str, uint32_t len) {
  uint16_t size = stmt->numParts + 1;
  if (size > stmt->sizeParts) {
    size *= 2;
    void* tmp = realloc(stmt->parts, sizeof(SNormalStmtPart) * size);
    if (tmp == NULL) {
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }
    stmt->sizeParts = size;
    stmt->parts = (SNormalStmtPart*)tmp;
  }

  stmt->parts[stmt->numParts].isParam = isParam;
  stmt->parts[stmt->numParts].str = str;
  stmt->parts[stmt->numParts].len = len;

  ++stmt->numParts;
  if (isParam) {
    ++stmt->numParams;
  }
  return TSDB_CODE_SUCCESS;
}

static int normalStmtBindParam(STscStmt* stmt, TAOS_BIND* bind) {
  SNormalStmt* normal = &stmt->normal;

  for (uint16_t i = 0; i < normal->numParams; ++i) {
    TAOS_BIND* tb = bind + i;
    tVariant* var = normal->params + i;
    tVariantDestroy(var);

    var->nLen = 0;
    if (tb->is_null != NULL && *(tb->is_null)) {
      var->nType = TSDB_DATA_TYPE_NULL;
      var->i64Key = 0;
      continue;
    }

    var->nType = tb->buffer_type;
    switch (tb->buffer_type) {
      case TSDB_DATA_TYPE_NULL:
        var->i64Key = 0;
        break;

      case TSDB_DATA_TYPE_BOOL:
        var->i64Key = (*(int8_t*)tb->buffer) ? 1 : 0;
        break;

      case TSDB_DATA_TYPE_TINYINT:
        var->i64Key = *(int8_t*)tb->buffer;
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        var->i64Key = *(int16_t*)tb->buffer;
        break;

      case TSDB_DATA_TYPE_INT:
        var->i64Key = *(int32_t*)tb->buffer;
        break;

      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        var->i64Key = *(int64_t*)tb->buffer;
        break;

      case TSDB_DATA_TYPE_FLOAT:
        var->dKey = GET_FLOAT_VAL(tb->buffer);
        break;

      case TSDB_DATA_TYPE_DOUBLE:
        var->dKey = GET_DOUBLE_VAL(tb->buffer);
        break;

      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR:
        var->pz = (char*)malloc((*tb->length) + 1);
        if (var->pz == NULL) {
          return TSDB_CODE_CLI_OUT_OF_MEMORY;
        }
        memcpy(var->pz, tb->buffer, (*tb->length));
        var->pz[*tb->length] = 0;
        var->nLen = (int32_t)(*tb->length);
        break;

      default:
        tscTrace("param %d: type mismatch or invalid", i);
        return TSDB_CODE_INVALID_VALUE;
    }
  }
  
  return TSDB_CODE_SUCCESS;
}


static int normalStmtPrepare(STscStmt* stmt) {
  SNormalStmt* normal = &stmt->normal;
  char* sql = stmt->pSql->sqlstr;
  uint32_t i = 0, start = 0;

  while (sql[i] != 0) {
    SSQLToken token = {0};
    token.n = tSQLGetToken(sql + i, &token.type);

    if (token.type == TK_QUESTION) {
      sql[i] = 0;
      if (i > start) {
        int code = normalStmtAddPart(normal, false, sql + start, i - start);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      int code = normalStmtAddPart(normal, true, NULL, 0);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      start = i + token.n;
    }

    i += token.n;
  }

  if (i > start) {
    int code = normalStmtAddPart(normal, false, sql + start, i - start);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (normal->numParams > 0) {
    normal->params = calloc(normal->numParams, sizeof(tVariant));
    if (normal->params == NULL) {
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static char* normalStmtBuildSql(STscStmt* stmt) {
  SNormalStmt* normal = &stmt->normal;
  SStringBuilder sb; memset(&sb, 0, sizeof(sb));

  if (taosStringBuilderSetJmp(&sb) != 0) {
    taosStringBuilderDestroy(&sb);
    return NULL;
  }

  taosStringBuilderEnsureCapacity(&sb, 4096);
  uint32_t idxParam = 0;

  for(uint16_t i = 0; i < normal->numParts; i++) {
    SNormalStmtPart* part = normal->parts + i;
    if (!part->isParam) {
      taosStringBuilderAppendStringLen(&sb, part->str, part->len);
      continue;
    }

    tVariant* var = normal->params + idxParam++;
    switch (var->nType)
    {
    case TSDB_DATA_TYPE_NULL:
      taosStringBuilderAppendNull(&sb);
      break;
    
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      taosStringBuilderAppendInteger(&sb, var->i64Key);
      break;

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      taosStringBuilderAppendDouble(&sb, var->dKey);
      break;

    case TSDB_DATA_TYPE_BINARY:
      taosStringBuilderAppendChar(&sb, '\'');
      for (char* p = var->pz; *p != 0; ++p) {
        if (*p == '\'' || *p == '"') {
          taosStringBuilderAppendChar(&sb, '\\');
        }
        taosStringBuilderAppendChar(&sb, *p);
      }
      taosStringBuilderAppendChar(&sb, '\'');
      break;

    case TSDB_DATA_TYPE_NCHAR:
      taosStringBuilderAppendChar(&sb, '\'');
      taosStringBuilderAppend(&sb, var->wpz, var->nLen);
      taosStringBuilderAppendChar(&sb, '\'');
      break;

    default:
      assert(false);
      break;
    }
  }

  return taosStringBuilderGetResult(&sb, NULL);
}

////////////////////////////////////////////////////////////////////////////////
// functions for insertion statement preparation

static int doBindParam(char* data, SParamInfo* param, TAOS_BIND* bind) {
  if (bind->is_null != NULL && *(bind->is_null)) {
    if (param->type == TSDB_DATA_TYPE_BINARY || param->type == TSDB_DATA_TYPE_NCHAR) {
      setVardataNull(data + param->offset, param->type);
    } else {
      setNull(data + param->offset, param->type, param->bytes);
    }
    return TSDB_CODE_SUCCESS;
  }

  if (bind->buffer_type != param->type) {
    return TSDB_CODE_INVALID_VALUE;
  }

  short size = 0;
  switch(param->type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
      size = 1;
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      size = 2;
      break;

    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_FLOAT:
      size = 4;
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_TIMESTAMP:
      size = 8;
      break;

    case TSDB_DATA_TYPE_BINARY:
      if ((*bind->length) > param->bytes) {
        return TSDB_CODE_INVALID_VALUE;
      }
      size = (short)*bind->length;
      STR_WITH_SIZE_TO_VARSTR(data + param->offset, bind->buffer, size);
      return TSDB_CODE_SUCCESS;
    
    case TSDB_DATA_TYPE_NCHAR: {
      size_t output = 0;
      if (!taosMbsToUcs4(bind->buffer, *bind->length, varDataVal(data + param->offset), param->bytes - VARSTR_HEADER_SIZE, &output)) {
        return TSDB_CODE_INVALID_VALUE;
      }      
      varDataSetLen(data + param->offset, output);
      return TSDB_CODE_SUCCESS;
    }
    default:
      assert(false);
      return TSDB_CODE_INVALID_VALUE;
  }

  memcpy(data + param->offset, bind->buffer, size);
  return TSDB_CODE_SUCCESS;
}

static int insertStmtBindParam(STscStmt* stmt, TAOS_BIND* bind) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;

  int32_t alloced = 1, binded = 0;
  if (pCmd->batchSize > 0) {
    alloced = (pCmd->batchSize + 1) / 2;
    binded = pCmd->batchSize / 2;
  }

  for (int32_t i = 0; i < pCmd->pDataBlocks->nSize; ++i) {
    STableDataBlocks* pBlock = pCmd->pDataBlocks->pData[i];
    uint32_t          totalDataSize = pBlock->size - sizeof(SSubmitBlk);
    uint32_t          dataSize = totalDataSize / alloced;
    assert(dataSize * alloced == totalDataSize);

    if (alloced == binded) {
      totalDataSize += dataSize + sizeof(SSubmitBlk);
      if (totalDataSize > pBlock->nAllocSize) {
        const double factor = 1.5;
        void* tmp = realloc(pBlock->pData, (uint32_t)(totalDataSize * factor));
        if (tmp == NULL) {
          return TSDB_CODE_CLI_OUT_OF_MEMORY;
        }
        pBlock->pData = (char*)tmp;
        pBlock->nAllocSize = (uint32_t)(totalDataSize * factor);
      }
    }

    char* data = pBlock->pData + sizeof(SSubmitBlk) + dataSize * binded;
    for (uint32_t j = 0; j < pBlock->numOfParams; ++j) {
      SParamInfo* param = pBlock->params + j;
      int code = doBindParam(data, param, bind + param->idx);
      if (code != TSDB_CODE_SUCCESS) {
        tscTrace("param %d: type mismatch or invalid", param->idx);
        return code;
      }
    }
  }

  // actual work of all data blocks is done, update block size and numOfRows.
  // note we don't do this block by block during the binding process, because 
  // we cannot recover if something goes wrong.
  pCmd->batchSize = binded * 2 + 1;

  if (binded < alloced) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < pCmd->pDataBlocks->nSize; ++i) {
    STableDataBlocks* pBlock = pCmd->pDataBlocks->pData[i];

    uint32_t totalDataSize = pBlock->size - sizeof(SSubmitBlk);
    pBlock->size += totalDataSize / alloced;

    SSubmitBlk* pSubmit = (SSubmitBlk*)pBlock->pData;
    pSubmit->numOfRows += pSubmit->numOfRows / alloced;
  }

  return TSDB_CODE_SUCCESS;
}

static int insertStmtAddBatch(STscStmt* stmt) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;
  if ((pCmd->batchSize % 2) == 1) {
    ++pCmd->batchSize;
  }
  return TSDB_CODE_SUCCESS;
}

static int insertStmtReset(STscStmt* pStmt) {
  SSqlCmd* pCmd = &pStmt->pSql->cmd;
  if (pCmd->batchSize > 2) {
    int32_t alloced = (pCmd->batchSize + 1) / 2;
    for (int32_t i = 0; i < pCmd->pDataBlocks->nSize; ++i) {
      STableDataBlocks* pBlock = pCmd->pDataBlocks->pData[i];

      uint32_t totalDataSize = pBlock->size - sizeof(SSubmitBlk);
      pBlock->size = sizeof(SSubmitBlk) + totalDataSize / alloced;

      SSubmitBlk* pSubmit = (SSubmitBlk*)pBlock->pData;
      pSubmit->numOfRows = pSubmit->numOfRows / alloced;
    }
  }
  pCmd->batchSize = 0;
  
  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  pTableMetaInfo->vgroupIndex = 0;
  return TSDB_CODE_SUCCESS;
}

static int insertStmtExecute(STscStmt* stmt) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;
  if (pCmd->batchSize == 0) {
    return TSDB_CODE_INVALID_VALUE;
  }
  if ((pCmd->batchSize % 2) == 1) {
    ++pCmd->batchSize;
  }

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  assert(pCmd->numOfClause == 1);
  
  if (pCmd->pDataBlocks->nSize > 0) {
    // merge according to vgid
    int code = tscMergeTableDataBlocks(stmt->pSql, pCmd->pDataBlocks);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    STableDataBlocks *pDataBlock = pCmd->pDataBlocks->pData[0];
    code = tscCopyDataBlockToPayload(stmt->pSql, pDataBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // set the next sent data vnode index in data block arraylist
    pTableMetaInfo->vgroupIndex = 1;
  } else {
    pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);
  }

  SSqlObj *pSql = stmt->pSql;
  SSqlRes *pRes = &pSql->res;
  pRes->numOfRows = 0;
  pRes->numOfTotal = 0;
  pRes->numOfClauseTotal = 0;
  
  pRes->qhandle = 0;

  pSql->insertType = 0;
  pSql->fetchFp    = waitForQueryRsp;
  pSql->fp         = (void(*)())tscHandleMultivnodeInsert;

  tscDoQuery(pSql);

  // wait for the callback function to post the semaphore
  tsem_wait(&pSql->rspSem);
  return pSql->res.code;

}

////////////////////////////////////////////////////////////////////////////////
// interface functions

TAOS_STMT* taos_stmt_init(TAOS* taos) {
  STscObj* pObj = (STscObj*)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_DISCONNECTED;
    tscError("connection disconnected");
    return NULL;
  }

  STscStmt* pStmt = calloc(1, sizeof(STscStmt));
  if (pStmt == NULL) {
    terrno = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("failed to allocate memory for statement");
    return NULL;
  }
  pStmt->taos = pObj;

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    free(pStmt);
    terrno = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("failed to allocate memory for statement");
    return NULL;
  }

  tsem_init(&pSql->rspSem, 0, 0);
  pSql->signature     = pSql;
  pSql->pTscObj       = pObj;
  //pSql->pTscObj->pSql = pSql;
  pSql->maxRetry      = TSDB_MAX_REPLICA_NUM;

  pStmt->pSql = pSql;
  return pStmt;
}

int taos_stmt_prepare(TAOS_STMT* stmt, const char* sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL || pStmt->taos == NULL || pStmt->pSql == NULL) {
    terrno = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlObj* pSql = pStmt->pSql;
  size_t   sqlLen = strlen(sql);
  
  //doAsyncQuery(pObj, pSql, waitForQueryRsp, taos, sqlstr, sqlLen);
  SSqlCmd *pCmd    = &pSql->cmd;
  SSqlRes *pRes    = &pSql->res;
  pSql->param      = (void*)pSql;
  pSql->fp         = waitForQueryRsp;
  pSql->insertType = TSDB_QUERY_TYPE_STMT_INSERT;
  
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    tscError("%p failed to malloc payload buffer", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }
  
  pSql->sqlstr = realloc(pSql->sqlstr, sqlLen + 1);
  
  if (pSql->sqlstr == NULL) {
    tscError("%p failed to malloc sql string buffer", pSql);
    free(pCmd->payload);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }
  
  pRes->qhandle = 0;
  pRes->numOfRows = 1;
  
  strtolower(pSql->sqlstr, sql);
  tscDump("%p SQL: %s", pSql, pSql->sqlstr);

  if (tscIsInsertData(pSql->sqlstr)) {  
    pStmt->isInsert = true;
    
    pSql->cmd.numOfParams = 0;
    pSql->cmd.batchSize   = 0;
    
    int32_t code = tsParseSql(pSql, true);
    if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
      // wait for the callback function to post the semaphore
      tsem_wait(&pSql->rspSem);
      return pSql->res.code;
    }
    
    return code;
  }

  pStmt->isInsert = false;
  return normalStmtPrepare(pStmt);
}

int taos_stmt_close(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (!pStmt->isInsert) {
    SNormalStmt* normal = &pStmt->normal;
    if (normal->params != NULL) {
      for (uint16_t i = 0; i < normal->numParams; i++) {
        tVariantDestroy(normal->params + i);
      }
      free(normal->params);
    }
    free(normal->parts);
    free(normal->sql);
  }

  tscFreeSqlObj(pStmt->pSql);
  free(pStmt);
  return TSDB_CODE_SUCCESS;
}

int taos_stmt_bind_param(TAOS_STMT* stmt, TAOS_BIND* bind) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (pStmt->isInsert) {
    return insertStmtBindParam(pStmt, bind);
  }
  return normalStmtBindParam(pStmt, bind);
}

int taos_stmt_add_batch(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (pStmt->isInsert) {
    return insertStmtAddBatch(pStmt);
  }
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int taos_stmt_reset(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (pStmt->isInsert) {
    return insertStmtReset(pStmt);
  }
  return TSDB_CODE_SUCCESS;
}

int taos_stmt_execute(TAOS_STMT* stmt) {
  int ret = 0;
  STscStmt* pStmt = (STscStmt*)stmt;
  if (pStmt->isInsert) {
    ret = insertStmtExecute(pStmt);
  } else {
    char* sql = normalStmtBuildSql(pStmt);
    if (sql == NULL) {
      ret = TSDB_CODE_CLI_OUT_OF_MEMORY;
    } else {
      tfree(pStmt->pSql->sqlstr);
      pStmt->pSql->sqlstr = sql;
      SSqlObj* pSql = taos_query((TAOS*)pStmt->taos, pStmt->pSql->sqlstr);
      ret = taos_errno(pSql);
      taos_free_result(pSql);
    }
  }
  return ret;
}

TAOS_RES *taos_stmt_use_result(TAOS_STMT* stmt) {
  if (stmt == NULL) {
    tscError("statement is invalid.");
    return NULL;
  }

  STscStmt* pStmt = (STscStmt*)stmt;
  if (pStmt->pSql == NULL) {
    tscError("result has been used already.");
    return NULL;
  }

  TAOS_RES* result = pStmt->pSql;
  pStmt->pSql = NULL;
  return result;
}
