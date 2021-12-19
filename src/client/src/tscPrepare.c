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
int32_t tsCheckTimestamp(STableDataBlocks *pDataBlocks, const char *start);

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

typedef struct SMultiTbStmt {
  bool      nameSet;
  bool      tagSet;
  bool      subSet;
  bool      tagColSet;
  uint64_t  currentUid;
  char     *sqlstr;
  uint32_t  tbNum;
  SStrToken tbname;
  SStrToken stbname;
  SStrToken values;
  SStrToken tagCols;
  SArray   *tags;
  STableDataBlocks *lastBlock;
  SHashObj *pTableHash;
  SHashObj *pTableBlockHashList;     // data block for each table
} SMultiTbStmt;

typedef enum {
  STMT_INIT = 1,
  STMT_PREPARE,
  STMT_SETTBNAME,
  STMT_BIND,
  STMT_BIND_COL,
  STMT_ADD_BATCH,
  STMT_EXECUTE
} STMT_ST;

typedef struct STscStmt {
  bool isInsert;
  bool multiTbInsert;
  int16_t  last;
  STscObj* taos;
  SSqlObj* pSql;
  SMultiTbStmt mtb;
  SNormalStmt normal;
} STscStmt;

#define STMT_RET(c) do {          \
  int32_t _code = c;               \
  if (pStmt && pStmt->pSql) { pStmt->pSql->res.code = _code; } else {terrno = _code;}   \
  return _code;             \
} while (0)

#define STMT_CHECK if (pStmt == NULL || pStmt->pSql == NULL || pStmt->taos == NULL) { \
                      STMT_RET(TSDB_CODE_TSC_DISCONNECTED);  \
                    }

static int32_t invalidOperationMsg(char* dstBuffer, const char* errMsg) {
  return tscInvalidOperationMsg(dstBuffer, errMsg, NULL);
}

static int normalStmtAddPart(SNormalStmt* stmt, bool isParam, char* str, uint32_t len) {
  uint16_t size = stmt->numParts + 1;
  if (size > stmt->sizeParts) {
    size *= 2;
    void* tmp = realloc(stmt->parts, sizeof(SNormalStmtPart) * size);
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
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

static int normalStmtBindParam(STscStmt* stmt, TAOS_BIND* tsc_bind) {
  SNormalStmt* normal = &stmt->normal;

  for (uint16_t i = 0; i < normal->numParams; ++i) {
    TAOS_BIND* tb = tsc_bind + i;
    tVariant* var = normal->params + i;
    tVariantDestroy(var);

    var->nLen = 0;
    if (tb->is_null != NULL && *(tb->is_null)) {
      var->nType = TSDB_DATA_TYPE_NULL;
      var->i64 = 0;
      continue;
    }

    var->nType = tb->buffer_type;
    switch (tb->buffer_type) {
      case TSDB_DATA_TYPE_NULL:
        var->i64 = 0;
        break;

      case TSDB_DATA_TYPE_BOOL:
        var->i64 = (*(int8_t*)tb->buffer) ? 1 : 0;
        break;

      case TSDB_DATA_TYPE_TINYINT:
        var->i64 = *(int8_t*)tb->buffer;
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        var->i64 = *(int16_t*)tb->buffer;
        break;

      case TSDB_DATA_TYPE_INT:
        var->i64 = *(int32_t*)tb->buffer;
        break;

      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        var->i64 = *(int64_t*)tb->buffer;
        break;

      case  TSDB_DATA_TYPE_UTINYINT:
        var->u64 = *(uint8_t*)tb->buffer;
        break;

      case  TSDB_DATA_TYPE_USMALLINT:
        var->u64 = *(uint16_t*)tb->buffer;
        break;

      case  TSDB_DATA_TYPE_UINT:
        var->u64 = *(uint32_t*)tb->buffer;
        break;

      case  TSDB_DATA_TYPE_UBIGINT:
        var->u64 = *(uint64_t*)tb->buffer;
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
          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
        memcpy(var->pz, tb->buffer, (*tb->length));
        var->pz[*tb->length] = 0;
        var->nLen = (int32_t)(*tb->length);
        break;

      default:
        tscError("0x%"PRIx64" bind column%d: type mismatch or invalid", stmt->pSql->self, i);
        return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "bind type mismatch or invalid");
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int normalStmtPrepare(STscStmt* stmt) {
  SNormalStmt* normal = &stmt->normal;
  char* sql = stmt->pSql->sqlstr;
  uint32_t i = 0, start = 0;

  while (sql[i] != 0) {
    SStrToken token = {0};
    token.n = tGetToken(sql + i, &token.type);

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
    } else if (token.type == TK_ILLEGAL) {
      return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "invalid sql");
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
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
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
    case TSDB_DATA_TYPE_TIMESTAMP:
      taosStringBuilderAppendInteger(&sb, var->i64);
      break;

    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      taosStringBuilderAppendUnsignedInteger(&sb, var->u64);
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

static int fillColumnsNull(STableDataBlocks* pBlock, int32_t rowNum) {
  SParsedDataColInfo* spd = &pBlock->boundColumnInfo;
  int32_t offset = 0;
  SSchema *schema = (SSchema*)pBlock->pTableMeta->schema;

  for (int32_t i = 0; i < spd->numOfCols; ++i) {
    if (spd->cols[i].valStat == VAL_STAT_NONE) {  // current column do not have any value to insert, set it to null
      for (int32_t n = 0; n < rowNum; ++n) {
        char *ptr = pBlock->pData + sizeof(SSubmitBlk) + pBlock->rowSize * n + offset;

        if (schema[i].type == TSDB_DATA_TYPE_BINARY) {
          varDataSetLen(ptr, sizeof(int8_t));
          *(uint8_t*) varDataVal(ptr) = TSDB_DATA_BINARY_NULL;
        } else if (schema[i].type == TSDB_DATA_TYPE_NCHAR) {
          varDataSetLen(ptr, sizeof(int32_t));
          *(uint32_t*) varDataVal(ptr) = TSDB_DATA_NCHAR_NULL;
        } else {
          setNull(ptr, schema[i].type, schema[i].bytes);
        }
      }
    }

    offset += schema[i].bytes;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t fillTablesColumnsNull(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  STableDataBlocks** p = taosHashIterate(pCmd->insertParam.pTableBlockHashList, NULL);

  STableDataBlocks* pOneTableBlock = *p;
  while(pOneTableBlock) {
    SSubmitBlk* pBlocks = (SSubmitBlk*) pOneTableBlock->pData;
    if (pBlocks->numOfRows > 0 && pOneTableBlock->boundColumnInfo.numOfBound < pOneTableBlock->boundColumnInfo.numOfCols) {
      fillColumnsNull(pOneTableBlock, pBlocks->numOfRows);
    }

    p = taosHashIterate(pCmd->insertParam.pTableBlockHashList, p);
    if (p == NULL) {
      break;
    }

    pOneTableBlock = *p;
  }

  return TSDB_CODE_SUCCESS;
}



////////////////////////////////////////////////////////////////////////////////
// functions for insertion statement preparation
static FORCE_INLINE int doBindParam(STableDataBlocks* pBlock, char* data, SParamInfo* param, TAOS_BIND* tsc_bind, int32_t colNum) {
    if (tsc_bind->is_null != NULL && *(tsc_bind->is_null)) {
      setNull(data + param->offset, param->type, param->bytes);
      return TSDB_CODE_SUCCESS;
    }

#if 0
  if (0) {
    // allow user bind param data with different type
    union {
      int8_t          v1;
      int16_t         v2;
      int32_t         v4;
      int64_t         v8;
      float           f4;
      double          f8;
      unsigned char   buf[32*1024];
    } u;
    switch (param->type) {
      case TSDB_DATA_TYPE_BOOL: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL: {
            u.v1 = *(int8_t*)tsc_bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_TINYINT: {
            u.v1 = *(int8_t*)tsc_bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.v1 = (int8_t)*(int16_t*)tsc_bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.v1 = (int8_t)*(int32_t*)tsc_bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.v1 = (int8_t)*(int64_t*)tsc_bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            // "0", "1" convertible
            if (strncmp((const char*)tsc_bind->buffer, "0", *tsc_bind->length)==0) {
              u.v1 = 0;
              break;
            }
            if (strncmp((const char*)tsc_bind->buffer, "1", *tsc_bind->length)==0) {
              u.v1 = 1;
              break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
        }
        memcpy(data + param->offset, &u.v1, sizeof(u.v1));
        return TSDB_CODE_SUCCESS;
      } break;
      case TSDB_DATA_TYPE_TINYINT: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            int8_t v = *(int8_t*)tsc_bind->buffer;
            u.v1 = v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            int16_t v = *(int16_t*)tsc_bind->buffer;
            u.v1 = (int8_t)v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_INT: {
            int32_t v = *(int32_t*)tsc_bind->buffer;
            u.v1 = (int8_t)v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t v = *(int64_t*)tsc_bind->buffer;
            u.v1 = (int8_t)v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int     n, r;
            r = sscanf((const char*)tsc_bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r == 1 && n == strlen((const char*)tsc_bind->buffer)) {
              u.v1 = (int8_t)v;
              if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
        }
        memcpy(data + param->offset, &u.v1, sizeof(u.v1));
        return TSDB_CODE_SUCCESS;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_SMALLINT: {
            int v = *(int16_t*)tsc_bind->buffer;
            u.v2 = (int16_t)v;
          } break;
          case TSDB_DATA_TYPE_INT: {
            int32_t v = *(int32_t*)tsc_bind->buffer;
            u.v2 = (int16_t)v;
            if (v >= SHRT_MIN && v <= SHRT_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t v = *(int64_t*)tsc_bind->buffer;
            u.v2 = (int16_t)v;
            if (v >= SHRT_MIN && v <= SHRT_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int     n, r;
            r = sscanf((const char*)tsc_bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r == 1 && n == strlen((const char*)tsc_bind->buffer)) {
              u.v2 = (int16_t)v;
              if (v >= SHRT_MIN && v <= SHRT_MAX) break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
        }
        memcpy(data + param->offset, &u.v2, sizeof(u.v2));
        return TSDB_CODE_SUCCESS;
      }
      case TSDB_DATA_TYPE_INT: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_INT: {
            u.v4 = *(int32_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t v = *(int64_t*)tsc_bind->buffer;
            u.v4 = (int32_t)v;
            if (v >= INT_MIN && v <= INT_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int n,r;
            r = sscanf((const char*)tsc_bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r==1 && n==strlen((const char*)tsc_bind->buffer)) {
              u.v4 = (int32_t)v;
              if (v >= INT_MIN && v <= INT_MAX) break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
        }
        memcpy(data + param->offset, &u.v2, sizeof(u.v2));
        return TSDB_CODE_SUCCESS;
			} break;
      case TSDB_DATA_TYPE_FLOAT: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            u.f4 = *(int8_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.f4 = *(int16_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.f4 = (float)*(int32_t*)tsc_bind->buffer;
            // shall we check equality?
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.f4 = (float)*(int64_t*)tsc_bind->buffer;
            // shall we check equality?
          } break;
          case TSDB_DATA_TYPE_FLOAT: {
            u.f4 = *(float*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_DOUBLE: {
            u.f4 = *(float*)tsc_bind->buffer;
            // shall we check equality?
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            float v;
            int n,r;
            r = sscanf((const char*)tsc_bind->buffer, "%f%n", &v, &n);
            if (r==1 && n==strlen((const char*)tsc_bind->buffer)) {
              u.f4 = v;
              break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
          case TSDB_DATA_TYPE_TIMESTAMP:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
        }
        memcpy(data + param->offset, &u.f4, sizeof(u.f4));
        return TSDB_CODE_SUCCESS;
			} break;
      case TSDB_DATA_TYPE_BIGINT: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            u.v8 = *(int8_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.v8 = *(int16_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.v8 = *(int32_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.v8 = *(int64_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int n,r;
            r = sscanf((const char*)tsc_bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r==1 && n==strlen((const char*)tsc_bind->buffer)) {
              u.v8 = v;
              break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
        }
        memcpy(data + param->offset, &u.v8, sizeof(u.v8));
        return TSDB_CODE_SUCCESS;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            u.f8 = *(int8_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.f8 = *(int16_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.f8 = *(int32_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.f8 = (double)*(int64_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_FLOAT: {
            u.f8 = *(float*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_DOUBLE: {
            u.f8 = *(double*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            double v;
            int n,r;
            r = sscanf((const char*)tsc_bind->buffer, "%lf%n", &v, &n);
            if (r==1 && n==strlen((const char*)tsc_bind->buffer)) {
              u.f8 = v;
              break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_TIMESTAMP:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
        }
        memcpy(data + param->offset, &u.f8, sizeof(u.f8));
        return TSDB_CODE_SUCCESS;
      }
      case TSDB_DATA_TYPE_TIMESTAMP: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_TIMESTAMP: {
            u.v8 = *(int64_t*)tsc_bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            // is this the correct way to call taosParseTime?
            int32_t len = (int32_t)*tsc_bind->length;
            if (taosParseTime(tsc_bind->buffer, &u.v8, len, 3, tsDaylight) == TSDB_CODE_SUCCESS) {
              break;
            }
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_DOUBLE:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
        };
        memcpy(data + param->offset, &u.v8, sizeof(u.v8));
        return TSDB_CODE_SUCCESS;
			}
      case TSDB_DATA_TYPE_BINARY: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_BINARY: {
            if ((*tsc_bind->length) > (uintptr_t)param->bytes) {
              return TSDB_CODE_TSC_INVALID_VALUE;
            }
            short size = (short)*tsc_bind->length;
            STR_WITH_SIZE_TO_VARSTR(data + param->offset, tsc_bind->buffer, size);
            return TSDB_CODE_SUCCESS;
          }
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
          case TSDB_DATA_TYPE_NCHAR:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
        }
      }
      case TSDB_DATA_TYPE_NCHAR: {
        switch (tsc_bind->buffer_type) {
          case TSDB_DATA_TYPE_NCHAR: {
            int32_t output = 0;
            if (!taosMbsToUcs4(tsc_bind->buffer, *tsc_bind->length, varDataVal(data + param->offset), param->bytes - VARSTR_HEADER_SIZE, &output)) {
              return TSDB_CODE_TSC_INVALID_VALUE;
            }
            varDataSetLen(data + param->offset, output);
            return TSDB_CODE_SUCCESS;
          }
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_FLOAT:
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_DOUBLE:
          case TSDB_DATA_TYPE_TIMESTAMP:
          case TSDB_DATA_TYPE_BINARY:
          default: {
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
        }
      }
      default: {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
    }
  }
#endif

  if (tsc_bind->buffer_type != param->type) {
    tscError("column type mismatch");
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  short size = 0;
  switch(param->type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      *(uint8_t *)(data + param->offset) = *(uint8_t *)tsc_bind->buffer;
      break;

    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      *(uint16_t *)(data + param->offset) = *(uint16_t *)tsc_bind->buffer;
      break;

    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_FLOAT:
      *(uint32_t *)(data + param->offset) = *(uint32_t *)tsc_bind->buffer;
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_TIMESTAMP:
      *(uint64_t *)(data + param->offset) = *(uint64_t *)tsc_bind->buffer;
      break;

    case TSDB_DATA_TYPE_BINARY:
      if ((*tsc_bind->length) > (uintptr_t)param->bytes) {
        tscError("column length is too big");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      size = (short)*tsc_bind->length;
      STR_WITH_SIZE_TO_VARSTR(data + param->offset, tsc_bind->buffer, size);
      return TSDB_CODE_SUCCESS;

    case TSDB_DATA_TYPE_NCHAR: {
      int32_t output = 0;
      if (!taosMbsToUcs4(tsc_bind->buffer, *tsc_bind->length, varDataVal(data + param->offset), param->bytes - VARSTR_HEADER_SIZE, &output)) {
        tscError("convert nchar failed");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      varDataSetLen(data + param->offset, output);
      return TSDB_CODE_SUCCESS;
    }
    default:
      assert(false);
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (param->offset == 0) {
    if (tsCheckTimestamp(pBlock, data + param->offset) != TSDB_CODE_SUCCESS) {
      tscError("invalid timestamp");
      return TSDB_CODE_TSC_INVALID_VALUE;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t insertStmtGenLastBlock(STableDataBlocks** lastBlock, STableDataBlocks* pBlock) {
  *lastBlock = (STableDataBlocks*)malloc(sizeof(STableDataBlocks));
  memcpy(*lastBlock, pBlock, sizeof(STableDataBlocks));
  (*lastBlock)->cloned = true;
  
  (*lastBlock)->pData    = NULL;
  (*lastBlock)->ordered  = true;
  (*lastBlock)->prevTS   = INT64_MIN;
  (*lastBlock)->size     = sizeof(SSubmitBlk);
  (*lastBlock)->tsSource = -1;

  return TSDB_CODE_SUCCESS;
}


static int32_t insertStmtGenBlock(STscStmt* pStmt, STableDataBlocks** pBlock, STableMeta* pTableMeta, SName* name) {
  int32_t code = 0;
  
  if (pStmt->mtb.lastBlock == NULL) {
    tscError("no previous data block");
    return TSDB_CODE_TSC_APP_ERROR;
  }

  int32_t msize = tscGetTableMetaSize(pTableMeta);
  int32_t tsize = sizeof(STableDataBlocks) + msize;

  void *t = malloc(tsize);
  *pBlock = t;

  memcpy(*pBlock, pStmt->mtb.lastBlock, sizeof(STableDataBlocks));

  t = (char *)t + sizeof(STableDataBlocks);
  (*pBlock)->pTableMeta = t;
  memcpy((*pBlock)->pTableMeta, pTableMeta, msize);

  (*pBlock)->pData = malloc((*pBlock)->nAllocSize);

  (*pBlock)->vgId     = (*pBlock)->pTableMeta->vgId;

  tNameAssign(&(*pBlock)->tableName, name);
  
  SSubmitBlk* blk = (SSubmitBlk*)(*pBlock)->pData;
  memset(blk, 0, sizeof(*blk));
  
  code = tsSetBlockInfo(blk, pTableMeta, 0);
  if (code != TSDB_CODE_SUCCESS) {
    STMT_RET(code);
  }

  return TSDB_CODE_SUCCESS;
}


static int doBindBatchParam(STableDataBlocks* pBlock, SParamInfo* param, TAOS_MULTI_BIND* tsc_bind, int32_t rowNum) {
  if (tsc_bind->buffer_type != param->type || !isValidDataType(param->type)) {
    tscError("column mismatch or invalid");
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (IS_VAR_DATA_TYPE(param->type) && tsc_bind->length == NULL) {
    tscError("BINARY/NCHAR no length");
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  for (int i = 0; i < tsc_bind->num; ++i) {
    char* data = pBlock->pData + sizeof(SSubmitBlk) + pBlock->rowSize * (rowNum + i);

    if (tsc_bind->is_null != NULL && tsc_bind->is_null[i]) {
      setNull(data + param->offset, param->type, param->bytes);
      continue;
    }

    if (!IS_VAR_DATA_TYPE(param->type)) {
      memcpy(data + param->offset, (char *)tsc_bind->buffer + tsc_bind->buffer_length * i, tDataTypes[param->type].bytes);

      if (param->offset == 0) {
        if (tsCheckTimestamp(pBlock, data + param->offset) != TSDB_CODE_SUCCESS) {
          tscError("invalid timestamp");
          return TSDB_CODE_TSC_INVALID_VALUE;
        }
      }
    } else if (param->type == TSDB_DATA_TYPE_BINARY) {
      if (tsc_bind->length[i] > (uintptr_t)param->bytes) {
        tscError("binary length too long, ignore it, max:%d, actual:%d", param->bytes, (int32_t)tsc_bind->length[i]);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      int16_t bsize = (short)tsc_bind->length[i];
      STR_WITH_SIZE_TO_VARSTR(data + param->offset, (char *)tsc_bind->buffer + tsc_bind->buffer_length * i, bsize);
    } else if (param->type == TSDB_DATA_TYPE_NCHAR) {
      if (tsc_bind->length[i] > (uintptr_t)param->bytes) {
        tscError("nchar string length too long, ignore it, max:%d, actual:%d", param->bytes, (int32_t)tsc_bind->length[i]);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      int32_t output = 0;
      if (!taosMbsToUcs4((char *)tsc_bind->buffer + tsc_bind->buffer_length * i, tsc_bind->length[i], varDataVal(data + param->offset), param->bytes - VARSTR_HEADER_SIZE, &output)) {
        tscError("convert nchar string to UCS4_LE failed:%s", (char*)((char *)tsc_bind->buffer + tsc_bind->buffer_length * i));
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      varDataSetLen(data + param->offset, output);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int insertStmtBindParam(STscStmt* stmt, TAOS_BIND* tsc_bind) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;
  STscStmt* pStmt = (STscStmt*)stmt;

  STableDataBlocks* pBlock = NULL;

  if (pStmt->multiTbInsert) {
    if (pCmd->insertParam.pTableBlockHashList == NULL) {
      tscError("0x%"PRIx64" Table block hash list is empty", pStmt->pSql->self);
      return TSDB_CODE_TSC_APP_ERROR;
    }

    STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pCmd->insertParam.pTableBlockHashList, (const char*)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid));
    if (t1 == NULL) {
      tscError("0x%"PRIx64" no table data block in hash list, uid:%" PRId64 , pStmt->pSql->self, pStmt->mtb.currentUid);
      return TSDB_CODE_TSC_APP_ERROR;
    }

    pBlock = *t1;
  } else {
    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);

    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    if (pCmd->insertParam.pTableBlockHashList == NULL) {
      pCmd->insertParam.pTableBlockHashList = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
    }

    int32_t ret =
        tscGetDataBlockFromList(pCmd->insertParam.pTableBlockHashList, pTableMeta->id.uid, TSDB_PAYLOAD_SIZE, sizeof(SSubmitBlk),
                                pTableMeta->tableInfo.rowSize, &pTableMetaInfo->name, pTableMeta, &pBlock, NULL);
    if (ret != 0) {
      return ret;
    }
  }

  uint32_t totalDataSize = sizeof(SSubmitBlk) + (pCmd->batchSize + 1) * pBlock->rowSize;
  if (totalDataSize > pBlock->nAllocSize) {
    const double factor = 1.5;

    void* tmp = realloc(pBlock->pData, (uint32_t)(totalDataSize * factor));
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    pBlock->pData = (char*)tmp;
    pBlock->nAllocSize = (uint32_t)(totalDataSize * factor);
  }

  char* data = pBlock->pData + sizeof(SSubmitBlk) + pBlock->rowSize * pCmd->batchSize;
  for (uint32_t j = 0; j < pBlock->numOfParams; ++j) {
    SParamInfo* param = &pBlock->params[j];

    int code = doBindParam(pBlock, data, param, &tsc_bind[param->idx], 1);
    if (code != TSDB_CODE_SUCCESS) {
      tscDebug("0x%"PRIx64" bind column %d: type mismatch or invalid", pStmt->pSql->self, param->idx);
      return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "bind column type mismatch or invalid");
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int insertStmtBindParamBatch(STscStmt* stmt, TAOS_MULTI_BIND* tsc_bind, int colIdx) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;
  STscStmt* pStmt = (STscStmt*)stmt;
  int rowNum = tsc_bind->num;

  STableDataBlocks* pBlock = NULL;

  if (pStmt->multiTbInsert) {
    if (pCmd->insertParam.pTableBlockHashList == NULL) {
      tscError("0x%"PRIx64" Table block hash list is empty", pStmt->pSql->self);
      return TSDB_CODE_TSC_APP_ERROR;
    }

    STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pCmd->insertParam.pTableBlockHashList, (const char*)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid));
    if (t1 == NULL) {
      tscError("0x%"PRIx64" no table data block in hash list, uid:%" PRId64 , pStmt->pSql->self, pStmt->mtb.currentUid);
      return TSDB_CODE_TSC_APP_ERROR;
    }

    pBlock = *t1;
  } else {
    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);

    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    if (pCmd->insertParam.pTableBlockHashList == NULL) {
      pCmd->insertParam.pTableBlockHashList = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
    }

    int32_t ret =
        tscGetDataBlockFromList(pCmd->insertParam.pTableBlockHashList, pTableMeta->id.uid, TSDB_PAYLOAD_SIZE, sizeof(SSubmitBlk),
                                pTableMeta->tableInfo.rowSize, &pTableMetaInfo->name, pTableMeta, &pBlock, NULL);
    if (ret != 0) {
      return ret;
    }
  }

  if (!(colIdx == -1 || (colIdx >= 0 && colIdx < pBlock->numOfParams))) {
    tscError("0x%"PRIx64" invalid colIdx:%d", pStmt->pSql->self, colIdx);
    return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "invalid param colIdx");
  }

  uint32_t totalDataSize = sizeof(SSubmitBlk) + (pCmd->batchSize + rowNum) * pBlock->rowSize;
  if (totalDataSize > pBlock->nAllocSize) {
    const double factor = 1.5;

    void* tmp = realloc(pBlock->pData, (uint32_t)(totalDataSize * factor));
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    pBlock->pData = (char*)tmp;
    pBlock->nAllocSize = (uint32_t)(totalDataSize * factor);
  }

  if (colIdx == -1) {
    for (uint32_t j = 0; j < pBlock->numOfParams; ++j) {
      SParamInfo* param = &pBlock->params[j];
      if (tsc_bind[param->idx].num != rowNum) {
        tscError("0x%"PRIx64" param %d: num[%d:%d] not match", pStmt->pSql->self, param->idx, rowNum, tsc_bind[param->idx].num);
        return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "tsc_bind row num mismatch");
      }

      int code = doBindBatchParam(pBlock, param, &tsc_bind[param->idx], pCmd->batchSize);
      if (code != TSDB_CODE_SUCCESS) {
        tscError("0x%"PRIx64" bind column %d: type mismatch or invalid", pStmt->pSql->self, param->idx);
        return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "tsc_bind column type mismatch or invalid");
      }
    }

    pCmd->batchSize += rowNum - 1;
  } else {
    SParamInfo* param = &pBlock->params[colIdx];

    int code = doBindBatchParam(pBlock, param, tsc_bind, pCmd->batchSize);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("0x%"PRIx64" bind column %d: type mismatch or invalid", pStmt->pSql->self, param->idx);
      return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "bind column type mismatch or invalid");
    }

    if (colIdx == (pBlock->numOfParams - 1)) {
      pCmd->batchSize += rowNum - 1;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int insertStmtUpdateBatch(STscStmt* stmt) {
  SSqlObj* pSql = stmt->pSql;
  SSqlCmd* pCmd = &pSql->cmd;
  STableDataBlocks* pBlock = NULL;

  if (pCmd->batchSize > INT16_MAX) {
    tscError("too many record:%d", pCmd->batchSize);
    return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "too many records");
  }

  if (taosHashGetSize(pCmd->insertParam.pTableBlockHashList) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pCmd->insertParam.pTableBlockHashList, (const char*)&stmt->mtb.currentUid, sizeof(stmt->mtb.currentUid));
  if (t1 == NULL) {
    tscError("0x%"PRIx64" no table data block in hash list, uid:%" PRId64 , pSql->self, stmt->mtb.currentUid);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  pBlock = *t1;

  STableMeta* pTableMeta = pBlock->pTableMeta;

  pBlock->size = sizeof(SSubmitBlk) + pCmd->batchSize * pBlock->rowSize;
  SSubmitBlk* pBlk = (SSubmitBlk*) pBlock->pData;
  pBlk->numOfRows = pCmd->batchSize;
  pBlk->dataLen = 0;
  pBlk->uid = pTableMeta->id.uid;
  pBlk->tid = pTableMeta->id.tid;

  return TSDB_CODE_SUCCESS;
}

static int insertStmtAddBatch(STscStmt* stmt) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;
  ++pCmd->batchSize;

  if (stmt->multiTbInsert) {
    return insertStmtUpdateBatch(stmt);
  }

  return TSDB_CODE_SUCCESS;
}

static int insertStmtReset(STscStmt* pStmt) {
  SSqlCmd* pCmd = &pStmt->pSql->cmd;
  if (pCmd->batchSize > 2) {
    int32_t alloced = (pCmd->batchSize + 1) / 2;

    size_t size = taosArrayGetSize(pCmd->insertParam.pDataBlocks);
    for (int32_t i = 0; i < size; ++i) {
      STableDataBlocks* pBlock = taosArrayGetP(pCmd->insertParam.pDataBlocks, i);

      uint32_t totalDataSize = pBlock->size - sizeof(SSubmitBlk);
      pBlock->size = sizeof(SSubmitBlk) + totalDataSize / alloced;

      SSubmitBlk* pSubmit = (SSubmitBlk*)pBlock->pData;
      pSubmit->numOfRows = pSubmit->numOfRows / alloced;
    }
  }
  pCmd->batchSize = 0;

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);
  pTableMetaInfo->vgroupIndex = 0;
  return TSDB_CODE_SUCCESS;
}

static int insertStmtExecute(STscStmt* stmt) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;
  if (pCmd->batchSize == 0) {
    tscError("no records bind");
    return invalidOperationMsg(tscGetErrorMsgPayload(&stmt->pSql->cmd), "no records tsc_bind");
  }

  if (taosHashGetSize(pCmd->insertParam.pTableBlockHashList) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);

  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  if (pCmd->insertParam.pTableBlockHashList == NULL) {
    pCmd->insertParam.pTableBlockHashList = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  }

  STableDataBlocks* pBlock = NULL;

  int32_t ret =
      tscGetDataBlockFromList(pCmd->insertParam.pTableBlockHashList, pTableMeta->id.uid, TSDB_PAYLOAD_SIZE, sizeof(SSubmitBlk),
                              pTableMeta->tableInfo.rowSize, &pTableMetaInfo->name, pTableMeta, &pBlock, NULL);
  assert(ret == 0);
  pBlock->size = sizeof(SSubmitBlk) + pCmd->batchSize * pBlock->rowSize;
  SSubmitBlk* pBlk = (SSubmitBlk*) pBlock->pData;
  pBlk->numOfRows = pCmd->batchSize;
  pBlk->dataLen = 0;
  pBlk->uid = pTableMeta->id.uid;
  pBlk->tid = pTableMeta->id.tid;

  fillTablesColumnsNull(stmt->pSql);

  int code = tscMergeTableDataBlocks(&stmt->pSql->cmd.insertParam, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  STableDataBlocks* pDataBlock = taosArrayGetP(pCmd->insertParam.pDataBlocks, 0);
  code = tscCopyDataBlockToPayload(stmt->pSql, pDataBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SSqlObj* pSql = stmt->pSql;
  SSqlRes* pRes = &pSql->res;
  pRes->numOfRows  = 0;
  pRes->numOfTotal = 0;

  tscBuildAndSendRequest(pSql, NULL);

  // wait for the callback function to post the semaphore
  tsem_wait(&pSql->rspSem);

  // data block reset
  pCmd->batchSize = 0;
  for(int32_t i = 0; i < pCmd->insertParam.numOfTables; ++i) {
    if (pCmd->insertParam.pTableNameList && pCmd->insertParam.pTableNameList[i]) {
      tfree(pCmd->insertParam.pTableNameList[i]);
    }
  }

  pCmd->insertParam.numOfTables = 0;
  tfree(pCmd->insertParam.pTableNameList);
  pCmd->insertParam.pDataBlocks = tscDestroyBlockArrayList(pCmd->insertParam.pDataBlocks);

  return pSql->res.code;
}

static void insertBatchClean(STscStmt* pStmt) {
  SSqlCmd *pCmd = &pStmt->pSql->cmd;
  SSqlObj *pSql = pStmt->pSql;
  int32_t size = taosHashGetSize(pCmd->insertParam.pTableBlockHashList);

  // data block reset
  pCmd->batchSize = 0;

  for(int32_t i = 0; i < size; ++i) {
    if (pCmd->insertParam.pTableNameList && pCmd->insertParam.pTableNameList[i]) {
      tfree(pCmd->insertParam.pTableNameList[i]);
    }
  }

  tfree(pCmd->insertParam.pTableNameList);

  pCmd->insertParam.pDataBlocks = tscDestroyBlockArrayList(pCmd->insertParam.pDataBlocks);
  pCmd->insertParam.numOfTables = 0;

  STableDataBlocks** p = taosHashIterate(pCmd->insertParam.pTableBlockHashList, NULL);
  while(p) {
    tfree((*p)->pData);
    p = taosHashIterate(pCmd->insertParam.pTableBlockHashList, p);
  }

  taosHashClear(pCmd->insertParam.pTableBlockHashList);
  tscFreeSqlResult(pSql);
  tscFreeSubobj(pSql);
  tfree(pSql->pSubs);
  pSql->subState.numOfSub = 0;
}

static int insertBatchStmtExecute(STscStmt* pStmt) {
  int32_t code = 0;
  
  if(pStmt->mtb.nameSet == false) {
    tscError("0x%"PRIx64" no table name set", pStmt->pSql->self);
    return invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "no table name set");
  }

  pStmt->pSql->retry = pStmt->pSql->maxRetry + 1;  //no retry

  if (taosHashGetSize(pStmt->pSql->cmd.insertParam.pTableBlockHashList) <= 0) { // merge according to vgId
    tscError("0x%"PRIx64" no data block to insert", pStmt->pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  fillTablesColumnsNull(pStmt->pSql);

  if ((code = tscMergeTableDataBlocks(&pStmt->pSql->cmd.insertParam, false)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tscHandleMultivnodeInsert(pStmt->pSql);

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // wait for the callback function to post the semaphore
  tsem_wait(&pStmt->pSql->rspSem);

  code = pStmt->pSql->res.code;
  
  insertBatchClean(pStmt);

  return code;
}

int stmtParseInsertTbTags(SSqlObj* pSql, STscStmt* pStmt) {
  SSqlCmd *pCmd    = &pSql->cmd;
  int32_t ret = TSDB_CODE_SUCCESS;

  if ((ret = tsInsertInitialCheck(pSql)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  int32_t tsc_index = 0;
  SStrToken sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
  if (sToken.n == 0) {
    tscError("table is is expected, sql:%s", pCmd->insertParam.sql);
    return tscSQLSyntaxErrMsg(pCmd->payload, "table name is expected", pCmd->insertParam.sql);
  }

  if (sToken.n == 1 && sToken.type == TK_QUESTION) {
    pStmt->multiTbInsert = true;
    pStmt->mtb.tbname = sToken;
    pStmt->mtb.nameSet = false;
    if (pStmt->mtb.pTableHash == NULL) {
      pStmt->mtb.pTableHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
    }

    if (pStmt->mtb.pTableBlockHashList == NULL) {
      pStmt->mtb.pTableBlockHashList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
    }

    pStmt->mtb.tagSet = true;

    sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
    if (sToken.n > 0 && (sToken.type == TK_VALUES || sToken.type == TK_LP)) {
      return TSDB_CODE_SUCCESS;
    }

    if (sToken.n <= 0 || sToken.type != TK_USING) {
      tscError("keywords USING is expected, sql:%s", pCmd->insertParam.sql);
      return tscSQLSyntaxErrMsg(pCmd->payload, "keywords USING is expected", sToken.z ? sToken.z : pCmd->insertParam.sql);
    }

    sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
    if (sToken.n <= 0 || ((sToken.type != TK_ID) && (sToken.type != TK_STRING))) {
      tscError("invalid token, sql:%s", pCmd->insertParam.sql);
      return tscSQLSyntaxErrMsg(pCmd->payload, "invalid token", sToken.z ? sToken.z : pCmd->insertParam.sql);
    }
    pStmt->mtb.stbname = sToken;

    sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
    if (sToken.n <= 0 || ((sToken.type != TK_TAGS) && (sToken.type != TK_LP))) {
      tscError("invalid token, sql:%s", pCmd->insertParam.sql);
      return tscSQLSyntaxErrMsg(pCmd->payload, "invalid token", sToken.z ? sToken.z : pCmd->insertParam.sql);
    }

    // ... (tag_col_list) TAGS(tag_val_list) ...
    int32_t tagColsCnt = 0;
    if (sToken.type == TK_LP) {
      pStmt->mtb.tagColSet = true;
      pStmt->mtb.tagCols = sToken;
      int32_t tagColsStart = tsc_index;
      while (1) {
        sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
        if (sToken.type == TK_ILLEGAL) {
          return tscSQLSyntaxErrMsg(pCmd->payload, "unrecognized token", sToken.z);
        }
        if (sToken.type == TK_ID) {
          ++tagColsCnt;
        }
        if (sToken.type == TK_RP) {
          break;
        }
      }
      if (tagColsCnt == 0) {
        tscError("tag column list expected, sql:%s", pCmd->insertParam.sql);
        return tscSQLSyntaxErrMsg(pCmd->payload, "tag column list expected", pCmd->insertParam.sql);
      }
      pStmt->mtb.tagCols.n = tsc_index - tagColsStart + 1;

      sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
      if (sToken.n <= 0 || sToken.type != TK_TAGS) {
        tscError("keyword TAGS expected, sql:%s", pCmd->insertParam.sql);
        return tscSQLSyntaxErrMsg(pCmd->payload, "keyword TAGS expected", sToken.z ? sToken.z : pCmd->insertParam.sql);
      }
    }

    sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
    if (sToken.n <= 0 || sToken.type != TK_LP) {
      tscError("( expected, sql:%s", pCmd->insertParam.sql);
      return tscSQLSyntaxErrMsg(pCmd->payload, "( expected", sToken.z ? sToken.z : pCmd->insertParam.sql);
    }

    pStmt->mtb.tags = taosArrayInit(4, sizeof(SStrToken));

    int32_t loopCont = 1;

    while (loopCont) {
      sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
      if (sToken.n <= 0) {
        tscError("unexpected sql end, sql:%s", pCmd->insertParam.sql);
        return tscSQLSyntaxErrMsg(pCmd->payload, "unexpected sql end", pCmd->insertParam.sql);
      }

      switch (sToken.type) {
        case TK_RP:
          loopCont = 0;
          break;
        case TK_VALUES:
          tscError("unexpected token values, sql:%s", pCmd->insertParam.sql);
          return tscSQLSyntaxErrMsg(pCmd->payload, "unexpected token", sToken.z);
        case TK_QUESTION:
          pStmt->mtb.tagSet = false; //continue
        default:
          taosArrayPush(pStmt->mtb.tags, &sToken);
          break;
      }
    }

    if (taosArrayGetSize(pStmt->mtb.tags) <= 0) {
      tscError("no tags, sql:%s", pCmd->insertParam.sql);
      return tscSQLSyntaxErrMsg(pCmd->payload, "no tags", pCmd->insertParam.sql);
    }

    if (tagColsCnt > 0 && taosArrayGetSize(pStmt->mtb.tags) != tagColsCnt) {
      tscError("not match tags, sql:%s", pCmd->insertParam.sql);
      return tscSQLSyntaxErrMsg(pCmd->payload, "not match tags", pCmd->insertParam.sql);
    }

    sToken = tStrGetToken(pCmd->insertParam.sql, &tsc_index, false);
    if (sToken.n <= 0 || (sToken.type != TK_VALUES && sToken.type != TK_LP)) {
      tscError("sql error, sql:%s", pCmd->insertParam.sql);
      return tscSQLSyntaxErrMsg(pCmd->payload, "sql error", sToken.z ? sToken.z : pCmd->insertParam.sql);
    }

    pStmt->mtb.values = sToken;

  }

  return TSDB_CODE_SUCCESS;
}

int stmtGenInsertStatement(SSqlObj* pSql, STscStmt* pStmt, const char* name, TAOS_BIND* tags) {
  size_t tagNum = taosArrayGetSize(pStmt->mtb.tags);
  size_t size = 1048576;
  char *str = calloc(1, size);
  size_t len = 0;
  int32_t ret = 0;
  int32_t j = 0;

  while (1) {
    if (pStmt->mtb.tagColSet) {
      len = (size_t)snprintf(str, size - 1, "insert into %s using %.*s %.*s tags(",
          name, pStmt->mtb.stbname.n, pStmt->mtb.stbname.z, pStmt->mtb.tagCols.n, pStmt->mtb.tagCols.z);
    } else {
      len = (size_t)snprintf(str, size - 1, "insert into %s using %.*s tags(", name, pStmt->mtb.stbname.n, pStmt->mtb.stbname.z);
    }

    if (len >= (size -1)) {
      size *= 2;
      free(str);
      str = calloc(1, size);
      continue;
    }

    j = 0;

    for (size_t i = 0; i < tagNum && len < (size - 1); ++i) {
      SStrToken *t = taosArrayGet(pStmt->mtb.tags, i);
      if (t->type == TK_QUESTION) {
        int32_t l = 0;
        if (i > 0) {
          str[len++] = ',';
        }

        if (tags[j].is_null && (*tags[j].is_null)) {
          ret = converToStr(str + len, TSDB_DATA_TYPE_NULL, NULL, -1, &l);
        } else {
          if (tags[j].buffer == NULL) {
            free(str);
            tscError("empty tag value in params");
            return invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "empty tag value in params");
          }

          ret = converToStr(str + len, tags[j].buffer_type, tags[j].buffer, tags[j].length ? (int32_t)*tags[j].length : -1, &l);
        }

        ++j;

        if (ret) {
          free(str);
          return ret;
        }

        len += l;
      } else {
        len += (size_t)snprintf(str + len, size - len - 1, i > 0 ? ",%.*s" : "%.*s", t->n, t->z);
      }
    }

    if (len >= (size - 1)) {
      size *= 2;
      free(str);
      str = calloc(1, size);
      continue;
    }

    strcat(str, ") ");
    len += 2;

    if ((len + strlen(pStmt->mtb.values.z)) >= (size - 1)) {
      size *= 2;
      free(str);
      str = calloc(1, size);
      continue;
    }

    strcat(str, pStmt->mtb.values.z);

    break;
  }

  if (pStmt->mtb.sqlstr == NULL) {
    pStmt->mtb.sqlstr = pSql->sqlstr;
  } else {
    tfree(pSql->sqlstr);
  }

  pSql->sqlstr = str;

  return TSDB_CODE_SUCCESS;
}

int32_t stmtValidateValuesFields(SSqlCmd *pCmd, char * sql) {
  int32_t loopCont = 1, index0 = 0, values = 0;
  SStrToken sToken;
  
  while (loopCont) {
    sToken = tStrGetToken(sql, &index0, false);
    if (sToken.n <= 0) {
      return TSDB_CODE_SUCCESS;
    }
  
    switch (sToken.type) {
      case TK_RP:
        if (values) {
         return TSDB_CODE_SUCCESS;
        }
        break;
      case TK_VALUES:
        values = 1;
        break;
      case TK_QUESTION:  
      case TK_LP:
        break;
      default:
        if (values) {
          tscError("only ? allowed in values");
          return tscInvalidOperationMsg(pCmd->payload, "only ? allowed in values", NULL);
        }
        break;
    }
  }

  return TSDB_CODE_SUCCESS;
}


////////////////////////////////////////////////////////////////////////////////
// interface functions

TAOS_STMT* taos_stmt_init(TAOS* taos) {
  STscObj* pObj = (STscObj*)taos;
  STscStmt* pStmt = NULL;

  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("connection disconnected");
    return NULL;
  }

  pStmt = calloc(1, sizeof(STscStmt));
  if (pStmt == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("failed to allocate memory for statement");
    return NULL;
  }
  pStmt->taos = pObj;

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));

  if (pSql == NULL) {
    free(pStmt);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("failed to allocate memory for statement");
    return NULL;
  }

  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    free(pSql);
    free(pStmt);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("failed to malloc payload buffer");
    return NULL;
  }

  tsem_init(&pSql->rspSem, 0, 0);
  pSql->signature = pSql;
  pSql->pTscObj   = pObj;
  pSql->maxRetry  = TSDB_MAX_REPLICA;
  pSql->isBind    = true;
  pStmt->pSql     = pSql;
  pStmt->last     = STMT_INIT;
  registerSqlObj(pSql);

  return pStmt;
}

int taos_stmt_prepare(TAOS_STMT* stmt, const char* sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHECK

  if (sql == NULL) {
    tscError("sql is NULL");
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "sql is NULL"));
  }

  if (pStmt->last != STMT_INIT) {
    tscError("prepare status error, last:%d", pStmt->last);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "prepare status error"));
  }

  pStmt->last = STMT_PREPARE;

  SSqlObj* pSql = pStmt->pSql;
  size_t   sqlLen = strlen(sql);

  SSqlCmd *pCmd    = &pSql->cmd;
  SSqlRes *pRes    = &pSql->res;
  pSql->param      = (void*) pSql;
  pSql->fp         = waitForQueryRsp;
  pSql->fetchFp    = waitForQueryRsp;
  
  pCmd->insertParam.insertType = TSDB_QUERY_TYPE_STMT_INSERT;
  pCmd->insertParam.objectId = pSql->self;

  char* sqlstr = realloc(pSql->sqlstr, sqlLen + 1);
  if(sqlstr == NULL && pSql->sqlstr) free(pSql->sqlstr);  
  pSql->sqlstr = sqlstr;
  if (pSql->sqlstr == NULL) {
    tscError("%p failed to malloc sql string buffer", pSql);
    STMT_RET(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }

  pRes->qId = 0;
  pRes->numOfRows = 1;

  strtolower(pSql->sqlstr, sql);
  tscDebugL("0x%"PRIx64" SQL: %s", pSql->self, pSql->sqlstr);

  if (tscIsInsertData(pSql->sqlstr)) {
    pStmt->isInsert = true;

    pSql->cmd.insertParam.numOfParams = 0;
    pSql->cmd.batchSize   = 0;

    int32_t ret = stmtParseInsertTbTags(pSql, pStmt);
    if (ret != TSDB_CODE_SUCCESS) {
      STMT_RET(ret);
    }

    ret = stmtValidateValuesFields(&pSql->cmd, pSql->sqlstr);
    if (ret != TSDB_CODE_SUCCESS) {
      STMT_RET(ret);
    }

    if (pStmt->multiTbInsert) {
      STMT_RET(TSDB_CODE_SUCCESS);
    }

    memset(&pStmt->mtb, 0, sizeof(pStmt->mtb));

    int32_t code = tsParseSql(pSql, true);
    if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
      // wait for the callback function to post the semaphore
      tsem_wait(&pSql->rspSem);
      STMT_RET(pSql->res.code);
    }

    STMT_RET(code);
  }

  pStmt->isInsert = false;
  STMT_RET(normalStmtPrepare(pStmt));
}

int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_BIND* tags) {
  STscStmt* pStmt = (STscStmt*)stmt;
  int32_t code = 0;

  STMT_CHECK

  SSqlObj* pSql = pStmt->pSql;
  SSqlCmd* pCmd = &pSql->cmd;

  if (name == NULL) {
    tscError("0x%"PRIx64" name is NULL", pSql->self);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "name is NULL"));
  }

  if (pStmt->multiTbInsert == false || !tscIsInsertData(pSql->sqlstr)) {
    tscError("0x%"PRIx64" not multiple table insert", pSql->self);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "not multiple table insert"));
  }

  if (pStmt->last == STMT_INIT || pStmt->last == STMT_BIND || pStmt->last == STMT_BIND_COL) {
    tscError("0x%"PRIx64" set_tbname_tags status error, last:%d", pSql->self, pStmt->last);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "set_tbname_tags status error"));
  }

  pStmt->last = STMT_SETTBNAME;

  char* tbname = strdup(name);
  if (!tbname) {
    tscError("0x%" PRIx64 " out of memory", pSql->self);
    STMT_RET(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  strntolower(tbname, tbname, (int32_t)strlen(tbname));  // N.B. use lowercase in following codes
  uint32_t nameLen = (uint32_t)strlen(tbname);

  uint64_t* uid = (uint64_t*)taosHashGet(pStmt->mtb.pTableHash, tbname, nameLen);
  if (uid != NULL) {
    pStmt->mtb.currentUid = *uid;

    STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pStmt->mtb.pTableBlockHashList, (const char*)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid));
    if (t1 == NULL) {
      tscError("0x%"PRIx64" no table data block in hash list, uid:%" PRId64 , pSql->self, pStmt->mtb.currentUid);
      tfree(tbname);
      STMT_RET(TSDB_CODE_TSC_APP_ERROR);
    }

    if ((*t1)->pData == NULL) {
      code = tscCreateDataBlockData(*t1, TSDB_PAYLOAD_SIZE, (*t1)->pTableMeta->tableInfo.rowSize, sizeof(SSubmitBlk));
      if (code != TSDB_CODE_SUCCESS) {
        tfree(tbname);
        STMT_RET(code);
      }
    }

    SSubmitBlk* pBlk = (SSubmitBlk*) (*t1)->pData;
    pCmd->batchSize = pBlk->numOfRows;
    if (pBlk->numOfRows == 0) {
      (*t1)->prevTS = INT64_MIN;
    }

    tsSetBlockInfo(pBlk, (*t1)->pTableMeta, pBlk->numOfRows);

    taosHashPut(pCmd->insertParam.pTableBlockHashList, (void *)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid), (void*)t1, POINTER_BYTES);

    tscDebug("0x%" PRIx64 " table:%s is already prepared, uid:%" PRIu64, pSql->self, tbname, pStmt->mtb.currentUid);
    tfree(tbname);
    STMT_RET(TSDB_CODE_SUCCESS);
  }

  if (pStmt->mtb.subSet && taosHashGetSize(pStmt->mtb.pTableHash) > 0) {
    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    char sTableName[TSDB_TABLE_FNAME_LEN] = {0};
    tstrncpy(sTableName, pTableMeta->sTableName, sizeof(sTableName));

    SStrToken tname = {0};
    tname.type = TK_STRING;
    tname.z = tbname;
    tname.n = nameLen;
    SName fullname = {0};
    tscSetTableFullName(&fullname, &tname, pSql);

    memcpy(&pTableMetaInfo->name, &fullname, sizeof(fullname));

    code = tscGetTableMetaEx(pSql, pTableMetaInfo, false, true);
    if (code != TSDB_CODE_SUCCESS) {
      tfree(tbname);
      STMT_RET(code);
    }

    pTableMeta = pTableMetaInfo->pTableMeta;

    if (strcmp(sTableName, pTableMeta->sTableName)) {
      tscError("0x%"PRIx64" only tables belongs to one stable is allowed", pSql->self);
      tfree(tbname);
      STMT_RET(TSDB_CODE_TSC_APP_ERROR);
    }

    STableDataBlocks* pBlock = NULL;

    insertStmtGenBlock(pStmt, &pBlock, pTableMeta, &pTableMetaInfo->name);

    pCmd->batchSize = 0;
    
    pStmt->mtb.currentUid = pTableMeta->id.uid;
    pStmt->mtb.tbNum++;
    
    taosHashPut(pCmd->insertParam.pTableBlockHashList, (void *)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid), (void*)&pBlock, POINTER_BYTES);
    taosHashPut(pStmt->mtb.pTableBlockHashList, (void *)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid), (void*)&pBlock, POINTER_BYTES);
    taosHashPut(pStmt->mtb.pTableHash, tbname, nameLen, (char*)&pTableMeta->id.uid, sizeof(pTableMeta->id.uid));

    tscDebug("0x%" PRIx64 " table:%s is prepared, uid:%" PRIx64, pSql->self, tbname, pStmt->mtb.currentUid);
    tfree(tbname);
    STMT_RET(TSDB_CODE_SUCCESS);
  }
  if (pStmt->mtb.tagSet) {
    pStmt->mtb.tbname = tscReplaceStrToken(&pSql->sqlstr, &pStmt->mtb.tbname, tbname);
  } else {
    if (tags == NULL) {
      tscError("No tags set");
      tfree(tbname);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "no tags set"));
    }

    int32_t ret = stmtGenInsertStatement(pSql, pStmt, tbname, tags);
    if (ret != TSDB_CODE_SUCCESS) {
      tfree(tbname);
      STMT_RET(ret);
    }
  }

  pStmt->mtb.nameSet = true;

  tscDebug("0x%"PRIx64" SQL: %s", pSql->self, pSql->sqlstr);

  pSql->cmd.insertParam.numOfParams = 0;
  pSql->cmd.batchSize   = 0;

  if (taosHashGetSize(pCmd->insertParam.pTableBlockHashList) > 0) {
    SHashObj* hashList = pCmd->insertParam.pTableBlockHashList;
    pCmd->insertParam.pTableBlockHashList = NULL;
    tscResetSqlCmd(pCmd, false);
    pCmd->insertParam.pTableBlockHashList = hashList;
  }

  code = tsParseSql(pStmt->pSql, true);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    // wait for the callback function to post the semaphore
    tsem_wait(&pStmt->pSql->rspSem);

    code = pStmt->pSql->res.code;
  }

  if (code == TSDB_CODE_SUCCESS) {
    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);

    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    STableDataBlocks* pBlock = NULL;
    code = tscGetDataBlockFromList(pCmd->insertParam.pTableBlockHashList, pTableMeta->id.uid, TSDB_PAYLOAD_SIZE, sizeof(SSubmitBlk),
                              pTableMeta->tableInfo.rowSize, &pTableMetaInfo->name, pTableMeta, &pBlock, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      STMT_RET(code);
    }

    SSubmitBlk* blk = (SSubmitBlk*)pBlock->pData;
    blk->numOfRows = 0;

    pStmt->mtb.currentUid = pTableMeta->id.uid;
    pStmt->mtb.tbNum++;

    taosHashPut(pStmt->mtb.pTableBlockHashList, (void *)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid), (void*)&pBlock, POINTER_BYTES);
    taosHashPut(pStmt->mtb.pTableHash, tbname, nameLen, (char*)&pTableMeta->id.uid, sizeof(pTableMeta->id.uid));

    if (pStmt->mtb.lastBlock == NULL) {
      insertStmtGenLastBlock(&pStmt->mtb.lastBlock, pBlock);
    }

    tscDebug("0x%" PRIx64 " table:%s is prepared, uid:%" PRIx64, pSql->self, tbname, pStmt->mtb.currentUid);
  }

  tfree(tbname);
  STMT_RET(code);
}

int taos_stmt_set_sub_tbname(TAOS_STMT* stmt, const char* name) {
  STscStmt* pStmt = (STscStmt*)stmt;
  STMT_CHECK
  pStmt->mtb.subSet = true;
  return taos_stmt_set_tbname_tags(stmt, name, NULL);
}

int taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name) {
  STscStmt* pStmt = (STscStmt*)stmt;
  STMT_CHECK
  pStmt->mtb.subSet = false;
  return taos_stmt_set_tbname_tags(stmt, name, NULL);
}

int taos_stmt_close(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (pStmt == NULL || pStmt->taos == NULL) {
    STMT_RET(TSDB_CODE_TSC_DISCONNECTED);
  }
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
  } else {
    if (pStmt->multiTbInsert) {
      taosHashCleanup(pStmt->mtb.pTableHash);
      bool rmMeta = false;
      if (pStmt->pSql && pStmt->pSql->res.code != 0) {
        rmMeta = true;
      }
      tscDestroyDataBlock(pStmt->mtb.lastBlock, rmMeta);
      pStmt->mtb.pTableBlockHashList = tscDestroyBlockHashTable(pStmt->mtb.pTableBlockHashList, rmMeta);
      if (pStmt->pSql){
        taosHashCleanup(pStmt->pSql->cmd.insertParam.pTableBlockHashList);
        pStmt->pSql->cmd.insertParam.pTableBlockHashList = NULL;
      }

      taosArrayDestroy(pStmt->mtb.tags);
      tfree(pStmt->mtb.sqlstr);
    }
  }

  taos_free_result(pStmt->pSql);
  tfree(pStmt);
  STMT_RET(TSDB_CODE_SUCCESS);
}

int taos_stmt_bind_param(TAOS_STMT* stmt, TAOS_BIND* tsc_bind) {
  STscStmt* pStmt = (STscStmt*)stmt;
  STMT_CHECK

  if (pStmt->isInsert) {
    if (pStmt->multiTbInsert) {
      if (pStmt->last != STMT_SETTBNAME && pStmt->last != STMT_ADD_BATCH) {
        tscError("0x%"PRIx64" tsc_bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
        STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "tsc_bind param status error"));
      }
    } else {
      if (pStmt->last != STMT_PREPARE && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_EXECUTE) {
        tscError("0x%"PRIx64" tsc_bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
        STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "tsc_bind param status error"));
      }
    }

    pStmt->last = STMT_BIND;

    tscDebug("tableId:%" PRIu64 ", try to bind one row", pStmt->mtb.currentUid);

    STMT_RET(insertStmtBindParam(pStmt, tsc_bind));
  } else {
    STMT_RET(normalStmtBindParam(pStmt, tsc_bind));
  }
}

int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* tsc_bind) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHECK

  if (tsc_bind == NULL || tsc_bind->num <= 0 || tsc_bind->num > INT16_MAX) {
    tscError("0x%"PRIx64" invalid parameter", pStmt->pSql->self);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "invalid bind param"));
  }

  if (!pStmt->isInsert) {
    tscError("0x%"PRIx64" not or invalid batch insert", pStmt->pSql->self);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "not or invalid batch insert"));
  }

  if (pStmt->multiTbInsert) {
    if (pStmt->last != STMT_SETTBNAME && pStmt->last != STMT_ADD_BATCH) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "bind param status error"));
    }
  } else {
    if (pStmt->last != STMT_PREPARE && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_EXECUTE) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "bind param status error"));
    }
  }

  pStmt->last = STMT_BIND;

  STMT_RET(insertStmtBindParamBatch(pStmt, tsc_bind, -1));
}

int taos_stmt_bind_single_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* tsc_bind, int colIdx) {
  STscStmt* pStmt = (STscStmt*)stmt;
  STMT_CHECK

  if (tsc_bind == NULL || tsc_bind->num <= 0 || tsc_bind->num > INT16_MAX || colIdx < 0) {
    tscError("0x%"PRIx64" invalid parameter", pStmt->pSql->self);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "invalid bind param"));
  }

  if (!pStmt->isInsert) {
    tscError("0x%"PRIx64" not or invalid batch insert", pStmt->pSql->self);
    STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "not or invalid batch insert"));
  }

  if (pStmt->multiTbInsert) {
    if (pStmt->last != STMT_SETTBNAME && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_BIND_COL) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "bind param status error"));
    }
  } else {
    if (pStmt->last != STMT_PREPARE && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_BIND_COL && pStmt->last != STMT_EXECUTE) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "bind param status error"));
    }
  }

  pStmt->last = STMT_BIND_COL;

  STMT_RET(insertStmtBindParamBatch(pStmt, tsc_bind, colIdx));
}

int taos_stmt_add_batch(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  STMT_CHECK

  if (pStmt->isInsert) {
    if (pStmt->last != STMT_BIND && pStmt->last != STMT_BIND_COL) {
      tscError("0x%"PRIx64" add batch status error, last:%d", pStmt->pSql->self, pStmt->last);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "add batch status error"));
    }

    pStmt->last = STMT_ADD_BATCH;

    STMT_RET(insertStmtAddBatch(pStmt));
  }

  STMT_RET(TSDB_CODE_COM_OPS_NOT_SUPPORT);
}

int taos_stmt_reset(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (pStmt->isInsert) {
    STMT_RET(insertStmtReset(pStmt));
  }
  STMT_RET(TSDB_CODE_SUCCESS);
}

int taos_stmt_execute(TAOS_STMT* stmt) {
  int ret = 0;
  STscStmt* pStmt = (STscStmt*)stmt;
  STMT_CHECK

  if (pStmt->isInsert) {
    if (pStmt->last != STMT_ADD_BATCH) {
      tscError("0x%"PRIx64" exec status error, last:%d", pStmt->pSql->self, pStmt->last);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "exec status error"));
    }

    pStmt->last = STMT_EXECUTE;

    pStmt->pSql->cmd.insertParam.payloadType = PAYLOAD_TYPE_RAW;
    if (pStmt->multiTbInsert) {
      ret = insertBatchStmtExecute(pStmt);
    } else {
      ret = insertStmtExecute(pStmt);
    }
  } else { // normal stmt query
    char* sql = normalStmtBuildSql(pStmt);
    if (sql == NULL) {
      ret = TSDB_CODE_TSC_OUT_OF_MEMORY;
    } else {
      taosReleaseRef(tscObjRef, pStmt->pSql->self);
      pStmt->pSql = taos_query((TAOS*)pStmt->taos, sql);
      ret = taos_errno(pStmt->pSql);
      free(sql);
    }
  }

  STMT_RET(ret);
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

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHECK

  if (insert) *insert = pStmt->isInsert;

  STMT_RET(TSDB_CODE_SUCCESS);
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHECK

  if (pStmt->isInsert) {
    SSqlObj* pSql = pStmt->pSql;
    SSqlCmd *pCmd = &pSql->cmd;
    *nums = pCmd->insertParam.numOfParams;
    STMT_RET(TSDB_CODE_SUCCESS);
  } else {
    SNormalStmt* normal = &pStmt->normal;
    *nums = normal->numParams;
    STMT_RET(TSDB_CODE_SUCCESS);
  }
}

int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  STscStmt* pStmt = (STscStmt*)stmt;

  STMT_CHECK

  if (pStmt->isInsert) {
    SSqlCmd* pCmd = &pStmt->pSql->cmd;
    STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    if (pCmd->insertParam.pTableBlockHashList == NULL) {
      pCmd->insertParam.pTableBlockHashList = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
    }

    STableDataBlocks* pBlock = NULL;

    int32_t ret =
      tscGetDataBlockFromList(pCmd->insertParam.pTableBlockHashList, pTableMeta->id.uid, TSDB_PAYLOAD_SIZE, sizeof(SSubmitBlk),
          pTableMeta->tableInfo.rowSize, &pTableMetaInfo->name, pTableMeta, &pBlock, NULL);
    if (ret != 0) {
      STMT_RET(ret);
    }

    if (idx<0 || idx>=pBlock->numOfParams) {
      tscError("0x%"PRIx64" param %d: out of range", pStmt->pSql->self, idx);
      STMT_RET(invalidOperationMsg(tscGetErrorMsgPayload(&pStmt->pSql->cmd), "idx out of range"));
    }

    SParamInfo* param = &pBlock->params[idx];
    if (type) *type = param->type;
    if (bytes) *bytes = param->bytes;

    STMT_RET(TSDB_CODE_SUCCESS);
  } else {
    STMT_RET(TSDB_CODE_COM_OPS_NOT_SUPPORT);
  }
}

char *taos_stmt_errstr(TAOS_STMT *stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL) {
    return (char*) tstrerror(terrno);
  }

  return taos_errstr(pStmt->pSql);
}

const char *taos_data_type(int type) {
  switch (type) {
    case TSDB_DATA_TYPE_NULL:            return "TSDB_DATA_TYPE_NULL";
    case TSDB_DATA_TYPE_BOOL:            return "TSDB_DATA_TYPE_BOOL";
    case TSDB_DATA_TYPE_TINYINT:         return "TSDB_DATA_TYPE_TINYINT";
    case TSDB_DATA_TYPE_SMALLINT:        return "TSDB_DATA_TYPE_SMALLINT";
    case TSDB_DATA_TYPE_INT:             return "TSDB_DATA_TYPE_INT";
    case TSDB_DATA_TYPE_BIGINT:          return "TSDB_DATA_TYPE_BIGINT";
    case TSDB_DATA_TYPE_FLOAT:           return "TSDB_DATA_TYPE_FLOAT";
    case TSDB_DATA_TYPE_DOUBLE:          return "TSDB_DATA_TYPE_DOUBLE";
    case TSDB_DATA_TYPE_BINARY:          return "TSDB_DATA_TYPE_BINARY";
    case TSDB_DATA_TYPE_TIMESTAMP:       return "TSDB_DATA_TYPE_TIMESTAMP";
    case TSDB_DATA_TYPE_NCHAR:           return "TSDB_DATA_TYPE_NCHAR";
    default: return "UNKNOWN";
  }
}
