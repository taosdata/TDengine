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
  uint64_t  currentUid;
  uint32_t  tbNum;
  SStrToken tbname;
  SStrToken stbname;
  SStrToken values;
  SArray   *tags;
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

static int normalStmtBindParam(STscStmt* stmt, TAOS_BIND* bind) {
  SNormalStmt* normal = &stmt->normal;

  for (uint16_t i = 0; i < normal->numParams; ++i) {
    TAOS_BIND* tb = bind + i;
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
        tscDebug("0x%"PRIx64" bind column%d: type mismatch or invalid", stmt->pSql->self, i);
        return TSDB_CODE_TSC_INVALID_VALUE;
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
      taosStringBuilderAppendInteger(&sb, var->i64);
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
    if (!spd->cols[i].hasVal) {  // current column do not have any value to insert, set it to null
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
static int doBindParam(STableDataBlocks* pBlock, char* data, SParamInfo* param, TAOS_BIND* bind, int32_t colNum) {
  if (bind->is_null != NULL && *(bind->is_null)) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL: {
            u.v1 = *(int8_t*)bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_TINYINT: {
            u.v1 = *(int8_t*)bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.v1 = (int8_t)*(int16_t*)bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.v1 = (int8_t)*(int32_t*)bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.v1 = (int8_t)*(int64_t*)bind->buffer;
            if (u.v1==0 || u.v1==1) break;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            // "0", "1" convertible
            if (strncmp((const char*)bind->buffer, "0", *bind->length)==0) {
              u.v1 = 0;
              break;
            }
            if (strncmp((const char*)bind->buffer, "1", *bind->length)==0) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            int8_t v = *(int8_t*)bind->buffer;
            u.v1 = v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            int16_t v = *(int16_t*)bind->buffer;
            u.v1 = (int8_t)v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_INT: {
            int32_t v = *(int32_t*)bind->buffer;
            u.v1 = (int8_t)v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t v = *(int64_t*)bind->buffer;
            u.v1 = (int8_t)v;
            if (v >= SCHAR_MIN && v <= SCHAR_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int     n, r;
            r = sscanf((const char*)bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r == 1 && n == strlen((const char*)bind->buffer)) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_SMALLINT: {
            int v = *(int16_t*)bind->buffer;
            u.v2 = (int16_t)v;
          } break;
          case TSDB_DATA_TYPE_INT: {
            int32_t v = *(int32_t*)bind->buffer;
            u.v2 = (int16_t)v;
            if (v >= SHRT_MIN && v <= SHRT_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t v = *(int64_t*)bind->buffer;
            u.v2 = (int16_t)v;
            if (v >= SHRT_MIN && v <= SHRT_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          }
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int     n, r;
            r = sscanf((const char*)bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r == 1 && n == strlen((const char*)bind->buffer)) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_INT: {
            u.v4 = *(int32_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t v = *(int64_t*)bind->buffer;
            u.v4 = (int32_t)v;
            if (v >= INT_MIN && v <= INT_MAX) break;
            return TSDB_CODE_TSC_INVALID_VALUE;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int n,r;
            r = sscanf((const char*)bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r==1 && n==strlen((const char*)bind->buffer)) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            u.f4 = *(int8_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.f4 = *(int16_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.f4 = (float)*(int32_t*)bind->buffer;
            // shall we check equality?
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.f4 = (float)*(int64_t*)bind->buffer;
            // shall we check equality?
          } break;
          case TSDB_DATA_TYPE_FLOAT: {
            u.f4 = *(float*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_DOUBLE: {
            u.f4 = *(float*)bind->buffer;
            // shall we check equality?
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            float v;
            int n,r;
            r = sscanf((const char*)bind->buffer, "%f%n", &v, &n);
            if (r==1 && n==strlen((const char*)bind->buffer)) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            u.v8 = *(int8_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.v8 = *(int16_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.v8 = *(int32_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.v8 = *(int64_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            int64_t v;
            int n,r;
            r = sscanf((const char*)bind->buffer, "%" PRId64 "%n", &v, &n);
            if (r==1 && n==strlen((const char*)bind->buffer)) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT: {
            u.f8 = *(int8_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_SMALLINT: {
            u.f8 = *(int16_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_INT: {
            u.f8 = *(int32_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BIGINT: {
            u.f8 = (double)*(int64_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_FLOAT: {
            u.f8 = *(float*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_DOUBLE: {
            u.f8 = *(double*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            double v;
            int n,r;
            r = sscanf((const char*)bind->buffer, "%lf%n", &v, &n);
            if (r==1 && n==strlen((const char*)bind->buffer)) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_TIMESTAMP: {
            u.v8 = *(int64_t*)bind->buffer;
          } break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR: {
            // is this the correct way to call taosParseTime?
            int32_t len = (int32_t)*bind->length;
            if (taosParseTime(bind->buffer, &u.v8, len, 3, tsDaylight) == TSDB_CODE_SUCCESS) {
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_BINARY: {
            if ((*bind->length) > (uintptr_t)param->bytes) {
              return TSDB_CODE_TSC_INVALID_VALUE;
            }
            short size = (short)*bind->length;
            STR_WITH_SIZE_TO_VARSTR(data + param->offset, bind->buffer, size);
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
        switch (bind->buffer_type) {
          case TSDB_DATA_TYPE_NCHAR: {
            int32_t output = 0;
            if (!taosMbsToUcs4(bind->buffer, *bind->length, varDataVal(data + param->offset), param->bytes - VARSTR_HEADER_SIZE, &output)) {
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

  if (bind->buffer_type != param->type) {
    return TSDB_CODE_TSC_INVALID_VALUE;
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
      if ((*bind->length) > (uintptr_t)param->bytes) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      size = (short)*bind->length;
      STR_WITH_SIZE_TO_VARSTR(data + param->offset, bind->buffer, size);
      return TSDB_CODE_SUCCESS;

    case TSDB_DATA_TYPE_NCHAR: {
      int32_t output = 0;
      if (!taosMbsToUcs4(bind->buffer, *bind->length, varDataVal(data + param->offset), param->bytes - VARSTR_HEADER_SIZE, &output)) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      varDataSetLen(data + param->offset, output);
      return TSDB_CODE_SUCCESS;
    }
    default:
      assert(false);
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  memcpy(data + param->offset, bind->buffer, size);
  if (param->offset == 0) {
    if (tsCheckTimestamp(pBlock, data + param->offset) != TSDB_CODE_SUCCESS) {
      tscError("invalid timestamp");
      return TSDB_CODE_TSC_INVALID_VALUE;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int doBindBatchParam(STableDataBlocks* pBlock, SParamInfo* param, TAOS_MULTI_BIND* bind, int32_t rowNum) {
  if (bind->buffer_type != param->type || !isValidDataType(param->type)) {
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (IS_VAR_DATA_TYPE(param->type) && bind->length == NULL) {
    tscError("BINARY/NCHAR no length");
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  for (int i = 0; i < bind->num; ++i) {
    char* data = pBlock->pData + sizeof(SSubmitBlk) + pBlock->rowSize * (rowNum + i);

    if (bind->is_null != NULL && bind->is_null[i]) {
      setNull(data + param->offset, param->type, param->bytes);
      continue;
    }

    if (!IS_VAR_DATA_TYPE(param->type)) {
      memcpy(data + param->offset, (char *)bind->buffer + bind->buffer_length * i, tDataTypes[param->type].bytes);

      if (param->offset == 0) {
        if (tsCheckTimestamp(pBlock, data + param->offset) != TSDB_CODE_SUCCESS) {
          tscError("invalid timestamp");
          return TSDB_CODE_TSC_INVALID_VALUE;
        }
      }
    } else if (param->type == TSDB_DATA_TYPE_BINARY) {
      if (bind->length[i] > (uintptr_t)param->bytes) {
        tscError("binary length too long, ignore it, max:%d, actual:%d", param->bytes, (int32_t)bind->length[i]);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      int16_t bsize = (short)bind->length[i];
      STR_WITH_SIZE_TO_VARSTR(data + param->offset, (char *)bind->buffer + bind->buffer_length * i, bsize);
    } else if (param->type == TSDB_DATA_TYPE_NCHAR) {
      if (bind->length[i] > (uintptr_t)param->bytes) {
        tscError("nchar string length too long, ignore it, max:%d, actual:%d", param->bytes, (int32_t)bind->length[i]);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      int32_t output = 0;
      if (!taosMbsToUcs4((char *)bind->buffer + bind->buffer_length * i, bind->length[i], varDataVal(data + param->offset), param->bytes - VARSTR_HEADER_SIZE, &output)) {
        tscError("convert nchar string to UCS4_LE failed:%s", (char*)((char *)bind->buffer + bind->buffer_length * i));
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      varDataSetLen(data + param->offset, output);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int insertStmtBindParam(STscStmt* stmt, TAOS_BIND* bind) {
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

    int code = doBindParam(pBlock, data, param, &bind[param->idx], 1);
    if (code != TSDB_CODE_SUCCESS) {
      tscDebug("0x%"PRIx64" bind column %d: type mismatch or invalid", pStmt->pSql->self, param->idx);
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int insertStmtBindParamBatch(STscStmt* stmt, TAOS_MULTI_BIND* bind, int colIdx) {
  SSqlCmd* pCmd = &stmt->pSql->cmd;
  STscStmt* pStmt = (STscStmt*)stmt;
  int rowNum = bind->num;

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

  assert(colIdx == -1 || (colIdx >= 0 && colIdx < pBlock->numOfParams));

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
      if (bind[param->idx].num != rowNum) {
        tscError("0x%"PRIx64" param %d: num[%d:%d] not match", pStmt->pSql->self, param->idx, rowNum, bind[param->idx].num);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      int code = doBindBatchParam(pBlock, param, &bind[param->idx], pCmd->batchSize);
      if (code != TSDB_CODE_SUCCESS) {
        tscError("0x%"PRIx64" bind column %d: type mismatch or invalid", pStmt->pSql->self, param->idx);
        return code;
      }
    }

    pCmd->batchSize += rowNum - 1;
  } else {
    SParamInfo* param = &pBlock->params[colIdx];

    int code = doBindBatchParam(pBlock, param, bind, pCmd->batchSize);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("0x%"PRIx64" bind column %d: type mismatch or invalid", pStmt->pSql->self, param->idx);
      return code;
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
    return TSDB_CODE_TSC_APP_ERROR;
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
    return TSDB_CODE_TSC_INVALID_VALUE;
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

  int code = tscMergeTableDataBlocks(stmt->pSql, false);
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

/*
  STableDataBlocks** p = taosHashIterate(pCmd->insertParam.pTableBlockHashList, NULL);

  STableDataBlocks* pOneTableBlock = *p;

  while (1) {
    SSubmitBlk* pBlocks = (SSubmitBlk*) pOneTableBlock->pData;

    pOneTableBlock->size = sizeof(SSubmitBlk);

    pBlocks->numOfRows = 0;

    p = taosHashIterate(pCmd->insertParam.pTableBlockHashList, p);
    if (p == NULL) {
      break;
    }

    pOneTableBlock = *p;
  }
*/

  pCmd->insertParam.pDataBlocks = tscDestroyBlockArrayList(pCmd->insertParam.pDataBlocks);
  pCmd->insertParam.numOfTables = 0;

  taosHashEmpty(pCmd->insertParam.pTableBlockHashList);
  tscFreeSqlResult(pSql);
  tscFreeSubobj(pSql);
  tfree(pSql->pSubs);
  pSql->subState.numOfSub = 0;
}

static int insertBatchStmtExecute(STscStmt* pStmt) {
  int32_t code = 0;

  if(pStmt->mtb.nameSet == false) {
    tscError("0x%"PRIx64" no table name set", pStmt->pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  pStmt->pSql->retry = pStmt->pSql->maxRetry + 1;  //no retry

  if (taosHashGetSize(pStmt->pSql->cmd.insertParam.pTableBlockHashList) <= 0) { // merge according to vgId
    tscError("0x%"PRIx64" no data block to insert", pStmt->pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  fillTablesColumnsNull(pStmt->pSql);

  if ((code = tscMergeTableDataBlocks(pStmt->pSql, false)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tscHandleMultivnodeInsert(pStmt->pSql);

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // wait for the callback function to post the semaphore
  tsem_wait(&pStmt->pSql->rspSem);

  insertBatchClean(pStmt);

  return pStmt->pSql->res.code;
}


int stmtParseInsertTbTags(SSqlObj* pSql, STscStmt* pStmt) {
  SSqlCmd *pCmd    = &pSql->cmd;
  int32_t ret = TSDB_CODE_SUCCESS;

  if ((ret = tsInsertInitialCheck(pSql)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  int32_t index = 0;
  SStrToken sToken = tStrGetToken(pCmd->curSql, &index, false);
  if (sToken.n == 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (sToken.n == 1 && sToken.type == TK_QUESTION) {
    pStmt->multiTbInsert = true;
    pStmt->mtb.tbname = sToken;
    pStmt->mtb.nameSet = false;
    if (pStmt->mtb.pTableHash == NULL) {
      pStmt->mtb.pTableHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
    }

    if (pStmt->mtb.pTableBlockHashList == NULL) {
      pStmt->mtb.pTableBlockHashList = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
    }

    pStmt->mtb.tagSet = true;

    sToken = tStrGetToken(pCmd->curSql, &index, false);
    if (sToken.n > 0 && sToken.type == TK_VALUES) {
      return TSDB_CODE_SUCCESS;
    }

    if (sToken.n <= 0 || sToken.type != TK_USING) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    sToken = tStrGetToken(pCmd->curSql, &index, false);
    if (sToken.n <= 0 || ((sToken.type != TK_ID) && (sToken.type != TK_STRING))) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    pStmt->mtb.stbname = sToken;

    sToken = tStrGetToken(pCmd->curSql, &index, false);
    if (sToken.n <= 0 || sToken.type != TK_TAGS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    sToken = tStrGetToken(pCmd->curSql, &index, false);
    if (sToken.n <= 0 || sToken.type != TK_LP) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    pStmt->mtb.tags = taosArrayInit(4, sizeof(SStrToken));

    int32_t loopCont = 1;

    while (loopCont) {
      sToken = tStrGetToken(pCmd->curSql, &index, false);
      if (sToken.n <= 0) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      switch (sToken.type) {
        case TK_RP:
          loopCont = 0;
          break;
        case TK_VALUES:
          return TSDB_CODE_TSC_INVALID_OPERATION;
        case TK_QUESTION:
          pStmt->mtb.tagSet = false; //continue
        default:
          taosArrayPush(pStmt->mtb.tags, &sToken);
          break;
      }
    }

    if (taosArrayGetSize(pStmt->mtb.tags) <= 0) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    sToken = tStrGetToken(pCmd->curSql, &index, false);
    if (sToken.n <= 0 || sToken.type != TK_VALUES) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
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
    len = (size_t)snprintf(str, size - 1, "insert into %s using %.*s tags(", name, pStmt->mtb.stbname.n, pStmt->mtb.stbname.z);
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
            tscError("empty");
            return TSDB_CODE_TSC_APP_ERROR;
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

  free(pSql->sqlstr);
  pSql->sqlstr = str;

  return TSDB_CODE_SUCCESS;
}



////////////////////////////////////////////////////////////////////////////////
// interface functions

TAOS_STMT* taos_stmt_init(TAOS* taos) {
  STscObj* pObj = (STscObj*)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("connection disconnected");
    return NULL;
  }

  STscStmt* pStmt = calloc(1, sizeof(STscStmt));
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

  tsem_init(&pSql->rspSem, 0, 0);
  pSql->signature = pSql;
  pSql->pTscObj   = pObj;
  pSql->maxRetry  = TSDB_MAX_REPLICA;
  pSql->isBind    = true;
  pStmt->pSql     = pSql;
  pStmt->last     = STMT_INIT;

  return pStmt;
}

int taos_stmt_prepare(TAOS_STMT* stmt, const char* sql, unsigned long length) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL || pStmt->taos == NULL || pStmt->pSql == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (pStmt->last != STMT_INIT) {
    tscError("prepare status error, last:%d", pStmt->last);
    return TSDB_CODE_TSC_APP_ERROR;
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

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    tscError("%p failed to malloc payload buffer", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pSql->sqlstr = realloc(pSql->sqlstr, sqlLen + 1);

  if (pSql->sqlstr == NULL) {
    tscError("%p failed to malloc sql string buffer", pSql);
    free(pCmd->payload);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pRes->qId = 0;
  pRes->numOfRows = 1;

  strtolower(pSql->sqlstr, sql);
  tscDebugL("%p SQL: %s", pSql, pSql->sqlstr);

  if (tscIsInsertData(pSql->sqlstr)) {
    pStmt->isInsert = true;

    pSql->cmd.numOfParams = 0;
    pSql->cmd.batchSize   = 0;

    registerSqlObj(pSql);

    int32_t ret = stmtParseInsertTbTags(pSql, pStmt);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if (pStmt->multiTbInsert) {
      return TSDB_CODE_SUCCESS;
    }

    memset(&pStmt->mtb, 0, sizeof(pStmt->mtb));

    int32_t code = tsParseSql(pSql, true);
    if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
      // wait for the callback function to post the semaphore
      tsem_wait(&pSql->rspSem);
      return pSql->res.code;
    }

    return code;
  }

  pStmt->isInsert = false;
  return normalStmtPrepare(pStmt);
}

int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_BIND* tags) {
  STscStmt* pStmt = (STscStmt*)stmt;
  SSqlObj* pSql = pStmt->pSql;
  SSqlCmd* pCmd = &pSql->cmd;

  if (stmt == NULL || pStmt->pSql == NULL || pStmt->taos == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (name == NULL) {
    terrno = TSDB_CODE_TSC_APP_ERROR;
    tscError("0x%"PRIx64" name is NULL", pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  if (pStmt->multiTbInsert == false || !tscIsInsertData(pSql->sqlstr)) {
    terrno = TSDB_CODE_TSC_APP_ERROR;
    tscError("0x%"PRIx64" not multi table insert", pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  if (pStmt->last == STMT_INIT || pStmt->last == STMT_BIND || pStmt->last == STMT_BIND_COL) {
    tscError("0x%"PRIx64" settbname status error, last:%d", pSql->self, pStmt->last);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  pStmt->last = STMT_SETTBNAME;

  uint64_t* uid = (uint64_t*)taosHashGet(pStmt->mtb.pTableHash, name, strlen(name));
  if (uid != NULL) {
    pStmt->mtb.currentUid = *uid;

    STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pStmt->mtb.pTableBlockHashList, (const char*)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid));
    if (t1 == NULL) {
      tscError("0x%"PRIx64" no table data block in hash list, uid:%" PRId64 , pSql->self, pStmt->mtb.currentUid);
      return TSDB_CODE_TSC_APP_ERROR;
    }

    SSubmitBlk* pBlk = (SSubmitBlk*) (*t1)->pData;
    pCmd->batchSize = pBlk->numOfRows;

    taosHashPut(pCmd->insertParam.pTableBlockHashList, (void *)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid), (void*)t1, POINTER_BYTES);

    tscDebug("0x%"PRIx64" table:%s is already prepared, uid:%" PRIu64, pSql->self, name, pStmt->mtb.currentUid);
    return TSDB_CODE_SUCCESS;
  }

  if (pStmt->mtb.tagSet) {
    pStmt->mtb.tbname = tscReplaceStrToken(&pSql->sqlstr, &pStmt->mtb.tbname, name);
  } else {
    if (tags == NULL) {
      tscError("No tags set");
      return TSDB_CODE_TSC_APP_ERROR;
    }

    int32_t ret = stmtGenInsertStatement(pSql, pStmt, name, tags);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  pStmt->mtb.nameSet = true;
  pStmt->mtb.tagSet = true;

  tscDebug("0x%"PRIx64" SQL: %s", pSql->self, pSql->sqlstr);

  pSql->cmd.numOfParams = 0;
  pSql->cmd.batchSize   = 0;

  if (taosHashGetSize(pCmd->insertParam.pTableBlockHashList) > 0) {
    SHashObj* hashList = pCmd->insertParam.pTableBlockHashList;
    pCmd->insertParam.pTableBlockHashList = NULL;
    tscResetSqlCmd(pCmd, true);
    pCmd->insertParam.pTableBlockHashList = hashList;
  }

  int32_t code = tsParseSql(pStmt->pSql, true);
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
      return code;
    }

    SSubmitBlk* blk = (SSubmitBlk*)pBlock->pData;
    blk->numOfRows = 0;

    pStmt->mtb.currentUid = pTableMeta->id.uid;
    pStmt->mtb.tbNum++;

    taosHashPut(pStmt->mtb.pTableBlockHashList, (void *)&pStmt->mtb.currentUid, sizeof(pStmt->mtb.currentUid), (void*)&pBlock, POINTER_BYTES);
    taosHashPut(pStmt->mtb.pTableHash, name, strlen(name), (char*) &pTableMeta->id.uid, sizeof(pTableMeta->id.uid));

    tscDebug("0x%"PRIx64" table:%s is prepared, uid:%" PRIx64, pSql->self, name, pStmt->mtb.currentUid);
  }

  return code;
}


int taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name) {
  return taos_stmt_set_tbname_tags(stmt, name, NULL);
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
  } else {
    if (pStmt->multiTbInsert) {
      taosHashCleanup(pStmt->mtb.pTableHash);
      pStmt->mtb.pTableBlockHashList = tscDestroyBlockHashTable(pStmt->mtb.pTableBlockHashList, true);
      taosHashCleanup(pStmt->pSql->cmd.insertParam.pTableBlockHashList);
      pStmt->pSql->cmd.insertParam.pTableBlockHashList = NULL;
      taosArrayDestroy(pStmt->mtb.tags);
    }
  }

  taos_free_result(pStmt->pSql);
  free(pStmt);
  return TSDB_CODE_SUCCESS;
}

int taos_stmt_bind_param(TAOS_STMT* stmt, TAOS_BIND* bind) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (stmt == NULL || pStmt->pSql == NULL || pStmt->taos == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (pStmt->isInsert) {
    if (pStmt->multiTbInsert) {
      if (pStmt->last != STMT_SETTBNAME && pStmt->last != STMT_ADD_BATCH) {
        tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
        return TSDB_CODE_TSC_APP_ERROR;
      }
    } else {
      if (pStmt->last != STMT_PREPARE && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_EXECUTE) {
        tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
        return TSDB_CODE_TSC_APP_ERROR;
      }
    }

    pStmt->last = STMT_BIND;

    return insertStmtBindParam(pStmt, bind);
  } else {
    return normalStmtBindParam(pStmt, bind);
  }
}


int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL || pStmt->pSql == NULL || pStmt->taos == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (bind == NULL || bind->num <= 0 || bind->num > INT16_MAX) {
    tscError("0x%"PRIx64" invalid parameter", pStmt->pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  if (!pStmt->isInsert) {
    tscError("0x%"PRIx64" not or invalid batch insert", pStmt->pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  if (pStmt->multiTbInsert) {
    if (pStmt->last != STMT_SETTBNAME && pStmt->last != STMT_ADD_BATCH) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      return TSDB_CODE_TSC_APP_ERROR;
    }
  } else {
    if (pStmt->last != STMT_PREPARE && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_EXECUTE) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      return TSDB_CODE_TSC_APP_ERROR;
    }
  }

  pStmt->last = STMT_BIND;

  return insertStmtBindParamBatch(pStmt, bind, -1);
}

int taos_stmt_bind_single_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind, int colIdx) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (stmt == NULL || pStmt->pSql == NULL || pStmt->taos == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (bind == NULL || bind->num <= 0 || bind->num > INT16_MAX) {
    tscError("0x%"PRIx64" invalid parameter", pStmt->pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  if (!pStmt->isInsert) {
    tscError("0x%"PRIx64" not or invalid batch insert", pStmt->pSql->self);
    return TSDB_CODE_TSC_APP_ERROR;
  }

  if (pStmt->multiTbInsert) {
    if (pStmt->last != STMT_SETTBNAME && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_BIND_COL) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      return TSDB_CODE_TSC_APP_ERROR;
    }
  } else {
    if (pStmt->last != STMT_PREPARE && pStmt->last != STMT_ADD_BATCH && pStmt->last != STMT_BIND_COL && pStmt->last != STMT_EXECUTE) {
      tscError("0x%"PRIx64" bind param status error, last:%d", pStmt->pSql->self, pStmt->last);
      return TSDB_CODE_TSC_APP_ERROR;
    }
  }

  pStmt->last = STMT_BIND_COL;

  return insertStmtBindParamBatch(pStmt, bind, colIdx);
}



int taos_stmt_add_batch(TAOS_STMT* stmt) {
  STscStmt* pStmt = (STscStmt*)stmt;
  if (stmt == NULL || pStmt->pSql == NULL || pStmt->taos == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (pStmt->isInsert) {
    if (pStmt->last != STMT_BIND && pStmt->last != STMT_BIND_COL) {
      tscError("0x%"PRIx64" add batch status error, last:%d", pStmt->pSql->self, pStmt->last);
      return TSDB_CODE_TSC_APP_ERROR;
    }

    pStmt->last = STMT_ADD_BATCH;

    return insertStmtAddBatch(pStmt);
  }

  return TSDB_CODE_COM_OPS_NOT_SUPPORT;
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
  if (stmt == NULL || pStmt->pSql == NULL || pStmt->taos == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (pStmt->isInsert) {
    if (pStmt->last != STMT_ADD_BATCH) {
      tscError("0x%"PRIx64" exec status error, last:%d", pStmt->pSql->self, pStmt->last);
      return TSDB_CODE_TSC_APP_ERROR;
    }

    pStmt->last = STMT_EXECUTE;

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
      if (pStmt->pSql != NULL) {
        tscFreeSqlObj(pStmt->pSql);
        pStmt->pSql = NULL;
      }

      pStmt->pSql = taos_query((TAOS*)pStmt->taos, sql);
      ret = taos_errno(pStmt->pSql);
      free(sql);
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

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL || pStmt->taos == NULL || pStmt->pSql == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (insert) *insert = pStmt->isInsert;

  return TSDB_CODE_SUCCESS;
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL || pStmt->taos == NULL || pStmt->pSql == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (pStmt->isInsert) {
    SSqlObj* pSql = pStmt->pSql;
    SSqlCmd *pCmd    = &pSql->cmd;
    *nums = pCmd->numOfParams;
    return TSDB_CODE_SUCCESS;
  } else {
    SNormalStmt* normal = &pStmt->normal;
    *nums = normal->numParams;
    return TSDB_CODE_SUCCESS;
  }
}

int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  STscStmt* pStmt = (STscStmt*)stmt;

  if (stmt == NULL || pStmt->taos == NULL || pStmt->pSql == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

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
      // todo handle error
    }

    if (idx<0 || idx>=pBlock->numOfParams) {
      tscError("0x%"PRIx64" param %d: out of range", pStmt->pSql->self, idx);
      abort();
    }

    SParamInfo* param = &pBlock->params[idx];
    if (type) *type = param->type;
    if (bytes) *bytes = param->bytes;

    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_TSC_APP_ERROR;
  }
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

