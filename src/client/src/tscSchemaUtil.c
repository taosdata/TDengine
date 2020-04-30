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
#include "taosmsg.h"
#include "tschemautil.h"
#include "ttokendef.h"
#include "taosdef.h"
#include "tutil.h"
#include "tsclient.h"

int32_t tscGetNumOfTags(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  
  if (pTableMeta->tableType == TSDB_NORMAL_TABLE) {
    assert(tinfo.numOfTags == 0);
    return 0;
  }
  
  if (pTableMeta->tableType == TSDB_SUPER_TABLE || pTableMeta->tableType == TSDB_CHILD_TABLE) {
    assert(tinfo.numOfTags >= 0);
    return tinfo.numOfTags;
  }
  
  assert(tinfo.numOfTags == 0);
  return 0;
}

int32_t tscGetNumOfColumns(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  
  // table created according to super table, use data from super table
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  return tinfo.numOfColumns;
}

SSchema *tscGetTableSchema(const STableMeta *pTableMeta) {
  assert(pTableMeta != NULL);
  
//  if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
//    STableMeta* pSTableMeta = pTableMeta->pSTable;
//    assert (pSTableMeta != NULL);
//
//    return pSTableMeta->schema;
//  }
  
  return (SSchema*) pTableMeta->schema;
}

SSchema* tscGetTableTagSchema(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL && (pTableMeta->tableType == TSDB_SUPER_TABLE || pTableMeta->tableType == TSDB_CHILD_TABLE));
  
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  assert(tinfo.numOfTags > 0);
  
  return tscGetTableColumnSchema(pTableMeta, tinfo.numOfColumns);
}

STableComInfo tscGetTableInfo(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);

#if 0
  if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
    assert (pTableMeta->pSTable != NULL);
    return pTableMeta->pSTable->tableInfo;
  }
#endif

  return pTableMeta->tableInfo;
}

bool isValidSchema(struct SSchema* pSchema, int32_t numOfCols) {
  if (!VALIDNUMOFCOLS(numOfCols)) {
    return false;
  }

  /* first column must be the timestamp, which is a primary key */
  if (pSchema[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    return false;
  }

  /* type is valid, length is valid */
  int32_t rowLen = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    // 1. valid types
    if (pSchema[i].type > TSDB_DATA_TYPE_TIMESTAMP || pSchema[i].type < TSDB_DATA_TYPE_BOOL) {
      return false;
    }

    // 2. valid length for each type
    if (pSchema[i].type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (pSchema[i].bytes > TSDB_MAX_BINARY_LEN) {
        return false;
      }
    } else {
      if (pSchema[i].bytes != tDataTypeDesc[pSchema[i].type].nSize) {
        return false;
      }
    }

    // 3. valid column names
    for (int32_t j = i + 1; j < numOfCols; ++j) {
      if (strncasecmp(pSchema[i].name, pSchema[j].name, TSDB_COL_NAME_LEN) == 0) {
        return false;
      }
    }

    rowLen += pSchema[i].bytes;
  }

  // valid total length
  return (rowLen <= TSDB_MAX_BYTES_PER_ROW);
}

SSchema* tscGetTableColumnSchema(const STableMeta* pTableMeta, int32_t startCol) {
  assert(pTableMeta != NULL);
  
  SSchema* pSchema = (SSchema*) pTableMeta->schema;
#if 0
  if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
    assert (pTableMeta->pSTable != NULL);
    pSchema = pTableMeta->pSTable->schema;
  }
#endif

  return &pSchema[startCol];
}

struct SSchema tscGetTbnameColumnSchema() {
  struct SSchema s = {
      .colId = TSDB_TBNAME_COLUMN_INDEX,
      .type  = TSDB_DATA_TYPE_BINARY,
      .bytes = TSDB_TABLE_NAME_LEN
  };
  
  strcpy(s.name, TSQL_TBNAME_L);
  return s;
}

STableMeta* tscCreateTableMetaFromMsg(STableMetaMsg* pTableMetaMsg, size_t* size) {
  assert(pTableMetaMsg != NULL);
  
  int32_t schemaSize = (pTableMetaMsg->numOfColumns + pTableMetaMsg->numOfTags) * sizeof(SSchema);
  STableMeta* pTableMeta = calloc(1, sizeof(STableMeta) + schemaSize);
  pTableMeta->tableType = pTableMetaMsg->tableType;
  
  pTableMeta->tableInfo = (STableComInfo) {
    .numOfTags    = pTableMetaMsg->numOfTags,
    .precision    = pTableMetaMsg->precision,
    .numOfColumns = pTableMetaMsg->numOfColumns,
  };
  
  pTableMeta->sid = pTableMetaMsg->sid;
  pTableMeta->uid = pTableMetaMsg->uid;
  pTableMeta->vgroupInfo = pTableMetaMsg->vgroup;
  
  memcpy(pTableMeta->schema, pTableMetaMsg->schema, schemaSize);
  
  int32_t numOfTotalCols = pTableMeta->tableInfo.numOfColumns;
  for(int32_t i = 0; i < numOfTotalCols; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }
  
  if (size != NULL) {
    *size = sizeof(STableMeta) + schemaSize;
  }
  
  return pTableMeta;
}

/**
 * the TableMeta data format in memory is as follows:
 *
 * +--------------------+
 * |STableMeta Body data|  sizeof(STableMeta)
 * +--------------------+
 * |Schema data         |  numOfTotalColumns * sizeof(SSchema)
 * +--------------------+
 * |Tags data           |  tag_col_1.bytes + tag_col_2.bytes + ....
 * +--------------------+
 *
 * @param pTableMeta
 * @return
 */
char* tsGetTagsValue(STableMeta* pTableMeta) {
  int32_t offset = 0;
//  int32_t  numOfTotalCols = pTableMeta->numOfColumns + pTableMeta->numOfTags;
//  uint32_t offset = sizeof(STableMeta) + numOfTotalCols * sizeof(SSchema);

  return ((char*)pTableMeta + offset);
}

// todo refactor
__attribute__ ((unused))static FORCE_INLINE char* skipSegments(char* input, char delim, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    while (*input != 0 && *input++ != delim) {
    };
  }
  return input;
}

__attribute__ ((unused)) static FORCE_INLINE size_t copy(char* dst, const char* src, char delimiter) {
  size_t len = 0;
  while (*src != delimiter && *src != 0) {
    *dst++ = *src++;
    len++;
  }
  
  return len;
}

/*
 * tablePrefix.columnName
 * extract table name and save it in pTable, with only column name in pToken
 */
void extractTableNameFromToken(SSQLToken* pToken, SSQLToken* pTable) {
  const char sep = TS_PATH_DELIMITER[0];

  if (pToken == pTable || pToken == NULL || pTable == NULL) {
    return;
  }

  char* r = strnchr(pToken->z, sep, pToken->n, false);

  if (r != NULL) {  // record the table name token
    pTable->n = r - pToken->z;
    pTable->z = pToken->z;

    r += 1;
    pToken->n -= (r - pToken->z);
    pToken->z = r;
  }
}
