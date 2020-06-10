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
#include "tsqldef.h"
#include "ttypes.h"
#include "tutil.h"

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

struct SSchema* tsGetSchema(SMeterMeta* pMeta) {
  if (pMeta == NULL) {
    return NULL;
  }
  return tsGetColumnSchema(pMeta, 0);
}

struct SSchema* tsGetTagSchema(SMeterMeta* pMeta) {
  if (pMeta == NULL || pMeta->numOfTags == 0) {
    return NULL;
  }

  return tsGetColumnSchema(pMeta, pMeta->numOfColumns);
}

struct SSchema* tsGetColumnSchema(SMeterMeta* pMeta, int32_t startCol) {
  return (SSchema*)(((char*)pMeta + sizeof(SMeterMeta)) + startCol * sizeof(SSchema));
}

struct SSchema tsGetTbnameColumnSchema() {
  struct SSchema s = {.colId = TSDB_TBNAME_COLUMN_INDEX, .type = TSDB_DATA_TYPE_BINARY, .bytes = TSDB_METER_NAME_LEN};
  strcpy(s.name, TSQL_TBNAME_L);
  
  return s;
}

/**
 * the MeterMeta data format in memory is as follows:
 *
 * +--------------------+
 * |SMeterMeta Body data|  sizeof(SMeterMeta)
 * +--------------------+
 * |Schema data         |  numOfTotalColumns * sizeof(SSchema)
 * +--------------------+
 * |Tags data           |  tag_col_1.bytes + tag_col_2.bytes + ....
 * +--------------------+
 *
 * @param pMeta
 * @return
 */
char* tsGetTagsValue(SMeterMeta* pMeta) {
  int32_t  numOfTotalCols = pMeta->numOfColumns + pMeta->numOfTags;
  uint32_t offset = sizeof(SMeterMeta) + numOfTotalCols * sizeof(SSchema);

  return ((char*)pMeta + offset);
}

bool tsMeterMetaIdentical(SMeterMeta* p1, SMeterMeta* p2) {
  if (p1 == NULL || p2 == NULL || p1->uid != p2->uid || p1->sversion != p2->sversion) {
    return false;
  }

  if (p1 == p2) {
    return true;
  }

  size_t size = sizeof(SMeterMeta) + p1->numOfColumns * sizeof(SSchema);

  for (int32_t i = 0; i < p1->numOfTags; ++i) {
    SSchema* pColSchema = tsGetColumnSchema(p1, i + p1->numOfColumns);
    size += pColSchema->bytes;
  }

  return memcmp(p1, p2, size) == 0;
}

// todo refactor
static FORCE_INLINE char* skipSegments(char* input, char delim, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    while (*input != 0 && *input++ != delim) {
    };
  }
  return input;
}

static FORCE_INLINE size_t copy(char* dst, const char* src, char delimiter) {
  size_t len = 0;
  while (*src != delimiter && *src != 0) {
    *dst++ = *src++;
    len++;
  }
  
  return len;
}

/**
 * extract table name from meterid, which the format of userid.dbname.metername
 * @param meterId
 * @return
 */
void extractTableName(char* meterId, char* name) {
  char* r = skipSegments(meterId, TS_PATH_DELIMITER[0], 2);
  size_t len = copy(name, r, TS_PATH_DELIMITER[0]);
  name[len] = 0;
}

SSQLToken extractDBName(char* meterId, char* name) {
  char* r = skipSegments(meterId, TS_PATH_DELIMITER[0], 1);
  size_t len = copy(name, r, TS_PATH_DELIMITER[0]);

  SSQLToken token = {.z = name, .n = len, .type = TK_STRING};
  return token;
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
