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
      if (strncasecmp(pSchema[i].name, pSchema[j].name, sizeof(pSchema[i].name) - 1) == 0) {
        return false;
      }
    }

    rowLen += pSchema[i].bytes;
  }

  // valid total length
  return (rowLen <= TSDB_MAX_BYTES_PER_ROW);
}

SSchema* tscGetTableColumnSchema(const STableMeta* pTableMeta, int32_t colIndex) {
  assert(pTableMeta != NULL);
  
  SSchema* pSchema = (SSchema*) pTableMeta->schema;
  return &pSchema[colIndex];
}

// TODO for large number of columns, employ the binary search method
SSchema* tscGetColumnSchemaById(STableMeta* pTableMeta, int16_t colId) {
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);

  for(int32_t i = 0; i < tinfo.numOfColumns + tinfo.numOfTags; ++i) {
    if (pTableMeta->schema[i].colId == colId) {
      return &pTableMeta->schema[i];
    }
  }

  return NULL;
}

static void tscInitCorVgroupInfo(SCorVgroupInfo *corVgroupInfo, SVgroupInfo *vgroupInfo) {
  corVgroupInfo->version = 0;
  corVgroupInfo->inUse = 0;
  corVgroupInfo->numOfEps = vgroupInfo->numOfEps;
  for (int32_t i = 0; i < corVgroupInfo->numOfEps; i++) {
    corVgroupInfo->epAddr[i].fqdn = strdup(vgroupInfo->epAddr[i].fqdn);
    corVgroupInfo->epAddr[i].port = vgroupInfo->epAddr[i].port;
  }
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
  
  pTableMeta->id.tid = pTableMetaMsg->tid;
  pTableMeta->id.uid = pTableMetaMsg->uid;

  SVgroupInfo* pVgroupInfo = &pTableMeta->vgroupInfo;
  pVgroupInfo->numOfEps = pTableMetaMsg->vgroup.numOfEps;
  pVgroupInfo->vgId = pTableMetaMsg->vgroup.vgId;

  for(int32_t i = 0; i < pVgroupInfo->numOfEps; ++i) {
    SEpAddrMsg* pEpMsg = &pTableMetaMsg->vgroup.epAddr[i];

    pVgroupInfo->epAddr[i].fqdn = strndup(pEpMsg->fqdn, tListLen(pEpMsg->fqdn));
    pVgroupInfo->epAddr[i].port = pEpMsg->port;
  }

  tscInitCorVgroupInfo(&pTableMeta->corVgroupInfo, pVgroupInfo);

  pTableMeta->sversion = pTableMetaMsg->sversion;
  pTableMeta->tversion = pTableMetaMsg->tversion;
  tstrncpy(pTableMeta->sTableId, pTableMetaMsg->sTableId, TSDB_TABLE_FNAME_LEN);
  
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

// todo refactor
UNUSED_FUNC static FORCE_INLINE char* skipSegments(char* input, char delim, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    while (*input != 0 && *input++ != delim) {
    };
  }
  return input;
}

UNUSED_FUNC static FORCE_INLINE size_t copy(char* dst, const char* src, char delimiter) {
  size_t len = 0;
  while (*src != delimiter && *src != 0) {
    *dst++ = *src++;
    len++;
  }
  
  return len;
}

