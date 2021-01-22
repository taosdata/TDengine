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

STableMeta* tscCreateTableMetaFromMsg(STableMetaMsg* pTableMetaMsg) {
  assert(pTableMetaMsg != NULL && pTableMetaMsg->numOfColumns >= 2 && pTableMetaMsg->numOfTags >= 0);
  
  int32_t schemaSize = (pTableMetaMsg->numOfColumns + pTableMetaMsg->numOfTags) * sizeof(SSchema);
  STableMeta* pTableMeta = calloc(1, sizeof(STableMeta) + schemaSize);

  pTableMeta->tableType = pTableMetaMsg->tableType;
  pTableMeta->vgId      = pTableMetaMsg->vgroup.vgId;

  pTableMeta->tableInfo = (STableComInfo) {
    .numOfTags    = pTableMetaMsg->numOfTags,
    .precision    = pTableMetaMsg->precision,
    .numOfColumns = pTableMetaMsg->numOfColumns,
  };
  
  pTableMeta->id.tid = pTableMetaMsg->tid;
  pTableMeta->id.uid = pTableMetaMsg->uid;

  pTableMeta->sversion = pTableMetaMsg->sversion;
  pTableMeta->tversion = pTableMetaMsg->tversion;

  tstrncpy(pTableMeta->sTableName, pTableMetaMsg->sTableName, TSDB_TABLE_FNAME_LEN);
  
  memcpy(pTableMeta->schema, pTableMetaMsg->schema, schemaSize);
  
  int32_t numOfTotalCols = pTableMeta->tableInfo.numOfColumns;
  for(int32_t i = 0; i < numOfTotalCols; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }
  
  return pTableMeta;
}

bool vgroupInfoIdentical(SNewVgroupInfo *pExisted, SVgroupMsg* src) {
  assert(pExisted != NULL && src != NULL);
  if (pExisted->numOfEps != src->numOfEps) {
    return false;
  }

  for(int32_t i = 0; i < pExisted->numOfEps; ++i) {
    if (pExisted->ep[i].port != src->epAddr[i].port) {
      return false;
    }

    if (strncmp(pExisted->ep[i].fqdn, src->epAddr[i].fqdn, tListLen(pExisted->ep[i].fqdn)) != 0) {
      return false;
    }
  }

  return true;
}

SNewVgroupInfo createNewVgroupInfo(SVgroupMsg *pVgroupMsg) {
  assert(pVgroupMsg != NULL);

  SNewVgroupInfo info = {0};
  info.numOfEps = pVgroupMsg->numOfEps;
  info.vgId     = pVgroupMsg->vgId;
  info.inUse    = 0;

  for(int32_t i = 0; i < pVgroupMsg->numOfEps; ++i) {
    tstrncpy(info.ep[i].fqdn, pVgroupMsg->epAddr[i].fqdn, TSDB_FQDN_LEN);
    info.ep[i].port = pVgroupMsg->epAddr[i].port;
  }

  return info;
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

