
#include "os.h"
#include "taosmsg.h"
#include "qTableMeta.h"
#include "ttokendef.h"
#include "taosdef.h"
#include "tutil.h"

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
  assert(pTableMetaMsg != NULL && pTableMetaMsg->numOfColumns >= 2);

  int32_t schemaSize = (pTableMetaMsg->numOfColumns + pTableMetaMsg->numOfTags) * sizeof(SSchema);
  STableMeta* pTableMeta = calloc(1, sizeof(STableMeta) + schemaSize);

  pTableMeta->tableType = pTableMetaMsg->tableType;
  pTableMeta->vgId      = pTableMetaMsg->vgroup.vgId;
  pTableMeta->suid      = pTableMetaMsg->suid;

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

