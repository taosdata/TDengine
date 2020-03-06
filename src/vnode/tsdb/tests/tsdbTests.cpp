#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"
#include "tsdbMeta.h"

TEST(TsdbTest, createTable)  {
    STsdbMeta *pMeta = tsdbCreateMeta(100);
    ASSERT_NE(pMeta, nullptr);

    STableCfg config;
    config.tableId.tid = 0;
    config.tableId.uid = 98868728187539L;
    config.numOfCols = 5;
    config.schema = tdNewSchema(config.numOfCols);
    for (int i = 0; i < schemaNCols(config.schema); i++) {
      SColumn *pCol = tdNewCol(TD_DATATYPE_BIGINT, i, 0);
      tdColCpy(schemaColAt(config.schema, i), pCol);
      tdFreeCol(pCol);
    }
    config.tagValues = nullptr;

    tsdbCreateTableImpl(pMeta, &config);

    STable *pTable = tsdbGetTableByUid(pMeta, config.tableId.uid);
    ASSERT_NE(pTable, nullptr);
}

TEST(TsdbTest, createRepo)  {
  STsdbCfg *pCfg = tsdbCreateDefaultCfg();

  tsdb_repo_t *pRepo = tsdbCreateRepo("/root/mnt/test/vnode0", pCfg, NULL);
  ASSERT_NE(pRepo, nullptr);
  tsdbFreeCfg(pCfg);

  STableCfg config;
  config.tableId.tid = 0;
  config.tableId.uid = 98868728187539L;
  config.numOfCols = 5;
  config.schema = tdNewSchema(config.numOfCols);
  SColumn *pCol = tdNewCol(TD_DATATYPE_TIMESTAMP, 0, 0);
  tdColCpy(schemaColAt(config.schema, 0), pCol);
  tdFreeCol(pCol);
  for (int i = 1; i < schemaNCols(config.schema); i++) {
    pCol = tdNewCol(TD_DATATYPE_BIGINT, i, 0);
    tdColCpy(schemaColAt(config.schema, i), pCol);
    tdFreeCol(pCol);
  }

  tsdbCreateTable(pRepo, &config);
  tdFreeSchema(config.schema);

  tsdbDropRepo(pRepo);
}