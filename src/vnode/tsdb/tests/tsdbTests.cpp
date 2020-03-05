#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdbMeta.h"

TEST(TsdbTest, createTable)  {
    STsdbMeta *pMeta = tsdbCreateMeta(100);
    ASSERT_NE(pMeta, nullptr);

    STableCfg config;
    config.tableId.tid = 0;
    config.tableId.uid = 98868728187539L;
    config.numOfCols = 2;
    config.schema = (SSchema *)malloc(sizeof(SSchema) + sizeof(SColumn) * config.numOfCols);
    config.schema->version = 0;
    config.schema->numOfCols = 2;
    config.schema->numOfTags = 0;
    config.schema->colIdCounter = 1;
    for (int i = 0; i < config.numOfCols; i++) {
      SColumn *pCol = config.schema->columns + i;
      pCol->type = TD_DATATYPE_BIGINT;
      pCol->colId = config.schema->colIdCounter++;
      pCol->offset = 10;
      pCol->colName = strdup("col1");
    }
    config.tagValues = nullptr;

    tsdbCreateTableImpl(pMeta, &config), 0;

    STable *pTable = tsdbGetTableByUid(pMeta, config.tableId.uid);
    ASSERT_NE(pTable, nullptr);
}

TEST(TsdbTest, DISABLED_createTsdbRepo) {
  STsdbCfg *pCfg = tsdbCreateDefaultCfg();


  tsdb_repo_t *pRepo = tsdbCreateRepo("/root/mnt/test/vnode0", pCfg, NULL);

  tsdbFreeCfg(pCfg);

  ASSERT_NE(pRepo, nullptr);

  STableCfg config;
  config.tableId.tid = 0;
  config.tableId.uid = 10889498868728187539;
  config.numOfCols = 2;
  config.schema = (SSchema *)malloc(sizeof(SSchema) + sizeof(SColumn) * config.numOfCols);
  config.schema->version = 0;
  config.schema->numOfCols = 2;
  config.schema->numOfTags = 0;
  config.schema->colIdCounter = 1;
  for (int i = 0; i < config.numOfCols; i++) {
    SColumn *pCol = config.schema->columns + i;
    pCol->type = TD_DATATYPE_BIGINT;
    pCol->colId = config.schema->colIdCounter++;
    pCol->offset = 10;
    pCol->colName = strdup("col1");
  }
  config.tagValues = NULL;

  tsdbCreateTable(pRepo, &config);

  tsdbCloseRepo(pRepo);
}