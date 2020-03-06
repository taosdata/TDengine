#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"
#include "tsdbMeta.h"

TEST(TsdbTest, createTable) {
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

TEST(TsdbTest, createRepo) {
  STsdbCfg *pCfg = tsdbCreateDefaultCfg();

  // Create a tsdb repository
  tsdb_repo_t *pRepo = tsdbCreateRepo("/root/mnt/test/vnode0", pCfg, NULL);
  ASSERT_NE(pRepo, nullptr);
  tsdbFreeCfg(pCfg);

  // create a normal table in this repository
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
  // Write some data

  int32_t size = sizeof(SSubmitMsg) + sizeof(SSubmitBlock) + tdMaxRowDataBytes(config.schema) * 10 + sizeof(int32_t);

  SSubmitMsg *pMsg = (SSubmitMsg *)malloc(size);
  pMsg->numOfTables = 1;  // TODO: use api

  SSubmitBlock *pBlock = (SSubmitBlock *)pMsg->data;
  pBlock->tableId = {.uid = 98868728187539L, .tid = 0};
  pBlock->sversion = 0;
  pBlock->len = sizeof(SSubmitBlock);

  SDataRows rows = pBlock->data;
  dataRowsInit(rows);

  SDataRow row = tdNewDataRow(tdMaxRowDataBytes(config.schema));
  int64_t ttime = 1583508800000;
  void *pDst = pBlock->data;
  for (int i = 0; i < 10; i++) {  // loop over rows
    ttime += (10000 * i);
    tdDataRowReset(row);
    for (int j = 0; j < schemaNCols(config.schema); j++) {
      if (j == 0) {  // set time stamp
        tdAppendColVal(row, (void *)(&ttime), schemaColAt(config.schema, j), 24);
      } else {       // set other fields
        int val = 10;
        tdAppendColVal(row, (void *)(&val), schemaColAt(config.schema, j), 24);
      }
    }

    dataRowCpy((void *)pDst, row);
    pDst += dataRowLen(row);
  }

  tsdbInsertData(pRepo, pMsg);

  tdFreeDataRow(row);

  tdFreeSchema(config.schema);
  tsdbDropRepo(pRepo);
}