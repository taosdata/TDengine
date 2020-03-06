#include <gtest/gtest.h>
#include <stdlib.h>

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