#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"
#include "dataformat.h"

TEST(TsdbTest, createRepo) {
  STsdbCfg config;

  // 1. Create a tsdb repository
  tsdbSetDefaultCfg(&config);
  tsdb_repo_t *pRepo = tsdbCreateRepo("/home/ubuntu/work/ttest/vnode0", &config, NULL);
  ASSERT_NE(pRepo, nullptr);

  // 2. Create a normal table
  STableCfg tCfg;
  ASSERT_EQ(tsdbInitTableCfg(&tCfg, TSDB_SUPER_TABLE, 987607499877672L, 0), -1);
  ASSERT_EQ(tsdbInitTableCfg(&tCfg, TSDB_NTABLE, 987607499877672L, 0), 0);

  int       nCols = 5;
  STSchema *schema = tdNewSchema(nCols);

  for (int i = 0; i < nCols; i++) {
    if (i == 0) {
      tdSchemaAppendCol(schema, TSDB_DATA_TYPE_TIMESTAMP, i, -1);
    } else {
      tdSchemaAppendCol(schema, TSDB_DATA_TYPE_INT, i, -1);
    }
  }

  tsdbTableSetSchema(&tCfg, schema, true);

  tsdbCreateTable(pRepo, &tCfg);

  // 3. Loop to write some simple data
  // int         size = tdMaxRowBytesFromSchema(schema);
  // int         nrows = 100;
  // SSubmitMsg *pMsg = (SSubmitMsg *)malloc(sizeof(SSubmitMsg) + sizeof(SSubmitBlk+ size * nrows);

  // {
  //   // TODO
  // }

  // tsdbInsertData(pRepo, pMsg);
}

