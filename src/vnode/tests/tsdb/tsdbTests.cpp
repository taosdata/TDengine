#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"

TEST(TsdbTest, createTsdbRepo) {
  STsdbCfg config;

  config.precision = TSDB_PRECISION_MILLI;
  config.tsdbId = 0;
  config.maxTables = 100;
  config.daysPerFile = 10;
  config.keep = 3650;
  config.minRowsPerFileBlock = 100;
  config.maxRowsPerFileBlock = 4096;
  config.maxCacheSize = 4 * 1024 * 1024;

  tsdb_repo_t *pRepo = tsdbCreateRepo("/root/mnt/test/vnode0", &config, NULL);

  ASSERT_NE(pRepo, nullptr);

  tsdbCloseRepo(pRepo);
}