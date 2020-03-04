#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"

TEST(TsdbTest, createTsdbRepo) {
  STsdbCfg *pCfg = tsdbCreateDefaultCfg();


  tsdb_repo_t *pRepo = tsdbCreateRepo("/root/mnt/test/vnode0", pCfg, NULL);

  tsdbFreeCfg(pCfg);

  ASSERT_NE(pRepo, nullptr);

  tsdbCloseRepo(pRepo);
}