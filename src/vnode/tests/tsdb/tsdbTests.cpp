#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"

TEST(TsdbTest, createTsdbRepo) {
    STSDBCfg *pCfg = (STSDBCfg *)malloc(sizeof(STSDBCfg));

    pCfg->rootDir = "/var/lib/taos/";

    int32_t err_num = 0;
    
    tsdb_repo_t *pRepo = tsdbCreateRepo(pCfg, &err_num);
    ASSERT_EQ(pRepo, NULL);
}