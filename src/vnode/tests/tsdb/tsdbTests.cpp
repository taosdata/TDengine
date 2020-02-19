#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"

TEST(TsdbTest, createTsdbRepo) {
    STSDBCfg *pCfg = (STSDBCfg *)malloc(sizeof(STSDBCfg));

    free(pCfg);

    ASSERT_EQ(1, 2/2);
}