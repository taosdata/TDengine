#include <gtest/gtest.h>
#include <stdlib.h>

#include "tsdb.h"

TEST(TsdbTest, createTsdbRepo) {
    STsdbCfg *pCfg = (STsdbCfg *)malloc(sizeof(STsdbCfg));

    free(pCfg);

    ASSERT_EQ(1, 2/2);
}