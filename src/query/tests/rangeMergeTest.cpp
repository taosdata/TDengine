#include <gtest/gtest.h>
#include <iostream>

#include "qResultbuf.h"
#include "taos.h"
#include "taosdef.h"

#include "qFilter.h"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"

namespace {

void intDataTest() {
  printf("running %s\n", __FUNCTION__);
  int32_t s0[3] = {-100, 1, 3};
  int32_t e0[3] = {0   , 2, 4};
  int64_t s1[3] = {INT64_MIN, 0 , 3};
  int64_t e1[3] = {100      , 50, 4};
  int64_t s2[5] = {1 , 3  , 10,30,70};
  int64_t e2[5] = {10, 100, 20,50,120};
  int64_t s3[3] = {1 , 20 , 5};
  int64_t e3[3] = {10, 100, 25};

  int32_t rs0[3];
  int32_t re0[3];
  int64_t rs1[3];
  int64_t re1[3];
  int64_t rs2[5];
  int64_t re2[5];
  int64_t rs3[5];
  int64_t re3[5];

  int32_t num = 0;
  
  void *h = filterInitMergeRange(TSDB_DATA_TYPE_INT, 0);
  for (int32_t i = 0; i < sizeof(s0)/sizeof(s0[0]); ++i) {
    filterAddMergeRange(h, s0 + i, e0 + i, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);



  h = filterInitMergeRange(TSDB_DATA_TYPE_INT, 0);
  for (int32_t i = 0; i < sizeof(s0)/sizeof(s0[0]); ++i) {
    filterAddMergeRange(h, s0 + i, e0 + i, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 3);
  filterGetMergeRangeRes(h, rs0, re0);
  ASSERT_EQ(rs0[0], -100);
  ASSERT_EQ(re0[0], 0);
  ASSERT_EQ(rs0[1], 1);
  ASSERT_EQ(re0[1], 2);
  ASSERT_EQ(rs0[2], 3);
  ASSERT_EQ(re0[2], 4);  
  filterFreeMergeRange(h);



  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s1)/sizeof(s1[0]); ++i) {
    filterAddMergeRange(h, s1 + i, e1 + i, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs1, re1);
  ASSERT_EQ(rs1[0], 3);
  ASSERT_EQ(re1[0], 4);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s1)/sizeof(s1[0]); ++i) {
    filterAddMergeRange(h, s1 + i, e1 + i, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs1, re1);
  ASSERT_EQ(rs1[0], INT64_MIN);
  ASSERT_EQ(re1[0], 100);
  filterFreeMergeRange(h);



  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s2)/sizeof(s2[0]); ++i) {
    filterAddMergeRange(h, s2 + i, e2 + i, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s2)/sizeof(s2[0]); ++i) {
    filterAddMergeRange(h, s2 + i, e2 + i, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs2, re2);
  ASSERT_EQ(rs2[0], 1);
  ASSERT_EQ(re2[0], 120);
  filterFreeMergeRange(h);



  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s2)/sizeof(s2[0]); ++i) {
    filterAddMergeRange(h, s2 + i, e2 + i, i % 2 ? TSDB_RELATION_OR : TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s2)/sizeof(s2[0]); ++i) {
    filterAddMergeRange(h, s2 + i, e2 + i, i % 2 ? TSDB_RELATION_AND : TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs2, re2);
  ASSERT_EQ(rs2[0], 70);
  ASSERT_EQ(re2[0], 120);  
  filterFreeMergeRange(h);



  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s3)/sizeof(s3[0]); ++i) {
    filterAddMergeRange(h, s3 + i, e3 + i, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < sizeof(s3)/sizeof(s3[0]); ++i) {
    filterAddMergeRange(h, s3 + i, e3 + i, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs3, re3);
  ASSERT_EQ(rs3[0], 1);
  ASSERT_EQ(re3[0], 100);
  filterFreeMergeRange(h);



}


}  // namespace

TEST(testCase, rangeMergeTest) {
  intDataTest();

}
