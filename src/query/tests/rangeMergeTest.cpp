#include <gtest/gtest.h>
#include <iostream>

#include "qResultbuf.h"
#include "taos.h"
#include "taosdef.h"

#include "qFilter.h"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"

extern "C" {
  extern int32_t filterAddMergeRange(void* h, SFilterRange* ra, int32_t optr);
}

namespace {


void intDataTest() {
  printf("running %s\n", __FUNCTION__);
  int32_t asize = 0;
  SFilterRange ra = {0};
  int64_t *s =NULL;
  int64_t *e =NULL;
  int64_t s0[3] = {-100, 1, 3};
  int64_t e0[3] = {0   , 2, 4};
  int64_t s1[3] = {INT64_MIN, 0 , 3};
  int64_t e1[3] = {100      , 50, 4};
  int64_t s2[5] = {1 , 3  , 10,30,70};
  int64_t e2[5] = {10, 100, 20,50,120};
  int64_t s3[3] = {1 , 20 , 5};
  int64_t e3[3] = {10, 100, 25};
  int64_t s4[2] = {10, 0};
  int64_t e4[2] = {20, 5};
  int64_t s5[3] = {0, 6 ,7};
  int64_t e5[5] = {4, 10,20};

  int64_t rs[10];
  int64_t re[10];

  int32_t num = 0;

  s = s0;
  e = e0;
  asize = sizeof(s0)/sizeof(s[0]);
  void *h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    filterAddMergeRange(h, &ra, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 3);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], -100);
  ASSERT_EQ(re[0], 0);
  ASSERT_EQ(rs[1], 1);
  ASSERT_EQ(re[1], 2);
  ASSERT_EQ(rs[2], 3);
  ASSERT_EQ(re[2], 4);  
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, MR_OPT_TS);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], -100);
  ASSERT_EQ(re[0], 4);
  filterFreeMergeRange(h);


  s = s1;
  e = e1;
  asize = sizeof(s1)/sizeof(s[0]);
  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];

    filterAddMergeRange(h, &ra, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], 3);
  ASSERT_EQ(re[0], 4);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], INT64_MIN);
  ASSERT_EQ(re[0], 100);
  filterFreeMergeRange(h);


  s = s2;
  e = e2;
  asize = sizeof(s2)/sizeof(s[0]);
  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], 1);
  ASSERT_EQ(re[0], 120);
  filterFreeMergeRange(h);



  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, i % 2 ? TSDB_RELATION_OR : TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, i % 2 ? TSDB_RELATION_AND : TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], 70);
  ASSERT_EQ(re[0], 120);  
  filterFreeMergeRange(h);


  s = s3;
  e = e3;
  asize = sizeof(s3)/sizeof(s[0]);
  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], 1);
  ASSERT_EQ(re[0], 100);
  filterFreeMergeRange(h);




  s = s4;
  e = e4;
  asize = sizeof(s4)/sizeof(s[0]);
  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 2);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], 0);
  ASSERT_EQ(re[0], 5);
  ASSERT_EQ(rs[1], 10);
  ASSERT_EQ(re[1], 20);
  filterFreeMergeRange(h);


  s = s5;
  e = e5;
  asize = sizeof(s5)/sizeof(s[0]);
  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_AND);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 2);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], 0);
  ASSERT_EQ(re[0], 4);
  ASSERT_EQ(rs[1], 6);
  ASSERT_EQ(re[1], 20);
  filterFreeMergeRange(h);


  h = filterInitMergeRange(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra.s = s[i];
    ra.e = e[i];
    
    filterAddMergeRange(h, &ra, (i == (asize -1)) ? TSDB_RELATION_AND : TSDB_RELATION_OR);
  }
  filterGetMergeRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetMergeRangeRes(h, rs, re);
  ASSERT_EQ(rs[0], 7);
  ASSERT_EQ(re[0], 10);
  filterFreeMergeRange(h);


}


}  // namespace

TEST(testCase, rangeMergeTest) {
  intDataTest();

}
