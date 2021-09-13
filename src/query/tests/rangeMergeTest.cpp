#include <gtest/gtest.h>
#include <iostream>

#include "qResultbuf.h"
#include "taos.h"
#include "taosdef.h"

#include "qFilter.h"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"

extern "C" {
  extern void* filterInitRangeCtx(int32_t type, int32_t options);
  extern int32_t filterGetRangeNum(void* h, int32_t* num);
  extern int32_t filterGetRangeRes(void* h, SFilterRange *ra);
  extern int32_t filterFreeRangeCtx(void* h);
  extern int32_t filterAddRange(void* h, SFilterRange* ra, int32_t optr);
}

namespace {


void intDataTest() {
  printf("running %s\n", __FUNCTION__);
  int32_t asize = 0;
  SFilterRange ra[10] = {0};
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
  int64_t e5[3] = {4, 10,20};

  int64_t rs[10];
  int64_t re[10];

  int32_t num = 0;
  void *h = NULL;

  s = s0;
  e = e0;
  asize = sizeof(s0)/sizeof(s[0]);  
  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    filterAddRange(h, ra, TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 3);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, -100);
  ASSERT_EQ(ra[0].e, 0);
  ASSERT_EQ(ra[1].s, 1);
  ASSERT_EQ(ra[1].e, 2);
  ASSERT_EQ(ra[2].s, 3);
  ASSERT_EQ(ra[2].e, 4);  
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, FI_OPTION_TIMESTAMP);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, -100);
  ASSERT_EQ(ra[0].e, 4);
  filterFreeRangeCtx(h);


  s = s1;
  e = e1;
  asize = sizeof(s1)/sizeof(s[0]);
  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];

    filterAddRange(h, ra, TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, 3);
  ASSERT_EQ(ra[0].e, 4);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, INT64_MIN);
  ASSERT_EQ(ra[0].e, 100);
  filterFreeRangeCtx(h);



  s = s2;
  e = e2;
  asize = sizeof(s2)/sizeof(s[0]);
  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, 1);
  ASSERT_EQ(ra[0].e, 120);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, i % 2 ? TSDB_RELATION_OR : TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, i % 2 ? TSDB_RELATION_AND : TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, 70);
  ASSERT_EQ(ra[0].e, 120);  
  filterFreeRangeCtx(h);


  s = s3;
  e = e3;
  asize = sizeof(s3)/sizeof(s[0]);
  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, 1);
  ASSERT_EQ(ra[0].e, 100);
  filterFreeRangeCtx(h);




  s = s4;
  e = e4;
  asize = sizeof(s4)/sizeof(s[0]);
  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 2);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, 0);
  ASSERT_EQ(ra[0].e, 5);
  ASSERT_EQ(ra[1].s, 10);
  ASSERT_EQ(ra[1].e, 20);
  filterFreeRangeCtx(h);


  s = s5;
  e = e5;
  asize = sizeof(s5)/sizeof(s[0]);
  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 0);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 2);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, 0);
  ASSERT_EQ(ra[0].e, 4);
  ASSERT_EQ(ra[1].s, 6);
  ASSERT_EQ(ra[1].e, 20);
  filterFreeRangeCtx(h);


  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].s = s[i];
    ra[0].e = e[i];
    
    filterAddRange(h, ra, (i == (asize -1)) ? TSDB_RELATION_AND : TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterGetRangeRes(h, ra);
  ASSERT_EQ(ra[0].s, 7);
  ASSERT_EQ(ra[0].e, 10);
  filterFreeRangeCtx(h);



  int64_t s6[2] = {0, 4};
  int64_t e6[2] = {4, 6};
  s = s6;
  e = e6;
  asize = sizeof(s6)/sizeof(s[0]);
  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].eflag = 1;
    ra[1].sflag = 4;

    ra[i].s = s[i];
    ra[i].e = e[i];
    
    filterAddRange(h, ra + i, TSDB_RELATION_AND);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 1);
  filterFreeRangeCtx(h);



  memset(ra, 0, sizeof(ra));
  h = filterInitRangeCtx(TSDB_DATA_TYPE_BIGINT, 0);
  for (int32_t i = 0; i < asize; ++i) {
    ra[0].eflag = 1;
    ra[1].sflag = 1;

    ra[i].s = s[i];
    ra[i].e = e[i];
    
    filterAddRange(h, ra + i, TSDB_RELATION_OR);
  }
  filterGetRangeNum(h, &num);
  ASSERT_EQ(num, 2);
  ASSERT_EQ(ra[0].s, 0);
  ASSERT_EQ(ra[0].e, 4);
  ASSERT_EQ(ra[0].eflag, 1);
  ASSERT_EQ(ra[1].s, 4);
  ASSERT_EQ(ra[1].e, 6);
  ASSERT_EQ(ra[1].sflag, 1);
  filterFreeRangeCtx(h);

}


}  // namespace

TEST(testCase, rangeMergeTest) {
  intDataTest();

}
