#include <gtest/gtest.h>
#include <iostream>

#include "taos.h"
#include "tsdb.h"
#include "qExtbuffer.h"

#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

namespace {
  int32_t comp(const void* p1, const void* p2) {
    int32_t* x1 = (int32_t*) p1;
    int32_t* x2 = (int32_t*) p2;

    if (*x1 == *x2) {
      return 0;
    } else {
      return (*x1 > *x2)? 1:-1;
    }
  }

  int32_t comp1(const void* p1, const void* p2) {
    int32_t ret = strncmp((char*) p1, (char*) p2, 20);

    if (ret == 0) {
      return 0;
    } else {
      return ret > 0 ? 1:-1;
    }
  }
}

TEST(testCase, colunmnwise_sort_test) {
  // void taoscQSort(void** pCols, SSchema* pSchema, int32_t numOfCols, int32_t numOfRows, int32_t index, __compar_fn_t compareFn)
  void* pCols[2] = {0};

  SSchema s[2] = {{0}};
  s[0].type = TSDB_DATA_TYPE_INT;
  s[0].bytes = 4;
  s[0].colId = 0;
  strcpy(s[0].name, "col1");

  s[1].type = TSDB_DATA_TYPE_BINARY;
  s[1].bytes = 20;
  s[1].colId = 1;
  strcpy(s[1].name, "col2");

  int32_t* p = (int32_t*) calloc(5, sizeof(int32_t));
  p[0] = 12;
  p[1] = 8;
  p[2] = 99;
  p[3] = 7;
  p[4] = 1;

  char* t1 = (char*) calloc(5, 20);
  strcpy(t1, "abc");
  strcpy(t1 + 20, "def");
  strcpy(t1 + 40, "xyz");
  strcpy(t1 + 60, "klm");
  strcpy(t1 + 80, "hij");

  pCols[0] = (char*) p;
  pCols[1] = (char*) t1;
  taoscQSort(reinterpret_cast<void**>(pCols), s, 2, 5, 0, comp);

  int32_t* px = (int32_t*) pCols[0];
  ASSERT_EQ(px[0], 1);
  ASSERT_EQ(px[1], 7);
  ASSERT_EQ(px[2], 8);
  ASSERT_EQ(px[3], 12);
  ASSERT_EQ(px[4], 99);

  char* px1 = (char*) pCols[1];
  ASSERT_STRCASEEQ(px1 + 20 * 0, "hij");
  ASSERT_STRCASEEQ(px1 + 20 * 1, "klm");
  ASSERT_STRCASEEQ(px1 + 20 * 2, "def");
  ASSERT_STRCASEEQ(px1 + 20 * 3, "abc");
  ASSERT_STRCASEEQ(px1 + 20 * 4, "xyz");

  taoscQSort(pCols, s, 2, 5, 1, comp1);
  px = (int32_t*) pCols[0];
  ASSERT_EQ(px[0], 12);
  ASSERT_EQ(px[1], 8);
  ASSERT_EQ(px[2], 1);
  ASSERT_EQ(px[3], 7);
  ASSERT_EQ(px[4], 99);

  px1 = (char*) pCols[1];
  ASSERT_STRCASEEQ(px1 + 20 * 0, "abc");
  ASSERT_STRCASEEQ(px1 + 20 * 1, "def");
  ASSERT_STRCASEEQ(px1 + 20 * 2, "hij");
  ASSERT_STRCASEEQ(px1 + 20 * 3, "klm");
  ASSERT_STRCASEEQ(px1 + 20 * 4, "xyz");
}

TEST(testCase, columnsort_test) {
  SSchema field[1] = {
      {TSDB_DATA_TYPE_INT, "k", sizeof(int32_t)},
  };

  const int32_t num = 2000;

  int32_t *d = (int32_t *)malloc(sizeof(int32_t) * num);
  for (int32_t i = 0; i < num; ++i) {
    d[i] = i % 4;
  }

  const int32_t     numOfOrderCols = 1;
  int32_t           orderColIdx = 0;
  SColumnModel     *pModel = createColumnModel(field, 1, 1000);
  tOrderDescriptor *pDesc = tOrderDesCreate(&orderColIdx, numOfOrderCols, pModel, 1);

  tColDataQSort(pDesc, num, 0, num - 1, (char *)d, 1);

  for (int32_t i = 0; i < num; ++i) {
    printf("%d\t", d[i]);
  }
  printf("\n");

  destroyColumnModel(pModel);
}