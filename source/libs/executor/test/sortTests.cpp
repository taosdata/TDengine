/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>
#include <tglobal.h>
#include <tsort.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "executor.h"
#include "executorInt.h"
#include "taos.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdef.h"
#include "trpc.h"
#include "tvariant.h"

namespace {
typedef struct {
  int32_t startVal;
  int32_t count;
  int32_t pageRows;
  int16_t type;
} _info;

int16_t VARCOUNT = 16;

float rand_f2() {
  unsigned r = taosRand();
  r &= 0x007fffff;
  r |= 0x40800000;
  return *(float*)&r - 6.0;
}

static const int32_t TEST_NUMBER = 1;
#define bigendian() ((*(char*)&TEST_NUMBER) == 0)

SSDataBlock* getSingleColDummyBlock(void* param) {
  _info* pInfo = (_info*)param;
  if (--pInfo->count < 0) {
    return NULL;
  }

  SSDataBlock* pBlock = createDataBlock();

  SColumnInfoData colInfo = {0};
  colInfo.info.type = pInfo->type;
  if (pInfo->type == TSDB_DATA_TYPE_NCHAR) {
    colInfo.info.bytes = TSDB_NCHAR_SIZE * VARCOUNT + VARSTR_HEADER_SIZE;
  } else if (pInfo->type == TSDB_DATA_TYPE_BINARY || pInfo->type == TSDB_DATA_TYPE_GEOMETRY) {
    colInfo.info.bytes = VARCOUNT + VARSTR_HEADER_SIZE;
  } else {
    colInfo.info.bytes = tDataTypes[pInfo->type].bytes;
  }
  colInfo.info.colId = 1;

  blockDataAppendColInfo(pBlock, &colInfo);
  blockDataEnsureCapacity(pBlock, pInfo->pageRows);

  for (int32_t i = 0; i < pInfo->pageRows; ++i) {
    SColumnInfoData* pColInfo = static_cast<SColumnInfoData*>(TARRAY_GET_ELEM(pBlock->pDataBlock, 0));

    if (pInfo->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t size = taosRand() % VARCOUNT;
      char    str[128] = {0};
      char    strOri[128] = {0};
      taosRandStr(strOri, size);
      int32_t len = 0;
      bool    ret = taosMbsToUcs4(strOri, size, (TdUcs4*)varDataVal(str), size * TSDB_NCHAR_SIZE, &len);
      if (!ret) {
        printf("error\n");
        return NULL;
      }
      varDataSetLen(str, len);
      colDataSetVal(pColInfo, i, reinterpret_cast<const char*>(str), false);
      pBlock->info.hasVarCol = true;
      printf("nchar: %s\n", strOri);
    } else if (pInfo->type == TSDB_DATA_TYPE_BINARY || pInfo->type == TSDB_DATA_TYPE_GEOMETRY) {
      int32_t size = taosRand() % VARCOUNT;
      char    str[64] = {0};
      taosRandStr(varDataVal(str), size);
      varDataSetLen(str, size);
      colDataSetVal(pColInfo, i, reinterpret_cast<const char*>(str), false);
      pBlock->info.hasVarCol = true;
      printf("binary: %s\n", varDataVal(str));
    } else if (pInfo->type == TSDB_DATA_TYPE_DOUBLE || pInfo->type == TSDB_DATA_TYPE_FLOAT) {
      double v = rand_f2();
      colDataSetVal(pColInfo, i, reinterpret_cast<const char*>(&v), false);
      printf("float: %f\n", v);
    } else {
      int64_t v = ++pInfo->startVal;
      char*   result = static_cast<char*>(taosMemoryCalloc(tDataTypes[pInfo->type].bytes, 1));
      if (!bigendian()) {
        memcpy(result, &v, tDataTypes[pInfo->type].bytes);
      } else {
        memcpy(result, (char*)(&v) + sizeof(int64_t) - tDataTypes[pInfo->type].bytes, tDataTypes[pInfo->type].bytes);
      }

      colDataSetVal(pColInfo, i, result, false);
      printf("int: %" PRId64 "\n", v);
      taosMemoryFree(result);
    }
  }

  pBlock->info.rows = pInfo->pageRows;
  return pBlock;
}

int32_t docomp(const void* p1, const void* p2, void* param) {
  int32_t pLeftIdx = *(int32_t*)p1;
  int32_t pRightIdx = *(int32_t*)p2;

  SMsortComparParam* pParam = (SMsortComparParam*)param;
  SSortSource**      px = reinterpret_cast<SSortSource**>(pParam->pSources);

  SArray* pInfo = pParam->orderInfo;

  SSortSource* pLeftSource = px[pLeftIdx];
  SSortSource* pRightSource = px[pRightIdx];

  // this input is exhausted, set the special value to denote this
  if (pLeftSource->src.rowIndex == -1) {
    return 1;
  }

  if (pRightSource->src.rowIndex == -1) {
    return -1;
  }

  SSDataBlock* pLeftBlock = pLeftSource->src.pBlock;
  SSDataBlock* pRightBlock = pRightSource->src.pBlock;

  for (int32_t i = 0; i < pInfo->size; ++i) {
    SBlockOrderInfo* pOrder = (SBlockOrderInfo*)TARRAY_GET_ELEM(pInfo, i);

    SColumnInfoData* pLeftColInfoData = (SColumnInfoData*)TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder->slotId);

    bool leftNull = false;
    if (pLeftColInfoData->hasNull) {
      leftNull = colDataIsNull(pLeftColInfoData, pLeftBlock->info.rows, pLeftSource->src.rowIndex,
                               pLeftBlock->pBlockAgg[pOrder->slotId]);
    }

    SColumnInfoData* pRightColInfoData = (SColumnInfoData*)TARRAY_GET_ELEM(pRightBlock->pDataBlock, pOrder->slotId);
    bool             rightNull = false;
    if (pRightColInfoData->hasNull) {
      rightNull = colDataIsNull(pRightColInfoData, pRightBlock->info.rows, pRightSource->src.rowIndex,
                                pRightBlock->pBlockAgg[pOrder->slotId]);
    }

    if (leftNull && rightNull) {
      continue;  // continue to next slot
    }

    if (rightNull) {
      return pOrder->nullFirst ? 1 : -1;
    }

    if (leftNull) {
      return pOrder->nullFirst ? -1 : 1;
    }

    void*         left1 = colDataGetData(pLeftColInfoData, pLeftSource->src.rowIndex);
    void*         right1 = colDataGetData(pRightColInfoData, pRightSource->src.rowIndex);
    __compar_fn_t fn = getKeyComparFunc(pLeftColInfoData->info.type, pOrder->order);

    int ret = fn(left1, right1);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}
}  // namespace

#if 0
TEST(testCase, inMem_sort_Test) {
  SBlockOrderInfo oi = {0};
  oi.order = TSDB_ORDER_ASC;
  oi.slotId = 0;
  SArray* orderInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));
  taosArrayPush(orderInfo, &oi);

  SSortHandle* phandle = tsortCreateSortHandle(orderInfo, SORT_SINGLESOURCE_SORT, 1024, 5, NULL, "test_abc");
  tsortSetFetchRawDataFp(phandle, getSingleColDummyBlock, NULL, NULL);

  _info* pInfo = (_info*) taosMemoryCalloc(1, sizeof(_info));
  pInfo->startVal = 0;
  pInfo->pageRows = 100;
  pInfo->count = 6;
  pInfo->type = TSDB_DATA_TYPE_USMALLINT;

  SSortSource* ps = static_cast<SSortSource*>(taosMemoryCalloc(1, sizeof(SSortSource)));
  ps->param = pInfo;
  tsortAddSource(phandle, ps);

  int32_t code = tsortOpen(phandle);
  int32_t row = 1;
  taosMemoryFreeClear(ps);

  while(1) {
    STupleHandle* pTupleHandle = tsortNextTuple(phandle);
    if (pTupleHandle == NULL) {
      break;
    }

    void* v = tsortGetValue(pTupleHandle, 0);
    printf("%d: %d\n", row, *(uint16_t*) v);
    ASSERT_EQ(row++, *(uint16_t*) v);

  }
  taosArrayDestroy(orderInfo);
  tsortDestroySortHandle(phandle);
  taosMemoryFree(pInfo);
}

TEST(testCase, external_mem_sort_Test) {

  _info* pInfo = (_info*) taosMemoryCalloc(8, sizeof(_info));
  pInfo[0].startVal = 0;
  pInfo[0].pageRows = 10;
  pInfo[0].count = 6;
  pInfo[0].type = TSDB_DATA_TYPE_BOOL;

  pInfo[1].startVal = 0;
  pInfo[1].pageRows = 10;
  pInfo[1].count = 6;
  pInfo[1].type = TSDB_DATA_TYPE_TINYINT;

  pInfo[2].startVal = 0;
  pInfo[2].pageRows = 100;
  pInfo[2].count = 6;
  pInfo[2].type = TSDB_DATA_TYPE_USMALLINT;

  pInfo[3].startVal = 0;
  pInfo[3].pageRows = 100;
  pInfo[3].count = 6;
  pInfo[3].type = TSDB_DATA_TYPE_INT;

  pInfo[4].startVal = 0;
  pInfo[4].pageRows = 100;
  pInfo[4].count = 6;
  pInfo[4].type = TSDB_DATA_TYPE_UBIGINT;

  pInfo[5].startVal = 0;
  pInfo[5].pageRows = 100;
  pInfo[5].count = 6;
  pInfo[5].type = TSDB_DATA_TYPE_DOUBLE;

  pInfo[6].startVal = 0;
  pInfo[6].pageRows = 50;
  pInfo[6].count = 6;
  pInfo[6].type = TSDB_DATA_TYPE_NCHAR;

  pInfo[7].startVal = 0;
  pInfo[7].pageRows = 100;
  pInfo[7].count = 6;
  pInfo[7].type = TSDB_DATA_TYPE_BINARY;

  for (int i = 0; i < 8; i++){
    SBlockOrderInfo oi = {0};

    if(pInfo[i].type == TSDB_DATA_TYPE_NCHAR){
      oi.order = TSDB_ORDER_DESC;
    }else{
      oi.order = TSDB_ORDER_ASC;
    }

    oi.slotId = 0;
    SArray* orderInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));
    taosArrayPush(orderInfo, &oi);

    SSortHandle* phandle = tsortCreateSortHandle(orderInfo, SORT_SINGLESOURCE_SORT, 128, 3, NULL, "test_abc");
    tsortSetFetchRawDataFp(phandle, getSingleColDummyBlock, NULL, NULL);

    SSortSource* ps = static_cast<SSortSource*>(taosMemoryCalloc(1, sizeof(SSortSource)));
    ps->param = &pInfo[i];

    tsortAddSource(phandle, ps);

    int32_t code = tsortOpen(phandle);
    int32_t row = 1;
    taosMemoryFreeClear(ps);

    printf("--------start with %s-----------\n", tDataTypes[pInfo[i].type].name);
    while(1) {
      STupleHandle* pTupleHandle = tsortNextTuple(phandle);
      if (pTupleHandle == NULL) {
        break;
      }

      void* v = tsortGetValue(pTupleHandle, 0);

      if(pInfo[i].type == TSDB_DATA_TYPE_NCHAR){
        char        buf[128] = {0};
        int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(v), varDataLen(v), buf);
        printf("%d: %s\n", row++, buf);
      }else if(pInfo[i].type == TSDB_DATA_TYPE_BINARY || pInfo[i]->type == TSDB_DATA_TYPE_GEOMETRY){
        char        buf[128] = {0};
        memcpy(buf, varDataVal(v), varDataLen(v));
        printf("%d: %s\n", row++, buf);
      }else if(pInfo[i].type == TSDB_DATA_TYPE_DOUBLE) {
        printf("double: %lf\n", *(double*)v);
      }else if (pInfo[i].type == TSDB_DATA_TYPE_FLOAT) {
        printf("float: %f\n", *(float*)v);
      }else{
        int64_t result = 0;
        if (!bigendian()){
          memcpy(&result, v, tDataTypes[pInfo[i].type].bytes);
        }else{
          memcpy((char*)(&result) + sizeof(int64_t) - tDataTypes[pInfo[i].type].bytes, v, tDataTypes[pInfo[i].type].bytes);
        }
        printf("%d: %" PRId64 "\n", row++, result);
      }
    }
    taosArrayDestroy(orderInfo);
    tsortDestroySortHandle(phandle);
  }
  taosMemoryFree(pInfo);
}

TEST(testCase, ordered_merge_sort_Test) {
  SBlockOrderInfo oi = {0};
  oi.order = TSDB_ORDER_ASC;
  oi.slotId = 0;
  SArray* orderInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));
  taosArrayPush(orderInfo, &oi);

  SSDataBlock* pBlock = createDataBlock();
  for (int32_t i = 0; i < 1; ++i) {
    SColumnInfoData colInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
    blockDataAppendColInfo(pBlock, &colInfo);
  }

  SSortHandle* phandle = tsortCreateSortHandle(orderInfo, SORT_MULTISOURCE_MERGE, 1024, 5, pBlock,"test_abc");
  tsortSetFetchRawDataFp(phandle, getSingleColDummyBlock, NULL, NULL);
  tsortSetComparFp(phandle, docomp);

  SSortSource* p[10] = {0};
  _info c[10] = {0};
  for(int32_t i = 0; i < 10; ++i) {
    p[i] = static_cast<SSortSource*>(taosMemoryCalloc(1, sizeof(SSortSource)));
    c[i].count    = 1;
    c[i].pageRows = 1000;
    c[i].startVal = i*1000;
    c[i].type = TSDB_DATA_TYPE_INT;

    p[i]->param = &c[i];
    tsortAddSource(phandle, p[i]);
  }

  int32_t code = tsortOpen(phandle);
  int32_t row = 1;

  while(1) {
    STupleHandle* pTupleHandle = tsortNextTuple(phandle);
    if (pTupleHandle == NULL) {
      break;
    }

    void* v = tsortGetValue(pTupleHandle, 0);
//    printf("%d: %d\n", row, *(int32_t*) v);
    ASSERT_EQ(row++, *(int32_t*) v);

  }

  taosArrayDestroy(orderInfo);
  tsortDestroySortHandle(phandle);
  blockDataDestroy(pBlock);
}

#endif

#pragma GCC diagnostic pop
