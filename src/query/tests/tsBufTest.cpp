#include "os.h"
#include <gtest/gtest.h>
#include <cassert>
#include <iostream>

#include "taos.h"
#include "tsdb.h"
#include "qTsbuf.h"
#include "tstoken.h"
#include "tutil.h"

namespace {
/**
 *
 * @param num  total number
 * @param step gap between two consecutive ts
 * @return
 */
int64_t* createTsList(int32_t num, int64_t start, int32_t step) {
  int64_t* pList = (int64_t*)malloc(num * sizeof(int64_t));

  for (int64_t i = 0; i < num; ++i) {
    pList[i] = start + i * step;
  }

  return pList;
}

// simple test
void simpleTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  // write 10 ts points
  int32_t num = 10;
  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;
  t.i64 = 1;

  int64_t* list = createTsList(10, 10000000, 30);
  tsBufAppend(pTSBuf, 0, &t, (const char*)list, num * sizeof(int64_t));
  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);

  EXPECT_EQ(pTSBuf->tsData.len, sizeof(int64_t) * num);
  EXPECT_EQ(tVariantCompare(&pTSBuf->block.tag, &t), 0);
  EXPECT_EQ(pTSBuf->numOfGroups, 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);

  free(list);
}

// one large list of ts, the ts list need to be split into several small blocks
void largeTSTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  // write 10 ts points
  int32_t num = 1000000;
  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;
  t.i64 = 1;

  int64_t* list = createTsList(num, 10000000, 30);
  tsBufAppend(pTSBuf, 0, &t, (const char*)list, num * sizeof(int64_t));

  // the data has been flush to disk, no data in cache
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(tVariantCompare(&pTSBuf->block.tag, &t), 0);
  EXPECT_EQ(pTSBuf->numOfGroups, 1);
  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);
  free(list);
}

void multiTagsTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t num = 10000;
  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;

  int64_t start = 10000000;
  int32_t numOfTags = 50;
  int32_t step = 30;

  for (int32_t i = 0; i < numOfTags; ++i) {
    int64_t* list = createTsList(num, start, step);
    t.i64 = i;

    tsBufAppend(pTSBuf, 0, &t, (const char*)list, num * sizeof(int64_t));
    free(list);

    start += step * num;
  }

  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);
  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));

  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);
  EXPECT_EQ(pTSBuf->numOfGroups, 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);
}

void multiVnodeTagsTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t num = 10000;
  int64_t start = 10000000;
  int32_t numOfTags = 50;
  int32_t step = 30;

  // 2000 vnodes
  for (int32_t j = 0; j < 20; ++j) {
    // vnodeId:0
    start = 10000000;
    tVariant t = {0};
    t.nType = TSDB_DATA_TYPE_BIGINT;

    for (int32_t i = 0; i < numOfTags; ++i) {
      int64_t* list = createTsList(num, start, step);
      t.i64 = i;

      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      free(list);

      start += step * num;
    }

    EXPECT_EQ(pTSBuf->numOfGroups, j + 1);
  }

  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);
  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));
  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));

  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);
}

void loadDataTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t num = 10000;
  int64_t oldStart = 10000000;
  int32_t numOfTags = 50;
  int32_t step = 30;
  int32_t numOfVnode = 200;

  // 10000 vnodes
  for (int32_t j = 0; j < numOfVnode; ++j) {
    // vnodeId:0
    int64_t start = 10000000;
    tVariant t = {0};
    t.nType = TSDB_DATA_TYPE_BIGINT;

    for (int32_t i = 0; i < numOfTags; ++i) {
      int64_t* list = createTsList(num, start, step);
      t.i64 = i;

      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      printf("%d - %" PRIu64 "\n", i, list[0]);

      free(list);
      start += step * num;
    }

    EXPECT_EQ(pTSBuf->numOfGroups, j + 1);
  }

  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);

  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));
  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));

  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  // create from exists file
  STSBuf* pNewBuf = tsBufCreateFromFile(pTSBuf->path, false);
  EXPECT_EQ(pNewBuf->tsOrder, pTSBuf->tsOrder);
  EXPECT_EQ(pNewBuf->numOfGroups, numOfVnode);
  EXPECT_EQ(pNewBuf->fileSize, pTSBuf->fileSize);

  EXPECT_EQ(pNewBuf->pData[0].info.offset, pTSBuf->pData[0].info.offset);
  EXPECT_EQ(pNewBuf->pData[0].info.numOfBlocks, pTSBuf->pData[0].info.numOfBlocks);
  EXPECT_EQ(pNewBuf->pData[0].info.compLen, pTSBuf->pData[0].info.compLen);

  EXPECT_STREQ(pNewBuf->path, pTSBuf->path);

  tsBufResetPos(pNewBuf);

  int64_t s = taosGetTimestampUs();
  printf("start:%" PRIu64 "\n", s);

  int32_t x = 0;
  while (tsBufNextPos(pNewBuf)) {
    STSElem elem = tsBufGetElem(pNewBuf);
    if (++x == 100000000) {
      break;
    }

    //    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.vnode, elem.tag, elem.ts);
  }

  int64_t e = taosGetTimestampUs();
  printf("end:%" PRIu64 ", elapsed:%" PRIu64 ", total obj:%d\n", e, e - s, x);
  tsBufDestroy(pTSBuf);
  tsBufDestroy(pNewBuf);
}

void randomIncTsTest() {}

void TSTraverse() {
  // 10000 vnodes
  int32_t num = 200000;
  int64_t oldStart = 10000000;
  int32_t numOfTags = 3;
  int32_t step = 30;
  int32_t numOfVnode = 2;

  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  for (int32_t j = 0; j < numOfVnode; ++j) {
    // vnodeId:0
    int64_t start = 10000000;
    tVariant t = {0};
    t.nType = TSDB_DATA_TYPE_BIGINT;

    for (int32_t i = 0; i < numOfTags; ++i) {
      int64_t* list = createTsList(num, start, step);
      t.i64 = i;

      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      printf("%d - %d - %" PRIu64 ", %" PRIu64 "\n", j, i, list[0], list[num - 1]);

      free(list);
      start += step * num;

      list = createTsList(num, start, step);
      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      printf("%d - %d - %" PRIu64 ", %" PRIu64 "\n", j, i, list[0], list[num - 1]);
      free(list);

      start += step * num;
    }

    EXPECT_EQ(pTSBuf->numOfGroups, j + 1);
  }

  tsBufResetPos(pTSBuf);

  ////////////////////////////////////////////////////////////////////////////////////////
  // reverse traverse
  int64_t s = taosGetTimestampUs();
  printf("start:%" PRIu64 "\n", s);

  pTSBuf->cur.order = TSDB_ORDER_DESC;

  // complete reverse traverse
  int32_t x = 0;
  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    //    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.vnode, elem.tag, elem.ts);
  }

  // specify the data block with vnode and tags value
  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSDB_ORDER_DESC;

  int32_t startVnode = 1;
  int32_t startTag = 2;

  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;
  t.i64 = startTag;

  tsBufGetElemStartPos(pTSBuf, startVnode, &t);

  int32_t totalOutput = 10;
  while (1) {
    STSElem elem = tsBufGetElem(pTSBuf);
    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.id, elem.tag->i64, elem.ts);

    if (!tsBufNextPos(pTSBuf)) {
      break;
    }

    if (--totalOutput <= 0) {
      totalOutput = 10;

      startTag -= 1;
      t.i64 = startTag;
      tsBufGetElemStartPos(pTSBuf, startVnode, &t);

      if (startTag == 0) {
        startVnode -= 1;
        startTag = 3;
      }

      if (startVnode < 0) {
        break;
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////
  // traverse
  pTSBuf->cur.order = TSDB_ORDER_ASC;
  tsBufResetPos(pTSBuf);

  // complete forwards traverse
  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    //    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.vnode, elem.tag, elem.ts);
  }

  // specify the data block with vnode and tags value
  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSDB_ORDER_ASC;

  startVnode = 1;
  startTag = 2;
  t.i64 = startTag;

  tsBufGetElemStartPos(pTSBuf, startVnode, &t);

  totalOutput = 10;
  while (1) {
    STSElem elem = tsBufGetElem(pTSBuf);
    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.id, elem.tag->i64, elem.ts);

    if (!tsBufNextPos(pTSBuf)) {
      break;
    }

    if (--totalOutput <= 0) {
      totalOutput = 10;

      startTag -= 1;
      t.i64 = startTag;
      tsBufGetElemStartPos(pTSBuf, startVnode, &t);

      if (startTag < 0) {
        startVnode -= 1;
        startTag = 3;
      }

      if (startVnode < 0) {
        break;
      }
    }
  }

  tsBufDestroy(pTSBuf);
}

void performanceTest() {}

void emptyTagTest() {}

void invalidFileTest() {
  const char* cmd = "touch /tmp/test";

  // create empty file
  system(cmd);

  STSBuf* pNewBuf = tsBufCreateFromFile("/tmp/test", true);
  EXPECT_TRUE(pNewBuf == NULL);
  tsBufDestroy(pNewBuf);

  pNewBuf = tsBufCreateFromFile("/tmp/911", true);
  EXPECT_TRUE(pNewBuf == NULL);

  tsBufDestroy(pNewBuf);
}

void mergeDiffVnodeBufferTest() {
  STSBuf* pTSBuf1 = tsBufCreate(true, TSDB_ORDER_ASC);
  STSBuf* pTSBuf2 = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t step = 30;
  int32_t num = 1000;
  int32_t numOfTags = 10;

  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;

  // vnodeId:0
  int64_t start = 10000000;
  for (int32_t i = 0; i < numOfTags; ++i) {
    int64_t* list = createTsList(num, start, step);
    t.i64 = i;

    tsBufAppend(pTSBuf1, 1, &t, (const char*)list, num * sizeof(int64_t));
    tsBufAppend(pTSBuf2, 9, &t, (const char*)list, num * sizeof(int64_t));

    free(list);

    start += step * num;
  }

  tsBufFlush(pTSBuf2);

  tsBufMerge(pTSBuf1, pTSBuf2);
  EXPECT_EQ(pTSBuf1->numOfGroups, 2);
  EXPECT_EQ(pTSBuf1->numOfTotal, numOfTags * 2 * num);

  tsBufDisplay(pTSBuf1);

  tsBufDestroy(pTSBuf2);
  tsBufDestroy(pTSBuf1);
}

void mergeIdenticalVnodeBufferTest() {
  STSBuf* pTSBuf1 = tsBufCreate(true, TSDB_ORDER_ASC);
  STSBuf* pTSBuf2 = tsBufCreate(true, TSDB_ORDER_ASC);

  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;

  int32_t step = 30;
  int32_t num = 1000;
  int32_t numOfTags = 10;

  // vnodeId:0
  int64_t start = 10000000;
  for (int32_t i = 0; i < numOfTags; ++i) {
    int64_t* list = createTsList(num, start, step);
    t.i64 = i;

    tsBufAppend(pTSBuf1, 12, &t, (const char*)list, num * sizeof(int64_t));
    free(list);

    start += step * num;
  }

  for (int32_t i = numOfTags; i < numOfTags * 2; ++i) {
    int64_t* list = createTsList(num, start, step);

    t.i64 = i;
    tsBufAppend(pTSBuf2, 77, &t, (const char*)list, num * sizeof(int64_t));
    free(list);

    start += step * num;
  }

  tsBufFlush(pTSBuf2);

  tsBufMerge(pTSBuf1, pTSBuf2);
  EXPECT_EQ(pTSBuf1->numOfGroups, 2);
  EXPECT_EQ(pTSBuf1->numOfTotal, numOfTags * 2 * num);

  tsBufResetPos(pTSBuf1);

  int32_t count = 0;
  while (tsBufNextPos(pTSBuf1)) {
    STSElem elem = tsBufGetElem(pTSBuf1);

    if (count++ < numOfTags * num) {
      EXPECT_EQ(elem.id, 12);
    } else {
      EXPECT_EQ(elem.id, 77);
    }

    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.id, elem.tag->i64, elem.ts);
  }

  tsBufDestroy(pTSBuf1);
  tsBufDestroy(pTSBuf2);
}
}  // namespace


//TODO add binary tag value test case
TEST(testCase, tsBufTest) {
  simpleTest();
  largeTSTest();
  multiTagsTest();
  multiVnodeTagsTest();
  loadDataTest();
  invalidFileTest();
//    randomIncTsTest();
  TSTraverse();
  mergeDiffVnodeBufferTest();
  mergeIdenticalVnodeBufferTest();
}
