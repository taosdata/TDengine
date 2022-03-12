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
#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#include <metaDef.h>
#include <tmsg.h>
#include <tsdbDef.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, tSmaEncodeDecodeTest) {
  // encode
  STSma tSma = {0};
  tSma.version = 0;
  tSma.intervalUnit = TD_TIME_UNIT_DAY;
  tSma.interval = 1;
  tSma.slidingUnit = TD_TIME_UNIT_HOUR;
  tSma.sliding = 0;
  tstrncpy(tSma.indexName, "sma_index_test", TSDB_INDEX_NAME_LEN);
  tstrncpy(tSma.timezone, "Asia/Shanghai", TD_TIMEZONE_LEN);
  tSma.indexUid = 2345678910;
  tSma.tableUid = 1234567890;

  STSmaWrapper tSmaWrapper = {.number = 1, .tSma = &tSma};
  uint32_t     bufLen = tEncodeTSmaWrapper(NULL, &tSmaWrapper);

  void *buf = calloc(bufLen, 1);
  assert(buf != NULL);

  STSmaWrapper *pSW = (STSmaWrapper *)buf;
  uint32_t      len = tEncodeTSmaWrapper(&buf, &tSmaWrapper);

  EXPECT_EQ(len, bufLen);

  // decode
  STSmaWrapper dstTSmaWrapper = {0};
  void *       result = tDecodeTSmaWrapper(pSW, &dstTSmaWrapper);
  assert(result != NULL);

  EXPECT_EQ(tSmaWrapper.number, dstTSmaWrapper.number);

  for (int i = 0; i < tSmaWrapper.number; ++i) {
    STSma *pSma = tSmaWrapper.tSma + i;
    STSma *qSma = dstTSmaWrapper.tSma + i;

    EXPECT_EQ(pSma->version, qSma->version);
    EXPECT_EQ(pSma->intervalUnit, qSma->intervalUnit);
    EXPECT_EQ(pSma->slidingUnit, qSma->slidingUnit);
    EXPECT_STRCASEEQ(pSma->indexName, qSma->indexName);
    EXPECT_STRCASEEQ(pSma->timezone, qSma->timezone);
    EXPECT_EQ(pSma->indexUid, qSma->indexUid);
    EXPECT_EQ(pSma->tableUid, qSma->tableUid);
    EXPECT_EQ(pSma->interval, qSma->interval);
    EXPECT_EQ(pSma->sliding, qSma->sliding);
    EXPECT_EQ(pSma->exprLen, qSma->exprLen);
    EXPECT_STRCASEEQ(pSma->expr, qSma->expr);
    EXPECT_EQ(pSma->tagsFilterLen, qSma->tagsFilterLen);
    EXPECT_STRCASEEQ(pSma->tagsFilter, qSma->tagsFilter);
  }

  // resource release
  tdDestroyTSma(&tSma);
  tdDestroyTSmaWrapper(&dstTSmaWrapper);
}
#if 1
TEST(testCase, tSma_DB_Put_Get_Del_Test) {
  const char *   smaIndexName1 = "sma_index_test_1";
  const char *   smaIndexName2 = "sma_index_test_2";
  const char *   timezone = "Asia/Shanghai";
  const char *   expr = "select count(a,b, top 20), from table interval 1d, sliding 1h;";
  const char *   tagsFilter = "I'm tags filter";
  const char *   smaTestDir = "./smaTest";
  const tb_uid_t tbUid = 1234567890;
  const int64_t  indexUid1 = 2000000001;
  const int64_t  indexUid2 = 2000000002;
  const uint32_t nCntTSma = 2;
  // encode
  STSma tSma = {0};
  tSma.version = 0;
  tSma.intervalUnit = TD_TIME_UNIT_DAY;
  tSma.interval = 1;
  tSma.slidingUnit = TD_TIME_UNIT_HOUR;
  tSma.sliding = 0;
  tSma.indexUid = indexUid1;
  tstrncpy(tSma.indexName, smaIndexName1, TSDB_INDEX_NAME_LEN);
  tstrncpy(tSma.timezone, timezone, TD_TIMEZONE_LEN);
  tSma.tableUid = tbUid;

  tSma.exprLen = strlen(expr);
  tSma.expr = (char *)calloc(tSma.exprLen + 1, 1);
  tstrncpy(tSma.expr, expr, tSma.exprLen + 1);

  tSma.tagsFilterLen = strlen(tagsFilter);
  tSma.tagsFilter = (char *)calloc(tSma.tagsFilterLen + 1, 1);
  tstrncpy(tSma.tagsFilter, tagsFilter, tSma.tagsFilterLen + 1);

  SMeta *         pMeta = NULL;
  STSma *         pSmaCfg = &tSma;
  const SMetaCfg *pMetaCfg = &defaultMetaOptions;

  taosRemoveDir(smaTestDir);

  pMeta = metaOpen(smaTestDir, pMetaCfg, NULL);
  assert(pMeta != NULL);
  // save index 1
  EXPECT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  pSmaCfg->indexUid = indexUid2;
  tstrncpy(pSmaCfg->indexName, smaIndexName2, TSDB_INDEX_NAME_LEN);
  pSmaCfg->version = 1;
  pSmaCfg->intervalUnit = TD_TIME_UNIT_HOUR;
  pSmaCfg->interval = 1;
  pSmaCfg->slidingUnit = TD_TIME_UNIT_MINUTE;
  pSmaCfg->sliding = 5;

  // save index 2
  EXPECT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  // get value by indexName
  STSma *qSmaCfg = NULL;
  qSmaCfg = metaGetSmaInfoByIndex(pMeta, indexUid1);
  assert(qSmaCfg != NULL);
  printf("name1 = %s\n", qSmaCfg->indexName);
  printf("timezone1 = %s\n", qSmaCfg->timezone);
  printf("expr1 = %s\n", qSmaCfg->expr != NULL ? qSmaCfg->expr : "");
  printf("tagsFilter1 = %s\n", qSmaCfg->tagsFilter != NULL ? qSmaCfg->tagsFilter : "");
  EXPECT_STRCASEEQ(qSmaCfg->indexName, smaIndexName1);
  EXPECT_EQ(qSmaCfg->tableUid, tSma.tableUid);
  tdDestroyTSma(qSmaCfg);
  tfree(qSmaCfg);

  qSmaCfg = metaGetSmaInfoByIndex(pMeta, indexUid2);
  assert(qSmaCfg != NULL);
  printf("name2 = %s\n", qSmaCfg->indexName);
  printf("timezone2 = %s\n", qSmaCfg->timezone);
  printf("expr2 = %s\n", qSmaCfg->expr != NULL ? qSmaCfg->expr : "");
  printf("tagsFilter2 = %s\n", qSmaCfg->tagsFilter != NULL ? qSmaCfg->tagsFilter : "");
  EXPECT_STRCASEEQ(qSmaCfg->indexName, smaIndexName2);
  EXPECT_EQ(qSmaCfg->interval, tSma.interval);
  tdDestroyTSma(qSmaCfg);
  tfree(qSmaCfg);

  // get index name by table uid
  SMSmaCursor *pSmaCur = metaOpenSmaCursor(pMeta, tbUid);
  assert(pSmaCur != NULL);
  uint32_t indexCnt = 0;
  while (1) {
    const char *indexName = metaSmaCursorNext(pSmaCur);
    if (indexName == NULL) {
      break;
    }
    printf("indexName = %s\n", indexName);
    ++indexCnt;
  }
  EXPECT_EQ(indexCnt, nCntTSma);
  metaCloseSmaCurosr(pSmaCur);

  // get wrapper by table uid
  STSmaWrapper *pSW = metaGetSmaInfoByTable(pMeta, tbUid);
  assert(pSW != NULL);
  EXPECT_EQ(pSW->number, nCntTSma);
  EXPECT_STRCASEEQ(pSW->tSma->indexName, smaIndexName1);
  EXPECT_STRCASEEQ(pSW->tSma->timezone, timezone);
  EXPECT_STRCASEEQ(pSW->tSma->expr, expr);
  EXPECT_STRCASEEQ(pSW->tSma->tagsFilter, tagsFilter);
  EXPECT_EQ(pSW->tSma->indexUid, indexUid1);
  EXPECT_EQ(pSW->tSma->tableUid, tbUid);
  EXPECT_STRCASEEQ((pSW->tSma + 1)->indexName, smaIndexName2);
  EXPECT_STRCASEEQ((pSW->tSma + 1)->timezone, timezone);
  EXPECT_STRCASEEQ((pSW->tSma + 1)->expr, expr);
  EXPECT_STRCASEEQ((pSW->tSma + 1)->tagsFilter, tagsFilter);
  EXPECT_EQ((pSW->tSma + 1)->indexUid, indexUid2);
  EXPECT_EQ((pSW->tSma + 1)->tableUid, tbUid);

  tdDestroyTSmaWrapper(pSW);
  tfree(pSW);

  // get all sma table uids
  SArray *pUids = metaGetSmaTbUids(pMeta, false);
  assert(pUids != NULL);
  for (uint32_t i = 0; i < taosArrayGetSize(pUids); ++i) {
    printf("metaGetSmaTbUids: uid[%" PRIu32 "] = %" PRIi64 "\n", i, *(tb_uid_t *)taosArrayGet(pUids, i));
    // printf("metaGetSmaTbUids: index[%" PRIu32 "] = %s", i, (char *)taosArrayGet(pUids, i));
  }
  EXPECT_EQ(taosArrayGetSize(pUids), 1);
  taosArrayDestroy(pUids);

  // resource release
  metaRemoveSmaFromDb(pMeta, smaIndexName1);
  metaRemoveSmaFromDb(pMeta, smaIndexName2);

  tdDestroyTSma(&tSma);
  metaClose(pMeta);
}
#endif

#if 1
TEST(testCase, tSmaInsertTest) {
  const int64_t     indexUid = 2000000002;
  STSmaDataWrapper *pSmaData = NULL;
  STsdb             tsdb = {0};
  STsdbCfg *        pCfg = &tsdb.config;

  pCfg->daysPerFile = 1;

  // init
  int32_t allocCnt = 0;
  int32_t allocStep = 40960;
  int32_t buffer = 4096;
  void *  buf = NULL;
  EXPECT_EQ(tsdbMakeRoom(&buf, allocStep), 0);
  int32_t  bufSize = taosTSizeof(buf);
  int32_t  numOfTables = 25;
  col_id_t numOfCols = 4096;
  EXPECT_GT(numOfCols, 0);

  pSmaData = (STSmaDataWrapper *)buf;
  printf(">> allocate [%d] time to %d and addr is %p\n", ++allocCnt, bufSize, pSmaData);
  pSmaData->skey = 1646987196;
  pSmaData->interval = 10;
  pSmaData->intervalUnit = TD_TIME_UNIT_MINUTE;
  pSmaData->indexUid = indexUid;

  int32_t len = sizeof(STSmaDataWrapper);
  for (int32_t t = 0; t < numOfTables; ++t) {
    STSmaTbData *pTbData = (STSmaTbData *)POINTER_SHIFT(pSmaData, len);
    pTbData->tableUid = t;

    int32_t tableDataLen = sizeof(STSmaTbData);
    for (col_id_t c = 0; c < numOfCols; ++c) {
      if (bufSize - len - tableDataLen < buffer) {
        EXPECT_EQ(tsdbMakeRoom(&buf, bufSize + allocStep), 0);
        pSmaData = (STSmaDataWrapper *)buf;
        pTbData = (STSmaTbData *)POINTER_SHIFT(pSmaData, len);
        bufSize = taosTSizeof(buf);
        printf(">> allocate [%d] time to %d and addr is %p\n", ++allocCnt, bufSize, pSmaData);
      }
      STSmaColData *pColData = (STSmaColData *)POINTER_SHIFT(pSmaData, len + tableDataLen);
      pColData->colId = c + PRIMARYKEY_TIMESTAMP_COL_ID;
      pColData->blockSize = ((c & 1) == 0) ? 8 : 16;
      // TODO: fill col data
      tableDataLen += (sizeof(STSmaColData) + pColData->blockSize);
    }
    pTbData->dataLen = (tableDataLen - sizeof(STSmaTbData));
    len += tableDataLen;
    // printf("bufSize=%d, len=%d, len of table[%d]=%d\n", bufSize, len, t, tableDataLen);
  }
  pSmaData->dataLen = (len - sizeof(STSmaDataWrapper));

  EXPECT_GE(bufSize, pSmaData->dataLen);

  // execute
  EXPECT_EQ(tsdbInsertTSmaData(&tsdb, (char *)pSmaData), TSDB_CODE_SUCCESS);

  // release
  taosTZfree(buf);
}
#endif

#pragma GCC diagnostic pop