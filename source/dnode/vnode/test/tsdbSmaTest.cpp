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

TEST(testCase, tSma_Meta_Encode_Decode_Test) {
  // encode
  STSma tSma = {0};
  tSma.version = 0;
  tSma.intervalUnit = TIME_UNIT_DAY;
  tSma.interval = 1;
  tSma.slidingUnit = TIME_UNIT_HOUR;
  tSma.sliding = 0;
  tstrncpy(tSma.indexName, "sma_index_test", TSDB_INDEX_NAME_LEN);
  tstrncpy(tSma.timezone, "Asia/Shanghai", TD_TIMEZONE_LEN);
  tSma.indexUid = 2345678910;
  tSma.tableUid = 1234567890;

  STSmaWrapper tSmaWrapper = {.number = 1, .tSma = &tSma};
  uint32_t     bufLen = tEncodeTSmaWrapper(NULL, &tSmaWrapper);

  void *buf = calloc(bufLen, 1);
  ASSERT_NE(buf, nullptr);

  STSmaWrapper *pSW = (STSmaWrapper *)buf;
  uint32_t      len = tEncodeTSmaWrapper(&buf, &tSmaWrapper);

  ASSERT_EQ(len, bufLen);

  // decode
  STSmaWrapper dstTSmaWrapper = {0};
  void *       result = tDecodeTSmaWrapper(pSW, &dstTSmaWrapper);
  ASSERT_NE(result, nullptr);

  ASSERT_EQ(tSmaWrapper.number, dstTSmaWrapper.number);

  for (int i = 0; i < tSmaWrapper.number; ++i) {
    STSma *pSma = tSmaWrapper.tSma + i;
    STSma *qSma = dstTSmaWrapper.tSma + i;

    ASSERT_EQ(pSma->version, qSma->version);
    ASSERT_EQ(pSma->intervalUnit, qSma->intervalUnit);
    ASSERT_EQ(pSma->slidingUnit, qSma->slidingUnit);
    ASSERT_STRCASEEQ(pSma->indexName, qSma->indexName);
    ASSERT_STRCASEEQ(pSma->timezone, qSma->timezone);
    ASSERT_EQ(pSma->indexUid, qSma->indexUid);
    ASSERT_EQ(pSma->tableUid, qSma->tableUid);
    ASSERT_EQ(pSma->interval, qSma->interval);
    ASSERT_EQ(pSma->sliding, qSma->sliding);
    ASSERT_EQ(pSma->exprLen, qSma->exprLen);
    ASSERT_STRCASEEQ(pSma->expr, qSma->expr);
    ASSERT_EQ(pSma->tagsFilterLen, qSma->tagsFilterLen);
    ASSERT_STRCASEEQ(pSma->tagsFilter, qSma->tagsFilter);
  }

  // resource release
  tdDestroyTSma(&tSma);
  tdDestroyTSmaWrapper(&dstTSmaWrapper);
}

#if 1
TEST(testCase, tSma_metaDB_Put_Get_Del_Test) {
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
  tSma.intervalUnit = TIME_UNIT_DAY;
  tSma.interval = 1;
  tSma.slidingUnit = TIME_UNIT_HOUR;
  tSma.sliding = 0;
  tSma.indexUid = indexUid1;
  tstrncpy(tSma.indexName, smaIndexName1, TSDB_INDEX_NAME_LEN);
  tstrncpy(tSma.timezone, timezone, TD_TIMEZONE_LEN);
  tSma.tableUid = tbUid;

  tSma.exprLen = strlen(expr);
  tSma.expr = (char *)calloc(tSma.exprLen + 1, 1);
  ASSERT_NE(tSma.expr, nullptr);
  tstrncpy(tSma.expr, expr, tSma.exprLen + 1);

  tSma.tagsFilterLen = strlen(tagsFilter);
  tSma.tagsFilter = (char *)calloc(tSma.tagsFilterLen + 1, 1);
  ASSERT_NE(tSma.tagsFilter, nullptr);
  tstrncpy(tSma.tagsFilter, tagsFilter, tSma.tagsFilterLen + 1);

  SMeta *         pMeta = NULL;
  STSma *         pSmaCfg = &tSma;
  const SMetaCfg *pMetaCfg = &defaultMetaOptions;

  taosRemoveDir(smaTestDir);

  pMeta = metaOpen(smaTestDir, pMetaCfg, NULL);
  assert(pMeta != NULL);
  // save index 1
  ASSERT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  pSmaCfg->indexUid = indexUid2;
  tstrncpy(pSmaCfg->indexName, smaIndexName2, TSDB_INDEX_NAME_LEN);
  pSmaCfg->version = 1;
  pSmaCfg->intervalUnit = TIME_UNIT_HOUR;
  pSmaCfg->interval = 1;
  pSmaCfg->slidingUnit = TIME_UNIT_MINUTE;
  pSmaCfg->sliding = 5;

  // save index 2
  ASSERT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  // get value by indexName
  STSma *qSmaCfg = NULL;
  qSmaCfg = metaGetSmaInfoByIndex(pMeta, indexUid1);
  assert(qSmaCfg != NULL);
  printf("name1 = %s\n", qSmaCfg->indexName);
  printf("timezone1 = %s\n", qSmaCfg->timezone);
  printf("expr1 = %s\n", qSmaCfg->expr != NULL ? qSmaCfg->expr : "");
  printf("tagsFilter1 = %s\n", qSmaCfg->tagsFilter != NULL ? qSmaCfg->tagsFilter : "");
  ASSERT_STRCASEEQ(qSmaCfg->indexName, smaIndexName1);
  ASSERT_EQ(qSmaCfg->tableUid, tSma.tableUid);
  tdDestroyTSma(qSmaCfg);
  tfree(qSmaCfg);

  qSmaCfg = metaGetSmaInfoByIndex(pMeta, indexUid2);
  assert(qSmaCfg != NULL);
  printf("name2 = %s\n", qSmaCfg->indexName);
  printf("timezone2 = %s\n", qSmaCfg->timezone);
  printf("expr2 = %s\n", qSmaCfg->expr != NULL ? qSmaCfg->expr : "");
  printf("tagsFilter2 = %s\n", qSmaCfg->tagsFilter != NULL ? qSmaCfg->tagsFilter : "");
  ASSERT_STRCASEEQ(qSmaCfg->indexName, smaIndexName2);
  ASSERT_EQ(qSmaCfg->interval, tSma.interval);
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
  ASSERT_EQ(indexCnt, nCntTSma);
  metaCloseSmaCurosr(pSmaCur);

  // get wrapper by table uid
  STSmaWrapper *pSW = metaGetSmaInfoByTable(pMeta, tbUid);
  assert(pSW != NULL);
  ASSERT_EQ(pSW->number, nCntTSma);
  ASSERT_STRCASEEQ(pSW->tSma->indexName, smaIndexName1);
  ASSERT_STRCASEEQ(pSW->tSma->timezone, timezone);
  ASSERT_STRCASEEQ(pSW->tSma->expr, expr);
  ASSERT_STRCASEEQ(pSW->tSma->tagsFilter, tagsFilter);
  ASSERT_EQ(pSW->tSma->indexUid, indexUid1);
  ASSERT_EQ(pSW->tSma->tableUid, tbUid);
  ASSERT_STRCASEEQ((pSW->tSma + 1)->indexName, smaIndexName2);
  ASSERT_STRCASEEQ((pSW->tSma + 1)->timezone, timezone);
  ASSERT_STRCASEEQ((pSW->tSma + 1)->expr, expr);
  ASSERT_STRCASEEQ((pSW->tSma + 1)->tagsFilter, tagsFilter);
  ASSERT_EQ((pSW->tSma + 1)->indexUid, indexUid2);
  ASSERT_EQ((pSW->tSma + 1)->tableUid, tbUid);

  tdDestroyTSmaWrapper(pSW);
  tfree(pSW);

  // get all sma table uids
  SArray *pUids = metaGetSmaTbUids(pMeta, false);
  assert(pUids != NULL);
  for (uint32_t i = 0; i < taosArrayGetSize(pUids); ++i) {
    printf("metaGetSmaTbUids: uid[%" PRIu32 "] = %" PRIi64 "\n", i, *(tb_uid_t *)taosArrayGet(pUids, i));
    // printf("metaGetSmaTbUids: index[%" PRIu32 "] = %s", i, (char *)taosArrayGet(pUids, i));
  }
  ASSERT_EQ(taosArrayGetSize(pUids), 1);
  taosArrayDestroy(pUids);

  // resource release
  metaRemoveSmaFromDb(pMeta, smaIndexName1);
  metaRemoveSmaFromDb(pMeta, smaIndexName2);

  tdDestroyTSma(&tSma);
  metaClose(pMeta);
}
#endif

#if 1
TEST(testCase, tSma_Data_Insert_Query_Test) {
  // step 1: prepare meta
  const char *   smaIndexName1 = "sma_index_test_1";
  const char *   timezone = "Asia/Shanghai";
  const char *   expr = "select count(a,b, top 20), from table interval 1d, sliding 1h;";
  const char *   tagsFilter = "where tags.location='Beijing' and tags.district='ChaoYang'";
  const char *   smaTestDir = "./smaTest";
  const tb_uid_t tbUid = 1234567890;
  const int64_t  indexUid1 = 2000000001;
  const int64_t  interval1 = 1;
  const int8_t   intervalUnit1 = TIME_UNIT_DAY;
  const uint32_t nCntTSma = 2;
  TSKEY          skey1 = 1646987196;
  const int64_t  testSmaData1 = 100;
  const int64_t  testSmaData2 = 200;
  // encode
  STSma tSma = {0};
  tSma.version = 0;
  tSma.intervalUnit = TIME_UNIT_DAY;
  tSma.interval = 1;
  tSma.slidingUnit = TIME_UNIT_HOUR;
  tSma.sliding = 0;
  tSma.indexUid = indexUid1;
  tstrncpy(tSma.indexName, smaIndexName1, TSDB_INDEX_NAME_LEN);
  tstrncpy(tSma.timezone, timezone, TD_TIMEZONE_LEN);
  tSma.tableUid = tbUid;

  tSma.exprLen = strlen(expr);
  tSma.expr = (char *)calloc(tSma.exprLen + 1, 1);
  ASSERT_NE(tSma.expr, nullptr);
  tstrncpy(tSma.expr, expr, tSma.exprLen + 1);

  tSma.tagsFilterLen = strlen(tagsFilter);
  tSma.tagsFilter = (char *)calloc(tSma.tagsFilterLen + 1, 1);
  ASSERT_NE(tSma.tagsFilter, nullptr);
  tstrncpy(tSma.tagsFilter, tagsFilter, tSma.tagsFilterLen + 1);

  SMeta *         pMeta = NULL;
  STSma *         pSmaCfg = &tSma;
  const SMetaCfg *pMetaCfg = &defaultMetaOptions;

  taosRemoveDir(smaTestDir);

  pMeta = metaOpen(smaTestDir, pMetaCfg, NULL);
  assert(pMeta != NULL);
  // save index 1
  ASSERT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  // step 2: insert data
  STSmaDataWrapper *pSmaData = NULL;
  STsdb             tsdb = {0};
  STsdbCfg *        pCfg = &tsdb.config;

  tsdb.pMeta = pMeta;
  tsdb.vgId = 2;
  tsdb.config.daysPerFile = 10;  // default days is 10
  tsdb.config.keep1 = 30;
  tsdb.config.keep2 = 90;
  tsdb.config.keep = 365;
  tsdb.config.precision = TSDB_TIME_PRECISION_MILLI;
  tsdb.config.update = TD_ROW_OVERWRITE_UPDATE;
  tsdb.config.compression = TWO_STAGE_COMP;

  switch (tsdb.config.precision) {
    case TSDB_TIME_PRECISION_MILLI:
      skey1 *= 1e3;
      break;
    case TSDB_TIME_PRECISION_MICRO:
      skey1 *= 1e6;
      break;
    case TSDB_TIME_PRECISION_NANO:
      skey1 *= 1e9;
      break;
    default:  // ms
      skey1 *= 1e3;
      break;
  }

  char *msg = (char *)calloc(100, 1);
  assert(msg != NULL);
  ASSERT_EQ(tsdbUpdateSmaWindow(&tsdb, TSDB_SMA_TYPE_TIME_RANGE, msg), 0);

  // init
  int32_t allocCnt = 0;
  int32_t allocStep = 16384;
  int32_t buffer = 1024;
  void *  buf = NULL;
  ASSERT_EQ(tsdbMakeRoom(&buf, allocStep), 0);
  int32_t  bufSize = taosTSizeof(buf);
  int32_t  numOfTables = 10;
  col_id_t numOfCols = 4096;
  ASSERT_GT(numOfCols, 0);

  pSmaData = (STSmaDataWrapper *)buf;
  printf(">> allocate [%d] time to %d and addr is %p\n", ++allocCnt, bufSize, pSmaData);
  pSmaData->skey = skey1;
  pSmaData->interval = interval1;
  pSmaData->intervalUnit = intervalUnit1;
  pSmaData->indexUid = indexUid1;

  int32_t len = sizeof(STSmaDataWrapper);
  for (int32_t t = 0; t < numOfTables; ++t) {
    STSmaTbData *pTbData = (STSmaTbData *)POINTER_SHIFT(pSmaData, len);
    pTbData->tableUid = tbUid + t;

    int32_t tableDataLen = sizeof(STSmaTbData);
    for (col_id_t c = 0; c < numOfCols; ++c) {
      if (bufSize - len - tableDataLen < buffer) {
        ASSERT_EQ(tsdbMakeRoom(&buf, bufSize + allocStep), 0);
        pSmaData = (STSmaDataWrapper *)buf;
        pTbData = (STSmaTbData *)POINTER_SHIFT(pSmaData, len);
        bufSize = taosTSizeof(buf);
        printf(">> allocate [%d] time to %d and addr is %p\n", ++allocCnt, bufSize, pSmaData);
      }
      STSmaColData *pColData = (STSmaColData *)POINTER_SHIFT(pSmaData, len + tableDataLen);
      pColData->colId = c + PRIMARYKEY_TIMESTAMP_COL_ID;

      // TODO: fill col data
      if ((c & 1) == 0) {
        pColData->blockSize = 8;
        memcpy(pColData->data, &testSmaData1, 8);
      } else {
        pColData->blockSize = 16;
        memcpy(pColData->data, &testSmaData1, 8);
        memcpy(POINTER_SHIFT(pColData->data, 8), &testSmaData2, 8);
      }

      tableDataLen += (sizeof(STSmaColData) + pColData->blockSize);
    }
    pTbData->dataLen = (tableDataLen - sizeof(STSmaTbData));
    len += tableDataLen;
    // printf("bufSize=%d, len=%d, len of table[%d]=%d\n", bufSize, len, t, tableDataLen);
  }
  pSmaData->dataLen = (len - sizeof(STSmaDataWrapper));

  ASSERT_GE(bufSize, pSmaData->dataLen);

  // execute
  ASSERT_EQ(tsdbInsertTSmaData(&tsdb, (char *)pSmaData), TSDB_CODE_SUCCESS);

  // step 3: query
  uint32_t checkDataCnt = 0;
  for (int32_t t = 0; t < numOfTables; ++t) {
    for (col_id_t c = 0; c < numOfCols; ++c) {
      ASSERT_EQ(tsdbGetTSmaData(&tsdb, NULL, indexUid1, interval1, intervalUnit1, tbUid + t,
                                c + PRIMARYKEY_TIMESTAMP_COL_ID, skey1, 1),
                TSDB_CODE_SUCCESS);
      ++checkDataCnt;
    }
  }

  printf("%s:%d The sma data check count for insert and query is %" PRIu32 "\n", __FILE__, __LINE__, checkDataCnt);

  // release data
  taosTZfree(buf);
  // release meta
  tdDestroyTSma(&tSma);
  metaClose(pMeta);
}
#endif

#pragma GCC diagnostic pop