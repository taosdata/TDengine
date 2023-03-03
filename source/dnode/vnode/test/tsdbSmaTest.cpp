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
#include <vnodeInt.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#include <tmsg.h>
#include <vnodeInt.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, unionEncodeDecodeTest) {
  typedef struct {
    union {
      uint8_t info;
      struct {
        uint8_t rollup : 1;  // 1 means rollup sma
        uint8_t type : 7;
      };
    };
    col_id_t  nBSmaCols;
    col_id_t *pBSmaCols;
  } SUnionTest;

  SUnionTest sut = {0};
  sut.rollup = 1;
  sut.type = 1;

  sut.nBSmaCols = 2;
  sut.pBSmaCols = (col_id_t *)taosMemoryMalloc(sut.nBSmaCols * sizeof(col_id_t));
  for (col_id_t i = 0; i < sut.nBSmaCols; ++i) {
    sut.pBSmaCols[i] = i + 100;
  }

  void   *buf = taosMemoryMalloc(1024);
  void   *pBuf = buf;
  void   *qBuf = buf;
  int32_t tlen = 0;
  tlen += taosEncodeFixedU8(&pBuf, sut.info);
  tlen += taosEncodeFixedI16(&pBuf, sut.nBSmaCols);
  for (col_id_t i = 0; i < sut.nBSmaCols; ++i) {
    tlen += taosEncodeFixedI16(&pBuf, sut.pBSmaCols[i]);
  }

  SUnionTest dut = {0};
  qBuf = taosDecodeFixedU8(qBuf, &dut.info);
  qBuf = taosDecodeFixedI16(qBuf, &dut.nBSmaCols);
  if (dut.nBSmaCols > 0) {
    dut.pBSmaCols = (col_id_t *)taosMemoryMalloc(dut.nBSmaCols * sizeof(col_id_t));
    for (col_id_t i = 0; i < dut.nBSmaCols; ++i) {
      qBuf = taosDecodeFixedI16(qBuf, dut.pBSmaCols + i);
    }
  } else {
    dut.pBSmaCols = NULL;
  }

  printf("sut.rollup=%" PRIu8 ", type=%" PRIu8 ", info=%" PRIu8 "\n", sut.rollup, sut.type, sut.info);
  printf("dut.rollup=%" PRIu8 ", type=%" PRIu8 ", info=%" PRIu8 "\n", dut.rollup, dut.type, dut.info);

  EXPECT_EQ(sut.rollup, dut.rollup);
  EXPECT_EQ(sut.type, dut.type);
  EXPECT_EQ(sut.nBSmaCols, dut.nBSmaCols);
  for (col_id_t i = 0; i < sut.nBSmaCols; ++i) {
    EXPECT_EQ(*(col_id_t *)(sut.pBSmaCols + i), sut.pBSmaCols[i]);
    EXPECT_EQ(*(col_id_t *)(sut.pBSmaCols + i), dut.pBSmaCols[i]);
  }

  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(dut.pBSmaCols);
  taosMemoryFreeClear(sut.pBSmaCols);
}
#if 1
TEST(testCase, tSma_Meta_Encode_Decode_Test) {
  // encode
  STSma tSma = {0};
  tSma.version = 0;
  tSma.intervalUnit = TIME_UNIT_DAY;
  tSma.interval = 1;
  tSma.slidingUnit = TIME_UNIT_HOUR;
  tSma.sliding = 0;
  tstrncpy(tSma.indexName, "sma_index_test", TSDB_INDEX_NAME_LEN);
  tSma.timezoneInt = 8;
  tSma.indexUid = 2345678910;
  tSma.tableUid = 1234567890;

  STSmaWrapper tSmaWrapper = {.number = 1, .tSma = &tSma};
  uint32_t     bufLen = tEncodeTSmaWrapper(NULL, &tSmaWrapper);

  void *buf = taosMemoryCalloc(1, bufLen);
  EXPECT_NE(buf, nullptr);

  STSmaWrapper *pSW = (STSmaWrapper *)buf;
  uint32_t      len = tEncodeTSmaWrapper(&buf, &tSmaWrapper);

  EXPECT_EQ(len, bufLen);

  // decode
  STSmaWrapper dstTSmaWrapper = {0};
  void        *result = tDecodeTSmaWrapper(pSW, &dstTSmaWrapper, false);
  EXPECT_NE(result, nullptr);

  EXPECT_EQ(tSmaWrapper.number, dstTSmaWrapper.number);

  for (int i = 0; i < tSmaWrapper.number; ++i) {
    STSma *pSma = tSmaWrapper.tSma + i;
    STSma *qSma = dstTSmaWrapper.tSma + i;

    EXPECT_EQ(pSma->version, qSma->version);
    EXPECT_EQ(pSma->intervalUnit, qSma->intervalUnit);
    EXPECT_EQ(pSma->slidingUnit, qSma->slidingUnit);
    EXPECT_STRCASEEQ(pSma->indexName, qSma->indexName);
    EXPECT_EQ(pSma->timezoneInt, qSma->timezoneInt);
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
  taosMemoryFreeClear(pSW);
  tDestroyTSma(&tSma);
  tDestroyTSmaWrapper(&dstTSmaWrapper);
}
#endif

#if 1
TEST(testCase, tSma_metaDB_Put_Get_Del_Test) {
  const char    *smaIndexName1 = "sma_index_test_1";
  const char    *smaIndexName2 = "sma_index_test_2";
  int8_t         timezone = 8;
  const char    *expr = "select count(a,b, top 20), from table interval 1d, sliding 1h;";
  const char    *tagsFilter = "I'm tags filter";
  const char    *smaTestDir = "./smaTest";
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
  tSma.timezoneInt = 8;
  tSma.tableUid = tbUid;

  tSma.exprLen = strlen(expr);
  tSma.expr = (char *)taosMemoryCalloc(1, tSma.exprLen + 1);
  EXPECT_NE(tSma.expr, nullptr);
  tstrncpy(tSma.expr, expr, tSma.exprLen + 1);

  tSma.tagsFilterLen = strlen(tagsFilter);
  tSma.tagsFilter = (char *)taosMemoryCalloc(tSma.tagsFilterLen + 1, 1);
  EXPECT_NE(tSma.tagsFilter, nullptr);
  tstrncpy(tSma.tagsFilter, tagsFilter, tSma.tagsFilterLen + 1);

  SMeta          *pMeta = NULL;
  STSma          *pSmaCfg = &tSma;
  const SMetaCfg *pMetaCfg = &defaultMetaOptions;

  taosRemoveDir(smaTestDir);

  pMeta = metaOpen(smaTestDir, pMetaCfg, NULL);
  assert(pMeta != NULL);
  // save index 1
  EXPECT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  pSmaCfg->indexUid = indexUid2;
  tstrncpy(pSmaCfg->indexName, smaIndexName2, TSDB_INDEX_NAME_LEN);
  pSmaCfg->version = 1;
  pSmaCfg->intervalUnit = TIME_UNIT_HOUR;
  pSmaCfg->interval = 1;
  pSmaCfg->slidingUnit = TIME_UNIT_MINUTE;
  pSmaCfg->sliding = 5;

  // save index 2
  EXPECT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  // get value by indexName
  STSma *qSmaCfg = NULL;
  qSmaCfg = metaGetSmaInfoByIndex(pMeta, indexUid1, true);
  assert(qSmaCfg != NULL);
  printf("name1 = %s\n", qSmaCfg->indexName);
  printf("timezone1 = %" PRIi8 "\n", qSmaCfg->timezoneInt);
  printf("expr1 = %s\n", qSmaCfg->expr != NULL ? qSmaCfg->expr : "");
  printf("tagsFilter1 = %s\n", qSmaCfg->tagsFilter != NULL ? qSmaCfg->tagsFilter : "");
  EXPECT_STRCASEEQ(qSmaCfg->indexName, smaIndexName1);
  EXPECT_EQ(qSmaCfg->tableUid, tSma.tableUid);
  tDestroyTSma(qSmaCfg);
  taosMemoryFreeClear(qSmaCfg);

  qSmaCfg = metaGetSmaInfoByIndex(pMeta, indexUid2, true);
  assert(qSmaCfg != NULL);
  printf("name2 = %s\n", qSmaCfg->indexName);
  printf("timezone2 = %" PRIi8 "\n", qSmaCfg->timezoneInt);
  printf("expr2 = %s\n", qSmaCfg->expr != NULL ? qSmaCfg->expr : "");
  printf("tagsFilter2 = %s\n", qSmaCfg->tagsFilter != NULL ? qSmaCfg->tagsFilter : "");
  EXPECT_STRCASEEQ(qSmaCfg->indexName, smaIndexName2);
  EXPECT_EQ(qSmaCfg->interval, tSma.interval);
  tDestroyTSma(qSmaCfg);
  taosMemoryFreeClear(qSmaCfg);

  // get index name by table uid
#if 0
  SMSmaCursor *pSmaCur = metaOpenSmaCursor(pMeta, tbUid);
  assert(pSmaCur != NULL);
  uint32_t indexCnt = 0;
  while (1) {
    const char *indexName = (const char *)metaSmaCursorNext(pSmaCur);
    if (indexName == NULL) {
      break;
    }
    printf("indexName = %s\n", indexName);
    ++indexCnt;
  }
  EXPECT_EQ(indexCnt, nCntTSma);
  metaCloseSmaCursor(pSmaCur);
#endif
  // get wrapper by table uid
  STSmaWrapper *pSW = metaGetSmaInfoByTable(pMeta, tbUid);
  assert(pSW != NULL);
  EXPECT_EQ(pSW->number, nCntTSma);
  EXPECT_STRCASEEQ(pSW->tSma->indexName, smaIndexName1);
  EXPECT_EQ(pSW->tSma->timezoneInt, timezone);
  EXPECT_STRCASEEQ(pSW->tSma->expr, expr);
  EXPECT_STRCASEEQ(pSW->tSma->tagsFilter, tagsFilter);
  EXPECT_EQ(pSW->tSma->indexUid, indexUid1);
  EXPECT_EQ(pSW->tSma->tableUid, tbUid);
  EXPECT_STRCASEEQ((pSW->tSma + 1)->indexName, smaIndexName2);
  EXPECT_EQ((pSW->tSma + 1)->timezoneInt, timezone);
  EXPECT_STRCASEEQ((pSW->tSma + 1)->expr, expr);
  EXPECT_STRCASEEQ((pSW->tSma + 1)->tagsFilter, tagsFilter);
  EXPECT_EQ((pSW->tSma + 1)->indexUid, indexUid2);
  EXPECT_EQ((pSW->tSma + 1)->tableUid, tbUid);

  tDestroyTSmaWrapper(pSW);
  taosMemoryFreeClear(pSW);

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
  metaRemoveSmaFromDb(pMeta, indexUid1);
  metaRemoveSmaFromDb(pMeta, indexUid2);

  tDestroyTSma(&tSma);
  metaClose(&pMeta);
}
#endif

#if 1
TEST(testCase, tSma_Data_Insert_Query_Test) {
  // step 1: prepare meta
  const char    *smaIndexName1 = "sma_index_test_1";
  const int8_t   timezone = 8;
  const char    *expr = "select count(a,b, top 20), from table interval 1d, sliding 1h;";
  const char    *tagsFilter = "where tags.location='Beijing' and tags.district='ChaoYang'";
  const char    *smaTestDir = "./smaTest";
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
  tSma.intervalUnit = TIME_UNIT_MINUTE;
  tSma.interval = 1;
  tSma.slidingUnit = TIME_UNIT_MINUTE;
  tSma.sliding = 1;  // sliding = interval when it's convert window
  tSma.indexUid = indexUid1;
  tstrncpy(tSma.indexName, smaIndexName1, TSDB_INDEX_NAME_LEN);
  tSma.timezoneInt = timezone;
  tSma.tableUid = tbUid;

  tSma.exprLen = strlen(expr);
  tSma.expr = (char *)taosMemoryCalloc(1, tSma.exprLen + 1);
  EXPECT_NE(tSma.expr, nullptr);
  tstrncpy(tSma.expr, expr, tSma.exprLen + 1);

  tSma.tagsFilterLen = strlen(tagsFilter);
  tSma.tagsFilter = (char *)taosMemoryCalloc(1, tSma.tagsFilterLen + 1);
  EXPECT_NE(tSma.tagsFilter, nullptr);
  tstrncpy(tSma.tagsFilter, tagsFilter, tSma.tagsFilterLen + 1);

  SMeta          *pMeta = NULL;
  STSma          *pSmaCfg = &tSma;
  const SMetaCfg *pMetaCfg = &defaultMetaOptions;

  taosRemoveDir(smaTestDir);

  pMeta = metaOpen(smaTestDir, pMetaCfg, NULL);
  assert(pMeta != NULL);
  // save index 1
  EXPECT_EQ(metaSaveSmaToDB(pMeta, pSmaCfg), 0);

  // step 2: insert data
  STsdb    *pTsdb = (STsdb *)taosMemoryCalloc(1, sizeof(STsdb));
  STsdbCfg *pCfg = &pTsdb->config;

  pTsdb->pMeta = pMeta;
  pTsdb->vgId = 2;
  pTsdb->config.daysPerFile = 10;  // default days is 10
  pTsdb->config.keep1 = 30;
  pTsdb->config.keep2 = 90;
  pTsdb->config.keep = 365;
  pTsdb->config.precision = TSDB_TIME_PRECISION_MILLI;
  pTsdb->config.update = TD_ROW_OVERWRITE_UPDATE;
  pTsdb->config.compression = TWO_STAGE_COMP;

  switch (pTsdb->config.precision) {
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

  SDiskCfg pDisks = {0};
  pDisks.level = 0;
  pDisks.primary = 1;
  strncpy(pDisks.dir, TD_DATA_DIR_PATH, TSDB_FILENAME_LEN);
  int32_t numOfDisks = 1;
  pTsdb->pTfs = tfsOpen(&pDisks, numOfDisks);
  EXPECT_NE(pTsdb->pTfs, nullptr);

  // generate SSubmitReq msg and update expire window
  int16_t  schemaVer = 0;
  uint32_t mockRowLen = sizeof(STSRow);
  uint32_t mockRowNum = 2;
  uint32_t mockBlkNum = 2;
  uint32_t msgLen = sizeof(SSubmitReq) + mockBlkNum * sizeof(SSubmitBlk) + mockBlkNum * mockRowNum * mockRowLen;

  SSubmitReq *pMsg = (SSubmitReq *)taosMemoryCalloc(1, msgLen);
  EXPECT_NE(pMsg, nullptr);
  pMsg->version = htobe64(schemaVer);
  pMsg->numOfBlocks = htonl(mockBlkNum);
  pMsg->length = htonl(msgLen);

  SSubmitBlk *pBlk = NULL;
  STSRow     *pRow = NULL;
  TSKEY       now = taosGetTimestamp(pTsdb->config.precision);

  for (uint32_t b = 0; b < mockBlkNum; ++b) {
    pBlk = (SSubmitBlk *)POINTER_SHIFT(pMsg, sizeof(SSubmitReq) + b * (sizeof(SSubmitBlk) + mockRowNum * mockRowLen));
    pBlk->uid = htobe64(tbUid);
    pBlk->suid = htobe64(tbUid);
    pBlk->sversion = htonl(schemaVer);
    pBlk->schemaLen = htonl(0);
    pBlk->numOfRows = htonl(mockRowNum);
    pBlk->dataLen = htonl(mockRowNum * mockRowLen);
    for (uint32_t r = 0; r < mockRowNum; ++r) {
      pRow = (STSRow *)POINTER_SHIFT(pBlk, sizeof(SSubmitBlk) + r * mockRowLen);
      pRow->len = mockRowLen;
      pRow->ts = now + b * 1000 + r * 1000;
      pRow->sver = schemaVer;
    }
  }

  // EXPECT_EQ(tdScanAndConvertSubmitMsg(pMsg), TSDB_CODE_SUCCESS);

  EXPECT_EQ(tsdbUpdateSmaWindow(pTsdb, pMsg, 0), 0);

  // init
  const int32_t  tSmaGroupSize = 4;
  const int32_t  tSmaNumOfTags = 2;
  const int64_t  tSmaGroupId = 12345670;
  const col_id_t tSmaNumOfCols = 9;  // binary/nchar/varbinary/varchar are only used for tags for group by conditions.
  const int32_t  tSmaNumOfRows = 2;

  SArray *pDataBlocks = taosArrayInit(tSmaGroupSize, sizeof(SSDataBlock *));
  EXPECT_NE(pDataBlocks, nullptr);
  int32_t tSmaTypeArray[tSmaNumOfCols] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_BOOL,     TSDB_DATA_TYPE_INT,
                                          TSDB_DATA_TYPE_UBIGINT,   TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT,
                                          TSDB_DATA_TYPE_DOUBLE,    TSDB_DATA_TYPE_VARCHAR,  TSDB_DATA_TYPE_NCHAR};
  // last 2 columns for group by tags
  // int32_t tSmaTypeArray[tSmaNumOfCols] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_BOOL};
  const char *tSmaGroupbyTags[tSmaGroupSize * tSmaNumOfTags] = {"BeiJing",  "HaiDian", "BeiJing",  "ChaoYang",
                                                                "ShangHai", "PuDong",  "ShangHai", "MinHang"};
  TSKEY       tSmaSKeyMs = (int64_t)1648535332 * 1000;
  int64_t     tSmaIntervalMs = tSma.interval * 60 * 1000;
  int64_t     tSmaInitVal = 0;

  for (int32_t g = 0; g < tSmaGroupSize; ++g) {
    SSDataBlock *pDataBlock = (SSDataBlock *)taosMemoryCalloc(1, sizeof(SSDataBlock));
    EXPECT_NE(pDataBlock, nullptr);
    pDataBlock->pBlockAgg = NULL;
    taosArrayGetSize(pDataBlock->pDataBlock) = tSmaNumOfCols;
    pDataBlock->info.rows = tSmaNumOfRows;
    pDataBlock->info.id.groupId = tSmaGroupId + g;

    pDataBlock->pDataBlock = taosArrayInit(tSmaNumOfCols, sizeof(SColumnInfoData *));
    EXPECT_NE(pDataBlock->pDataBlock, nullptr);
    for (int32_t c = 0; c < tSmaNumOfCols; ++c) {
      SColumnInfoData *pColInfoData = (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
      EXPECT_NE(pColInfoData, nullptr);

      pColInfoData->info.type = tSmaTypeArray[c];
      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        pColInfoData->info.bytes = 100;  // update accordingly
      } else {
        pColInfoData->info.bytes = TYPE_BYTES[pColInfoData->info.type];
      }
      pColInfoData->pData = (char *)taosMemoryCalloc(1, tSmaNumOfRows * pColInfoData->info.bytes);

      for (int32_t r = 0; r < tSmaNumOfRows; ++r) {
        void *pCellData = pColInfoData->pData + r * pColInfoData->info.bytes;
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            *(TSKEY *)pCellData = tSmaSKeyMs + tSmaIntervalMs * r;
            break;
          case TSDB_DATA_TYPE_BOOL:
            *(bool *)pCellData = (bool)tSmaInitVal++;
            break;
          case TSDB_DATA_TYPE_INT:
            *(int *)pCellData = (int)tSmaInitVal++;
            break;
          case TSDB_DATA_TYPE_UBIGINT:
            *(uint64_t *)pCellData = (uint64_t)tSmaInitVal++;
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            *(int16_t *)pCellData = (int16_t)tSmaInitVal++;
            break;
          case TSDB_DATA_TYPE_FLOAT:
            *(float *)pCellData = (float)tSmaInitVal++;
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            *(double *)pCellData = (double)tSmaInitVal++;
            break;
          case TSDB_DATA_TYPE_VARCHAR:  // city
            varDataSetLen(pCellData, strlen(tSmaGroupbyTags[g * 2]));
            memcpy(varDataVal(pCellData), tSmaGroupbyTags[g * 2], varDataLen(pCellData));
            break;
          case TSDB_DATA_TYPE_NCHAR:  // district
            varDataSetLen(pCellData, strlen(tSmaGroupbyTags[g * 2 + 1]));
            memcpy(varDataVal(pCellData), tSmaGroupbyTags[g * 2 + 1], varDataLen(pCellData));
            break;
          default:
            EXPECT_EQ(0, 1);  // add definition
            break;
        }
      }
      // push SColumnInfoData
      taosArrayPush(pDataBlock->pDataBlock, &pColInfoData);
    }
    // push SSDataBlock
    taosArrayPush(pDataBlocks, &pDataBlock);
  }

  // execute
  EXPECT_EQ(tsdbInsertTSmaData(pTsdb, tSma.indexUid, (const char *)pDataBlocks), TSDB_CODE_SUCCESS);

#if 0
  STSmaDataWrapper *pSmaData = NULL;
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
        EXPECT_EQ(tsdbMakeRoom(&buf, bufSize + allocStep), 0);
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

  EXPECT_GE(bufSize, pSmaData->dataLen);
  // execute
  EXPECT_EQ(tsdbInsertTSmaData(pTsdb, (char *)pSmaData), TSDB_CODE_SUCCESS);
#endif

  // step 3: query
  uint32_t checkDataCnt = 0;
  EXPECT_EQ(tsdbGetTSmaData(pTsdb, NULL, indexUid1, skey1, 1), TSDB_CODE_SUCCESS);
  ++checkDataCnt;

  printf("%s:%d The sma data check count for insert and query is %" PRIu32 "\n", __FILE__, __LINE__, checkDataCnt);

  // release data
  taosMemoryFreeClear(pMsg);

  for (int32_t i = 0; i < taosArrayGetSize(pDataBlocks); ++i) {
    SSDataBlock *pDataBlock = *(SSDataBlock **)taosArrayGet(pDataBlocks, i);
    int32_t      numOfOutput = taosArrayGetSize(pDataBlock->pDataBlock);
    for (int32_t j = 0; j < numOfOutput; ++j) {
      SColumnInfoData *pColInfoData = *(SColumnInfoData **)taosArrayGet(pDataBlock->pDataBlock, j);
      colDataDestroy(pColInfoData);
      taosMemoryFreeClear(pColInfoData);
    }

    taosArrayDestroy(pDataBlock->pDataBlock);
    taosMemoryFreeClear(pDataBlock->pBlockAgg);
    taosMemoryFreeClear(pDataBlock);
  }
  taosArrayDestroy(pDataBlocks);

  // release meta
  tDestroyTSma(&tSma);
  tfsClose(pTsdb->pTfs);
  tsdbClose(pTsdb);
  metaClose(&pMeta);
}

#endif

#pragma GCC diagnostic pop
