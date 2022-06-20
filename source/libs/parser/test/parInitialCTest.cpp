/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), AS published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserInitialCTest : public ParserDdlTest {};

// todo compact

TEST_F(ParserInitialCTest, createAccount) {
  useDb("root", "test");

  run("CREATE ACCOUNT ac_wxy PASS '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT, PARSER_STAGE_PARSE);
}

TEST_F(ParserInitialCTest, createBnode) {
  useDb("root", "test");

  run("CREATE BNODE ON DNODE 1");
}

/*
 * CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 *
 * database_options:
 *     database_option ...
 *
 * database_option: {
 *     BUFFER value
 *   | CACHELAST value
 *   | COMP {0 | 1 | 2}
 *   | DURATION value
 *   | FSYNC value
 *   | MAXROWS value
 *   | MINROWS value
 *   | KEEP value
 *   | PAGES value
 *   | PAGESIZE  value
 *   | PRECISION {'ms' | 'us' | 'ns'}
 *   | REPLICA value
 *   | RETENTIONS ingestion_duration:keep_duration ...
 *   | STRICT value
 *   | WAL value
 *   | VGROUPS value
 *   | SINGLE_STABLE {0 | 1}
 * }
 */
TEST_F(ParserInitialCTest, createDatabase) {
  useDb("root", "test");

  SCreateDbReq expect = {0};

  auto clearCreateDbReq = [&]() {
    tFreeSCreateDbReq(&expect);
    memset(&expect, 0, sizeof(SCreateDbReq));
  };

  auto setCreateDbReqFunc = [&](const char* pDbname, int8_t igExists = 0) {
    int32_t len = snprintf(expect.db, sizeof(expect.db), "0.%s", pDbname);
    expect.db[len] = '\0';
    expect.ignoreExist = igExists;
    expect.buffer = TSDB_DEFAULT_BUFFER_PER_VNODE;
    expect.cacheLastRow = TSDB_DEFAULT_CACHE_LAST_ROW;
    expect.compression = TSDB_DEFAULT_COMP_LEVEL;
    expect.daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
    expect.fsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
    expect.maxRows = TSDB_DEFAULT_MAXROWS_FBLOCK;
    expect.minRows = TSDB_DEFAULT_MINROWS_FBLOCK;
    expect.daysToKeep0 = TSDB_DEFAULT_KEEP;
    expect.daysToKeep1 = TSDB_DEFAULT_KEEP;
    expect.daysToKeep2 = TSDB_DEFAULT_KEEP;
    expect.pages = TSDB_DEFAULT_PAGES_PER_VNODE;
    expect.pageSize = TSDB_DEFAULT_PAGESIZE_PER_VNODE;
    expect.precision = TSDB_DEFAULT_PRECISION;
    expect.replications = TSDB_DEFAULT_DB_REPLICA;
    expect.strict = TSDB_DEFAULT_DB_STRICT;
    expect.walLevel = TSDB_DEFAULT_WAL_LEVEL;
    expect.numOfVgroups = TSDB_DEFAULT_VN_PER_DB;
    expect.numOfStables = TSDB_DEFAULT_DB_SINGLE_STABLE;
    expect.schemaless = TSDB_DEFAULT_DB_SCHEMALESS;
  };

  auto setDbBufferFunc = [&](int32_t buffer) { expect.buffer = buffer; };
  auto setDbCachelastFunc = [&](int8_t CACHELAST) { expect.cacheLastRow = CACHELAST; };
  auto setDbCompressionFunc = [&](int8_t compressionLevel) { expect.compression = compressionLevel; };
  auto setDbDaysFunc = [&](int32_t daysPerFile) { expect.daysPerFile = daysPerFile; };
  auto setDbFsyncFunc = [&](int32_t fsyncPeriod) { expect.fsyncPeriod = fsyncPeriod; };
  auto setDbMaxRowsFunc = [&](int32_t maxRowsPerBlock) { expect.maxRows = maxRowsPerBlock; };
  auto setDbMinRowsFunc = [&](int32_t minRowsPerBlock) { expect.minRows = minRowsPerBlock; };
  auto setDbKeepFunc = [&](int32_t keep0, int32_t keep1 = 0, int32_t keep2 = 0) {
    expect.daysToKeep0 = keep0;
    expect.daysToKeep1 = 0 == keep1 ? expect.daysToKeep0 : keep1;
    expect.daysToKeep2 = 0 == keep2 ? expect.daysToKeep1 : keep2;
  };
  auto setDbPagesFunc = [&](int32_t pages) { expect.pages = pages; };
  auto setDbPageSizeFunc = [&](int32_t pagesize) { expect.pageSize = pagesize; };
  auto setDbPrecisionFunc = [&](int8_t precision) { expect.precision = precision; };
  auto setDbReplicaFunc = [&](int8_t replica) { expect.replications = replica; };
  auto setDbStrictaFunc = [&](int8_t strict) { expect.strict = strict; };
  auto setDbWalLevelFunc = [&](int8_t walLevel) { expect.walLevel = walLevel; };
  auto setDbVgroupsFunc = [&](int32_t numOfVgroups) { expect.numOfVgroups = numOfVgroups; };
  auto setDbSingleStableFunc = [&](int8_t singleStable) { expect.numOfStables = singleStable; };
  auto addDbRetentionFunc = [&](int64_t freq, int64_t keep, int8_t freqUnit, int8_t keepUnit) {
    SRetention retention = {0};
    retention.freq = freq;
    retention.keep = keep;
    retention.freqUnit = freqUnit;
    retention.keepUnit = keepUnit;
    if (NULL == expect.pRetensions) {
      expect.pRetensions = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SRetention));
    }
    taosArrayPush(expect.pRetensions, &retention);
    ++expect.numOfRetensions;
  };
  auto setDbSchemalessFunc = [&](int8_t schemaless) { expect.schemaless = schemaless; };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_DATABASE_STMT);
    SCreateDbReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSCreateDbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.db), std::string(expect.db));
    ASSERT_EQ(req.numOfVgroups, expect.numOfVgroups);
    ASSERT_EQ(req.numOfStables, expect.numOfStables);
    ASSERT_EQ(req.buffer, expect.buffer);
    ASSERT_EQ(req.pageSize, expect.pageSize);
    ASSERT_EQ(req.pages, expect.pages);
    ASSERT_EQ(req.daysPerFile, expect.daysPerFile);
    ASSERT_EQ(req.daysToKeep0, expect.daysToKeep0);
    ASSERT_EQ(req.daysToKeep1, expect.daysToKeep1);
    ASSERT_EQ(req.daysToKeep2, expect.daysToKeep2);
    ASSERT_EQ(req.minRows, expect.minRows);
    ASSERT_EQ(req.maxRows, expect.maxRows);
    ASSERT_EQ(req.fsyncPeriod, expect.fsyncPeriod);
    ASSERT_EQ(req.walLevel, expect.walLevel);
    ASSERT_EQ(req.precision, expect.precision);
    ASSERT_EQ(req.compression, expect.compression);
    ASSERT_EQ(req.replications, expect.replications);
    ASSERT_EQ(req.strict, expect.strict);
    ASSERT_EQ(req.cacheLastRow, expect.cacheLastRow);
    // ASSERT_EQ(req.schemaless, expect.schemaless);
    ASSERT_EQ(req.ignoreExist, expect.ignoreExist);
    ASSERT_EQ(req.numOfRetensions, expect.numOfRetensions);
    if (expect.numOfRetensions > 0) {
      ASSERT_EQ(taosArrayGetSize(req.pRetensions), expect.numOfRetensions);
      ASSERT_EQ(taosArrayGetSize(req.pRetensions), taosArrayGetSize(expect.pRetensions));
      for (int32_t i = 0; i < expect.numOfRetensions; ++i) {
        SRetention* pReten = (SRetention*)taosArrayGet(req.pRetensions, i);
        SRetention* pExpectReten = (SRetention*)taosArrayGet(expect.pRetensions, i);
        ASSERT_EQ(pReten->freq, pExpectReten->freq);
        ASSERT_EQ(pReten->keep, pExpectReten->keep);
        ASSERT_EQ(pReten->freqUnit, pExpectReten->freqUnit);
        ASSERT_EQ(pReten->keepUnit, pExpectReten->keepUnit);
      }
    }
    tFreeSCreateDbReq(&req);
  });

  setCreateDbReqFunc("wxy_db");
  run("CREATE DATABASE wxy_db");
  clearCreateDbReq();

  setCreateDbReqFunc("wxy_db", 1);
  setDbBufferFunc(64);
  setDbCachelastFunc(2);
  setDbCompressionFunc(1);
  setDbDaysFunc(100 * 1440);
  setDbFsyncFunc(100);
  setDbMaxRowsFunc(1000);
  setDbMinRowsFunc(100);
  setDbKeepFunc(1440 * 1440);
  setDbPagesFunc(96);
  setDbPageSizeFunc(8);
  setDbPrecisionFunc(TSDB_TIME_PRECISION_NANO);
  setDbReplicaFunc(3);
  addDbRetentionFunc(15 * MILLISECOND_PER_SECOND, 7 * MILLISECOND_PER_DAY, TIME_UNIT_SECOND, TIME_UNIT_DAY);
  addDbRetentionFunc(1 * MILLISECOND_PER_MINUTE, 21 * MILLISECOND_PER_DAY, TIME_UNIT_MINUTE, TIME_UNIT_DAY);
  addDbRetentionFunc(15 * MILLISECOND_PER_MINUTE, 500 * MILLISECOND_PER_DAY, TIME_UNIT_MINUTE, TIME_UNIT_DAY);
  setDbStrictaFunc(1);
  setDbWalLevelFunc(2);
  setDbVgroupsFunc(100);
  setDbSingleStableFunc(1);
  setDbSchemalessFunc(1);
  run("CREATE DATABASE IF NOT EXISTS wxy_db "
      "BUFFER 64 "
      "CACHELAST 2 "
      "COMP 1 "
      "DURATION 100 "
      "FSYNC 100 "
      "MAXROWS 1000 "
      "MINROWS 100 "
      "KEEP 1440 "
      "PAGES 96 "
      "PAGESIZE 8 "
      "PRECISION 'ns' "
      "REPLICA 3 "
      "RETENTIONS 15s:7d,1m:21d,15m:500d "
      "STRICT 1 "
      "WAL 2 "
      "VGROUPS 100 "
      "SINGLE_STABLE 1 "
      "SCHEMALESS 1");
  clearCreateDbReq();

  setCreateDbReqFunc("wxy_db", 1);
  setDbDaysFunc(100);
  setDbKeepFunc(1440, 300 * 60, 400 * 1440);
  run("CREATE DATABASE IF NOT EXISTS wxy_db "
      "DURATION 100m "
      "KEEP 1440m,300h,400d ");
  clearCreateDbReq();
}

TEST_F(ParserInitialCTest, createDatabaseSemanticCheck) {
  useDb("root", "test");

  run("create database db2 retentions 0s:1d", TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
  run("create database db2 retentions 10s:0d", TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
  run("create database db2 retentions 1w:1d", TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
  run("create database db2 retentions 1w:1n", TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
  run("create database db2 retentions 15s:7d,15m:21d,10m:500d", TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
  run("create database db2 retentions 15s:7d,5m:21d,10m:10d", TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
}

TEST_F(ParserInitialCTest, createDnode) {
  useDb("root", "test");

  SCreateDnodeReq expect = {0};

  auto clearCreateDnodeReq = [&]() { memset(&expect, 0, sizeof(SCreateDnodeReq)); };

  auto setCreateDnodeReqFunc = [&](const char* pFqdn, int32_t port = tsServerPort) {
    strcpy(expect.fqdn, pFqdn);
    expect.port = port;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_DNODE_STMT);
    SCreateDnodeReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSCreateDnodeReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.fqdn), std::string(expect.fqdn));
    ASSERT_EQ(req.port, expect.port);
  });

  setCreateDnodeReqFunc("abc1", 7030);
  run("CREATE DNODE 'abc1' PORT 7030");
  clearCreateDnodeReq();

  setCreateDnodeReqFunc("1.1.1.1", 8030);
  run("CREATE DNODE 1.1.1.1 PORT 8030");
  clearCreateDnodeReq();

  setCreateDnodeReqFunc("host1", 9030);
  run("CREATE DNODE host1 PORT 9030");
  clearCreateDnodeReq();

  setCreateDnodeReqFunc("abc2", 7040);
  run("CREATE DNODE 'abc2:7040'");
  clearCreateDnodeReq();

  setCreateDnodeReqFunc("1.1.1.2");
  run("CREATE DNODE 1.1.1.2");
  clearCreateDnodeReq();

  setCreateDnodeReqFunc("host2");
  run("CREATE DNODE host2");
  clearCreateDnodeReq();
}

// CREATE [AGGREGATE] FUNCTION [IF NOT EXISTS] func_name AS library_path OUTPUTTYPE type_name [BUFSIZE value]
TEST_F(ParserInitialCTest, createFunction) {
  useDb("root", "test");

  SCreateFuncReq expect = {0};

  auto setCreateFuncReqFunc = [&](const char* pUdfName, int8_t outputType, int32_t outputBytes = 0,
                                  int8_t funcType = TSDB_FUNC_TYPE_SCALAR, int8_t igExists = 0, int32_t bufSize = 0) {
    memset(&expect, 0, sizeof(SCreateFuncReq));
    strcpy(expect.name, pUdfName);
    expect.igExists = igExists;
    expect.funcType = funcType;
    expect.scriptType = TSDB_FUNC_SCRIPT_BIN_LIB;
    expect.outputType = outputType;
    expect.outputLen = outputBytes > 0 ? outputBytes : tDataTypes[outputType].bytes;
    expect.bufSize = bufSize;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_FUNCTION_STMT);
    SCreateFuncReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSCreateFuncReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(req.igExists, expect.igExists);
    ASSERT_EQ(req.funcType, expect.funcType);
    ASSERT_EQ(req.scriptType, expect.scriptType);
    ASSERT_EQ(req.outputType, expect.outputType);
    ASSERT_EQ(req.outputLen, expect.outputLen);
    ASSERT_EQ(req.bufSize, expect.bufSize);
  });

  setCreateFuncReqFunc("udf1", TSDB_DATA_TYPE_INT);
  // run("CREATE FUNCTION udf1 AS './build/lib/libudf1.so' OUTPUTTYPE INT");

  setCreateFuncReqFunc("udf2", TSDB_DATA_TYPE_DOUBLE, 0, TSDB_FUNC_TYPE_AGGREGATE, 1, 8);
  // run("CREATE AGGREGATE FUNCTION IF NOT EXISTS udf2 AS './build/lib/libudf2.so' OUTPUTTYPE DOUBLE BUFSIZE 8");
}

TEST_F(ParserInitialCTest, createSmaIndex) {
  useDb("root", "test");

  run("CREATE SMA INDEX index1 ON t1 FUNCTION(MAX(c1), MIN(c3 + 10), SUM(c4)) INTERVAL(10s)");

  run("CREATE SMA INDEX index2 ON st1 FUNCTION(MAX(c1), MIN(tag1)) INTERVAL(10s)");
}

TEST_F(ParserInitialCTest, createMnode) {
  useDb("root", "test");

  run("CREATE MNODE ON DNODE 1");
}

TEST_F(ParserInitialCTest, createQnode) {
  useDb("root", "test");

  run("CREATE QNODE ON DNODE 1");
}

TEST_F(ParserInitialCTest, createSnode) {
  useDb("root", "test");

  run("CREATE SNODE ON DNODE 1");
}

TEST_F(ParserInitialCTest, createStable) {
  useDb("root", "test");

  SMCreateStbReq expect = {0};

  auto clearCreateStbReq = [&]() {
    tFreeSMCreateStbReq(&expect);
    memset(&expect, 0, sizeof(SMCreateStbReq));
  };

  auto setCreateStbReqFunc = [&](const char* pTbname, int8_t igExists = 0, int64_t delay1 = -1, int64_t delay2 = -1,
                                 int64_t watermark1 = TSDB_DEFAULT_ROLLUP_WATERMARK,
                                 int64_t watermark2 = TSDB_DEFAULT_ROLLUP_WATERMARK,
                                 int32_t ttl = TSDB_DEFAULT_TABLE_TTL, const char* pComment = nullptr) {
    int32_t len = snprintf(expect.name, sizeof(expect.name), "0.test.%s", pTbname);
    expect.name[len] = '\0';
    expect.igExists = igExists;
    expect.delay1 = delay1;
    expect.delay2 = delay2;
    expect.watermark1 = watermark1;
    expect.watermark2 = watermark2;
//    expect.ttl = ttl;
    if (nullptr != pComment) {
      expect.comment = strdup(pComment);
      expect.commentLen = strlen(pComment);
    }
  };

  auto addFieldToCreateStbReqFunc = [&](bool col, const char* pFieldName, uint8_t type, int32_t bytes = 0,
                                        int8_t flags = COL_SMA_ON) {
    SField field = {0};
    strcpy(field.name, pFieldName);
    field.type = type;
    field.bytes = bytes > 0 ? bytes : tDataTypes[type].bytes;
    field.flags = flags;

    if (col) {
      if (NULL == expect.pColumns) {
        expect.pColumns = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SField));
      }
      taosArrayPush(expect.pColumns, &field);
      expect.numOfColumns += 1;
    } else {
      if (NULL == expect.pTags) {
        expect.pTags = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SField));
      }
      taosArrayPush(expect.pTags, &field);
      expect.numOfTags += 1;
    }
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_TABLE_STMT);
    SMCreateStbReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSMCreateStbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(req.igExists, expect.igExists);
    ASSERT_EQ(req.delay1, expect.delay1);
    ASSERT_EQ(req.delay2, expect.delay2);
    ASSERT_EQ(req.watermark1, expect.watermark1);
    ASSERT_EQ(req.watermark2, expect.watermark2);
    ASSERT_EQ(req.ttl, expect.ttl);
    ASSERT_EQ(req.numOfColumns, expect.numOfColumns);
    ASSERT_EQ(req.numOfTags, expect.numOfTags);
//    ASSERT_EQ(req.commentLen, expect.commentLen);
    ASSERT_EQ(req.ast1Len, expect.ast1Len);
    ASSERT_EQ(req.ast2Len, expect.ast2Len);

    if (expect.numOfColumns > 0) {
      ASSERT_EQ(taosArrayGetSize(req.pColumns), expect.numOfColumns);
      ASSERT_EQ(taosArrayGetSize(req.pColumns), taosArrayGetSize(expect.pColumns));
      for (int32_t i = 0; i < expect.numOfColumns; ++i) {
        SField* pField = (SField*)taosArrayGet(req.pColumns, i);
        SField* pExpectField = (SField*)taosArrayGet(expect.pColumns, i);
        ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
        ASSERT_EQ(pField->type, pExpectField->type);
        ASSERT_EQ(pField->bytes, pExpectField->bytes);
        ASSERT_EQ(pField->flags, pExpectField->flags);
      }
    }
    if (expect.numOfTags > 0) {
      ASSERT_EQ(taosArrayGetSize(req.pTags), expect.numOfTags);
      ASSERT_EQ(taosArrayGetSize(req.pTags), taosArrayGetSize(expect.pTags));
      for (int32_t i = 0; i < expect.numOfTags; ++i) {
        SField* pField = (SField*)taosArrayGet(req.pTags, i);
        SField* pExpectField = (SField*)taosArrayGet(expect.pTags, i);
        ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
        ASSERT_EQ(pField->type, pExpectField->type);
        ASSERT_EQ(pField->bytes, pExpectField->bytes);
        ASSERT_EQ(pField->flags, pExpectField->flags);
      }
    }
    if (expect.commentLen > 0) {
      ASSERT_EQ(std::string(req.comment), std::string(expect.comment));
    }
    if (expect.ast1Len > 0) {
      ASSERT_EQ(std::string(req.pAst1), std::string(expect.pAst1));
    }
    if (expect.ast2Len > 0) {
      ASSERT_EQ(std::string(req.pAst2), std::string(expect.pAst2));
    }
    tFreeSMCreateStbReq(&req);
  });

  setCreateStbReqFunc("t1");
  addFieldToCreateStbReqFunc(true, "ts", TSDB_DATA_TYPE_TIMESTAMP);
  addFieldToCreateStbReqFunc(true, "c1", TSDB_DATA_TYPE_INT);
  addFieldToCreateStbReqFunc(false, "id", TSDB_DATA_TYPE_INT);
  run("CREATE STABLE t1(ts TIMESTAMP, c1 INT) TAGS(id INT)");
  clearCreateStbReq();

  setCreateStbReqFunc("t1", 1, 100 * MILLISECOND_PER_SECOND, 10 * MILLISECOND_PER_MINUTE, 10,
                      1 * MILLISECOND_PER_MINUTE, 100, "test create table");
  addFieldToCreateStbReqFunc(true, "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0);
  addFieldToCreateStbReqFunc(true, "c1", TSDB_DATA_TYPE_INT);
  addFieldToCreateStbReqFunc(true, "c2", TSDB_DATA_TYPE_UINT);
  addFieldToCreateStbReqFunc(true, "c3", TSDB_DATA_TYPE_BIGINT);
  addFieldToCreateStbReqFunc(true, "c4", TSDB_DATA_TYPE_UBIGINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c5", TSDB_DATA_TYPE_FLOAT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c6", TSDB_DATA_TYPE_DOUBLE, 0, 0);
  addFieldToCreateStbReqFunc(true, "c7", TSDB_DATA_TYPE_BINARY, 20 + VARSTR_HEADER_SIZE, 0);
  addFieldToCreateStbReqFunc(true, "c8", TSDB_DATA_TYPE_SMALLINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c9", TSDB_DATA_TYPE_USMALLINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c10", TSDB_DATA_TYPE_TINYINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c11", TSDB_DATA_TYPE_UTINYINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c12", TSDB_DATA_TYPE_BOOL, 0, 0);
  addFieldToCreateStbReqFunc(true, "c13", TSDB_DATA_TYPE_NCHAR, 30 * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE, 0);
  addFieldToCreateStbReqFunc(true, "c14", TSDB_DATA_TYPE_VARCHAR, 50 + VARSTR_HEADER_SIZE, 0);
  addFieldToCreateStbReqFunc(false, "a1", TSDB_DATA_TYPE_TIMESTAMP);
  addFieldToCreateStbReqFunc(false, "a2", TSDB_DATA_TYPE_INT);
  addFieldToCreateStbReqFunc(false, "a3", TSDB_DATA_TYPE_UINT);
  addFieldToCreateStbReqFunc(false, "a4", TSDB_DATA_TYPE_BIGINT);
  addFieldToCreateStbReqFunc(false, "a5", TSDB_DATA_TYPE_UBIGINT);
  addFieldToCreateStbReqFunc(false, "a6", TSDB_DATA_TYPE_FLOAT);
  addFieldToCreateStbReqFunc(false, "a7", TSDB_DATA_TYPE_DOUBLE);
  addFieldToCreateStbReqFunc(false, "a8", TSDB_DATA_TYPE_BINARY, 20 + VARSTR_HEADER_SIZE);
  addFieldToCreateStbReqFunc(false, "a9", TSDB_DATA_TYPE_SMALLINT);
  addFieldToCreateStbReqFunc(false, "a10", TSDB_DATA_TYPE_USMALLINT);
  addFieldToCreateStbReqFunc(false, "a11", TSDB_DATA_TYPE_TINYINT);
  addFieldToCreateStbReqFunc(false, "a12", TSDB_DATA_TYPE_UTINYINT);
  addFieldToCreateStbReqFunc(false, "a13", TSDB_DATA_TYPE_BOOL);
  addFieldToCreateStbReqFunc(false, "a14", TSDB_DATA_TYPE_NCHAR, 30 * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
  addFieldToCreateStbReqFunc(false, "a15", TSDB_DATA_TYPE_VARCHAR, 50 + VARSTR_HEADER_SIZE);
  run("CREATE STABLE IF NOT EXISTS test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), "
      "c8 SMALLINT, c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, "
      "c13 NCHAR(30), c14 VARCHAR(50)) "
      "TAGS (a1 TIMESTAMP, a2 INT, a3 INT UNSIGNED, a4 BIGINT, a5 BIGINT UNSIGNED, a6 FLOAT, a7 DOUBLE, "
      "a8 BINARY(20), a9 SMALLINT, a10 SMALLINT UNSIGNED COMMENT 'test column comment', a11 TINYINT, "
      "a12 TINYINT UNSIGNED, a13 BOOL, a14 NCHAR(30), a15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create table' SMA(c1, c2, c3) ROLLUP (MIN) MAX_DELAY 100s,10m WATERMARK 10a,1m");
  clearCreateStbReq();
}

TEST_F(ParserInitialCTest, createStableSemanticCheck) {
  useDb("root", "test");

  run("CREATE STABLE stb2 (ts TIMESTAMP, c1 INT) TAGS (tag1 INT) ROLLUP(CEIL)", TSDB_CODE_PAR_INVALID_ROLLUP_OPTION);

  run("CREATE STABLE stb2 (ts TIMESTAMP, c1 INT) TAGS (tag1 INT) ROLLUP(MAX) MAX_DELAY 0s WATERMARK 1m",
      TSDB_CODE_PAR_INVALID_RANGE_OPTION);

  run("CREATE STABLE stb2 (ts TIMESTAMP, c1 INT) TAGS (tag1 INT) ROLLUP(MAX) MAX_DELAY 10s WATERMARK 18m",
      TSDB_CODE_PAR_INVALID_RANGE_OPTION);
}

TEST_F(ParserInitialCTest, createStream) {
  useDb("root", "test");

  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  auto setCreateStreamReqFunc =
      [&](const char* pStream, const char* pSrcDb, const char* pSql, const char* pDstStb = nullptr, int8_t igExists = 0,
          int8_t triggerType = STREAM_TRIGGER_AT_ONCE, int64_t maxDelay = 0, int64_t watermark = 0) {
        snprintf(expect.name, sizeof(expect.name), "0.%s", pStream);
        snprintf(expect.sourceDB, sizeof(expect.sourceDB), "0.%s", pSrcDb);
        if (NULL != pDstStb) {
          snprintf(expect.targetStbFullName, sizeof(expect.targetStbFullName), "0.test.%s", pDstStb);
        }
        expect.igExists = igExists;
        expect.sql = strdup(pSql);
        expect.triggerType = triggerType;
        expect.maxDelay = maxDelay;
        expect.watermark = watermark;
      };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
                tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(std::string(req.sourceDB), std::string(expect.sourceDB));
    ASSERT_EQ(std::string(req.targetStbFullName), std::string(expect.targetStbFullName));
    ASSERT_EQ(req.igExists, expect.igExists);
    ASSERT_EQ(std::string(req.sql), std::string(expect.sql));
    ASSERT_EQ(req.triggerType, expect.triggerType);
    ASSERT_EQ(req.maxDelay, expect.maxDelay);
    ASSERT_EQ(req.watermark, expect.watermark);
    tFreeSCMCreateStreamReq(&req);
  });

  setCreateStreamReqFunc("s1", "test", "create stream s1 as select * from t1");
  run("CREATE STREAM s1 AS SELECT * FROM t1");
  clearCreateStreamReq();

  setCreateStreamReqFunc("s1", "test", "create stream if not exists s1 as select * from t1", nullptr, 1);
  run("CREATE STREAM IF NOT EXISTS s1 AS SELECT * FROM t1");
  clearCreateStreamReq();

  setCreateStreamReqFunc("s1", "test", "create stream s1 into st1 as select * from t1", "st1");
  run("CREATE STREAM s1 INTO st1 AS SELECT * FROM t1");
  clearCreateStreamReq();

  setCreateStreamReqFunc(
      "s1", "test", "create stream if not exists s1 trigger max_delay 20s watermark 10s into st1 as select * from t1",
      "st1", 1, STREAM_TRIGGER_MAX_DELAY, 20 * MILLISECOND_PER_SECOND, 10 * MILLISECOND_PER_SECOND);
  run("CREATE STREAM IF NOT EXISTS s1 TRIGGER MAX_DELAY 20s WATERMARK 10s INTO st1 AS SELECT * FROM t1");
  clearCreateStreamReq();
}

TEST_F(ParserInitialCTest, createStreamSemanticCheck) {
  useDb("root", "test");

  run("CREATE STREAM s1 AS SELECT PERCENTILE(c1, 30) FROM t1", TSDB_CODE_PAR_STREAM_NOT_ALLOWED_FUNC);
}

TEST_F(ParserInitialCTest, createTable) {
  useDb("root", "test");

  run("CREATE TABLE t1(ts TIMESTAMP, c1 INT)");

  run("CREATE TABLE IF NOT EXISTS test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), "
      "c8 SMALLINT, c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, "
      "c13 NCHAR(30), c15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create table' SMA(c1, c2, c3)");

  run("CREATE TABLE IF NOT EXISTS test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), "
      "c8 SMALLINT, c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, "
      "c13 NCHAR(30), c14 VARCHAR(50)) "
      "TAGS (a1 TIMESTAMP, a2 INT, a3 INT UNSIGNED, a4 BIGINT, a5 BIGINT UNSIGNED, a6 FLOAT, a7 DOUBLE, a8 BINARY(20), "
      "a9 SMALLINT, a10 SMALLINT UNSIGNED COMMENT 'test column comment', a11 TINYINT, a12 TINYINT UNSIGNED, a13 BOOL, "
      "a14 NCHAR(30), a15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create table' SMA(c1, c2, c3) ROLLUP (MIN)");

  run("CREATE TABLE IF NOT EXISTS t1 USING st1 TAGS(1, 'wxy', NOW)");

  run("CREATE TABLE "
      "IF NOT EXISTS test.t1 USING test.st1 (tag1, tag2) TAGS(1, 'abc') "
      "IF NOT EXISTS test.t2 USING test.st1 (tag1, tag2) TAGS(2, 'abc') "
      "IF NOT EXISTS test.t3 USING test.st1 (tag1, tag2) TAGS(3, 'abc') ");

  // run("CREATE TABLE IF NOT EXISTS t1 USING st1 TAGS(1, 'wxy', NOW + 1S)");
}

TEST_F(ParserInitialCTest, createTopic) {
  useDb("root", "test");

  SCMCreateTopicReq expect = {0};

  auto clearCreateTopicReq = [&]() { memset(&expect, 0, sizeof(SCMCreateTopicReq)); };

  auto setCreateTopicReqFunc = [&](const char* pTopicName, int8_t igExists, const char* pSql, const char* pAst,
                                   const char* pDbName = nullptr, const char* pTbname = nullptr) {
    snprintf(expect.name, sizeof(expect.name), "0.%s", pTopicName);
    expect.igExists = igExists;
    expect.sql = (char*)pSql;
    if (nullptr != pTbname) {
      expect.subType = TOPIC_SUB_TYPE__TABLE;
      snprintf(expect.subStbName, sizeof(expect.subStbName), "0.%s.%s", pDbName, pTbname);
    } else if (nullptr != pAst) {
      expect.subType = TOPIC_SUB_TYPE__COLUMN;
      expect.ast = (char*)pAst;
    } else {
      expect.subType = TOPIC_SUB_TYPE__DB;
      snprintf(expect.subDbName, sizeof(expect.subDbName), "0.%s", pDbName);
    }
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_TOPIC_STMT);
    SCMCreateTopicReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
                tDeserializeSCMCreateTopicReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(req.igExists, expect.igExists);
    ASSERT_EQ(req.subType, expect.subType);
    ASSERT_EQ(std::string(req.sql), std::string(expect.sql));
    switch (expect.subType) {
      case TOPIC_SUB_TYPE__DB:
        ASSERT_EQ(std::string(req.subDbName), std::string(expect.subDbName));
        break;
      case TOPIC_SUB_TYPE__TABLE:
        ASSERT_EQ(std::string(req.subStbName), std::string(expect.subStbName));
        break;
      case TOPIC_SUB_TYPE__COLUMN:
        ASSERT_NE(req.ast, nullptr);
        break;
      default:
        ASSERT_TRUE(false);
    }
    tFreeSCMCreateTopicReq(&req);
  });

  setCreateTopicReqFunc("tp1", 0, "create topic tp1 as select * from t1", "ast");
  run("CREATE TOPIC tp1 AS SELECT * FROM t1");
  clearCreateTopicReq();

  setCreateTopicReqFunc("tp1", 1, "create topic if not exists tp1 as select ts, ceil(c1) from t1", "ast");
  run("CREATE TOPIC IF NOT EXISTS tp1 AS SELECT ts, CEIL(c1) FROM t1");
  clearCreateTopicReq();

  setCreateTopicReqFunc("tp1", 0, "create topic tp1 as database test", nullptr, "test");
  run("CREATE TOPIC tp1 AS DATABASE test");
  clearCreateTopicReq();

  setCreateTopicReqFunc("tp1", 1, "create topic if not exists tp1 as stable st1", nullptr, "test", "st1");
  run("CREATE TOPIC IF NOT EXISTS tp1 AS STABLE st1");
  clearCreateTopicReq();
}

TEST_F(ParserInitialCTest, createUser) {
  useDb("root", "test");

  run("CREATE USER wxy PASS '123456'");
}

}  // namespace ParserTest
