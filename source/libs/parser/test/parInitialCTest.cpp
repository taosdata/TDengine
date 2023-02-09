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

TEST_F(ParserInitialCTest, createAccount) {
  useDb("root", "test");

  run("CREATE ACCOUNT ac_wxy PASS '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT, PARSER_STAGE_PARSE);
}

/*
 * CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 *
 * database_options:
 *     database_option ...
 *
 * database_option: {
 *     BUFFER value
 *   | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
 *   | CACHESIZE value
 *   | COMP {0 | 1 | 2}
 *   | DURATION value
 *   | WAL_FSYNC_PERIOD value
 *   | MAXROWS value
 *   | MINROWS value
 *   | KEEP value
 *   | PAGES value
 *   | PAGESIZE  value
 *   | PRECISION {'ms' | 'us' | 'ns'}
 *   | REPLICA value
 *   | RETENTIONS ingestion_duration:keep_duration ...
 *   | STRICT {'off' | 'on'}  // not support
 *   | WAL_LEVEL value
 *   | VGROUPS value
 *   | SINGLE_STABLE {0 | 1}
 *   | WAL_RETENTION_PERIOD value
 *   | WAL_ROLL_PERIOD value
 *   | WAL_RETENTION_SIZE value
 *   | WAL_SEGMENT_SIZE value
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
    expect.cacheLast = TSDB_DEFAULT_CACHE_MODEL;
    expect.cacheLastSize = TSDB_DEFAULT_CACHE_SIZE;
    expect.compression = TSDB_DEFAULT_COMP_LEVEL;
    expect.daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
    expect.walFsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
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
    expect.walRetentionPeriod = TSDB_REP_DEF_DB_WAL_RET_PERIOD;
    expect.walRetentionSize = TSDB_REP_DEF_DB_WAL_RET_SIZE;
    expect.walRollPeriod = TSDB_REP_DEF_DB_WAL_ROLL_PERIOD;
    expect.walSegmentSize = TSDB_DEFAULT_DB_WAL_SEGMENT_SIZE;
    expect.sstTrigger = TSDB_DEFAULT_SST_TRIGGER;
    expect.hashPrefix = TSDB_DEFAULT_HASH_PREFIX;
    expect.hashSuffix = TSDB_DEFAULT_HASH_SUFFIX;
    expect.tsdbPageSize = TSDB_DEFAULT_TSDB_PAGESIZE;
  };

  auto setDbBufferFunc = [&](int32_t buffer) { expect.buffer = buffer; };
  auto setDbCachelastFunc = [&](int8_t cachelast) { expect.cacheLast = cachelast; };
  auto setDbCachelastSize = [&](int8_t cachelastSize) { expect.cacheLastSize = cachelastSize; };
  auto setDbCompressionFunc = [&](int8_t compressionLevel) { expect.compression = compressionLevel; };
  auto setDbDaysFunc = [&](int32_t daysPerFile) { expect.daysPerFile = daysPerFile; };
  auto setDbFsyncFunc = [&](int32_t fsyncPeriod) { expect.walFsyncPeriod = fsyncPeriod; };
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
  auto setDbWalRetentionPeriod = [&](int32_t walRetentionPeriod) { expect.walRetentionPeriod = walRetentionPeriod; };
  auto setDbWalRetentionSize = [&](int32_t walRetentionSize) { expect.walRetentionSize = walRetentionSize; };
  auto setDbWalRollPeriod = [&](int32_t walRollPeriod) { expect.walRollPeriod = walRollPeriod; };
  auto setDbWalSegmentSize = [&](int32_t walSegmentSize) { expect.walSegmentSize = walSegmentSize; };
  auto setDbSstTrigger = [&](int32_t sstTrigger) { expect.sstTrigger = sstTrigger; };
  auto setDbHashPrefix = [&](int32_t hashPrefix) { expect.hashPrefix = hashPrefix; };
  auto setDbHashSuffix = [&](int32_t hashSuffix) { expect.hashSuffix = hashSuffix; };
  auto setDbTsdbPageSize = [&](int32_t tsdbPageSize) { expect.tsdbPageSize = tsdbPageSize; };

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
    ASSERT_EQ(req.walFsyncPeriod, expect.walFsyncPeriod);
    ASSERT_EQ(req.walLevel, expect.walLevel);
    ASSERT_EQ(req.precision, expect.precision);
    ASSERT_EQ(req.compression, expect.compression);
    ASSERT_EQ(req.replications, expect.replications);
    ASSERT_EQ(req.strict, expect.strict);
    ASSERT_EQ(req.cacheLast, expect.cacheLast);
    ASSERT_EQ(req.cacheLastSize, expect.cacheLastSize);
    ASSERT_EQ(req.walRetentionPeriod, expect.walRetentionPeriod);
    ASSERT_EQ(req.walRetentionSize, expect.walRetentionSize);
    ASSERT_EQ(req.walRollPeriod, expect.walRollPeriod);
    ASSERT_EQ(req.walSegmentSize, expect.walSegmentSize);
    ASSERT_EQ(req.sstTrigger, expect.sstTrigger);
    ASSERT_EQ(req.hashPrefix, expect.hashPrefix);
    ASSERT_EQ(req.hashSuffix, expect.hashSuffix);
    ASSERT_EQ(req.tsdbPageSize, expect.tsdbPageSize);
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
  setDbCachelastSize(20);
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
  // setDbStrictaFunc(1);
  setDbWalLevelFunc(2);
  setDbVgroupsFunc(100);
  setDbSingleStableFunc(1);
  setDbSchemalessFunc(1);
  setDbWalRetentionPeriod(-1);
  setDbWalRetentionSize(-1);
  setDbWalRollPeriod(10);
  setDbWalSegmentSize(20);
  setDbSstTrigger(16);
  setDbHashPrefix(3);
  setDbHashSuffix(4);
  setDbTsdbPageSize(32);
  run("CREATE DATABASE IF NOT EXISTS wxy_db "
      "BUFFER 64 "
      "CACHEMODEL 'last_value' "
      "CACHESIZE 20 "
      "COMP 1 "
      "DURATION 100 "
      "WAL_FSYNC_PERIOD 100 "
      "MAXROWS 1000 "
      "MINROWS 100 "
      "KEEP 1440 "
      "PAGES 96 "
      "PAGESIZE 8 "
      "PRECISION 'ns' "
      "REPLICA 3 "
      "RETENTIONS 15s:7d,1m:21d,15m:500d "
      //      "STRICT 'on' "
      "WAL_LEVEL 2 "
      "VGROUPS 100 "
      "SINGLE_STABLE 1 "
      "SCHEMALESS 1 "
      "WAL_RETENTION_PERIOD -1 "
      "WAL_RETENTION_SIZE -1 "
      "WAL_ROLL_PERIOD 10 "
      "WAL_SEGMENT_SIZE 20 "
      "STT_TRIGGER 16 "
      "TABLE_PREFIX 3 "
      "TABLE_SUFFIX 4 "
      "TSDB_PAGESIZE 32");
  clearCreateDbReq();

  setCreateDbReqFunc("wxy_db", 1);
  setDbDaysFunc(100);
  setDbKeepFunc(1440, 300 * 60, 400 * 1440);
  run("CREATE DATABASE IF NOT EXISTS wxy_db "
      "DURATION 100m "
      "KEEP 1440m,300h,400d ");
  clearCreateDbReq();

  setCreateDbReqFunc("wxy_db", 1);
  setDbReplicaFunc(3);
  setDbWalRetentionPeriod(TSDB_REPS_DEF_DB_WAL_RET_PERIOD);
  setDbWalRetentionSize(TSDB_REPS_DEF_DB_WAL_RET_SIZE);
  setDbWalRollPeriod(TSDB_REPS_DEF_DB_WAL_ROLL_PERIOD);
  run("CREATE DATABASE IF NOT EXISTS wxy_db REPLICA 3");
  clearCreateDbReq();
}

TEST_F(ParserInitialCTest, createDatabaseSemanticCheck) {
  useDb("root", "test");

  run("create database db2 retentions 0s:1d", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("create database db2 retentions 10s:0d", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("create database db2 retentions 1w:1d", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("create database db2 retentions 1w:1n", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("create database db2 retentions 15s:7d,15m:21d,10m:500d", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("create database db2 retentions 15s:7d,5m:21d,10m:10d", TSDB_CODE_PAR_INVALID_DB_OPTION);
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

  SMCreateSmaReq expect = {0};

  auto setCreateSmacReq = [&](const char* pIndexName, const char* pStbName, int64_t interval, int8_t intervalUnit,
                              int64_t offset = 0, int64_t sliding = -1, int8_t slidingUnit = -1, int8_t igExists = 0) {
    memset(&expect, 0, sizeof(SMCreateSmaReq));
    strcpy(expect.name, pIndexName);
    strcpy(expect.stb, pStbName);
    expect.igExists = igExists;
    expect.intervalUnit = intervalUnit;
    expect.slidingUnit = slidingUnit < 0 ? intervalUnit : slidingUnit;
    expect.timezone = 0;
    expect.dstVgId = 1;
    expect.interval = interval;
    expect.offset = offset;
    expect.sliding = sliding < 0 ? interval : sliding;
    expect.maxDelay = -1;
    expect.watermark = TSDB_DEFAULT_ROLLUP_WATERMARK;
    expect.deleteMark = TSDB_DEFAULT_ROLLUP_DELETE_MARK;
  };

  auto setOptionsForCreateSmacReq = [&](int64_t maxDelay, int64_t watermark, int64_t deleteMark) {
    expect.maxDelay = maxDelay;
    expect.watermark = watermark;
    expect.deleteMark = deleteMark;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_INDEX_STMT);
    SMCreateSmaReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSMCreateSmaReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(std::string(req.stb), std::string(expect.stb));
    ASSERT_EQ(req.igExists, expect.igExists);
    ASSERT_EQ(req.intervalUnit, expect.intervalUnit);
    ASSERT_EQ(req.slidingUnit, expect.slidingUnit);
    ASSERT_EQ(req.timezone, expect.timezone);
    ASSERT_EQ(req.dstVgId, expect.dstVgId);
    ASSERT_EQ(req.interval, expect.interval);
    ASSERT_EQ(req.offset, expect.offset);
    ASSERT_EQ(req.sliding, expect.sliding);
    ASSERT_EQ(req.maxDelay, expect.maxDelay);
    ASSERT_EQ(req.watermark, expect.watermark);
    ASSERT_EQ(req.deleteMark, expect.deleteMark);
    ASSERT_GT(req.exprLen, 0);
    ASSERT_EQ(req.tagsFilterLen, 0);
    ASSERT_GT(req.sqlLen, 0);
    ASSERT_GT(req.astLen, 0);
    ASSERT_NE(req.expr, nullptr);
    ASSERT_EQ(req.tagsFilter, nullptr);
    ASSERT_NE(req.sql, nullptr);
    ASSERT_NE(req.ast, nullptr);
    tFreeSMCreateSmaReq(&req);
  });

  setCreateSmacReq("0.test.index1", "0.test.t1", 10 * MILLISECOND_PER_SECOND, 's');
  run("CREATE SMA INDEX index1 ON t1 FUNCTION(MAX(c1), MIN(c3 + 10), SUM(c4)) INTERVAL(10s)");

  setCreateSmacReq("0.test.index2", "0.test.st1", 5 * MILLISECOND_PER_SECOND, 's');
  setOptionsForCreateSmacReq(10 * MILLISECOND_PER_SECOND, 20 * MILLISECOND_PER_SECOND, 1000 * MILLISECOND_PER_SECOND);
  run("CREATE SMA INDEX index2 ON st1 FUNCTION(MAX(c1), MIN(tag1)) INTERVAL(5s) WATERMARK 20s MAX_DELAY 10s "
      "DELETE_MARK 1000s");
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

  auto setCreateStbReqFunc =
      [&](const char* pDbName, const char* pTbName, int8_t igExists = 0, int64_t delay1 = -1, int64_t delay2 = -1,
          int64_t watermark1 = TSDB_DEFAULT_ROLLUP_WATERMARK, int64_t watermark2 = TSDB_DEFAULT_ROLLUP_WATERMARK,
          int64_t deleteMark1 = TSDB_DEFAULT_ROLLUP_DELETE_MARK, int64_t deleteMark2 = TSDB_DEFAULT_ROLLUP_DELETE_MARK,
          int32_t ttl = TSDB_DEFAULT_TABLE_TTL, const char* pComment = nullptr) {
        int32_t len = snprintf(expect.name, sizeof(expect.name), "0.%s.%s", pDbName, pTbName);
        expect.name[len] = '\0';
        expect.igExists = igExists;
        expect.delay1 = delay1;
        expect.delay2 = delay2;
        expect.watermark1 = watermark1;
        expect.watermark2 = watermark2;
        expect.deleteMark1 = deleteMark1;
        expect.deleteMark2 = deleteMark2;
        // expect.ttl = ttl;
        if (nullptr != pComment) {
          expect.pComment = strdup(pComment);
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
      ASSERT_EQ(std::string(req.pComment), std::string(expect.pComment));
    }
    if (expect.ast1Len > 0) {
      ASSERT_EQ(std::string(req.pAst1), std::string(expect.pAst1));
    }
    if (expect.ast2Len > 0) {
      ASSERT_EQ(std::string(req.pAst2), std::string(expect.pAst2));
    }
    tFreeSMCreateStbReq(&req);
  });

  setCreateStbReqFunc("test", "t1");
  addFieldToCreateStbReqFunc(true, "ts", TSDB_DATA_TYPE_TIMESTAMP);
  addFieldToCreateStbReqFunc(true, "c1", TSDB_DATA_TYPE_INT);
  addFieldToCreateStbReqFunc(false, "id", TSDB_DATA_TYPE_INT);
  run("CREATE STABLE t1(ts TIMESTAMP, c1 INT) TAGS(id INT)");
  clearCreateStbReq();

  setCreateStbReqFunc("rollup_db", "t1", 1, 100 * MILLISECOND_PER_SECOND, 10 * MILLISECOND_PER_MINUTE, 10,
                      1 * MILLISECOND_PER_MINUTE, 1000 * MILLISECOND_PER_SECOND, 200 * MILLISECOND_PER_MINUTE, 100,
                      "test create table");
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
  run("CREATE STABLE IF NOT EXISTS rollup_db.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), "
      "c8 SMALLINT, c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, "
      "c13 NCHAR(30), c14 VARCHAR(50)) "
      "TAGS (a1 TIMESTAMP, a2 INT, a3 INT UNSIGNED, a4 BIGINT, a5 BIGINT UNSIGNED, a6 FLOAT, a7 DOUBLE, "
      "a8 BINARY(20), a9 SMALLINT, a10 SMALLINT UNSIGNED COMMENT 'test column comment', a11 TINYINT, "
      "a12 TINYINT UNSIGNED, a13 BOOL, a14 NCHAR(30), a15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create table' SMA(c1, c2, c3) ROLLUP (MIN) MAX_DELAY 100s,10m WATERMARK 10a,1m "
      "DELETE_MARK 1000s,200m");
  clearCreateStbReq();
}

TEST_F(ParserInitialCTest, createStableSemanticCheck) {
  useDb("root", "test");

  run("CREATE STABLE rollup_db.stb2 (ts TIMESTAMP, c1 INT) TAGS (tag1 INT) ROLLUP(CEIL)",
      TSDB_CODE_PAR_INVALID_TABLE_OPTION);

  run("CREATE STABLE rollup_db.stb2 (ts TIMESTAMP, c1 INT) TAGS (tag1 INT) ROLLUP(MAX) MAX_DELAY 0s WATERMARK 1m",
      TSDB_CODE_PAR_INVALID_TABLE_OPTION);

  run("CREATE STABLE rollup_db.stb2 (ts TIMESTAMP, c1 INT) TAGS (tag1 INT) ROLLUP(MAX) MAX_DELAY 10s WATERMARK 18m",
      TSDB_CODE_PAR_INVALID_TABLE_OPTION);
}

TEST_F(ParserInitialCTest, createStream) {
  useDb("root", "test");

  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  auto setCreateStreamReq = [&](const char* pStream, const char* pSrcDb, const char* pSql, const char* pDstStb,
                                int8_t igExists = 0, int8_t triggerType = STREAM_TRIGGER_AT_ONCE, int64_t maxDelay = 0,
                                int64_t watermark = 0, int8_t igExpired = STREAM_DEFAULT_IGNORE_EXPIRED,
                                int8_t fillHistory = STREAM_DEFAULT_FILL_HISTORY,
                                int8_t igUpdate = STREAM_DEFAULT_IGNORE_UPDATE) {
    snprintf(expect.name, sizeof(expect.name), "0.%s", pStream);
    snprintf(expect.sourceDB, sizeof(expect.sourceDB), "0.%s", pSrcDb);
    snprintf(expect.targetStbFullName, sizeof(expect.targetStbFullName), "0.test.%s", pDstStb);
    expect.igExists = igExists;
    expect.sql = strdup(pSql);
    expect.triggerType = triggerType;
    expect.maxDelay = maxDelay;
    expect.watermark = watermark;
    expect.fillHistory = fillHistory;
    expect.igExpired = igExpired;
    expect.igUpdate = igUpdate;
  };

  auto addTag = [&](const char* pFieldName, uint8_t type, int32_t bytes = 0) {
    SField field = {0};
    strcpy(field.name, pFieldName);
    field.type = type;
    field.bytes = bytes > 0 ? bytes : tDataTypes[type].bytes;
    field.flags |= COL_SMA_ON;

    if (NULL == expect.pTags) {
      expect.pTags = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SField));
    }
    taosArrayPush(expect.pTags, &field);
    expect.numOfTags += 1;
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
    ASSERT_EQ(req.fillHistory, expect.fillHistory);
    ASSERT_EQ(req.igExpired, expect.igExpired);
    ASSERT_EQ(req.numOfTags, expect.numOfTags);
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
    ASSERT_EQ(req.igUpdate, expect.igUpdate);
    tFreeSCMCreateStreamReq(&req);
  });

  setCreateStreamReq("s1", "test", "create stream s1 into st1 as select count(*) from t1 interval(10s)", "st1");
  run("CREATE STREAM s1 INTO st1 AS SELECT COUNT(*) FROM t1 INTERVAL(10S)");
  clearCreateStreamReq();

  setCreateStreamReq(
      "s1", "test",
      "create stream if not exists s1 trigger max_delay 20s watermark 10s ignore expired 0 fill_history 1 ignore "
      "update 1 into st1 as select count(*) from t1 interval(10s)",
      "st1", 1, STREAM_TRIGGER_MAX_DELAY, 20 * MILLISECOND_PER_SECOND, 10 * MILLISECOND_PER_SECOND, 0, 1, 1);
  run("CREATE STREAM IF NOT EXISTS s1 TRIGGER MAX_DELAY 20s WATERMARK 10s IGNORE EXPIRED 0 FILL_HISTORY 1 IGNORE "
      "UPDATE 1 INTO st1 AS SELECT COUNT(*) FROM t1 INTERVAL(10S)");
  clearCreateStreamReq();

  setCreateStreamReq("s1", "test",
                     "create stream s1 into st3 tags(tname varchar(10), id int) subtable(concat('new-', tname)) as "
                     "select _wstart wstart, count(*) cnt from st1 partition by tbname tname, tag1 id interval(10s)",
                     "st3");
  addTag("tname", TSDB_DATA_TYPE_VARCHAR, 10 + VARSTR_HEADER_SIZE);
  addTag("id", TSDB_DATA_TYPE_INT);
  run("CREATE STREAM s1 INTO st3 TAGS(tname VARCHAR(10), id INT) SUBTABLE(CONCAT('new-', tname)) "
      "AS SELECT _WSTART wstart, COUNT(*) cnt FROM st1 PARTITION BY TBNAME tname, tag1 id INTERVAL(10S)");
  clearCreateStreamReq();
}

TEST_F(ParserInitialCTest, createStreamSemanticCheck) {
  useDb("root", "test");

  run("CREATE STREAM s1 INTO st1 AS SELECT PERCENTILE(c1, 30) FROM t1 INTERVAL(10S)",
      TSDB_CODE_PAR_STREAM_NOT_ALLOWED_FUNC);
}

TEST_F(ParserInitialCTest, createTable) {
  useDb("root", "test");

  run("CREATE TABLE t1(ts TIMESTAMP, c1 INT)");

  run("CREATE TABLE IF NOT EXISTS test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), "
      "c8 SMALLINT, c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, "
      "c13 NCHAR(30), c15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create table' SMA(c1, c2, c3)");

  run("CREATE TABLE IF NOT EXISTS rollup_db.t1("
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

TEST_F(ParserInitialCTest, createTableSemanticCheck) {
  useDb("root", "test");

  string sql = "CREATE TABLE st1(ts TIMESTAMP, ";
  for (int32_t i = 1; i < 4096; ++i) {
    if (i > 1) {
      sql.append(", ");
    }
    sql.append("c" + to_string(i) + " INT");
  }
  sql.append(") TAGS (t1 int)");

  run(sql, TSDB_CODE_PAR_TOO_MANY_COLUMNS);
}

TEST_F(ParserInitialCTest, createTopic) {
  useDb("root", "test");

  SCMCreateTopicReq expect = {0};

  auto clearCreateTopicReq = [&]() { memset(&expect, 0, sizeof(SCMCreateTopicReq)); };

  auto setCreateTopicReqFunc = [&](const char* pTopicName, int8_t igExists, const char* pSql, const char* pAst,
                                   const char* pDbName = nullptr, const char* pTbname = nullptr, int8_t withMeta = 0) {
    snprintf(expect.name, sizeof(expect.name), "0.%s", pTopicName);
    expect.igExists = igExists;
    expect.sql = (char*)pSql;
    expect.withMeta = withMeta;
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
    ASSERT_EQ(req.withMeta, expect.withMeta);
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

  setCreateTopicReqFunc("tp1", 0, "create topic tp1 with meta as database test", nullptr, "test", nullptr, 1);
  run("CREATE TOPIC tp1 WITH META AS DATABASE test");
  clearCreateTopicReq();

  setCreateTopicReqFunc("tp1", 1, "create topic if not exists tp1 as stable st1", nullptr, "test", "st1");
  run("CREATE TOPIC IF NOT EXISTS tp1 AS STABLE st1");
  clearCreateTopicReq();

  setCreateTopicReqFunc("tp1", 1, "create topic if not exists tp1 with meta as stable st1", nullptr, "test", "st1", 1);
  run("CREATE TOPIC IF NOT EXISTS tp1 WITH META AS STABLE st1");
  clearCreateTopicReq();
}

TEST_F(ParserInitialCTest, createUser) {
  useDb("root", "test");

  SCreateUserReq expect = {0};

  auto clearCreateUserReq = [&]() { memset(&expect, 0, sizeof(SCreateUserReq)); };

  auto setCreateUserReq = [&](const char* pUser, const char* pPass, int8_t sysInfo = 1) {
    strcpy(expect.user, pUser);
    strcpy(expect.pass, pPass);
    expect.createType = 0;
    expect.superUser = 0;
    expect.sysInfo = sysInfo;
    expect.enable = 1;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_USER_STMT);
    SCreateUserReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSCreateUserReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(req.createType, expect.createType);
    ASSERT_EQ(req.superUser, expect.superUser);
    ASSERT_EQ(req.sysInfo, expect.sysInfo);
    ASSERT_EQ(req.enable, expect.enable);
    ASSERT_EQ(std::string(req.user), std::string(expect.user));
    ASSERT_EQ(std::string(req.pass), std::string(expect.pass));
  });

  setCreateUserReq("wxy", "123456");
  run("CREATE USER wxy PASS '123456'");
  clearCreateUserReq();

  setCreateUserReq("wxy1", "a123456", 1);
  run("CREATE USER wxy1 PASS 'a123456' SYSINFO 1");
  clearCreateUserReq();
}

}  // namespace ParserTest
