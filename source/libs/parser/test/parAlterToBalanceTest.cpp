/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <array>
#include <memory>

#include "mockCatalog.h"  // mockChkColRefByPrivCode: drive the stubbed RESTRICT verdict
#include "parInt.h"
#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserInitialATest : public ParserDdlTest {};

namespace {

// g_chkColRefByPrivCode (mockCatalog.cpp) is process-global and the harness has no SetUp/TearDown
// that resets it. A run() mismatch throws (see DO_WITH_THROW), which unwinds the whole test body,
// so an inline "reset to SUCCESS" after the last assertion would be skipped and the stale RESTRICT
// verdict would leak into every later RENAME COLUMN/TAG case in this binary. Restoring from a
// destructor makes the reset happen on every exit path, including the throwing one.
struct ScopedChkColRefByPrivCode {
  explicit ScopedChkColRefByPrivCode(int32_t code) { mockChkColRefByPrivCode(code); }
  ~ScopedChkColRefByPrivCode() { mockChkColRefByPrivCode(TSDB_CODE_SUCCESS); }

  ScopedChkColRefByPrivCode(const ScopedChkColRefByPrivCode&) = delete;
  ScopedChkColRefByPrivCode& operator=(const ScopedChkColRefByPrivCode&) = delete;
  ScopedChkColRefByPrivCode(ScopedChkColRefByPrivCode&&) = delete;
  ScopedChkColRefByPrivCode& operator=(ScopedChkColRefByPrivCode&&) = delete;
};

}  // namespace

/*
 * ALTER ACCOUNT account_name alter_account_options
 *
 * alter_account_options:
 *     alter_account_option ...
 *
 * alter_account_option: {
 *     PASS value
 *   | PPS value
 *   | TSERIES value
 *   | STORAGE value
 *   | STREAMS value
 *   | QTIME value
 *   | DBS value
 *   | USERS value
 *   | CONNS value
 *   | STATE value
 * }
 */
TEST_F(ParserInitialATest, alterAccount) {
  useDb("root", "test");

  run("ALTER ACCOUNT ac_wxy PASS '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT, PARSER_STAGE_PARSE);
}

/*
 * ALTER DNODE dnode_id 'config' ['value']
 * ALTER ALL DNODES 'config' ['value']
 */
TEST_F(ParserInitialATest, alterDnodeTimezoneValidatedByServer) {
  // Error-code-only responses and logs must not expose format placeholders.
  EXPECT_STREQ(tstrerror(TSDB_CODE_PAR_INVALID_TIMEZONE), "Invalid timezone");
  struct TestCase {
    const char* sql;
    int32_t dnodeId;
    const char* config;
    const char* value;
  };
  const array<TestCase, 8> cases = {{
      {"ALTER ALL DNODES 'timezone' 'UTF-8'", -1, "timezone", "UTF-8"},
      {"ALTER ALL DNODES 'timezone UTF-8'", -1, "timezone UTF-8", ""},
      {"ALTER DNODE 1 'timezone' 'UTF-8'", 1, "timezone", "UTF-8"},
      {"ALTER ALL DNODES 'TIMEZONE' 'UTF-8'", -1, "TIMEZONE", "UTF-8"},
      {"ALTER ALL DNODES 'timezone' 'Europe/Kyiv'", -1,
       "timezone", "Europe/Kyiv"},
      {"ALTER ALL DNODES 'timezone Europe/Kyiv'", -1,
       "timezone Europe/Kyiv", ""},
      {"ALTER ALL DNODES 'timezone' 'America/Nuuk'", -1,
       "timezone", "America/Nuuk"},
      {"ALTER ALL DNODES 'timezone America/Nuuk'", -1,
       "timezone America/Nuuk", ""},
  }};
  for (const auto& testCase : cases) {
    const char* sql = testCase.sql;
    array<char, 1024> message = {0};
    SParseContext context = {0};
    context.acctId = 0;
    context.db = "test";
    context.pUser = "root";
    context.pSql = sql;
    context.sqlLen = strlen(sql);
    context.pMsg = message.data();
    context.msgLen = message.size();
    SQuery* query = nullptr;
    ASSERT_EQ(parse(&context, &query), TSDB_CODE_SUCCESS) << sql;
    unique_ptr<SQuery, decltype(&qDestroyQuery)> guard(query, qDestroyQuery);
    ASSERT_NE(query, nullptr);
    ASSERT_EQ(translate(&context, query, nullptr), TSDB_CODE_SUCCESS) << sql;
    ASSERT_NE(query->pCmdMsg, nullptr) << sql;
    EXPECT_EQ(query->pCmdMsg->msgType, TDMT_MND_CONFIG_DNODE) << sql;
    SMCfgDnodeReq req = {0};
    ASSERT_EQ(tDeserializeSMCfgDnodeReq(query->pCmdMsg->pMsg,
                                        query->pCmdMsg->msgLen, &req),
              TSDB_CODE_SUCCESS) << sql;
    EXPECT_EQ(req.dnodeId, testCase.dnodeId) << sql;
    EXPECT_STREQ(req.config, testCase.config) << sql;
    EXPECT_STREQ(req.value, testCase.value) << sql;
    tFreeSMCfgDnodeReq(&req);
  }
}

TEST_F(ParserInitialATest, alterDnode) {
  useDb("root", "test");

  SMCfgDnodeReq expect = {0};

  auto clearCfgDnodeReq = [&]() { memset(&expect, 0, sizeof(SMCfgDnodeReq)); };

  auto setCfgDnodeReq = [&](int32_t dnodeId, const char* pConfig, const char* pValue = nullptr) {
    expect.dnodeId = dnodeId;
    strcpy(expect.config, pConfig);
    if (nullptr != pValue) {
      strcpy(expect.value, pValue);
    }
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_DNODE_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_CONFIG_DNODE);
    SMCfgDnodeReq req = {0};
    ASSERT_EQ(tDeserializeSMCfgDnodeReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(req.dnodeId, expect.dnodeId);
    ASSERT_EQ(std::string(req.config), std::string(expect.config));
    ASSERT_EQ(std::string(req.value), std::string(expect.value));
    tFreeSMCfgDnodeReq(&req);
  });

  setCfgDnodeReq(1, "resetLog");
  run("ALTER DNODE 1 'resetLog'");
  clearCfgDnodeReq();

  setCfgDnodeReq(2, "debugFlag", "134");
  run("ALTER DNODE 2 'debugFlag' '134'");
  clearCfgDnodeReq();

  setCfgDnodeReq(-1, "resetQueryCache");
  run("ALTER ALL DNODES 'resetQueryCache'");
  clearCfgDnodeReq();

  setCfgDnodeReq(-1, "qDebugflag", "135");
  run("ALTER ALL DNODES 'qDebugflag' '135'");
  clearCfgDnodeReq();

  setCfgDnodeReq(-1, "timezone", "UTC");
  run("ALTER ALL DNODES 'timezone' 'UTC'");
  clearCfgDnodeReq();

  setCfgDnodeReq(-1, "timezone UTC");
  run("ALTER ALL DNODES 'timezone UTC'");
  clearCfgDnodeReq();
}

/*
 * ALTER DATABASE db_name [alter_database_options]
 *
 * alter_database_options:
 *     alter_database_option ...
 *
 * alter_database_option: {
 *     BUFFER int_value                                          -- range [3, 16384], default 96, unit MB
 *   | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}  -- default 'none'
 *   | CACHESIZE int_value                                       -- range [1, 65536], default 1, unit MB
 *   | WAL_FSYNC_PERIOD int_value                                -- rang [0, 180000], default 3000, unit ms
 *   | KEEP {int_value | duration_value}                         -- rang [1, 365000], default 3650, unit day
 *   | PAGES int_value                                           -- rang [64, INT32_MAX], default 256, unit page
 *   | REPLICA int_value                                         -- todo: enum 1, 3, default 1, unit replica
 *   | WAL_LEVEL int_value                                       -- enum 1, 2, default 1
 *   | STT_TRIGGER int_value                                     -- rang [1, 16], default 8
 *   | MINROWS int_value                                         -- rang [10, 1000], default 100
 *   | WAL_RETENTION_PERIOD int_value                            -- rang [-1, INT32_MAX], default 0
 *   | WAL_RETENTION_SIZE int_value                              -- rang [-1, INT32_MAX], default 0
 */
TEST_F(ParserInitialATest, alterDatabase) {
  useDb("root", "test");

  SAlterDbReq expect = {0};

  auto clearAlterDbReq = [&]() { memset(&expect, 0, sizeof(SAlterDbReq)); };

  auto initAlterDb = [&](const char* pDb) {
    snprintf(expect.db, sizeof(expect.db), "0.%s", pDb);
    expect.buffer = -1;
    expect.pageSize = -1;
    expect.pages = -1;
    expect.daysPerFile = -1;
    expect.daysToKeep0 = -1;
    expect.daysToKeep1 = -1;
    expect.daysToKeep2 = -1;
    expect.walFsyncPeriod = -1;
    expect.walLevel = -1;
    expect.strict = -1;
    expect.cacheLast = -1;
    expect.cacheLastSize = -1;
    expect.replications = -1;
    expect.sstTrigger = -1;
    expect.minRows = -1;
    expect.walRetentionPeriod = -2;
    expect.walRetentionSize = -2;
  };
  auto setAlterDbBuffer = [&](int32_t buffer) { expect.buffer = buffer; };
  auto setAlterDbPageSize = [&](int32_t pageSize) { expect.pageSize = pageSize; };
  auto setAlterDbPages = [&](int32_t pages) { expect.pages = pages; };
  auto setAlterDbCacheSize = [&](int32_t cacheSize) { expect.cacheLastSize = cacheSize; };
  auto setAlterDbDuration = [&](int32_t duration) { expect.daysPerFile = duration; };
  auto setAlterDbKeep = [&](int32_t daysToKeep0, int32_t daysToKeep1 = -1, int32_t daysToKeep2 = -1) {
    expect.daysToKeep0 = daysToKeep0;
    expect.daysToKeep1 = (-1 == daysToKeep1 ? expect.daysToKeep0 : daysToKeep1);
    expect.daysToKeep2 = (-1 == daysToKeep1 ? expect.daysToKeep1 : daysToKeep2);
  };
  auto setAlterDbFsync = [&](int32_t fsync) { expect.walFsyncPeriod = fsync; };
  auto setAlterDbWal = [&](int8_t wal) { expect.walLevel = wal; };
  auto setAlterDbStrict = [&](int8_t strict) { expect.strict = strict; };
  auto setAlterDbCacheModel = [&](int8_t cacheModel) { expect.cacheLast = cacheModel; };
  auto setAlterDbReplica = [&](int8_t replications) { expect.replications = replications; };
  auto setAlterDbSttTrigger = [&](int8_t sstTrigger) { expect.sstTrigger = sstTrigger; };
  auto setAlterDbMinRows = [&](int32_t minRows) { expect.minRows = minRows; };
  auto setAlterDbWalRetentionPeriod = [&](int32_t walRetentionPeriod) {
    expect.walRetentionPeriod = walRetentionPeriod;
  };
  auto setAlterDbWalRetentionSize = [&](int32_t walRetentionSize) { expect.walRetentionSize = walRetentionSize; };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_DATABASE_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_ALTER_DB);
    SAlterDbReq req = {0};
    ASSERT_EQ(tDeserializeSAlterDbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(std::string(req.db), std::string(expect.db));
    ASSERT_EQ(req.buffer, expect.buffer);
    ASSERT_EQ(req.pageSize, expect.pageSize);
    ASSERT_EQ(req.pages, expect.pages);
    ASSERT_EQ(req.cacheLastSize, expect.cacheLastSize);
    ASSERT_EQ(req.daysToKeep0, expect.daysToKeep0);
    ASSERT_EQ(req.daysToKeep1, expect.daysToKeep1);
    ASSERT_EQ(req.daysToKeep2, expect.daysToKeep2);
    ASSERT_EQ(req.walFsyncPeriod, expect.walFsyncPeriod);
    ASSERT_EQ(req.walLevel, expect.walLevel);
    ASSERT_EQ(req.strict, expect.strict);
    ASSERT_EQ(req.cacheLast, expect.cacheLast);
    ASSERT_EQ(req.replications, expect.replications);
    ASSERT_EQ(req.sstTrigger, expect.sstTrigger);
    ASSERT_EQ(req.minRows, expect.minRows);
    ASSERT_EQ(req.walRetentionPeriod, expect.walRetentionPeriod);
    ASSERT_EQ(req.walRetentionSize, expect.walRetentionSize);
    tFreeSAlterDbReq(&req);
  });

  const int32_t MINUTE_PER_DAY = MILLISECOND_PER_DAY / MILLISECOND_PER_MINUTE;
  const int32_t MINUTE_PER_HOUR = MILLISECOND_PER_HOUR / MILLISECOND_PER_MINUTE;

  initAlterDb("test");
  setAlterDbCacheSize(32);
  setAlterDbKeep(10 * MINUTE_PER_DAY);
  setAlterDbFsync(200);
  setAlterDbWal(1);
  setAlterDbCacheModel(TSDB_CACHE_MODEL_LAST_ROW);
  setAlterDbSttTrigger(16);
  setAlterDbBuffer(16);
  setAlterDbPages(128);
  setAlterDbReplica(3);
  setAlterDbWalRetentionPeriod(10);
  setAlterDbWalRetentionSize(20);
  run("ALTER DATABASE test BUFFER 16 CACHEMODEL 'last_row' CACHESIZE 32 WAL_FSYNC_PERIOD 200 KEEP 10 PAGES 128 "
      "REPLICA 3 WAL_LEVEL 1 "
      "STT_TRIGGER 16 "
      "WAL_RETENTION_PERIOD 10 WAL_RETENTION_SIZE 20");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbBuffer(3);
  run("ALTER DATABASE test BUFFER 3");
  setAlterDbBuffer(64);
  run("ALTER DATABASE test BUFFER 64");
  setAlterDbBuffer(16384);
  run("ALTER DATABASE test BUFFER 16384");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbCacheModel(TSDB_CACHE_MODEL_NONE);
  run("ALTER DATABASE test CACHEMODEL 'none'");
  setAlterDbCacheModel(TSDB_CACHE_MODEL_LAST_ROW);
  run("ALTER DATABASE test CACHEMODEL 'last_row'");
  setAlterDbCacheModel(TSDB_CACHE_MODEL_LAST_VALUE);
  run("ALTER DATABASE test CACHEMODEL 'last_value'");
  setAlterDbCacheModel(TSDB_CACHE_MODEL_BOTH);
  run("ALTER DATABASE test CACHEMODEL 'both'");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbCacheSize(1);
  run("ALTER DATABASE test CACHESIZE 1");
  setAlterDbCacheSize(64);
  run("ALTER DATABASE test CACHESIZE 64");
  setAlterDbCacheSize(65536);
  run("ALTER DATABASE test CACHESIZE 65536");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbFsync(0);
  run("ALTER DATABASE test WAL_FSYNC_PERIOD 0");
  setAlterDbFsync(1000);
  run("ALTER DATABASE test WAL_FSYNC_PERIOD 1000");
  setAlterDbFsync(180000);
  run("ALTER DATABASE test WAL_FSYNC_PERIOD 180000");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbKeep(1 * MINUTE_PER_DAY);
  run("ALTER DATABASE test KEEP 1");
  setAlterDbKeep(30 * MINUTE_PER_DAY);
  run("ALTER DATABASE test KEEP 30");
  setAlterDbKeep(365000 * MINUTE_PER_DAY);
  run("ALTER DATABASE test KEEP 365000");
  setAlterDbKeep(1440);
  run("ALTER DATABASE test KEEP 1440m");
  setAlterDbKeep(14400);
  run("ALTER DATABASE test KEEP 14400m");
  setAlterDbKeep(525600000);
  run("ALTER DATABASE test KEEP 525600000m");
  setAlterDbKeep(5 * MINUTE_PER_DAY, 35 * MINUTE_PER_DAY, 500 * MINUTE_PER_DAY);
  run("ALTER DATABASE test KEEP 5,35,500");
  setAlterDbKeep(14400, 2400 * MINUTE_PER_HOUR, 1500 * MINUTE_PER_DAY);
  run("ALTER DATABASE test KEEP 14400m,2400h,1500d");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbPages(64);
  run("ALTER DATABASE test PAGES 64");
  setAlterDbPages(1024);
  run("ALTER DATABASE test PAGES 1024");
  setAlterDbPages(16384);
  run("ALTER DATABASE test PAGES 16384");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbWal(1);
  run("ALTER DATABASE test WAL_LEVEL 1");
  setAlterDbWal(2);
  run("ALTER DATABASE test WAL_LEVEL 2");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbReplica(1);
  run("ALTER DATABASE test REPLICA 1");
  setAlterDbReplica(3);
  run("ALTER DATABASE test REPLICA 3");
  clearAlterDbReq();

#ifdef _STORAGE
  initAlterDb("test");
  setAlterDbSttTrigger(1);
  run("ALTER DATABASE test STT_TRIGGER 1");
  setAlterDbSttTrigger(4);
  run("ALTER DATABASE test STT_TRIGGER 4");
  setAlterDbSttTrigger(16);
  run("ALTER DATABASE test STT_TRIGGER 16");
  clearAlterDbReq();
#endif

  initAlterDb("test");
  setAlterDbMinRows(10);
  run("ALTER DATABASE test MINROWS 10");
  setAlterDbMinRows(50);
  run("ALTER DATABASE test MINROWS 50");
  setAlterDbMinRows(1000);
  run("ALTER DATABASE test MINROWS 1000");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbWalRetentionPeriod(-1);
  run("ALTER DATABASE test WAL_RETENTION_PERIOD -1");
  setAlterDbWalRetentionPeriod(50);
  run("ALTER DATABASE test WAL_RETENTION_PERIOD 50");
  clearAlterDbReq();

  initAlterDb("test");
  setAlterDbWalRetentionSize(-1);
  run("ALTER DATABASE test WAL_RETENTION_SIZE -1");
  setAlterDbWalRetentionSize(50);
  run("ALTER DATABASE test WAL_RETENTION_SIZE 50");
  clearAlterDbReq();
}

TEST_F(ParserInitialATest, alterDatabaseSemanticCheck) {
  useDb("root", "test");

  run("ALTER DATABASE test BUFFER 2", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test BUFFER 16385", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test CACHEMODEL 'other'", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test CACHESIZE 0", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test CACHESIZE 65537", TSDB_CODE_PAR_INVALID_DB_OPTION);
  // The syntax limits it to only positive numbers
  run("ALTER DATABASE test WAL_FSYNC_PERIOD -1", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("ALTER DATABASE test WAL_FSYNC_PERIOD 180001", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test KEEP 0", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test KEEP 365001", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test KEEP 1000000000s", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test KEEP 1w", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test PAGES 63", TSDB_CODE_PAR_INVALID_DB_OPTION);
  // run("ALTER DATABASE test WAL_LEVEL 0", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test WAL_LEVEL 3", TSDB_CODE_PAR_INVALID_DB_OPTION);
  // run("ALTER DATABASE test REPLICA 2", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test STT_TRIGGER 0", TSDB_CODE_PAR_INVALID_DB_OPTION);
  run("ALTER DATABASE test STT_TRIGGER 17", TSDB_CODE_PAR_INVALID_DB_OPTION);
  // Regardless of the specific sentence
  run("ALTER DATABASE db WAL_LEVEL 0     # td-14436", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

/*
 * ALTER LOCAL 'config' ['value']
 */
TEST_F(ParserInitialATest, alterLocal) {
  useDb("root", "test");

  pair<string, string> expect;

  auto clearAlterLocal = [&]() {
    expect.first.clear();
    expect.second.clear();
  };

  auto setAlterLocal = [&](const char* pConfig, const char* pValue = nullptr) {
    (void)expect.first.assign(pConfig);
    if (nullptr != pValue) {
      (void)expect.second.assign(pValue);
    }
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_LOCAL_STMT);
    ASSERT_EQ(pQuery->execMode, QUERY_EXEC_MODE_LOCAL);
    SAlterLocalStmt* pStmt = (SAlterLocalStmt*)pQuery->pRoot;
    ASSERT_EQ(string(pStmt->config), expect.first);
    ASSERT_EQ(string(pStmt->value), expect.second);
  });

  setAlterLocal("resetlog");
  run("ALTER LOCAL 'resetlog'");
  clearAlterLocal();

  setAlterLocal("querypolicy", "2");
  run("ALTER LOCAL 'querypolicy' '2'");
  clearAlterLocal();
}

/*
 * ALTER STABLE [db_name.]tb_name alter_table_clause
 *
 * alter_table_clause: {
 *     alter_table_options
 *   | ADD COLUMN col_name column_type
 *   | DROP COLUMN col_name
 *   | MODIFY COLUMN col_name column_type
 *   | RENAME COLUMN old_col_name new_col_name  -- only normal table
 *   | ADD TAG tag_name tag_type                -- only super table
 *   | DROP TAG tag_name                        -- only super table
 *   | MODIFY TAG tag_name tag_type             -- only super table
 *   | RENAME TAG old_tag_name new_tag_name     -- only super table
 *   | SET TAG tag_name = new_tag_value         -- only child table
 * }
 *
 * alter_table_options:
 *     alter_table_option ...
 *
 * alter_table_option: {
 *    TTL int_value                             -- only child/normal table
 *  | COMMENT 'string_value'
 * }
 */
TEST_F(ParserInitialATest, alterSTable) {
  useDb("root", "test");

  SMAlterStbReq expect = {0};

  auto clearAlterStbReq = [&]() {
    tFreeSMAltertbReq(&expect);
    memset(&expect, 0, sizeof(SMAlterStbReq));
  };

  auto setAlterStbReq = [&](const char* pTbname, int8_t alterType, int32_t numOfFields = 0,
                            const char* pField1Name = nullptr, int8_t field1Type = 0, int32_t field1Bytes = 0,
                            const char* pField2Name = nullptr, const char* pComment = nullptr) {
    int32_t len = snprintf(expect.name, sizeof(expect.name), "0.test.%s", pTbname);
    expect.name[len] = '\0';
    expect.alterType = alterType;
    if (nullptr != pComment) {
      expect.comment = taosStrdup(pComment);
      expect.commentLen = strlen(pComment);
    }

    expect.numOfFields = numOfFields;
    if (NULL == expect.pFields) {
      expect.pFields = taosArrayInit(2, sizeof(TAOS_FIELD));
      ASSERT_TRUE(expect.pFields);
      TAOS_FIELD field = {0};
      ASSERT_TRUE(nullptr != taosArrayPush(expect.pFields, &field));
      ASSERT_TRUE(nullptr != taosArrayPush(expect.pFields, &field));
    }

    TAOS_FIELD* pField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 0);
    if (NULL != pField1Name) {
      strcpy(pField->name, pField1Name);
      pField->name[strlen(pField1Name)] = '\0';
    } else {
      memset(pField, 0, sizeof(TAOS_FIELD));
    }
    pField->type = field1Type;
    pField->bytes = field1Bytes > 0 ? field1Bytes : (field1Type > 0 ? tDataTypes[field1Type].bytes : 0);

    pField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 1);
    if (NULL != pField2Name) {
      strcpy(pField->name, pField2Name);
      pField->name[strlen(pField2Name)] = '\0';
    } else {
      memset(pField, 0, sizeof(TAOS_FIELD));
    }
    pField->type = 0;
    pField->bytes = 0;
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_SUPER_TABLE_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_ALTER_STB);
    SMAlterStbReq req = {0};
    ASSERT_EQ(tDeserializeSMAlterStbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(req.alterType, expect.alterType);
    ASSERT_EQ(req.numOfFields, expect.numOfFields);
    if (expect.numOfFields > 0) {
      TAOS_FIELD* pField = (TAOS_FIELD*)taosArrayGet(req.pFields, 0);
      TAOS_FIELD* pExpectField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 0);
      ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
      ASSERT_EQ(pField->type, pExpectField->type);
      ASSERT_EQ(pField->bytes, pExpectField->bytes);
    }
    if (expect.numOfFields > 1) {
      TAOS_FIELD* pField = (TAOS_FIELD*)taosArrayGet(req.pFields, 1);
      TAOS_FIELD* pExpectField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 1);
      ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
      ASSERT_EQ(pField->type, pExpectField->type);
      ASSERT_EQ(pField->bytes, pExpectField->bytes);
    }
    tFreeSMAltertbReq(&req);
  });

  setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_OPTIONS, 0, nullptr, 0, 0, nullptr, "test");
  run("ALTER STABLE st1 COMMENT 'test'");
  clearAlterStbReq();

  setAlterStbReq("st1", TSDB_ALTER_TABLE_ADD_COLUMN, 1, "cc1", TSDB_DATA_TYPE_BIGINT);
  run("ALTER STABLE st1 ADD COLUMN cc1 BIGINT");
  clearAlterStbReq();

  setAlterStbReq("st1", TSDB_ALTER_TABLE_DROP_COLUMN, 1, "c1");
  run("ALTER STABLE st1 DROP COLUMN c1");
  clearAlterStbReq();

  setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, 1, "c2", TSDB_DATA_TYPE_VARCHAR, 30 + VARSTR_HEADER_SIZE);
  run("ALTER STABLE st1 MODIFY COLUMN c2 VARCHAR(30)");
  clearAlterStbReq();

  setAlterStbReq("st1", TSDB_ALTER_TABLE_ADD_TAG, 1, "tag11", TSDB_DATA_TYPE_BIGINT);
  run("ALTER STABLE st1 ADD TAG tag11 BIGINT");
  clearAlterStbReq();

  setAlterStbReq("st1", TSDB_ALTER_TABLE_DROP_TAG, 1, "tag1");
  run("ALTER STABLE st1 DROP TAG tag1");
  clearAlterStbReq();

  setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, 1, "tag2", TSDB_DATA_TYPE_VARCHAR, 30 + VARSTR_HEADER_SIZE);
  run("ALTER STABLE st1 MODIFY TAG tag2 VARCHAR(30)");
  clearAlterStbReq();

  setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_TAG_NAME, 2, "tag1", 0, 0, "tag11");
  run("ALTER STABLE st1 RENAME TAG tag1 tag11");
  clearAlterStbReq();

  // super table column rename is now supported (routes to mnode as UPDATE_COLUMN_NAME,
  // carrying old/new names in pFields[0]/[1], same shape as tag rename)
  setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, 2, "c1", 0, 0, "cc1");
  run("ALTER STABLE st1 RENAME COLUMN c1 cc1");
  clearAlterStbReq();
}

TEST_F(ParserInitialATest, alterSTableSemanticCheck) {
  useDb("root", "test");

  run("ALTER STABLE st1 MODIFY COLUMN c2 NCHAR(10)", TSDB_CODE_PAR_INVALID_MODIFY_COL);
  run("ALTER STABLE st1 MODIFY TAG tag2 NCHAR(10)", TSDB_CODE_PAR_INVALID_MODIFY_COL);
  run("ALTER STABLE st1 SET TAG tag1 = 10", TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  run("ALTER STABLE st1 TTL 10", TSDB_CODE_PAR_INVALID_ALTER_TABLE);
}

/*
 * ALTER TABLE [db_name.]tb_name alter_table_clause
 *
 * alter_table_clause: {
 *     alter_table_options
 *   | ADD COLUMN col_name column_type
 *   | DROP COLUMN col_name
 *   | MODIFY COLUMN col_name column_type
 *   | RENAME COLUMN old_col_name new_col_name  -- only normal table
 *   | ADD TAG tag_name tag_type                -- only super table
 *   | DROP TAG tag_name                        -- only super table
 *   | MODIFY TAG tag_name tag_type             -- only super table
 *   | RENAME TAG old_tag_name new_tag_name     -- only super table
 *   | SET TAG tag_name = new_tag_value         -- only child table
 * }
 *
 * alter_table_options:
 *     alter_table_option ...
 *
 * alter_table_option: {
 *    TTL int_value                             -- only child/normal table
 *  | COMMENT 'string_value'
 * }
 */

TEST_F(ParserInitialATest, alterTable) {
  useDb("root", "test");

  // normal/child table
  {
    SVAlterTbReq expect = {0};

    auto clearAlterTbReq = [&]() {
      free(expect.tbName);
      free(expect.colName);
      free(expect.colNewName);
      free(expect.tagName);
      memset(&expect, 0, sizeof(SVAlterTbReq));
    };

    auto setAlterTableCol = [&](const char* pTbname, int8_t alterType, const char* pColName, int8_t dataType = 0,
                                int32_t dataBytes = 0, const char* pNewColName = nullptr) {
      expect.tbName = strdup(pTbname);
      expect.action = alterType;
      expect.colName = strdup(pColName);

      switch (alterType) {
        case TSDB_ALTER_TABLE_ADD_COLUMN:
          expect.type = dataType;
          expect.flags = COL_SMA_ON;
          expect.bytes = dataBytes > 0 ? dataBytes : (dataType > 0 ? tDataTypes[dataType].bytes : 0);
          break;
        case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
          expect.colModBytes = dataBytes;
          break;
        case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
        case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
          expect.colNewName = strdup(pNewColName);
          break;
        default:
          break;
      }
    };

    auto setAlterTableTag = [&](const char* pTbname, const char* pTagName, uint8_t* pNewVal, uint32_t bytes) {
      expect.tbName = strdup(pTbname);
      expect.action = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
      expect.tagName = strdup(pTagName);

      expect.isNull = (nullptr == pNewVal);
      expect.nTagVal = bytes;
      expect.pTagVal = pNewVal;
    };

    auto setAlterTableOptions = [&](const char* pTbname, int32_t ttl, char* pComment = nullptr) {
      expect.tbName = strdup(pTbname);
      expect.action = TSDB_ALTER_TABLE_UPDATE_OPTIONS;
      if (-1 != ttl) {
        expect.updateTTL = true;
        expect.newTTL = ttl;
      }
      if (nullptr != pComment) {
        expect.newCommentLen = strlen(pComment);
        expect.newComment = pComment;
      }
    };

    setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
      ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_VNODE_MODIFY_STMT);
      SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)pQuery->pRoot;

      ASSERT_EQ(pStmt->sqlNodeType, QUERY_NODE_ALTER_TABLE_STMT);
      ASSERT_NE(pStmt->pDataBlocks, nullptr);
      ASSERT_EQ(taosArrayGetSize(pStmt->pDataBlocks), 1);
      SVgDataBlocks* pVgData = (SVgDataBlocks*)taosArrayGetP(pStmt->pDataBlocks, 0);
      void*          pBuf = POINTER_SHIFT(pVgData->pData, sizeof(SMsgHead));
      SVAlterTbReq   req = {0};
      SDecoder       coder = {0};
      tDecoderInit(&coder, (uint8_t*)pBuf, pVgData->size);
      ASSERT_EQ(tDecodeSVAlterTbReq(&coder, &req), TSDB_CODE_SUCCESS);

      if (req.action == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL) {
        ASSERT_NE(req.tables, nullptr);
        ASSERT_EQ((int32_t)taosArrayGetSize(req.tables), 1);
        SUpdateTableTagVal* pTable = (SUpdateTableTagVal*)taosArrayGet(req.tables, 0);
        ASSERT_EQ(std::string(pTable->tbName), std::string(expect.tbName));
        ASSERT_NE(pTable->tags, nullptr);
        ASSERT_EQ((int32_t)taosArrayGetSize(pTable->tags), 1);
        SUpdatedTagVal* pTag = (SUpdatedTagVal*)taosArrayGet(pTable->tags, 0);
        ASSERT_EQ(std::string(pTag->tagName), std::string(expect.tagName));
        ASSERT_EQ(pTag->isNull, expect.isNull);
        ASSERT_EQ(pTag->nTagVal, expect.nTagVal);
        if (nullptr != pTag->pTagVal) {
          ASSERT_EQ(memcmp(pTag->pTagVal, expect.pTagVal, expect.nTagVal), 0);
        }
      } else {
        ASSERT_EQ(std::string(req.tbName), std::string(expect.tbName));
        ASSERT_EQ(req.action, expect.action);
        if (nullptr != expect.colName) {
          ASSERT_EQ(std::string(req.colName), std::string(expect.colName));
        }
        ASSERT_EQ(req.type, expect.type);
        ASSERT_EQ(req.flags, expect.flags);
        ASSERT_EQ(req.bytes, expect.bytes);
        ASSERT_EQ(req.colModBytes, expect.colModBytes);
        if (nullptr != expect.colNewName) {
          ASSERT_EQ(std::string(req.colNewName), std::string(expect.colNewName));
        }
        if (nullptr != expect.tagName) {
          ASSERT_EQ(std::string(req.tagName), std::string(expect.tagName));
        }
        ASSERT_EQ(req.isNull, expect.isNull);
        ASSERT_EQ(req.nTagVal, expect.nTagVal);
        if (nullptr != req.pTagVal) {
          ASSERT_EQ(memcmp(req.pTagVal, expect.pTagVal, expect.nTagVal), 0);
        }
        ASSERT_EQ(req.updateTTL, expect.updateTTL);
        ASSERT_EQ(req.newTTL, expect.newTTL);
        if (nullptr != expect.newComment) {
          ASSERT_EQ(std::string(req.newComment), std::string(expect.newComment));
          ASSERT_EQ(req.newCommentLen, strlen(req.newComment));
          ASSERT_EQ(expect.newCommentLen, strlen(expect.newComment));
        }
      }

      destroyAlterTbReq(&req);
      tDecoderClear(&coder);
    });

    setAlterTableOptions("t1", 10, nullptr);
    run("ALTER TABLE t1 TTL 10");
    clearAlterTbReq();

    setAlterTableOptions("t1", -1, (char*)"test");
    run("ALTER TABLE t1 COMMENT 'test'");
    clearAlterTbReq();

    setAlterTableCol("t1", TSDB_ALTER_TABLE_ADD_COLUMN, "cc1", TSDB_DATA_TYPE_BIGINT);
    run("ALTER TABLE t1 ADD COLUMN cc1 BIGINT");
    clearAlterTbReq();

    setAlterTableCol("t1", TSDB_ALTER_TABLE_DROP_COLUMN, "c1");
    run("ALTER TABLE t1 DROP COLUMN c1");
    clearAlterTbReq();

    setAlterTableCol("t1", TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, "c2", TSDB_DATA_TYPE_VARCHAR, 30 + VARSTR_HEADER_SIZE);
    run("ALTER TABLE t1 MODIFY COLUMN c2 VARCHAR(30)");
    clearAlterTbReq();

    setAlterTableCol("t1", TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, "c1", 0, 0, "cc1");
    run("ALTER TABLE t1 RENAME COLUMN c1 cc1");
    clearAlterTbReq();

    // RENAME TAG on the owned tags of a normal table goes to the vnode (t3 owns ntag1/ntag2);
    // the super-table form is routed to the mnode instead (asserted in the super table block).
    setAlterTableCol("t3", TSDB_ALTER_TABLE_UPDATE_TAG_NAME, "ntag1", 0, 0, "ntag1_new");
    run("ALTER TABLE t3 RENAME TAG ntag1 ntag1_new");
    clearAlterTbReq();

    int32_t val = 10;
    setAlterTableTag("st1s1", "tag1", (uint8_t*)&val, sizeof(val));
    run("ALTER TABLE st1s1 SET TAG tag1=10");
    clearAlterTbReq();
  }

  // super table
  {
    SMAlterStbReq expect = {0};

    auto clearAlterStbReq = [&]() {
      tFreeSMAltertbReq(&expect);
      memset(&expect, 0, sizeof(SMAlterStbReq));
    };

    auto setAlterStbReq = [&](const char* pTbname, int8_t alterType, int32_t numOfFields = 0,
                              const char* pField1Name = nullptr, int8_t field1Type = 0, int32_t field1Bytes = 0,
                              const char* pField2Name = nullptr, const char* pComment = nullptr) {
      int32_t len = snprintf(expect.name, sizeof(expect.name), "0.test.%s", pTbname);
      expect.name[len] = '\0';
      expect.alterType = alterType;
      if (nullptr != pComment) {
        expect.comment = strdup(pComment);
        expect.commentLen = strlen(pComment);
      }

      expect.numOfFields = numOfFields;
      if (NULL == expect.pFields) {
        expect.pFields = taosArrayInit(2, sizeof(TAOS_FIELD));
        ASSERT_TRUE(expect.pFields);
        TAOS_FIELD field = {0};
        ASSERT_TRUE(nullptr != taosArrayPush(expect.pFields, &field));
        ASSERT_TRUE(nullptr != taosArrayPush(expect.pFields, &field));
      }

      TAOS_FIELD* pField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 0);
      if (NULL != pField1Name) {
        strcpy(pField->name, pField1Name);
        pField->name[strlen(pField1Name)] = '\0';
      } else {
        memset(pField, 0, sizeof(TAOS_FIELD));
      }
      pField->type = field1Type;
      pField->bytes = field1Bytes > 0 ? field1Bytes : (field1Type > 0 ? tDataTypes[field1Type].bytes : 0);

      pField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 1);
      if (NULL != pField2Name) {
        strcpy(pField->name, pField2Name);
        pField->name[strlen(pField2Name)] = '\0';
      } else {
        memset(pField, 0, sizeof(TAOS_FIELD));
      }
      pField->type = 0;
      pField->bytes = 0;
    };

    setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
      ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_TABLE_STMT);
      ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_ALTER_STB);
      SMAlterStbReq req = {0};
      ASSERT_EQ(tDeserializeSMAlterStbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
      ASSERT_EQ(std::string(req.name), std::string(expect.name));
      ASSERT_EQ(req.alterType, expect.alterType);
      ASSERT_EQ(req.numOfFields, expect.numOfFields);
      if (expect.numOfFields > 0) {
        TAOS_FIELD* pField = (TAOS_FIELD*)taosArrayGet(req.pFields, 0);
        TAOS_FIELD* pExpectField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 0);
        ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
        ASSERT_EQ(pField->type, pExpectField->type);
        ASSERT_EQ(pField->bytes, pExpectField->bytes);
      }
      if (expect.numOfFields > 1) {
        TAOS_FIELD* pField = (TAOS_FIELD*)taosArrayGet(req.pFields, 1);
        TAOS_FIELD* pExpectField = (TAOS_FIELD*)taosArrayGet(expect.pFields, 1);
        ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
        ASSERT_EQ(pField->type, pExpectField->type);
        ASSERT_EQ(pField->bytes, pExpectField->bytes);
      }
      tFreeSMAltertbReq(&req);
    });

    setAlterStbReq("st1", TSDB_ALTER_TABLE_ADD_TAG, 1, "tag11", TSDB_DATA_TYPE_BIGINT);
    run("ALTER TABLE st1 ADD TAG tag11 BIGINT");
    clearAlterStbReq();

    setAlterStbReq("st1", TSDB_ALTER_TABLE_DROP_TAG, 1, "tag1");
    run("ALTER TABLE st1 DROP TAG tag1");
    clearAlterStbReq();

    setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, 1, "tag2", TSDB_DATA_TYPE_VARCHAR,
                   30 + VARSTR_HEADER_SIZE);
    run("ALTER TABLE st1 MODIFY TAG tag2 VARCHAR(30)");
    clearAlterStbReq();

    setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_TAG_NAME, 2, "tag1", 0, 0, "tag11");
    run("ALTER TABLE st1 RENAME TAG tag1 tag11");
    clearAlterStbReq();

    // super table column rename via ALTER TABLE (non-STABLE) syntax also routes to mnode
    setAlterStbReq("st1", TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, 2, "c1", 0, 0, "cc1");
    run("ALTER TABLE st1 RENAME COLUMN c1 cc1");
    clearAlterStbReq();
  }
}

TEST_F(ParserInitialATest, alterTableSemanticCheck) {
  useDb("root", "test");

  run("ALTER TABLE st1s1 RENAME COLUMN c1 cc1", TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  // RENAME TAG on a normal table's owned tags: the old tag must exist and the new name must not
  // collide with an existing tag or column. RENAME COLUMN and RENAME TAG are not interchangeable.
  run("ALTER TABLE t3 RENAME TAG nosuchtag x", TSDB_CODE_PAR_INVALID_TAG_NAME);
  run("ALTER TABLE t3 RENAME TAG ntag1 ntag2", TSDB_CODE_PAR_DUPLICATED_COLUMN);
  run("ALTER TABLE t3 RENAME TAG ntag1 c1", TSDB_CODE_PAR_DUPLICATED_COLUMN);
  run("ALTER TABLE t3 RENAME COLUMN ntag1 x", TSDB_CODE_PAR_INVALID_COLUMN);
  run("ALTER TABLE t3 RENAME TAG c1 x", TSDB_CODE_PAR_INVALID_TAG_NAME);
  // a normal table without tags has nothing to rename
  run("ALTER TABLE t1 RENAME TAG t x", TSDB_CODE_PAR_INVALID_TAG_NAME);

  // The RESTRICT verdict comes back from the mnode over TDMT_MND_CHECK_COL_REF_BY_PRIV and must
  // travel out of translate() unchanged for both RENAME forms — checkColRefByPriv is the only
  // barrier between these two statements and the privilege-dependency check, so a swallowed or
  // remapped code would silently turn "referenced by a privilege" into an accepted rename.
  // The stub answer is process-global, hence the scoped guards.
  {
    ScopedChkColRefByPrivCode userPriv(TSDB_CODE_MND_COL_REF_BY_USER_PRIV);
    run("ALTER TABLE t3 RENAME TAG ntag1 ntag1_new", TSDB_CODE_MND_COL_REF_BY_USER_PRIV);
    run("ALTER TABLE t1 RENAME COLUMN c1 cc1", TSDB_CODE_MND_COL_REF_BY_USER_PRIV);
  }
  {
    ScopedChkColRefByPrivCode rolePriv(TSDB_CODE_MND_COL_REF_BY_ROLE_PRIV);
    run("ALTER TABLE t3 RENAME TAG ntag1 ntag1_new", TSDB_CODE_MND_COL_REF_BY_ROLE_PRIV);
  }

  run("ALTER TABLE st1s1 ADD TAG tag11 BIGINT", TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  run("ALTER TABLE st1s1 DROP TAG tag1", TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  run("ALTER TABLE st1s1 MODIFY TAG tag2 VARCHAR(30)", TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  run("ALTER TABLE st1s1 RENAME TAG tag1 tag11", TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  run("ALTER TABLE st1s1 SET TAG tag2 =  '123456789012345678901'", TSDB_CODE_PAR_VALUE_TOO_LONG);
}

#if 0
/*
 * ALTER USER user_name alter_user_clause
 *
 * alter_user_clause: {
 *    PASS str_value
 *  | ENABLE int_value
 *  | SYSINFO int_value
 * }
 */
TEST_F(ParserInitialATest, alterUser) {
  useDb("root", "test");

  SAlterUserReq expect = {0};

  auto clearAlterUserReq = [&]() { memset(&expect, 0, sizeof(SAlterUserReq)); };

  auto setAlterUserReq = [&](const char* pUser, int8_t alterType, const char* pPass = nullptr, int8_t sysInfo = 0,
                             int8_t enable = 0) {
    strcpy(expect.user, pUser);
    expect.alterType = alterType;
    expect.sysinfo = sysInfo;
    expect.enable = enable;
    if (nullptr != pPass) {
      taosEncryptPass_c((uint8_t*)pPass, strlen(pPass), expect.pass);
    }
    strcpy(expect.objname, "test");
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_USER_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_ALTER_USER);
    SAlterUserReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSAlterUserReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(req.alterType, expect.alterType);
    ASSERT_EQ(req.sysinfo, expect.sysinfo);
    ASSERT_EQ(req.enable, expect.enable);
    ASSERT_EQ(std::string(req.user), std::string(expect.user));
    ASSERT_EQ(std::string(req.pass), std::string(expect.pass));
    ASSERT_EQ(std::string(req.objname), std::string(expect.objname));
    tFreeSAlterUserReq(&req);
  });

  setAlterUserReq("wxy", TSDB_ALTER_USER_PASSWD, "12345678@Abc");
  run("ALTER USER wxy PASS '12345678@Abc'");
  clearAlterUserReq();

  setAlterUserReq("wxy", TSDB_ALTER_USER_ENABLE, nullptr, 0, 1);
  run("ALTER USER wxy ENABLE 1");
  clearAlterUserReq();

  setAlterUserReq("wxy", TSDB_ALTER_USER_SYSINFO, nullptr, 1);
  run("ALTER USER wxy SYSINFO 1");
  clearAlterUserReq();
}
#endif

/*
 * BALANCE VGROUP
 */
TEST_F(ParserInitialATest, balanceVgroup) {
  useDb("root", "test");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_BALANCE_VGROUP_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_BALANCE_VGROUP);
    SBalanceVgroupReq req = {0};
    ASSERT_EQ(tDeserializeSBalanceVgroupReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
    tFreeSBalanceVgroupReq(&req);
  });

  run("BALANCE VGROUP");
}

/*
 * BALANCE VGROUP LEADER [ ON vgroup_id | DATABASE db_name ] [ FORCE ]
 *
 * The command was split into two behaviors selected by the optional FORCE keyword.
 * The parser encodes that keyword into SBalanceVgroupLeaderReq.reserved:
 *   - reserved == 0 -> non-forced (default, random rebalance, what old clients send)
 *   - reserved == 1 -> forced (deterministic, bias elect baseline)
 * We assert the round-tripped reserved value for every syntactic variant. Note the
 * DATABASE variant produces a different node type
 * (QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT), so we do NOT assert a single
 * unified node type here; validating the message-layer reserved flag is the goal.
 */
TEST_F(ParserInitialATest, balanceVgroupLeader) {
  useDb("root", "test");

  // Expected value of req.reserved for the statement currently under test. The check
  // lambda captures this by reference; set it before each run() call.
  int32_t expectedReserved = 0;

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_BALANCE_VGROUP_LEADER);
    SBalanceVgroupLeaderReq req = {0};
    ASSERT_EQ(tDeserializeSBalanceVgroupLeaderReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
              TSDB_CODE_SUCCESS);
    // reserved carries the FORCE flag (0 = non-forced, 1 = forced).
    ASSERT_EQ(req.reserved, expectedReserved);
    tFreeSBalanceVgroupLeaderReq(&req);
  });

  // Non-forced variants: FORCE keyword absent -> reserved == 0.
  expectedReserved = 0;
  run("BALANCE VGROUP LEADER");
  run("BALANCE VGROUP LEADER ON 1");
  run("BALANCE VGROUP LEADER DATABASE test");

  // Forced variants: FORCE keyword present -> reserved == 1.
  expectedReserved = 1;
  run("BALANCE VGROUP LEADER FORCE");
  run("BALANCE VGROUP LEADER ON 1 FORCE");
  run("BALANCE VGROUP LEADER DATABASE test FORCE");
}

}  // namespace ParserTest
