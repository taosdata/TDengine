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

#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserInitialATest : public ParserDdlTest {};

TEST_F(ParserInitialATest, alterAccount) {
  useDb("root", "test");

  run("ALTER ACCOUNT ac_wxy PASS '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT);
}

TEST_F(ParserInitialATest, alterDnode) {
  useDb("root", "test");

  run("ALTER DNODE 1 'resetLog'");

  run("ALTER DNODE 1 'debugFlag' '134'");
}

TEST_F(ParserInitialATest, alterDatabase) {
  useDb("root", "test");

  run("ALTER DATABASE wxy_db CACHELAST 1 FSYNC 200 WAL 1");
}

// todo ALTER local
// todo ALTER stable

/*
 * ALTER TABLE [db_name.]tb_name alter_table_clause
 *
 * alter_table_clause: {
 *     alter_table_options
 *   | ADD COLUMN col_name column_type
 *   | DROP COLUMN col_name
 *   | MODIFY COLUMN col_name column_type
 *   | RENAME COLUMN old_col_name new_col_name
 *   | ADD TAG tag_name tag_type
 *   | DROP TAG tag_name
 *   | MODIFY TAG tag_name tag_type
 *   | RENAME TAG old_tag_name new_tag_name
 *   | SET TAG tag_name = new_tag_value
 *   | ADD {FULLTEXT | SMA} INDEX index_name (col_name [, col_name] ...) [index_option]
 * }
 *
 * alter_table_options:
 *     alter_table_option ...
 *
 * alter_table_option: {
 *     TTL value
 *   | COMMENT 'string_value'
 * }
 */
TEST_F(ParserInitialATest, alterTable) {
  useDb("root", "test");

  SMAlterStbReq expect = {0};

  auto setAlterStbReqFunc = [&](const char* pTbname, int8_t alterType, int32_t numOfFields = 0,
                                const char* pField1Name = nullptr, int8_t field1Type = 0, int32_t field1Bytes = 0,
                                const char* pField2Name = nullptr, const char* pComment = nullptr,
                                int32_t ttl = TSDB_DEFAULT_TABLE_TTL) {
    int32_t len = snprintf(expect.name, sizeof(expect.name), "0.test.%s", pTbname);
    expect.name[len] = '\0';
    expect.alterType = alterType;
    expect.ttl = ttl;
    if (nullptr != pComment) {
      expect.comment = strdup(pComment);
      expect.commentLen = strlen(pComment) + 1;
    }

    expect.numOfFields = numOfFields;
    if (NULL == expect.pFields) {
      expect.pFields = taosArrayInit(2, sizeof(TAOS_FIELD));
      TAOS_FIELD field = {0};
      taosArrayPush(expect.pFields, &field);
      taosArrayPush(expect.pFields, &field);
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
    SMAlterStbReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSMAlterStbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
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
  });

  setAlterStbReqFunc("t1", TSDB_ALTER_TABLE_UPDATE_OPTIONS, 0, nullptr, 0, 0, nullptr, nullptr, 10);
  run("ALTER TABLE t1 TTL 10");

  setAlterStbReqFunc("t1", TSDB_ALTER_TABLE_UPDATE_OPTIONS, 0, nullptr, 0, 0, nullptr, "test");
  run("ALTER TABLE t1 COMMENT 'test'");

  setAlterStbReqFunc("t1", TSDB_ALTER_TABLE_ADD_COLUMN, 1, "cc1", TSDB_DATA_TYPE_BIGINT);
  run("ALTER TABLE t1 ADD COLUMN cc1 BIGINT");

  setAlterStbReqFunc("t1", TSDB_ALTER_TABLE_DROP_COLUMN, 1, "c1");
  run("ALTER TABLE t1 DROP COLUMN c1");

  setAlterStbReqFunc("t1", TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, 1, "c1", TSDB_DATA_TYPE_VARCHAR,
                     20 + VARSTR_HEADER_SIZE);
  run("ALTER TABLE t1 MODIFY COLUMN c1 VARCHAR(20)");

  setAlterStbReqFunc("t1", TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, 2, "c1", 0, 0, "cc1");
  run("ALTER TABLE t1 RENAME COLUMN c1 cc1");

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_ADD_TAG, 1, "tag11", TSDB_DATA_TYPE_BIGINT);
  run("ALTER TABLE st1 ADD TAG tag11 BIGINT");

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_DROP_TAG, 1, "tag1");
  run("ALTER TABLE st1 DROP TAG tag1");

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, 1, "tag1", TSDB_DATA_TYPE_VARCHAR,
                     20 + VARSTR_HEADER_SIZE);
  run("ALTER TABLE st1 MODIFY TAG tag1 VARCHAR(20)");

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_TAG_NAME, 2, "tag1", 0, 0, "tag11");
  run("ALTER TABLE st1 RENAME TAG tag1 tag11");

  // run("ALTER TABLE st1s1 SET TAG tag1=10");

  // todo
  // ADD {FULLTEXT | SMA} INDEX index_name (col_name [, col_name] ...) [index_option]
}

TEST_F(ParserInitialATest, alterUser) {
  useDb("root", "test");

  run("ALTER user wxy PASS '123456'");

  run("ALTER user wxy privilege 'write'");
}

TEST_F(ParserInitialATest, bug001) {
  useDb("root", "test");

  run("ALTER DATABASE db WAL 0     # td-14436", TSDB_CODE_PAR_SYNTAX_ERROR);
}

}  // namespace ParserTest