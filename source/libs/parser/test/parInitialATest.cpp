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

  run("ALTER ACCOUNT ac_wxy PASS '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT, PARSER_STAGE_PARSE);
}

TEST_F(ParserInitialATest, alterDnode) {
  useDb("root", "test");

  run("ALTER DNODE 1 'resetLog'");

  run("ALTER DNODE 1 'debugFlag' '134'");
}

TEST_F(ParserInitialATest, alterDatabase) {
  useDb("root", "test");

  run("ALTER DATABASE wxy_db CACHELAST 1 FSYNC 200 WAL 1");

  run("ALTER DATABASE wxy_db KEEP 2400");
}

TEST_F(ParserInitialATest, alterLocal) {
  useDb("root", "test");

  pair<string, string> expect;

  auto clearAlterLocal = [&]() {
    expect.first.clear();
    expect.second.clear();
  };

  auto setAlterLocalFunc = [&](const char* pConfig, const char* pValue = nullptr) {
    expect.first.assign(pConfig);
    if (nullptr != pValue) {
      expect.second.assign(pValue);
    }
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_LOCAL_STMT);
    ASSERT_EQ(pQuery->execMode, QUERY_EXEC_MODE_LOCAL);
    SAlterLocalStmt* pStmt = (SAlterLocalStmt*)pQuery->pRoot;
    ASSERT_EQ(string(pStmt->config), expect.first);
    ASSERT_EQ(string(pStmt->value), expect.second);
  });

  setAlterLocalFunc("resetlog");
  run("ALTER LOCAL 'resetlog'");
  clearAlterLocal();

  setAlterLocalFunc("querypolicy", "2");
  run("ALTER LOCAL 'querypolicy' '2'");
  clearAlterLocal();
}

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
TEST_F(ParserInitialATest, alterSTable) {
  useDb("root", "test");

  SMAlterStbReq expect = {0};

  auto clearAlterStbReq = [&]() {
    tFreeSMAltertbReq(&expect);
    memset(&expect, 0, sizeof(SMAlterStbReq));
  };

  auto setAlterStbReqFunc = [&](const char* pTbname, int8_t alterType, int32_t numOfFields = 0,
                                const char* pField1Name = nullptr, int8_t field1Type = 0, int32_t field1Bytes = 0,
                                const char* pField2Name = nullptr, const char* pComment = nullptr,
                                int32_t ttl = TSDB_DEFAULT_TABLE_TTL) {
    int32_t len = snprintf(expect.name, sizeof(expect.name), "0.test.%s", pTbname);
    expect.name[len] = '\0';
    expect.alterType = alterType;
    //    expect.ttl = ttl;
    if (nullptr != pComment) {
      expect.comment = strdup(pComment);
      expect.commentLen = strlen(pComment);
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
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_SUPER_TABLE_STMT);
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

  //  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_OPTIONS, 0, nullptr, 0, 0, nullptr, nullptr, 10);
  //  run("ALTER STABLE st1 TTL 10");
  //  clearAlterStbReq();

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_OPTIONS, 0, nullptr, 0, 0, nullptr, "test");
  run("ALTER STABLE st1 COMMENT 'test'");
  clearAlterStbReq();

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_ADD_COLUMN, 1, "cc1", TSDB_DATA_TYPE_BIGINT);
  run("ALTER STABLE st1 ADD COLUMN cc1 BIGINT");
  clearAlterStbReq();

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_DROP_COLUMN, 1, "c1");
  run("ALTER STABLE st1 DROP COLUMN c1");
  clearAlterStbReq();

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, 1, "c2", TSDB_DATA_TYPE_VARCHAR,
                     30 + VARSTR_HEADER_SIZE);
  run("ALTER STABLE st1 MODIFY COLUMN c2 VARCHAR(30)");
  clearAlterStbReq();

  // setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, 2, "c1", 0, 0, "cc1");
  // run("ALTER STABLE st1 RENAME COLUMN c1 cc1");

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_ADD_TAG, 1, "tag11", TSDB_DATA_TYPE_BIGINT);
  run("ALTER STABLE st1 ADD TAG tag11 BIGINT");
  clearAlterStbReq();

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_DROP_TAG, 1, "tag1");
  run("ALTER STABLE st1 DROP TAG tag1");
  clearAlterStbReq();

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, 1, "tag2", TSDB_DATA_TYPE_VARCHAR,
                     30 + VARSTR_HEADER_SIZE);
  run("ALTER STABLE st1 MODIFY TAG tag2 VARCHAR(30)");
  clearAlterStbReq();

  setAlterStbReqFunc("st1", TSDB_ALTER_TABLE_UPDATE_TAG_NAME, 2, "tag1", 0, 0, "tag11");
  run("ALTER STABLE st1 RENAME TAG tag1 tag11");
  clearAlterStbReq();

  // todo
  // ADD {FULLTEXT | SMA} INDEX index_name (col_name [, col_name] ...) [index_option]
}

TEST_F(ParserInitialATest, alterSTableSemanticCheck) {
  useDb("root", "test");

  run("ALTER STABLE st1 RENAME COLUMN c1 cc1", TSDB_CODE_PAR_INVALID_ALTER_TABLE);

  run("ALTER STABLE st1 MODIFY COLUMN c2 NCHAR(10)", TSDB_CODE_PAR_INVALID_MODIFY_COL);

  run("ALTER STABLE st1 MODIFY TAG tag2 NCHAR(10)", TSDB_CODE_PAR_INVALID_MODIFY_COL);
}

TEST_F(ParserInitialATest, alterTable) {
  useDb("root", "test");

  SVAlterTbReq expect = {0};

  auto clearAlterTbReq = [&]() {
    free(expect.tbName);
    free(expect.colName);
    free(expect.colNewName);
    free(expect.tagName);
    memset(&expect, 0, sizeof(SVAlterTbReq));
  };

  auto setAlterColFunc = [&](const char* pTbname, int8_t alterType, const char* pColName, int8_t dataType = 0,
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
        expect.colNewName = strdup(pNewColName);
        break;
      default:
        break;
    }
  };

  auto setAlterTagFunc = [&](const char* pTbname, const char* pTagName, uint8_t* pNewVal, uint32_t bytes) {
    expect.tbName = strdup(pTbname);
    expect.action = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
    expect.tagName = strdup(pTagName);

    expect.isNull = (nullptr == pNewVal);
    expect.nTagVal = bytes;
    expect.pTagVal = pNewVal;
  };

  auto setAlterOptionsFunc = [&](const char* pTbname, int32_t ttl, char* pComment = nullptr) {
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
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_VNODE_MODIF_STMT);
    SVnodeModifOpStmt* pStmt = (SVnodeModifOpStmt*)pQuery->pRoot;

    ASSERT_EQ(pStmt->sqlNodeType, QUERY_NODE_ALTER_TABLE_STMT);
    ASSERT_NE(pStmt->pDataBlocks, nullptr);
    ASSERT_EQ(taosArrayGetSize(pStmt->pDataBlocks), 1);
    SVgDataBlocks* pVgData = (SVgDataBlocks*)taosArrayGetP(pStmt->pDataBlocks, 0);
    void*          pBuf = POINTER_SHIFT(pVgData->pData, sizeof(SMsgHead));
    SVAlterTbReq   req = {0};
    SDecoder       coder = {0};
    tDecoderInit(&coder, (uint8_t*)pBuf, pVgData->size);
    ASSERT_EQ(tDecodeSVAlterTbReq(&coder, &req), TSDB_CODE_SUCCESS);

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
    ASSERT_EQ(memcmp(req.pTagVal, expect.pTagVal, expect.nTagVal), 0);
    ASSERT_EQ(req.updateTTL, expect.updateTTL);
    ASSERT_EQ(req.newTTL, expect.newTTL);
    if (nullptr != expect.newComment) {
      ASSERT_EQ(std::string(req.newComment), std::string(expect.newComment));
      ASSERT_EQ(req.newCommentLen, strlen(req.newComment));
      ASSERT_EQ(expect.newCommentLen, strlen(expect.newComment));
    }

    tDecoderClear(&coder);
  });

  setAlterOptionsFunc("t1", 10, nullptr);
  run("ALTER TABLE t1 TTL 10");
  clearAlterTbReq();

  setAlterOptionsFunc("t1", -1, (char*)"test");
  run("ALTER TABLE t1 COMMENT 'test'");
  clearAlterTbReq();

  setAlterColFunc("t1", TSDB_ALTER_TABLE_ADD_COLUMN, "cc1", TSDB_DATA_TYPE_BIGINT);
  run("ALTER TABLE t1 ADD COLUMN cc1 BIGINT");
  clearAlterTbReq();

  setAlterColFunc("t1", TSDB_ALTER_TABLE_DROP_COLUMN, "c1");
  run("ALTER TABLE t1 DROP COLUMN c1");
  clearAlterTbReq();

  setAlterColFunc("t1", TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, "c2", TSDB_DATA_TYPE_VARCHAR, 30 + VARSTR_HEADER_SIZE);
  run("ALTER TABLE t1 MODIFY COLUMN c2 VARCHAR(30)");
  clearAlterTbReq();

  setAlterColFunc("t1", TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, "c1", 0, 0, "cc1");
  run("ALTER TABLE t1 RENAME COLUMN c1 cc1");
  clearAlterTbReq();

  int32_t val = 10;
  setAlterTagFunc("st1s1", "tag1", (uint8_t*)&val, sizeof(val));
  run("ALTER TABLE st1s1 SET TAG tag1=10");
  clearAlterTbReq();

  // todo
  // ADD {FULLTEXT | SMA} INDEX index_name (col_name [, col_name] ...) [index_option]
}

TEST_F(ParserInitialATest, alterTableSemanticCheck) {
  useDb("root", "test");

  run("ALTER TABLE st1s1 RENAME COLUMN c1 cc1", TSDB_CODE_PAR_INVALID_ALTER_TABLE);

  run("ALTER TABLE st1s1 SET TAG tag2 =  '123456789012345678901'", TSDB_CODE_PAR_WRONG_VALUE_TYPE);
}

TEST_F(ParserInitialATest, alterUser) {
  useDb("root", "test");

  SAlterUserReq expect = {0};

  auto clearAlterUserReq = [&]() { memset(&expect, 0, sizeof(SAlterUserReq)); };

  auto setAlterUserReq = [&](const char* pUser, int8_t alterType, const char* pPass = nullptr, int8_t sysInfo = 0,
                             int8_t enable = 0) {
    strcpy(expect.user, pUser);
    expect.alterType = alterType;
    expect.superUser = 0;
    expect.sysInfo = sysInfo;
    expect.enable = enable;
    if (nullptr != pPass) {
      strcpy(expect.pass, pPass);
    }
    strcpy(expect.dbname, "test");
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_ALTER_USER_STMT);
    SAlterUserReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSAlterUserReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(req.alterType, expect.alterType);
    ASSERT_EQ(req.superUser, expect.superUser);
    ASSERT_EQ(req.sysInfo, expect.sysInfo);
    ASSERT_EQ(req.enable, expect.enable);
    ASSERT_EQ(std::string(req.user), std::string(expect.user));
    ASSERT_EQ(std::string(req.pass), std::string(expect.pass));
    ASSERT_EQ(std::string(req.dbname), std::string(expect.dbname));
  });

  setAlterUserReq("wxy", TSDB_ALTER_USER_PASSWD, "123456");
  run("ALTER USER wxy PASS '123456'");
  clearAlterUserReq();

  setAlterUserReq("wxy", TSDB_ALTER_USER_ENABLE, nullptr, 0, 1);
  run("ALTER USER wxy ENABLE 1");
  clearAlterUserReq();

  setAlterUserReq("wxy", TSDB_ALTER_USER_SYSINFO, nullptr, 1);
  run("ALTER USER wxy SYSINFO 1");
  clearAlterUserReq();
}

TEST_F(ParserInitialATest, balanceVgroup) {
  useDb("root", "test");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_BALANCE_VGROUP_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_BALANCE_VGROUP);
    SBalanceVgroupReq req = {0};
    ASSERT_EQ(tDeserializeSBalanceVgroupReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
  });

  run("BALANCE VGROUP");
}

TEST_F(ParserInitialATest, bug001) {
  useDb("root", "test");

  run("ALTER DATABASE db WAL 0     # td-14436", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

}  // namespace ParserTest