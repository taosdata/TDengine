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

#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserXnodeTest : public ParserDdlTest {
 protected:
  void checkShowName(const SQuery* pQuery, const string& expected) {
    const auto* select = reinterpret_cast<const SSelectStmt*>(pQuery->pRoot);
    ASSERT_NE(select->pWhere, nullptr);
    ASSERT_EQ(nodeType(select->pWhere), QUERY_NODE_OPERATOR);
    const auto* condition = reinterpret_cast<const SOperatorNode*>(select->pWhere);
    ASSERT_EQ(nodeType(condition->pRight), QUERY_NODE_VALUE);
    const auto* value = reinterpret_cast<const SValueNode*>(condition->pRight);
    EXPECT_STREQ(value->literal, expected.c_str());
  }

  void checkTaskName(const string& token, const string& expected) {
    SCOPED_TRACE(token);
    setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
      if (nodeType(pQuery->pRoot) == QUERY_NODE_SELECT_STMT) {
        checkShowName(pQuery, expected);
        return;
      }

      ASSERT_NE(pQuery->pCmdMsg, nullptr);
      switch (pQuery->pCmdMsg->msgType) {
        case TDMT_MND_CREATE_XNODE_TASK: {
          SMCreateXnodeTaskReq req = {0};
          ASSERT_EQ(tDeserializeSMCreateXnodeTaskReq(pQuery->pCmdMsg->pMsg,
                                                    pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
          EXPECT_EQ(string(req.name.ptr, req.name.len), expected);
          tFreeSMCreateXnodeTaskReq(&req);
          break;
        }
        case TDMT_MND_START_XNODE_TASK: {
          SMStartXnodeTaskReq req = {0};
          ASSERT_EQ(tDeserializeSMStartXnodeTaskReq(pQuery->pCmdMsg->pMsg,
                                                   pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
          EXPECT_EQ(string(req.name.ptr, req.name.len), expected);
          tFreeSMStartXnodeTaskReq(&req);
          break;
        }
        case TDMT_MND_STOP_XNODE_TASK: {
          SMStopXnodeTaskReq req = {0};
          ASSERT_EQ(tDeserializeSMStopXnodeTaskReq(pQuery->pCmdMsg->pMsg,
                                                  pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
          EXPECT_EQ(string(req.name.ptr, req.name.len), expected);
          tFreeSMStopXnodeTaskReq(&req);
          break;
        }
        case TDMT_MND_UPDATE_XNODE_TASK: {
          SMUpdateXnodeTaskReq req = {0};
          ASSERT_EQ(tDeserializeSMUpdateXnodeTaskReq(pQuery->pCmdMsg->pMsg,
                                                    pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
          EXPECT_EQ(string(req.name.ptr, req.name.len), expected);
          tFreeSMUpdateXnodeTaskReq(&req);
          break;
        }
        case TDMT_MND_DROP_XNODE_TASK: {
          SMDropXnodeTaskReq req = {0};
          ASSERT_EQ(tDeserializeSMDropXnodeTaskReq(pQuery->pCmdMsg->pMsg,
                                                  pQuery->pCmdMsg->msgLen, &req), TSDB_CODE_SUCCESS);
          EXPECT_EQ(string(req.name.ptr, req.name.len), expected);
          tFreeSMDropXnodeTaskReq(&req);
          break;
        }
        default:
          FAIL() << "Unexpected message type: " << pQuery->pCmdMsg->msgType;
      }
    });

    run("CREATE XNODE TASK " + token + " FROM 'source' TO 'sink'");
    run("CREATE XNODE TASK " + token + " FROM 'source' TO 'sink' WITH type 'replica'");
    run("START XNODE TASK " + token);
    run("STOP XNODE TASK " + token);
    run("ALTER XNODE TASK " + token + " WITH status 'stopped'");
    run("DROP XNODE TASK " + token);
    run("SHOW XNODE TASKS WHERE name = " + token);
  }
};

TEST_F(ParserXnodeTest, taskNameSqlEscapesAreDecoded) {
  login("root");
  useDb("root", "test");

  struct NameCase {
    string token;
    string expected;
  };
  const NameCase nameCases[] = {
      {"'plain'", "plain"},
      {"'test''copy'", "test'copy"},
      {R"('test\'copy')", "test'copy"},
      {R"("test""copy")", "test\"copy"},
      {R"('test\\copy')", R"(test\copy)"},
      {R"('test\ncopy')", "test\ncopy"},
      {R"('test\%\_\xcopy')", R"(test\%\_\xcopy)"},
      {"'test''''copy'", "test''copy"},
      {"'123'", "123"},
  };
  for (const NameCase& nameCase : nameCases) {
    checkTaskName(nameCase.token, nameCase.expected);
  }
}

TEST_F(ParserXnodeTest, taskNameLimitsUseDecodedLength) {
  login("root");
  useDb("root", "test");

  checkTaskName("'" + string(TSDB_XNODE_TASK_NAME_LEN, 'a') + "'",
                string(TSDB_XNODE_TASK_NAME_LEN, 'a'));
  checkTaskName("'" + string(TSDB_XNODE_TASK_NAME_LEN * 2, '\'') + "'",
                string(TSDB_XNODE_TASK_NAME_LEN, '\''));
  checkTaskName("'" + string(TSDB_XNODE_TASK_NAME_LEN * 2, '\\') + "'",
                string(TSDB_XNODE_TASK_NAME_LEN, '\\'));
}

TEST_F(ParserXnodeTest, taskNamesRejectEmptyAndDecodedOverflow) {
  login("root");
  useDb("root", "test");

  const string tokens[] = {
      "''",
      "'" + string(TSDB_XNODE_TASK_NAME_LEN + 1, 'a') + "'",
      "'" + string((TSDB_XNODE_TASK_NAME_LEN + 1) * 2, '\'') + "'",
  };
  for (const string& token : tokens) {
    const string statements[] = {
        "CREATE XNODE TASK " + token + " FROM 'source' TO 'sink'",
        "START XNODE TASK " + token,
        "STOP XNODE TASK " + token,
        "ALTER XNODE TASK " + token + " WITH status 'stopped'",
        "DROP XNODE TASK " + token,
    };
    for (const string& statement : statements) {
      run(statement, TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
    }
  }
}

TEST_F(ParserXnodeTest, taskNumericIdsArePreserved) {
  login("root");
  useDb("root", "test");

  setCheckDdlFunc([](const SQuery* pQuery, ParserStage stage) {
    if (nodeType(pQuery->pRoot) == QUERY_NODE_UPDATE_XNODE_TASK_STMT) {
      const auto* task = reinterpret_cast<const SUpdateXnodeTaskStmt*>(pQuery->pRoot);
      EXPECT_EQ(task->tid, 123);
      EXPECT_EQ(task->name.len, 0);
    } else {
      ASSERT_TRUE(nodeType(pQuery->pRoot) == QUERY_NODE_START_XNODE_TASK_STMT ||
                  nodeType(pQuery->pRoot) == QUERY_NODE_STOP_XNODE_TASK_STMT);
      const auto* task = reinterpret_cast<const SStartXnodeTaskStmt*>(pQuery->pRoot);
      EXPECT_EQ(task->tid, 123);
      EXPECT_EQ(task->name.len, 0);
    }
  });

  run("START XNODE TASK 123");
  run("STOP XNODE TASK 123");
  run("ALTER XNODE TASK 123 WITH status 'stopped'");
}

TEST_F(ParserXnodeTest, agentNamesUseDecodedLengthAndSqlEscapes) {
  login("root");
  useDb("root", "test");

  struct NameCase {
    string token;
    string expected;
  };
  const string maxName(TSDB_XNODE_AGENT_NAME_LEN, 'a');
  const NameCase nameCases[] = {
      {"'test''agent'", "test'agent"},
      {R"('test\'agent')", "test'agent"},
      {R"('test\\agent')", R"(test\agent)"},
      {"'test''''agent'", "test''agent"},
      {"'" + maxName + "'", maxName},
      {"'" + string(TSDB_XNODE_AGENT_NAME_LEN * 2, '\'') + "'", string(TSDB_XNODE_AGENT_NAME_LEN, '\'')},
  };
  for (const NameCase& nameCase : nameCases) {
    SCOPED_TRACE(nameCase.token);
    setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
      switch (nodeType(pQuery->pRoot)) {
        case QUERY_NODE_CREATE_XNODE_AGENT_STMT: {
          const auto* agent = reinterpret_cast<const SCreateXnodeAgentStmt*>(pQuery->pRoot);
          EXPECT_STREQ(agent->name, nameCase.expected.c_str());
          break;
        }
        case QUERY_NODE_ALTER_XNODE_AGENT_STMT: {
          const auto* agent = reinterpret_cast<const SAlterXnodeAgentStmt*>(pQuery->pRoot);
          ASSERT_NE(agent->name.ptr, nullptr);
          EXPECT_EQ(string(agent->name.ptr, agent->name.len), nameCase.expected);
          break;
        }
        case QUERY_NODE_DROP_XNODE_AGENT_STMT: {
          const auto* agent = reinterpret_cast<const SDropXnodeAgentStmt*>(pQuery->pRoot);
          EXPECT_STREQ(agent->name, nameCase.expected.c_str());
          break;
        }
        case QUERY_NODE_SELECT_STMT:
          checkShowName(pQuery, nameCase.expected);
          break;
        default:
          FAIL() << "Unexpected node type: " << nodeType(pQuery->pRoot);
      }
    });

    run("CREATE XNODE AGENT " + nameCase.token + " WITH status 'running'");
    run("ALTER XNODE AGENT " + nameCase.token + " WITH status 'stopped'");
    run("DROP XNODE AGENT " + nameCase.token);
    run("SHOW XNODE AGENTS WHERE name = " + nameCase.token);
  }
}

TEST_F(ParserXnodeTest, agentNamesRejectEmptyAndDecodedOverflow) {
  login("root");
  useDb("root", "test");

  const string tokens[] = {
      "''",
      "'" + string(TSDB_XNODE_AGENT_NAME_LEN + 1, 'a') + "'",
      "'" + string(TSDB_XNODE_AGENT_NAME_LEN, 'a') + R"(\%')",
  };
  for (const string& token : tokens) {
    run("CREATE XNODE AGENT " + token + " WITH status 'running'", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
    run("ALTER XNODE AGENT " + token + " WITH status 'stopped'", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
    run("DROP XNODE AGENT " + token, TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  }
}

TEST_F(ParserXnodeTest, agentNumericIdsArePreserved) {
  login("root");
  useDb("root", "test");

  setCheckDdlFunc([](const SQuery* pQuery, ParserStage stage) {
    if (nodeType(pQuery->pRoot) == QUERY_NODE_ALTER_XNODE_AGENT_STMT) {
      const auto* agent = reinterpret_cast<const SAlterXnodeAgentStmt*>(pQuery->pRoot);
      EXPECT_EQ(agent->id, 123);
      EXPECT_EQ(agent->name.len, 0);
    } else {
      ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_DROP_XNODE_AGENT_STMT);
      const auto* agent = reinterpret_cast<const SDropXnodeAgentStmt*>(pQuery->pRoot);
      EXPECT_EQ(agent->id, 123);
      EXPECT_EQ(agent->name, nullptr);
    }
  });
  run("ALTER XNODE AGENT 123 WITH status 'stopped'");
  run("DROP XNODE AGENT 123");
}

TEST_F(ParserXnodeTest, restartXnode) {
  login("root");
  useDb("root", "test");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_RESTART_XNODE_STMT);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_RESTART_XNODE);

    SMRestartXnodeReq req = {0};
    ASSERT_EQ(tDeserializeSMRestartXnodeReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
              TSDB_CODE_SUCCESS);
    tFreeSMRestartXnodeReq(&req);
  });

  run("RESTART XNODE");
}

TEST_F(ParserXnodeTest, taskDsnSqlEscapesAreDecoded) {
  login("root");
  useDb("root", "test");

  const string source =
      R"(csv:C:\data?quote="&db_sql=CREATE DATABASE db CACHEMODEL 'none'&separator=\pipe)";
  const string sink = R"(kafka://localhost:9092/?sasl_password=o'reilly&quote="&path=C:\data)";
  bool         create = true;

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (create) {
      ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_XNODE_TASK_STMT);
      ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_CREATE_XNODE_TASK);

      SMCreateXnodeTaskReq req = {0};
      ASSERT_EQ(tDeserializeSMCreateXnodeTaskReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
                TSDB_CODE_SUCCESS);
      EXPECT_EQ(string(req.source.cstr.ptr, req.source.cstr.len), source);
      EXPECT_EQ(string(req.sink.cstr.ptr, req.sink.cstr.len), sink);
      tFreeSMCreateXnodeTaskReq(&req);
    } else {
      ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_UPDATE_XNODE_TASK_STMT);
      ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_UPDATE_XNODE_TASK);

      SMUpdateXnodeTaskReq req = {0};
      ASSERT_EQ(tDeserializeSMUpdateXnodeTaskReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
                TSDB_CODE_SUCCESS);
      EXPECT_EQ(string(req.source.cstr.ptr, req.source.cstr.len), source);
      EXPECT_EQ(string(req.sink.cstr.ptr, req.sink.cstr.len), sink);
      tFreeSMUpdateXnodeTaskReq(&req);
    }
  });

  run(R"(CREATE XNODE TASK 'task' FROM 'csv:C:\\data?quote=\"&db_sql=CREATE DATABASE db CACHEMODEL ''none''&separator=\\pipe' TO 'kafka://localhost:9092/?sasl_password=o''reilly&quote=\"&path=C:\\data')");

  create = false;
  run(R"(ALTER XNODE TASK 1 FROM 'csv:C:\\data?quote=\"&db_sql=CREATE DATABASE db CACHEMODEL ''none''&separator=\\pipe' TO 'kafka://localhost:9092/?sasl_password=o''reilly&quote=\"&path=C:\\data')");
}

TEST_F(ParserXnodeTest, taskTopicIdentifiersArePreserved) {
  login("root");
  useDb("root", "test");

  struct TopicCase {
    string token;
    string expected;
  };
  const TopicCase topicCases[] = {
      {"tp", "tp"}, {"aaaa", "aaaa"}, {"`topic``name`", "topic`name"},
      {R"(`topic\n\tail`)", R"(topic\n\tail)"}};

  for (const TopicCase& topicCase : topicCases) {
    bool create = true;
    setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
      if (create) {
        ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_XNODE_TASK_STMT);
        ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_CREATE_XNODE_TASK);

        SMCreateXnodeTaskReq req = {0};
        ASSERT_EQ(tDeserializeSMCreateXnodeTaskReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
                  TSDB_CODE_SUCCESS);
        EXPECT_EQ(req.source.type, XNODE_TASK_SOURCE_TOPIC);
        EXPECT_EQ(string(req.source.cstr.ptr, req.source.cstr.len), topicCase.expected);
        tFreeSMCreateXnodeTaskReq(&req);
      } else {
        ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_UPDATE_XNODE_TASK_STMT);
        ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_UPDATE_XNODE_TASK);

        SMUpdateXnodeTaskReq req = {0};
        ASSERT_EQ(tDeserializeSMUpdateXnodeTaskReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
                  TSDB_CODE_SUCCESS);
        EXPECT_EQ(req.source.type, XNODE_TASK_SOURCE_TOPIC);
        EXPECT_EQ(string(req.source.cstr.ptr, req.source.cstr.len), topicCase.expected);
        tFreeSMUpdateXnodeTaskReq(&req);
      }
    });

    run("CREATE XNODE TASK 'task' FROM TOPIC " + topicCase.token + " TO 'taos://localhost:6030/db'");

    create = false;
    run("ALTER XNODE TASK 1 FROM TOPIC " + topicCase.token + " TO 'taos://localhost:6030/db'");
  }
}

TEST_F(ParserXnodeTest, taskDatabaseIdentifiersArePreserved) {
  login("root");
  useDb("root", "test");

  struct DatabaseCase {
    string token;
    string expected;
  };
  const DatabaseCase databaseCases[] = {{"db", "db"}, {R"(`db``name\path`)", R"(db`name\path)"}};

  for (const DatabaseCase& databaseCase : databaseCases) {
    bool create = true;
    setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
      if (create) {
        ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_XNODE_TASK_STMT);
        ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_CREATE_XNODE_TASK);

        SMCreateXnodeTaskReq req = {0};
        ASSERT_EQ(tDeserializeSMCreateXnodeTaskReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
                  TSDB_CODE_SUCCESS);
        EXPECT_EQ(req.source.type, XNODE_TASK_SOURCE_DATABASE);
        EXPECT_EQ(req.sink.type, XNODE_TASK_SINK_DATABASE);
        EXPECT_EQ(string(req.source.cstr.ptr, req.source.cstr.len), databaseCase.expected);
        EXPECT_EQ(string(req.sink.cstr.ptr, req.sink.cstr.len), databaseCase.expected);
        tFreeSMCreateXnodeTaskReq(&req);
      } else {
        ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_UPDATE_XNODE_TASK_STMT);
        ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_UPDATE_XNODE_TASK);

        SMUpdateXnodeTaskReq req = {0};
        ASSERT_EQ(tDeserializeSMUpdateXnodeTaskReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req),
                  TSDB_CODE_SUCCESS);
        EXPECT_EQ(req.source.type, XNODE_TASK_SOURCE_DATABASE);
        EXPECT_EQ(req.sink.type, XNODE_TASK_SINK_DATABASE);
        EXPECT_EQ(string(req.source.cstr.ptr, req.source.cstr.len), databaseCase.expected);
        EXPECT_EQ(string(req.sink.cstr.ptr, req.sink.cstr.len), databaseCase.expected);
        tFreeSMUpdateXnodeTaskReq(&req);
      }
    });

    run("CREATE XNODE TASK 'task' FROM DATABASE " + databaseCase.token +
        " TO DATABASE " + databaseCase.token);

    create = false;
    run("ALTER XNODE TASK 1 FROM DATABASE " + databaseCase.token +
        " TO DATABASE " + databaseCase.token);
  }
}

TEST_F(ParserXnodeTest, alterTaskParseErrorsReleaseInputs) {
  login("root");
  useDb("root", "test");

  const string sink(TSDB_XNODE_TASK_SINK_LEN + 1, 'a');
  run("ALTER XNODE TASK 1 FROM 'source' TO '" + sink + "'", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

TEST_F(ParserXnodeTest, alterTaskRejectsImmutableType) {
  login("root");
  useDb("root", "test");
  run("ALTER XNODE TASK 1 WITH TYPE 'replica'", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserXnodeTest, incompleteCreateReleasesSource) {
  login("root");
  useDb("root", "test");

  run("CREATE XNODE TASK 'task' FROM 'source' TO", TSDB_CODE_PAR_INCOMPLETE_SQL,
      PARSER_STAGE_PARSE);
}

TEST_F(ParserXnodeTest, nonTaskResourcesRejectTaskIo) {
  login("root");
  useDb("root", "test");

  const string statements[] = {
      "CREATE XNODE AGENT 'agent' FROM 'source' TO 'sink'",
      "ALTER XNODE AGENT 'agent' FROM 'source' WITH status 'running'",
      "ALTER XNODE JOB 1 TO 'sink' WITH config 'x'",
  };
  for (const string& statement : statements) {
    run(statement, TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  }
}

TEST_F(ParserXnodeTest, emptyTaskIoIsRejected) {
  login("root");
  useDb("root", "test");

  const string statements[] = {
      "CREATE XNODE TASK 'task' FROM '' TO 'sink'",
      "CREATE XNODE TASK 'task' FROM 'source' TO ''",
      "CREATE XNODE TASK 'task' FROM TOPIC `` TO 'sink'",
      "CREATE XNODE TASK 'task' FROM DATABASE `` TO DATABASE db",
      "CREATE XNODE TASK 'task' FROM DATABASE db TO DATABASE ``",
      "ALTER XNODE TASK 1 FROM ''",
      "ALTER XNODE TASK 1 TO ''",
      "ALTER XNODE TASK 1 FROM TOPIC ``",
      "ALTER XNODE TASK 1 FROM DATABASE ``",
      "ALTER XNODE TASK 1 TO DATABASE ``",
  };
  for (const string& statement : statements) {
    run(statement, TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  }
}

TEST_F(ParserXnodeTest, showXnodeDsnOutputModeIsPropagated) {
  login("root");
  useDb("root", "test");

  setCheckDdlFunc([](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_SELECT_STMT);
    ASSERT_EQ(pQuery->xnodeDsnOutputMode, XNODE_DSN_OUTPUT_ENCRYPTED);
  });

  run("SHOW XNODE TASKS WITH ENCRYPTED WHERE id = 1");
  run("show xnode jobs with encrypted where task_id = 1");

  runParseOnly("SHOW XNODE AGENTS WITH ENCRYPTED", TSDB_CODE_PAR_SYNTAX_ERROR);
  runParseOnly("SHOW XNODES WITH ENCRYPTED", TSDB_CODE_PAR_SYNTAX_ERROR);
  runParseOnly("SHOW XNODE TASKS WHERE id = 1 WITH ENCRYPTED", TSDB_CODE_PAR_SYNTAX_ERROR);
}

// parseXnodeDsnOutputMode only recognizes "encrypted"; any other bare identifier after WITH must
// be rejected rather than silently falling back to the default output mode.
TEST_F(ParserXnodeTest, showXnodeRejectsUnknownDsnOutputMode) {
  login("root");
  useDb("root", "test");

  runParseOnly("SHOW XNODE TASKS WITH FOO", TSDB_CODE_PAR_SYNTAX_ERROR);
  runParseOnly("SHOW XNODE TASKS WITH ENCRYPT", TSDB_CODE_PAR_SYNTAX_ERROR);
  runParseOnly("SHOW XNODE TASKS WITH ENCRYPTEDX", TSDB_CODE_PAR_SYNTAX_ERROR);
  runParseOnly("SHOW XNODE TASKS WITH FOO WHERE id = 1", TSDB_CODE_PAR_SYNTAX_ERROR);
  runParseOnly("SHOW XNODE JOBS WITH FOO", TSDB_CODE_PAR_SYNTAX_ERROR);
}

// The default (no WITH clause) path must stay untouched.
TEST_F(ParserXnodeTest, showXnodeDefaultsToPlainDsnOutputMode) {
  login("root");
  useDb("root", "test");

  setCheckDdlFunc([](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;
    ASSERT_EQ(pQuery->xnodeDsnOutputMode, XNODE_DSN_OUTPUT_DEFAULT);
  });

  run("SHOW XNODE TASKS");
  run("SHOW XNODE JOBS WHERE task_id = 1");
  run("SHOW XNODE AGENTS");
  run("SHOW XNODES");
}

TEST_F(ParserXnodeTest, showXnodeDsnOutputModeSurvivesAstClone) {
  login("root");
  useDb("root", "test");

  setCheckDdlFunc([](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;
    ASSERT_EQ(pQuery->xnodeDsnOutputMode, XNODE_DSN_OUTPUT_ENCRYPTED);
  });

  runClonedAst("SHOW XNODE TASKS WITH ENCRYPTED");
}

TEST_F(ParserXnodeTest, createOptionFailuresReleaseInputsOnce) {
  login("root");
  useDb("root", "test");

  const string longTaskName(TSDB_XNODE_TASK_NAME_LEN + 1, 't');
  const string longAgentName(TSDB_XNODE_AGENT_NAME_LEN + 1, 'a');
  const string statements[] = {
      "CREATE XNODE TASK '" + longTaskName +
          "' FROM 'source' TO 'sink' WITH status 'running'",
      "CREATE XNODE AGENT '' WITH status 'running'",
      "CREATE XNODE AGENT '" + longAgentName + "' WITH status 'running'",
  };
  for (const string& statement : statements) {
    run(statement, TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  }
}

}  // namespace ParserTest
