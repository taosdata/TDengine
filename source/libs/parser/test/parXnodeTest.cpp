#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserXnodeTest : public ParserDdlTest {};

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
