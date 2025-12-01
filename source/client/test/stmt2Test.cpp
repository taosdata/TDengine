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
#include <string.h>
#include <atomic>
#include <chrono>
#include <thread>
#include "clientInt.h"
#include "geosWrapper.h"
#include "osSemaphore.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "../inc/clientStmt.h"
#include "../inc/clientStmt2.h"
#include "executor.h"
#include "taos.h"

const char* STMT_STATUS_NAMES[] = {
    "INVALID",   "STMT_INIT",     "STMT_PREPARE",   "STMT_SETTBNAME", "STMT_SETTAGS", "STMT_FETCH_FIELDS",
    "STMT_BIND", "STMT_BIND_COL", "STMT_ADD_BATCH", "STMT_EXECUTE",   "STMT_MAX",
};

namespace {
void checkError(TAOS_STMT2* stmt, int code, const char* file, int line) {
  if (code != TSDB_CODE_SUCCESS) {
    STscStmt2* pStmt = (STscStmt2*)stmt;
    if (pStmt == nullptr || pStmt->sql.sqlStr == nullptr) {
      printf("[STMT2 FAILED]\n  status : %s\n  errcode : %X\n  errstr : %s\n", STMT_STATUS_NAMES[pStmt->sql.status],
             code, taos_stmt2_error(stmt));
    } else {
      printf("[STMT2 FAILED]\n  sql : %s\n  stats : %s\n  errcode : %X\n  errstr : %s\n", pStmt->sql.sqlStr,
             STMT_STATUS_NAMES[pStmt->sql.status], code, taos_stmt2_error(stmt));
    }
    printf("  file : %s\n  line : %d\n", file, line);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
}

typedef struct AsyncArgs {
  int    async_affected_rows;
  tsem_t sem;
} AsyncArgs;

void stmtAsyncQueryCb(void* param, TAOS_RES* pRes, int code) {
  ((AsyncArgs*)param)->async_affected_rows = taos_affected_rows(pRes);
  ASSERT_EQ(tsem_post(&((AsyncArgs*)param)->sem), TSDB_CODE_SUCCESS);
  return;
}

void getFieldsSuccess(TAOS* taos, const char* sql, TAOS_FIELD_ALL* expectedFields, int expectedFieldNum) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, expectedFieldNum);

  for (int i = 0; i < fieldNum; i++) {
    ASSERT_STREQ(pFields[i].name, expectedFields[i].name);
    ASSERT_EQ(pFields[i].type, expectedFields[i].type);
    ASSERT_EQ(pFields[i].field_type, expectedFields[i].field_type);
    ASSERT_EQ(pFields[i].precision, expectedFields[i].precision);
    ASSERT_EQ(pFields[i].bytes, expectedFields[i].bytes);
    ASSERT_EQ(pFields[i].scale, expectedFields[i].scale);
  }
  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

void getFieldsError(TAOS* taos, const char* sql, int errorCode, const char* errorMsg) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, errorCode);
  ASSERT_STREQ(taos_stmt2_error(stmt), errorMsg);
  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

void getQueryFields(TAOS* taos, const char* sql, int expectedFieldNum) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, expectedFieldNum);
  taos_stmt2_free_fields(stmt, NULL);
  taos_stmt2_close(stmt);
}

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  // printf("sql: %s\n", sql);
  int code = taos_errno(result);
  while (code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    result = taos_query(taos, sql);
    code = taos_errno(result);
  }
  if (code != TSDB_CODE_SUCCESS) {
    printf("query failen  sql : %s\n  errstr : %s\n", sql, taos_errstr(result));
    ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS);
  }
  taos_free_result(result);
}

void do_error_query(TAOS* taos, const char* sql, int errorCode) {
  TAOS_RES* result = taos_query(taos, sql);
  // printf("sql: %s\n", sql);
  int code = taos_errno(result);
  while (code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    result = taos_query(taos, sql);
    code = taos_errno(result);
  }
  ASSERT_EQ(code, errorCode);
  taos_free_result(result);
}

void do_stmt(const char* msg, TAOS* taos, TAOS_STMT2_OPTION* option, const char* sql, int CTB_NUMS, int ROW_NUMS,
             int CYC_NUMS, bool hastags, bool createTable) {
  printf("stmt2 [%s] : %s\n", msg, sql);
  do_query(taos, "drop database if exists stmt2_testdb_1");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_1");
  do_query(taos, "create stable stmt2_testdb_1.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

  TAOS_STMT2* stmt = taos_stmt2_init(taos, option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);
  int total_affected = 0;

  // tbname
  char** tbs = (char**)taosMemoryMalloc(CTB_NUMS * sizeof(char*));
  for (int i = 0; i < CTB_NUMS; i++) {
    tbs[i] = (char*)taosMemoryMalloc(sizeof(char) * 20);
    sprintf(tbs[i], "ctb_%d", i);
    if (createTable) {
      char* tmp = (char*)taosMemoryMalloc(sizeof(char) * 100);
      sprintf(tmp, "create table stmt2_testdb_1.%s using stmt2_testdb_1.stb tags(0, 'after')", tbs[i]);
      do_query(taos, tmp);
    }
  }
  for (int r = 0; r < CYC_NUMS; r++) {
    // col params
    int64_t** ts = (int64_t**)taosMemoryMalloc(CTB_NUMS * sizeof(int64_t*));
    char**    b = (char**)taosMemoryMalloc(CTB_NUMS * sizeof(char*));
    int*      ts_len = (int*)taosMemoryMalloc(ROW_NUMS * sizeof(int));
    int*      b_len = (int*)taosMemoryMalloc(ROW_NUMS * sizeof(int));
    for (int i = 0; i < ROW_NUMS; i++) {
      ts_len[i] = sizeof(int64_t);
      b_len[i] = 1;
    }
    for (int i = 0; i < CTB_NUMS; i++) {
      ts[i] = (int64_t*)taosMemoryMalloc(ROW_NUMS * sizeof(int64_t));
      b[i] = (char*)taosMemoryMalloc(ROW_NUMS * sizeof(char));
      for (int j = 0; j < ROW_NUMS; j++) {
        ts[i][j] = 1591060628000 + r * 100000 + j;
        b[i][j] = 'a' + j;
      }
    }
    // tag params
    int t1 = 0;
    int t1len = sizeof(int);
    int t2len = 3;
    //   TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};

    // bind params
    TAOS_STMT2_BIND** paramv = (TAOS_STMT2_BIND**)taosMemoryMalloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
    TAOS_STMT2_BIND** tags = NULL;
    if (hastags) {
      tags = (TAOS_STMT2_BIND**)taosMemoryMalloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
      for (int i = 0; i < CTB_NUMS; i++) {
        // create tags
        tags[i] = (TAOS_STMT2_BIND*)taosMemoryMalloc(2 * sizeof(TAOS_STMT2_BIND));
        tags[i][0] = {TSDB_DATA_TYPE_INT, &t1, &t1len, NULL, 0};
        tags[i][1] = {TSDB_DATA_TYPE_BINARY, (void*)"after", &t2len, NULL, 0};
      }
    }

    for (int i = 0; i < CTB_NUMS; i++) {
      // create col params
      paramv[i] = (TAOS_STMT2_BIND*)taosMemoryMalloc(2 * sizeof(TAOS_STMT2_BIND));
      paramv[i][0] = {TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};
      paramv[i][1] = {TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};
    }
    // bind
    TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, tags, paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    // exec
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    if (option->asyncExecFn == NULL) {
      total_affected += affected;
    } else {
      AsyncArgs* params = (AsyncArgs*)option->userdata;
      code = tsem_wait(&params->sem);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
      total_affected += params->async_affected_rows;
    }

    for (int i = 0; i < CTB_NUMS; i++) {
      if (hastags) {
        taosMemoryFree(tags[i]);
      }
      taosMemoryFree(paramv[i]);
      taosMemoryFree(ts[i]);
      taosMemoryFree(b[i]);
    }
    taosMemoryFree(ts);
    taosMemoryFree(b);
    taosMemoryFree(ts_len);
    taosMemoryFree(b_len);
    taosMemoryFree(paramv);
    if (hastags) {
      taosMemoryFree(tags);
    }
  }
  ASSERT_EQ(total_affected, CYC_NUMS * ROW_NUMS * CTB_NUMS);
  for (int i = 0; i < CTB_NUMS; i++) {
    taosMemoryFree(tbs[i]);
  }
  taosMemoryFree(tbs);

  taos_stmt2_close(stmt);
}

TAOS* getConnWithTz(const char* tz) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != nullptr);
  if (tz != NULL) {
    int code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, tz);
    ASSERT(code == 0);
  }
  return pConn;
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(stmt2Case, timezone) {
  const char* sql = "select * from stmt2_testdb_0.tt where ts = ?";
  // prepare data and check
  {
    TAOS* taos = getConnWithTz("UTC-8");  // Asia/Shanghai timezone
    do_query(taos, "drop database if exists stmt2_testdb_0");
    do_query(taos, "create database IF NOT EXISTS stmt2_testdb_0");
    do_query(taos, "create table stmt2_testdb_0.tt (ts timestamp, val int)");
    do_query(taos, "insert into stmt2_testdb_0.tt values('2025-08-08 08:08:08', 88)");
    do_query(taos, "use stmt2_testdb_0");

    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    // stmt2 with timestamp
    int64_t          ts = 1754611688000;  // '2025-08-08 08:08:08' in Asia/Shanghai timezone
    int32_t          t64_len = sizeof(int64_t);
    TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1};
    TAOS_STMT2_BIND* paramv = &params;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);
    int getRecordCounts = 0;
    while ((taos_fetch_row(pRes))) {
      getRecordCounts++;
    }
    ASSERT_EQ(getRecordCounts, 1);
    taos_stmt2_close(stmt);
    taos_close(taos);
  }

  // stmt2 with time str in Asia/Shanghai timezone
  {
    TAOS* taos = getConnWithTz("UTC-8");
    do_query(taos, "use stmt2_testdb_0");

    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    ASSERT_NE(stmt, nullptr);
    char*            timeStrShanghai = "2025-08-08 08:08:08";
    int32_t          timeStrLen = strlen(timeStrShanghai);
    TAOS_STMT2_BIND  paramsCST = {TSDB_DATA_TYPE_BINARY, timeStrShanghai, &timeStrLen, NULL, 1};
    TAOS_STMT2_BIND* paramvCST = &paramsCST;
    TAOS_STMT2_BINDV bindvCST = {1, NULL, NULL, &paramvCST};
    code = taos_stmt2_bind_param(stmt, &bindvCST, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);
    int getRecordCounts = 0;
    while ((taos_fetch_row(pRes))) {
      getRecordCounts++;
    }
    ASSERT_EQ(getRecordCounts, 1);
  }

  // stmt2 wiht time str in UTC timezone
  {
    TAOS* taos = getConnWithTz("UTC+0");
    do_query(taos, "use stmt2_testdb_0");

    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            timeStrUTC = "2025-08-08 00:08:08";  // '2025-08-08 08:08:08+8' in UTC timezone
    int32_t          timeStrUTCLen = strlen(timeStrUTC);
    TAOS_STMT2_BIND  paramsUTC = {TSDB_DATA_TYPE_BINARY, timeStrUTC, &timeStrUTCLen, NULL, 1};
    TAOS_STMT2_BIND* paramvUTC = &paramsUTC;
    TAOS_STMT2_BINDV bindvUTC = {1, NULL, NULL, &paramvUTC};
    code = taos_stmt2_bind_param(stmt, &bindvUTC, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);
    int getRecordCounts = 0;
    while ((taos_fetch_row(pRes))) {
      getRecordCounts++;
    }
    ASSERT_EQ(getRecordCounts, 1);

    taos_stmt2_close(stmt);
    do_query(taos, "drop database if exists stmt2_testdb_0");
    taos_close(taos);
  }
}

TEST(stmt2Case, not_support_vtable) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_37");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_37");
  do_query(taos,
           "CREATE STABLE stmt2_testdb_37.meters_v (ts timestamp,current float, voltage int, phase float) "
           "TAGS(location varchar(64), "
           "group_id int) VIRTUAL 1;");
  do_query(taos, "use stmt2_testdb_37");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql = "select * from stmt2_testdb_37.meters_v where ts > ? and ts < ? limit ?";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int              t64_len[1] = {sizeof(int64_t)};
  int              b_len[1] = {3};
  int              x = 2;
  int              x_len = sizeof(int);
  int64_t          ts[2] = {1591060627000, 1591060628005};
  TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], t64_len, NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &ts[1], t64_len, NULL, 1},
                                {TSDB_DATA_TYPE_INT, &x, &x_len, NULL, 1}};
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  ASSERT_EQ(code, TSDB_CODE_VTABLE_NOT_SUPPORT_STMT);

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_test_limit) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_7");
  do_query(taos, "create stable stmt2_testdb_7.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
           "insert into stmt2_testdb_7.tb2 using stmt2_testdb_7.stb tags(2,'xyz') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628004, 'hij')");
  do_query(taos, "use stmt2_testdb_7");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql = "select * from stmt2_testdb_7.tb2 where ts > ? and ts < ? limit ?";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, 3);

  int              t64_len[1] = {sizeof(int64_t)};
  int              b_len[1] = {3};
  int              x = 2;
  int              x_len = sizeof(int);
  int64_t          ts[2] = {1591060627000, 1591060628005};
  TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], t64_len, NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &ts[1], t64_len, NULL, 1},
                                {TSDB_DATA_TYPE_INT, &x, &x_len, NULL, 1}};
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);

  int getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  taos_close(taos);
}

TEST(stmt2Case, insert_stb_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_2");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_2 PRECISION 'ns'");
  do_query(taos,
           "create stable stmt2_testdb_2.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(
      taos,
      "create stable if not exists stmt2_testdb_2.all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 "
      "bigint, "
      "v6 tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");
  printf("support case \n");

  // case 1 : test super table
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  {
    // case 2 : no tag
    const char*    sql = "insert into stmt2_testdb_2.stb(ts,b,tbname) values(?,?,?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 3 : random order
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(tbname,ts,t2,b,t1) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 4 : random order 2
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(ts,tbname,b,t2,t1) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG}};
    printf("case 4 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 5 : 'db'.'stb'
  {
    const char*    sql = "insert into 'stmt2_testdb_2'.'stb'(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 5 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 6 : use db
  {
    do_query(taos, "use stmt2_testdb_2");
    const char*    sql = "insert into stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 6 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 7 : less param
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(ts,tbname) values(?,?)";
    TAOS_FIELD_ALL expectedFields[2] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 7 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 2);
  }

  // case 8 : test all types
  {
    const char* sql =
        "insert into "
        "all_stb(tbname,tts,tv1,tv2,tv3,tv4,tv5,tv6,tv7,tv8,tv9,tv10,tv11,tv12,tv13,tv14,tv15,ts,v1,v2,v3,v4,v5,v6,v7,"
        "v8,v9,v10,"
        "v11,v12,v13,v14,v15) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[33] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                         {"tts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_TAG},
                                         {"tv1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_TAG},
                                         {"tv15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_TAG},
                                         {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                         {"v1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_COL},
                                         {"v15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_COL}};
    printf("case 8 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 33);
  }

  // case 9 : fixed value
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(t1,t2,ts,b,tbname) values(1,?,?,'abc',?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 9 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // not support case
  printf("not support case \n");

  // case 2 : no pk
  {
    const char* sql = "insert into stmt2_testdb_2.stb(b,tbname) values(?,?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION, "Primary timestamp column should not be null");
  }

  // case 3 : no tbname and tag(not support bind)
  {
    const char* sql = "insert into stmt2_testdb_2.stb(ts,b) values(?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION, "tbname column should not be null");
  }

  // case 4 : no col and tag(not support bind)
  {
    const char* sql = "insert into stmt2_testdb_2.stb(tbname) values(?)";
    printf("case 4 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION, "Primary timestamp column should not be null");
  }

  // case 5 : no field name
  {
    const char* sql = "insert into stmt2_testdb_2.stb values(?,?,?,?,?)";
    printf("case 5 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR,
                   "syntax error near ' values(?,?,?,?,?)' ((...tbname, ts...) bounded cols is expected for supertable "
                   "insertion)");
  }

  // case 6 :  test super table not exist
  {
    const char* sql = "insert into stmt2_testdb_2.nstb values(?,?,?,?,?)";
    printf("case 6 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_STMT_TBNAME_ERROR, "Table does not exist");
  }

  // case 7 :  no col
  {
    const char* sql = "insert into stmt2_testdb_2.stb(t1,t2,tbname) values(?,?,?)";
    printf("case 7 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION, "Primary timestamp column should not be null");
  }

  // case 8 :   wrong para nums
  {
    const char* sql = "insert into stmt2_testdb_2.stb(ts,b,tbname) values(?,?,?,?,?)";
    printf("case 8 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of columns");
  }

  // case 9 :   wrong simbol
  {
    const char* sql = "insert into stmt2_testdb_2.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)";
    printf("case 9 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of columns");
  }

  do_query(taos, "drop database if exists stmt2_testdb_2");
  taos_close(taos);
}

TEST(stmt2Case, insert_ctb_using_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_3");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_3 PRECISION 'ns'");
  do_query(taos,
           "create stable stmt2_testdb_3.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(
      taos,
      "create stable if not exists stmt2_testdb_3.all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 "
      "bigint, "
      "v6 tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");
  do_query(taos, "CREATE TABLE stmt2_testdb_3.t0 USING stmt2_testdb_3.stb (t1,t2) TAGS (7,'Cali');");

  printf("support case \n");
  // case 1 : test child table already exist
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.t0(ts,b)using stmt2_testdb_3.stb (t1,t2) TAGS(?,?) VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 2 : insert clause
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.? using stmt2_testdb_3.stb (t1,t2) TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 3 : insert child table not exist
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.d1 using stmt2_testdb_3.stb (t1,t2)TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 4 : random order
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.? using stmt2_testdb_3.stb (t2,t1)TAGS(?,?) (b,ts)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 4 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 5 : less para
  {
    const char*    sql = "insert into stmt2_testdb_3.? using stmt2_testdb_3.stb (t2)tags(?) (ts)values(?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 5 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 6 : insert into db.? using db.stb tags(?, ?) values(?,?)
  // no field name
  {
    const char*    sql = "insert into stmt2_testdb_3.? using stmt2_testdb_3.stb tags(?, ?) values(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 6 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 7 : insert into db.d0 (ts)values(?)
  //  less para
  {
    const char*    sql = "insert into stmt2_testdb_3.t0 (ts)values(?)";
    TAOS_FIELD_ALL expectedFields[1] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 7 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 1);
  }

  // case 8 : 'db' 'stb'
  {
    const char* sql = "INSERT INTO 'stmt2_testdb_3'.? using 'stmt2_testdb_3'.'stb' (t1,t2) TAGS(?,?)(ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 8 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 9 : use db
  {
    do_query(taos, "use stmt2_testdb_3");
    const char*    sql = "INSERT INTO ? using stb (t1,t2) TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 9 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }
  // case 11: TD-34097
  {
    do_query(taos, "use stmt2_testdb_3");
    const char*    sql = "INSERT INTO ? using stb (t1,t2) TAGS(1,'abc') (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 11 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 10 : test all types
  {
    do_query(taos, "use stmt2_testdb_3");
    const char* sql =
        "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[33] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                         {"tts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_TAG},
                                         {"tv1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_TAG},
                                         {"tv15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_TAG},
                                         {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                         {"v1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_COL},
                                         {"v15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_COL}};
    printf("case 10 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 33);
  }

  // case 12 : fixed value
  {
    do_query(taos, "use stmt2_testdb_3");
    const char*    sql = "INSERT INTO ? using stb (t1,t2) TAGS(1,?) (ts,b)VALUES(?,'abc')";
    TAOS_FIELD_ALL expectedFields[3] = {
        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
    };
    printf("case 12 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }
  // case 13 : mix value and ?
  {
    do_query(taos, "use stmt2_testdb_3");
    const char*    sql = "INSERT INTO ? using stb (t1,t2) TAGS(?,?) (ts,b)VALUES(15910606280001,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 13 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }
  // case 14 : mix value and ?
  {
    do_query(taos, "use stmt2_testdb_3");
    const char*    sql = "INSERT INTO ? using stb (t1,t2) TAGS(?,?) (ts,b)VALUES(15910606280001,'abc')";
    TAOS_FIELD_ALL expectedFields[3] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG}};
    printf("case 13 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  printf("not support case \n");

  // case 1 : test super table not exist
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.? using stmt2_testdb_3.nstb (t1,t2) TAGS(?,?) (ts,b)VALUES (?,?)";
    printf("case 1 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_STMT_TBNAME_ERROR, "Table does not exist");
  }

  // case 2 : no pk
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.? using stmt2_testdb_3.stb (t1,t2) TAGS(?,?) (b)VALUES (?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION, "Primary timestamp column should not be null");
  }

  // case 3 : less param and no filed name
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.? using stmt2_testdb_3.stb TAGS(?)VALUES (?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR,
                   "syntax error near ')values (?,?)' (invalid data or symbol)");
  }

  // case 4 :  no tags for ctbname
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.d0 using stmt2_testdb_3.stb values(?,?)";
    printf("case 4 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR, "syntax error near 'values(?,?)' (TAGS is expected)");
  }

  // case 5 :  wrong placeholder
  {
    const char* sql = "insert into ! using stmt2_testdb_3.stb tags(?, ?) values(?,?)";
    printf("case 5 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR,
                   "syntax error near '! using stmt2_testdb_3.stb tags(?, ?) values(?,?)' (table_name is expected)");
  }
  do_query(taos, "drop database if exists stmt2_testdb_3");
  taos_close(taos);
}

TEST(stmt2Case, insert_ntb_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_4");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_4 PRECISION 'ms'");
  do_query(taos, "CREATE TABLE stmt2_testdb_4.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");
  do_query(
      taos,
      "create table if not exists stmt2_testdb_4.all_ntb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 "
      "bigint, v6 tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 "
      "double, v12 binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20));");

  printf("support case \n");

  // case 1 : test normal table no field name
  {
    const char*    sql = "INSERT INTO stmt2_testdb_4.ntb VALUES(?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL},
                                        {"nb", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"nvc", TSDB_DATA_TYPE_BINARY, 0, 0, 18, TAOS_FIELD_COL},
                                        {"ni", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 2 : test random order
  {
    const char*    sql = "INSERT INTO stmt2_testdb_4.ntb (ni,nb,nvc,nts)VALUES(?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"ni", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                        {"nb", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"nvc", TSDB_DATA_TYPE_BINARY, 0, 0, 18, TAOS_FIELD_COL},
                                        {"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 3 : less param
  {
    const char*    sql = "INSERT INTO stmt2_testdb_4.ntb (nts)VALUES(?)";
    TAOS_FIELD_ALL expectedFields[1] = {{"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 1);
  }

  // case 4 : test all types
  {
    const char*    sql = "insert into stmt2_testdb_4.all_ntb values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[16] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_COL},
                                         {"v15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_COL}};
    printf("case 4 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 16);
  }

  printf("not support case \n");

  // case 1 :  wrong db
  {
    const char* sql = "insert into ntb values(?,?,?,?)";
    printf("case 1 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION, "db is not specified");
  }

  // case 2 :  normal table must have tbnam
  {
    const char* sql = "insert into stmt2_testdb_4.? values(?,?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_STMT_TBNAME_ERROR, "Table does not exist");
  }

  // case 3 :  wrong para nums
  {
    const char* sql = "insert into stmt2_testdb_4.ntb(nts,ni) values(?,?,?,?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM, "Illegal number of columns");
  }

  do_query(taos, "drop database if exists stmt2_testdb_4");
  taos_close(taos);
}

TEST(stmt2Case, select_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_5");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_5 PRECISION 'ns'");
  do_query(taos, "use stmt2_testdb_5");
  do_query(taos, "CREATE TABLE stmt2_testdb_5.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");
  {
    // case 1 :
    const char* sql = "select * from ntb where ts = ?";
    printf("case 1 : %s\n", sql);
    getQueryFields(taos, sql, 1);
  }

  {
    // case 2 :
    const char* sql = "select * from ntb where ts = ? and b = ?";
    printf("case 2 : %s\n", sql);
    getQueryFields(taos, sql, 2);
  }

  do_query(taos, "drop database if exists stmt2_testdb_5");
  taos_close(taos);
}

TEST(stmt2Case, get_fields_error_Test) {
  // case 1 :
  {
    printf("case 1 : NULL param \n");
    int code = taos_stmt2_get_fields(NULL, NULL, NULL);
    ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  }
}

TEST(stmt2Case, stmt2_init_prepare_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  {
    (void)taos_stmt2_init(NULL, NULL);
    ASSERT_EQ(terrno, TSDB_CODE_INVALID_PARA);
    terrno = 0;
  }

  {
    (void)taos_stmt2_prepare(NULL, NULL, 0);
    ASSERT_EQ(terrno, TSDB_CODE_INVALID_PARA);
    terrno = 0;
  }

  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "wrong sql", 0);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(((STscStmt2*)stmt)->db, nullptr);
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "insert into 'stmt2_testdb_5'.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)", 0);
    ASSERT_NE(stmt, nullptr);
    ASSERT_STREQ(((STscStmt2*)stmt)->db, "stmt2_testdb_5");  // add in main TD-33332
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2_OPTION option = {0, true, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    taos_stmt2_close(stmt);
  }
  taos_close(taos);
}

TEST(stmt2Case, stmt2_stb_insert) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  // normal
  TAOS_STMT2_OPTION option = {0, false, true, NULL, NULL};
  {
    do_stmt("no-interlcace", taos, &option, "insert into `stmt2_testdb_1`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)",
            3, 3, 3, true, true);
  }
  {
    do_stmt("no-interlcace", taos, &option,
            "insert into `stmt2_testdb_1`.? using `stmt2_testdb_1`.`stb` tags(?,?) values(?,?)", 3, 3, 3, true, true);
  }

  // async
  AsyncArgs* aa = (AsyncArgs*)taosMemMalloc(sizeof(AsyncArgs));
  aa->async_affected_rows = 0;
  ASSERT_EQ(tsem_init(&aa->sem, 0, 0), TSDB_CODE_SUCCESS);
  void* param = aa;
  option = {0, false, true, stmtAsyncQueryCb, param};
  {
    do_stmt("no-interlcace & aync exec", taos, &option,
            "insert into stmt2_testdb_1.stb (ts,b,tbname,t1,t2) values(?,?,?,?,?)", 3, 3, 3, true, true);
  }
  {
    do_stmt("no-interlcace & aync exec", taos, &option,
            "insert into stmt2_testdb_1.? using stmt2_testdb_1.stb (t1,t2)tags(?,?) (ts,b)values(?,?)", 3, 3, 3, true,
            true);
  }
  // TD-34123 : interlace=0 with fixed tags
  {
    do_stmt("no-interlcace & aync exec", taos, &option,
            "insert into `stmt2_testdb_1`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)", 3, 3, 3, false, true);
  }

  // interlace = 0 & use db]
  do_query(taos, "use stmt2_testdb_1");
  option = {0, false, false, NULL, NULL};
  {
    do_stmt("no-interlcace & no-db", taos, &option, "insert into stb (tbname,ts,b) values(?,?,?)", 3, 3, 3, false,
            true);
  }
  {
    do_stmt("no-interlcace & no-db", taos, &option, "insert into ? using stb (t1,t2)tags(?,?) (ts,b)values(?,?)", 3, 3,
            3, true, true);
  }
  {
    do_stmt("no-interlcace & no-db", taos, &option, "insert into ? values(?,?)", 3, 3, 3, false, true);
  }

  // interlace = 1
  option = {0, true, true, stmtAsyncQueryCb, param};
  {
    do_stmt("interlcace & preCreateTB", taos, &option, "insert into ? values(?,?)", 3, 3, 3, false, true);
  }
  option = {0, true, true, NULL, NULL};
  {
    do_stmt("interlcace & preCreateTB", taos, &option, "insert into ? values(?,?)", 3, 3, 3, false, true);
  }

  //  auto create table
  // interlace = 1
  option = {0, true, true, NULL, NULL};
  {
    do_stmt("interlcace & no-preCreateTB", taos, &option, "insert into ? using stb (t1,t2)tags(?,?) (ts,b)values(?,?)",
            3, 3, 3, true, false);
  }
  {
    do_stmt("interlcace & no-preCreateTB", taos, &option,
            "insert into stmt2_testdb_1.? using stb (t1,t2)tags(1,'abc') (ts,b)values(?,?)", 3, 3, 3, false, false);
  }
  {
    do_stmt("interlcace & no-preCreateTB", taos, &option,
            "insert into stmt2_testdb_1.stb (ts,b,tbname,t1,t2) values(?,?,?,?,?)", 3, 3, 3, true, false);
  }
  // interlace = 0
  option = {0, false, false, NULL, NULL};
  {
    do_stmt("no-interlcace & no-preCreateTB", taos, &option,
            "insert into ? using stb (t1,t2)tags(?,?) (ts,b)values(?,?)", 3, 3, 3, true, false);
  }
  {
    do_stmt("no-interlcace & no-preCreateTB", taos, &option,
            "insert into stmt2_testdb_1.stb (ts,b,tbname,t1,t2) values(?,?,?,?,?)", 3, 3, 3, true, false);
  }

  do_query(taos, "drop database if exists stmt2_testdb_1");
  (void)tsem_destroy(&aa->sem);
  taosMemFree(aa);
  taos_close(taos);
}

// TD-33417
// TS-6515
// TD-35141
// TS-6798
TEST(stmt2Case, stmt2_insert_non_statndard) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_6");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_6");
  do_query(taos,
           "create stable stmt2_testdb_6.stb1  (ts timestamp, int_col int,long_col bigint,double_col "
           "double,bool_col bool,binary_col binary(1),nchar_col nchar(20),varbinary_col varbinary(20),geometry_col "
           "geometry(200)) tags(int_tag int,long_tag bigint,double_tag double,bool_tag bool,binary_tag "
           "binary(20),nchar_tag nchar(20),varbinary_tag varbinary(20),geometry_tag geometry(200));");
  do_query(taos, "use stmt2_testdb_6");
  do_error_query(taos, "INSERT INTO stmt2_testdb_6.stb1 (tbname,ts)VALUES (?,?)", TSDB_CODE_TSC_INVALID_OPERATION);

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // less cols and tags using stb
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql = "INSERT INTO stmt2_testdb_6.? using stmt2_testdb_6.stb1 (int_tag)tags(1) (ts)  VALUES (?)";
    printf("stmt2 [%s] : %s\n", "less params", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    // test get fields
    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 2);
    ASSERT_STREQ(pFields[0].name, "tbname");
    ASSERT_STREQ(pFields[1].name, "ts");

    int total_affect_rows = 0;

    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int64_t ts[2] = {1591060628000, 1591060628100};
    for (int i = 0; i < 3; i++) {
      ts[0] += 1000;
      ts[1] += 1000;

      TAOS_STMT2_BIND tags1 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
      TAOS_STMT2_BIND tags2 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
      TAOS_STMT2_BIND params1 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};
      TAOS_STMT2_BIND params2 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};

      TAOS_STMT2_BIND* tagv[2] = {&tags1, &tags2};
      TAOS_STMT2_BIND* paramv[2] = {&params1, &params2};
      char*            tbname[2] = {"tb1", "tb2"};
      TAOS_STMT2_BINDV bindv = {2, &tbname[0], tagv, &paramv[0]};
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
      checkError(stmt, code, __FILE__, __LINE__);
      ASSERT_EQ(fieldNum, 2);
      ASSERT_STREQ(pFields[0].name, "tbname");
      ASSERT_STREQ(pFields[1].name, "ts");

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      total_affect_rows += affected_rows;
      checkError(stmt, code, __FILE__, __LINE__);

      code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
      checkError(stmt, code, __FILE__, __LINE__);
      ASSERT_EQ(fieldNum, 2);
      ASSERT_STREQ(pFields[0].name, "tbname");
      ASSERT_STREQ(pFields[1].name, "ts");
    }

    ASSERT_EQ(total_affect_rows, 12);
    taos_stmt2_close(stmt);
  }

  // less cols and tags
  {
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
    ASSERT_NE(stmt, nullptr);
    const char* sql = "INSERT INTO stmt2_testdb_6.stb1 (ts,int_tag,tbname)  VALUES (?,?,?)";
    printf("stmt2 [%s] : %s\n", "less params", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    int total_affect_rows = 0;

    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int64_t ts[2] = {1591060628000, 1591060628100};
    for (int i = 0; i < 3; i++) {
      ts[0] += 1000;
      ts[1] += 1000;

      TAOS_STMT2_BIND tags1 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
      TAOS_STMT2_BIND tags2 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
      TAOS_STMT2_BIND params1 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};
      TAOS_STMT2_BIND params2 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};

      TAOS_STMT2_BIND* tagv[2] = {&tags1, &tags2};
      TAOS_STMT2_BIND* paramv[2] = {&params1, &params2};
      char*            tbname[2] = {"tb1", "tb2"};
      TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      total_affect_rows += affected_rows;

      checkError(stmt, code, __FILE__, __LINE__);
    }

    ASSERT_EQ(total_affect_rows, 12);
    taos_stmt2_close(stmt);
  }

  // disorder cols and tags
  {
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    do_query(taos,
             "INSERT INTO stmt2_testdb_6.stb1 (ts, int_tag, tbname)  VALUES (1591060627000, 5, 'tb5')(1591060627000, "
             "6,'tb6') ");
    const char* sql = "INSERT INTO stmt2_testdb_6.stb1 (binary_tag,int_col,tbname,ts,int_tag)  VALUES (?,?,?,?,?)";
    printf("stmt2 [%s] : %s\n", "disorder params", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int     tag_bl = 3;
    int64_t ts[2] = {1591060628000, 1591060628100};
    int64_t ts_2[2] = {1591060628800, 1591060628900};
    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     coli[2] = {1, 2};
    int     coli_2[2] = {3, 4};
    int     ilen[2] = {sizeof(int), sizeof(int)};
    int     total_affect_rows = 0;
    for (int i = 0; i < 3; i++) {
      ts[0] += 1000;
      ts[1] += 1000;
      ts_2[0] += 1000;
      ts_2[1] += 1000;

      TAOS_STMT2_BIND tags[2][2] = {
          {{TSDB_DATA_TYPE_BINARY, (void*)"abc", &tag_bl, NULL, 1}, {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1}},
          {{TSDB_DATA_TYPE_BINARY, (void*)"def", &tag_bl, NULL, 1}, {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1}}};
      TAOS_STMT2_BIND params[2][2] = {
          {{TSDB_DATA_TYPE_INT, &coli[0], &ilen[0], NULL, 2}, {TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2}},
          {{TSDB_DATA_TYPE_INT, &coli_2[0], &ilen[0], NULL, 2},
           {TSDB_DATA_TYPE_TIMESTAMP, &ts_2[0], &t64_len[0], NULL, 2}}};

      TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};
      TAOS_STMT2_BIND* paramv[2] = {&params[0][0], &params[1][0]};
      char*            tbname[2] = {"tb5", "tb6"};
      TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      total_affect_rows += affected_rows;
      checkError(stmt, code, __FILE__, __LINE__);
    }
    ASSERT_EQ(total_affect_rows, 12);
    taos_stmt2_close(stmt);
  }

  // pk error
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql =
        "INSERT INTO stmt2_testdb_6.? using  stmt2_testdb_6.stb1 (int_tag)tags(1) (int_col,ts)VALUES (?,?)";
    printf("stmt2 [%s] : %s\n", "PK error", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int     tag_bl = 3;
    int64_t ts[2] = {1591060628000, NULL};
    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     coli[2] = {1, 2};
    int     ilen[2] = {sizeof(int), sizeof(int)};
    int     total_affect_rows = 0;
    char    is_null[2] = {1, 1};

    TAOS_STMT2_BIND params1[2] = {{TSDB_DATA_TYPE_INT, &coli, &ilen[0], is_null, 2},
                                  {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], is_null, 2}};

    TAOS_STMT2_BIND* paramv = &params1[0];
    char*            tbname = "tb3";
    TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL);

    taos_stmt2_close(stmt);
  }
  // TD-34123 disorder pk ts
  {
    do_query(taos, "create stable stmt2_testdb_6.stb2  (ts timestamp, int_col int PRIMARY KEY) tags(int_tag int);");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql =
        "INSERT INTO stmt2_testdb_6.? using  stmt2_testdb_6.stb2 (int_tag)tags(1) (ts,int_col)VALUES (?,?)";
    printf("stmt2 [%s] : %s\n", "disorder pk ts", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int     tag_bl = 3;
    int64_t ts[5] = {1591060628003, 1591060628002, 1591060628002, 1591060628002, 1591060628001};
    int     t64_len[5] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
    int     coli[5] = {1, 4, 4, 3, 2};
    int     ilen[5] = {sizeof(int), sizeof(int), sizeof(int), sizeof(int), sizeof(int)};
    char    is_null[2] = {1, 1};

    TAOS_STMT2_BIND params1[2] = {
        {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 5},
        {TSDB_DATA_TYPE_INT, &coli, &ilen[0], NULL, 5},
    };

    TAOS_STMT2_BIND* paramv = &params1[0];
    char*            tbname = "tb3";
    TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);

    int64_t         ts2[2] = {1591060628003, 1591060628002};
    int             t64_len2[2] = {sizeof(int64_t), sizeof(int64_t)};
    int             coli2[2] = {1, 2};
    int             ilen2[2] = {sizeof(int), sizeof(int)};
    TAOS_STMT2_BIND params2[2] = {
        {TSDB_DATA_TYPE_TIMESTAMP, &ts2, &t64_len2[0], NULL, 2},
        {TSDB_DATA_TYPE_INT, &coli2, &ilen2[0], NULL, 2},
    };
    TAOS_STMT2_BIND* paramv2 = &params2[0];
    TAOS_STMT2_BINDV bindv2 = {1, &tbname, NULL, &paramv2};
    code = taos_stmt2_bind_param(stmt, &bindv2, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows2;
    taos_stmt2_exec(stmt, &affected_rows2);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows2, 2);

    taos_stmt2_close(stmt);
  }

  // get fields insert into ? valuse
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    do_query(taos, "create table stmt2_testdb_6.ntb(ts timestamp, b binary(10))");
    do_query(taos, "use stmt2_testdb_6");
    const char* sql = "INSERT INTO ? VALUES (?,?)";
    printf("stmt2 [%s] : %s\n", "get fields", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            tbname = "ntb";
    TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, NULL};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, 0);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 3);
    ASSERT_STREQ(pFields[0].name, "tbname");
    ASSERT_STREQ(pFields[1].name, "ts");
    ASSERT_STREQ(pFields[2].name, "b");

    taos_stmt2_close(stmt);
  }

  // null sql
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql = "";
    printf("stmt2 [%s] : %s\n", "null sql", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);
    ASSERT_STREQ(taos_stmt2_error(stmt), "stmt only support 'SELECT' or 'INSERT'");

    taos_stmt2_close(stmt);
  }

  // TS-6798 alter schema
  {
    do_query(taos, "alter stable stmt2_testdb_6.stb1 DROP COLUMN int_col");
    do_query(taos, "alter stable stmt2_testdb_6.stb1 ADD COLUMN int_col_new int");
    do_query(taos, "alter stable stmt2_testdb_6.stb1 MODIFY COLUMN binary_col binary(10)");

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql =
        "INSERT INTO stmt2_testdb_6.? using stmt2_testdb_6.stb1 "
        "(int_tag,binary_tag)tags(?,?) "
        "(ts,varbinary_col,binary_col,int_col_new)VALUES (?,?,?,?)";
    printf("stmt2 [%s] : %s\n", "alter schema", sql);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 7);

    int total_affect_rows = 0;

    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int64_t ts[2] = {1591060628000, 1591060628100};
    for (int i = 0; i < 3; i++) {
      ts[0] += 1000;
      ts[1] += 1000;

      int  tag_i = 1 + i;
      int  tag_l = sizeof(int);
      int  tag_bl = 3;
      char tag_binary[2][4] = {"abc", "def"};

      int   coli[2] = {100 + i, 200 + i};
      int   ilen[2] = {sizeof(int), sizeof(int)};
      bool  bool_val[2] = {true, false};
      int   bool_len[2] = {sizeof(bool), sizeof(bool)};
      char* varbinary_val = "xyzuvw";
      int   varbinary_len[2] = {3, 3};

      TAOS_STMT2_BIND tags[2][2] = {
          {{TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1}, {TSDB_DATA_TYPE_BINARY, tag_binary[0], &tag_bl, NULL, 1}},
          {{TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1}, {TSDB_DATA_TYPE_BINARY, tag_binary[1], &tag_bl, NULL, 1}}};

      TAOS_STMT2_BIND params[2][4] = {{{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                                       {TSDB_DATA_TYPE_VARBINARY, &varbinary_val[0], &varbinary_len[0], NULL, 2},
                                       {TSDB_DATA_TYPE_BINARY, &varbinary_val[0], &varbinary_len[0], NULL, 2},
                                       {TSDB_DATA_TYPE_INT, &coli[0], &ilen[0], NULL, 2}},
                                      {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                                       {TSDB_DATA_TYPE_VARBINARY, &varbinary_val[0], &varbinary_len[0], NULL, 2},
                                       {TSDB_DATA_TYPE_BINARY, &varbinary_val[0], &varbinary_len[0], NULL, 2},
                                       {TSDB_DATA_TYPE_INT, &coli[0], &ilen[0], NULL, 2}}};

      TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};
      TAOS_STMT2_BIND* paramv[2] = {&params[0][0], &params[1][0]};
      char*            tbname[2] = {"tb1", "tb2"};

      TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      int affected_rows;
      code = taos_stmt2_exec(stmt, &affected_rows);
      checkError(stmt, code, __FILE__, __LINE__);
      total_affect_rows += affected_rows;
    }

    ASSERT_EQ(total_affect_rows, 12);
    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_6");
  taos_close(taos);
}

void asyncInsertDB(void* param, TAOS_RES* pRes, int code) {
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  return;
}

// TD-33419
// TD-34075
// TD-34504
TEST(stmt2Case, stmt2_insert_db) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_12");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_12");
  do_query(taos, "create stable `stmt2_testdb_12`.`stb1`(ts timestamp, int_col int) tags(int_tag int)");
  do_query(taos,
           "INSERT INTO `stmt2_testdb_12`.`stb1` (ts,int_tag,tbname)  VALUES "
           "(1591060627000,1,'tb1')(1591060627000,2,'tb2')");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // test 1
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  const char* sql = "INSERT INTO `stmt2_testdb_12`.`stb1` (ts,int_tag,tbname)  VALUES (?,?,?)";

  int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  int     tag_i = 0;
  int     tag_l = sizeof(int);
  int64_t ts[2] = {1591060628000, 1591060628100};
  int     total_affect_rows = 0;

  TAOS_STMT2_BIND tags1 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
  TAOS_STMT2_BIND tags2 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
  TAOS_STMT2_BIND params1 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};
  TAOS_STMT2_BIND params2 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};

  TAOS_STMT2_BIND* tagv[2] = {&tags1, &tags2};
  TAOS_STMT2_BIND* paramv[2] = {&params1, &params2};
  char*            tbname[2] = {"tb1", "tb2"};
  TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};

  for (int i = 0; i < 3; i++) {
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    ts[0] += 1000;
    ts[1] += 1000;

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    total_affect_rows += affected_rows;
    checkError(stmt, code, __FILE__, __LINE__);
  }

  ASSERT_EQ(total_affect_rows, 12);
  taos_stmt2_close(stmt);

  // test 2
  total_affect_rows = 0;
  option = {0, false, false, NULL, NULL};
  do_query(taos, "use stmt2_testdb_12");
  stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  sql = "INSERT INTO stb1 (ts,int_tag,tbname)  VALUES (?,?,?)";

  for (int i = 0; i < 3; i++) {
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    ts[0] += 1000;
    ts[1] += 1000;

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    taos_stmt2_exec(stmt, &affected_rows);
    total_affect_rows += affected_rows;
    checkError(stmt, code, __FILE__, __LINE__);
  }

  ASSERT_EQ(total_affect_rows, 12);
  taos_stmt2_close(stmt);

  // test 3
  option = {0, true, true, asyncInsertDB, NULL};
  do_query(taos, "use stmt2_testdb_12");
  stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  sql = "INSERT INTO stb1 (ts,int_tag,tbname)  VALUES (?,?,?)";

  for (int i = 0; i < 3; i++) {
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    ts[0] += 1000;
    ts[1] += 1000;

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
  }

  taos_stmt2_close(stmt);

  do_query(taos, "drop database if exists stmt2_testdb_12");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_insert_duplicate) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_18");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_18");
  do_query(taos, "create stable `stmt2_testdb_18`.`stb1`(ts timestamp, int_col int) tags(int_tag int)");
  do_query(taos, "create stable `stmt2_testdb_18`.`stb2`(ts timestamp, int_col int) tags(int_tag int)");
  do_query(taos, "create table `stmt2_testdb_18`.`tb1` using `stmt2_testdb_18`.`stb1` (int_tag)tags(1)");
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // test 1
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  const char* sql = "INSERT INTO `stmt2_testdb_18`.`stb1` (ts,int_col,int_tag,tbname)  VALUES (?,?,?,?)";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  int     tag_i[2] = {1, 2};
  int     tag_l[2] = {sizeof(int), sizeof(int)};
  int64_t ts[2] = {1591060628000, 1591060629000};
  int     total_affect_rows = 0;

  TAOS_STMT2_BIND params1[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                                {TSDB_DATA_TYPE_INT, &tag_i[0], &tag_l[0], NULL, 2}};

  TAOS_STMT2_BIND* paramv = &params1[0];
  char*            tbname[2] = {"tb1"};

  TAOS_STMT2_BINDV bindv = {1, &tbname[0], NULL, &paramv};

  for (int i = 0; i < 3; i++) {
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
  }
  int affected_rows;
  code = taos_stmt2_exec(stmt, &affected_rows);
  ASSERT_EQ(affected_rows, 2);
  checkError(stmt, code, __FILE__, __LINE__);  // ASSERT_STREQ(taos_stmt2_error(stmt), "Table name duplicated");

  char*           tbname2[2] = {"tb2", "tb3"};
  TAOS_STMT2_BIND paramv2[2][2]{
      {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2}, {TSDB_DATA_TYPE_INT, &tag_i[0], &tag_l[0], NULL, 2}},
      {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2}, {TSDB_DATA_TYPE_INT, &tag_i[0], &tag_l[0], NULL, 2}}};
  TAOS_STMT2_BIND* paramvs2[2] = {&paramv2[0][0], &paramv2[1][0]};
  TAOS_STMT2_BINDV bindv2 = {2, &tbname2[0], NULL, &paramvs2[0]};
  for (int i = 0; i < 3; i++) {
    code = taos_stmt2_bind_param(stmt, &bindv2, -1);
    checkError(stmt, code, __FILE__, __LINE__);
  }

  code = taos_stmt2_exec(stmt, &affected_rows);
  ASSERT_EQ(affected_rows, 4);
  checkError(stmt, code, __FILE__, __LINE__);

  taos_stmt2_close(stmt);

  TAOS_RES* pRes = taos_query(taos, "select * from `stmt2_testdb_18`.`tb1`");
  ASSERT_NE(pRes, nullptr);

  int getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from `stmt2_testdb_18`.`tb2`");
  ASSERT_NE(pRes, nullptr);

  getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from `stmt2_testdb_18`.`tb3`");
  ASSERT_NE(pRes, nullptr);

  getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_free_result(pRes);

  // no interlace mode
  option = {0, false, false, NULL, NULL};
  stmt = taos_stmt2_init(taos, &option);
  code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  for (int i = 0; i < 3; i++) {
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
  }
  code = taos_stmt2_exec(stmt, &affected_rows);
  ASSERT_EQ(affected_rows, 2);
  checkError(stmt, code, __FILE__, __LINE__);  // ASSERT_STREQ(taos_stmt2_error(stmt), "Table name duplicated");
  taos_stmt2_close(stmt);

  pRes = taos_query(taos, "select * from `stmt2_testdb_18`.`tb1`");
  ASSERT_NE(pRes, nullptr);

  getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_free_result(pRes);

  // insert same tbname from different stb
  option = {0, true, true, NULL, NULL};
  stmt = taos_stmt2_init(taos, &option);
  code =
      taos_stmt2_prepare(stmt, "INSERT INTO `stmt2_testdb_18`.`stb2` (ts,int_col,int_tag,tbname)  VALUES (?,?,?,?)", 0);
      checkError(stmt, code, __FILE__, __LINE__);
  char*            tbname3[2] = {"tb1", "tb10"};
  TAOS_STMT2_BIND  tag = {TSDB_DATA_TYPE_INT, &tag_i[0], &tag_l[0], NULL, 1};
  TAOS_STMT2_BIND* pTag[2] = {&tag, &tag};
  TAOS_STMT2_BINDV bindv3 = {2, &tbname3[0], &pTag[0], &paramvs2[0]};

  code = taos_stmt2_bind_param(stmt, &bindv3, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, &affected_rows);
  ASSERT_EQ(code, TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE);

  stmt = taos_stmt2_init(taos, &option);
  taos_stmt2_prepare(stmt, "INSERT INTO `stmt2_testdb_18`.`stb2` (ts,int_col,tbname)  VALUES (?,?,?)", 0);
  bindv3 = {1, &tbname3[0], NULL, &paramvs2[0]};
  code = taos_stmt2_bind_param(stmt, &bindv3, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, &affected_rows);
  ASSERT_EQ(code, TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE);

  taos_stmt2_close(stmt);

  do_query(taos, "drop database if exists stmt2_testdb_18");
  taos_close(taos);
}

void taos_retrieve_call_back(void* param, TAOS_RES* tres, int numOfRows) {
  if (numOfRows > 0) {
    // synchronous API to retrieve a row from batch of records
    TAOS_ROW row = taos_fetch_row(tres);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
    ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);

    row = taos_fetch_row(tres);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb2", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "xyz", 3), 0);
    ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);
    taos_fetch_rows_a(tres, taos_retrieve_call_back, param);

  } else {
    ASSERT_EQ(tsem_post(&((AsyncArgs*)param)->sem), TSDB_CODE_SUCCESS);
  }
}

void asyncExec(void* param, TAOS_RES* res, int code) {
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  taos_fetch_rows_a(res, taos_retrieve_call_back, param);
  return;
}

TEST(stmt2Case, stmt2_query) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_7");
  do_query(taos, "create stable stmt2_testdb_7.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
           "insert into stmt2_testdb_7.tb1 using stmt2_testdb_7.stb tags(1,'abc') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628002, 'hij')");
  do_query(taos,
           "insert into stmt2_testdb_7.tb2 using stmt2_testdb_7.stb tags(2,'xyz') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628004, 'hij')");
  do_query(taos, "use stmt2_testdb_7");

  // sync query
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "select tbname,t2,b from stmt2_testdb_7.stb where ts = ? order by tbname";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 1);

    int              t64_len = sizeof(int64_t);
    int64_t          ts = 1591060628000;
    TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1};
    TAOS_STMT2_BIND* paramv = &params;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    taos_stmt2_close(stmt);
  }

  // async query with async fetch
  {
    AsyncArgs* aa = (AsyncArgs*)taosMemMalloc(sizeof(AsyncArgs));
    aa->async_affected_rows = 0;
    ASSERT_EQ(tsem_init(&aa->sem, 0, 0), TSDB_CODE_SUCCESS);

    TAOS_STMT2_OPTION option = {0, true, true, asyncExec, (void*)aa};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "select tbname,t2,b from stmt2_testdb_7.stb where ts = ? order by tbname";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 1);

    int              t64_len = sizeof(int64_t);
    int64_t          ts = 1591060628000;
    TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1};
    TAOS_STMT2_BIND* paramv = &params;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    tsem_wait(&aa->sem);
    tsem_destroy(&aa->sem);
    taosMemFree(aa);

    taos_stmt2_close(stmt);
  }

  // int code = taos_stmt2_prepare(stmt, "select tbname,t2,b from stmt2_testdb_7.stb where ts = ? and tbname = ?", 0);
  // checkError(stmt, code, __FILE__, __LINE__);

  // code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  // checkError(stmt, code, __FILE__, __LINE__);
  // ASSERT_EQ(fieldNum, 2);

  // int32_t b_len = 3;

  // TAOS_STMT2_BIND  params2[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1},
  //                                {TSDB_DATA_TYPE_BINARY, (void*)"tb1", &b_len, NULL, 1}};
  // TAOS_STMT2_BIND* paramv2[] = {&params2[0], &params2[1]};
  // TAOS_STMT2_BINDV bindv2 = {1, NULL, NULL, &paramv2[0]};

  // code = taos_stmt2_bind_param(stmt, &bindv2, -1);
  // checkError(stmt, code, __FILE__, __LINE__);

  // taos_stmt2_exec(stmt, NULL);
  // checkError(stmt, code, __FILE__, __LINE__);

  // TAOS_RES* pRes = taos_stmt2_result(stmt);
  // ASSERT_NE(pRes, nullptr);

  // TAOS_ROW row = taos_fetch_row(pRes);
  // ASSERT_NE(row, nullptr);
  // ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
  // ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
  // ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);

  do_query(taos, "drop database if exists stmt2_testdb_7");
  taos_close(taos);
}

void asyncSelectError(void* param, TAOS_RES* pRes, int code) {
  taosMsleep(500);
  // wrong usage 3 : sync fetch in callback
  taos_fetch_row(pRes);
  ASSERT_EQ(terrno, TSDB_CODE_TSC_INVALID_OPERATION);

  ((AsyncArgs*)param)->async_affected_rows = taos_affected_rows(pRes);
  ASSERT_EQ(tsem_post(&((AsyncArgs*)param)->sem), TSDB_CODE_SUCCESS);
}

TEST(stmt2Case, query_error) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_27");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_27");
  do_query(taos, "create stable stmt2_testdb_27.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
           "insert into stmt2_testdb_27.tb1 using stmt2_testdb_27.stb tags(1,'abc') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628002, 'hij')");
  do_query(taos,
           "insert into stmt2_testdb_27.tb2 using stmt2_testdb_27.stb tags(2,'xyz') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628004, 'hij')");
  do_query(taos, "use stmt2_testdb_27");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // no tbname
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "select * from ? where ts = ?", 0);
    ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);
    ASSERT_STREQ(taos_stmt2_error(stmt), "syntax error near \"? where ts = ?\"");
    taos_stmt2_close(stmt);
  }

  // tbname not exist
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "select * from stmt2_testdb_27.tb_not_exist where ts = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int fieldNum = 0;
    code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 1);

    int              t64_len = sizeof(int64_t);
    int64_t          ts = 1591060628000;
    TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1};
    TAOS_STMT2_BIND* paramv = &params;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);
    ASSERT_STREQ(taos_stmt2_error(stmt), "Table does not exist");
    taos_stmt2_close(stmt);
  }

  {
    AsyncArgs* aa = (AsyncArgs*)taosMemMalloc(sizeof(AsyncArgs));
    aa->async_affected_rows = 0;
    ASSERT_EQ(tsem_init(&aa->sem, 0, 0), TSDB_CODE_SUCCESS);

    TAOS_STMT2_OPTION option = {0, true, true, asyncSelectError, (void*)aa};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code =
        taos_stmt2_prepare(stmt, "select tbname,b,t1,t2 from stmt2_testdb_27.stb where ts = ? order by tbname", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int fieldNum = 0;
    code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 1);

    int              t64_len = sizeof(int64_t);
    int64_t          ts = 1591060628000;
    TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1};
    TAOS_STMT2_BIND* paramv = &params;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    // wrong usage 1 : before call back
    TAOS_RES* pRes = taos_stmt2_result(stmt);
    ASSERT_EQ(pRes, nullptr);

    tsem_wait(&aa->sem);
    tsem_destroy(&aa->sem);
    taosMemoryFree(aa);
    // correct usage 2 : sync fetch in async query
    pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);
    TAOS_ROW row = taos_fetch_row(pRes);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
    ASSERT_EQ(*(int*)row[2], 1);
    ASSERT_EQ(strncmp((char*)row[3], "abc", 3), 0);

    row = taos_fetch_row(pRes);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb2", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
    ASSERT_EQ(*(int*)row[2], 2);
    ASSERT_EQ(strncmp((char*)row[3], "xyz", 3), 0);

    do_query(taos, "drop database if exists stmt2_testdb_7");
    taos_stmt2_close(stmt);
  }
  do_query(taos, "drop database if exists stmt2_testdb_27");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_ntb_insert) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  do_query(taos, "drop database if exists stmt2_testdb_8");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_8");
  do_query(taos, "create table stmt2_testdb_8.ntb(ts timestamp, b binary(10))");
  do_query(taos, "use stmt2_testdb_8");
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  // test fixed tbname and bind per col
  {
    int         total_affected_rows = 0;
    const char* sql = "insert into stmt2_testdb_8.ntb values(?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    for (int i = 0; i < 3; i++) {
      int64_t ts[3] = {1591060628000 + i * 3, 1591060628001 + i * 3, 1591060628002 + i * 3};
      int     t64_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
      int     b_len[3] = {5, 5, 5};

      TAOS_STMT2_BIND  params1 = {TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 3};
      TAOS_STMT2_BIND  params2 = {TSDB_DATA_TYPE_BINARY, (void*)"abcdefghijklmnopqrstuvwxyz", &b_len[0], NULL, 3};
      TAOS_STMT2_BIND* paramv1 = &params1;
      TAOS_STMT2_BIND* paramv2 = &params2;

      TAOS_STMT2_BINDV bindv1 = {1, NULL, NULL, &paramv1};
      TAOS_STMT2_BINDV bindv2 = {1, NULL, NULL, &paramv2};

      code = taos_stmt2_bind_param(stmt, &bindv1, 0);
      code = taos_stmt2_bind_param(stmt, &bindv2, 1);
      checkError(stmt, code, __FILE__, __LINE__);

      int affected_rows;
      code = taos_stmt2_exec(stmt, &affected_rows);
      total_affected_rows += affected_rows;
      checkError(stmt, code, __FILE__, __LINE__);
    }
    ASSERT_EQ(total_affected_rows, 9);
  }

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_8");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_status_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);

  int64_t ts[3] = {1591060628000, 1591060628001, 1591060628002};
  int     t64_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};

  TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 3};
  TAOS_STMT2_BIND* paramv = &params;
  TAOS_STMT2_BINDV bindv1 = {1, NULL, NULL, &paramv};

  int code = taos_stmt2_bind_param(stmt, &bindv1, 0);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "bind number out of range or not match");

  code = taos_stmt2_exec(stmt, NULL);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "Stmt API usage error");

  const char* sql = "insert into stmt2_testdb_9.ntb values(?,?)";
  code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "Stmt API usage error");

  taos_stmt2_close(stmt);
  taos_close(taos);
}

TEST(stmt2Case, stmt2_nchar) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  do_query(taos, "drop database if exists stmt2_testdb_10;");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_10;");
  do_query(taos, "use stmt2_testdb_10;");
  do_query(taos,
           "create table m1 (ts timestamp, nchar1 nchar(10), nchar2 nchar(10),nchar3 nchar(10),nchar4 nchar(10),nchar5 "
           "nchar(10),nchar6 nchar(10));");

  // insert 10 records
  struct {
    int64_t ts[10];
    char    blob[10][1];
    char    blob2[10][1];
    char    blob3[10][1];
    char    blob4[10][1];
    char    blob5[10][1];
  } v;

  int32_t* t64_len = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len2 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len3 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len4 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len5 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    TAOS_STMT2_BIND params[10];
    char            is_null[10] = {0};

    params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    // params[0].buffer_length = sizeof(v.ts[0]);
    params[0].buffer = v.ts;
    params[0].length = t64_len;
    params[0].is_null = is_null;
    params[0].num = 10;

    params[1].buffer_type = TSDB_DATA_TYPE_NCHAR;
    // params[8].buffer_length = sizeof(v.blob2[0]);
    params[1].buffer = v.blob2;
    params[1].length = blob_len2;
    params[1].is_null = is_null;
    params[1].num = 10;

    params[2].buffer_type = TSDB_DATA_TYPE_NCHAR;
    // params[9].buffer_length = sizeof(v.blob[0]);
    params[2].buffer = v.blob3;
    params[2].length = blob_len;
    params[2].is_null = is_null;
    params[2].num = 10;

    params[3].buffer_type = TSDB_DATA_TYPE_NCHAR;
    // params[9].buffer_length = sizeof(v.blob[0]);
    params[3].buffer = v.blob4;
    params[3].length = blob_len;
    params[3].is_null = is_null;
    params[3].num = 10;

    params[4].buffer_type = TSDB_DATA_TYPE_NCHAR;
    // params[9].buffer_length = sizeof(v.blob[0]);
    params[4].buffer = v.blob;
    params[4].length = blob_len;
    params[4].is_null = is_null;
    params[4].num = 10;

    params[5].buffer_type = TSDB_DATA_TYPE_NCHAR;
    // params[9].buffer_length = sizeof(v.blob[0]);
    params[5].buffer = v.blob5;
    params[5].length = blob_len;
    params[5].is_null = is_null;
    params[5].num = 10;

    int len[10] = {0};
    params[6].buffer_type = TSDB_DATA_TYPE_NCHAR;
    // params[9].buffer_length = sizeof(v.blob[0]);
    params[6].buffer = NULL;
    params[6].length = &len[0];
    params[6].is_null = is_null;
    params[6].num = 10;

    int code = taos_stmt2_prepare(
        stmt, "insert into ? (ts, nchar1, nchar2, nchar3, nchar4, nchar5, nchar6) values(?,?,?,?,?,?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);
    int64_t ts = 1591060628000;
    for (int i = 0; i < 10; ++i) {
      is_null[i] = 0;

      v.ts[i] = ts++;

      v.blob[i][0] = 'a' + i;
      v.blob2[i][0] = 'f' + i;
      v.blob3[i][0] = 't' + i;
      v.blob4[i][0] = 'A' + i;
      v.blob5[i][0] = 'G' + i;

      blob_len[i] = sizeof(char);
      blob_len2[i] = sizeof(char);
      blob_len3[i] = sizeof(char);
      blob_len4[i] = sizeof(char);
      blob_len5[i] = sizeof(char);
    }

    char*            tbname = "m1";
    TAOS_STMT2_BIND* bind_cols[1] = {&params[0]};
    TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &bind_cols[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 10);

    taos_stmt2_close(stmt);
  }
  taosMemoryFree(blob_len);
  taosMemoryFree(blob_len2);
  taosMemoryFree(blob_len5);
  taosMemoryFree(blob_len3);
  taosMemoryFree(blob_len4);

  // test null value
  do_query(taos, "create stable stmt2_testdb_10.stb (ts timestamp, b nchar(10)) tags(t1 nchar(10))");
  TAOS_STMT2_OPTION options[2] = {{0, true, true, NULL, NULL}, {0, false, false, NULL, NULL}};
  for (int i = 0; i < 2; i++) {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &options[i]);
    ASSERT_NE(stmt, nullptr);

    int code =
        taos_stmt2_prepare(stmt, "INSERT INTO stmt2_testdb_10.? using stmt2_testdb_10.stb tags(?) values(?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            tbname[2] = {"tb1", "tb2"};
    int64_t          ts[2] = {1591060628000, 1591060628001};
    int              t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    char             isNull[2] = {1, 0};
    int              nchar_len[2] = {0, 0};
    TAOS_STMT2_BIND  tags = {TSDB_DATA_TYPE_NCHAR, NULL, &nchar_len[1], NULL, 1};
    TAOS_STMT2_BIND* tagv[2] = {&tags, &tags};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                                  {TSDB_DATA_TYPE_NCHAR, NULL, &nchar_len[0], &isNull[0], 2}};
    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[0]};
    TAOS_STMT2_BINDV bindv1 = {1, &tbname[i], &tagv[0], &paramv[0]};

    code = taos_stmt2_bind_param(stmt, &bindv1, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 2);

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_10;");
  taos_close(taos);
}

// TEST(stmt2Case, stmt2_blob) {
//   TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
//   do_query(taos, "drop database if exists stmt2_testdb_10;");
//   do_query(taos, "create database IF NOT EXISTS stmt2_testdb_10;");
//   do_query(taos, "use stmt2_testdb_10;");
//   do_query(
//       taos,
//       "create table m1 (ts timestamp, blob1 blob, blob2 nchar(10),blob3 nchar(10),blob4 nchar(10),blob5 nchar(12))");

//   // insert 10 records
//   struct {
//     int64_t ts[10];
//     char    blob[10][1];
//     char    blob2[10][1];
//     char    blob3[10][1];
//     char    blob4[10][1];
//     char    blob5[10][1];

//   } v;

//   int32_t* t64_len = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
//   int32_t* nchar_len1 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
//   int32_t* nchar_len2 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
//   int32_t* nchar_len3 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
//   int32_t* nchar_len4 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
//   int32_t* nchar_len5 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);

//   TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

//   TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
//   ASSERT_NE(stmt, nullptr);
//   TAOS_STMT2_BIND params[10];
//   char            is_null[10] = {0};

//   params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
//   // params[0].buffer_length = sizeof(v.ts[0]);
//   params[0].buffer = v.ts;
//   params[0].length = t64_len;
//   params[0].is_null = is_null;
//   params[0].num = 10;

//   params[1].buffer_type = TSDB_DATA_TYPE_NCHAR;
//   // params[8].buffer_length = sizeof(v.nchar2[0]);
//   params[1].buffer = v.nchar1;
//   params[1].length = nchar_len1;
//   params[1].is_null = is_null;
//   params[1].num = 10;

//   params[2].buffer_type = TSDB_DATA_TYPE_NCHAR;
//   // params[9].buffer_length = sizeof(v.nchar[0]);
//   params[2].buffer = v.nchar2;
//   params[2].length = nchar_len2;
//   params[2].is_null = is_null;
//   params[2].num = 10;

//   params[3].buffer_type = TSDB_DATA_TYPE_NCHAR;
//   // params[9].buffer_length = sizeof(v.nchar[0]);
//   params[3].buffer = v.nchar3;
//   params[3].length = nchar_len3;
//   params[3].is_null = is_null;
//   params[3].num = 10;

//   params[4].buffer_type = TSDB_DATA_TYPE_NCHAR;
//   // params[9].buffer_length = sizeof(v.nchar[0]);
//   params[4].buffer = v.nchar4;
//   params[4].length = nchar_len4;
//   params[4].is_null = is_null;
//   params[4].num = 10;

//   params[5].buffer_type = TSDB_DATA_TYPE_NCHAR;
//   // params[9].buffer_length = sizeof(v.nchar[0]);
//   params[5].buffer = v.nchar5;
//   params[5].length = nchar_len5;
//   params[5].is_null = is_null;
//   params[5].num = 10;

//   int code = taos_stmt2_prepare(stmt, "insert into ? (ts, blob1, blob2 blob3, blob4, blob5) values(?,?,?,?,?,?)", 0);
//   checkError(stmt, code, __FILE__, __LINE__);

//   int64_t ts = 1591060628000;
//   for (int i = 0; i < 10; ++i) {
//     is_null[i] = 0;

//     v.ts[i] = ts++;

//     v.nchar1[i][0] = 'a' + i;
//     v.nchar2[i][0] = 'f' + i;
//     v.nchar3[i][0] = 't' + i;
//     v.nchar4[i][0] = 'A' + i;
//     v.nchar5[i][0] = 'G' + i;

//     nchar_len1[i] = sizeof(char);
//     nchar_len2[i] = sizeof(char);
//     nchar_len3[i] = sizeof(char);
//     nchar_len4[i] = sizeof(char);
//     nchar_len5[i] = sizeof(char);
//   }

//   char*            tbname = "m1";
//   TAOS_STMT2_BIND* bind_cols[1] = {&params[0]};
//   TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &bind_cols[0]};
//   code = taos_stmt2_bind_param(stmt, &bindv, -1);
//   checkError(stmt, code, __FILE__, __LINE__);

//   int affected_rows;
//   code = taos_stmt2_exec(stmt, &affected_rows);
//   checkError(stmt, code, __FILE__, __LINE__);
//   ASSERT_EQ(affected_rows, 10);

//   taos_stmt2_close(stmt);
//   do_query(taos, "drop database if exists stmt2_testdb_10;");
//   taos_close(taos);
//   taosMemoryFree(nchar_len1);
//   taosMemoryFree(nchar_len2);
//   taosMemoryFree(nchar_len5);
//   taosMemoryFree(nchar_len3);
//   taosMemoryFree(nchar_len4);
// }

TEST(stmt2Case, all_type) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_11");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_11");
  do_query(taos,
           "create stable stmt2_testdb_11.stb(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(128), c6 "
           "smallint, c7 "
           "tinyint, c8 bool, c9 nchar(128), c10 geometry(256))TAGS(tts timestamp, t1 int, t2 bigint, t3 float, t4 "
           "double, t5 "
           "binary(128), t6 smallint, t7 tinyint, t8 bool, t9 nchar(128), t10 geometry(256))");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int       code = 0;
  uintptr_t c10len = 0;
  struct {
    int64_t       c1;
    int32_t       c2;
    int64_t       c3;
    float         c4;
    double        c5;
    unsigned char c6[8];
    int16_t       c7;
    int8_t        c8;
    int8_t        c9;
    char          c10[32];
  } v = {1591060628000, 1, 2, 3.0, 4.0, "abcdef", 5, 6, 7, "ijnop"};

  struct {
    int32_t c1;
    int32_t c2;
    int32_t c3;
    int32_t c4;
    int32_t c5;
    int32_t c6;
    int32_t c7;
    int32_t c8;
    int32_t c9;
    int32_t c10;
  } v_len = {sizeof(int64_t), sizeof(int32_t),
             sizeof(int64_t), sizeof(float),
             sizeof(double),  8,
             sizeof(int16_t), sizeof(int8_t),
             sizeof(int8_t),  8};
  TAOS_STMT2_BIND params[11];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].length = (int32_t*)&v_len.c1;
  params[0].buffer = &v.c1;
  params[0].is_null = NULL;
  params[0].num = 1;

  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer = &v.c2;
  params[1].length = (int32_t*)&v_len.c2;
  params[1].is_null = NULL;
  params[1].num = 1;

  params[2].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[2].buffer = &v.c3;
  params[2].length = (int32_t*)&v_len.c3;
  params[2].is_null = NULL;
  params[2].num = 1;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer = &v.c4;
  params[3].length = (int32_t*)&v_len.c4;
  params[3].is_null = NULL;
  params[3].num = 1;

  params[4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[4].buffer = &v.c5;
  params[4].length = (int32_t*)&v_len.c5;
  params[4].is_null = NULL;
  params[4].num = 1;

  params[5].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[5].buffer = &v.c6;
  params[5].length = (int32_t*)&v_len.c6;
  params[5].is_null = NULL;
  params[5].num = 1;

  params[6].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[6].buffer = &v.c7;
  params[6].length = (int32_t*)&v_len.c7;
  params[6].is_null = NULL;
  params[6].num = 1;

  params[7].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[7].buffer = &v.c8;
  params[7].length = (int32_t*)&v_len.c8;
  params[7].is_null = NULL;
  params[7].num = 1;

  params[8].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[8].buffer = &v.c9;
  params[8].length = (int32_t*)&v_len.c9;
  params[8].is_null = NULL;
  params[8].num = 1;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer = &v.c10;
  params[9].length = (int32_t*)&v_len.c10;
  params[9].is_null = NULL;
  params[9].num = 1;

  unsigned char* outputGeom1;
  size_t         size1;
  initCtxMakePoint();
  code = doMakePoint(1.000, 2.000, &outputGeom1, &size1);
  checkError(stmt, code, __FILE__, __LINE__);
  params[10].buffer_type = TSDB_DATA_TYPE_GEOMETRY;
  params[10].buffer = outputGeom1;
  params[10].length = (int32_t*)&size1;
  params[10].is_null = NULL;
  params[10].num = 1;

  char* stmt_sql = "insert into stmt2_testdb_11.? using stb tags(?,?,?,?,?,?,?,?,?,?,?)values (?,?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt2_prepare(stmt, stmt_sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  char*            tbname[1] = {"tb1"};
  TAOS_STMT2_BIND* tags = &params[0];
  TAOS_STMT2_BIND* cols = &params[0];
  TAOS_STMT2_BINDV bindv = {1, &tbname[0], &tags, &cols};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  int affected_rows;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected_rows, 1);
  taos_stmt2_close(stmt);

  option = {0, true, true, NULL, NULL};
  stmt = taos_stmt2_init(taos, &option);
  stmt_sql = "insert into stmt2_testdb_11.stb(tbname,ts,c5,c9,t5,t9)values(?,?,?,?,?,?)";
  code = taos_stmt2_prepare(stmt, stmt_sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);
  int     tag_l[2] = {128, 128};
  int64_t ts[2]{1591060628001, 1591060628002};
  int32_t t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  char    c[2][128] = {
      "abcdefjhijklmnopqrstuvwxyzabcdefjhijklmnopqrstuvwxyzabcdefjhijklmnopqrstuvwxyzabcdefjhijklmnopqrstuvwxyz",
      "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"};
  for (int i = 0; i < 3; i++) {
    TAOS_STMT2_BIND tags1[2] = {{TSDB_DATA_TYPE_BINARY, &c[0], &tag_l[0], NULL, 1},
                                {TSDB_DATA_TYPE_NCHAR, &c[1], &tag_l[0], NULL, 1}};
    TAOS_STMT2_BIND params1[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                                  {TSDB_DATA_TYPE_BINARY, &c[0], &tag_l[0], NULL, 2},
                                  {TSDB_DATA_TYPE_NCHAR, &c[0], &tag_l[0], NULL, 2}};

    TAOS_STMT2_BIND* tagv[2] = {&tags1[0], &tags1[0]};
    TAOS_STMT2_BIND* paramv[2] = {&params1[0], &params1[0]};
    char*            tbname[2] = {"tb2", "tb3"};
    TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);

    stmt_sql = "insert into stmt2_testdb_11.? using stmt2_testdb_11.stb(t5,t9)tags(?,?) (ts,c5,c9)values(?,?,?)";
    code = taos_stmt2_prepare(stmt, stmt_sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char* tbname2[2] = {"tb4", "tb5"};
    bindv = {2, &tbname2[0], &tagv[0], &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);
  }

  geosFreeBuffer(outputGeom1);
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_11");
  taos_close(taos);
}

TEST(stmt2Case, geometry) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_13");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_13");
  do_query(taos,
           "CREATE STABLE stmt2_testdb_13.stb (ts timestamp, c1 geometry(256)) tags(t1 timestamp,t2 geometry(256))");
  do_query(taos, "CREATE TABLE stmt2_testdb_13.tb1(ts timestamp,c1 geometry(256))");

  //  wrong wkb input
  unsigned char wkb1[] = {
      // 1
      0x01,                                            // 字节顺序：小端字节序
      0x01, 0x00, 0x00, 0x00,                          // 几何类型：Point (1)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F,  // p1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,  // p2
                                                       // 2
      0x01, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0xf0, 0x3f,
      // 3
      0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x40};
  //  wrong wkb input
  unsigned char wkb2[3][61] = {
      {
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
      },
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}};

  // unsigned char* wkb_all[3]{&wkb1[0], &wkb2[0], &wkb3[0]};
  int32_t wkb_len[4] = {21, 61, 41, 0};

  int64_t         ts[4] = {1591060628000, 1591060628001, 1591060628002, 1591060628003};
  int32_t         t64_len[4] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
  char            is_null[4] = {0, 0, 0, 1};
  TAOS_STMT2_BIND params[2];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer = &ts[0];
  params[0].length = &t64_len[0];
  params[0].is_null = NULL;
  params[0].num = 4;

  params[1].buffer_type = TSDB_DATA_TYPE_GEOMETRY;
  params[1].buffer = &wkb1[0];
  params[1].length = &wkb_len[0];
  params[1].is_null = is_null;
  params[1].num = 4;

  // case 1 : ntb
  {
    printf("  case 1 : ntb\n");
    TAOS_STMT2_OPTION option = {0};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    char* stmt_sql = "insert into stmt2_testdb_13.tb1 (ts,c1)values(?,?)";
    int   code = taos_stmt2_prepare(stmt, stmt_sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND* cols = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &cols};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);
  }
  // case 2 : interlace = 1
  {
    printf("  case 2 : interlace = 1\n");
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    char* stmt_sql = "insert into stmt2_testdb_13.? using stmt2_testdb_13.stb tags(?,?)values(?,?)";
    int   code = taos_stmt2_prepare(stmt, stmt_sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            ctbname[1] = {"ctb1"};
    TAOS_STMT2_BIND* cols = &params[0];
    TAOS_STMT2_BINDV bindv = {1, ctbname, &cols, &cols};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);
    taos_stmt2_close(stmt);
  }
  // case 3 : interlace = 0
  {
    printf("  case 3 : interlace = 0\n");
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    char* stmt_sql = "insert into stmt2_testdb_13.? using stmt2_testdb_13.stb tags(?,?)values(?,?)";
    int   code = taos_stmt2_prepare(stmt, stmt_sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            ctbname[1] = {"ctb2"};
    TAOS_STMT2_BIND* cols = &params[0];
    TAOS_STMT2_BINDV bindv = {1, ctbname, &cols, &cols};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);
    taos_stmt2_close(stmt);
  }
  // case 4 : error case
  params[1].buffer = &wkb2[0];
  {
    printf("  case 4 : error format\n");
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    char* stmt_sql = "insert into stmt2_testdb_13.? using stmt2_testdb_13.stb tags(?,?)values(?,?)";
    int   code = taos_stmt2_prepare(stmt, stmt_sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            ctbname[1] = {"ctb3"};
    TAOS_STMT2_BIND* cols = &params[0];
    TAOS_STMT2_BINDV bindv = {1, ctbname, &cols, &cols};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);
    taos_stmt2_close(stmt);
  }

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_13");
  taos_close(taos);
}

TEST(stmt2Case, decimal) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_20");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_20");
  do_query(taos, "CREATE STABLE `stmt2_testdb_20`.stb (ts TIMESTAMP, b1 DECIMAL(4,2), b2 DECIMAL(20,10)) TAGS (t INT)");
  do_query(taos, "CREATE TABLE `stmt2_testdb_20`.ntb (ts TIMESTAMP, b1 DECIMAL(4,2), b2 DECIMAL(20,10))");

  TAOS_STMT2_OPTION option[2] = {{0, true, true, NULL, NULL}, {0, false, false, NULL, NULL}};

  int64_t ts[3] = {1591060628000, 1591060629000, 1591060630000};

  char b1_data[64] =
      "99.9876"
      "1.0234";
  char b2_data[128] =
      "1234567890.1234567890123"
      "1.23e+5";
  int t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  int b1_len[2] = {7, 6};
  int b2_len[2] = {24, 7};

  int   tag_data = 1;
  int   tag_len = sizeof(int64_t);
  char* tbnames[2] = {"tb1", "tb2"};
  int   affected_rows;

  // insert stb with interlace and no interlace
  for (int i = 0; i < 2; i++) {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[i]);
    ASSERT_NE(stmt, nullptr);
    char* sql = "insert into `stmt2_testdb_20`.? using `stmt2_testdb_20`.stb tags(?) values(?,?,?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 5);
    ASSERT_STREQ(pFields[3].name, "b1");
    ASSERT_EQ(pFields[3].type, TSDB_DATA_TYPE_DECIMAL64);
    ASSERT_EQ(pFields[3].precision, 4);
    ASSERT_EQ(pFields[3].scale, 2);

    ASSERT_STREQ(pFields[4].name, "b2");
    ASSERT_EQ(pFields[4].type, TSDB_DATA_TYPE_DECIMAL);
    ASSERT_EQ(pFields[4].precision, 20);
    ASSERT_EQ(pFields[4].scale, 10);

    taos_stmt2_free_fields(stmt, pFields);

    TAOS_STMT2_BIND  tag = {TSDB_DATA_TYPE_INT, &tag_data, &tag_len, NULL, 1};
    TAOS_STMT2_BIND* pTags[2] = {&tag, &tag};

    TAOS_STMT2_BIND  col[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                               {TSDB_DATA_TYPE_DECIMAL64, &b1_data[0], &b1_len[0], NULL, 2},
                               {TSDB_DATA_TYPE_DECIMAL, &b2_data[0], &b2_len[0], NULL, 2}};
    TAOS_STMT2_BIND* cols[2] = {&col[0], &col[0]};
    TAOS_STMT2_BINDV bindv = {2, &tbnames[0], &pTags[0], &cols[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);

    TAOS_RES* result = taos_query(taos, "select b1,b2,tbname from stmt2_testdb_20.stb order by tbname");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), 0);

    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], "99.99");
    ASSERT_STREQ((char*)row[1], "1234567890.1234567890");
    ASSERT_EQ(strncmp((char*)row[2], "tb1", 3), 0);

    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], "1.02");
    ASSERT_STREQ((char*)row[1], "123000.0000000000");
    ASSERT_EQ(strncmp((char*)row[2], "tb1", 3), 0);

    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], "99.99");
    ASSERT_STREQ((char*)row[1], "1234567890.1234567890");
    ASSERT_EQ(strncmp((char*)row[2], "tb2", 3), 0);

    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], "1.02");
    ASSERT_STREQ((char*)row[1], "123000.0000000000");
    ASSERT_EQ(strncmp((char*)row[2], "tb2", 3), 0);

    taos_free_result(result);

    // check null decimal
    char             is_null1 = '1';
    TAOS_STMT2_BIND  col1[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[2], &t64_len[0], NULL, 1},
                                {TSDB_DATA_TYPE_DECIMAL64, NULL, NULL, &is_null1, 1},
                                {TSDB_DATA_TYPE_DECIMAL, NULL, NULL, &is_null1, 1}};
    TAOS_STMT2_BIND* cols1 = &col1[0];
    bindv = TAOS_STMT2_BINDV{1, &tbnames[0], &pTags[0], &cols1};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 1);

    result = taos_query(taos, "select b1,b2,tbname from stmt2_testdb_20.tb1 where ts = 1591060630000 order by tbname");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), 0);

    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], NULL);
    ASSERT_STREQ((char*)row[1], NULL);
    ASSERT_EQ(strncmp((char*)row[2], "tb1", 3), 0);
    taos_free_result(result);

    do_query(taos, "delete from stmt2_testdb_20.tb1");
    do_query(taos, "delete from stmt2_testdb_20.tb2");
    taos_stmt2_close(stmt);
  }

  // check decimal overflow
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[0]);
    ASSERT_NE(stmt, nullptr);
    char* sql = "insert into `stmt2_testdb_20`.? using `stmt2_testdb_20`.stb tags(1) values(?,?,?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            b1_data2 = "99.99999";
    char*            b2_data2 = "1.23e+5";
    int32_t          b1_len2 = 8;
    int32_t          b2_len2 = 7;
    TAOS_STMT2_BIND  col2[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 1},
                                {TSDB_DATA_TYPE_DECIMAL64, b1_data2, &b1_len2, NULL, 1},
                                {TSDB_DATA_TYPE_DECIMAL, b2_data2, &b2_len2, NULL, 1}};
    TAOS_STMT2_BIND* cols2 = &col2[0];
    TAOS_STMT2_BINDV bindv = TAOS_STMT2_BINDV{1, &tbnames[0], NULL, &cols2};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_DECIMAL_OVERFLOW);

    taos_stmt2_close(stmt);
  }

  // normal table
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[0]);
    ASSERT_NE(stmt, nullptr);
    char* sql = "insert into stmt2_testdb_20.ntb values(?,?,?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 3);
    ASSERT_STREQ(pFields[1].name, "b1");
    ASSERT_EQ(pFields[1].type, TSDB_DATA_TYPE_DECIMAL64);
    ASSERT_EQ(pFields[1].precision, 4);
    ASSERT_EQ(pFields[1].scale, 2);

    ASSERT_STREQ(pFields[2].name, "b2");
    ASSERT_EQ(pFields[2].type, TSDB_DATA_TYPE_DECIMAL);
    ASSERT_EQ(pFields[2].precision, 20);
    ASSERT_EQ(pFields[2].scale, 10);

    taos_stmt2_free_fields(stmt, pFields);

    TAOS_STMT2_BIND  col[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                               {TSDB_DATA_TYPE_DECIMAL64, &b1_data[0], &b1_len[0], NULL, 2},
                               {TSDB_DATA_TYPE_DECIMAL, &b2_data[0], &b2_len[0], NULL, 2}};
    TAOS_STMT2_BIND* cols[2] = {&col[0]};
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &cols[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 2);

    TAOS_RES* result = taos_query(taos, "select b1,b2 from stmt2_testdb_20.ntb");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), 0);

    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], "99.99");
    ASSERT_STREQ((char*)row[1], "1234567890.1234567890");

    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], "1.02");
    ASSERT_STREQ((char*)row[1], "123000.0000000000");
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_20");
  taos_close(taos);
}

void testMultiPrepare(TAOS* taos, TAOS_STMT2_OPTION* option) {
  TAOS_STMT2* stmt = taos_stmt2_init(taos, option);
  ASSERT_NE(stmt, nullptr);
  // 1 insert stb
  char* sql = "insert into ? using stb tags(?,?) values(?,?)";
  int   code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);
  int total_affected = 0;

  char*            tbname = "t1";
  int64_t          tt = 1591060628000;
  int32_t          tb = 100;
  int              tag_len[2] = {sizeof(int64_t), sizeof(int32_t)};
  TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tt, &tag_len[0], NULL, 1},
                              {TSDB_DATA_TYPE_INT, &tb, &tag_len[1], NULL, 1}};
  TAOS_STMT2_BIND* tagv = &tags[0];

  int64_t          ts[2] = {1591060628000, 1591060629000};
  int32_t          values[2] = {100, 200};
  int              t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  int              val_len[2] = {sizeof(int32_t), sizeof(int32_t)};
  TAOS_STMT2_BIND  col[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                             {TSDB_DATA_TYPE_INT, &values[0], &val_len[0], NULL, 2}};
  TAOS_STMT2_BIND* cols = &col[0];
  TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &cols};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  int affected_rows;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected_rows, 2);

  // 2 insert ntb
  sql = "INSERT INTO t VALUES (?, ?)";
  code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  TAOS_STMT2_BIND params[2];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer = &ts[0];
  params[0].length = &t64_len[0];
  params[0].is_null = NULL;
  params[0].num = 2;

  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer = &values[0];
  params[1].length = &val_len[0];
  params[1].is_null = NULL;
  params[1].num = 2;
  TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};

  bindv = {1, NULL, NULL, &paramv[0]};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  affected_rows = 0;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected_rows, 2);

  // 3 select data
  const char* query_sql = "SELECT * FROM t WHERE ts >= ? AND ts <= ?";
  code = taos_stmt2_prepare(stmt, query_sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, 2);

  int64_t start_ts[2] = {1591060627000, 1591060630000};
  int     t64_len_query[2] = {sizeof(int64_t), sizeof(int64_t)};

  TAOS_STMT2_BIND  param3[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &start_ts[0], &t64_len_query[0], NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &start_ts[1], &t64_len_query[1], NULL, 1}};
  TAOS_STMT2_BIND* paramv2 = &param3[0];
  TAOS_STMT2_BINDV bindv2 = {1, NULL, NULL, &paramv2};
  code = taos_stmt2_bind_param(stmt, &bindv2, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  TAOS_RES* res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);

  TAOS_ROW row;
  int      rows = 0;
  while ((row = taos_fetch_row(res))) {
    rows++;
  }
  ASSERT_EQ(rows, 2);  // 应该返回2条记录

  taos_stmt2_close(stmt);
}

TEST(stmt2Case, prepare) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_prepare");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_prepare");
  do_query(taos, "USE stmt2_testdb_prepare");
  do_query(taos, "CREATE TABLE t (ts TIMESTAMP, b INT)");
  do_query(taos, "CREATE STABLE stb (ts TIMESTAMP, b INT) TAGS (tt TIMESTAMP, tb INT)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  testMultiPrepare(taos, &option);

  option = {0, false, false, NULL, NULL};
  testMultiPrepare(taos, &option);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_prepare");
  taos_close(taos);
}

// TD-34593
TEST(stmt2Case, prepare_fixedtags) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_prepare2");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_prepare2");
  do_query(taos, "CREATE STABLE `stmt2_testdb_prepare2`.stb (ts TIMESTAMP, b INT) TAGS (tt TIMESTAMP, tb INT)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  for (int i = 0; i < 10; i++) {
    // 1 insert stb
    char* sql = "insert into `stmt2_testdb_prepare2`.? using `stmt2_testdb_prepare2`.stb tags(now,1) values(?,?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    int total_affected = 0;

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 3);

    int64_t          tt = 1591060628000;
    int32_t          tb = 100;
    int              tag_len[2] = {sizeof(int64_t), sizeof(int32_t)};
    TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tt, &tag_len[0], NULL, 1},
                                {TSDB_DATA_TYPE_INT, &tb, &tag_len[1], NULL, 1}};
    TAOS_STMT2_BIND* tagv = &tags[0];
    char             tbname[10];
    sprintf(tbname, "t%d", i);
    char*            tbnames = &tbname[0];
    int64_t          ts[2] = {1591060628000, 1591060629000};
    int32_t          values[2] = {100, 200};
    int              t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int              val_len[2] = {sizeof(int32_t), sizeof(int32_t)};
    TAOS_STMT2_BIND  col[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                               {TSDB_DATA_TYPE_INT, &values[0], &val_len[0], NULL, 2}};
    TAOS_STMT2_BIND* cols = &col[0];
    TAOS_STMT2_BINDV bindv = {1, &tbnames, NULL, &cols};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 2);

    taos_stmt2_free_fields(stmt, pFields);
  }

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_prepare2");
  taos_stmt2_close(stmt);
  taos_close(taos);
}

// TD-33921
TEST(stmt2Case, mixed_bind) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_19");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_19");
  do_query(taos,
           "create stable stmt2_testdb_19.stb(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(128), c6 "
           "smallint, c7 "
           "tinyint, c8 bool, c9 nchar(128), c10 geometry(256))TAGS(tts timestamp, t1 int, t2 bigint, t3 float, t4 "
           "double, t5 "
           "binary(128), t6 smallint, t7 tinyint, t8 bool, t9 nchar(128), t10 geometry(256))");
  do_query(taos, "use stmt2_testdb_19");

  int code = 0;
  struct {
    int64_t       c1;
    int32_t       c2;
    int64_t       c3;
    float         c4;
    double        c5;
    unsigned char c6[8];
    int16_t       c7;
    int8_t        c8;
    int8_t        c9;
    char          c10[32];
  } v = {1591060628000, 1, 2, 3.0, 4.0, "abcdef", 5, 6, 7, "ijnop"};

  struct {
    int32_t c1;
    int32_t c2;
    int32_t c3;
    int32_t c4;
    int32_t c5;
    int32_t c6;
    int32_t c7;
    int32_t c8;
    int32_t c9;
    int32_t c10;
  } v_len = {sizeof(int64_t), sizeof(int32_t),
             sizeof(int64_t), sizeof(float),
             sizeof(double),  8,
             sizeof(int16_t), sizeof(int8_t),
             sizeof(int8_t),  8};
  unsigned char* outputGeom1;
  size_t         size1;
  initCtxMakePoint();
  code = doMakePoint(1.000, 2.000, &outputGeom1, &size1);
  char* tbname[8] = {"tb1", "tb2", "tb3", "tb4", "tb5", "tb6", "tb7", "tb8"};

  TAOS_STMT2_BIND params_tags[9] = {{TSDB_DATA_TYPE_INT, &v.c2, (int32_t*)&v_len.c2, NULL, 1},
                                    {TSDB_DATA_TYPE_BIGINT, &v.c3, (int32_t*)&v_len.c3, NULL, 1},
                                    {TSDB_DATA_TYPE_FLOAT, &v.c4, (int32_t*)&v_len.c4, NULL, 1},
                                    {TSDB_DATA_TYPE_BINARY, &v.c6, (int32_t*)&v_len.c6, NULL, 1},
                                    {TSDB_DATA_TYPE_SMALLINT, &v.c7, (int32_t*)&v_len.c7, NULL, 1},
                                    {TSDB_DATA_TYPE_TINYINT, &v.c8, (int32_t*)&v_len.c8, NULL, 1},
                                    {TSDB_DATA_TYPE_BOOL, &v.c9, (int32_t*)&v_len.c9, NULL, 1},
                                    {TSDB_DATA_TYPE_NCHAR, &v.c10, (int32_t*)&v_len.c10, NULL, 1},
                                    {TSDB_DATA_TYPE_GEOMETRY, outputGeom1, (int32_t*)&size1, NULL, 1}};

  TAOS_STMT2_BIND params_cols[9] = {{TSDB_DATA_TYPE_TIMESTAMP, &v.c1, (int32_t*)&v_len.c1, NULL, 1},
                                    {TSDB_DATA_TYPE_BIGINT, &v.c3, (int32_t*)&v_len.c3, NULL, 1},
                                    {TSDB_DATA_TYPE_FLOAT, &v.c4, (int32_t*)&v_len.c4, NULL, 1},
                                    {TSDB_DATA_TYPE_DOUBLE, &v.c5, (int32_t*)&v_len.c5, NULL, 1},
                                    {TSDB_DATA_TYPE_BINARY, &v.c6, (int32_t*)&v_len.c6, NULL, 1},
                                    {TSDB_DATA_TYPE_SMALLINT, &v.c7, (int32_t*)&v_len.c7, NULL, 1},
                                    {TSDB_DATA_TYPE_TINYINT, &v.c8, (int32_t*)&v_len.c8, NULL, 1},
                                    {TSDB_DATA_TYPE_NCHAR, &v.c10, (int32_t*)&v_len.c10, NULL, 1},
                                    {TSDB_DATA_TYPE_GEOMETRY, outputGeom1, (int32_t*)&size1, NULL, 1}};

  TAOS_STMT2_OPTION option[2] = {{0, false, false, NULL, NULL}, {0, true, true, NULL, NULL}};
  for (int k = 0; k < 2; k++) {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[k]);
    ASSERT_NE(stmt, nullptr);
    char* stmt_sql =
        "insert into stmt2_testdb_19.? using stb tags(1591060628000,?,?,?,4.0,?,?,?,?,?,?)values "
        "(?,2,?,?,?,?,?,?,1,?,?)";
    code = taos_stmt2_prepare(stmt, stmt_sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    // code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    // checkError(stmt, code, __FILE__, __LINE__);
    // ASSERT_EQ(fieldNum, 19);

    TAOS_STMT2_BIND* tags[2] = {&params_tags[0], &params_tags[0]};
    TAOS_STMT2_BIND* cols[2] = {&params_cols[0], &params_cols[0]};
    TAOS_STMT2_BINDV bindv = {2, &tbname[0 + k * 4], &tags[0], &cols[0]};
    for (int i = 0; i < 3; i++) {
      v.c1 += i * 10000;
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
    }
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 6);

    stmt_sql =
        "insert into stb (tbname, tts, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, ts, c1, c2, c3, c4, c5, c6, c7, c8, "
        "c9, c10) values(?,1591060628000,?,?,?,4.0,?,?,?,?,?,?,?,2,?,?,?,?,?,?,1,?,?)";
    code = taos_stmt2_prepare(stmt, stmt_sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    bindv = {2, &tbname[2 + k * 4], &tags[0], &cols[0]};
    for (int i = 0; i < 3; i++) {
      v.c1 += i * 10000;
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
    }
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 6);
    taos_stmt2_close(stmt);
  }

  geosFreeBuffer(outputGeom1);
  do_query(taos, "drop database if exists stmt2_testdb_19");
  taos_close(taos);
}

// TD-33582
TEST(stmt2Case, errcode) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_14");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_14");
  do_query(taos, "use stmt2_testdb_14");
  do_query(taos, "create table stmt2_testdb_14.tb (ts timestamp, b binary(10))");
  do_query(taos, "CREATE STABLE `stmt2_testdb_14`.stb (ts TIMESTAMP, b INT) TAGS (tt TIMESTAMP, tb INT)");

  {
    TAOS_STMT2_OPTION option = {0};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    char* sql = "insert into ? values(?,?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    ASSERT_EQ(code, TSDB_CODE_TSC_STMT_TBNAME_ERROR);

    // get fail dont influence the next stmt prepare
    sql = "insert into ? (ts, name) values (?, ?)";
    code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    taos_stmt2_close(stmt);
  }
  // TD-38218, skip bind data before execute
  {
    TAOS_STMT2_OPTION option = {0};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    char* sql = "insert into tb values(?,?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);

    taos_stmt2_close(stmt);
  }

  // already support in TS-7067
  // {
  //   TAOS_STMT2_OPTION option = {0, false, true, NULL, NULL};
  //   TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  //   ASSERT_NE(stmt, nullptr);
  //   char* sql = "insert into stmt2_testdb_14.? using stmt2_testdb_14.stb tags(now, 1) values(?, ?)";
  //   int   code = taos_stmt2_prepare(stmt, sql, 0);
  //   checkError(stmt, code, __FILE__, __LINE__);
  //   char* wrong_tbname[2] = {"tb-1", "tb-2"};

  //   int64_t          ts[2] = {1591060628000, 1591060629000};
  //   int32_t          values[2] = {100, 200};
  //   int              t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  //   int              val_len[2] = {sizeof(int32_t), sizeof(int32_t)};
  //   TAOS_STMT2_BIND  col[4] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
  //                              {TSDB_DATA_TYPE_INT, &values[0], &val_len[0], NULL, 2},
  //                              {TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
  //                              {TSDB_DATA_TYPE_INT, &values[0], &val_len[0], NULL, 2}};
  //   TAOS_STMT2_BIND* cols[2] = {&col[0], &col[2]};
  //   TAOS_STMT2_BINDV bindv = {2, &wrong_tbname[0], NULL, &cols[0]};
  //   code = taos_stmt2_bind_param(stmt, &bindv, -1);
  //   ASSERT_EQ(code, TSDB_CODE_TSC_STMT_TBNAME_ERROR);

  //   taos_stmt2_close(stmt);
  // }
  // TS-7502
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    char* sql = "insert into ? using stb tags(now(),1) values(now(),?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    // int             fieldNum = 0;
    // TAOS_FIELD_ALL* pFields = NULL;
    // code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    // checkError(stmt, code, __FILE__, __LINE__);

    int   values[2] = {100, 200};
    int   val_len[2] = {sizeof(int32_t), sizeof(int32_t)};
    char* tbname[2] = {"tb3", "tb4"};

    TAOS_STMT2_BIND  param = {TSDB_DATA_TYPE_INT, &values[0], &val_len[0], NULL, 2};
    TAOS_STMT2_BIND* params[2] = {&param, &param};
    for (int i = 0; i < 3; i++) {
      TAOS_STMT2_BINDV bindv = {2, &tbname[0], NULL, &params[0]};
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      code = taos_stmt2_exec(stmt, NULL);
      checkError(stmt, code, __FILE__, __LINE__);
    }
    taos_stmt2_close(stmt);
  }

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_14");
  taos_close(taos);
}

TEST(stmt2Case, usage_error) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_17");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_17");
  do_query(taos, "use stmt2_testdb_17");
  do_query(taos, "CREATE TABLE stmt2_testdb_17.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");

  TAOS_STMT2_OPTION option = {0};
  // get fields error
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  char* sql = "delete from ntb where ts=?";
  int   code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "stmt only support 'SELECT' or 'INSERT'");
  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "stmt only support 'SELECT' or 'INSERT'");
  // bind error

  int              t64_len[1] = {sizeof(int64_t)};
  int64_t          ts = 1591060628000;
  TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, t64_len, NULL, 1};
  TAOS_STMT2_BIND* params1 = &params;

  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &params1};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "stmt only support 'SELECT' or 'INSERT'");

  taos_stmt2_close(stmt);
  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_17");
  taos_close(taos);
}

void stmtAsyncBindCb(void* param, TAOS_RES* pRes, int code) {
  bool* finish = (bool*)param;
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  taosMsleep(500);
  *finish = true;
  return;
}

void stmtAsyncQueryCb2(void* param, TAOS_RES* pRes, int code) {
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  taosMsleep(500);
  return;
}

void stmtAsyncBindCb2(void* param, TAOS_RES* pRes, int code) {
  bool* finish = (bool*)param;
  taosMsleep(500);
  *finish = true;
  return;
}

void stmt2_async_test(std::atomic<bool>& stop_task) {
  int CTB_NUMS = 2;
  int ROW_NUMS = 2;
  int CYC_NUMS = 2;

  TAOS*             taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb2, NULL};
  char*             sql = "insert into ? values(?,?)";

  do_query(taos, "drop database if exists stmt2_testdb_15");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_15");
  do_query(taos, "create stable stmt2_testdb_15.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos, "use stmt2_testdb_15");

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);
  int total_affected = 0;

  // tbname
  char** tbs = (char**)taosMemoryMalloc(CTB_NUMS * sizeof(char*));
  for (int i = 0; i < CTB_NUMS; i++) {
    tbs[i] = (char*)taosMemoryMalloc(sizeof(char) * 20);
    sprintf(tbs[i], "ctb_%d", i);
    char* tmp = (char*)taosMemoryMalloc(sizeof(char) * 100);
    sprintf(tmp, "create table stmt2_testdb_15.%s using stmt2_testdb_15.stb tags(0, 'after')", tbs[i]);
    do_query(taos, tmp);
  }
  // params
  TAOS_STMT2_BIND** paramv = (TAOS_STMT2_BIND**)taosMemoryMalloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
  // col params
  int64_t** ts = (int64_t**)taosMemoryMalloc(CTB_NUMS * sizeof(int64_t*));
  char**    b = (char**)taosMemoryMalloc(CTB_NUMS * sizeof(char*));
  int*      ts_len = (int*)taosMemoryMalloc(ROW_NUMS * sizeof(int));
  int*      b_len = (int*)taosMemoryMalloc(ROW_NUMS * sizeof(int));
  for (int i = 0; i < ROW_NUMS; i++) {
    ts_len[i] = sizeof(int64_t);
    b_len[i] = 1;
  }
  for (int i = 0; i < CTB_NUMS; i++) {
    ts[i] = (int64_t*)taosMemoryMalloc(ROW_NUMS * sizeof(int64_t));
    b[i] = (char*)taosMemoryMalloc(ROW_NUMS * sizeof(char));
    for (int j = 0; j < ROW_NUMS; j++) {
      ts[i][j] = 1591060628000 + 100000 + j;
      b[i][j] = 'a' + j;
    }
  }
  // bind params
  for (int i = 0; i < CTB_NUMS; i++) {
    // create col params
    paramv[i] = (TAOS_STMT2_BIND*)taosMemoryMalloc(2 * sizeof(TAOS_STMT2_BIND));
    paramv[i][0] = {TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};
    paramv[i][1] = {TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};
  }

  // case 1 : bind_a->exec_a->bind_a->exec_a->...
  {
    printf("case 1 : bind_a->exec_a->bind_a->exec_a->...\n");
    for (int r = 0; r < CYC_NUMS; r++) {
      // bind
      TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, NULL, paramv};
      bool             finish = false;
      code = taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindCb, (void*)&finish);

      checkError(stmt, code, __FILE__, __LINE__);

      // exec
      code = taos_stmt2_exec(stmt, NULL);
      checkError(stmt, code, __FILE__, __LINE__);
    }
  }

  // case 2 : bind_a->bind_a->bind_a->exec_a->...
  {
    printf("case 2 : bind_a->bind_a->bind_a->exec_a->...\n");
    for (int r = 0; r < CYC_NUMS; r++) {
      // bind params
      TAOS_STMT2_BIND** paramv = (TAOS_STMT2_BIND**)taosMemoryMalloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
      for (int i = 0; i < CTB_NUMS; i++) {
        // create col params
        paramv[i] = (TAOS_STMT2_BIND*)taosMemoryMalloc(2 * sizeof(TAOS_STMT2_BIND));
        paramv[i][0] = {TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};
        paramv[i][1] = {TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};
      }
      // bind
      TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, NULL, paramv};
      bool             finish = false;
      code = taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindCb, (void*)&finish);
      while (!finish) {
        taosMsleep(100);
      }
      checkError(stmt, code, __FILE__, __LINE__);
    }
    // exec
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
  }

  // case 3 : bind->exec_a->bind->exec_a->...
  {
    printf("case 3 : bind->exec_a->bind->exec_a->...\n");
    for (int r = 0; r < CYC_NUMS; r++) {
      // bind
      TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, NULL, paramv};
      bool             finish = false;
      code = taos_stmt2_bind_param(stmt, &bindv, -1);

      checkError(stmt, code, __FILE__, __LINE__);

      // exec
      code = taos_stmt2_exec(stmt, NULL);
      checkError(stmt, code, __FILE__, __LINE__);
    }
  }

  // case 4 : bind_a->close
  {
    printf("case 4 : bind_a->close\n");
    // bind
    TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, NULL, paramv};
    bool             finish = false;
    code = taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindCb, (void*)&finish);
    checkError(stmt, code, __FILE__, __LINE__);
    taos_stmt2_close(stmt);
    checkError(stmt, code, __FILE__, __LINE__);
  }

  // case 5 : bind_a->exec_a->close
  {
    printf("case 5 : bind_a->exec_a->close\n");
    // init
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    // bind
    TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, NULL, paramv};
    bool             finish = false;
    code = taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindCb, (void*)&finish);
    checkError(stmt, code, __FILE__, __LINE__);
    // exec
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    // close
    taos_stmt2_close(stmt);
    checkError(stmt, code, __FILE__, __LINE__);
  }

  option = {0, false, false, NULL, NULL};
  stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  // case 6 : bind_a->exec->bind_a->exec->...
  {
    printf("case 6 : bind_a->exec->bind_a->exec->...\n");
    // init

    checkError(stmt, code, __FILE__, __LINE__);
    for (int r = 0; r < CYC_NUMS; r++) {
      // bind
      TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, NULL, paramv};
      bool             finish = false;
      code = taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindCb, (void*)&finish);
      checkError(stmt, code, __FILE__, __LINE__);
      // exec
      code = taos_stmt2_exec(stmt, NULL);
      checkError(stmt, code, __FILE__, __LINE__);
    }
  }

  // case 7 (error:no wait error) : bind_a->bind_a
  {
    printf("case 7 (error:no wait error) : bind_a->bind_a\n");
    // bind
    TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, NULL, paramv};
    bool             finish = false;
    code = taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindCb2, (void*)&finish);
    checkError(stmt, code, __FILE__, __LINE__);
    taosMsleep(200);
    code = taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindCb2, (void*)&finish);
    ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
    while (!finish) {
      taosMsleep(100);
    }
  }
  // close
  taos_stmt2_close(stmt);

  // free memory
  for (int i = 0; i < CTB_NUMS; i++) {
    taosMemoryFree(paramv[i]);
    taosMemoryFree(ts[i]);
    taosMemoryFree(b[i]);
  }
  taosMemoryFree(ts);
  taosMemoryFree(b);
  taosMemoryFree(ts_len);
  taosMemoryFree(b_len);
  taosMemoryFree(paramv);
  for (int i = 0; i < CTB_NUMS; i++) {
    taosMemoryFree(tbs[i]);
  }
  taosMemoryFree(tbs);
  stop_task = true;
}

// TEST(stmt2Case, async_order) {
//   std::atomic<bool> stop_task(false);
//   std::thread       t(stmt2_async_test, std::ref(stop_task));

//   // 等待 60 秒钟
//   auto start_time = std::chrono::steady_clock::now();
//   while (!stop_task) {
//     auto elapsed_time = std::chrono::steady_clock::now() - start_time;
//     if (std::chrono::duration_cast<std::chrono::seconds>(elapsed_time).count() > 100) {
//       if (t.joinable()) {
//         t.detach();
//       }
//       FAIL() << "Test[stmt2_async_test] timed out";
//       break;
//     }
//     std::this_thread::sleep_for(std::chrono::seconds(1));  // 每 1s 检查一次
//   }
//   if (t.joinable()) {
//     t.join();
//   }
// }

TEST(stmt2Case, rowformat_bind) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_16");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_16");
  do_query(
      taos,
      "create stable stmt2_testdb_16.stb(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(8), c6 "
      "smallint, c7 "
      "tinyint, c8 bool, c9 nchar(8), c10 geometry(256))TAGS(tts timestamp, t1 int, t2 bigint, t3 float, t4 double, t5 "
      "binary(8), t6 smallint, t7 tinyint, t8 bool, t9 nchar(8), t10 geometry(256))");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int       code = 0;
  uintptr_t c10len = 0;
  struct {
    int64_t       c1;
    int32_t       c2;
    int64_t       c3;
    float         c4;
    double        c5;
    unsigned char c6[8];
    int16_t       c7;
    int8_t        c8;
    int8_t        c9;
    char          c10[32];
  } v = {1591060628000, 1, 2, 3.0, 4.0, "abcdef", 5, 6, 7, "ijnop"};

  struct {
    int32_t c1;
    int32_t c2;
    int32_t c3;
    int32_t c4;
    int32_t c5;
    int32_t c6;
    int32_t c7;
    int32_t c8;
    int32_t c9;
    int32_t c10;
  } v_len = {sizeof(int64_t), sizeof(int32_t),
             sizeof(int64_t), sizeof(float),
             sizeof(double),  8,
             sizeof(int16_t), sizeof(int8_t),
             sizeof(int8_t),  8};
  TAOS_STMT2_BIND params[11];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].length = (int32_t*)&v_len.c1;
  params[0].buffer = &v.c1;
  params[0].is_null = NULL;
  params[0].num = 1;

  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer = &v.c2;
  params[1].length = (int32_t*)&v_len.c2;
  params[1].is_null = NULL;
  params[1].num = 1;

  params[2].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[2].buffer = &v.c3;
  params[2].length = (int32_t*)&v_len.c3;
  params[2].is_null = NULL;
  params[2].num = 1;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer = &v.c4;
  params[3].length = (int32_t*)&v_len.c4;
  params[3].is_null = NULL;
  params[3].num = 1;

  params[4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[4].buffer = &v.c5;
  params[4].length = (int32_t*)&v_len.c5;
  params[4].is_null = NULL;
  params[4].num = 1;

  params[5].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[5].buffer = &v.c6;
  params[5].length = (int32_t*)&v_len.c6;
  params[5].is_null = NULL;
  params[5].num = 1;

  params[6].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[6].buffer = &v.c7;
  params[6].length = (int32_t*)&v_len.c7;
  params[6].is_null = NULL;
  params[6].num = 1;

  params[7].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[7].buffer = &v.c8;
  params[7].length = (int32_t*)&v_len.c8;
  params[7].is_null = NULL;
  params[7].num = 1;

  params[8].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[8].buffer = &v.c9;
  params[8].length = (int32_t*)&v_len.c9;
  params[8].is_null = NULL;
  params[8].num = 1;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer = &v.c10;
  params[9].length = (int32_t*)&v_len.c10;
  params[9].is_null = NULL;
  params[9].num = 1;

  unsigned char* outputGeom1;
  size_t         size1;
  initCtxMakePoint();
  code = doMakePoint(1.000, 2.000, &outputGeom1, &size1);
  checkError(stmt, code, __FILE__, __LINE__);
  params[10].buffer_type = TSDB_DATA_TYPE_GEOMETRY;
  params[10].buffer = outputGeom1;
  params[10].length = (int32_t*)&size1;
  params[10].is_null = NULL;
  params[10].num = 1;

  char* stmt_sql = "insert into stmt2_testdb_16.? using stb tags(?,?,?,?,?,?,?,?,?,?,?)values (?,?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt2_prepare(stmt, stmt_sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  char*            tbname[1] = {"tb1"};
  TAOS_STMT2_BIND* tags = &params[0];
  TAOS_STMT2_BIND* cols = &params[0];
  TAOS_STMT2_BINDV bindv = {1, &tbname[0], &tags, &cols};
  code = taos_stmt2_bind_param(stmt, &bindv, -2);
  checkError(stmt, code, __FILE__, __LINE__);

  int affected_rows;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected_rows, 1);

  int64_t ts2 = 1591060628000;
  params[0].buffer = &ts2;
  code = taos_stmt2_bind_param(stmt, &bindv, -2);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected_rows, 1);

  params[0].buffer = &ts2;
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);

  geosFreeBuffer(outputGeom1);
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_16");
  taos_close(taos);
}

// TS-7047
void stmtAsyncQueryCb3(void* param, TAOS_RES* pRes, int code) {
  ASSERT_EQ(code, TSDB_CODE_TDB_TABLE_NOT_EXIST);
  return;
}

TEST(stmt2Case, exec_retry) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb3, NULL};
  do_query(taos, "drop database if exists stmt2_testdb_21");
  do_query(taos, "create database stmt2_testdb_21");
  do_query(taos, "create stable stmt2_testdb_21.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  // do_query(taos, "create stable stmt2_testdb_21.stb2 (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
           "insert into stmt2_testdb_21.tb1 using stmt2_testdb_21.stb tags(1, 'after') "
           "values(1591060628000, 'abcde')");
  // do_query(taos, "create table stmt2_testdb_8.ntb(ts timestamp, b binary(10))");
  do_query(taos, "use stmt2_testdb_21");
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  // test fixed tbname and bind per col
  {
    int         total_affected_rows = 0;
    const char* sql = "insert into stmt2_testdb_21.stb (tbname,ts,b)values(?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    int64_t          ts[3] = {1591060628000, 1591060628001, 1591060628002};
    int              t64_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
    int              b_len[3] = {5, 5, 5};
    char*            tbname = "tb1";
    TAOS_STMT2_BIND  params1[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 3},
                                   {TSDB_DATA_TYPE_BINARY, (void*)"abcdefghijklmnopqrstuvwxyz", &b_len[0], NULL, 3}};
    TAOS_STMT2_BIND* paramv1 = &params1[0];

    TAOS_STMT2_BINDV bindv1 = {1, &tbname, NULL, &paramv1};

    code = taos_stmt2_bind_param(stmt, &bindv1, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    // wait async bind finish
    taosMsleep(2000);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb1");

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
  }

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_21");
}

void stmtAsyncQueryCb4(void* param, TAOS_RES* pRes, int code) { ASSERT_EQ(code, TSDB_CODE_SUCCESS); }

TEST(stmt2Case, core) {
  printf("stmt2Test: select COUNT(1) from (select LAST(operate_time) ...) with params\n");

  TAOS* taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists ivs");
  do_query(taos, "create database ivs");
  do_query(taos, "use ivs");
  do_query(taos,
           "create table alarm_operate_info ("
           "operate_time timestamp, "
           "alarm_id int, "
           "operator_id int, "
           "operator_name binary(32), "
           "operator_info binary(64), "
           "operator_status int, "
           "device_id int)");

  do_query(taos,
           "insert into alarm_operate_info values"
           "(1591060628000, 1, 100, '张三', 'info1', 0, 10),"
           "(1591060629000, 1, 101, '李四', 'info2', 1, 10),"
           "(1591060630000, 2, 102, '王五', 'info3', 0, 11)");

  TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb4, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql =
      "select COUNT(1) from ("
      " select LAST(operate_time) as operate_time, alarm_id, operator_id, operator_name, operator_info, "
      "operator_status, device_id"
      " from ivs.alarm_operate_info"
      " WHERE operate_time >= ? and operate_time <= ?"
      " PARTITION BY alarm_id"
      " ORDER BY operate_time desc"
      ")";

  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, 2);

  int64_t ts_begin[1] = {1591060628000};
  int64_t ts_end[1] = {1591060630000};
  int32_t len[1] = {sizeof(int64_t)};

  TAOS_STMT2_BIND params[2] = {0};
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer = &ts_begin[0];
  params[0].length = &len[0];
  params[0].is_null = NULL;
  params[0].num = 1;

  params[1].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[1].buffer = &ts_end[0];
  params[1].length = &len[0];
  params[1].is_null = NULL;
  params[1].num = 1;

  TAOS_STMT2_BIND* c = &params[0];

  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &c};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  taos_stmt2_close(stmt);

  do_query(taos, "drop database if exists ivs");
  taos_close(taos);
}

// TS-7067
TEST(stmt2Case, tbname) {
  TAOS* taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_22");
  do_query(taos, "create database stmt2_testdb_22");
  do_query(taos, "use stmt2_testdb_22");
  do_query(taos, "create table stmt2_testdb_22.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

  TAOS_STMT2_OPTION option[2] = {{0, false, false, NULL, NULL}, {0, true, true, NULL, NULL}};
  for (int i = 0; i < 2; i++) {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[i]);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "insert into stmt2_testdb_22.stb (tbname,ts,b,t1,t2)values(?,?,?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 5);

    int     t1 = 1;
    char*   t2 = "abc";
    int32_t t2_len = 3;
    int32_t t1_len = sizeof(int);

    int64_t ts[2] = {1591060628000, 1591060628001};
    int32_t len[2] = {sizeof(int64_t), sizeof(int64_t)};

    char* tbname[2] = {"CTb-1", "中文表名"};

    TAOS_STMT2_BIND tag[2] = {{TSDB_DATA_TYPE_INT, &t1, &t1_len, NULL, 1},
                              {TSDB_DATA_TYPE_BINARY, &t2, &t2_len, NULL, 1}};

    TAOS_STMT2_BIND col[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &len[0], NULL, 2},
                              {TSDB_DATA_TYPE_BINARY, &tbname[0], &len[0], NULL, 2}};

    TAOS_STMT2_BIND* pTag[2] = {&tag[0], &tag[0]};
    TAOS_STMT2_BIND* pCol[2] = {&col[0], &col[0]};

    TAOS_STMT2_BINDV bindv = {2, &tbname[0], &pTag[0], &pCol[0]};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affected_rows = 0;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);

    TAOS_RES* result = taos_query(taos, "select tbname from stmt2_testdb_22.stb group by tbname order by tbname");
    ASSERT_NE(result, nullptr);

    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_EQ(strncmp((char*)row[0], "CTb-1", 5), 0);

    row = taos_fetch_row(result);
    ASSERT_EQ(strncmp((char*)row[0], "中文表名", 4), 0);

    taos_free_result(result);
    taos_stmt2_close(stmt);
    do_query(taos, "drop table if exists stmt2_testdb_22.tb1");
    do_query(taos, "drop table if exists stmt2_testdb_22.tb2");
  }
}

// TS-7074
TEST(stmt2Case, no_tag) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_23");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_23");
  do_query(taos, "create stable stmt2_testdb_23.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(
      taos,
      "insert into stmt2_testdb_23.tb_exist using stmt2_testdb_23.stb tags(1, 'tag1') values(1591060628000, 'abc')");

  const char* sql = "INSERT INTO stmt2_testdb_23.stb (tbname,ts,b) VALUES (?,?,?)";
  char*       tbname[2] = {"tb_exist", "tb_not_exist"};
  int64_t     ts = 1591060628000;
  int         t64_len = sizeof(int64_t);
  char*       b = "abc";
  int         b_len = 3;

  TAOS_STMT2_BIND params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1},
                               {TSDB_DATA_TYPE_BINARY, b, &b_len, NULL, 1}};
  // interlace=1
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    printf("case 1 : interlace=1] \n");
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1},
                                 {TSDB_DATA_TYPE_BINARY, b, &b_len, NULL, 1}};

    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, &tbname[1], NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);

    taos_stmt2_close(stmt);
  }

  // interlace=0
  {
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    printf("case 2 : interlace=0] \n");
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1},
                                 {TSDB_DATA_TYPE_BINARY, b, &b_len, NULL, 1}};

    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, &tbname[1], NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);

    taos_stmt2_close(stmt);
  }

  // interlace=0 && first table exist
  {
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    printf("case 3 : interlace=0 && first table exist] \n");
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[0]};
    TAOS_STMT2_BINDV bindv = {2, &tbname[0], NULL, &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);

    taos_stmt2_close(stmt);
  }
  // interlace=1 && first table exist
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    printf("case 4 : interlace=1 && first table exist] \n");
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[0]};
    TAOS_STMT2_BINDV bindv = {2, &tbname[0], NULL, &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);

    taos_stmt2_close(stmt);
  }

  int             t1 = 1;
  char*           t2 = "abc";
  int             t1_len = sizeof(int);
  int             t2_len = 3;
  TAOS_STMT2_BIND tag[2] = {{TSDB_DATA_TYPE_INT, &t1, &t1_len, NULL, 1}, {TSDB_DATA_TYPE_BINARY, t2, &t2_len, NULL, 1}};
  TAOS_STMT2_BIND* pTag = &tag[0];

  // interlace=0 && with tag
  {
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    printf("case 5 : interlace=0 && with tag] \n");
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[0]};
    TAOS_STMT2_BINDV bindv = {1, &tbname[1], &pTag, &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);

    taos_stmt2_close(stmt);
  }

  // interlace=1 && with tag
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    printf("case 6 : interlace=1 && with tag] \n");
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[0]};
    TAOS_STMT2_BINDV bindv = {1, &tbname[1], &pTag, &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);

    taos_stmt2_close(stmt);
  }
  do_query(taos, "drop database if exists stmt2_testdb_23");
  taos_close(taos);
}

#pragma GCC diagnostic pop
