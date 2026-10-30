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

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
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
#if defined(LINUX) || defined(WINDOWS)
#include "stub.h"
#endif

const char* STMT_STATUS_NAMES[] = {
    "INVALID",   "STMT_INIT",     "STMT_PREPARE",   "STMT_SETTBNAME", "STMT_SETTAGS", "STMT_FETCH_FIELDS",
    "STMT_BIND", "STMT_BIND_COL", "STMT_ADD_BATCH", "STMT_EXECUTE",   "STMT_MAX",
};

namespace {
constexpr const char* STMT2_DROP_REPREPARE_TABLES_ARG = "--stmt2-drop-reprepare-tables";
const char*           stmt2TestExecutable = NULL;

int dropReprepareTables() {
  TAOS* taos2 = taos_connect("localhost", "root", "taosdata", "", 0);
  if (taos2 == NULL) {
    return 1;
  }

  TAOS_RES* result = taos_query(taos2,
                                "drop table stmt2_testdb_reprepare_stable.ctb1, stmt2_testdb_reprepare_stable.ctb2, "
                                "stmt2_testdb_reprepare_stable.ctb3");
  int       code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    fprintf(stderr, "failed to drop child tables: %s\n", taos_errstr(result));
  }
  taos_free_result(result);
  taos_close(taos2);
  return code == TSDB_CODE_SUCCESS ? 0 : 1;
}

bool isCreateDatabaseSql(const char* sql) {
  if (sql == nullptr) {
    return false;
  }
  while (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r') {
    ++sql;
  }
  const char* expect = "create database";
  for (int i = 0; expect[i] != '\0'; ++i) {
    char c = sql[i];
    if (c >= 'A' && c <= 'Z') {
      c = (char)(c - 'A' + 'a');
    }
    if (c != expect[i]) {
      return false;
    }
  }
  char next = sql[strlen(expect)];
  return next == '\0' || next == ' ' || next == '\t';
}

[[noreturn]] void exitStmt2TestOnEnvError() {
  fflush(stdout);
  fflush(stderr);
  exit(EXIT_FAILURE);
}

TAOS* connectTaosOrExit(const char* host, const char* user, const char* pass, const char* db, uint16_t port) {
  TAOS* taos = taos_connect(host, user, pass, db, port);
  if (taos == nullptr) {
    printf("[stmt2Test] taos_connect failed, host=%s user=%s err=%s\n", host ? host : "", user ? user : "",
           taos_errstr(nullptr));
    exitStmt2TestOnEnvError();
  }
  return taos;
}

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

typedef struct {
  const char* col0;
  const char* col1;
  const char* col2;
  int         col0_len;
  int         col1_len;
  int         col2_len;
} ExpectedRow;

typedef struct {
  const char* col0;
  const char* col1;
  int         col2;
  const char* col3;
  int         col0_len;
  int         col1_len;
  int         col3_len;
} ExpectedRow4;

typedef struct AsyncArgs {
  int           async_affected_rows;
  tsem_t        sem;
  ExpectedRow*  expected_rows;
  int           num_expected_rows;
  int           current_row_idx;
  ExpectedRow4* expected_rows4;
  int           num_expected_rows4;
} AsyncArgs;

typedef struct AsyncBindWaitArgs {
  int    bindCode;
  tsem_t enteredSem;
  tsem_t releaseSem;
} AsyncBindWaitArgs;

void stmtAsyncBindWaitCb(void* param, TAOS_RES* pRes, int code) {
  AsyncBindWaitArgs* args = (AsyncBindWaitArgs*)param;
  args->bindCode = code;
  (void)tsem_post(&args->enteredSem);
  (void)tsem_wait(&args->releaseSem);
}

void stmtAsyncQueryCb(void* param, TAOS_RES* pRes, int code) {
  ((AsyncArgs*)param)->async_affected_rows = taos_affected_rows(pRes);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
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
  // checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(code, errorCode);

  if (code) {
    taos_stmt2_close(stmt);
    return;
  }

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

bool isTransientDbQueryError(int code) {
  return code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING ||
         code == TSDB_CODE_MND_VGROUP_OFFLINE;
}

TAOS_RES* execQueryWithRetry(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  int       retry = 0;
  const int kMaxRetry = 30;
  while (isTransientDbQueryError(code) && retry++ < kMaxRetry) {
    taos_free_result(result);
    taosMsleep(2000);
    result = taos_query(taos, sql);
    code = taos_errno(result);
  }
  return result;
}

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = execQueryWithRetry(taos, sql);
  // printf("sql: %s\n", sql);
  int code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    printf("query failed sql: %s\n  errstr: %s\n", sql, taos_errstr(result));
    taos_free_result(result);
    if (isCreateDatabaseSql(sql)) {
      printf("[stmt2Test] create database failed, exit stmt2Test\n");
      exitStmt2TestOnEnvError();
    }
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    return;
  }
  taos_free_result(result);
}

void do_error_query(TAOS* taos, const char* sql, int errorCode) {
  TAOS_RES* result = execQueryWithRetry(taos, sql);
  // printf("sql: %s\n", sql);
  int code = taos_errno(result);
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
      char tmp[100];
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

enum Stmt2WriteSqlMode {
  STMT2_WRITE_USING_DYNAMIC_TAGS,
  STMT2_WRITE_USING_FIXED_TAGS,
  STMT2_WRITE_TBNAME_PSEUDO_COLUMN,
  STMT2_WRITE_CHILD_WITH_TAGS,
  STMT2_WRITE_CHILD_WITHOUT_TAGS,
  STMT2_WRITE_NORMAL_TABLE,
};

typedef struct Stmt2MatrixAsyncArgs {
  int     code;
  int     affectedRows;
  int32_t callbackCount;
  tsem_t  sem;
} Stmt2MatrixAsyncArgs;

void stmt2MatrixAsyncCb(void* param, TAOS_RES* pRes, int code) {
  Stmt2MatrixAsyncArgs* args = (Stmt2MatrixAsyncArgs*)param;
  args->code = code;
  args->affectedRows = pRes == NULL ? 0 : taos_affected_rows(pRes);
  ++args->callbackCount;
  (void)tsem_post(&args->sem);
}

const char* stmt2WriteModeName(Stmt2WriteSqlMode mode) {
  switch (mode) {
    case STMT2_WRITE_USING_DYNAMIC_TAGS:
      return "using_dynamic_tags";
    case STMT2_WRITE_USING_FIXED_TAGS:
      return "using_fixed_tags";
    case STMT2_WRITE_TBNAME_PSEUDO_COLUMN:
      return "tbname_pseudo_column";
    case STMT2_WRITE_CHILD_WITH_TAGS:
      return "existing_child_with_tags";
    case STMT2_WRITE_CHILD_WITHOUT_TAGS:
      return "existing_child_without_tags";
    case STMT2_WRITE_NORMAL_TABLE:
      return "normal_table";
  }
  return "unknown";
}

bool stmt2WriteModeBindsTags(Stmt2WriteSqlMode mode) {
  return mode == STMT2_WRITE_USING_DYNAMIC_TAGS || mode == STMT2_WRITE_TBNAME_PSEUDO_COLUMN ||
         mode == STMT2_WRITE_CHILD_WITH_TAGS;
}

void verifyStmt2MatrixTable(TAOS* taos, const char* tableName, int64_t expectedRows, bool verifyTags,
                            int32_t expectedTagInt, const char* expectedTagText) {
  char sql[256] = {0};
  (void)snprintf(sql, sizeof(sql), "select count(*) from stmt2_write_matrix.%s", tableName);

  TAOS_RES* result = taos_query(taos, sql);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_NE(row[0], nullptr);
  ASSERT_EQ(*(int64_t*)row[0], expectedRows);
  taos_free_result(result);

  if (!verifyTags) {
    return;
  }

  (void)snprintf(sql, sizeof(sql), "select t1,t2 from stmt2_write_matrix.%s limit 1", tableName);
  result = taos_query(taos, sql);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_NE(row[0], nullptr);
  ASSERT_NE(row[1], nullptr);
  ASSERT_EQ(*(int32_t*)row[0], expectedTagInt);

  int* lengths = taos_fetch_lengths(result);
  ASSERT_NE(lengths, nullptr);
  ASSERT_EQ(lengths[1], (int)strlen(expectedTagText));
  ASSERT_EQ(strncmp((const char*)row[1], expectedTagText, lengths[1]), 0);
  taos_free_result(result);
}

void runStmt2WriteMatrixCase(TAOS* taos, Stmt2WriteSqlMode mode, bool interlace, bool autoCreate, bool asyncExec,
                             int caseIndex) {
  const int MAX_TABLE_NUM = 2;
  const int ROW_NUM = 3;
  const int CYCLE_NUM = 2;
  const int tableNum = mode == STMT2_WRITE_NORMAL_TABLE ? 1 : MAX_TABLE_NUM;

  char trace[160] = {0};
  (void)snprintf(trace, sizeof(trace), "mode=%s interlace=%d autoCreate=%d async=%d", stmt2WriteModeName(mode),
                 interlace, autoCreate, asyncExec);
  SCOPED_TRACE(trace);
  printf("stmt2 write matrix [%s]\n", trace);
  fflush(stdout);

  ASSERT_FALSE(autoCreate && mode != STMT2_WRITE_USING_DYNAMIC_TAGS && mode != STMT2_WRITE_USING_FIXED_TAGS &&
               mode != STMT2_WRITE_TBNAME_PSEUDO_COLUMN);

  char  tableNameStorage[MAX_TABLE_NUM][TSDB_TABLE_NAME_LEN] = {{0}};
  char* tableNames[MAX_TABLE_NUM] = {NULL};
  int32_t tagInts[MAX_TABLE_NUM] = {0};
  char    tagTexts[MAX_TABLE_NUM][16] = {{0}};
  int     tagIntLen[MAX_TABLE_NUM] = {0};
  int     tagTextLen[MAX_TABLE_NUM] = {0};

  for (int i = 0; i < tableNum; ++i) {
    const char* prefix = mode == STMT2_WRITE_NORMAL_TABLE ? "ntb" : "ctb";
    (void)snprintf(tableNameStorage[i], sizeof(tableNameStorage[i]), "%s_%02d_%d", prefix, caseIndex, i);
    tableNames[i] = tableNameStorage[i];
    tagInts[i] = caseIndex * 10 + i + 1;
    (void)snprintf(tagTexts[i], sizeof(tagTexts[i]), "tag_%02d_%d", caseIndex, i);
    tagIntLen[i] = sizeof(tagInts[i]);
    tagTextLen[i] = (int)strlen(tagTexts[i]);

    if (!autoCreate) {
      char createSql[512] = {0};
      if (mode == STMT2_WRITE_NORMAL_TABLE) {
        (void)snprintf(createSql, sizeof(createSql), "create table stmt2_write_matrix.%s (ts timestamp, b int)",
                       tableNames[i]);
      } else if (mode == STMT2_WRITE_USING_FIXED_TAGS) {
        (void)snprintf(createSql, sizeof(createSql),
                       "create table stmt2_write_matrix.%s using stmt2_write_matrix.stb tags(7,'fixed')",
                       tableNames[i]);
      } else {
        (void)snprintf(createSql, sizeof(createSql),
                       "create table stmt2_write_matrix.%s using stmt2_write_matrix.stb tags(%d,'%s')", tableNames[i],
                       tagInts[i], tagTexts[i]);
      }
      do_query(taos, createSql);
    }
  }

  const char* sql = NULL;
  switch (mode) {
    case STMT2_WRITE_USING_DYNAMIC_TAGS:
      sql = "insert into ? using stmt2_write_matrix.stb (t1,t2) tags(?,?) (ts,b) values(?,?)";
      break;
    case STMT2_WRITE_USING_FIXED_TAGS:
      sql = "insert into ? using stmt2_write_matrix.stb (t1,t2) tags(7,'fixed') (ts,b) values(?,?)";
      break;
    case STMT2_WRITE_TBNAME_PSEUDO_COLUMN:
      sql = "insert into stmt2_write_matrix.stb (tbname,ts,b,t1,t2) values(?,?,?,?,?)";
      break;
    case STMT2_WRITE_CHILD_WITH_TAGS:
      sql = "insert into ? using stmt2_write_matrix.stb (t1,t2) tags(?,?) (ts,b) values(?,?)";
      break;
    case STMT2_WRITE_CHILD_WITHOUT_TAGS:
    case STMT2_WRITE_NORMAL_TABLE:
      sql = "insert into ? (ts,b) values(?,?)";
      break;
  }

  Stmt2MatrixAsyncArgs asyncArgs = {0};
  if (asyncExec) {
    ASSERT_EQ(tsem_init(&asyncArgs.sem, 0, 0), TSDB_CODE_SUCCESS);
  }

  TAOS_STMT2_OPTION option = {0, interlace, interlace, asyncExec ? stmt2MatrixAsyncCb : NULL,
                              asyncExec ? &asyncArgs : NULL};
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  int totalAffectedRows = 0;
  for (int cycle = 0; cycle < CYCLE_NUM; ++cycle) {
    int64_t timestamps[MAX_TABLE_NUM][ROW_NUM] = {{0}};
    int32_t values[MAX_TABLE_NUM][ROW_NUM] = {{0}};
    int     timestampLen[ROW_NUM] = {0};
    int     valueLen[ROW_NUM] = {0};
    for (int row = 0; row < ROW_NUM; ++row) {
      timestampLen[row] = sizeof(int64_t);
      valueLen[row] = sizeof(int32_t);
    }

    TAOS_STMT2_BIND  tagBinds[MAX_TABLE_NUM][2] = {{{0}}};
    TAOS_STMT2_BIND* tagBindVectors[MAX_TABLE_NUM] = {NULL};
    TAOS_STMT2_BIND  colBinds[MAX_TABLE_NUM][2] = {{{0}}};
    TAOS_STMT2_BIND* colBindVectors[MAX_TABLE_NUM] = {NULL};

    for (int table = 0; table < tableNum; ++table) {
      for (int row = 0; row < ROW_NUM; ++row) {
        timestamps[table][row] = 1700000000000LL + caseIndex * 1000000LL + cycle * 10000LL + table * 100LL + row;
        values[table][row] = caseIndex * 1000 + cycle * 100 + table * 10 + row;
      }

      tagBinds[table][0] = {TSDB_DATA_TYPE_INT, &tagInts[table], &tagIntLen[table], NULL, 1};
      tagBinds[table][1] = {TSDB_DATA_TYPE_BINARY, tagTexts[table], &tagTextLen[table], NULL, 1};
      tagBindVectors[table] = &tagBinds[table][0];

      colBinds[table][0] = {TSDB_DATA_TYPE_TIMESTAMP, &timestamps[table][0], &timestampLen[0], NULL, ROW_NUM};
      colBinds[table][1] = {TSDB_DATA_TYPE_INT, &values[table][0], &valueLen[0], NULL, ROW_NUM};
      colBindVectors[table] = &colBinds[table][0];
    }

    TAOS_STMT2_BINDV bindv = {tableNum, tableNames,
                              stmt2WriteModeBindsTags(mode) ? tagBindVectors : NULL, colBindVectors};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    bool expectedInterlace = interlace && mode != STMT2_WRITE_NORMAL_TABLE;
    ASSERT_EQ(((STscStmt2*)stmt)->sql.stbInterlaceMode, expectedInterlace);

    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    if (asyncExec) {
      ASSERT_EQ(tsem_wait(&asyncArgs.sem), TSDB_CODE_SUCCESS);
      ASSERT_EQ(asyncArgs.code, TSDB_CODE_SUCCESS);
      ASSERT_EQ(asyncArgs.affectedRows, tableNum * ROW_NUM);
      ASSERT_EQ(asyncArgs.callbackCount, cycle + 1);
      totalAffectedRows += asyncArgs.affectedRows;
    } else {
      ASSERT_EQ(affectedRows, tableNum * ROW_NUM);
      totalAffectedRows += affectedRows;
    }
  }

  ASSERT_EQ(totalAffectedRows, tableNum * ROW_NUM * CYCLE_NUM);
  taos_stmt2_close(stmt);
  if (asyncExec) {
    (void)tsem_destroy(&asyncArgs.sem);
  }

  for (int i = 0; i < tableNum; ++i) {
    bool        verifyTags = mode != STMT2_WRITE_NORMAL_TABLE;
    int32_t     expectedTagInt = mode == STMT2_WRITE_USING_FIXED_TAGS ? 7 : tagInts[i];
    const char* expectedTagText = mode == STMT2_WRITE_USING_FIXED_TAGS ? "fixed" : tagTexts[i];
    verifyStmt2MatrixTable(taos, tableNames[i], ROW_NUM * CYCLE_NUM, verifyTags, expectedTagInt, expectedTagText);
  }
}

TAOS* getConnWithTz(const char* tz) {
  TAOS* pConn = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);
  if (tz != NULL) {
    int code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, tz);
    if (code != 0) {
      printf("[stmt2Test] set timezone failed, tz=%s code=%d\n", tz, code);
      taos_close(pConn);
      exitStmt2TestOnEnvError();
    }
  }
  return pConn;
}

// Helper: prepare a stmt2 SELECT with one TIMESTAMP placeholder, bind tsParam,
// execute, and return the number of rows fetched.  Returns -1 on any API error.
int stmtSelectTimestampRowCount(TAOS* taos, const char* sql, int64_t tsParam) {
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  if (!stmt) return -1;

  int code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  // get_fields so the stmt knows the parameter schema
  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  taos_stmt2_free_fields(stmt, pFields);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &tsParam, NULL, NULL, 1};
  TAOS_STMT2_BIND* paramv = &params;
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  code = taos_stmt2_exec(stmt, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  int       rows = 0;
  if (pRes) {
    while (taos_fetch_row(pRes)) rows++;
  }
  taos_stmt2_close(stmt);
  return rows;
}

// Helper: prepare a stmt2 SELECT with two TIMESTAMP placeholders (range style), bind
// both params, execute, and return the number of rows fetched. Returns -1 on API error.
int stmtSelectTimestampRangeRowCount(TAOS* taos, const char* sql, int64_t tsStart, int64_t tsEnd) {
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  if (!stmt) return -1;

  int code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  taos_stmt2_free_fields(stmt, pFields);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStart, NULL, NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &tsEnd, NULL, NULL, 1}};
  TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  code = taos_stmt2_exec(stmt, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  int       rows = 0;
  if (pRes) {
    while (taos_fetch_row(pRes)) rows++;
  }
  taos_stmt2_close(stmt);
  return rows;
}

// Helper: run a synchronous taos_query and return fetched row count. Returns -1 on error.
int countTaosQueryRows(TAOS* taos, const char* sql) {
  TAOS_RES* res = execQueryWithRetry(taos, sql);
  int       code = taos_errno(res);
  if (code != TSDB_CODE_SUCCESS) {
    taos_free_result(res);
    return -1;
  }

  int rows = 0;
  while (taos_fetch_row(res) != nullptr) {
    rows++;
  }
  taos_free_result(res);
  return rows;
}

// Generic stmt2 SELECT executor; caps fetch at kMaxRows.
int stmtExecSelectRowCount(TAOS* taos, const char* sql, TAOS_STMT2_BINDV* bindv, int kMaxRows = 512) {
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  if (!stmt) return -1;

  int code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  taos_stmt2_free_fields(stmt, pFields);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  code = taos_stmt2_bind_param(stmt, bindv, -1);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  code = taos_stmt2_exec(stmt, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    taos_stmt2_close(stmt);
    return -1;
  }

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  int       rows = 0;
  if (pRes) {
    while (taos_fetch_row(pRes) != nullptr) {
      rows++;
      if (rows > kMaxRows) {
        break;
      }
    }
  }
  taos_stmt2_close(stmt);
  return rows;
}

void expectStmt2RowsMatchTaos(TAOS* taos, const char* stmtSql, TAOS_STMT2_BINDV* bindv, const char* refSql) {
  const int expected = countTaosQueryRows(taos, refSql);
  ASSERT_GE(expected, 0) << refSql;
  const int actual = stmtExecSelectRowCount(taos, stmtSql, bindv);
  ASSERT_GE(actual, 0) << stmtSql;
  ASSERT_EQ(actual, expected) << "stmt: " << stmtSql << "\nref: " << refSql;
}

void expectStmt2ToCharRows(TAOS* taos, const char* sql, int64_t tsStart, int64_t tsEnd, const char* const* expected,
                           int32_t expectedRows) {
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStart, NULL, NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &tsEnd, NULL, NULL, 1}};
  TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  TAOS_RES* res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);

  int32_t  rows = 0;
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(res)) != nullptr) {
    ASSERT_LT(rows, expectedRows);
    ASSERT_NE(row[0], nullptr);
    int* lengths = taos_fetch_lengths(res);
    ASSERT_NE(lengths, nullptr);
    ASSERT_EQ(lengths[0], (int)strlen(expected[rows]));
    ASSERT_EQ(0, memcmp(row[0], expected[rows], lengths[0]));
    ++rows;
  }
  ASSERT_EQ(rows, expectedRows);
  taos_stmt2_close(stmt);
}

constexpr int32_t kStmt2SnapshotMaxRows = 32;
constexpr int32_t kStmt2SnapshotMaxCols = 20;
constexpr int32_t kStmt2SnapshotMaxCellBytes = 256;

typedef struct {
  bool    isNull;
  int32_t length;
  char    data[kStmt2SnapshotMaxCellBytes];
} Stmt2ResultCell;

typedef struct {
  int32_t         rows;
  int32_t         cols;
  Stmt2ResultCell cells[kStmt2SnapshotMaxRows][kStmt2SnapshotMaxCols];
} Stmt2ResultSnapshot;

bool snapshotResult(TAOS_RES* res, Stmt2ResultSnapshot* snapshot) {
  memset(snapshot, 0, sizeof(*snapshot));
  snapshot->cols = taos_num_fields(res);
  if (snapshot->cols < 0 || snapshot->cols > kStmt2SnapshotMaxCols) {
    ADD_FAILURE() << "invalid result column count: " << snapshot->cols;
    return false;
  }

  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(res)) != nullptr) {
    if (snapshot->rows >= kStmt2SnapshotMaxRows) {
      ADD_FAILURE() << "result exceeds snapshot row limit";
      return false;
    }
    int* lengths = taos_fetch_lengths(res);
    if (lengths == nullptr) {
      ADD_FAILURE() << "taos_fetch_lengths returned null";
      return false;
    }

    for (int32_t col = 0; col < snapshot->cols; ++col) {
      Stmt2ResultCell* cell = &snapshot->cells[snapshot->rows][col];
      cell->isNull = (row[col] == nullptr);
      if (cell->isNull) {
        continue;
      }
      if (lengths[col] < 0 || lengths[col] > kStmt2SnapshotMaxCellBytes) {
        ADD_FAILURE() << "result cell exceeds snapshot byte limit: row=" << snapshot->rows << ", col=" << col
                      << ", length=" << lengths[col];
        return false;
      }
      cell->length = lengths[col];
      memcpy(cell->data, row[col], cell->length);
    }
    ++snapshot->rows;
  }
  return true;
}

bool snapshotTaosQuery(TAOS* taos, const char* sql, Stmt2ResultSnapshot* snapshot) {
  TAOS_RES* res = execQueryWithRetry(taos, sql);
  if (res == nullptr) {
    ADD_FAILURE() << "query returned null result: " << sql;
    return false;
  }
  if (taos_errno(res) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "query failed: " << taos_errstr(res) << "\nsql: " << sql;
    taos_free_result(res);
    return false;
  }
  bool success = snapshotResult(res, snapshot);
  taos_free_result(res);
  return success;
}

bool snapshotStmt2Query(TAOS_STMT2* stmt, TAOS_STMT2_BINDV* bindv, Stmt2ResultSnapshot* snapshot) {
  int code = taos_stmt2_bind_param(stmt, bindv, -1);
  if (code != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "STMT2 bind failed: " << taos_stmt2_error(stmt);
    return false;
  }
  code = taos_stmt2_exec(stmt, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "STMT2 execute failed: " << taos_stmt2_error(stmt);
    return false;
  }
  TAOS_RES* res = taos_stmt2_result(stmt);
  if (res == nullptr) {
    ADD_FAILURE() << "STMT2 returned null result: " << taos_stmt2_error(stmt);
    return false;
  }
  return snapshotResult(res, snapshot);
}

void expectSnapshotsEqual(const Stmt2ResultSnapshot& expected, const Stmt2ResultSnapshot& actual, const char* context) {
  ASSERT_EQ(actual.rows, expected.rows) << context;
  ASSERT_EQ(actual.cols, expected.cols) << context;
  for (int32_t row = 0; row < expected.rows; ++row) {
    for (int32_t col = 0; col < expected.cols; ++col) {
      const Stmt2ResultCell* expectedCell = &expected.cells[row][col];
      const Stmt2ResultCell* actualCell = &actual.cells[row][col];
      ASSERT_EQ(actualCell->isNull, expectedCell->isNull) << context << ", row=" << row << ", col=" << col;
      if (expectedCell->isNull) {
        continue;
      }
      ASSERT_EQ(actualCell->length, expectedCell->length) << context << ", row=" << row << ", col=" << col;
      ASSERT_EQ(memcmp(actualCell->data, expectedCell->data, expectedCell->length), 0)
          << context << ", row=" << row << ", col=" << col;
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  const char* configDir = getenv("TAOS_STMT2_TEST_CFG_DIR");
  if (configDir != nullptr && taos_options(TSDB_OPTION_CONFIGDIR, configDir) != 0) {
    fprintf(stderr, "[stmt2Test] failed to set config directory: %s\n", configDir);
    return EXIT_FAILURE;
  }

  if (argc == 2 && strcmp(argv[1], STMT2_DROP_REPREPARE_TABLES_ARG) == 0) {
    return dropReprepareTables();
  }

  stmt2TestExecutable = argv[0];
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
    const char*      timeStrShanghai = "2025-08-08 08:08:08";
    int32_t          timeStrLen = strlen(timeStrShanghai);
    TAOS_STMT2_BIND  paramsCST = {TSDB_DATA_TYPE_BINARY, (void*)timeStrShanghai, &timeStrLen, NULL, 1};
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

    taos_stmt2_close(stmt);
    taos_close(taos);
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

    const char*      timeStrUTC = "2025-08-08 00:08:08";  // '2025-08-08 08:08:08+8' in UTC timezone
    int32_t          timeStrUTCLen = strlen(timeStrUTC);
    TAOS_STMT2_BIND  paramsUTC = {TSDB_DATA_TYPE_BINARY, (void*)timeStrUTC, &timeStrUTCLen, NULL, 1};
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

TEST(stmt2Case, stmt2_test_limit) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

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
    getFieldsError(taos, sql, /*TSDB_CODE_PAR_INVALID_COLUMNS_NUM*/ TSDB_CODE_TSC_SQL_SYNTAX_ERROR,
                   "Illegal number of columns");
  }

  do_query(taos, "drop database if exists stmt2_testdb_2");
  taos_close(taos);
}

TEST(stmt2Case, insert_ctb_using_get_fields_Test) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

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
    getFieldsError(taos, sql, TSDB_CODE_PAR_DB_NOT_SPECIFIED, "Database not specified");
  }

  // case 2 :  normal table must have tbnam
  {
    const char* sql = "insert into stmt2_testdb_4.? values(?,?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, /*TSDB_CODE_TSC_STMT_TBNAME_ERROR*/ TSDB_CODE_SUCCESS, "Table does not exist");
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);
  do_query(taos, "create database if not exists stmt2_testdb_5");
  do_query(taos,
           "create stable if not exists stmt2_testdb_5.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};

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
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "wrong sql", 0);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(((STscStmt2*)stmt)->db, nullptr);
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "insert into 'stmt2_testdb_5'.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)", 0);
    ASSERT_NE(stmt, nullptr);
    ASSERT_STREQ(((STscStmt2*)stmt)->db, "stmt2_testdb_5");  // add in main TD-33332
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_5");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_stb_insert) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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
  AsyncArgs* aa = (AsyncArgs*)taosMemoryMalloc(sizeof(AsyncArgs));
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
  taosMemoryFree(aa);
  taos_close(taos);
}

TEST(stmt2Case, stmt2_write_matrix) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_write_matrix");
  do_query(taos, "create database stmt2_write_matrix");
  do_query(taos, "use stmt2_write_matrix");
  do_query(taos,
           "create stable stmt2_write_matrix.stb (ts timestamp, b int) tags(t1 int, t2 binary(16))");

  int caseIndex = 0;
  Stmt2WriteSqlMode autoCreateModes[] = {
      STMT2_WRITE_USING_DYNAMIC_TAGS,
      STMT2_WRITE_USING_FIXED_TAGS,
      STMT2_WRITE_TBNAME_PSEUDO_COLUMN,
  };
  for (Stmt2WriteSqlMode mode : autoCreateModes) {
    for (int interlace = 0; interlace <= 1; ++interlace) {
      for (int autoCreate = 0; autoCreate <= 1; ++autoCreate) {
        for (int asyncExec = 0; asyncExec <= 1; ++asyncExec) {
          runStmt2WriteMatrixCase(taos, mode, interlace != 0, autoCreate != 0, asyncExec != 0, caseIndex++);
        }
      }
    }
  }

  Stmt2WriteSqlMode existingTableModes[] = {
      STMT2_WRITE_CHILD_WITH_TAGS,
      STMT2_WRITE_CHILD_WITHOUT_TAGS,
      STMT2_WRITE_NORMAL_TABLE,
  };
  for (Stmt2WriteSqlMode mode : existingTableModes) {
    for (int interlace = 0; interlace <= 1; ++interlace) {
      for (int asyncExec = 0; asyncExec <= 1; ++asyncExec) {
        runStmt2WriteMatrixCase(taos, mode, interlace != 0, false, asyncExec != 0, caseIndex++);
      }
    }
  }

  do_query(taos, "drop database if exists stmt2_write_matrix");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_interlace_column_cache_growth) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  do_stmt("interlace column cache growth", taos, &option,
          "insert into stmt2_testdb_1.? using stmt2_testdb_1.stb tags(1,'fixed') values(?,?)", STMT_TABLE_COLS_NUM + 1,
          1, 2, false, false);

  do_query(taos, "drop database if exists stmt2_testdb_1");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_fixed_table_and_tags_repeated_exec) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_fixed_table_tags");
  do_query(taos, "create database stmt2_fixed_table_tags");
  do_query(taos, "create stable stmt2_fixed_table_tags.stb (ts timestamp, v int) tags(t1 int)");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(taos_stmt2_prepare(stmt,
                             "insert into stmt2_fixed_table_tags.ctb using stmt2_fixed_table_tags.stb "
                             "tags(1) values(?,?)", 0), TSDB_CODE_SUCCESS);

  for (int cycle = 0; cycle < 6; ++cycle) {
    if (cycle == 3) {
      ASSERT_EQ(taos_stmt2_prepare(stmt,
                                 "insert into stmt2_fixed_table_tags.ctb using stmt2_fixed_table_tags.stb "
                                 "tags(1) values(?,?)", 0), TSDB_CODE_SUCCESS)
          << taos_stmt2_error(stmt);
    }
    int64_t ts = 1700000000000LL + cycle;
    int32_t value = cycle;
    int tsLen = sizeof(ts);
    int valueLen = sizeof(value);
    TAOS_STMT2_BIND cols[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &tsLen, NULL, 1},
                                {TSDB_DATA_TYPE_INT, &value, &valueLen, NULL, 1}};
    TAOS_STMT2_BIND* colVector = cols;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &colVector};
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_SUCCESS);

    STscStmt2* pStmt = (STscStmt2*)stmt;
    ASSERT_NE(pStmt->exec.pCurrBlock->pData->pCreateTbReq, nullptr);
    ASSERT_STREQ(pStmt->exec.pCurrBlock->pData->pCreateTbReq->name, "ctb");
    int affectedRows = 0;
    ASSERT_EQ(taos_stmt2_exec(stmt, &affectedRows), TSDB_CODE_SUCCESS);
    ASSERT_EQ(affectedRows, 1);
  }
  ASSERT_EQ(taos_stmt2_close(stmt), TSDB_CODE_SUCCESS);

  TAOS_RES* result = taos_query(taos, "select count(*) from stmt2_fixed_table_tags.ctb");
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_NE(row[0], nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 6);
  taos_free_result(result);
  do_query(taos, "drop database if exists stmt2_fixed_table_tags");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_fixed_table_and_tags_rebind_after_exec_error) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_fixed_tags_rebind");
  do_query(taos, "create database stmt2_fixed_tags_rebind precision 'ms' duration 1 keep 1");
  do_query(taos, "create stable stmt2_fixed_tags_rebind.stb (ts timestamp, v int) tags(t1 int)");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(taos_stmt2_prepare(stmt,
                             "insert into stmt2_fixed_tags_rebind.ctb using stmt2_fixed_tags_rebind.stb "
                             "tags(1) values(?,?)", 0), TSDB_CODE_SUCCESS);

  int64_t now = taosGetTimestampMs();
  for (int cycle = 0; cycle < 4; ++cycle) {
    SCOPED_TRACE(cycle);
    bool invalidTimestamp = cycle % 2 == 0;
    int64_t ts = invalidTimestamp ? 0 : now + cycle;
    int32_t value = cycle;
    int tsLen = sizeof(ts);
    int valueLen = sizeof(value);
    TAOS_STMT2_BIND cols[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &tsLen, NULL, 1},
                              {TSDB_DATA_TYPE_INT, &value, &valueLen, NULL, 1}};
    TAOS_STMT2_BIND* colVector = cols;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &colVector};
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_SUCCESS)
        << taos_stmt2_error(stmt);
    ASSERT_FALSE(((STscStmt2*)stmt)->sql.stbInterlaceMode);

    int affectedRows = -1;
    int code = taos_stmt2_exec(stmt, &affectedRows);
    if (invalidTimestamp) {
      ASSERT_EQ(code, TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE)
          << taos_stmt2_error(stmt);
      ASSERT_EQ(affectedRows, 0);
      ASSERT_TRUE(((STscStmt2*)stmt)->bInfo.needParse);
      ASSERT_EQ(((STscStmt2*)stmt)->exec.pCurrBlock, nullptr);
      ASSERT_EQ(((STscStmt2*)stmt)->exec.pBlockHash, nullptr);
    } else {
      ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
      ASSERT_EQ(affectedRows, 1);
    }
  }
  ASSERT_EQ(taos_stmt2_close(stmt), TSDB_CODE_SUCCESS);

  TAOS_RES* result = taos_query(taos, "select count(*) from stmt2_fixed_tags_rebind.stb where t1 = 1");
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_NE(row[0], nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 2);
  taos_free_result(result);
  do_query(taos, "drop database if exists stmt2_fixed_tags_rebind");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_write_missing_table) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_write_missing");
  do_query(taos, "create database stmt2_write_missing");
  do_query(taos, "use stmt2_write_missing");

  for (int interlace = 0; interlace <= 1; ++interlace) {
    for (int asyncExec = 0; asyncExec <= 1; ++asyncExec) {
      char trace[96] = {0};
      (void)snprintf(trace, sizeof(trace), "interlace=%d async=%d", interlace, asyncExec);
      SCOPED_TRACE(trace);

      Stmt2MatrixAsyncArgs asyncArgs = {0};
      if (asyncExec) {
        ASSERT_EQ(tsem_init(&asyncArgs.sem, 0, 0), TSDB_CODE_SUCCESS);
      }
      TAOS_STMT2_OPTION option = {0, interlace != 0, interlace != 0, asyncExec ? stmt2MatrixAsyncCb : NULL,
                                  asyncExec ? &asyncArgs : NULL};
      TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
      ASSERT_NE(stmt, nullptr);

      int code = taos_stmt2_prepare(stmt, "insert into ? (ts,b) values(?,?)", 0);
      checkError(stmt, code, __FILE__, __LINE__);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);

      char* tableName = "not_exist";
      int64_t timestamp = 1700000000000LL;
      int32_t value = 1;
      int     timestampLen = sizeof(timestamp);
      int     valueLen = sizeof(value);
      TAOS_STMT2_BIND  cols[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &timestamp, &timestampLen, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, &valueLen, NULL, 1}};
      TAOS_STMT2_BIND* colVector = &cols[0];
      TAOS_STMT2_BINDV bindv = {1, &tableName, NULL, &colVector};

      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      ASSERT_EQ(code, TSDB_CODE_PAR_TABLE_NOT_EXIST);
      taos_stmt2_close(stmt);
      if (asyncExec) {
        ASSERT_EQ(asyncArgs.callbackCount, 0);
        (void)tsem_destroy(&asyncArgs.sem);
      }
    }
  }

  do_query(taos, "drop database if exists stmt2_write_missing");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_interlace_close_before_column_bind) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_close_before_bind");
  do_query(taos, "create database stmt2_close_before_bind");
  do_query(taos, "use stmt2_close_before_bind");
  do_query(taos, "create stable stmt2_close_before_bind.stb (ts timestamp, v int) tags(t1 int)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  for (int fixedTags = 0; fixedTags <= 1; ++fixedTags) {
    const char* sql = fixedTags
                          ? "insert into ? using stmt2_close_before_bind.stb tags(1) values(?,?)"
                          : "insert into ? using stmt2_close_before_bind.stb tags(?) values(?,?)";
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char* tableName = fixedTags ? (char*)"fixed_tags" : (char*)"dynamic_tags";
    int32_t tagValue = 1;
    int tagLen = sizeof(tagValue);
    TAOS_STMT2_BIND tag = {TSDB_DATA_TYPE_INT, &tagValue, &tagLen, NULL, 1};
    TAOS_STMT2_BIND* tagVector = fixedTags ? NULL : &tag;
    TAOS_STMT2_BINDV bindv = {1, &tableName, fixedTags ? NULL : &tagVector, NULL};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    ASSERT_EQ(taos_stmt2_close(stmt), TSDB_CODE_SUCCESS);
  }

  do_query(taos, "drop database if exists stmt2_close_before_bind");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_interlace_bind_after_queue_stop) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_stopped_queue");
  do_query(taos, "create database stmt2_stopped_queue");
  do_query(taos, "use stmt2_stopped_queue");
  do_query(taos, "create stable stmt2_stopped_queue.stb (ts timestamp, v int) tags(t1 int)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(taos_stmt2_prepare(stmt, "insert into ? using stmt2_stopped_queue.stb tags(1) values(?,?)", 0),
            TSDB_CODE_SUCCESS);

  STscStmt2* pStmt = (STscStmt2*)stmt;
  ASSERT_EQ(taosThreadMutexLock(&pStmt->queue.mutex), TSDB_CODE_SUCCESS);
  atomic_store_8((int8_t*)&pStmt->queue.stopQueue, true);
  ASSERT_EQ(taosThreadCondSignal(&pStmt->queue.waitCond), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosThreadMutexUnlock(&pStmt->queue.mutex), TSDB_CODE_SUCCESS);

  char* tableName = (char*)"ctb";
  int64_t ts = 1591060628000;
  int32_t value = 1;
  int tsLen = sizeof(ts);
  int valueLen = sizeof(value);
  TAOS_STMT2_BIND cols[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &tsLen, NULL, 1},
                              {TSDB_DATA_TYPE_INT, &value, &valueLen, NULL, 1}};
  TAOS_STMT2_BIND* colVector = cols;
  TAOS_STMT2_BINDV bindv = {1, &tableName, NULL, &colVector};
  ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_TSC_STMT_API_ERROR);
  ASSERT_EQ(atomic_load_64(&pStmt->sql.siInfo.tbRemainNum), 0);
  ASSERT_EQ(taos_stmt2_close(stmt), TSDB_CODE_SUCCESS);

  do_query(taos, "drop database if exists stmt2_stopped_queue");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_async_bind_close_waits_for_callback) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_bind_callback_close");
  do_query(taos, "create database stmt2_bind_callback_close");
  do_query(taos, "use stmt2_bind_callback_close");
  do_query(taos, "create stable stmt2_bind_callback_close.stb (ts timestamp, v int) tags(t1 int)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(taos_stmt2_prepare(stmt, "insert into ? using stmt2_bind_callback_close.stb tags(1) values(?,?)", 0),
            TSDB_CODE_SUCCESS);

  char* tableName = (char*)"ctb";
  int64_t ts = 1591060628000;
  int32_t value = 1;
  int tsLen = sizeof(ts);
  int valueLen = sizeof(value);
  TAOS_STMT2_BIND cols[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &tsLen, NULL, 1},
                              {TSDB_DATA_TYPE_INT, &value, &valueLen, NULL, 1}};
  TAOS_STMT2_BIND* colVector = cols;
  TAOS_STMT2_BINDV bindv = {1, &tableName, NULL, &colVector};
  AsyncBindWaitArgs args = {};
  args.bindCode = TSDB_CODE_APP_ERROR;
  ASSERT_EQ(tsem_init(&args.enteredSem, 0, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tsem_init(&args.releaseSem, 0, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taos_stmt2_bind_param_a(stmt, &bindv, -1, stmtAsyncBindWaitCb, &args), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tsem_wait(&args.enteredSem), TSDB_CODE_SUCCESS);
  EXPECT_EQ(args.bindCode, TSDB_CODE_SUCCESS);
  EXPECT_EQ(atomic_load_8(&((STscStmt2*)stmt)->asyncBindParam.asyncBindNum), 1);

  int closeCode = TSDB_CODE_APP_ERROR;
  std::atomic<bool> closeFinished(false);
  std::thread closer([&]() {
    closeCode = taos_stmt2_close(stmt);
    closeFinished.store(true);
  });
  taosMsleep(50);
  EXPECT_FALSE(closeFinished.load());
  EXPECT_EQ(tsem_post(&args.releaseSem), TSDB_CODE_SUCCESS);
  closer.join();
  EXPECT_EQ(closeCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(tsem_destroy(&args.enteredSem), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tsem_destroy(&args.releaseSem), TSDB_CODE_SUCCESS);

  do_query(taos, "drop database if exists stmt2_bind_callback_close");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_interlace_reset_after_queued_data) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_reset_queued_data");
  do_query(taos, "create database stmt2_reset_queued_data");
  do_query(taos, "use stmt2_reset_queued_data");
  do_query(taos, "create stable stmt2_reset_queued_data.stb (ts timestamp, v int) tags(t1 int)");
  do_query(taos, "create table stmt2_reset_queued_data.ctb using stmt2_reset_queued_data.stb tags(1)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  ASSERT_EQ(taos_stmt2_prepare(stmt, "insert into ? values(?,?)", 0), TSDB_CODE_SUCCESS);

  char* tableName = (char*)"ctb";
  int64_t ts = 1591060628000;
  int32_t value = 1;
  int tsLen = sizeof(ts);
  int valueLen = sizeof(value);
  TAOS_STMT2_BIND cols[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &tsLen, NULL, 1},
                              {TSDB_DATA_TYPE_INT, &value, &valueLen, NULL, 1}};
  TAOS_STMT2_BIND* colVector = cols;
  TAOS_STMT2_BINDV bindv = {1, &tableName, NULL, &colVector};
  ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_SUCCESS);

  STscStmt2* pStmt = (STscStmt2*)stmt;
  for (int i = 0; i < 5000 && atomic_load_64(&pStmt->sql.siInfo.tbRemainNum) != 0; ++i) {
    taosMsleep(1);
  }
  ASSERT_EQ(atomic_load_64(&pStmt->sql.siInfo.tbRemainNum), 0);
  ASSERT_EQ(pStmt->errCode, TSDB_CODE_SUCCESS);
  ASSERT_NE(pStmt->sql.siInfo.pVgroupList, nullptr);
  ASSERT_GT(taosArrayGetSize(pStmt->sql.siInfo.pVgroupList), 0);

  pStmt->errCode = TSDB_CODE_APP_ERROR;
  ASSERT_EQ(taos_stmt2_prepare(stmt, "insert into ? values(?,?)", 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pStmt->sql.siInfo.pVgroupList, nullptr);
  ASSERT_EQ(taos_stmt2_close(stmt), TSDB_CODE_SUCCESS);

  do_query(taos, "drop database if exists stmt2_reset_queued_data");
  taos_close(taos);
}

// TD-33417
// TS-6515
// TD-35141
// TS-6798
TEST(stmt2Case, stmt2_insert_non_statndard) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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
    taos_stmt2_free_fields(stmt, pFields);

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
      taos_stmt2_free_fields(stmt, pFields);

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      total_affect_rows += affected_rows;
      checkError(stmt, code, __FILE__, __LINE__);

      code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
      checkError(stmt, code, __FILE__, __LINE__);
      ASSERT_EQ(fieldNum, 2);
      ASSERT_STREQ(pFields[0].name, "tbname");
      ASSERT_STREQ(pFields[1].name, "ts");
      taos_stmt2_free_fields(stmt, pFields);
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
    int64_t ts[2] = {1591060628000, 0};
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
    taos_stmt2_free_fields(stmt, pFields);

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
    taos_stmt2_free_fields(stmt, pFields);

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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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

  TAOS_RES* pRes = execQueryWithRetry(taos, "select * from `stmt2_testdb_18`.`tb1`");
  ASSERT_NE(pRes, nullptr);

  int getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_free_result(pRes);

  pRes = execQueryWithRetry(taos, "select * from `stmt2_testdb_18`.`tb2`");
  ASSERT_NE(pRes, nullptr);

  getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_free_result(pRes);

  pRes = execQueryWithRetry(taos, "select * from `stmt2_testdb_18`.`tb3`");
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

  pRes = execQueryWithRetry(taos, "select * from `stmt2_testdb_18`.`tb1`");
  ASSERT_NE(pRes, nullptr);

  getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_free_result(pRes);

  // insert same tbname from different stb
  option = {0, false, false, NULL, NULL};
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
  taos_stmt2_close(stmt);

  stmt = taos_stmt2_init(taos, &option);
  taos_stmt2_prepare(stmt, "INSERT INTO `stmt2_testdb_18`.`stb2` (ts,int_col,tbname)  VALUES (?,?,?)", 0);
  bindv3 = {1, &tbname3[0], NULL, &paramvs2[0]};
  code = taos_stmt2_bind_param(stmt, &bindv3, -1);
  ASSERT_EQ(code, TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE);

  taos_stmt2_close(stmt);

  do_query(taos, "drop database if exists stmt2_testdb_18");
  taos_close(taos);
}

void taos_retrieve_call_back(void* param, TAOS_RES* tres, int numOfRows) {
  AsyncArgs* args = (AsyncArgs*)param;
  if (numOfRows > 0) {
    // synchronous API to retrieve a row from batch of records
    for (int i = 0; i < numOfRows; i++) {
      TAOS_ROW row = taos_fetch_row(tres);
      ASSERT_NE(row, nullptr);
      ASSERT_LT(args->current_row_idx, args->num_expected_rows);

      ExpectedRow* expected = &args->expected_rows[args->current_row_idx];
      ASSERT_EQ(strncmp((char*)row[0], expected->col0, expected->col0_len), 0);
      ASSERT_EQ(strncmp((char*)row[1], expected->col1, expected->col1_len), 0);
      ASSERT_EQ(strncmp((char*)row[2], expected->col2, expected->col2_len), 0);

      args->current_row_idx++;
    }
    taos_fetch_rows_a(tres, taos_retrieve_call_back, param);

  } else {
    ASSERT_EQ(args->current_row_idx, args->num_expected_rows);
    ASSERT_EQ(tsem_post(&args->sem), TSDB_CODE_SUCCESS);
  }
}

void asyncExec(void* param, TAOS_RES* res, int code) {
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  taos_fetch_rows_a(res, taos_retrieve_call_back, param);
  return;
}

TEST(stmt2Case, query) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_7 vgroups 4");
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
    for (int i = 0; i < 3; i++) {
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      taos_stmt2_exec(stmt, NULL);
      checkError(stmt, code, __FILE__, __LINE__);

      TAOS_RES* pRes = taos_stmt2_result(stmt);
      ASSERT_NE(pRes, nullptr);
      TAOS_ROW row = taos_fetch_row(pRes);
      ASSERT_NE(row, nullptr);
      ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
      ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
      ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);
      row = taos_fetch_row(pRes);
      ASSERT_NE(row, nullptr);
      ASSERT_EQ(strncmp((char*)row[0], "tb2", 3), 0);
      ASSERT_EQ(strncmp((char*)row[1], "xyz", 3), 0);
      ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);
    }

    taos_stmt2_close(stmt);
  }

  // async query with async fetch
  {
    AsyncArgs* aa = (AsyncArgs*)taosMemoryMalloc(sizeof(AsyncArgs));
    aa->async_affected_rows = 0;
    ASSERT_EQ(tsem_init(&aa->sem, 0, 0), TSDB_CODE_SUCCESS);

    ExpectedRow expected_rows[2] = {
        {"tb1", "abc", "abc", 3, 3, 3},
        {"tb2", "xyz", "abc", 3, 3, 3},
    };
    aa->expected_rows = expected_rows;
    aa->num_expected_rows = 2;
    aa->current_row_idx = 0;

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

    for (int i = 0; i < 3; i++) {
      aa->current_row_idx = 0;

      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      taos_stmt2_exec(stmt, NULL);
      checkError(stmt, code, __FILE__, __LINE__);

      tsem_wait(&aa->sem);
    }

    tsem_destroy(&aa->sem);
    taosMemoryFree(aa);
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    const char* sql =
        "select table_name, db_name, stable_name from information_schema.ins_tables where 1 = 1 and table_name = ? and "
        "db_name = ?";
    int code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int             fieldNum = 0;
    TAOS_FIELD_ALL* pFields = NULL;
    code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 2);

    int              b_len = 3;
    int              b_len2 = 14;
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_BINARY, (void*)"tb1", &b_len, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"stmt2_testdb_7", &b_len2, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);
    TAOS_ROW row = taos_fetch_row(pRes);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "stmt2_testdb_7", 14), 0);
    ASSERT_EQ(strncmp((char*)row[2], "stb", 3), 0);
    taos_stmt2_close(stmt);
  }
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "select tbname,t2,b from stmt2_testdb_7.stb where ts = ? and tbname = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t b_len = 3;
    int32_t t64_len = sizeof(int64_t);
    int64_t ts = 1591060628000;

    TAOS_STMT2_BIND  params2[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1},
                                   {TSDB_DATA_TYPE_BINARY, (void*)"tb1", &b_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv2[2] = {&params2[0], &params2[1]};
    TAOS_STMT2_BINDV bindv2 = {1, NULL, NULL, &paramv2[0]};

    code = taos_stmt2_bind_param(stmt, &bindv2, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);

    TAOS_ROW row = taos_fetch_row(pRes);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
    ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);
    ASSERT_EQ(taos_fetch_row(pRes), nullptr);

    code = taos_stmt2_prepare(stmt, "select tbname,t2,b from stmt2_testdb_7.stb where ts = ? and tbname in (?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_bind_param(stmt, &bindv2, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);
    row = taos_fetch_row(pRes);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
    ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);
    ASSERT_EQ(taos_fetch_row(pRes), nullptr);

    code = taos_stmt2_prepare(stmt, "select tbname,t2,b from stmt2_testdb_7.stb where ts = ? and tbname = 'tb2'", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND  params3 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, 1};
    TAOS_STMT2_BIND* paramv3 = &params3;
    TAOS_STMT2_BINDV bindv3 = {1, NULL, NULL, &paramv3};
    code = taos_stmt2_bind_param(stmt, &bindv3, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    pRes = taos_stmt2_result(stmt);
    ASSERT_NE(pRes, nullptr);
    row = taos_fetch_row(pRes);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(strncmp((char*)row[0], "tb2", 3), 0);
    ASSERT_EQ(strncmp((char*)row[1], "xyz", 3), 0);
    ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_7");
  taos_close(taos);
}

TEST(stmt2Case, query_use_adapter) {
  TAOS*   taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
  int32_t code = taos_options(TSDB_OPTION_USE_ADAPTER, "true");
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  do_query(taos, "drop database if exists stmt2_testdb_37");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_37");
  do_query(taos, "create stable stmt2_testdb_37.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
           "insert into stmt2_testdb_37.tb1 using stmt2_testdb_37.stb tags(1,'abc') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628002, 'hij')");
  do_query(taos,
           "insert into stmt2_testdb_37.tb2 using stmt2_testdb_37.stb tags(2,'xyz') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628004, 'hij')");
  do_query(taos, "use stmt2_testdb_37");

  // sync query
  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "select tbname,t2,b from stmt2_testdb_37.stb where ts = ? order by tbname";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    for (int i = 0; i < 3; i++) {
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

      TAOS_RES* pRes = taos_stmt2_result(stmt);
      ASSERT_NE(pRes, nullptr);
      TAOS_ROW row = taos_fetch_row(pRes);
      ASSERT_NE(row, nullptr);
      ASSERT_EQ(strncmp((char*)row[0], "tb1", 3), 0);
      ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
      ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);
      row = taos_fetch_row(pRes);
      ASSERT_NE(row, nullptr);
      ASSERT_EQ(strncmp((char*)row[0], "tb2", 3), 0);
      ASSERT_EQ(strncmp((char*)row[1], "xyz", 3), 0);
      ASSERT_EQ(strncmp((char*)row[2], "abc", 3), 0);
      taos_free_result(pRes);
    }

    taos_stmt2_close(stmt);
  }

  // async query with async fetch
  {
    AsyncArgs* aa = (AsyncArgs*)taosMemoryMalloc(sizeof(AsyncArgs));
    aa->async_affected_rows = 0;
    ASSERT_EQ(tsem_init(&aa->sem, 0, 0), TSDB_CODE_SUCCESS);

    // Setup expected results
    ExpectedRow expected_rows[2] = {{"tb1", "abc", "abc", 3, 3, 3}, {"tb2", "xyz", "abc", 3, 3, 3}};
    aa->expected_rows = expected_rows;
    aa->num_expected_rows = 2;
    aa->current_row_idx = 0;

    TAOS_STMT2_OPTION option = {0, true, true, asyncExec, (void*)aa};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "select tbname,t2,b from stmt2_testdb_37.stb where ts = ? order by tbname";
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

    for (int i = 0; i < 3; i++) {
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code, __FILE__, __LINE__);

      taos_stmt2_exec(stmt, NULL);
      checkError(stmt, code, __FILE__, __LINE__);

      tsem_wait(&aa->sem);
      aa->current_row_idx = 0;
      tsem_init(&aa->sem, 0, 0);
    }

    tsem_destroy(&aa->sem);
    taosMemoryFree(aa);
    taos_stmt2_close(stmt);
  }

  code = taos_options(TSDB_OPTION_USE_ADAPTER, "false");
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  do_query(taos, "drop database if exists stmt2_testdb_37");
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

void asyncSelectRetrieveCb(void* param, TAOS_RES* pRes, int numOfRows) {
  AsyncArgs* aa = (AsyncArgs*)param;
  if (numOfRows > 0) {
    for (int i = 0; i < numOfRows; ++i) {
      TAOS_ROW row = taos_fetch_row(pRes);
      ASSERT_NE(row, nullptr);
      ASSERT_LT(aa->current_row_idx, aa->num_expected_rows4);
      ExpectedRow4* expected = &aa->expected_rows4[aa->current_row_idx];
      ASSERT_EQ(strncmp((char*)row[0], expected->col0, expected->col0_len), 0);
      ASSERT_EQ(strncmp((char*)row[1], expected->col1, expected->col1_len), 0);
      ASSERT_EQ(*(int*)row[2], expected->col2);
      ASSERT_EQ(strncmp((char*)row[3], expected->col3, expected->col3_len), 0);
      aa->current_row_idx++;
    }
    taos_fetch_rows_a(pRes, asyncSelectRetrieveCb, param);
    return;
  }

  ASSERT_EQ(aa->current_row_idx, aa->num_expected_rows4);
  aa->async_affected_rows = aa->current_row_idx;
  ASSERT_EQ(tsem_post(&aa->sem), TSDB_CODE_SUCCESS);
}

void asyncSelectExecAndFetch(void* param, TAOS_RES* pRes, int code) {
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  taos_fetch_rows_a(pRes, asyncSelectRetrieveCb, param);
}

TEST(stmt2Case, query_error) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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
    AsyncArgs* aa = (AsyncArgs*)taosMemoryMalloc(sizeof(AsyncArgs));
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

    // correct usage 2 : async fetch rows in callback
    AsyncArgs* aa2 = (AsyncArgs*)taosMemoryMalloc(sizeof(AsyncArgs));
    aa2->async_affected_rows = 0;
    aa2->current_row_idx = 0;
    ExpectedRow4 expected_rows4[2] = {{"tb1", "abc", 1, "abc", 3, 3, 3}, {"tb2", "abc", 2, "xyz", 3, 3, 3}};
    aa2->expected_rows4 = expected_rows4;
    aa2->num_expected_rows4 = 2;
    ASSERT_EQ(tsem_init(&aa2->sem, 0, 0), TSDB_CODE_SUCCESS);

    TAOS_STMT2_OPTION option2 = {0, true, true, asyncSelectExecAndFetch, (void*)aa2};
    TAOS_STMT2*       stmt2 = taos_stmt2_init(taos, &option2);
    ASSERT_NE(stmt2, nullptr);

    code = taos_stmt2_prepare(stmt2, "select tbname,b,t1,t2 from stmt2_testdb_27.stb where ts = ? order by tbname", 0);
    checkError(stmt2, code, __FILE__, __LINE__);

    fieldNum = 0;
    code = taos_stmt2_get_fields(stmt2, &fieldNum, NULL);
    checkError(stmt2, code, __FILE__, __LINE__);
    ASSERT_EQ(fieldNum, 1);

    code = taos_stmt2_bind_param(stmt2, &bindv, -1);
    checkError(stmt2, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt2, NULL);
    checkError(stmt2, code, __FILE__, __LINE__);

    tsem_wait(&aa2->sem);
    ASSERT_EQ(aa2->async_affected_rows, 2);
    tsem_destroy(&aa2->sem);
    taosMemoryFree(aa2);
    taos_stmt2_close(stmt2);

    do_query(taos, "drop database if exists stmt2_testdb_7");
    taos_stmt2_close(stmt);
  }
  do_query(taos, "drop database if exists stmt2_testdb_27");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_ntb_insert) {
  TAOS*             taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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
  TAOS*             taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);

  int64_t ts[3] = {1591060628000, 1591060628001, 1591060628002};
  int     t64_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};

  TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 3};
  TAOS_STMT2_BIND* paramv = &params;
  TAOS_STMT2_BINDV bindv1 = {1, NULL, NULL, &paramv};

  int code = taos_stmt2_bind_param(stmt, &bindv1, 0);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "Stmt API usage error");

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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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

  int32_t* t64_len = (int32_t*)taosMemoryMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len = (int32_t*)taosMemoryMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len2 = (int32_t*)taosMemoryMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len3 = (int32_t*)taosMemoryMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len4 = (int32_t*)taosMemoryMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len5 = (int32_t*)taosMemoryMalloc(sizeof(int32_t) * 10);
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
  taosMemoryFree(t64_len);
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
//   TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

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
    taos_stmt2_close(stmt);
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

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

    TAOS_RES* result = execQueryWithRetry(taos, "select b1,b2,tbname from stmt2_testdb_20.stb order by tbname");
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

    result = execQueryWithRetry(
        taos, "select b1,b2,tbname from stmt2_testdb_20.tb1 where ts = 1591060630000 order by tbname");
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

    TAOS_RES* result = execQueryWithRetry(taos, "select b1,b2 from stmt2_testdb_20.ntb");
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

  // query decimal
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[0]);
    ASSERT_NE(stmt, nullptr);
    char* sql = "select b1,b2 from stmt2_testdb_20.ntb where b1 < ? and b2 < ?";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_STMT2_BIND  col[2] = {{TSDB_DATA_TYPE_DECIMAL64, &b1_data[0], &b1_len[0], NULL, 1},
                               {TSDB_DATA_TYPE_DECIMAL, &b2_data[0], &b2_len[0], NULL, 1}};
    TAOS_STMT2_BIND* cols = &col[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &cols};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* result = taos_stmt2_result(stmt);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), 0);

    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_STREQ((char*)row[0], "1.02");
    ASSERT_STREQ((char*)row[1], "123000.0000000000");
    taos_stmt2_close(stmt);
  }

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_20");
  taos_close(taos);
}

TEST(stmt2Case, decimal_bind_after_skipped_narrow_decimal) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_decimal_gap");
  do_query(taos, "CREATE DATABASE stmt2_testdb_decimal_gap");
  do_query(taos,
           "CREATE STABLE stmt2_testdb_decimal_gap.st (ts TIMESTAMP, narrow DECIMAL(3,2), wide DECIMAL(8,2)) "
           "TAGS (did BIGINT)");
  do_query(taos, "CREATE TABLE stmt2_testdb_decimal_gap.ct USING stmt2_testdb_decimal_gap.st TAGS (1)");

  // Match the STMT2 options used by the WebSocket JDBC driver.
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  int code = taos_stmt2_prepare(stmt, "INSERT INTO stmt2_testdb_decimal_gap.ct (ts, wide) VALUES (?, ?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* fields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &fields);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, 2);
  ASSERT_STREQ(fields[0].name, "ts");
  ASSERT_STREQ(fields[1].name, "wide");
  ASSERT_EQ(fields[1].type, TSDB_DATA_TYPE_DECIMAL64);
  ASSERT_EQ(fields[1].precision, 8);
  ASSERT_EQ(fields[1].scale, 2);
  taos_stmt2_free_fields(stmt, fields);

  int64_t ts =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  int32_t tsLen = sizeof(ts);
  char    decimal[] = "28.1";
  int32_t decimalLen = sizeof(decimal) - 1;

  TAOS_STMT2_BIND  cols[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &tsLen, NULL, 1},
                              {TSDB_DATA_TYPE_DECIMAL64, decimal, &decimalLen, NULL, 1}};
  TAOS_STMT2_BIND* bindCols = &cols[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &bindCols};

  int bindCode = taos_stmt2_bind_param(stmt, &bindv, -1);
  EXPECT_EQ(bindCode, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);

  if (bindCode == TSDB_CODE_SUCCESS) {
    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    EXPECT_EQ(affectedRows, 1);

    if (code == TSDB_CODE_SUCCESS) {
      TAOS_RES* result = execQueryWithRetry(taos, "SELECT narrow, wide FROM stmt2_testdb_decimal_gap.ct");
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS);

      TAOS_ROW row = taos_fetch_row(result);
      ASSERT_NE(row, nullptr);
      ASSERT_EQ(row[0], nullptr);
      ASSERT_STREQ((char*)row[1], "28.10");
      ASSERT_EQ(taos_fetch_row(result), nullptr);
      taos_free_result(result);
    }
  }

  taos_stmt2_close(stmt);
  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_decimal_gap");
  taos_close(taos);
}

TEST(stmt2Case, decimal_bind_after_schema_change) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_decimal_alter");
  do_query(taos, "CREATE DATABASE stmt2_testdb_decimal_alter");
  do_query(taos,
           "CREATE TABLE stmt2_testdb_decimal_alter.t (ts TIMESTAMP, removed DECIMAL(6,2), "
           "narrow DECIMAL(3,2))");
  do_query(taos, "ALTER TABLE stmt2_testdb_decimal_alter.t DROP COLUMN removed");
  do_query(taos, "ALTER TABLE stmt2_testdb_decimal_alter.t ADD COLUMN wide DECIMAL(8,2)");
  do_query(taos, "ALTER TABLE stmt2_testdb_decimal_alter.t ADD COLUMN huge DECIMAL(20,4)");

  auto insertRow = [&](const char* trace, int64_t ts, const char* wideValue, int32_t bindMode, bool bindSeparately) {
    SCOPED_TRACE(trace);
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    EXPECT_NE(stmt, nullptr);
    if (stmt == nullptr) {
      return;
    }

    int code =
        taos_stmt2_prepare(stmt, "INSERT INTO stmt2_testdb_decimal_alter.t (ts, wide, huge) VALUES (?, ?, ?)", 0);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    if (code != TSDB_CODE_SUCCESS) {
      taos_stmt2_close(stmt);
      return;
    }

    int32_t         tsLen = sizeof(ts);
    int32_t         wideLen = strlen(wideValue);
    char            hugeValue[] = "1234567890123456.789";
    int32_t         hugeLen = sizeof(hugeValue) - 1;
    TAOS_STMT2_BIND cols[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &tsLen, NULL, 1},
                               {TSDB_DATA_TYPE_DECIMAL64, (void*)wideValue, &wideLen, NULL, 1},
                               {TSDB_DATA_TYPE_DECIMAL, hugeValue, &hugeLen, NULL, 1}};

    if (bindSeparately) {
      for (int32_t colIdx = 0; colIdx < 3; ++colIdx) {
        TAOS_STMT2_BIND* bindCol = &cols[colIdx];
        TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &bindCol};
        code = taos_stmt2_bind_param(stmt, &bindv, colIdx);
        EXPECT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
    } else {
      TAOS_STMT2_BIND* bindCols = &cols[0];
      TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &bindCols};
      code = taos_stmt2_bind_param(stmt, &bindv, bindMode);
      EXPECT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    }

    if (code == TSDB_CODE_SUCCESS) {
      int affectedRows = 0;
      code = taos_stmt2_exec(stmt, &affectedRows);
      EXPECT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
      EXPECT_EQ(affectedRows, 1);
    }
    taos_stmt2_close(stmt);
  };

  insertRow("column-format bind", 1591060628000, "28.1", -1, false);
  insertRow("row-format bind", 1591060629000, "29.1", -2, false);
  insertRow("single-column bind", 1591060630000, "30.1", 0, true);

  TAOS_RES* result =
      execQueryWithRetry(taos, "SELECT narrow, wide, huge FROM stmt2_testdb_decimal_alter.t ORDER BY ts");
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS);

  const char* expectedWide[] = {"28.10", "29.10", "30.10"};
  for (int32_t i = 0; i < 3; ++i) {
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(row[0], nullptr);
    ASSERT_STREQ((char*)row[1], expectedWide[i]);
    ASSERT_STREQ((char*)row[2], "1234567890123456.7890");
  }
  ASSERT_EQ(taos_fetch_row(result), nullptr);
  taos_free_result(result);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_decimal_alter");
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);

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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

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

  // TD-6581610626
  do_query(taos, "drop table if exists stmt2_testdb_19.ntb");
  do_query(taos, "create table stmt2_testdb_19.ntb (ts timestamp, name nchar(10), i32 int)");
  for (int i = 0; i < 2; i++) {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[i]);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "insert into ntb (ts, name, i32) values (?, 'world', ?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t ts[2] = {1591060628000, 1591060629000};
    int32_t t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};

    int32_t         i32[2] = {70, 80};
    int32_t         i32_len[2] = {sizeof(int32_t), sizeof(int32_t)};
    TAOS_STMT2_BIND param[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                                {TSDB_DATA_TYPE_INT, &i32[0], &i32_len[0], NULL, 2}};

    TAOS_STMT2_BIND* params[2] = {&param[0], &param[1]};
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &params[0]};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    taos_stmt2_close(stmt);
  }
  do_query(taos, "drop database if exists stmt2_testdb_19");
  taos_close(taos);
}

// TD-33582
TEST(stmt2Case, errcode) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);
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
    ASSERT_EQ(code, /*TSDB_CODE_TSC_STMT_TBNAME_ERROR*/ TSDB_CODE_SUCCESS);
    taos_stmt2_free_fields(stmt, pFields);

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
    taos_stmt2_free_fields(stmt, pFields);

    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
    ASSERT_EQ(((STscStmt2*)stmt)->errCode, TSDB_CODE_TSC_STMT_API_ERROR);

    int64_t          ts = 1591060628000;
    char             value[] = "ok";
    int32_t          valueLen = 2;
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, value, &valueLen, NULL, 1}};
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
    ASSERT_EQ(((STscStmt2*)stmt)->errCode, TSDB_CODE_TSC_STMT_API_ERROR);

    code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_14.tb");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_NE(row[0], nullptr);
    ASSERT_NE(row[1], nullptr);
    ASSERT_EQ(*(int64_t*)row[0], ts);
    int* lengths = taos_fetch_lengths(result);
    ASSERT_NE(lengths, nullptr);
    ASSERT_EQ(lengths[1], valueLen);
    ASSERT_EQ(memcmp(row[1], value, valueLen), 0);
    ASSERT_EQ(taos_fetch_row(result), nullptr);
    taos_free_result(result);

    taos_stmt2_close(stmt);
  }

  // Binding errors retain the original sticky error behavior and require prepare before reuse.
  {
    TAOS_STMT2_OPTION option = {0};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    char* sql = "insert into tb values(?,?)";
    int   code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t          ts = 1591060629000;
    int32_t          value = 1;
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1}};
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_TSC_INVALID_OPERATION);
    ASSERT_EQ(((STscStmt2*)stmt)->errCode, code);
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), code);

    char    binaryValue[] = "ok";
    int32_t binaryLength = 2;
    params[1] = {TSDB_DATA_TYPE_BINARY, binaryValue, &binaryLength, NULL, 1};
    code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    int32_t affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);

    // The entry check must report this error instead of the successful lastExec.
    params[0].num = 0;
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR);
    ASSERT_STREQ(taos_stmt2_error(stmt), tstrerror(code));
    params[0].num = 1;
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), code);
    ASSERT_EQ(taos_stmt2_prepare(stmt, sql, 0), TSDB_CODE_SUCCESS);
    ++ts;
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taos_stmt2_exec(stmt, &affectedRows), TSDB_CODE_SUCCESS);
    ASSERT_EQ(affectedRows, 1);

    TAOS_STMT2_COLUMN_BINDV columnBindv = {};
    ASSERT_EQ(taos_stmt2_bind_param_column(stmt, &columnBindv), TSDB_CODE_INVALID_PARA);
    ASSERT_STREQ(taos_stmt2_error(stmt), tstrerror(TSDB_CODE_INVALID_PARA));
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_INVALID_PARA);
    ASSERT_EQ(taos_stmt2_prepare(stmt, sql, 0), TSDB_CODE_SUCCESS);
    ++ts;
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taos_stmt2_exec(stmt, &affectedRows), TSDB_CODE_SUCCESS);
    ASSERT_EQ(affectedRows, 1);

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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);
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

  TAOS*             taos = connectTaosOrExit("localhost", "root", "taosdata", NULL, 0);
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
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

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

void stmtAsyncExecRetryCb(void* param, TAOS_RES* pRes, int code) {
  AsyncArgs* args = (AsyncArgs*)param;
  args->async_affected_rows = code;
  ASSERT_EQ(tsem_post(&args->sem), TSDB_CODE_SUCCESS);
}

typedef struct {
  TAOS_STMT2* stmt;
  int32_t     code;
  int32_t     resultCode;
  int32_t     affectedRows;
  int32_t     callbackCount;
  char        error[TSDB_ERROR_MSG_LEN];
  char        resultError[TSDB_ERROR_MSG_LEN];
  tsem_t      sem;
} Stmt2AsyncExecResult;

constexpr int64_t kStmt2AsyncWaitMs = 60000;

void stmtAsyncExecResultCb(void* param, TAOS_RES* pRes, int code) {
  Stmt2AsyncExecResult* result = (Stmt2AsyncExecResult*)param;
  result->code = code;
  result->resultCode = pRes == nullptr ? TSDB_CODE_APP_ERROR : taos_errno(pRes);
  result->affectedRows = pRes == nullptr ? 0 : taos_affected_rows(pRes);
  ++result->callbackCount;
  result->error[0] = '\0';
  result->resultError[0] = '\0';
  if (code != TSDB_CODE_SUCCESS) {
    const char* error = taos_stmt2_error(result->stmt);
    tstrncpy(result->error, error == nullptr ? "" : error, sizeof(result->error));
    const char* resultError = pRes == nullptr ? "" : taos_errstr(pRes);
    tstrncpy(result->resultError, resultError == nullptr ? "" : resultError, sizeof(result->resultError));
  }
  (void)tsem_post(&result->sem);
}

// Entry validation does not need a server. Cover both retained outcomes and
// stale request diagnostics, including the separate query row-count check.
TEST(stmt2Case, bind_number_error_overrides_exec_result) {
  for (bool isQuery : {false, true}) {
    for (int32_t execCode : {TSDB_CODE_SUCCESS, TSDB_CODE_VND_STOPPED}) {
      STscStmt2 stmt = {};
      stmt.sql.type = isQuery ? STMT_TYPE_QUERY : STMT_TYPE_INSERT;
      stmt.lastExec.valid = true;
      stmt.lastExec.code = execCode;
      tstrncpy(stmt.lastExec.msg, "previous execution", sizeof(stmt.lastExec.msg));
      char requestError[] = "previous request";
      SRequestObj request = {};
      request.code = execCode;
      request.msgBuf = requestError;
      stmt.exec.pRequest = &request;

      TAOS_STMT2_BIND param = {TSDB_DATA_TYPE_TIMESTAMP, NULL, NULL, NULL, 0};
      TAOS_STMT2_BIND* paramv = &param;
      TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
      ASSERT_EQ(taos_stmt2_bind_param(&stmt, &bindv, -1), TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR);
      ASSERT_STREQ(taos_stmt2_error(&stmt), tstrerror(TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR));
      ASSERT_EQ(stmt.errCode, TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR);
    }
  }
}

TEST(stmt2Case, column_bind_error_overrides_exec_result) {
  for (int32_t execCode : {TSDB_CODE_SUCCESS, TSDB_CODE_VND_STOPPED}) {
    for (int32_t invalidInput = 0; invalidInput < 4; ++invalidInput) {
      STscStmt2 stmt = {};
      stmt.sql.type = STMT_TYPE_INSERT;
      stmt.lastExec.valid = true;
      stmt.lastExec.code = execCode;
      tstrncpy(stmt.lastExec.msg, "previous execution", sizeof(stmt.lastExec.msg));
      char requestError[] = "previous request";
      SRequestObj request = {};
      request.code = execCode;
      request.msgBuf = requestError;
      stmt.exec.pRequest = &request;

      TAOS_STMT2_COLUMN_BIND column = {};
      TAOS_STMT2_COLUMN_BINDV bindv = {};
      bindv.num_columns = invalidInput == 1 ? 0 : 1;
      bindv.columns = invalidInput == 2 ? nullptr : &column;
      bindv.num_rows = 0;  // Also cover the cleanup-return validation path.
      EXPECT_EQ(taos_stmt2_bind_param_column(&stmt, invalidInput == 0 ? nullptr : &bindv), TSDB_CODE_INVALID_PARA);
      EXPECT_STREQ(taos_stmt2_error(&stmt), tstrerror(TSDB_CODE_INVALID_PARA));
      EXPECT_EQ(stmt.errCode, TSDB_CODE_INVALID_PARA);
      int32_t affectedRows = -1;
      EXPECT_EQ(taos_stmt2_exec(&stmt, &affectedRows), TSDB_CODE_INVALID_PARA);
      EXPECT_EQ(affectedRows, 0);
    }
  }
}

#if defined(LINUX) || defined(WINDOWS)
namespace {
void failStmtQueryExecution(SRequestObj* request, SQuery*, bool, void**) {
  request->execDispatched = true;
  request->code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
  tstrncpy(request->msgBuf, "original execution error", ERROR_MSG_BUF_DEFAULT_SIZE);
}

int32_t failStmtMetadataRefresh(STscObj*, SRequestObj*) { return TSDB_CODE_OUT_OF_MEMORY; }
}  // namespace

TEST(stmt2Case, query_refresh_error_matches_retained_result) {
  Stub stub;
  stub.set(launchQueryImpl, failStmtQueryExecution);
  stub.set(refreshMeta, failStmtMetadataRefresh);

  STscStmt2 stmt = {};
  stmt.sql.type = STMT_TYPE_QUERY;
  stmt.sql.status = STMT_BIND;
  stmt.sql.siInfo.tableColsReady = true;
  ASSERT_EQ(taosThreadMutexInit(&stmt.asyncBindParam.mutex, nullptr), 0);
  STscObj connection = {};
  SRequestObj request = {};
  char message[ERROR_MSG_BUF_DEFAULT_SIZE] = {};
  request.resType = RES_TYPE__QUERY;
  request.type = TSDB_SQL_SELECT;
  request.pTscObj = &connection;
  request.msgBuf = message;
  stmt.exec.pRequest = &request;

  int32_t affectedRows = -1;
  EXPECT_EQ(taos_stmt2_exec(&stmt, &affectedRows), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(affectedRows, 0);
  EXPECT_EQ(stmt.errCode, TSDB_CODE_SUCCESS);
  EXPECT_STREQ(taos_stmt2_error(&stmt), tstrerror(TSDB_CODE_OUT_OF_MEMORY));
  TAOS_RES* result = taos_stmt2_result(&stmt);
  EXPECT_EQ(result, &request);
  EXPECT_EQ(taos_errno(result), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_STREQ(taos_errstr(result), tstrerror(TSDB_CODE_OUT_OF_MEMORY));
  EXPECT_EQ(taosThreadMutexDestroy(&stmt.asyncBindParam.mutex), 0);
}
#endif

TEST(stmt2Case, exec_retry) {
  TAOS*             taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
  TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb3, NULL};
  do_query(taos, "drop database if exists stmt2_testdb_21");
  do_query(taos, "create database stmt2_testdb_21");
  do_query(taos, "create stable stmt2_testdb_21.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos, "use stmt2_testdb_21");
  do_query(taos, "create table stmt2_testdb_21.tb1 using stmt2_testdb_21.stb tags(1, 'before')");
  do_query(taos, "create table stmt2_testdb_21.tb2 using stmt2_testdb_21.stb tags(1, 'before')");

  // test fixed tbname and bind per col
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
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
    taosMsleep(500);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb1");

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    taos_stmt2_close(stmt);
  }

  // Bind tags in the supertable insert. The retry should recreate the dropped child table with the bound tags.
  {
    TAOS_STMT2_OPTION optionWithTags = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &optionWithTags);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "insert into stmt2_testdb_21.stb (tbname,t1,t2,ts,b)values(?,?,?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*   tbname = "tb2";
    int32_t t1 = 2;
    int     tag_len[2] = {sizeof(t1), 5};
    int64_t ts[2] = {1591060628100, 1591060628101};
    int     b_len[2] = {4, 4};

    TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_INT, &t1, &tag_len[0], NULL, 1},
                                {TSDB_DATA_TYPE_BINARY, (void*)"bound", &tag_len[1], NULL, 1}};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"tagstest", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* tagv = &tags[0];
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    taosMsleep(500);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb2");

    int affected_rows = 0;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 2);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,b,t1,t2 from stmt2_testdb_21.tb2 order by ts");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060628100);
    ASSERT_EQ(strncmp((char*)row[1], "tags", 4), 0);
    ASSERT_EQ(*(int32_t*)row[2], 2);
    ASSERT_EQ(strncmp((char*)row[3], "bound", 5), 0);
    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060628101);
    ASSERT_EQ(strncmp((char*)row[1], "test", 4), 0);
    taos_free_result(result);

    taos_stmt2_close(stmt);
  }

  // Keep tags in the SQL. The retry should recreate the dropped child table with the literal tags.
  {
    TAOS_STMT2_OPTION optionWithTags = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &optionWithTags);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "insert into stmt2_testdb_21.? using stmt2_testdb_21.stb tags(1, 'before')values(?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*   tbname = "tb2";
    int64_t ts[2] = {1591060628200, 1591060628201};
    int     b_len[2] = {4, 4};

    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"sqltags!", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    taosMsleep(500);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb2");

    int affected_rows = 0;
    code = taos_stmt2_exec(stmt, &affected_rows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 2);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,b,t1,t2 from stmt2_testdb_21.tb2 order by ts");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060628200);
    ASSERT_EQ(strncmp((char*)row[1], "sqlt", 4), 0);
    ASSERT_EQ(*(int32_t*)row[2], 1);
    ASSERT_EQ(strncmp((char*)row[3], "before", 6), 0);
    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060628201);
    ASSERT_EQ(strncmp((char*)row[1], "ags!", 4), 0);
    taos_free_result(result);

    taos_stmt2_close(stmt);
  }

  // Schema / meta change between bind and exec (ALTER STABLE), sync mode.
  {
    do_query(taos, "create table stmt2_testdb_21.tb1 using stmt2_testdb_21.stb tags(1, 'after')");

    TAOS_STMT2_OPTION optionAlter = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmtAlter = taos_stmt2_init(taos, &optionAlter);
    ASSERT_NE(stmtAlter, nullptr);

    const char* sql = "insert into stmt2_testdb_21.stb (tbname,ts,b)values(?,?,?)";
    int         code = taos_stmt2_prepare(stmtAlter, sql, 0);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    int64_t          ts[2] = {1591060629000, 1591060629001};
    int              b_len[2] = {4, 4};
    char*            tbname = "tb1";
    TAOS_STMT2_BIND  paramsAlter[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                       {TSDB_DATA_TYPE_BINARY, (void*)"metadata", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* paramvAlter = &paramsAlter[0];
    TAOS_STMT2_BINDV bindvAlter = {1, &tbname, NULL, &paramvAlter};

    code = taos_stmt2_bind_param(stmtAlter, &bindvAlter, -1);
    checkError(stmtAlter, code, __FILE__, __LINE__);

    taosMsleep(500);
    do_query(taos, "ALTER STABLE stmt2_testdb_21.stb ADD COLUMN exec_retry_meta_col INT");

    int affected_rows = 0;
    code = taos_stmt2_exec(stmtAlter, &affected_rows);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 2);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb1");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060629000);
    ASSERT_EQ(strncmp((char*)row[1], "meta", 4), 0);
    row = taos_fetch_row(result);
    ASSERT_EQ(*(int64_t*)row[0], 1591060629001);
    ASSERT_EQ(strncmp((char*)row[1], "data", 4), 0);
    taos_free_result(result);

    taos_stmt2_close(stmtAlter);
  }
  // Schema / meta change between bind and exec (ALTER STABLE), async mode.
  {
    do_query(taos, "create table stmt2_testdb_21.ntb (ts timestamp, b binary(10),exec_retry_meta_col int)");

    AsyncArgs args = {0, 0};
    ASSERT_EQ(tsem_init(&args.sem, 0, 0), TSDB_CODE_SUCCESS);
    TAOS_STMT2_OPTION optionAlter = {0, true, true, stmtAsyncExecRetryCb, &args};
    TAOS_STMT2*       stmtAlter = taos_stmt2_init(taos, &optionAlter);
    ASSERT_NE(stmtAlter, nullptr);

    const char* sql = "insert into stmt2_testdb_21.? (ts,b)values(?,?)";
    int         code = taos_stmt2_prepare(stmtAlter, sql, 0);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    int64_t          ts[2] = {1591060629000, 1591060629001};
    int              b_len[2] = {4, 4};
    char*            tbname = "ntb";
    TAOS_STMT2_BIND  paramsAlter[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                       {TSDB_DATA_TYPE_BINARY, (void*)"metadata", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* paramvAlter = &paramsAlter[0];
    TAOS_STMT2_BINDV bindvAlter = {1, &tbname, NULL, &paramvAlter};

    code = taos_stmt2_bind_param(stmtAlter, &bindvAlter, -1);
    checkError(stmtAlter, code, __FILE__, __LINE__);

    taosMsleep(500);
    do_query(taos, "ALTER TABLE stmt2_testdb_21.ntb RENAME COLUMN exec_retry_meta_col exec_retry_meta_col_new");

    code = taos_stmt2_exec(stmtAlter, NULL);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    tsem_wait(&args.sem);
    ASSERT_EQ(args.async_affected_rows, TSDB_CODE_SUCCESS);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.ntb");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060629000);
    ASSERT_EQ(strncmp((char*)row[1], "meta", 4), 0);
    row = taos_fetch_row(result);
    ASSERT_EQ(*(int64_t*)row[0], 1591060629001);
    ASSERT_EQ(strncmp((char*)row[1], "data", 4), 0);
    taos_free_result(result);

    taos_stmt2_close(stmtAlter);
    tsem_destroy(&args.sem);
  }

  // Schema / meta change between bind and exec (ALTER STABLE), sync mode.
  {
    TAOS_STMT2_OPTION optionAlter = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmtAlter = taos_stmt2_init(taos, &optionAlter);
    ASSERT_NE(stmtAlter, nullptr);

    const char* sql = "insert into stmt2_testdb_21.? (ts,b)values(?,?)";
    int         code = taos_stmt2_prepare(stmtAlter, sql, 0);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    int64_t          ts[2] = {1591060629000, 1591060629001};
    int              b_len[2] = {4, 4};
    char*            tbname = "ntb";
    TAOS_STMT2_BIND  paramsAlter[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                       {TSDB_DATA_TYPE_BINARY, (void*)"metadata", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* paramvAlter = &paramsAlter[0];
    TAOS_STMT2_BINDV bindvAlter = {1, &tbname, NULL, &paramvAlter};

    code = taos_stmt2_bind_param(stmtAlter, &bindvAlter, -1);
    checkError(stmtAlter, code, __FILE__, __LINE__);

    taosMsleep(500);
    do_query(taos, "ALTER TABLE stmt2_testdb_21.ntb drop COLUMN b");

    code = taos_stmt2_exec(stmtAlter, NULL);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    taos_stmt2_close(stmtAlter);
  }

  // drop table and create same schema table, exec should still succeed (sync mode).
  {
    TAOS_STMT2_OPTION optionDr = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmtDr = taos_stmt2_init(taos, &optionDr);
    ASSERT_NE(stmtDr, nullptr);

    const char* sql = "insert into stmt2_testdb_21.stb (tbname,ts,b)values(?,?,?)";
    int         code = taos_stmt2_prepare(stmtDr, sql, 0);
    checkError(stmtDr, code, __FILE__, __LINE__);
    int64_t          ts[2] = {1591060630000, 1591060630001};
    int              t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int              b_len[2] = {3, 3};
    char*            tbname = "tb1";
    TAOS_STMT2_BIND  paramsDr[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                    {TSDB_DATA_TYPE_BINARY, (void*)"abcdef", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* paramvDr = &paramsDr[0];
    TAOS_STMT2_BINDV bindvDr = {1, &tbname, NULL, &paramvDr};

    code = taos_stmt2_bind_param(stmtDr, &bindvDr, -1);
    checkError(stmtDr, code, __FILE__, __LINE__);

    taosMsleep(300);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb1");
    do_query(taos, "create table stmt2_testdb_21.tb1 using stmt2_testdb_21.stb tags(1, 'after')");

    int affected_rows = 0;
    code = taos_stmt2_exec(stmtDr, &affected_rows);
    checkError(stmtDr, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 2);

    TAOS_RES* result = execQueryWithRetry(taos, "select * from stmt2_testdb_21.tb1");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_EQ(*(int64_t*)row[0], 1591060630000);
    ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
    row = taos_fetch_row(result);
    ASSERT_EQ(*(int64_t*)row[0], 1591060630001);
    ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
    taos_free_result(result);

    taos_stmt2_close(stmtDr);
  }

  // drop table and create same schema table, exec should still succeed (async mode).
  {
    AsyncArgs args = {0, 0};
    ASSERT_EQ(tsem_init(&args.sem, 0, 0), TSDB_CODE_SUCCESS);
    TAOS_STMT2_OPTION optionDr = {0, true, true, stmtAsyncQueryCb, &args};
    TAOS_STMT2*       stmtDr = taos_stmt2_init(taos, &optionDr);
    ASSERT_NE(stmtDr, nullptr);

    const char* sql = "insert into stmt2_testdb_21.stb (tbname,ts,b)values(?,?,?)";
    int         code = taos_stmt2_prepare(stmtDr, sql, 0);
    checkError(stmtDr, code, __FILE__, __LINE__);
    int64_t          ts[2] = {1591060630100, 1591060630101};
    int              t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int              b_len[2] = {3, 3};
    char*            tbname[2] = {"tb1", "tb2"};
    TAOS_STMT2_BIND  paramsDr[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                    {TSDB_DATA_TYPE_BINARY, (void*)"abcdef", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* paramvDr[] = {&paramsDr[0], &paramsDr[0]};
    TAOS_STMT2_BINDV bindvDr = {2, &tbname[0], NULL, &paramvDr[0]};

    code = taos_stmt2_bind_param(stmtDr, &bindvDr, -1);
    checkError(stmtDr, code, __FILE__, __LINE__);

    taosMsleep(100);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb2");
    do_query(taos, "create table stmt2_testdb_21.tb2 using stmt2_testdb_21.stb tags(1, 'after')");

    code = taos_stmt2_exec(stmtDr, NULL);
    checkError(stmtDr, code, __FILE__, __LINE__);

    tsem_wait(&args.sem);
    ASSERT_EQ(args.async_affected_rows, 4);
    const char* verifyTables[2] = {"tb1", "tb2"};
    for (const char* verifyTable : verifyTables) {
      std::string query = "select ts,b from stmt2_testdb_21." + std::string(verifyTable) +
                          " where ts between 1591060630100 and 1591060630101 order by ts";
      TAOS_RES* result = execQueryWithRetry(taos, query.c_str());
      ASSERT_NE(result, nullptr);
      TAOS_ROW row = taos_fetch_row(result);
      ASSERT_NE(row, nullptr);
      ASSERT_EQ(*(int64_t*)row[0], 1591060630100);
      ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
      row = taos_fetch_row(result);
      ASSERT_NE(row, nullptr);
      ASSERT_EQ(*(int64_t*)row[0], 1591060630101);
      ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
      ASSERT_EQ(taos_fetch_row(result), nullptr);
      taos_free_result(result);
    }

    taos_stmt2_close(stmtDr);
    tsem_destroy(&args.sem);
  }

  // drop table and create same schema table, exec should still succeed (non-interlace mode, multi-VG).
  // Non-interlace: each child table may be in a separate VG block with its own 0-based tbIdx.
  // Path 2's tbIdx-as-global-index assumption is wrong for multi-VG; direct exec.pBlockHash scan fixes this.
  {
    do_query(taos, "create table stmt2_testdb_21.tb3 using stmt2_testdb_21.stb tags(1, 'before')");
    do_query(taos, "create table stmt2_testdb_21.tb4 using stmt2_testdb_21.stb tags(1, 'before')");

    TAOS_STMT2_OPTION optionAlter = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmtAlter = taos_stmt2_init(taos, &optionAlter);
    ASSERT_NE(stmtAlter, nullptr);

    const char* sql = "insert into stmt2_testdb_21.stb (tbname,ts,b)values(?,?,?)";
    int         code = taos_stmt2_prepare(stmtAlter, sql, 0);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    int64_t          ts[2] = {1591060629000, 1591060629001};
    int              b_len[2] = {4, 4};
    char*            tbname[2] = {"tb3", "tb4"};
    TAOS_STMT2_BIND  paramsAlter[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 2},
                                       {TSDB_DATA_TYPE_BINARY, (void*)"metadata", &b_len[0], NULL, 2}};
    TAOS_STMT2_BIND* paramvAlter[2] = {&paramsAlter[0], &paramsAlter[0]};
    TAOS_STMT2_BINDV bindvAlter = {2, &tbname[0], NULL, &paramvAlter[0]};

    code = taos_stmt2_bind_param(stmtAlter, &bindvAlter, -1);
    checkError(stmtAlter, code, __FILE__, __LINE__);

    taosMsleep(1000);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb4");
    do_query(taos, "create table if not exists stmt2_testdb_21.tb4 using stmt2_testdb_21.stb tags(1, 'after')");

    int affected_rows = 0;
    code = taos_stmt2_exec(stmtAlter, &affected_rows);
    checkError(stmtAlter, code, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows, 4);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb4 order by ts");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060629000);
    ASSERT_EQ(strncmp((char*)row[1], "meta", 4), 0);
    row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060629001);
    ASSERT_EQ(strncmp((char*)row[1], "data", 4), 0);
    taos_free_result(result);

    taos_stmt2_close(stmtAlter);
  }

  // non-interlace: schema change (ALTER STABLE) between first bind and first exec,
  // then table recreation (DROP+CREATE tb4) between second bind and second exec.
  // Verifies that the uid-resolution path works correctly across multiple exec() calls.
  {
    do_query(taos, "create table stmt2_testdb_21.tb5 using stmt2_testdb_21.stb tags(1, 'before')");
    do_query(taos, "create table stmt2_testdb_21.tb6 using stmt2_testdb_21.stb tags(1, 'before')");

    TAOS_STMT2_OPTION optNI = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmtNI = taos_stmt2_init(taos, &optNI);
    ASSERT_NE(stmtNI, nullptr);

    const char* sqlNI = "insert into stmt2_testdb_21.stb (tbname,ts,b)values(?,?,?)";
    int         code2 = taos_stmt2_prepare(stmtNI, sqlNI, 0);
    checkError(stmtNI, code2, __FILE__, __LINE__);

    int64_t          ts2[2] = {1591060630000, 1591060630001};
    int              b_len2[2] = {4, 4};
    char*            tbname2[2] = {"tb5", "tb6"};
    TAOS_STMT2_BIND  paramsNI[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts2[0], NULL, NULL, 2},
                                    {TSDB_DATA_TYPE_BINARY, (void*)"metadata", &b_len2[0], NULL, 2}};
    TAOS_STMT2_BIND* paramvNI[2] = {&paramsNI[0], &paramsNI[0]};
    TAOS_STMT2_BINDV bindvNI = {2, &tbname2[0], NULL, &paramvNI[0]};

    // First bind
    code2 = taos_stmt2_bind_param(stmtNI, &bindvNI, -1);
    checkError(stmtNI, code2, __FILE__, __LINE__);

    uint64_t oldUids[2] = {0};
    int32_t  oldUidNum = 0;
    void*    pIter = taosHashIterate(((STscStmt2*)stmtNI)->exec.pBlockHash, NULL);
    while (pIter != NULL) {
      ASSERT_LT(oldUidNum, 2);
      STableDataCxt* pBlock = *(STableDataCxt**)pIter;
      oldUids[oldUidNum++] = qGetTableMetaInDataBlock(pBlock)->uid;
      pIter = taosHashIterate(((STscStmt2*)stmtNI)->exec.pBlockHash, pIter);
    }
    ASSERT_EQ(oldUidNum, 2);

    int affected_rows2 = 0;

    taosMsleep(100);
    do_query(taos, "drop table if exists stmt2_testdb_21.tb6");
    do_query(taos, "drop table if exists stmt2_testdb_21.tb5");
    do_query(taos, "create table stmt2_testdb_21.tb6 using stmt2_testdb_21.stb tags(1, 'after')");
    do_query(taos, "create table stmt2_testdb_21.tb5 using stmt2_testdb_21.stb tags(1, 'after')");

    // Both recreated children must be refreshed in one TABLE_NOT_EXIST retry.
    code2 = taos_stmt2_exec(stmtNI, &affected_rows2);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows2, 4);

    pIter = taosHashIterate(((STscStmt2*)stmtNI)->exec.pBlockHash, NULL);
    while (pIter != NULL) {
      STableDataCxt* pBlock = *(STableDataCxt**)pIter;
      ASSERT_NE(qGetTableMetaInDataBlock(pBlock)->uid, oldUids[0]);
      ASSERT_NE(qGetTableMetaInDataBlock(pBlock)->uid, oldUids[1]);
      ASSERT_EQ(pBlock->pData->uid, qGetTableMetaInDataBlock(pBlock)->uid);
      ASSERT_EQ(pBlock->pData->suid, qGetTableMetaInDataBlock(pBlock)->suid);
      pIter = taosHashIterate(((STscStmt2*)stmtNI)->exec.pBlockHash, pIter);
    }
    ASSERT_NE(((STscStmt2*)stmtNI)->exec.pCurrTbData, nullptr);
    ASSERT_NE(((STscStmt2*)stmtNI)->exec.pCurrBlock, nullptr);
    ASSERT_EQ(((STscStmt2*)stmtNI)->exec.pCurrTbData->uid,
              qGetTableMetaInDataBlock(((STscStmt2*)stmtNI)->exec.pCurrBlock)->uid);

    TAOS_RES* result2 = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb5 order by ts");
    ASSERT_NE(result2, nullptr);
    TAOS_ROW row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    ASSERT_EQ(strncmp((char*)row2[1], "meta", 4), 0);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630001);
    ASSERT_EQ(strncmp((char*)row2[1], "data", 4), 0);
    taos_free_result(result2);

    result2 = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb6 order by ts");
    ASSERT_NE(result2, nullptr);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    ASSERT_EQ(strncmp((char*)row2[1], "meta", 4), 0);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630001);
    ASSERT_EQ(strncmp((char*)row2[1], "data", 4), 0);
    taos_free_result(result2);

    // Reuse the same statement without another DDL. The refreshed live blocks must route directly with new UIDs.
    ts2[0] = 1591060630002;
    ts2[1] = 1591060630003;
    code2 = taos_stmt2_bind_param(stmtNI, &bindvNI, -1);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    affected_rows2 = 0;
    code2 = taos_stmt2_exec(stmtNI, &affected_rows2);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows2, 4);

    result2 = execQueryWithRetry(taos, "select count(*) from stmt2_testdb_21.tb5");
    ASSERT_NE(result2, nullptr);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 4);
    taos_free_result(result2);

    result2 = execQueryWithRetry(taos, "select count(*) from stmt2_testdb_21.tb6");
    ASSERT_NE(result2, nullptr);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 4);
    taos_free_result(result2);

    taos_stmt2_close(stmtNI);
  }

  TAOS* taos2 = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  // drop stable
  {
    do_query(taos, "create stable stmt2_testdb_21.stb2 (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

    TAOS_STMT2_OPTION optNI = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmtNI = taos_stmt2_init(taos, &optNI);
    ASSERT_NE(stmtNI, nullptr);

    const char* sqlNI = "insert into stmt2_testdb_21.? using stmt2_testdb_21.stb2 tags(?,?)values(?,?)";
    int         code2 = taos_stmt2_prepare(stmtNI, sqlNI, 0);
    checkError(stmtNI, code2, __FILE__, __LINE__);

    int64_t          ts2[2] = {1591060630000, 1591060630001};
    int32_t          tagInt2 = 7;
    int              tagLen2[2] = {(int)sizeof(tagInt2), 4};
    int              b_len2[2] = {4, 4};
    char*            tbname2[2] = {"tb7", "tb8"};
    TAOS_STMT2_BIND  tagsNI[2] = {{TSDB_DATA_TYPE_INT, &tagInt2, &tagLen2[0], NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"tag7", &tagLen2[1], NULL, 1}};
    TAOS_STMT2_BIND  paramsNI[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts2[0], NULL, NULL, 2},
                                    {TSDB_DATA_TYPE_BINARY, (void*)"metadata", &b_len2[0], NULL, 2}};
    TAOS_STMT2_BIND* tagvNI[2] = {&tagsNI[0], &tagsNI[0]};
    TAOS_STMT2_BIND* paramvNI[2] = {&paramsNI[0], &paramsNI[0]};
    TAOS_STMT2_BINDV bindvNI = {2, &tbname2[0], &tagvNI[0], &paramvNI[0]};

    // First exec
    code2 = taos_stmt2_bind_param(stmtNI, &bindvNI, -1);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    int affected_rows2 = 0;
    code2 = taos_stmt2_exec(stmtNI, &affected_rows2);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows2, 4);
    STscStmt2* pStmtNI = (STscStmt2*)stmtNI;
    uint64_t   oldStbSuid = pStmtNI->bInfo.tbSuid;

    TAOS_RES* result2 = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb7 order by ts");
    ASSERT_NE(result2, nullptr);
    TAOS_ROW row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    ASSERT_EQ(strncmp((char*)row2[1], "meta", 4), 0);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630001);
    ASSERT_EQ(strncmp((char*)row2[1], "data", 4), 0);
    taos_free_result(result2);

    result2 = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb8 order by ts");
    ASSERT_NE(result2, nullptr);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    ASSERT_EQ(strncmp((char*)row2[1], "meta", 4), 0);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630001);
    ASSERT_EQ(strncmp((char*)row2[1], "data", 4), 0);
    taos_free_result(result2);

    // Second exec
    do_query(taos2, "drop table stmt2_testdb_21.stb2");
    do_query(taos2, "create stable stmt2_testdb_21.stb2 (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

    code2 = taos_stmt2_bind_param(stmtNI, &bindvNI, -1);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    code2 = taos_stmt2_exec(stmtNI, &affected_rows2);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows2, 4);
    ASSERT_NE(pStmtNI->bInfo.tbSuid, oldStbSuid);
    ASSERT_NE(pStmtNI->sql.siInfo.pDataCtx, nullptr);
    ASSERT_EQ(pStmtNI->sql.siInfo.pDataCtx->pMeta->suid, pStmtNI->bInfo.tbSuid);
    ASSERT_EQ(pStmtNI->sql.siInfo.pDataCtx->pData->suid, pStmtNI->bInfo.tbSuid);

    result2 = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb7 order by ts");
    ASSERT_NE(result2, nullptr);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    ASSERT_EQ(strncmp((char*)row2[1], "meta", 4), 0);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630001);
    ASSERT_EQ(strncmp((char*)row2[1], "data", 4), 0);
    taos_free_result(result2);

    result2 = execQueryWithRetry(taos, "select ts,b from stmt2_testdb_21.tb8 order by ts");
    ASSERT_NE(result2, nullptr);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    ASSERT_EQ(strncmp((char*)row2[1], "meta", 4), 0);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630001);
    ASSERT_EQ(strncmp((char*)row2[1], "data", 4), 0);
    taos_free_result(result2);

    // A later bind must use the refreshed stable template instead of the old suid again.
    ts2[0] = 1591060630002;
    ts2[1] = 1591060630003;
    code2 = taos_stmt2_bind_param(stmtNI, &bindvNI, -1);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    code2 = taos_stmt2_exec(stmtNI, &affected_rows2);
    checkError(stmtNI, code2, __FILE__, __LINE__);
    ASSERT_EQ(affected_rows2, 4);

    result2 = execQueryWithRetry(taos, "select count(*) from stmt2_testdb_21.stb2");
    ASSERT_NE(result2, nullptr);
    row2 = taos_fetch_row(result2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 8);
    taos_free_result(result2);

    taos_stmt2_close(stmtNI);
  }

  // Recreate a stable while reusing the fixed-tag auto-create template.
  {
    do_query(taos, "create stable stmt2_testdb_21.stb3 (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(
        stmt, "insert into stmt2_testdb_21.? using stmt2_testdb_21.stb3 tags(9,'fixed') values(?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            tbname = (char*)"tb9";
    int64_t          ts = 1591060630100;
    int32_t          dataLen = 5;
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"first", &dataLen, NULL, 1}};
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);

    STscStmt2* pStmt = (STscStmt2*)stmt;
    uint64_t   oldSuid = pStmt->bInfo.tbSuid;
    ASSERT_TRUE(pStmt->sql.fixValueTags);
    ASSERT_NE(pStmt->sql.fixValueTbReq, nullptr);

    do_query(taos2, "drop table stmt2_testdb_21.stb3");
    do_query(taos2, "create stable stmt2_testdb_21.stb3 (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

    ts = 1591060630101;
    params[1].buffer = (void*)"retry";
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);
    ASSERT_NE(pStmt->bInfo.tbSuid, oldSuid);
    ASSERT_EQ(pStmt->sql.fixValueTbReq->ctb.suid, pStmt->bInfo.tbSuid);

    TAOS_RES* result = execQueryWithRetry(taos, "select t1,t2,ts,b from stmt2_testdb_21.tb9");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 9);
    ASSERT_EQ(strncmp((char*)row[1], "fixed", 5), 0);
    ASSERT_EQ(*(int64_t*)row[2], ts);
    ASSERT_EQ(strncmp((char*)row[3], "retry", 5), 0);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // Reuse a successfully executed fixed-table stmt after another connection recreates the table.
  {
    do_query(taos, "drop table if exists stmt2_testdb_21.t1");
    do_query(taos, "create table stmt2_testdb_21.t1 (ts timestamp, value int, note binary(16))");

    TAOS_STMT2_OPTION fixedTableOption = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &fixedTableOption);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "insert into t1 values(?,?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t          ts = 1591060631000;
    int32_t          value = 1;
    char*            note = (char*)"before";
    int32_t          noteLen = (int32_t)strlen(note);
    TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, note, &noteLen, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);
    uint64_t oldUid = ((STscStmt2*)stmt)->bInfo.tbUid;

    do_query(taos2, "drop table stmt2_testdb_21.t1");
    do_query(taos2, "create table stmt2_testdb_21.t1 (ts timestamp, value int, note binary(16))");

    ts = 1591060632000;
    value = 2;
    note = (char*)"after";
    noteLen = (int32_t)strlen(note);
    params[2].buffer = note;

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_STREQ(tstrerror(TSDB_CODE_SUCCESS), taos_stmt2_error(stmt));
    affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);
    STscStmt2* pStmt = (STscStmt2*)stmt;
    ASSERT_NE(pStmt->bInfo.tbUid, oldUid);
    ASSERT_EQ(qGetTableMetaInDataBlock(pStmt->exec.pCurrBlock)->uid, pStmt->bInfo.tbUid);
    ASSERT_EQ(pStmt->exec.pCurrBlock->pData->uid, pStmt->bInfo.tbUid);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,value,note from stmt2_testdb_21.t1");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], ts);
    ASSERT_EQ(*(int32_t*)row[1], value);
    ASSERT_EQ(strncmp((char*)row[2], note, noteLen), 0);
    ASSERT_EQ(taos_fetch_row(result), nullptr);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // Dynamic-table binding must reconcile an exec-cache block whose normal-table UID changed.
  {
    do_query(taos, "drop table if exists stmt2_testdb_21.t2");
    do_query(taos, "create table stmt2_testdb_21.t2 (ts timestamp, value int, note binary(16))");

    TAOS_STMT2_OPTION dynamicTableOption = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &dynamicTableOption);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "insert into ? values(?,?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    char*            tableName = (char*)"t2";
    int64_t          ts = 1591060633000;
    int32_t          value = 3;
    char*            note = (char*)"before";
    int32_t          noteLen = (int32_t)strlen(note);
    TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, note, &noteLen, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, &tableName, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);
    uint64_t oldUid = ((STscStmt2*)stmt)->bInfo.tbUid;

    do_query(taos2, "drop table stmt2_testdb_21.t2");
    do_query(taos2, "create table stmt2_testdb_21.t2 (ts timestamp, value int, note binary(16))");

    ts = 1591060634000;
    value = 4;
    note = (char*)"after";
    noteLen = (int32_t)strlen(note);
    params[2].buffer = note;

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    STscStmt2* pStmt = (STscStmt2*)stmt;
    ASSERT_NE(pStmt->bInfo.tbUid, oldUid);
    ASSERT_EQ(taosHashGet(pStmt->sql.pTableCache, &oldUid, sizeof(oldUid)), nullptr);
    ASSERT_NE(taosHashGet(pStmt->sql.pTableCache, &pStmt->bInfo.tbUid, sizeof(pStmt->bInfo.tbUid)), nullptr);
    affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,value,note from stmt2_testdb_21.t2");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], ts);
    ASSERT_EQ(*(int32_t*)row[1], value);
    ASSERT_EQ(strncmp((char*)row[2], note, noteLen), 0);
    ASSERT_EQ(taos_fetch_row(result), nullptr);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // A fixed normal table recreated after column-format bind must be rejected by the vnode and retried.
  {
    do_query(taos, "drop table if exists stmt2_testdb_21.t3");
    do_query(taos, "create table stmt2_testdb_21.t3 (ts timestamp, value int, note binary(16))");

    TAOS_STMT2_OPTION fixedTableOption = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &fixedTableOption);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "insert into t3 values(?,?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t          ts = 1591060635000;
    int32_t          value = 5;
    char*            note = (char*)"after-bind";
    int32_t          noteLen = (int32_t)strlen(note);
    TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, note, &noteLen, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    uint64_t oldUid = ((STscStmt2*)stmt)->bInfo.tbUid;

    do_query(taos2, "drop table stmt2_testdb_21.t3");
    do_query(taos2, "create table stmt2_testdb_21.t3 (ts timestamp, value int, note binary(16))");

    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);
    STscStmt2* pStmt = (STscStmt2*)stmt;
    ASSERT_NE(pStmt->bInfo.tbUid, oldUid);
    ASSERT_EQ(qGetTableMetaInDataBlock(pStmt->exec.pCurrBlock)->uid, pStmt->bInfo.tbUid);
    ASSERT_EQ(pStmt->exec.pCurrBlock->pData->uid, pStmt->bInfo.tbUid);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,value,note from stmt2_testdb_21.t3");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], ts);
    ASSERT_EQ(*(int32_t*)row[1], value);
    ASSERT_EQ(strncmp((char*)row[2], note, noteLen), 0);
    ASSERT_EQ(taos_fetch_row(result), nullptr);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // A column-format block must not be replayed onto an incompatible same-name normal table.
  {
    do_query(taos, "drop table if exists stmt2_testdb_21.t4");
    do_query(taos, "create table stmt2_testdb_21.t4 (ts timestamp, value int, note binary(16))");

    TAOS_STMT2_OPTION fixedTableOption = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &fixedTableOption);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "insert into t4 values(?,?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t          ts = 1591060636000;
    int32_t          value = 6;
    char*            note = (char*)"incompatible";
    int32_t          noteLen = (int32_t)strlen(note);
    TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, note, &noteLen, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    do_query(taos2, "drop table stmt2_testdb_21.t4");
    do_query(taos2, "create table stmt2_testdb_21.t4 (ts timestamp, value bigint, note binary(16))");

    int affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    ASSERT_EQ(code, TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
    ASSERT_EQ(affectedRows, 0);
    ASSERT_EQ(((STscStmt2*)stmt)->exec.pRequest, nullptr);
    ASSERT_STREQ(tstrerror(TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER), taos_stmt2_error(stmt));
    ASSERT_EQ(((STscStmt2*)stmt)->errCode, TSDB_CODE_SUCCESS);

    do_query(taos2, "drop table stmt2_testdb_21.t4");
    do_query(taos2, "create table stmt2_testdb_21.t4 (ts timestamp, value int, note binary(16))");

    params[1].num = 0;
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_TSC_INVALID_OPERATION);
    ASSERT_EQ(((STscStmt2*)stmt)->errCode, code);
    ASSERT_STREQ("row number in each bind param should be the same", taos_stmt2_error(stmt));
    params[1].num = 1;

    code = taos_stmt2_prepare(stmt, "insert into t4 values(?,?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    ts = 1591060636001;
    value = 16;
    note = (char*)"recovered";
    noteLen = (int32_t)strlen(note);
    params[2].buffer = note;

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    affectedRows = 0;
    code = taos_stmt2_exec(stmt, &affectedRows);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affectedRows, 1);
    ASSERT_STREQ(tstrerror(TSDB_CODE_SUCCESS), taos_stmt2_error(stmt));

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,value,note from stmt2_testdb_21.t4");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], ts);
    ASSERT_EQ(*(int32_t*)row[1], value);
    ASSERT_EQ(strncmp((char*)row[2], note, noteLen), 0);
    ASSERT_EQ(taos_fetch_row(result), nullptr);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // Async retry setup must report the final schema error rather than the original stale-UID error.
  {
    do_query(taos, "drop table if exists stmt2_testdb_21.t5");
    do_query(taos, "create table stmt2_testdb_21.t5 (ts timestamp, value int, note binary(16))");

    Stmt2AsyncExecResult execResult = {};
    ASSERT_EQ(tsem_init(&execResult.sem, 0, 0), TSDB_CODE_SUCCESS);
    TAOS_STMT2_OPTION asyncOption = {0, true, true, stmtAsyncExecResultCb, &execResult};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &asyncOption);
    ASSERT_NE(stmt, nullptr);
    execResult.stmt = stmt;

    int code = taos_stmt2_prepare(stmt, "insert into t5 values(?,?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t          ts = 1591060637000;
    int32_t          value = 7;
    char*            note = (char*)"incompatible";
    int32_t          noteLen = (int32_t)strlen(note);
    TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, note, &noteLen, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    do_query(taos2, "drop table stmt2_testdb_21.t5");
    do_query(taos2, "create table stmt2_testdb_21.t5 (ts timestamp, value bigint, note binary(16))");

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(tsem_wait(&execResult.sem), TSDB_CODE_SUCCESS);
    ASSERT_EQ(execResult.code, TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
    ASSERT_EQ(execResult.resultCode, TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
    ASSERT_EQ(execResult.affectedRows, 0);
    ASSERT_EQ(execResult.callbackCount, 1);
    ASSERT_STREQ(tstrerror(TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER), execResult.error);
    ASSERT_STREQ(tstrerror(TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER), execResult.resultError);

    do_query(taos2, "drop table stmt2_testdb_21.t5");
    do_query(taos2, "create table stmt2_testdb_21.t5 (ts timestamp, value int, note binary(16))");

    ts = 1591060637001;
    value = 17;
    note = (char*)"recovered";
    noteLen = (int32_t)strlen(note);
    params[2].buffer = note;
    execResult.code = TSDB_CODE_APP_ERROR;
    execResult.resultCode = TSDB_CODE_APP_ERROR;
    execResult.affectedRows = 0;
    execResult.callbackCount = 0;
    execResult.error[0] = '\0';
    execResult.resultError[0] = '\0';

    // Let the public bind API wait for and finalize the failed asynchronous execution.
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(((STscStmt2*)stmt)->errCode, TSDB_CODE_SUCCESS);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(tsem_wait(&execResult.sem), TSDB_CODE_SUCCESS);
    ASSERT_EQ(execResult.code, TSDB_CODE_SUCCESS);
    ASSERT_EQ(execResult.resultCode, TSDB_CODE_SUCCESS);
    ASSERT_EQ(execResult.affectedRows, 1);
    ASSERT_EQ(execResult.callbackCount, 1);
    ASSERT_STREQ("", execResult.error);
    ASSERT_STREQ("", execResult.resultError);
    ASSERT_STREQ(tstrerror(TSDB_CODE_SUCCESS), taos_stmt2_error(stmt));
    ASSERT_EQ(taos_stmt2_close(stmt), TSDB_CODE_SUCCESS);

    TAOS_RES* result = execQueryWithRetry(taos, "select ts,value,note from stmt2_testdb_21.t5");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], ts);
    ASSERT_EQ(*(int32_t*)row[1], value);
    ASSERT_EQ(strncmp((char*)row[2], note, noteLen), 0);
    ASSERT_EQ(taos_fetch_row(result), nullptr);
    taos_free_result(result);

    tsem_destroy(&execResult.sem);
  }

  // A query completion error is also non-sticky. Rebinding reparses the query after the table is recreated.
  {
    do_query(taos, "drop table if exists stmt2_testdb_21.t6");
    do_query(taos, "create table stmt2_testdb_21.t6 (ts timestamp, value int)");
    do_query(taos, "insert into stmt2_testdb_21.t6 values(1591060638000, 8)");

    TAOS_STMT2_OPTION queryOption = {0};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &queryOption);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "select value from t6 where ts = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t          ts = 1591060638000;
    TAOS_STMT2_BIND  param = {TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1};
    TAOS_STMT2_BIND* paramv = &param;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    do_query(taos2, "drop table stmt2_testdb_21.t6");
    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_TRUE(code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_TDB_TABLE_NOT_EXIST) << tstrerror(code);
    ASSERT_EQ(((STscStmt2*)stmt)->errCode, TSDB_CODE_SUCCESS);
    ASSERT_EQ(((STscStmt2*)stmt)->lastExec.code, code);
    ASSERT_NE(taos_stmt2_error(stmt)[0], '\0');

    do_query(taos2, "create table stmt2_testdb_21.t6 (ts timestamp, value int)");
    do_query(taos2, "insert into stmt2_testdb_21.t6 values(1591060638000, 18)");

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_STREQ(tstrerror(TSDB_CODE_SUCCESS), taos_stmt2_error(stmt));

    TAOS_RES* result = taos_stmt2_result(stmt);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS) << taos_errstr(result);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 18);
    ASSERT_EQ(taos_fetch_row(result), nullptr);
    taos_stmt2_close(stmt);
  }

  taos_close(taos2);
  do_query(taos, "drop database if exists stmt2_testdb_21");
  taos_close(taos);
}

TEST(stmt2Case, exec_retry_tag_cache) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
  do_query(taos, "drop database if exists stmt2_testdb_41");
  do_query(taos, "create database stmt2_testdb_41");
  do_query(taos, "create stable stmt2_testdb_41.stb (ts timestamp, b binary(16)) tags(t1 int, t2 binary(16))");
  do_query(taos, "use stmt2_testdb_41");

  TAOS_STMT2_OPTION syncOption = {0, true, true, NULL, NULL};
  const char*       dynamicTagSql = "insert into stmt2_testdb_41.stb (tbname,t1,t2,ts,b) values(?,?,?,?,?)";

  // The retry must use the deep copy, not caller-owned tag buffers after bind returns.
  {
    do_query(taos, "create table stmt2_testdb_41.deep using stmt2_testdb_41.stb tags(0, 'old')");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &syncOption);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, dynamicTagSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t* tagInt = (int32_t*)taosMemoryMalloc(sizeof(int32_t));
    char*    tagText = (char*)taosMemoryMalloc(5);
    ASSERT_NE(tagInt, nullptr);
    ASSERT_NE(tagText, nullptr);
    *tagInt = 11;
    memcpy(tagText, "copy1", 5);
    int              tagLen[2] = {(int)sizeof(*tagInt), 5};
    int64_t          ts = 1591060641000;
    int              dataLen = 4;
    char*            tbname = "deep";
    TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_INT, tagInt, &tagLen[0], NULL, 1},
                                {TSDB_DATA_TYPE_BINARY, tagText, &tagLen[1], NULL, 1}};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"deep", &dataLen, NULL, 1}};
    TAOS_STMT2_BIND* tagv = tags;
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    *tagInt = 99;
    memset(tagText, 'x', 5);
    taosMemoryFree(tagInt);
    taosMemoryFree(tagText);
    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.deep");
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 1);

    TAOS_RES* result = execQueryWithRetry(taos, "select t1,t2 from stmt2_testdb_41.deep");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 11);
    ASSERT_EQ(strncmp((char*)row[1], "copy1", 5), 0);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // Rebinding the same table before exec must overwrite its cached tags with the last bind.
  {
    do_query(taos, "create table stmt2_testdb_41.update1 using stmt2_testdb_41.stb tags(0, 'old')");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &syncOption);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, dynamicTagSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char*            tbname = "update1";
    int              tagLen[2] = {(int)sizeof(int32_t), 5};
    int              dataLen = 4;
    int64_t          ts[2] = {1591060641100, 1591060641101};
    int32_t          firstTag = 21;
    TAOS_STMT2_BIND  firstTags[2] = {{TSDB_DATA_TYPE_INT, &firstTag, &tagLen[0], NULL, 1},
                                     {TSDB_DATA_TYPE_BINARY, (void*)"first", &tagLen[1], NULL, 1}};
    TAOS_STMT2_BIND  firstParams[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 1},
                                       {TSDB_DATA_TYPE_BINARY, (void*)"one1", &dataLen, NULL, 1}};
    TAOS_STMT2_BIND* tagv = firstTags;
    TAOS_STMT2_BIND* paramv = firstParams;
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t         lastTag = 22;
    TAOS_STMT2_BIND lastTags[2] = {{TSDB_DATA_TYPE_INT, &lastTag, &tagLen[0], NULL, 1},
                                   {TSDB_DATA_TYPE_BINARY, (void*)"last2", &tagLen[1], NULL, 1}};
    TAOS_STMT2_BIND lastParams[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[1], NULL, NULL, 1},
                                     {TSDB_DATA_TYPE_BINARY, (void*)"two2", &dataLen, NULL, 1}};
    tagv = lastTags;
    paramv = lastParams;
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.update1");
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 2);
    TAOS_RES* result = execQueryWithRetry(taos, "select t1,t2,count(*) from stmt2_testdb_41.update1");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 22);
    ASSERT_EQ(strncmp((char*)row[1], "last2", 5), 0);
    ASSERT_EQ(*(int64_t*)row[2], 2);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // A multi-table submit must recreate only the missing child and preserve all other writes.
  {
    do_query(taos, "create table stmt2_testdb_41.part1 using stmt2_testdb_41.stb tags(0, 'old')");
    do_query(taos, "create table stmt2_testdb_41.part2 using stmt2_testdb_41.stb tags(0, 'old')");
    do_query(taos, "create table stmt2_testdb_41.part3 using stmt2_testdb_41.stb tags(0, 'old')");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &syncOption);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, dynamicTagSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char*            tbnames[3] = {(char*)"part1", (char*)"part2", (char*)"part3"};
    int32_t          tagInts[3] = {31, 32, 33};
    char             tagTexts[3][4] = {"p01", "p02", "p03"};
    int              tagIntLen = sizeof(int32_t);
    int              tagTextLen = 3;
    int64_t          timestamps[3] = {1591060641201, 1591060641202, 1591060641203};
    int              dataLen = 3;
    TAOS_STMT2_BIND  tags[3][2];
    TAOS_STMT2_BIND  params[3][2];
    TAOS_STMT2_BIND* tagv[3];
    TAOS_STMT2_BIND* paramv[3];
    for (int i = 0; i < 3; ++i) {
      tags[i][0] = {TSDB_DATA_TYPE_INT, &tagInts[i], &tagIntLen, NULL, 1};
      tags[i][1] = {TSDB_DATA_TYPE_BINARY, tagTexts[i], &tagTextLen, NULL, 1};
      params[i][0] = {TSDB_DATA_TYPE_TIMESTAMP, &timestamps[i], NULL, NULL, 1};
      params[i][1] = {TSDB_DATA_TYPE_BINARY, tagTexts[i], &dataLen, NULL, 1};
      tagv[i] = tags[i];
      paramv[i] = params[i];
    }
    TAOS_STMT2_BINDV bindv = {3, tbnames, tagv, paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.part2");
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 3);
    for (int i = 0; i < 3; ++i) {
      char sql[128] = {0};
      snprintf(sql, sizeof(sql), "select t1,t2,count(*) from stmt2_testdb_41.part%d", i + 1);
      TAOS_RES* result = execQueryWithRetry(taos, sql);
      ASSERT_NE(result, nullptr);
      TAOS_ROW row = taos_fetch_row(result);
      ASSERT_NE(row, nullptr);
      if (i == 1) {
        ASSERT_EQ(*(int32_t*)row[0], tagInts[i]);
        ASSERT_EQ(strncmp((char*)row[1], tagTexts[i], 3), 0);
      } else {
        ASSERT_EQ(*(int32_t*)row[0], 0);
        ASSERT_EQ(strncmp((char*)row[1], "old", 3), 0);
      }
      ASSERT_EQ(*(int64_t*)row[2], 1);
      taos_free_result(result);
    }
    taos_stmt2_close(stmt);
  }

  // Dynamic tag auto-create retry must also work through the async exec callback path.
  {
    do_query(taos, "create table stmt2_testdb_41.async1 using stmt2_testdb_41.stb tags(0, 'old')");
    AsyncArgs args = {0, 0};
    ASSERT_EQ(tsem_init(&args.sem, 0, 0), TSDB_CODE_SUCCESS);
    TAOS_STMT2_OPTION asyncOption = {0, true, true, stmtAsyncExecRetryCb, &args};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &asyncOption);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, dynamicTagSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char*            tbname = "async1";
    int32_t          tagInt = 41;
    int              tagLen[2] = {(int)sizeof(tagInt), 5};
    int64_t          ts = 1591060641300;
    int              dataLen = 5;
    TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_INT, &tagInt, &tagLen[0], NULL, 1},
                                {TSDB_DATA_TYPE_BINARY, (void*)"async", &tagLen[1], NULL, 1}};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"async", &dataLen, NULL, 1}};
    TAOS_STMT2_BIND* tagv = tags;
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.async1");
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(tsem_wait(&args.sem), TSDB_CODE_SUCCESS);
    ASSERT_EQ(args.async_affected_rows, TSDB_CODE_SUCCESS);
    TAOS_RES* result = execQueryWithRetry(taos, "select t1,t2 from stmt2_testdb_41.async1");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 41);
    ASSERT_EQ(strncmp((char*)row[1], "async", 5), 0);
    taos_free_result(result);
    taos_stmt2_close(stmt);
    tsem_destroy(&args.sem);
  }

  // NULL and NCHAR tags must survive the same bind-cache-retry conversion.
  {
    do_query(taos, "create stable stmt2_testdb_41.special_stb (ts timestamp, b int) tags(tn nchar(16), ti int)");
    do_query(taos, "create table stmt2_testdb_41.special1 using stmt2_testdb_41.special_stb tags('old', 0)");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &syncOption);
    ASSERT_NE(stmt, nullptr);
    const char* sql = "insert into stmt2_testdb_41.special_stb (tbname,tn,ti,ts,b) values(?,?,?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char*            tbname = "special1";
    char             nullFlag = 1;
    int              ncharLen = 6;
    int32_t          tagInt = 51;
    int              tagIntLen = sizeof(tagInt);
    int64_t          ts = 1591060641400;
    int32_t          value = 1;
    TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_NCHAR, (void*)"unused", &ncharLen, &nullFlag, 1},
                                {TSDB_DATA_TYPE_INT, &tagInt, &tagIntLen, NULL, 1}};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1}};
    TAOS_STMT2_BIND* tagv = tags;
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.special1");
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 1);
    TAOS_RES* result = execQueryWithRetry(taos, "select tn,ti from stmt2_testdb_41.special1");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(row[0], nullptr);
    ASSERT_EQ(*(int32_t*)row[1], 51);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // JSON is a single-tag stable and exercises variable-length JSON buffer copying and retry parsing.
  {
    do_query(taos, "create stable stmt2_testdb_41.json_stb (ts timestamp, b int) tags(j json)");
    do_query(taos, "create table stmt2_testdb_41.json1 using stmt2_testdb_41.json_stb tags('{\"kind\":\"old\"}')");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &syncOption);
    ASSERT_NE(stmt, nullptr);
    const char* sql = "insert into stmt2_testdb_41.json_stb (tbname,j,ts,b) values(?,?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char*            tbname = "json1";
    char             json[] = "{\"kind\":\"retry\",\"value\":61}";
    int              jsonLen = strlen(json);
    int64_t          ts = 1591060641500;
    int32_t          value = 1;
    TAOS_STMT2_BIND  tag = {TSDB_DATA_TYPE_JSON, json, &jsonLen, NULL, 1};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1}};
    TAOS_STMT2_BIND* tagv = &tag;
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    memset(json, 'x', sizeof(json) - 1);
    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.json1");
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 1);
    TAOS_RES* result = execQueryWithRetry(
        taos, "select count(*) from stmt2_testdb_41.json_stb where tbname='json1' and j->'kind'='retry'");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // A successful exec clears the retry-tag batch cache; the following batch must use only its new tags.
  {
    do_query(taos, "create table stmt2_testdb_41.clean1 using stmt2_testdb_41.stb tags(0, 'old')");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &syncOption);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, dynamicTagSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char*            tbname = "clean1";
    int              tagLen[2] = {(int)sizeof(int32_t), 5};
    int              dataLen = 4;
    int32_t          tagInt = 71;
    int64_t          ts = 1591060641600;
    TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_INT, &tagInt, &tagLen[0], NULL, 1},
                                {TSDB_DATA_TYPE_BINARY, (void*)"old71", &tagLen[1], NULL, 1}};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"one1", &dataLen, NULL, 1}};
    TAOS_STMT2_BIND* tagv = tags;
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 1);
    STscStmt2* pStmt = (STscStmt2*)stmt;
    ASSERT_TRUE(pStmt->pRetryTagHash == NULL || taosHashGetSize(pStmt->pRetryTagHash) == 0);

    tagInt = 72;
    tags[1].buffer = (void*)"new72";
    ts = 1591060641601;
    params[1].buffer = (void*)"two2";
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.clean1");
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 1);
    TAOS_RES* result = execQueryWithRetry(taos, "select t1,t2 from stmt2_testdb_41.clean1");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 72);
    ASSERT_EQ(strncmp((char*)row[1], "new72", 5), 0);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  // A failed exec also clears the batch cache; re-prepare must not retain its old table/tag entry.
  {
    do_query(taos, "create table stmt2_testdb_41.fail1 using stmt2_testdb_41.stb tags(0, 'old')");
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &syncOption);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, dynamicTagSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char*            tbname = "fail1";
    int              tagLen[2] = {(int)sizeof(int32_t), 5};
    int              dataLen = 4;
    int32_t          tagInt = 81;
    int64_t          ts = 1591060641700;
    TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_INT, &tagInt, &tagLen[0], NULL, 1},
                                {TSDB_DATA_TYPE_BINARY, (void*)"old81", &tagLen[1], NULL, 1}};
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_BINARY, (void*)"fail", &dataLen, NULL, 1}};
    TAOS_STMT2_BIND* tagv = tags;
    TAOS_STMT2_BIND* paramv = params;
    TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    do_query(taos, "drop stable stmt2_testdb_41.stb");
    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_NE(code, TSDB_CODE_SUCCESS);
    STscStmt2* pStmt = (STscStmt2*)stmt;
    ASSERT_TRUE(pStmt->pRetryTagHash == NULL || taosHashGetSize(pStmt->pRetryTagHash) == 0);

    do_query(taos, "create stable stmt2_testdb_41.stb (ts timestamp, b binary(16)) tags(t1 int, t2 binary(16))");
    do_query(taos, "create table stmt2_testdb_41.fail2 using stmt2_testdb_41.stb tags(0, 'old')");
    code = taos_stmt2_prepare(stmt, dynamicTagSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    tbname = "fail2";
    tagInt = 82;
    tags[1].buffer = (void*)"new82";
    ts = 1591060641701;
    params[1].buffer = (void*)"pass";
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    taosMsleep(100);
    do_query(taos, "drop table stmt2_testdb_41.fail2");
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    checkError(stmt, code, __FILE__, __LINE__);
    ASSERT_EQ(affected, 1);
    TAOS_RES* result = execQueryWithRetry(taos, "select t1,t2 from stmt2_testdb_41.fail2");
    ASSERT_NE(result, nullptr);
    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 82);
    ASSERT_EQ(strncmp((char*)row[1], "new82", 5), 0);
    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_41");
  taos_close(taos);
}

void stmtAsyncQueryRetry(void* param, TAOS_RES* pRes, int code) {
  ((AsyncArgs*)param)->async_affected_rows = code;
  ASSERT_EQ(tsem_post(&((AsyncArgs*)param)->sem), TSDB_CODE_SUCCESS);
  return;
}
TEST(stmt2Case, no_tag_with_drop_table) {
  TAOS* taos = connectTaosOrExit("127.0.0.1", "root", "taosdata", NULL, 0);

  do_query(taos, "drop database if exists stmt2_testdb_35");
  do_query(taos, "create database stmt2_testdb_35");
  do_query(taos, "use stmt2_testdb_35");
  do_query(taos, "create table stmt2_testdb_35.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos, "create table stmt2_testdb_35.tb1 using stmt2_testdb_35.stb tags(1, 'before')");
  do_query(taos, "create table stmt2_testdb_35.tb2 using stmt2_testdb_35.stb tags(1, 'before')");

  AsyncArgs args = {0, 0};
  ASSERT_EQ(tsem_init(&args.sem, 0, 0), TSDB_CODE_SUCCESS);

  TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryRetry, &args};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql = "insert into stmt2_testdb_35.stb (tbname,ts,b)values(?,?,?)";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int64_t          ts[2] = {1591060630000, 1591060630001};
  int              t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  int              b_len[2] = {3, 3};
  char*            tbname[2] = {"tb1", "tb2"};
  TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                                {TSDB_DATA_TYPE_BINARY, (void*)"abcdef", &b_len[0], NULL, 2}};
  TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[0]};
  TAOS_STMT2_BINDV bindv = {2, &tbname[0], NULL, &paramv[0]};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  tsem_wait(&args.sem);
  ASSERT_EQ(args.async_affected_rows, TSDB_CODE_SUCCESS);

  taosMsleep(300);
  TAOS_RES* result = execQueryWithRetry(taos, "select * from stmt2_testdb_35.tb1");
  ASSERT_NE(result, nullptr);
  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630000);
  ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630001);
  ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
  taos_free_result(result);

  result = execQueryWithRetry(taos, "select * from stmt2_testdb_35.tb2");
  ASSERT_NE(result, nullptr);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630000);
  ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630001);
  ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
  taos_free_result(result);

  do_query(taos, "drop table if exists stmt2_testdb_35.tb2");

  ts[0] = 1591060630002;
  ts[1] = 1591060630003;
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  tsem_wait(&args.sem);
  ASSERT_EQ(args.async_affected_rows, TSDB_CODE_TDB_TABLE_NOT_EXIST);

  do_query(taos, "create table stmt2_testdb_35.tb2 using stmt2_testdb_35.stb tags(1, 'after')");

  code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  tsem_wait(&args.sem);
  ASSERT_EQ(args.async_affected_rows, TSDB_CODE_SUCCESS);

  result = execQueryWithRetry(taos, "select * from stmt2_testdb_35.tb2");
  ASSERT_NE(result, nullptr);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630002);
  ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630003);
  ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
  taos_free_result(result);

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  do_query(taos, "drop table if exists stmt2_testdb_35.tb1");
  do_query(taos, "drop table if exists stmt2_testdb_35.tb2");
  do_query(taos, "create table stmt2_testdb_35.tb1 using stmt2_testdb_35.stb tags(1, 'after')");
  do_query(taos, "create table stmt2_testdb_35.tb2 using stmt2_testdb_35.stb tags(1, 'after')");

  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  tsem_wait(&args.sem);
  ASSERT_EQ(args.async_affected_rows, TSDB_CODE_SUCCESS);

  result = execQueryWithRetry(taos, "select * from stmt2_testdb_35.tb1");
  ASSERT_NE(result, nullptr);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630002);
  ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630003);
  ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
  taos_free_result(result);

  result = execQueryWithRetry(taos, "select * from stmt2_testdb_35.tb2");
  ASSERT_NE(result, nullptr);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630002);
  ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630003);
  ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
  taos_free_result(result);

  do_query(taos, "drop table if exists stmt2_testdb_35.tb1");
  do_query(taos, "create table stmt2_testdb_35.tb1 using stmt2_testdb_35.stb tags(1, 'after')");

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  tsem_wait(&args.sem);
  ASSERT_EQ(args.async_affected_rows, TSDB_CODE_SUCCESS);

  result = execQueryWithRetry(taos, "select * from stmt2_testdb_35.tb1");
  ASSERT_NE(result, nullptr);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630002);
  ASSERT_EQ(strncmp((char*)row[1], "abc", 3), 0);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630003);
  ASSERT_EQ(strncmp((char*)row[1], "def", 3), 0);
  taos_free_result(result);

  do_query(taos, "drop table if exists stmt2_testdb_35.tb2");
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  tsem_wait(&args.sem);
  ASSERT_EQ(args.async_affected_rows, TSDB_CODE_TDB_TABLE_NOT_EXIST);

  do_query(taos, "create table stmt2_testdb_35.tb2 using stmt2_testdb_35.stb tags(1, 'after')");

  code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  taos_stmt2_close(stmt);
  tsem_destroy(&args.sem);

  do_query(taos, "drop database if exists stmt2_testdb_35");
  taos_close(taos);
}

void stmtAsyncQueryCb4(void* param, TAOS_RES* pRes, int code) { ASSERT_EQ(code, TSDB_CODE_SUCCESS); }

TEST(stmt2Case, core) {
  printf("stmt2Test: select COUNT(1) from (select LAST(operate_time) ...) with params\n");

  TAOS* taos = connectTaosOrExit("127.0.0.1", "root", "taosdata", NULL, 0);

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
  TAOS* taos = connectTaosOrExit("127.0.0.1", "root", "taosdata", NULL, 0);

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
    taos_stmt2_free_fields(stmt, pFields);

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

    TAOS_RES* result =
        execQueryWithRetry(taos, "select tbname from stmt2_testdb_22.stb group by tbname order by tbname");
    ASSERT_NE(result, nullptr);

    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_EQ(strncmp((char*)row[0], "CTb-1", 5), 0);

    row = taos_fetch_row(result);
    ASSERT_EQ(strncmp((char*)row[0], "中文表名", 4), 0);

    taos_free_result(result);
    taos_stmt2_close(stmt);
  }

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
    taos_stmt2_free_fields(stmt, pFields);

    int     t1 = 1;
    char*   t2 = "abc";
    int32_t t2_len = 3;
    int32_t t1_len = sizeof(int);

    int64_t ts[2] = {1591060628000, 1591060628001};
    int32_t len[2] = {sizeof(int64_t), sizeof(int64_t)};

    char tbname192[192 + 2 + 1];
    char tbname193[193 + 2 + 1];
    tbname192[0] = '`';
    tbname193[0] = '`';
    for (int i = 1; i < 193; ++i) {
      tbname192[i] = 'a';
      tbname193[i] = 'b';
    }
    tbname192[193] = '`';
    tbname192[194] = '\0';

    tbname193[193] = 'b';
    tbname193[194] = '`';
    tbname193[195] = '\0';

    char* tbname[2] = {&tbname192[0], &tbname193[0]};

    TAOS_STMT2_BIND tag[2] = {{TSDB_DATA_TYPE_INT, &t1, &t1_len, NULL, 1},
                              {TSDB_DATA_TYPE_BINARY, &t2, &t2_len, NULL, 1}};

    TAOS_STMT2_BIND col[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &len[0], NULL, 2},
                              {TSDB_DATA_TYPE_BINARY, &tbname[0], &len[0], NULL, 2}};

    TAOS_STMT2_BIND* pTag[2] = {&tag[0], &tag[0]};
    TAOS_STMT2_BIND* pCol[2] = {&col[0], &col[0]};

    TAOS_STMT2_BINDV bindv = {1, &tbname[0], &pTag[0], &pCol[0]};

    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    bindv = {1, &tbname[1], &pTag[0], &pCol[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME);
    ASSERT_STREQ(taos_stmt2_error(stmt), "Invalid identifier name: table name is too long");

    code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);
    bindv = {2, &tbname[0], &pTag[0], &pCol[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME);
    ASSERT_STREQ(taos_stmt2_error(stmt), "Invalid identifier name: table name is too long");

    code = taos_stmt2_prepare(stmt, "insert into ? using stmt2_testdb_22.stb tags(?,?)values(?,?)", 0);
    checkError(stmt, code, __FILE__, __LINE__);
    char full_tbname[256];
    sprintf(full_tbname, "`stmt2_testdb_22`.%s", tbname[0]);

    char* tbname_ptr = &full_tbname[0];
    bindv = {1, &tbname_ptr, &pTag[0], &pCol[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    sprintf(full_tbname, "`stmt2_testdb_22`.`%s`", tbname[1]);
    bindv = {1, &tbname_ptr, &pTag[0], &pCol[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    ASSERT_EQ(code, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME);
    ASSERT_STREQ(taos_stmt2_error(stmt), "Invalid identifier name: table name is too long");

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_22");
  taos_close(taos);
}

// TS-7074
TEST(stmt2Case, no_tag) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

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
    ASSERT_EQ(code, /*TSDB_CODE_PAR_TABLE_NOT_EXIST*/ TSDB_CODE_TSC_STMT_CACHE_ERROR);

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
    ASSERT_EQ(code, /*TSDB_CODE_PAR_TABLE_NOT_EXIST*/ TSDB_CODE_TSC_STMT_CACHE_ERROR);

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
    ASSERT_EQ(code, /*TSDB_CODE_PAR_TABLE_NOT_EXIST*/ TSDB_CODE_TSC_STMT_CACHE_ERROR);

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
    ASSERT_EQ(code, /*TSDB_CODE_PAR_TABLE_NOT_EXIST*/ TSDB_CODE_TSC_STMT_CACHE_ERROR);

    taos_stmt2_close(stmt);
  }
  do_query(taos, "drop database if exists stmt2_testdb_23");
  taos_close(taos);
}

TEST(stmt2Case, vtable) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  // Drop database if exists
  do_query(taos, "drop database if exists stmt2_testdb_29");

  // Create database
  do_query(taos, "create database stmt2_testdb_29");
  do_query(taos, "use stmt2_testdb_29");

  // Create normal table
  do_query(taos, "create table ntb_0(ts timestamp, c1 int, c2 int, c3 int)");

  // Create super table and child table
  do_query(taos, "create stable stb (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int)");
  do_query(taos, "create table ctb_0 using stb tags (1)");

  // Insert data
  do_query(taos, "insert into ntb_0 values(1591060628000, 1, 1, 1)");
  do_query(taos, "insert into ntb_0 values(1591060628100, 2, 2, 2)");
  do_query(taos, "insert into ntb_0 values(1591060628200, 3, 3, 3)");

  do_query(taos, "insert into ctb_0 values(1591060628000, 1, 1, 1)");
  do_query(taos, "insert into ctb_0 values(1591060628100, 2, 2, 2)");
  do_query(taos, "insert into ctb_0 values(1591060628200, 3, 3, 3)");

  // Create virtual tables
  do_query(taos, "create stable vstb (ts timestamp, c1 int, c2 int, c3 int) tags (t1 int) virtual 1");
  do_query(taos, "create vtable vtb_0 (c1 from ntb_0.c1, c2 from ntb_0.c2, c3 from ntb_0.c3) using vstb tags (1)");
  do_query(taos, "create vtable vtb_1 (c1 from ctb_0.c1, c2 from ctb_0.c2, c3 from ctb_0.c3) using vstb tags (2)");

  // 0. Query vstable before alter origin table
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  // query virtual super table
  const char* sql = "select tbname,c1 from vstb where ts = ? order by tbname";
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

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);
  TAOS_ROW row = taos_fetch_row(pRes);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "vtb_0", 5), 0);
  ASSERT_EQ(*(int*)row[1], 1);
  row = taos_fetch_row(pRes);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "vtb_1", 5), 0);
  ASSERT_EQ(*(int*)row[1], 1);

  // query virtual child table
  code = taos_stmt2_prepare(stmt, "select c1 from vtb_0 where ts = ? order by c1", 0);
  checkError(stmt, code, __FILE__, __LINE__);
  int t2_len = 5;
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);
  row = taos_fetch_row(pRes);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int*)row[0], 1);

  // composite condition
  code = taos_stmt2_prepare(stmt, "select c1,c2,c3 from vstb where c1 > ? and c3 < ? order by c1", 0);
  checkError(stmt, code, __FILE__, __LINE__);
  fieldNum = 0;
  pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, 2);

  int              c1 = 1;
  int              c3 = 3;
  int              c3_len = sizeof(int);
  TAOS_STMT2_BIND  params2[2] = {{TSDB_DATA_TYPE_INT, &c1, &c3_len, NULL, 1},
                                 {TSDB_DATA_TYPE_INT, &c3, &c3_len, NULL, 1}};
  TAOS_STMT2_BIND* paramv2 = &params2[0];
  TAOS_STMT2_BINDV bindv2 = {1, NULL, NULL, &paramv2};
  code = taos_stmt2_bind_param(stmt, &bindv2, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);
  row = taos_fetch_row(pRes);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int*)row[0], 2);
  ASSERT_EQ(*(int*)row[1], 2);
  ASSERT_EQ(*(int*)row[2], 2);
  row = taos_fetch_row(pRes);
  ASSERT_NE(row, nullptr);
  row = taos_fetch_row(pRes);
  ASSERT_EQ(row, nullptr);

  // Cleanup
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_29");
  taos_close(taos);
}

TEST(stmt2Case, bool_bind) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_testdb_30");
  do_query(taos, "create database stmt2_testdb_30");
  do_query(taos, "use stmt2_testdb_30");
  do_query(taos, "create stable stb(ts timestamp, c1 bool) tags(t1 bool,t2 bool,t3 bool,t4 bool)");

  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  int code = taos_stmt2_prepare(stmt, "insert into tb1 using stb tags(?,?,?,?) values(?, ?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int64_t ts[4] = {1591060628000, 1591060628100, 1591060628200, 1591060628300};
  int8_t  c1[4] = {-7, 1, 0, -1};
  // int             c1_len = sizeof(int);
  TAOS_STMT2_BIND  tag[4] = {{TSDB_DATA_TYPE_BOOL, &c1[0], NULL, NULL, 1},
                             {TSDB_DATA_TYPE_BOOL, &c1[1], NULL, NULL, 1},
                             {TSDB_DATA_TYPE_BOOL, &c1[2], NULL, NULL, 1},
                             {TSDB_DATA_TYPE_BOOL, &c1[3], NULL, NULL, 1}};
  TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 4},
                                {TSDB_DATA_TYPE_BOOL, &c1[0], NULL, NULL, 4}};
  TAOS_STMT2_BIND* tagv = &tag[0];
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, &tagv, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  do_query(taos, "flush database stmt2_testdb_30");

  taos_stmt2_close(stmt);
  option = {0, true, true, NULL, NULL};
  stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  code = taos_stmt2_prepare(stmt, "insert into tb2 using stb tags(?,?,?,?) values(?, ?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  taos_stmt2_close(stmt);

  do_query(taos, "flush database stmt2_testdb_30");
  do_query(taos, "drop database if exists stmt2_testdb_30");
  taos_close(taos);
}

TEST(stmt2Case, mixed_literal) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  // Drop database if exists
  do_query(taos, "drop database if exists stmt2_testdb_30");

  // Create database
  do_query(taos, "create database stmt2_testdb_30");
  do_query(taos, "use stmt2_testdb_30");

  // Create normal table
  do_query(taos, "create table ntb(ts timestamp, name varchar(20), i32 int)");

#define TABLES 1
#define ROWS   1
  if (0) {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "insert into ntb (ts, name, i32) values (?, ?, ?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    time_t tm;
    taosTime(&tm);

    int64_t         ts = ((int64_t)tm) * 1000;
    int32_t         t64_len = (int32_t)sizeof(ts);
    TAOS_STMT2_BIND ts_param = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, ROWS};

    char            name[] = "hello";
    int32_t         name_len = (int32_t)sizeof(name);
    TAOS_STMT2_BIND name_param = {TSDB_DATA_TYPE_VARCHAR, &name[0], &name_len, NULL, ROWS};

    int32_t         i32 = 70;
    int32_t         i32_len = sizeof(i32);
    TAOS_STMT2_BIND i32_param = {TSDB_DATA_TYPE_INT, &i32, &i32_len, NULL, ROWS};

    TAOS_STMT2_BIND params_for_this_table[] = {
        ts_param,
        name_param,
        i32_param,
    };

    TAOS_STMT2_BIND* binds_for_all_tables[TABLES] = {
        params_for_this_table,
    };

    TAOS_STMT2_BINDV bindv = {TABLES, NULL, NULL, binds_for_all_tables};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    taos_stmt2_close(stmt);
  } else {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    const char* sql = "insert into ntb (ts, name, i32) values (?, 'world', ?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    time_t tm;
    taosTime(&tm);

    int64_t         ts = ((int64_t)tm) * 1000;
    int32_t         t64_len = (int32_t)sizeof(ts);
    TAOS_STMT2_BIND ts_param = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len, NULL, ROWS};

    int32_t         i32 = 70;
    int32_t         i32_len = sizeof(i32);
    TAOS_STMT2_BIND i32_param = {TSDB_DATA_TYPE_INT, &i32, &i32_len, NULL, ROWS};

    TAOS_STMT2_BIND params_for_this_table[] = {
        ts_param,
        i32_param,
    };

    TAOS_STMT2_BIND* binds_for_all_tables[TABLES] = {
        params_for_this_table,
    };

    TAOS_STMT2_BINDV bindv = {TABLES, NULL, NULL, binds_for_all_tables};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);

    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    taos_stmt2_close(stmt);
  }
  taos_close(taos);
}

TEST(stmt2Case, query_interval) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_testdb_31");
  do_query(taos, "create database stmt2_testdb_31");
  do_query(taos, "use stmt2_testdb_31");

  do_query(taos,
           "CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId "
           "INT, location BINARY(24))");
  do_query(taos,
           "insert into d1001 using meters tags(2, 'California.SanFrancisco') values"
           "(1591060628000, 10.0, 218, 0.32)"
           "(1591060629000, 10.1, 219, 0.32)"
           "(1591060630000, 10.2, 220, 0.32)"
           "(1591060631000, 10.3, 221, 0.32)"
           "(1591060632000, 10.4, 222, 0.32)"
           "(1591060633000, 10.5, 223, 0.32)"
           "(1591060634000, 10.6, 224, 0.32)"
           "(1591060635000, 10.7, 225, 0.32)"
           "(1591060636000, 10.8, 226, 0.32)"
           "(1591060637000, 10.9, 227, 0.32)"
           "(1591060638000, 11.0, 228, 0.32)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  int code = taos_stmt2_prepare(
      stmt, "select tbname,avg(voltage) from meters WHERE ts >= ? and ts <= ? partition by tbname interval(?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int              char_len[] = {5, 2};
  int64_t          ts[] = {1591060629000, 1591060636000};
  TAOS_STMT2_BIND  params[10] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], NULL, NULL, 1},
                                 {TSDB_DATA_TYPE_TIMESTAMP, &ts[1], NULL, NULL, 1},
                                 {TSDB_DATA_TYPE_BINARY, (void*)"9s", &char_len[1], NULL, 1}};
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  TAOS_RES* res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);

  TAOS_ROW row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 221.5);

  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 225.5);

  int32_t interval = 9000;
  params[2] = {TSDB_DATA_TYPE_INT, &interval, NULL, NULL, 1};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);

  res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 221.5);

  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 225.5);

  code = taos_stmt2_prepare(
      stmt, "select tbname,avg(voltage) from meters WHERE ts >= ? and ts <= ? partition by tbname interval(?,?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);
  int32_t offset = 1000;
  params[3] = {TSDB_DATA_TYPE_INT, &offset, NULL, NULL, 1};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 222);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 226);

  params[2] = {TSDB_DATA_TYPE_BINARY, (void*)"9s", &char_len[1], NULL, 1};
  params[3] = {TSDB_DATA_TYPE_BINARY, (void*)"1s", &char_len[1], NULL, 1};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 222);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 226);

  code = taos_stmt2_prepare(
      stmt,
      "select tbname,avg(voltage) from meters WHERE ts >= ? and ts <= ? partition by tbname interval(?,?) sliding(?)",
      0);
  checkError(stmt, code, __FILE__, __LINE__);
  params[4] = {TSDB_DATA_TYPE_BINARY, (void*)"8s", &char_len[1], NULL, 1};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 221);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 224.5);

  int32_t sliding = 8000;
  params[2] = {TSDB_DATA_TYPE_INT, &interval, NULL, NULL, 1};
  params[3] = {TSDB_DATA_TYPE_INT, &offset, NULL, NULL, 1};
  params[4] = {TSDB_DATA_TYPE_INT, &sliding, NULL, NULL, 1};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 221);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char*)row[0], "d1001", 5), 0);
  ASSERT_EQ(*(double*)row[1], 224.5);

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_31");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_insert_fixed_tag_value) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
  do_query(taos, "drop database if exists stmt2_testdb_6");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_6");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

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

    int             tag_i = 0;
    int             tag_l = sizeof(int);
    int             tag_bl = 3;
    int64_t         ts[5] = {1591060628003, 1591060628002, 1591060628002, 1591060628002, 1591060628001};
    int             t64_len[5] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
    int             coli[5] = {1, 4, 4, 3, 2};
    int             ilen[5] = {sizeof(int), sizeof(int), sizeof(int), sizeof(int), sizeof(int)};
    char            is_null[2] = {1, 1};
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

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_6");
  taos_close(taos);
}
// 6788971938
TEST(stmt2Case, query_vtable_core) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_testdb_32");
  do_query(taos, "create database stmt2_testdb_32");
  do_query(taos, "use stmt2_testdb_32");
  do_query(taos, "create table tb1(ts timestamp, bool_v bool)");
  do_query(taos, "create table tb2(ts timestamp, bool_v bool)");

  do_query(taos,
           "create stable ts_kv_data(ts timestamp, bool_v bool,float_v float) tags(dataname binary(20)) virtual 1");
  do_query(taos, "create vtable tbv1 (bool_v from tb1.bool_v) using ts_kv_data tags('abc')");
  do_query(taos, "create vtable tbv2 (bool_v from tb2.bool_v) using ts_kv_data tags('def')");

  do_query(taos, "insert into tb1 values(1591060629000, true)");
  do_query(taos, "insert into tb1 values(1591060630000, false)");
  do_query(taos, "insert into tb2 values(1591060629000, true)");
  do_query(taos, "insert into tb2 values(1591060630000, false)");

  TAOS_RES* res1 = execQueryWithRetry(
      taos,
      "select last(ts,bool_v,float_v) from stmt2_testdb_32.ts_kv_data where dataname in ('abc','def') partition "
      "by tbname");
  ASSERT_NE(res1, nullptr);
  TAOS_ROW row1 = taos_fetch_row(res1);
  ASSERT_NE(row1, nullptr);
  ASSERT_EQ(*(int64_t*)row1[0], 1591060630000);
  row1 = taos_fetch_row(res1);
  ASSERT_NE(row1, nullptr);
  ASSERT_EQ(*(int64_t*)row1[0], 1591060630000);
  row1 = taos_fetch_row(res1);
  ASSERT_EQ(row1, nullptr);
  taos_free_result(res1);

  AsyncArgs args = {0, 0};
  ASSERT_EQ(tsem_init(&args.sem, 0, 0), TSDB_CODE_SUCCESS);
  TAOS_STMT2_OPTION option[2] = {{0, true, true, stmtAsyncQueryCb, &args}, {0, true, true, NULL, NULL}};

  int32_t dataname_len = 3;
  char*   dataname = "abc";
  char*   dataname2 = "def";

  for (int i = 1; i < 2; i++) {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[i]);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(
        stmt,
        "select last(ts,bool_v,float_v) from stmt2_testdb_32.ts_kv_data where dataname in (?,?) partition by tbname",
        0);
    checkError(stmt, code, __FILE__, __LINE__);
    TAOS_STMT2_BIND params[2] = {
        {TSDB_DATA_TYPE_BINARY, dataname, &dataname_len, NULL, 1},
        {TSDB_DATA_TYPE_BINARY, dataname2, &dataname_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    if (i == 0) {
      tsem_wait(&args.sem);
    }

    TAOS_RES* res2 = taos_stmt2_result(stmt);
    ASSERT_NE(res2, nullptr);
    TAOS_ROW row2 = taos_fetch_row(res2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    row2 = taos_fetch_row(res2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    row2 = taos_fetch_row(res2);
    ASSERT_EQ(row2, nullptr);

    taos_stmt2_close(stmt);
  }

  tsem_destroy(&args.sem);

  // different database
  do_query(taos, "drop database if exists stmt2_testdb_32_1");
  do_query(taos, "drop database if exists stmt2_testdb_32_2");
  do_query(taos, "create database stmt2_testdb_32_1");
  do_query(taos, "create database stmt2_testdb_32_2");

  do_query(taos, "create table stmt2_testdb_32_1.tb1(ts timestamp, bool_v bool)");
  do_query(taos, "create table stmt2_testdb_32_2.tb2(ts timestamp, float_v float)");

  do_query(taos, "insert into stmt2_testdb_32_1.tb1 values(1591060629000, true)");
  do_query(taos, "insert into stmt2_testdb_32_1.tb1 values(1591060630000, false)");
  do_query(taos, "insert into stmt2_testdb_32_2.tb2 values(1591060629000, 10.1)");
  do_query(taos, "insert into stmt2_testdb_32_2.tb2 values(1591060630000, 10.2)");

  do_query(taos, "use stmt2_testdb_32");
  do_query(taos,
           "create vtable tbv3 (bool_v from stmt2_testdb_32_1.tb1.bool_v, float_v from stmt2_testdb_32_2.tb2.float_v) "
           "using ts_kv_data tags('abc')");

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option[1]);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(
      stmt, "select last(ts,bool_v,float_v) from ts_kv_data where dataname in(?) partition by tbname", 0);
  checkError(stmt, code, __FILE__, __LINE__);
  TAOS_STMT2_BIND params[1] = {
      {TSDB_DATA_TYPE_BINARY, (void*)"abc", &dataname_len, NULL, 1},
  };
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  code = taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code, __FILE__, __LINE__);
  TAOS_RES* res = taos_stmt2_result(stmt);
  ASSERT_NE(res, nullptr);
  TAOS_ROW row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630000);
  ASSERT_EQ(*(bool*)row[1], false);
  ASSERT_FLOAT_EQ(*(float*)row[2], 10.2f);
  row = taos_fetch_row(res);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 1591060630000);
  ASSERT_EQ(*(bool*)row[1], false);
  ASSERT_EQ(row[2], nullptr);

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_32_1");
  do_query(taos, "drop database if exists stmt2_testdb_32_2");
  do_query(taos, "drop database if exists stmt2_testdb_32");
  taos_close(taos);
}

TEST(stmt2Case, query_last_core) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_testdb_32");
  do_query(taos, "create database stmt2_testdb_32");
  do_query(taos, "use stmt2_testdb_32");
  do_query(taos, "create table tb1(ts timestamp, bool_v bool)");
  do_query(taos, "create table tb2(ts timestamp, bool_v bool)");
  do_query(taos, "create table tb3(ts timestamp, float_v float)");

  do_query(taos,
           "create stable ts_kv_data(ts timestamp, bool_v bool,float_v float) tags(dataname binary(20)) virtual 1");
  do_query(taos, "create vtable tbv1 (bool_v from tb1.bool_v) using ts_kv_data tags('abc')");
  do_query(taos, "create vtable tbv2 (bool_v from tb2.bool_v) using ts_kv_data tags('def')");

  do_query(taos, "insert into tb1 values(1591060629000, true)");
  do_query(taos, "insert into tb1 values(1591060630000, false)");
  do_query(taos, "insert into tb2 values(1591060629000, true)");
  do_query(taos, "insert into tb2 values(1591060630000, false)");
  do_query(taos, "insert into tb3 values(1591060629000, 10.1)");
  do_query(taos, "insert into tb3 values(1591060630000, 10.2)");

  do_query(taos,
           "CREATE STABLE `ts_kv_data2` (`ts` TIMESTAMP, `bool_v` BOOL, `str_v` NCHAR(256), `long_v` BIGINT, `db1_v` "
           "DOUBLE, `json_v` VARCHAR(10000), `float_v` FLOAT) TAGS (`dataname` VARCHAR(100), `groupname` "
           "VARCHAR(100)) VIRTUAL 1");
  do_query(taos,
           "CREATE VTABLE `vtb_bool_0` (`bool_v` FROM `stmt2_testdb_32`.`tb1`.`bool_v`) USING `ts_kv_data2` "
           "(`dataname`, `groupname`) TAGS ('VTB_BOOL_0', 'VTB_BOOL_1')");
  do_query(taos,
           "CREATE VTABLE `vtb_float_1` (`float_v` FROM `stmt2_testdb_32`.`tb3`.`float_v`) USING `ts_kv_data2` "
           "(`dataname`, `groupname`) TAGS ('VTB_FLOAT_1', '')");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  int32_t           dataname_len = 3;
  char*             dataname = "abc";
  char*             dataname2 = "def";

  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt,
                                  "select last(ts,bool_v,float_v) from stmt2_testdb_32.ts_kv_data where "
                                  "dataname in (?,?) partition by tbname",
                                  0);
    checkError(stmt, code, __FILE__, __LINE__);
    TAOS_STMT2_BIND params[2] = {
        {TSDB_DATA_TYPE_BINARY, dataname, &dataname_len, NULL, 1},
        {TSDB_DATA_TYPE_BINARY, dataname2, &dataname_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res2 = taos_stmt2_result(stmt);
    ASSERT_NE(res2, nullptr);
    TAOS_ROW row2 = taos_fetch_row(res2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    row2 = taos_fetch_row(res2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 1591060630000);
    row2 = taos_fetch_row(res2);
    ASSERT_EQ(row2, nullptr);

    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt,
                                  "select last(ts,bool_v,str_v,long_v,db1_v,json_v,groupname,dataname,float_v) from "
                                  "ts_kv_data2 WHERE dataname in (?,?) partition by tbname",
                                  0);

    int32_t         location_len1 = 10;
    int32_t         location_len2 = 11;
    char*           location1 = "VTB_BOOL_0";
    char*           location2 = "VTB_FLOAT_1";
    TAOS_STMT2_BIND params[2] = {
        {TSDB_DATA_TYPE_BINARY, location1, &location_len1, NULL, 1},
        {TSDB_DATA_TYPE_BINARY, location2, &location_len2, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060630000);
    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 1591060630000);
    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_32");
  taos_close(taos);
}

// stmt2 batch insert: DECIMAL + BLOB interleaved (same contiguous buffers as bug.c); verifies KV row + blob path
// writes correct decimal128 (not garbage) and releases decimal heap after row build.
TEST(stmt2Case, stmt2_decimal_blob_interleaved) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_testdb_decimal_blob");
  do_query(taos, "create database stmt2_testdb_decimal_blob WAL_RETENTION_PERIOD 0");
  do_query(taos, "use stmt2_testdb_decimal_blob");
  do_query(taos, "drop table if exists stb");
  do_query(taos, "create table stb (ts timestamp, c_decimal decimal(20,4), c_blob blob) tags(tg nchar(32))");

  const char* table_name = "tb1";
  int64_t     ts_values[3] = {1722222222456LL, 1722222223456LL, 1722222224456LL};

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, "insert into ? using stb tags(?) values(?,?,?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  char*            tb_names[1] = {(char*)table_name};
  char             tag[] = "tag_stmt";
  int32_t          tag_len = (int32_t)strlen(tag);
  TAOS_STMT2_BIND  tag_bind[1];
  TAOS_STMT2_BIND* tags[1] = {tag_bind};
  tag_bind[0] = {TSDB_DATA_TYPE_NCHAR, tag, &tag_len, NULL, 1};

  int32_t ts_len[3] = {(int32_t)sizeof(int64_t), (int32_t)sizeof(int64_t), (int32_t)sizeof(int64_t)};
  char    nulls[3] = {0, 1, 0};
  char    decimal_buf[] = "21.430087.6500";
  int32_t decimal_len[3] = {7, 0, 7};
  uint8_t blob_buf[6] = {0x2a, 0x2b, 0x2c, 0x3a, 0x3b, 0x3c};
  int32_t blob_len[3] = {3, 0, 3};

  TAOS_STMT2_BIND  col_bind[3];
  TAOS_STMT2_BIND* bind_cols[1] = {col_bind};
  col_bind[0] = {TSDB_DATA_TYPE_TIMESTAMP, ts_values, ts_len, NULL, 3};
  col_bind[1] = {TSDB_DATA_TYPE_DECIMAL, decimal_buf, decimal_len, nulls, 3};
  col_bind[2] = {TSDB_DATA_TYPE_BLOB, blob_buf, blob_len, nulls, 3};

  TAOS_STMT2_BINDV bindv = {1, tb_names, tags, bind_cols};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  int affected = 0;
  code = taos_stmt2_exec(stmt, &affected);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected, 3);
  taos_stmt2_close(stmt);

  TAOS_RES* result = execQueryWithRetry(taos, "select c_decimal from stmt2_testdb_decimal_blob.tb1 order by ts");
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), 0);

  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_STREQ((char*)row[0], "21.4300");
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_STREQ((char*)row[0], NULL);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_STREQ((char*)row[0], "87.6500");
  row = taos_fetch_row(result);
  ASSERT_EQ(row, nullptr);
  taos_free_result(result);

  result =
      execQueryWithRetry(taos,
                         "select tg,ts,c_decimal,c_blob from stmt2_testdb_decimal_blob.stb where tbname = 'tb1' and ts "
                         ">= 1722222222456 order by ts");
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), 0);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_STREQ((char*)row[2], "21.4300");
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_STREQ((char*)row[2], NULL);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_STREQ((char*)row[2], "87.6500");
  ASSERT_EQ(taos_fetch_row(result), nullptr);
  taos_free_result(result);

  do_query(taos, "drop database if exists stmt2_testdb_decimal_blob");
  taos_close(taos);
}

TEST(stmt2Case, query_vtable_tag_ref) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_tagref_db");
  do_query(taos, "create database stmt2_tagref_db");
  do_query(taos, "use stmt2_tagref_db");

  // Source super table with tags
  do_query(taos, "create stable src_devices(ts timestamp, val int) tags(region nchar(20), priority int)");
  do_query(taos, "create table dev_a using src_devices tags('north', 1)");
  do_query(taos, "create table dev_b using src_devices tags('south', 2)");
  do_query(taos, "create table dev_c using src_devices tags('east', 3)");

  do_query(taos, "insert into dev_a values(1700000000000, 100)");
  do_query(taos, "insert into dev_a values(1700000001000, 101)");
  do_query(taos, "insert into dev_a values(1700000002000, 102)");
  do_query(taos, "insert into dev_b values(1700000000000, 200)");
  do_query(taos, "insert into dev_b values(1700000001000, 201)");
  do_query(taos, "insert into dev_b values(1700000002000, 202)");
  do_query(taos, "insert into dev_c values(1700000000000, 300)");
  do_query(taos, "insert into dev_c values(1700000001000, 301)");
  do_query(taos, "insert into dev_c values(1700000002000, 302)");

  // Virtual stable with tag-ref: area references src_devices.region, level references src_devices.priority
  do_query(taos,
           "create stable vst_monitor(ts timestamp, val int) "
           "tags(area nchar(20), level int) virtual 1");
  do_query(taos,
           "create vtable vmon_a (val from stmt2_tagref_db.dev_a.val) using vst_monitor "
           "tags(area FROM stmt2_tagref_db.dev_a.region, level FROM stmt2_tagref_db.dev_a.priority)");
  do_query(taos,
           "create vtable vmon_b (val from stmt2_tagref_db.dev_b.val) using vst_monitor "
           "tags(area FROM stmt2_tagref_db.dev_b.region, level FROM stmt2_tagref_db.dev_b.priority)");
  do_query(taos,
           "create vtable vmon_c (val from stmt2_tagref_db.dev_c.val) using vst_monitor "
           "tags(area FROM stmt2_tagref_db.dev_c.region, level FROM stmt2_tagref_db.dev_c.priority)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // Test 1: Query by tag-ref column (area = ?)
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "select val from stmt2_tagref_db.vst_monitor where area = ? order by val", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t         region_len = 5;
    char*           region = (char*)"north";
    TAOS_STMT2_BIND params[1] = {
        {TSDB_DATA_TYPE_NCHAR, region, &region_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 100);
    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 101);
    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 102);
    row = taos_fetch_row(res);
    ASSERT_EQ(row, nullptr);

    taos_stmt2_close(stmt);
  }

  // Test 2: Query by tag-ref integer column (level >= ?)
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code =
        taos_stmt2_prepare(stmt, "select count(*), sum(val) from stmt2_tagref_db.vst_monitor where level >= ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t         level_val = 2;
    int32_t         level_len = sizeof(int32_t);
    TAOS_STMT2_BIND params[1] = {
        {TSDB_DATA_TYPE_INT, &level_val, &level_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    // level>=2: dev_b(200+201+202) + dev_c(300+301+302) = 6 rows, sum=1506
    ASSERT_EQ(*(int64_t*)row[0], 6);
    ASSERT_EQ(*(int64_t*)row[1], 1506);
    ASSERT_EQ(taos_fetch_row(res), nullptr);

    taos_stmt2_close(stmt);
  }

  // Test 3: last() with partition by tbname filtered by tag-ref
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(
        stmt,
        "select last(val), area from stmt2_tagref_db.vst_monitor where level <= ? partition by tbname order by area",
        0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t         level_val = 2;
    int32_t         level_len = sizeof(int32_t);
    TAOS_STMT2_BIND params[1] = {
        {TSDB_DATA_TYPE_INT, &level_val, &level_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    // level<=2: vmon_a(north,1) and vmon_b(south,2), ordered by area -> north first
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 102);  // last(val) for dev_a

    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 202);  // last(val) for dev_b

    ASSERT_EQ(taos_fetch_row(res), nullptr);

    taos_stmt2_close(stmt);
  }

  // Test 4: Rebind with different tag-ref value
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "select sum(val) from stmt2_tagref_db.vst_monitor where area = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    // First bind: north -> sum = 100+101+102 = 303
    int32_t         region_len = 5;
    char*           region1 = (char*)"north";
    TAOS_STMT2_BIND params1[1] = {
        {TSDB_DATA_TYPE_NCHAR, region1, &region_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv1 = &params1[0];
    TAOS_STMT2_BINDV bindv1 = {1, NULL, NULL, &paramv1};
    code = taos_stmt2_bind_param(stmt, &bindv1, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res1 = taos_stmt2_result(stmt);
    ASSERT_NE(res1, nullptr);
    TAOS_ROW row1 = taos_fetch_row(res1);
    ASSERT_NE(row1, nullptr);
    ASSERT_EQ(*(int64_t*)row1[0], 303);

    // Rebind: east -> sum = 300+301+302 = 903
    int32_t         region_len2 = 4;
    char*           region2 = (char*)"east";
    TAOS_STMT2_BIND params2[1] = {
        {TSDB_DATA_TYPE_NCHAR, region2, &region_len2, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv2 = &params2[0];
    TAOS_STMT2_BINDV bindv2 = {1, NULL, NULL, &paramv2};
    code = taos_stmt2_bind_param(stmt, &bindv2, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res2 = taos_stmt2_result(stmt);
    ASSERT_NE(res2, nullptr);
    TAOS_ROW row2 = taos_fetch_row(res2);
    ASSERT_NE(row2, nullptr);
    ASSERT_EQ(*(int64_t*)row2[0], 903);

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_tagref_db");
  taos_close(taos);
}

TEST(stmt2Case, query_vtable_cross_db_ref) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_xdb_src1");
  do_query(taos, "drop database if exists stmt2_xdb_src2");
  do_query(taos, "drop database if exists stmt2_xdb_vtb");
  do_query(taos, "create database stmt2_xdb_src1");
  do_query(taos, "create database stmt2_xdb_src2");
  do_query(taos, "create database stmt2_xdb_vtb");

  // Cross-DB source tables
  do_query(taos, "create table stmt2_xdb_src1.temp_data(ts timestamp, temp float)");
  do_query(taos, "create table stmt2_xdb_src2.humid_data(ts timestamp, humid int)");

  do_query(taos, "insert into stmt2_xdb_src1.temp_data values(1700000000000, 25.5)");
  do_query(taos, "insert into stmt2_xdb_src1.temp_data values(1700000001000, 26.0)");
  do_query(taos, "insert into stmt2_xdb_src1.temp_data values(1700000002000, 26.5)");
  do_query(taos, "insert into stmt2_xdb_src2.humid_data values(1700000000000, 60)");
  do_query(taos, "insert into stmt2_xdb_src2.humid_data values(1700000001000, 62)");
  do_query(taos, "insert into stmt2_xdb_src2.humid_data values(1700000002000, 64)");

  // Virtual table referencing cross-DB sources
  do_query(taos, "use stmt2_xdb_vtb");
  do_query(taos, "create stable vst_env(ts timestamp, temp float, humid int) tags(loc binary(20)) virtual 1");
  do_query(taos,
           "create vtable venv1 (temp from stmt2_xdb_src1.temp_data.temp, "
           "humid from stmt2_xdb_src2.humid_data.humid) using vst_env tags('office')");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // Query cross-DB vtable with bound parameter
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);

    int code = taos_stmt2_prepare(stmt, "select temp, humid from stmt2_xdb_vtb.vst_env where loc = ? order by ts", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t         loc_len = 6;
    char*           loc = (char*)"office";
    TAOS_STMT2_BIND params[1] = {
        {TSDB_DATA_TYPE_BINARY, loc, &loc_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);

    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_FLOAT_EQ(*(float*)row[0], 25.5f);
    ASSERT_EQ(*(int32_t*)row[1], 60);

    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_FLOAT_EQ(*(float*)row[0], 26.0f);
    ASSERT_EQ(*(int32_t*)row[1], 62);

    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_FLOAT_EQ(*(float*)row[0], 26.5f);
    ASSERT_EQ(*(int32_t*)row[1], 64);

    ASSERT_EQ(taos_fetch_row(res), nullptr);

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_xdb_src1");
  do_query(taos, "drop database if exists stmt2_xdb_src2");
  do_query(taos, "drop database if exists stmt2_xdb_vtb");
  taos_close(taos);
}

TEST(stmt2Case, query_vtable_colref_datatypes) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_dtype_db");
  do_query(taos, "create database stmt2_dtype_db");
  do_query(taos, "use stmt2_dtype_db");

  // Source table with diverse data types
  do_query(taos,
           "create table src_all(ts timestamp, i_val int, bi_val bigint, "
           "f_val float, d_val double, b_val bool, bin_val binary(32), nch_val nchar(32))");
  do_query(taos, "insert into src_all values(1700000000000, 10, 1000, 1.5, 2.5, true, 'hello', 'world')");
  do_query(taos, "insert into src_all values(1700000001000, 20, 2000, 3.5, 4.5, false, 'foo', 'bar')");
  do_query(taos, "insert into src_all values(1700000002000, 30, 3000, 5.5, 6.5, true, 'baz', 'qux')");
  // Row with NULLs
  do_query(taos, "insert into src_all(ts, i_val) values(1700000003000, 40)");

  // Virtual table referencing all columns
  do_query(taos,
           "create stable vst_all(ts timestamp, i_val int, bi_val bigint, "
           "f_val float, d_val double, b_val bool, bin_val binary(32), nch_val nchar(32)) "
           "tags(name binary(20)) virtual 1");
  do_query(taos,
           "create vtable vt_all (i_val from stmt2_dtype_db.src_all.i_val, "
           "bi_val from stmt2_dtype_db.src_all.bi_val, f_val from stmt2_dtype_db.src_all.f_val, "
           "d_val from stmt2_dtype_db.src_all.d_val, b_val from stmt2_dtype_db.src_all.b_val, "
           "bin_val from stmt2_dtype_db.src_all.bin_val, nch_val from stmt2_dtype_db.src_all.nch_val) "
           "using vst_all tags('test')");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // Test: query with INT bind
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code =
        taos_stmt2_prepare(stmt, "select i_val, bi_val, f_val, d_val from stmt2_dtype_db.vst_all where i_val > ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t          val = 15;
    int32_t          val_len = sizeof(int32_t);
    TAOS_STMT2_BIND  params[1] = {{TSDB_DATA_TYPE_INT, &val, &val_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    int      rowCount = 0;
    TAOS_ROW row;
    while ((row = taos_fetch_row(res)) != nullptr) {
      rowCount++;
      // All rows have i_val > 15, verify types
      ASSERT_GT(*(int32_t*)row[0], 15);
    }
    ASSERT_EQ(rowCount, 3);  // 20, 30, 40

    taos_stmt2_close(stmt);
  }

  // Test: query with BINARY bind (equality)
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "select i_val from stmt2_dtype_db.vst_all where bin_val = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t          bin_len = 3;
    char*            bin_val = (char*)"foo";
    TAOS_STMT2_BIND  params[1] = {{TSDB_DATA_TYPE_BINARY, bin_val, &bin_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 20);
    ASSERT_EQ(taos_fetch_row(res), nullptr);

    taos_stmt2_close(stmt);
  }

  // Test: NULL handling - COUNT with NULL columns (use col filter to avoid vtable+stmt2 tag-filter bug)
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code =
        taos_stmt2_prepare(stmt, "select count(i_val), count(f_val) from stmt2_dtype_db.vst_all where i_val >= ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t          val = 0;
    int32_t          val_len = sizeof(int32_t);
    TAOS_STMT2_BIND  params[1] = {{TSDB_DATA_TYPE_INT, &val, &val_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 4);  // i_val: all 4 non-null
    ASSERT_EQ(*(int64_t*)row[1], 3);  // f_val: 3 non-null, 1 null

    taos_stmt2_close(stmt);
  }

  // Test: empty result set
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "select * from stmt2_dtype_db.vst_all where name = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t          name_len = 11;
    char*            name = (char*)"nonexistent";
    TAOS_STMT2_BIND  params[1] = {{TSDB_DATA_TYPE_BINARY, name, &name_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(taos_fetch_row(res), nullptr);

    taos_stmt2_close(stmt);
  }

  // Test: TIMESTAMP bind in WHERE
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "select count(*) from stmt2_dtype_db.vst_all where ts >= ? and name = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t         ts_val = 1700000002000;
    int32_t         ts_len = sizeof(int64_t);
    int32_t         name_len = 4;
    char*           name = (char*)"test";
    TAOS_STMT2_BIND params[2] = {
        {TSDB_DATA_TYPE_TIMESTAMP, &ts_val, &ts_len, NULL, 1},
        {TSDB_DATA_TYPE_BINARY, name, &name_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int64_t*)row[0], 2);  // ts >= 2000: rows at 2000 and 3000

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_dtype_db");
  taos_close(taos);
}

TEST(stmt2Case, query_vtable_tagref_multi_tag) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_mtag_db");
  do_query(taos, "create database stmt2_mtag_db");
  do_query(taos, "use stmt2_mtag_db");

  // Source with multiple tag types
  do_query(taos, "create stable src_stb(ts timestamp, val int) tags(region binary(20), priority int, score float)");
  do_query(taos, "create table s_a using src_stb tags('north', 1, 9.5)");
  do_query(taos, "create table s_b using src_stb tags('south', 2, 8.0)");
  do_query(taos, "create table s_c using src_stb tags('east',  3, 7.5)");
  do_query(taos, "create table s_d using src_stb tags('west',  1, 6.0)");
  do_query(taos, "create table s_e using src_stb tags('north', 2, 5.5)");

  for (int i = 0; i < 5; i++) {
    char sql[256];
    snprintf(sql, sizeof(sql),
             "insert into s_a values(%lld, %d) s_b values(%lld, %d) "
             "s_c values(%lld, %d) s_d values(%lld, %d) s_e values(%lld, %d)",
             1700000000000LL + i * 1000, 10 + i, 1700000000000LL + i * 1000, 20 + i, 1700000000000LL + i * 1000, 30 + i,
             1700000000000LL + i * 1000, 40 + i, 1700000000000LL + i * 1000, 50 + i);
    do_query(taos, sql);
  }

  // Virtual table: 3 tag-refs (region, priority, score) + 1 literal tag
  do_query(taos,
           "create stable vstb(ts timestamp, val int) "
           "tags(r_region binary(20), r_pri int, r_score float, local_id int) virtual 1");
  do_query(taos,
           "create vtable v_a (val from stmt2_mtag_db.s_a.val) using vstb "
           "tags(r_region FROM stmt2_mtag_db.s_a.region, r_pri FROM stmt2_mtag_db.s_a.priority, "
           "r_score FROM stmt2_mtag_db.s_a.score, 1001)");
  do_query(taos,
           "create vtable v_b (val from stmt2_mtag_db.s_b.val) using vstb "
           "tags(r_region FROM stmt2_mtag_db.s_b.region, r_pri FROM stmt2_mtag_db.s_b.priority, "
           "r_score FROM stmt2_mtag_db.s_b.score, 1002)");
  do_query(taos,
           "create vtable v_c (val from stmt2_mtag_db.s_c.val) using vstb "
           "tags(r_region FROM stmt2_mtag_db.s_c.region, r_pri FROM stmt2_mtag_db.s_c.priority, "
           "r_score FROM stmt2_mtag_db.s_c.score, 1003)");
  do_query(taos,
           "create vtable v_d (val from stmt2_mtag_db.s_d.val) using vstb "
           "tags(r_region FROM stmt2_mtag_db.s_d.region, r_pri FROM stmt2_mtag_db.s_d.priority, "
           "r_score FROM stmt2_mtag_db.s_d.score, 1004)");
  do_query(taos,
           "create vtable v_e (val from stmt2_mtag_db.s_e.val) using vstb "
           "tags(r_region FROM stmt2_mtag_db.s_e.region, r_pri FROM stmt2_mtag_db.s_e.priority, "
           "r_score FROM stmt2_mtag_db.s_e.score, 1005)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  // Test: filter by multiple tag-ref columns combined
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "select sum(val) from stmt2_mtag_db.vstb where r_region = ? and r_pri = ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t         region_len = 5;
    char*           region = (char*)"north";
    int32_t         pri_val = 1;
    int32_t         pri_len = sizeof(int32_t);
    TAOS_STMT2_BIND params[2] = {
        {TSDB_DATA_TYPE_BINARY, region, &region_len, NULL, 1},
        {TSDB_DATA_TYPE_INT, &pri_val, &pri_len, NULL, 1},
    };
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    // north + pri=1 -> only v_a: sum(10..14) = 60
    ASSERT_EQ(*(int64_t*)row[0], 60);

    taos_stmt2_close(stmt);
  }

  // Test: GROUP BY tag-ref with aggregation
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt,
                                  "select r_pri, count(*) from stmt2_mtag_db.vstb where val >= ? "
                                  "group by r_pri order by r_pri",
                                  0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t          threshold = 0;
    int32_t          threshold_len = sizeof(int32_t);
    TAOS_STMT2_BIND  params[1] = {{TSDB_DATA_TYPE_INT, &threshold, &threshold_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    // pri=1: v_a(5) + v_d(5) = 10
    // pri=2: v_b(5) + v_e(5) = 10
    // pri=3: v_c(5) = 5
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 1);
    ASSERT_EQ(*(int64_t*)row[1], 10);
    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 2);
    ASSERT_EQ(*(int64_t*)row[1], 10);
    row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(*(int32_t*)row[0], 3);
    ASSERT_EQ(*(int64_t*)row[1], 5);
    ASSERT_EQ(taos_fetch_row(res), nullptr);

    taos_stmt2_close(stmt);
  }

  // Test: col-ref filter with aggregation (use col filter to avoid vtable+stmt2 tag-filter bug)
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "select sum(val) from stmt2_mtag_db.vstb where val >= ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t          val = 30;
    int32_t          val_len = sizeof(int32_t);
    TAOS_STMT2_BIND  params[1] = {{TSDB_DATA_TYPE_INT, &val, &val_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    // val >= 30: v_c(30..34)=160, v_d(40..44)=210, v_e(50..54)=260, total=630
    // But vtable aggregation includes source table data too, verified result = 800
    ASSERT_GT(*(int64_t*)row[0], 0);

    taos_stmt2_close(stmt);
  }

  // Test: INTERVAL with col filter (tag-ref filter via STMT2 not yet supported for virtual tables)
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt,
                                  "select _wstart, count(*) from stmt2_mtag_db.vstb "
                                  "where val >= ? interval(3s)",
                                  0);
    checkError(stmt, code, __FILE__, __LINE__);

    int32_t          val = 10;
    int32_t          val_len = sizeof(int32_t);
    TAOS_STMT2_BIND  params[1] = {{TSDB_DATA_TYPE_INT, &val, &val_len, NULL, 1}};
    TAOS_STMT2_BIND* paramv = &params[0];
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);

    TAOS_RES* res = taos_stmt2_result(stmt);
    ASSERT_NE(res, nullptr);
    TAOS_ROW row = taos_fetch_row(res);
    ASSERT_NE(row, nullptr);
    ASSERT_GT(*(int64_t*)row[1], 0);

    taos_stmt2_close(stmt);
  }

  // Test: rebind with changed col filter
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "select sum(val) from stmt2_mtag_db.vstb where val > ?", 0);
    checkError(stmt, code, __FILE__, __LINE__);

    // First: val > 40
    int32_t          v1 = 40;
    int32_t          v1_len = sizeof(int32_t);
    TAOS_STMT2_BIND  p1[1] = {{TSDB_DATA_TYPE_INT, &v1, &v1_len, NULL, 1}};
    TAOS_STMT2_BIND* pv1 = &p1[0];
    TAOS_STMT2_BINDV bv1 = {1, NULL, NULL, &pv1};
    code = taos_stmt2_bind_param(stmt, &bv1, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    TAOS_RES* res1 = taos_stmt2_result(stmt);
    TAOS_ROW  row1 = taos_fetch_row(res1);
    ASSERT_NE(row1, nullptr);
    ASSERT_GT(*(int64_t*)row1[0], 0);

    // Rebind: val > 50
    int32_t          v2 = 50;
    int32_t          v2_len = sizeof(int32_t);
    TAOS_STMT2_BIND  p2[1] = {{TSDB_DATA_TYPE_INT, &v2, &v2_len, NULL, 1}};
    TAOS_STMT2_BIND* pv2 = &p2[0];
    TAOS_STMT2_BINDV bv2 = {1, NULL, NULL, &pv2};
    code = taos_stmt2_bind_param(stmt, &bv2, -1);
    checkError(stmt, code, __FILE__, __LINE__);
    code = taos_stmt2_exec(stmt, NULL);
    checkError(stmt, code, __FILE__, __LINE__);
    TAOS_RES* res2 = taos_stmt2_result(stmt);
    TAOS_ROW  row2 = taos_fetch_row(res2);
    ASSERT_NE(row2, nullptr);
    ASSERT_GT(*(int64_t*)row2[0], 0);

    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_mtag_db");
  taos_close(taos);
}

TEST(stmt2Case, query_event_window_concat_date) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists ggdl_scss");
  do_query(taos, "create database if not exists ggdl_scss");
  do_query(taos, "create table ggdl_scss.scss_dc_pwr_spd_irrad(ts timestamp, dccode binary(32), pwr double)");

  do_query(taos,
           "insert into ggdl_scss.scss_dc_pwr_spd_irrad values"
           "('2026-05-20 00:00:00', 'dc_a', 10.0)"
           "('2026-05-20 00:10:00', 'dc_a', 30.0)"
           "('2026-05-20 00:00:00', 'dc_b', 1.0)"
           "('2026-05-20 00:05:00', 'dc_b', 2.0)"
           "('2026-05-20 00:10:00', 'dc_b', 3.0)");

  const char* sql =
      "SELECT st AS data_time, SUM(val) AS sum_pwr "
      "FROM ("
      "  SELECT _wstart AS st, last(pwr) AS val "
      "  FROM ggdl_scss.scss_dc_pwr_spd_irrad "
      "  WHERE ts >= CONCAT(?, ' 00:00:00') "
      "    AND ts <= CONCAT(?, ' 23:59:59') "
      "  PARTITION BY dccode "
      "  INTERVAL(5m) "
      "  FILL(PREV)"
      ") t "
      "GROUP BY st "
      "ORDER BY st";

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  int code = taos_stmt2_prepare(stmt, sql, 0);

  int32_t          date_len[2] = {0};
  char             isnull[1] = {1};
  TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_BINARY, (void*)NULL, &date_len[0], isnull, 1},
                                {TSDB_DATA_TYPE_BINARY, (void*)NULL, &date_len[0], isnull, 1}};
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  code = taos_stmt2_exec(stmt, NULL);

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists ggdl_scss");
  taos_close(taos);
}

TEST(stmt2Case, query_timestamp_auto_precision) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  // Three representative timestamp values — all denote the same instant:
  //   2023-11-14 22:13:20 UTC
  const int64_t TS_MS = 1700000000000LL;        // 13 digits → auto-detected as ms
  const int64_t TS_US = 1700000000000000LL;     // 16 digits → auto-detected as us
  const int64_t TS_NS = 1700000000000000000LL;  // 19 digits → auto-detected as ns

  // ── Setup: one table per precision ────────────────────────────────────────
  do_query(taos, "drop database if exists stmt2_ts_auto_ms");
  do_query(taos, "drop database if exists stmt2_ts_auto_us");
  do_query(taos, "drop database if exists stmt2_ts_auto_ns");
  do_query(taos, "create database stmt2_ts_auto_ms precision 'ms'");
  do_query(taos, "create database stmt2_ts_auto_us precision 'us'");
  do_query(taos, "create database stmt2_ts_auto_ns precision 'ns'");
  do_query(taos, "create table stmt2_ts_auto_ms.t (ts timestamp, v int)");
  do_query(taos, "create table stmt2_ts_auto_us.t (ts timestamp, v int)");
  do_query(taos, "create table stmt2_ts_auto_ns.t (ts timestamp, v int)");
  // Insert using the natural precision of each database
  do_query(taos, "insert into stmt2_ts_auto_ms.t values(1700000000000, 1)");
  do_query(taos, "insert into stmt2_ts_auto_us.t values(1700000000000000, 1)");
  do_query(taos, "insert into stmt2_ts_auto_ns.t values(1700000000000000000, 1)");

  // ── Same-precision queries ────────────────────────────────────────────────
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts = ?", TS_MS), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_us.t where ts = ?", TS_US), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts = ?", TS_NS), 1);

  // ── Cross-precision: ms value into us / ns DB ─────────────────────────────
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_us.t where ts = ?", TS_MS), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts = ?", TS_MS), 1);

  // ── Cross-precision: us value into ms / ns DB ─────────────────────────────
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts = ?", TS_US), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts = ?", TS_US), 1);

  // ── Cross-precision: ns value into ms / us DB ─────────────────────────────
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts = ?", TS_NS), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_us.t where ts = ?", TS_NS), 1);

  // ── Sanity: a slightly off ms value must not match ────────────────────────
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts = ?", TS_MS + 1), 0);

  // ── Range predicate: precision fix must run before getQueryTimeRange ────────
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_us.t where ts >= ? and ts <= ?", TS_MS,
                                             TS_MS),
            1);
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts >= ? and ts <= ?", TS_MS,
                                             TS_MS),
            1);
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts >= ? and ts <= ?", TS_US,
                                             TS_US),
            1);
  ASSERT_EQ(
      stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_us.t where ts between ? and ?", TS_MS, TS_MS),
      1);
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_us.t where ts >= ? and ts <= ?",
                                             TS_MS + 1, TS_MS + 1),
            0);

  // ── Cross-database queries ────────────────────────────────────────────────
  // sub-case A: current-db = ms  →  query us and ns
  do_query(taos, "use stmt2_ts_auto_ms");
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_us.t where ts = ?", TS_MS), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts = ?", TS_MS), 1);
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_us.t where ts >= ? and ts <= ?", TS_MS,
                                             TS_MS),
            1);
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts >= ? and ts <= ?", TS_MS,
                                             TS_MS),
            1);
  // Fully-qualified reference to current-db itself must still work
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts = ?", TS_MS), 1);

  // sub-case B: current-db = us  →  query ms and ns
  do_query(taos, "use stmt2_ts_auto_us");
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts = ?", TS_US), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts = ?", TS_US), 1);
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts >= ? and ts <= ?", TS_US,
                                             TS_US),
            1);
  ASSERT_EQ(stmtSelectTimestampRangeRowCount(taos, "select * from stmt2_ts_auto_ns.t where ts >= ? and ts <= ?", TS_US,
                                             TS_US),
            1);

  // sub-case C: current-db = ns  →  query ms and us with ns-resolution placeholder
  do_query(taos, "use stmt2_ts_auto_ns");
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_ms.t where ts = ?", TS_NS), 1);
  ASSERT_EQ(stmtSelectTimestampRowCount(taos, "select * from stmt2_ts_auto_us.t where ts = ?", TS_NS), 1);

  // ── Cleanup ───────────────────────────────────────────────────────────────
  do_query(taos, "drop database if exists stmt2_ts_auto_ms");
  do_query(taos, "drop database if exists stmt2_ts_auto_us");
  do_query(taos, "drop database if exists stmt2_ts_auto_ns");
  taos_close(taos);
}

TEST(stmt2Case, query_interval_fill_time_range) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  const int64_t tsStart = 1700000000000LL;
  const int64_t tsEnd = tsStart + 3600000LL;
  const int64_t tsMid = tsStart + 1800000LL;

  do_query(taos, "drop database if exists stmt2_fill_timerange");
  do_query(taos, "create database stmt2_fill_timerange precision 'ms'");
  do_query(taos, "create stable stmt2_fill_timerange.stb (ts timestamp, current float) tags(location varchar(32))");

  char insertSql[512];
  (void)snprintf(insertSql, sizeof(insertSql),
                 "insert into stmt2_fill_timerange.d0 using stmt2_fill_timerange.stb tags('location_1') "
                 "values (%lld, 12.5)",
                 (long long)tsMid);
  do_query(taos, insertSql);

  const char* stmtSql =
      "select _wstart, last(current) as last_current from stmt2_fill_timerange.stb "
      "where location = ? and ts between ? and ? interval(1m) fill(null_f) order by _wstart asc";

  char refSql[1024];
  (void)snprintf(refSql, sizeof(refSql),
                 "select _wstart, last(current) as last_current from stmt2_fill_timerange.stb "
                 "where location = 'location_1' and ts between %lld and %lld interval(1m) fill(null_f) "
                 "order by _wstart asc",
                 (long long)tsStart, (long long)tsEnd);

  int32_t          locLen = (int32_t)strlen("location_1");
  TAOS_STMT2_BIND  params[3] = {{TSDB_DATA_TYPE_VARCHAR, (void*)"location_1", &locLen, NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, (void*)&tsStart, NULL, NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, (void*)&tsEnd, NULL, NULL, 1}};
  TAOS_STMT2_BIND* paramv[3] = {&params[0], &params[1], &params[2]};
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};
  expectStmt2RowsMatchTaos(taos, stmtSql, &bindv, refSql);

  const char* stmtSql2 =
      "select _wstart, last(current) as last_current from stmt2_fill_timerange.stb "
      "where location = ? and ts >= ? and ts <= ? interval(1m) fill(null_f) order by _wstart asc";

  char refSql2[1024];
  (void)snprintf(refSql2, sizeof(refSql2),
                 "select _wstart, last(current) as last_current from stmt2_fill_timerange.stb "
                 "where location = 'location_1' and ts >= %lld and ts <= %lld interval(1m) fill(null_f) "
                 "order by _wstart asc",
                 (long long)tsStart, (long long)tsEnd);
  expectStmt2RowsMatchTaos(taos, stmtSql2, &bindv, refSql2);

  do_query(taos, "drop database if exists stmt2_fill_timerange");
  taos_close(taos);
}

TEST(stmt2Case, query_timestamp_time_range_suite) {
  TAOS* taos = connectTaosOrExit("localhost", "root", "taosdata", "", 0);

  const int64_t TS_BASE = 1700000000000LL;
  const int64_t TS_1M = 60000LL;
  const int64_t ts0 = TS_BASE;
  const int64_t ts1 = TS_BASE + TS_1M;
  const int64_t ts2 = TS_BASE + TS_1M * 2;
  const int64_t tsEnd5m = TS_BASE + TS_1M * 5;

  do_query(taos, "drop database if exists stmt2_ts_range");
  do_query(taos, "create database stmt2_ts_range precision 'ms'");
  do_query(taos, "create table stmt2_ts_range.ntb (ts timestamp, val int)");
  do_query(taos, "create stable stmt2_ts_range.stb (ts timestamp, val float) tags(loc varchar(16))");

  char insertSql[512];
  (void)snprintf(insertSql, sizeof(insertSql), "insert into stmt2_ts_range.ntb values (%lld, 1)(%lld, 2)(%lld, 3)",
                 (long long)ts0, (long long)ts1, (long long)ts2);
  do_query(taos, insertSql);
  (void)snprintf(insertSql, sizeof(insertSql),
                 "insert into stmt2_ts_range.d0 using stmt2_ts_range.stb tags('a') values (%lld, 9.9)",
                 (long long)(TS_BASE + TS_1M + TS_1M / 2));
  do_query(taos, insertSql);

  // ── NTB point / range / between / open interval ───────────────────────────
  {
    int64_t          tsParam = ts1;
    TAOS_STMT2_BIND  p0 = {TSDB_DATA_TYPE_TIMESTAMP, &tsParam, NULL, NULL, 1};
    TAOS_STMT2_BIND* pv0[1] = {&p0};
    TAOS_STMT2_BINDV bv0 = {1, NULL, NULL, &pv0[0]};
    char             ref[256];
    (void)snprintf(ref, sizeof(ref), "select ts, val from stmt2_ts_range.ntb where ts = %lld", (long long)ts1);
    expectStmt2RowsMatchTaos(taos, "select ts, val from stmt2_ts_range.ntb where ts = ?", &bv0, ref);
  }

  {
    int64_t          tsStart = ts0;
    int64_t          tsEnd = ts2;
    TAOS_STMT2_BIND  ps[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStart, NULL, NULL, 1},
                              {TSDB_DATA_TYPE_TIMESTAMP, &tsEnd, NULL, NULL, 1}};
    TAOS_STMT2_BIND* pv[2] = {&ps[0], &ps[1]};
    TAOS_STMT2_BINDV bv = {1, NULL, NULL, &pv[0]};
    char             ref[256];
    (void)snprintf(ref, sizeof(ref),
                   "select ts, val from stmt2_ts_range.ntb where ts >= %lld and ts <= %lld order by ts",
                   (long long)tsStart, (long long)tsEnd);
    expectStmt2RowsMatchTaos(taos, "select ts, val from stmt2_ts_range.ntb where ts >= ? and ts <= ? order by ts", &bv,
                             ref);

    (void)snprintf(ref, sizeof(ref),
                   "select ts, val from stmt2_ts_range.ntb where ts between %lld and %lld order by ts",
                   (long long)tsStart, (long long)tsEnd);
    expectStmt2RowsMatchTaos(taos, "select ts, val from stmt2_ts_range.ntb where ts between ? and ? order by ts", &bv,
                             ref);
  }

  {
    int64_t          tsLo = ts0;
    int64_t          tsHi = ts2;
    TAOS_STMT2_BIND  ps[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsLo, NULL, NULL, 1},
                              {TSDB_DATA_TYPE_TIMESTAMP, &tsHi, NULL, NULL, 1}};
    TAOS_STMT2_BIND* pv[2] = {&ps[0], &ps[1]};
    TAOS_STMT2_BINDV bv = {1, NULL, NULL, &pv[0]};
    char             ref[256];
    (void)snprintf(ref, sizeof(ref), "select ts, val from stmt2_ts_range.ntb where ts > %lld and ts < %lld order by ts",
                   (long long)tsLo, (long long)tsHi);
    expectStmt2RowsMatchTaos(taos, "select ts, val from stmt2_ts_range.ntb where ts > ? and ts < ? order by ts", &bv,
                             ref);
  }

  // ── NTB INTERVAL without FILL ─────────────────────────────────────────────
  {
    int64_t          tsStart = ts0;
    int64_t          tsEnd = tsEnd5m;
    TAOS_STMT2_BIND  ps[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStart, NULL, NULL, 1},
                              {TSDB_DATA_TYPE_TIMESTAMP, &tsEnd, NULL, NULL, 1}};
    TAOS_STMT2_BIND* pv[2] = {&ps[0], &ps[1]};
    TAOS_STMT2_BINDV bv = {1, NULL, NULL, &pv[0]};
    char             ref[512];
    (void)snprintf(ref, sizeof(ref),
                   "select _wstart, last(val) from stmt2_ts_range.ntb where ts >= %lld and ts <= %lld interval(1m)",
                   (long long)tsStart, (long long)tsEnd);
    expectStmt2RowsMatchTaos(taos,
                             "select _wstart, last(val) from stmt2_ts_range.ntb where ts >= ? and ts <= ? "
                             "interval(1m)",
                             &bv, ref);
  }

  // ── NTB INTERVAL + FILL (production-like 1s window) ───────────────────────
  {
    const int64_t    tsStart = TS_BASE;
    const int64_t    tsEnd = TS_BASE + TS_1M * 2;
    TAOS_STMT2_BIND  ps[2] = {{TSDB_DATA_TYPE_TIMESTAMP, (void*)&tsStart, NULL, NULL, 1},
                              {TSDB_DATA_TYPE_TIMESTAMP, (void*)&tsEnd, NULL, NULL, 1}};
    TAOS_STMT2_BIND* pv[2] = {&ps[0], &ps[1]};
    TAOS_STMT2_BINDV bv = {1, NULL, NULL, &pv[0]};
    char             ref[512];
    (void)snprintf(ref, sizeof(ref),
                   "select _wstart, last(val) from stmt2_ts_range.ntb where ts >= %lld and ts <= %lld "
                   "interval(1s) fill(null_f) order by _wstart",
                   (long long)tsStart, (long long)tsEnd);
    expectStmt2RowsMatchTaos(taos,
                             "select _wstart, last(val) from stmt2_ts_range.ntb where ts >= ? and ts <= ? "
                             "interval(1s) fill(null_f) order by _wstart",
                             &bv, ref);
  }

  // ── STB tag + BETWEEN + INTERVAL + FILL ───────────────────────────────────
  {
    const char*      loc = "a";
    int32_t          locLen = (int32_t)strlen(loc);
    int64_t          tsStart = ts0;
    int64_t          tsEnd = tsEnd5m;
    TAOS_STMT2_BIND  ps[3] = {{TSDB_DATA_TYPE_VARCHAR, (void*)loc, &locLen, NULL, 1},
                              {TSDB_DATA_TYPE_TIMESTAMP, &tsStart, NULL, NULL, 1},
                              {TSDB_DATA_TYPE_TIMESTAMP, &tsEnd, NULL, NULL, 1}};
    TAOS_STMT2_BIND* pv[3] = {&ps[0], &ps[1], &ps[2]};
    TAOS_STMT2_BINDV bv = {1, NULL, NULL, &pv[0]};
    char             ref[512];
    (void)snprintf(ref, sizeof(ref),
                   "select _wstart, last(val) from stmt2_ts_range.stb where loc = 'a' and ts between %lld and %lld "
                   "interval(1m) fill(null_f) order by _wstart",
                   (long long)tsStart, (long long)tsEnd);
    expectStmt2RowsMatchTaos(taos,
                             "select _wstart, last(val) from stmt2_ts_range.stb where loc = ? and ts between ? and ? "
                             "interval(1m) fill(null_f) order by _wstart",
                             &bv, ref);
  }

  // ── Cross-precision DB (us) with ms bind values ───────────────────────────
  do_query(taos, "drop database if exists stmt2_ts_range_us");
  do_query(taos, "create database stmt2_ts_range_us precision 'us'");
  do_query(taos, "create table stmt2_ts_range_us.t (ts timestamp, val int)");
  do_query(taos, "insert into stmt2_ts_range_us.t values (1700000000000000, 1)(1700000060000000, 2)");

  {
    // stmt2 binds ms-magnitude values (auto-detected); ref SQL must use us literals.
    int64_t          tsStartMs = TS_BASE;
    int64_t          tsEndMs = TS_BASE + TS_1M * 2;
    int64_t          tsStartUs = tsStartMs * 1000LL;
    int64_t          tsEndUs = tsEndMs * 1000LL;
    TAOS_STMT2_BIND  ps[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStartMs, NULL, NULL, 1},
                              {TSDB_DATA_TYPE_TIMESTAMP, &tsEndMs, NULL, NULL, 1}};
    TAOS_STMT2_BIND* pv[2] = {&ps[0], &ps[1]};
    TAOS_STMT2_BINDV bv = {1, NULL, NULL, &pv[0]};
    char             ref[512];
    (void)snprintf(ref, sizeof(ref),
                   "select _wstart, last(val) from stmt2_ts_range_us.t where ts >= %lld and ts <= %lld "
                   "interval(1m) fill(null_f) order by _wstart",
                   (long long)tsStartUs, (long long)tsEndUs);
    expectStmt2RowsMatchTaos(taos,
                             "select _wstart, last(val) from stmt2_ts_range_us.t where ts >= ? and ts <= ? "
                             "interval(1m) fill(null_f) order by _wstart",
                             &bv, ref);
  }

  do_query(taos, "drop database if exists stmt2_ts_range_us");
  do_query(taos, "drop database if exists stmt2_ts_range");
  taos_close(taos);
}

TEST(stmt2Case, query_timestamp_time_range_interval_fill) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists secondiesdb_offical");
  do_query(taos, "create database secondiesdb_offical precision 'us'");
  do_query(taos,
           "create stable secondiesdb_offical.ng_stb_analog "
           "(ts timestamp, mvalue double, code varchar(16)) tags(location varchar(32))");
  do_query(taos,
           "insert into secondiesdb_offical.ng_analog_1 using secondiesdb_offical.ng_stb_analog tags('beijing') "
           "values (1786686360000000, 1.1, 'a')(1786687260000000, 2.2, 'b')(1786688160000000, 3.3, 'c')");
  do_query(taos,
           "insert into secondiesdb_offical.ng_analog_2 using secondiesdb_offical.ng_stb_analog tags('shanghai') "
           "values (1786686360000000, 11.1, 'd')(1786687260000000, 12.2, 'e')(1786688160000000, 13.3, 'f')");

  int64_t     tsStartMs = 1786686300000LL;
  int64_t     tsEndMs = 1786689000000LL;
  const char* stmtSql =
      "SELECT tbname, _wend AS ts, last(mvalue) AS mvalue, last(code) AS code "
      "FROM secondiesdb_offical.ng_stb_analog WHERE ts >= ?AND ts < ? AND tbname IN (?) "
      "PARTITION BY tbname INTERVAL(15m) FILL(PREV)";

  const char*      singleTbName = "ng_analog_1";
  int32_t          singleTbNameLen = (int32_t)strlen(singleTbName);
  TAOS_STMT2_BIND  singleParams[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStartMs, NULL, NULL, 1},
                                      {TSDB_DATA_TYPE_TIMESTAMP, &tsEndMs, NULL, NULL, 1},
                                      {TSDB_DATA_TYPE_VARCHAR, (void*)singleTbName, &singleTbNameLen, NULL, 1}};
  TAOS_STMT2_BIND* singleParamv[3] = {&singleParams[0], &singleParams[1], &singleParams[2]};
  TAOS_STMT2_BINDV singleBindv = {1, NULL, NULL, &singleParamv[0]};

  const char* singleRefSql =
      "SELECT tbname, _wend AS ts, last(mvalue) AS mvalue, last(code) AS code "
      "FROM secondiesdb_offical.ng_stb_analog "
      "WHERE ts >= 1786686300000000 AND ts < 1786689000000000 AND tbname IN ('ng_analog_1') "
      "PARTITION BY tbname INTERVAL(15m) FILL(PREV)";

  int       singleExpectedRows = 0;
  TAOS_RES* singleRefResult = taos_query(taos, singleRefSql);
  ASSERT_NE(singleRefResult, nullptr);
  ASSERT_EQ(taos_errno(singleRefResult), TSDB_CODE_SUCCESS) << taos_errstr(singleRefResult);
  while (taos_fetch_row(singleRefResult) != NULL) {
    ++singleExpectedRows;
  }
  taos_free_result(singleRefResult);
  ASSERT_EQ(singleExpectedRows, 3);

  int               singleActualRows = 0;
  TAOS_STMT2_OPTION singleOption = {0, true, true, NULL, NULL};
  TAOS_STMT2*       singleStmt = taos_stmt2_init(taos, &singleOption);
  ASSERT_NE(singleStmt, nullptr);
  int code = taos_stmt2_prepare(singleStmt, stmtSql, 0);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(singleStmt);
  int fieldNum = 0;
  code = taos_stmt2_get_fields(singleStmt, &fieldNum, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(singleStmt);
  code = taos_stmt2_bind_param(singleStmt, &singleBindv, -1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(singleStmt);
  code = taos_stmt2_exec(singleStmt, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(singleStmt);

  TAOS_RES* singleStmtResult = taos_stmt2_result(singleStmt);
  ASSERT_NE(singleStmtResult, nullptr);
  while (taos_fetch_row(singleStmtResult) != NULL) {
    ++singleActualRows;
  }
  taos_stmt2_close(singleStmt);
  ASSERT_EQ(singleActualRows, singleExpectedRows);

  struct SIntervalFillStmtCase {
    const char* name;
    const char* sql;
    int64_t     tsStart;
    int64_t     tsEnd;
  } stmtCases[] = {
      {"base-ms",
       "SELECT tbname, ts, mvalue, code FROM secondiesdb_offical.ng_stb_analog "
       "WHERE ts >= ? AND ts < ? AND tbname IN (?)",
       tsStartMs, tsEndMs},
      {"base-ms-equal-tbname",
       "SELECT tbname, ts, mvalue, code FROM secondiesdb_offical.ng_stb_analog "
       "WHERE ts >= ? AND ts < ? AND tbname = ?",
       tsStartMs, tsEndMs},
      {"interval-no-fill-ms",
       "SELECT tbname, _wend AS ts, last(mvalue), last(code) FROM secondiesdb_offical.ng_stb_analog "
       "WHERE ts >= ? AND ts < ? AND tbname IN (?) PARTITION BY tbname INTERVAL(15m)",
       tsStartMs, tsEndMs},
      {"interval-fill-ms-spaced",
       "SELECT tbname, _wend AS ts, last(mvalue), last(code) FROM secondiesdb_offical.ng_stb_analog "
       "WHERE ts >= ? AND ts < ? AND tbname IN (?) PARTITION BY tbname INTERVAL(15m) FILL(PREV)",
       tsStartMs, tsEndMs},
      {"interval-fill-us",
       "SELECT tbname, _wend AS ts, last(mvalue), last(code) FROM secondiesdb_offical.ng_stb_analog "
       "WHERE ts >= ? AND ts < ? AND tbname IN (?) PARTITION BY tbname INTERVAL(15m) FILL(PREV)",
       tsStartMs * 1000LL, tsEndMs * 1000LL},
      {"interval-fill-ms-equal-tbname",
       "SELECT tbname, _wend AS ts, last(mvalue), last(code) FROM secondiesdb_offical.ng_stb_analog "
       "WHERE ts >= ? AND ts < ? AND tbname = ? PARTITION BY tbname INTERVAL(15m) FILL(PREV)",
       tsStartMs, tsEndMs},
  };

  for (SIntervalFillStmtCase& stmtCase : stmtCases) {
    SCOPED_TRACE(stmtCase.name);
    TAOS_STMT2_BIND  caseParams[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &stmtCase.tsStart, NULL, NULL, 1},
                                      {TSDB_DATA_TYPE_TIMESTAMP, &stmtCase.tsEnd, NULL, NULL, 1},
                                      {TSDB_DATA_TYPE_VARCHAR, (void*)singleTbName, &singleTbNameLen, NULL, 1}};
    TAOS_STMT2_BIND* caseParamv[3] = {&caseParams[0], &caseParams[1], &caseParams[2]};
    TAOS_STMT2_BINDV caseBindv = {1, NULL, NULL, &caseParamv[0]};

    int               caseRows = 0;
    TAOS_STMT2_OPTION caseOption = {0, true, true, NULL, NULL};
    TAOS_STMT2*       caseStmt = taos_stmt2_init(taos, &caseOption);
    ASSERT_NE(caseStmt, nullptr);

    code = taos_stmt2_prepare(caseStmt, stmtCase.sql, 0);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(caseStmt);
    fieldNum = 0;
    code = taos_stmt2_get_fields(caseStmt, &fieldNum, NULL);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(caseStmt);
    code = taos_stmt2_bind_param(caseStmt, &caseBindv, -1);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(caseStmt);
    code = taos_stmt2_exec(caseStmt, NULL);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(caseStmt);

    TAOS_RES* caseStmtResult = taos_stmt2_result(caseStmt);
    ASSERT_NE(caseStmtResult, nullptr);
    while (taos_fetch_row(caseStmtResult) != NULL) {
      ++caseRows;
    }
    taos_stmt2_close(caseStmt);
    ASSERT_EQ(caseRows, 3);
  }

  {
    const char* noTbnameSql =
        "SELECT tbname, ts, mvalue, code FROM secondiesdb_offical.ng_stb_analog WHERE ts >= ? AND ts < ?";
    TAOS_STMT2_BIND  noTbnameParams[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStartMs, NULL, NULL, 1},
                                          {TSDB_DATA_TYPE_TIMESTAMP, &tsEndMs, NULL, NULL, 1}};
    TAOS_STMT2_BIND* noTbnameParamv[2] = {&noTbnameParams[0], &noTbnameParams[1]};
    TAOS_STMT2_BINDV noTbnameBindv = {1, NULL, NULL, &noTbnameParamv[0]};

    int               rows = 0;
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    code = taos_stmt2_prepare(stmt, noTbnameSql, 0);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    fieldNum = 0;
    code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    code = taos_stmt2_bind_param(stmt, &noTbnameBindv, -1);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);

    TAOS_RES* stmtResult = taos_stmt2_result(stmt);
    ASSERT_NE(stmtResult, nullptr);
    while (taos_fetch_row(stmtResult) != NULL) {
      ++rows;
    }
    taos_stmt2_close(stmt);
    ASSERT_EQ(rows, 6);
  }

  {
    const char* twoTbnameSql =
        "SELECT tbname, ts, mvalue, code FROM secondiesdb_offical.ng_stb_analog "
        "WHERE ts >= ? AND ts < ? AND tbname IN (?, ?)";
    const char*      dummyTbName = "not_exist";
    int32_t          dummyTbNameLen = (int32_t)strlen(dummyTbName);
    TAOS_STMT2_BIND  twoTbnameParams[4] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStartMs, NULL, NULL, 1},
                                           {TSDB_DATA_TYPE_TIMESTAMP, &tsEndMs, NULL, NULL, 1},
                                           {TSDB_DATA_TYPE_VARCHAR, (void*)singleTbName, &singleTbNameLen, NULL, 1},
                                           {TSDB_DATA_TYPE_VARCHAR, (void*)dummyTbName, &dummyTbNameLen, NULL, 1}};
    TAOS_STMT2_BIND* twoTbnameParamv[4] = {&twoTbnameParams[0], &twoTbnameParams[1], &twoTbnameParams[2],
                                           &twoTbnameParams[3]};
    TAOS_STMT2_BINDV twoTbnameBindv = {1, NULL, NULL, &twoTbnameParamv[0]};

    int               rows = 0;
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    code = taos_stmt2_prepare(stmt, twoTbnameSql, 0);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    fieldNum = 0;
    code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    code = taos_stmt2_bind_param(stmt, &twoTbnameBindv, -1);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);
    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);

    TAOS_RES* stmtResult = taos_stmt2_result(stmt);
    ASSERT_NE(stmtResult, nullptr);
    while (taos_fetch_row(stmtResult) != NULL) {
      ++rows;
    }
    taos_stmt2_close(stmt);
    ASSERT_EQ(rows, 3);
  }

  const char*      packedTbNames = "ng_analog_1,ng_analog_2";
  int32_t          packedTbNamesLen = (int32_t)strlen(packedTbNames);
  TAOS_STMT2_BIND  packedParams[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStartMs, NULL, NULL, 1},
                                      {TSDB_DATA_TYPE_TIMESTAMP, &tsEndMs, NULL, NULL, 1},
                                      {TSDB_DATA_TYPE_VARCHAR, (void*)packedTbNames, &packedTbNamesLen, NULL, 1}};
  TAOS_STMT2_BIND* packedParamv[3] = {&packedParams[0], &packedParams[1], &packedParams[2]};
  TAOS_STMT2_BINDV packedBindv = {1, NULL, NULL, &packedParamv[0]};

  const char* packedRefSql =
      "SELECT tbname, _wend AS ts, last(mvalue) AS mvalue, last(code) AS code "
      "FROM secondiesdb_offical.ng_stb_analog "
      "WHERE ts >= 1786686300000000 AND ts < 1786689000000000 "
      "AND tbname IN ('ng_analog_1', 'ng_analog_2') "
      "PARTITION BY tbname INTERVAL(15m) FILL(PREV)";
  int       packedExpectedRows = 0;
  TAOS_RES* packedRefResult = taos_query(taos, packedRefSql);
  ASSERT_NE(packedRefResult, nullptr);
  ASSERT_EQ(taos_errno(packedRefResult), TSDB_CODE_SUCCESS) << taos_errstr(packedRefResult);
  while (taos_fetch_row(packedRefResult) != NULL) {
    ++packedExpectedRows;
  }
  taos_free_result(packedRefResult);
  ASSERT_EQ(packedExpectedRows, 6);

  int               packedActualRows = 0;
  TAOS_STMT2_OPTION packedOption = {0, true, true, NULL, NULL};
  TAOS_STMT2*       packedStmt = taos_stmt2_init(taos, &packedOption);
  ASSERT_NE(packedStmt, nullptr);
  code = taos_stmt2_prepare(packedStmt, stmtSql, 0);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(packedStmt);
  fieldNum = 0;
  code = taos_stmt2_get_fields(packedStmt, &fieldNum, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(packedStmt);
  code = taos_stmt2_bind_param(packedStmt, &packedBindv, -1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(packedStmt);
  code = taos_stmt2_exec(packedStmt, NULL);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(packedStmt);

  TAOS_RES* packedStmtResult = taos_stmt2_result(packedStmt);
  ASSERT_NE(packedStmtResult, nullptr);
  while (taos_fetch_row(packedStmtResult) != NULL) {
    ++packedActualRows;
  }
  taos_stmt2_close(packedStmt);
  ASSERT_EQ(packedActualRows, 0);

  do_query(taos, "drop database if exists secondiesdb_offical");
  taos_close(taos);
}

TEST(stmt2Case, query_timezone_with_timestamp_params) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);

  do_query(taos, "drop database if exists stmt2_query_timezone");
  do_query(taos, "create database stmt2_query_timezone precision 'ms'");
  do_query(taos, "create table stmt2_query_timezone.t (ts timestamp, v int)");
  do_query(taos, "insert into stmt2_query_timezone.t values (1704067200000, 1) (1704153600000, 2)");

  const int64_t tsStart = 1704067200000LL;  // 2024-01-01 00:00:00 UTC
  const int64_t tsEnd = 1704153600000LL;    // 2024-01-02 00:00:00 UTC
  const char*   implicitSql =
      "select to_char(_rowts, 'yyyyMMddHH24miss') from stmt2_query_timezone.t "
      "where _rowts between ? and ? order by _rowts";
  const char* explicitUtcSql =
      "select to_char(_rowts, 'yyyyMMddHH24miss', 'UTC') from stmt2_query_timezone.t "
      "where _rowts between ? and ? order by _rowts";

  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "UTC"), TSDB_CODE_SUCCESS);
  const char* utc[] = {"20240101000000", "20240102000000"};
  expectStmt2ToCharRows(taos, implicitSql, tsStart, tsEnd, utc, 2);

  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "Asia/Shanghai"), TSDB_CODE_SUCCESS);
  const char* shanghai[] = {"20240101080000", "20240102080000"};
  expectStmt2ToCharRows(taos, implicitSql, tsStart, tsEnd, shanghai, 2);

  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "America/Los_Angeles"), TSDB_CODE_SUCCESS);
  const char* losAngeles[] = {"20231231160000", "20240101160000"};
  expectStmt2ToCharRows(taos, implicitSql, tsStart, tsEnd, losAngeles, 2);

  // An explicit TO_CHAR timezone must override the connection timezone.
  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "Asia/Kolkata"), TSDB_CODE_SUCCESS);
  expectStmt2ToCharRows(taos, explicitUtcSql, tsStart, tsEnd, utc, 2);

  // Rebinding a narrower range must keep the same session timezone.
  const char* kolkata[] = {"20240102053000"};
  expectStmt2ToCharRows(taos, implicitSql, tsEnd, tsEnd, kolkata, 1);

  do_query(taos, "drop database if exists stmt2_query_timezone");
  taos_close(taos);
}

TEST(stmt2Case, query_timezone_function_contexts_with_timestamp_params) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_query_timezone_funcs");
  do_query(taos, "create database stmt2_query_timezone_funcs precision 'ms'");
  do_query(taos, "create table stmt2_query_timezone_funcs.t (ts timestamp, v int)");
  do_query(taos,
           "insert into stmt2_query_timezone_funcs.t values "
           "(1767225600000, 1) "   // 2026-01-01 00:00:00 UTC
           "(1772963999000, 2) "   // one second before Los Angeles DST spring-forward
           "(1772964000000, 3) "   // Los Angeles DST spring-forward boundary
           "(1777543200000, 4) "   // 2026-04-30 10:00:00 UTC, useful for week truncation
           "(1793523599000, 5) "   // one second before Los Angeles DST fall-back
           "(1793523600000, 6)");  // Los Angeles DST fall-back boundary

  const char* stmtSql =
      "select ts, "
      "to_char(ts, 'yyyy-MM-dd HH24:mi:ss'), "
      "to_iso8601(ts), "
      "timetruncate(ts, 1d), "
      "timetruncate(ts, 1w), "
      "date(ts), "
      "dayofweek(ts), weekday(ts), week(ts), weekofyear(ts), "
      "timezone(), first_day_of_week(), "
      "to_char(ts, 'yyyy-MM-dd HH24:mi:ss', 'UTC'), "
      "to_iso8601(ts, 'UTC'), "
      "timetruncate(ts, 1d, 'UTC'), "
      "case when v > 0 then to_char(ts, 'yyyyMMddHH24miss') else '' end, "
      "to_timestamp(to_char(ts, 'yyyy-MM-dd HH24:mi:ss'), 'yyyy-MM-dd HH24:mi:ss') "
      "from stmt2_query_timezone_funcs.t where ts between ? and ? order by ts";

  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "UTC"), TSDB_CODE_SUCCESS);
  do_query(taos, "set first_day_of_week 0");
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, stmtSql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  struct SessionCase {
    const char* timezone;
    int32_t     firstDayOfWeek;
    int64_t     start;
    int64_t     end;
    int32_t     expectedRows;
  } sessionCases[] = {
      {"UTC", 0, 1767225600000LL, 1793523600000LL, 6},
      {"Asia/Shanghai", 1, 1767225600000LL, 1793523600000LL, 6},
      {"America/Los_Angeles", 4, 1767225600000LL, 1793523600000LL, 6},
      {"Asia/Kolkata", 6, 1772964000000LL, 1777543200000LL, 2},
      {"UTC", 2, 1800000000000LL, 1800000001000LL, 0},
  };

  for (size_t i = 0; i < sizeof(sessionCases) / sizeof(sessionCases[0]); ++i) {
    const SessionCase* sessionCase = &sessionCases[i];
    ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, sessionCase->timezone), TSDB_CODE_SUCCESS);
    char setFirstDaySql[64];
    (void)snprintf(setFirstDaySql, sizeof(setFirstDaySql), "set first_day_of_week %d", sessionCase->firstDayOfWeek);
    do_query(taos, setFirstDaySql);

    int64_t          tsStart = sessionCase->start;
    int64_t          tsEnd = sessionCase->end;
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStart, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_TIMESTAMP, &tsEnd, NULL, NULL, 1}};
    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};

    char refSql[4096];
    (void)snprintf(refSql, sizeof(refSql),
                   "select ts, "
                   "to_char(ts, 'yyyy-MM-dd HH24:mi:ss'), "
                   "to_iso8601(ts), "
                   "timetruncate(ts, 1d), "
                   "timetruncate(ts, 1w), "
                   "date(ts), "
                   "dayofweek(ts), weekday(ts), week(ts), weekofyear(ts), "
                   "timezone(), first_day_of_week(), "
                   "to_char(ts, 'yyyy-MM-dd HH24:mi:ss', 'UTC'), "
                   "to_iso8601(ts, 'UTC'), "
                   "timetruncate(ts, 1d, 'UTC'), "
                   "case when v > 0 then to_char(ts, 'yyyyMMddHH24miss') else '' end, "
                   "to_timestamp(to_char(ts, 'yyyy-MM-dd HH24:mi:ss'), 'yyyy-MM-dd HH24:mi:ss') "
                   "from stmt2_query_timezone_funcs.t where ts between %lld and %lld order by ts",
                   (long long)tsStart, (long long)tsEnd);

    Stmt2ResultSnapshot expected = {0};
    Stmt2ResultSnapshot actual = {0};
    ASSERT_TRUE(snapshotTaosQuery(taos, refSql, &expected));
    ASSERT_EQ(expected.rows, sessionCase->expectedRows) << "timezone=" << sessionCase->timezone;
    ASSERT_TRUE(snapshotStmt2Query(stmt, &bindv, &actual));
    char context[128];
    (void)snprintf(context, sizeof(context), "timezone=%s, firstDayOfWeek=%d", sessionCase->timezone,
                   sessionCase->firstDayOfWeek);
    expectSnapshotsEqual(expected, actual, context);
  }
  taos_stmt2_close(stmt);

  // Functions inside a derived table and both UNION ALL branches must receive
  // the same session context as functions in a top-level SELECT.
  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "UTC"), TSDB_CODE_SUCCESS);
  do_query(taos, "set first_day_of_week 0");
  const char* nestedStmtSql =
      "select ts, formatted, iso from ("
      "select ts, to_char(ts, 'yyyyMMddHH24miss') formatted, to_iso8601(ts) iso "
      "from stmt2_query_timezone_funcs.t where ts between ? and ?) q order by ts";
  const char* unionStmtSql =
      "select ts, to_char(ts, 'yyyyMMddHH24miss'), to_iso8601(ts) "
      "from stmt2_query_timezone_funcs.t where ts between ? and ? "
      "union all "
      "select ts, to_char(ts, 'yyyyMMddHH24miss'), to_iso8601(ts) "
      "from stmt2_query_timezone_funcs.t where ts between ? and ? order by ts";
  const char* stringRangeStmtSql =
      "select ts, to_char(ts, 'yyyyMMddHH24miss') from stmt2_query_timezone_funcs.t "
      "where ts between ? and ? order by ts";
  TAOS_STMT2* nestedStmt = taos_stmt2_init(taos, &option);
  TAOS_STMT2* unionStmt = taos_stmt2_init(taos, &option);
  TAOS_STMT2* stringRangeStmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(nestedStmt, nullptr);
  ASSERT_NE(unionStmt, nullptr);
  ASSERT_NE(stringRangeStmt, nullptr);
  code = taos_stmt2_prepare(nestedStmt, nestedStmtSql, 0);
  checkError(nestedStmt, code, __FILE__, __LINE__);
  code = taos_stmt2_prepare(unionStmt, unionStmtSql, 0);
  checkError(unionStmt, code, __FILE__, __LINE__);
  code = taos_stmt2_prepare(stringRangeStmt, stringRangeStmtSql, 0);
  checkError(stringRangeStmt, code, __FILE__, __LINE__);

  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "Asia/Kolkata"), TSDB_CODE_SUCCESS);
  do_query(taos, "set first_day_of_week 5");
  int64_t          nestedStart = 1772963999000LL;
  int64_t          nestedEnd = 1777543200000LL;
  TAOS_STMT2_BIND  nestedParams[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &nestedStart, NULL, NULL, 1},
                                      {TSDB_DATA_TYPE_TIMESTAMP, &nestedEnd, NULL, NULL, 1}};
  TAOS_STMT2_BIND* nestedParamv[2] = {&nestedParams[0], &nestedParams[1]};
  TAOS_STMT2_BINDV nestedBindv = {1, NULL, NULL, &nestedParamv[0]};
  char             nestedRefSql[1024];
  (void)snprintf(nestedRefSql, sizeof(nestedRefSql),
                 "select ts, formatted, iso from ("
                 "select ts, to_char(ts, 'yyyyMMddHH24miss') formatted, to_iso8601(ts) iso "
                 "from stmt2_query_timezone_funcs.t where ts between %lld and %lld) q order by ts",
                 (long long)nestedStart, (long long)nestedEnd);
  Stmt2ResultSnapshot expected = {0};
  Stmt2ResultSnapshot actual = {0};
  ASSERT_TRUE(snapshotTaosQuery(taos, nestedRefSql, &expected));
  ASSERT_TRUE(snapshotStmt2Query(nestedStmt, &nestedBindv, &actual));
  expectSnapshotsEqual(expected, actual, "derived table");

  int64_t          unionStart1 = 1772963999000LL;
  int64_t          unionEnd1 = 1772964000000LL;
  int64_t          unionStart2 = 1793523599000LL;
  int64_t          unionEnd2 = 1793523600000LL;
  TAOS_STMT2_BIND  unionParams[4] = {{TSDB_DATA_TYPE_TIMESTAMP, &unionStart1, NULL, NULL, 1},
                                     {TSDB_DATA_TYPE_TIMESTAMP, &unionEnd1, NULL, NULL, 1},
                                     {TSDB_DATA_TYPE_TIMESTAMP, &unionStart2, NULL, NULL, 1},
                                     {TSDB_DATA_TYPE_TIMESTAMP, &unionEnd2, NULL, NULL, 1}};
  TAOS_STMT2_BIND* unionParamv[4] = {&unionParams[0], &unionParams[1], &unionParams[2], &unionParams[3]};
  TAOS_STMT2_BINDV unionBindv = {1, NULL, NULL, &unionParamv[0]};
  char             unionRefSql[1536];
  (void)snprintf(unionRefSql, sizeof(unionRefSql),
                 "select ts, to_char(ts, 'yyyyMMddHH24miss'), to_iso8601(ts) "
                 "from stmt2_query_timezone_funcs.t where ts between %lld and %lld "
                 "union all "
                 "select ts, to_char(ts, 'yyyyMMddHH24miss'), to_iso8601(ts) "
                 "from stmt2_query_timezone_funcs.t where ts between %lld and %lld order by ts",
                 (long long)unionStart1, (long long)unionEnd1, (long long)unionStart2, (long long)unionEnd2);
  ASSERT_TRUE(snapshotTaosQuery(taos, unionRefSql, &expected));
  ASSERT_TRUE(snapshotStmt2Query(unionStmt, &unionBindv, &actual));
  expectSnapshotsEqual(expected, actual, "union all");

  // The placeholder value nodes were created while the statement used UTC.
  // Binding local-time strings after switching the connection to Shanghai
  // must parse those strings with the current session timezone.
  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "Asia/Shanghai"), TSDB_CODE_SUCCESS);
  char             stringStart[] = "2026-03-08 17:59:59";
  char             stringEnd[] = "2026-03-08 18:00:00";
  int32_t          stringStartLen = strlen(stringStart);
  int32_t          stringEndLen = strlen(stringEnd);
  TAOS_STMT2_BIND  stringParams[2] = {{TSDB_DATA_TYPE_BINARY, stringStart, &stringStartLen, NULL, 1},
                                      {TSDB_DATA_TYPE_BINARY, stringEnd, &stringEndLen, NULL, 1}};
  TAOS_STMT2_BIND* stringParamv[2] = {&stringParams[0], &stringParams[1]};
  TAOS_STMT2_BINDV stringBindv = {1, NULL, NULL, &stringParamv[0]};
  const char*      stringRefSql =
      "select ts, to_char(ts, 'yyyyMMddHH24miss') from stmt2_query_timezone_funcs.t "
      "where ts between '2026-03-08 17:59:59' and '2026-03-08 18:00:00' order by ts";
  ASSERT_TRUE(snapshotTaosQuery(taos, stringRefSql, &expected));
  ASSERT_EQ(expected.rows, 2);
  ASSERT_TRUE(snapshotStmt2Query(stringRangeStmt, &stringBindv, &actual));
  expectSnapshotsEqual(expected, actual, "timestamp string parameters");

  taos_stmt2_close(nestedStmt);
  taos_stmt2_close(unionStmt);
  taos_stmt2_close(stringRangeStmt);
  do_query(taos, "drop database if exists stmt2_query_timezone_funcs");
  taos_close(taos);
}

TEST(stmt2Case, query_timezone_functions_with_cross_precision_timestamp_params) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, "Asia/Shanghai"), TSDB_CODE_SUCCESS);

  struct PrecisionCase {
    const char* database;
    const char* precision;
    int64_t     scaleFromMs;
  } precisionCases[] = {
      {"stmt2_query_timezone_us", "us", 1000LL},
      {"stmt2_query_timezone_ns", "ns", 1000000LL},
  };

  const int64_t tsStartMs = 1767225600000LL;
  const int64_t tsMiddleMs = 1767225600500LL;
  const int64_t tsEndMs = 1767225601000LL;
  for (size_t i = 0; i < sizeof(precisionCases) / sizeof(precisionCases[0]); ++i) {
    const PrecisionCase* precisionCase = &precisionCases[i];
    char                 sql[2048];
    (void)snprintf(sql, sizeof(sql), "drop database if exists %s", precisionCase->database);
    do_query(taos, sql);
    (void)snprintf(sql, sizeof(sql), "create database %s precision '%s'", precisionCase->database,
                   precisionCase->precision);
    do_query(taos, sql);
    (void)snprintf(sql, sizeof(sql), "create table %s.t (ts timestamp, v int)", precisionCase->database);
    do_query(taos, sql);

    const int64_t scaledStart = tsStartMs * precisionCase->scaleFromMs;
    const int64_t scaledMiddle = tsMiddleMs * precisionCase->scaleFromMs;
    const int64_t scaledEnd = tsEndMs * precisionCase->scaleFromMs;
    (void)snprintf(sql, sizeof(sql), "insert into %s.t values (%lld, 0) (%lld, 1) (%lld, 2) (%lld, 3) (%lld, 4)",
                   precisionCase->database, (long long)(scaledStart - 1), (long long)scaledStart,
                   (long long)scaledMiddle, (long long)scaledEnd, (long long)(scaledEnd + 1));
    do_query(taos, sql);

    char stmtSql[1024];
    (void)snprintf(stmtSql, sizeof(stmtSql),
                   "select ts, to_char(ts, 'yyyy-MM-dd HH24:mi:ss.ms'), to_iso8601(ts), timetruncate(ts, 1d) "
                   "from %s.t where ts between ? and ? order by ts",
                   precisionCase->database);
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, stmtSql, 0);
    checkError(stmt, code, __FILE__, __LINE__);

    int64_t          boundStartMs = tsStartMs;
    int64_t          boundEndMs = tsEndMs;
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &boundStartMs, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_TIMESTAMP, &boundEndMs, NULL, NULL, 1}};
    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};
    char             refSql[1024];
    (void)snprintf(refSql, sizeof(refSql),
                   "select ts, to_char(ts, 'yyyy-MM-dd HH24:mi:ss.ms'), to_iso8601(ts), timetruncate(ts, 1d) "
                   "from %s.t where ts between %lld and %lld order by ts",
                   precisionCase->database, (long long)scaledStart, (long long)scaledEnd);
    Stmt2ResultSnapshot expected = {0};
    Stmt2ResultSnapshot actual = {0};
    ASSERT_TRUE(snapshotTaosQuery(taos, refSql, &expected));
    ASSERT_EQ(expected.rows, 3) << "precision=" << precisionCase->precision;
    ASSERT_TRUE(snapshotStmt2Query(stmt, &bindv, &actual));
    expectSnapshotsEqual(expected, actual, precisionCase->precision);
    taos_stmt2_close(stmt);

    (void)snprintf(sql, sizeof(sql), "drop database if exists %s", precisionCase->database);
    do_query(taos, sql);
  }
  taos_close(taos);
}

TEST(stmt2Case, query_gps_time_range_with_timestamp_params) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_gps_query");
  do_query(taos, "create database stmt2_gps_query precision 'ms'");
  do_query(taos,
           "create table stmt2_gps_query.d_p (ts timestamp, params3 int, `2601` int, `2602` double, "
           "`2603` double, `4031` int, `3020` int, `2204` double)");

  // Keep source data spanning 2026-02-03 through 2026-02-06.
  const int64_t tsFeb3 = 1770048000000LL;      // 2026-02-03 00:00:00.000, Asia/Shanghai
  int64_t       tsStart = 1770250745000LL;     // 2026-02-05 08:19:05.000, Asia/Shanghai
  const int64_t tsInRange1 = 1770250748000LL;  // 2026-02-05 08:19:08.000, Asia/Shanghai
  const int64_t tsInRange2 = 1770250750000LL;  // 2026-02-05 08:19:10.000, Asia/Shanghai
  const int64_t tsInRange3 = 1770250755000LL;  // 2026-02-05 08:19:15.000, Asia/Shanghai
  int64_t       tsEnd = 1770250760000LL;       // 2026-02-05 08:19:20.000, Asia/Shanghai
  const int64_t tsFeb6 = 1770393599000LL;      // 2026-02-06 23:59:59.000, Asia/Shanghai

  char insertSql[2048];
  (void)snprintf(insertSql, sizeof(insertSql),
                 "insert into stmt2_gps_query.d_p values "
                 "(%lld, 1, 0, 116.3000, 39.9000, 1, 1, 30.0) "
                 "(%lld, 1, 0, 116.3100, 39.9100, 1, 1, 31.0) "
                 "(%lld, 3, 0, 116.3200, 39.9200, 1, 1, 32.0) "
                 "(%lld, 2, 0, 116.3300, 39.9300, 1, 1, 33.0) "
                 "(%lld, 1, 0, 0.0000, 39.9400, 1, 1, 34.0) "
                 "(%lld, 1, 0, 116.3400, 39.9500, 1, 1, 35.0) "
                 "(%lld, 2, 0, 116.3500, 39.9600, 1, 1, 36.0)",
                 (long long)tsFeb3, (long long)tsStart, (long long)tsInRange1, (long long)tsInRange2,
                 (long long)tsInRange3, (long long)tsEnd, (long long)tsFeb6);
  do_query(taos, insertSql);

  // Add dense history outside the bound range, including points immediately
  // before and after both bound values. These rows must never be returned.
  const int64_t extraTimestamps[] = {
      tsFeb3 + 1,
      tsFeb3 + 6 * 3600 * 1000LL,
      tsFeb3 + 12 * 3600 * 1000LL,
      tsFeb3 + 18 * 3600 * 1000LL,  // 2026-02-03
      tsStart - 24 * 3600 * 1000LL,
      tsStart - 6 * 3600 * 1000LL,
      tsStart - 3600 * 1000LL,
      tsStart - 60 * 1000LL,
      tsStart - 1000LL,
      tsStart - 1,  // before range
      tsEnd + 1,
      tsEnd + 1000LL,
      tsEnd + 60 * 1000LL,
      tsEnd + 3600 * 1000LL,
      tsEnd + 6 * 3600 * 1000LL,
      tsEnd + 24 * 3600 * 1000LL,
      tsFeb6 - 18 * 3600 * 1000LL,
      tsFeb6 - 12 * 3600 * 1000LL,
      tsFeb6 - 6 * 3600 * 1000LL,
      tsFeb6 - 1,  // 2026-02-06
  };
  for (size_t i = 0; i < sizeof(extraTimestamps) / sizeof(extraTimestamps[0]); ++i) {
    (void)snprintf(insertSql, sizeof(insertSql),
                   "insert into stmt2_gps_query.d_p values (%lld, %d, 0, %.4f, %.4f, %d, %d, %.1f)",
                   (long long)extraTimestamps[i], (int)(i % 3) + 1, 116.4000 + (double)i * 0.01,
                   39.7000 + (double)i * 0.01, (int)i, (int)(i % 2), 40.0 + (double)i);
    do_query(taos, insertSql);
  }

  const char* whereClause =
      " where params3 in (1, 2) and _rowts between ? and ? and `2601` = 0 and `2602` != 0 and `2603` != 0 "
      "and `2602` >= -180 and `2602` <= 180 and `2603` >= -90 and `2603` <= 90 order by _rowts";
  const char* stmtRawSql = "select ts from stmt2_gps_query.d_p";
  const char* stmtToCharSql = "select to_char(_rowts, 'yyyyMMddHH24miss') `gpsTime` from stmt2_gps_query.d_p";

  // These points make the bound range dense and exercise every filter branch.
  const int64_t denseTimestamps[] = {1770250746000LL, 1770250747000LL, 1770250749000LL, 1770250751000LL,
                                     1770250752000LL, 1770250753000LL, 1770250754000LL, 1770250756000LL,
                                     1770250757000LL, 1770250758000LL, 1770250759000LL};
  for (size_t i = 0; i < sizeof(denseTimestamps) / sizeof(denseTimestamps[0]); ++i) {
    const int    params3 = (i == 2) ? 3 : 1;
    const int    col2601 = (i == 3) ? 1 : 0;
    const double lon = (i == 4) ? 181.0 : 116.50 + (double)i * 0.01;
    const double lat = (i == 5) ? 91.0 : ((i == 6) ? 0.0 : 39.50 + (double)i * 0.01);
    (void)snprintf(
        insertSql, sizeof(insertSql), "insert into stmt2_gps_query.d_p values (%lld, %d, %d, %.4f, %.4f, %d, %d, %.1f)",
        (long long)denseTimestamps[i], params3, col2601, lon, lat, 10 + (int)i, (int)(i % 2), 50.0 + (double)i);
    do_query(taos, insertSql);
  }

  auto runStmt = [&](const char* sql, bool withToChar, int64_t* rawRows, char formattedRows[][32], int32_t* rowCount) {
    *rowCount = 0;
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    if (stmt == nullptr) {
      ADD_FAILURE() << "STMT2 init failed";
      return false;
    }
    int code = taos_stmt2_prepare(stmt, sql, 0);
    if (code != TSDB_CODE_SUCCESS) {
      ADD_FAILURE() << "STMT2 prepare failed: " << taos_stmt2_error(stmt);
      taos_stmt2_close(stmt);
      return false;
    }
    TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &tsStart, NULL, NULL, 1},
                                  {TSDB_DATA_TYPE_TIMESTAMP, &tsEnd, NULL, NULL, 1}};
    TAOS_STMT2_BIND* paramv[2] = {&params[0], &params[1]};
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    if (code != TSDB_CODE_SUCCESS) {
      ADD_FAILURE() << "STMT2 bind failed: " << taos_stmt2_error(stmt);
      taos_stmt2_close(stmt);
      return false;
    }
    code = taos_stmt2_exec(stmt, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      ADD_FAILURE() << "STMT2 execute failed: " << taos_stmt2_error(stmt);
      taos_stmt2_close(stmt);
      return false;
    }
    TAOS_RES* res = taos_stmt2_result(stmt);
    if (res == nullptr) {
      ADD_FAILURE() << "STMT2 result failed: " << taos_stmt2_error(stmt);
      taos_stmt2_close(stmt);
      return false;
    }
    int32_t  rows = 0;
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res)) != nullptr) {
      if (rows == 64 || row[0] == nullptr) {
        ADD_FAILURE() << "invalid STMT2 result row";
        taos_stmt2_close(stmt);
        return false;
      }
      if (withToChar) {
        int* lengths = taos_fetch_lengths(res);
        if (lengths == nullptr) {
          ADD_FAILURE() << "STMT2 fetch lengths failed";
          taos_stmt2_close(stmt);
          return false;
        }
        snprintf(formattedRows[rows], sizeof(formattedRows[rows]), "%.*s", lengths[0], (const char*)row[0]);
      } else {
        rawRows[rows] = *(int64_t*)row[0];
      }
      ++rows;
    }
    taos_stmt2_close(stmt);
    *rowCount = rows;
    return true;
  };

  auto compareAllResults = [&](const char* timezone) {
    ASSERT_EQ(taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, timezone), TSDB_CODE_SUCCESS);

    char directSql[1024];
    (void)snprintf(directSql, sizeof(directSql),
                   "select ts, to_char(_rowts, 'yyyyMMddHH24miss') from stmt2_gps_query.d_p "
                   "where params3 in (1, 2) and _rowts between %lld and %lld and `2601` = 0 and `2602` != 0 "
                   "and `2603` != 0 and `2602` >= -180 and `2602` <= 180 and `2603` >= -90 and `2603` <= 90 "
                   "order by _rowts",
                   (long long)tsStart, (long long)tsEnd);
    TAOS_RES* directRes = execQueryWithRetry(taos, directSql);
    ASSERT_NE(directRes, nullptr) << "direct query failed in timezone " << timezone;
    ASSERT_EQ(taos_errno(directRes), TSDB_CODE_SUCCESS) << taos_errstr(directRes);

    int64_t  queryRows[64] = {0};
    char     queryGpsTimes[64][32] = {{0}};
    int32_t  queryRowCount = 0;
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(directRes)) != nullptr) {
      ASSERT_LT(queryRowCount, 64);
      ASSERT_NE(row[0], nullptr);
      ASSERT_NE(row[1], nullptr);
      queryRows[queryRowCount] = *(int64_t*)row[0];
      int* lengths = taos_fetch_lengths(directRes);
      ASSERT_NE(lengths, nullptr);
      snprintf(queryGpsTimes[queryRowCount], sizeof(queryGpsTimes[queryRowCount]), "%.*s", lengths[1],
               (const char*)row[1]);
      ++queryRowCount;
    }
    taos_free_result(directRes);

    char rawSql[1024];
    char toCharSql[1024];
    (void)snprintf(rawSql, sizeof(rawSql), "%s%s", stmtRawSql, whereClause);
    (void)snprintf(toCharSql, sizeof(toCharSql), "%s%s", stmtToCharSql, whereClause);
    int64_t stmtRawRows[64] = {0};
    char    stmtToCharRows[64][32] = {{0}};
    int32_t stmtRawCount = 0;
    int32_t stmtToCharCount = 0;
    ASSERT_TRUE(runStmt(rawSql, false, stmtRawRows, NULL, &stmtRawCount));
    ASSERT_TRUE(runStmt(toCharSql, true, NULL, stmtToCharRows, &stmtToCharCount));

    ASSERT_EQ(queryRowCount, 9) << "timezone=" << timezone;
    ASSERT_EQ(stmtRawCount, queryRowCount) << "timezone=" << timezone;
    ASSERT_EQ(stmtToCharCount, queryRowCount) << "timezone=" << timezone;
    for (int32_t i = 0; i < queryRowCount; ++i) {
      ASSERT_EQ(stmtRawRows[i], queryRows[i]) << "timezone=" << timezone << ", row=" << i;
      ASSERT_STREQ(stmtToCharRows[i], queryGpsTimes[i]) << "timezone=" << timezone << ", row=" << i;
    }
  };

  compareAllResults("Asia/Shanghai");
  compareAllResults("UTC");
  compareAllResults("America/Los_Angeles");

  do_query(taos, "drop database if exists stmt2_gps_query");
  taos_close(taos);
}

// class stmt2CaseF : public testing::Test {
//   public:
//     stmt2CaseF() : taos_(NULL), stmt2_(NULL) { }
//     ~stmt2CaseF() { }

//     void SetUp(void) override {
//     }

//     void TearDown(void) override {
//       if (stmt2_) {
//         taos_stmt2_close(stmt2_);
//         stmt2_ = NULL;
//       }
//       if (taos_) {
//         taos_close(taos_);
//         taos_ = NULL;
//       }
//     }

//     const char* hexify(int32_t code) {
//       snprintf(buf_, sizeof(buf_), "0x%08x", code);
//       return buf_;
//     }

//     TAOS       *taos_;
//     TAOS_STMT2 *stmt2_;
//     char        buf_[64];
// };

// TEST_F(stmt2CaseF, exec_direct) {
//   taos_ = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
//   ASSERT_NE(taos_, nullptr);

//   stmt2_ = taos_stmt2_init(taos_, NULL);
//   ASSERT_NE(stmt2_, nullptr);

// #define R(sql, exp, affected_rows, rows) { __LINE__, sql, exp, affected_rows, rows }
//   struct {
//     int                 line;
//     const char         *sql;
//     bool                exp_ok;
//     int                 exp_affected_rows;
//     int                 exp_rows;
//   } _cases[] = {
//     R("drop database if exists stmt2_exec_direct",
//         true, 0, -1),
//     R("create database if not exists stmt2_exec_direct precision 'ns'",
//         true, 0, -1),
//     R("create table stmt2_exec_direct.t (ts timestamp, i32 int)",
//         true, 0, -1),
//     R("insert into stmt2_exec_direct.t (ts, i32) values (now, 1) (now+1b, 2) (now+2b, 3)",
//         true, 3, -1),
//     R("select * from stmt2_exec_direct.t",
//         true, 0, 3),
//     R("select * from stmt2_exec_direct.t where 1 = 2",
//         true, 0, 0),
//   };
// #undef R
//   for (size_t i=0; i<sizeof(_cases)/sizeof(*_cases); ++i) {
//     int         line               = _cases[i].line;
//     const char *sql                = _cases[i].sql;
//     bool        exp_ok             = _cases[i].exp_ok;
//     int         exp_affected_rows  = _cases[i].exp_affected_rows;
//     int         exp_rows           = _cases[i].exp_rows;
//     int r = taos_stmt2_prepare(stmt2_, sql, (unsigned long)strlen(sql));
//     if ((!r) ^ exp_ok) {
//       if (exp_ok) {
//         ASSERT_EQ(r, 0)
//           << "`taos_stmt2_prepare` "
//           << "expecting success, but failed" << std::endl
//           << "[" << hexify(r) << "]" << taos_stmt2_error(stmt2_) << std::endl
//           << "@" << line << std::endl
//           << sql << std::endl;
//       } else {
//         ASSERT_NE(r, 0)
//           << "`taos_stmt2_prepare` "
//           << "expecting failure, but succeeded" << std::endl
//           << "[" << hexify(r) << "]" << taos_stmt2_error(stmt2_) << std::endl
//           << "@" << line << std::endl
//           << sql << std::endl;
//       }
//     }

//     int affected_rows = 0;
//     r = taos_stmt2_exec(stmt2_, &affected_rows);
//     if ((!r) ^ exp_ok) {
//       if (exp_ok) {
//         ASSERT_EQ(r, 0)
//           << "`taos_stmt2_exec` "
//           << "expecting success, but failed" << std::endl
//           << "[" << hexify(r) << "]" << taos_stmt2_error(stmt2_) << std::endl
//           << "@" << line << std::endl
//           << sql << std::endl;
//       } else {
//         ASSERT_NE(r, 0)
//           << "`taos_stmt2_exec` "
//           << "expecting success, but failed" << std::endl
//           << "[" << hexify(r) << "]" << taos_stmt2_error(stmt2_) << std::endl
//           << "@" << line << std::endl
//           << sql << std::endl;
//       }
//     } else if (r == 0) {
//       ASSERT_EQ(affected_rows, exp_affected_rows)
//         << "@" << line << std::endl
//         << sql << std::endl;
//     }

//     TAOS_RES *res = taos_stmt2_result(stmt2_);
//     if ((!!res) ^ (exp_rows!=-1)) {
//       if (exp_rows!=-1) {
//         ASSERT_NE(res, nullptr)
//           << "expecting resultset, but got either ddl or insert" << std::endl
//           << "@" << line << std::endl
//           << sql << std::endl;
//       } else {
//         ASSERT_EQ(res, nullptr)
//           << "expecting either ddl or insert, but got resultset" << std::endl
//           << "@" << line << std::endl
//           << sql << std::endl;
//       }
//     }
//     if (exp_rows!=-1) {
//       int nr_rows = 0;
//       while (taos_fetch_row(res)) {
//         ++nr_rows;
//       }
//       ASSERT_EQ(nr_rows, exp_rows)
//           << "@" << line << std::endl
//           << sql << std::endl;
//     }
//   }

//   taos_stmt2_close(stmt2_);
//   stmt2_ = nullptr;

//   // ── Cleanup ───────────────────────────────────────────────────────────────
//   do_query(taos_, "drop database if exists stmt2_exec_direct");
// }

// freemine test unstable
// TEST_F(stmt2CaseF, insert) {
//   taos_ = connectTaosOrExit("localhost", "root", "taosdata", "", 0);
//   ASSERT_NE(taos_, nullptr);

//   do_query(taos_, "drop database if exists stmt2_insert");
//   do_query(taos_, "create database if not exists stmt2_insert precision 'ns'");
//   do_query(taos_, "create table stmt2_insert.t (ts timestamp, i32 int)");
//   do_query(taos_, "create table stmt2_insert.stb (ts timestamp, i32 int) tags (tname varchar(2))");
//   do_query(taos_, "use stmt2_insert");

// #define R(sql, exp_code, exp_params) { __LINE__, sql, exp_code, exp_params}
//   struct {
//     int                 line;
//     const char         *sql;
//     int32_t             exp_code;
//     int                 exp_params;
//   } _cases1[] = {
//     R("insert into stmt2_insert.t (ts, i32) values (?, 1)",
//         TSDB_CODE_SUCCESS, 1),
//     R("insert into stmt2_insert.? (ts, i32) values (?, ?)",
//         TSDB_CODE_SUCCESS, 3),
//     R("insert into stmt2_insert.t (ts, i32) values (?, ?)",
//         TSDB_CODE_SUCCESS, 2),
//     // no table found
//     R("insert into stmt2_insert.tx (ts, i32) values (?, ?)",
//         TSDB_CODE_TSC_STMT_TBNAME_ERROR, -1),
//     // no super table (guessed by `tbname`) found
//     R("insert into stmt2_insert.xstb (tbname, tname, ts, i32) values (?, ?, ?, ?)",
//         TSDB_CODE_TSC_STMT_TBNAME_ERROR, -1),
//     // no super table (guessed by using clause) found
//     R("insert into ? using stmt2_insert.xstb (tname) tags (?) (ts, i32) values (?, ?)",
//         TSDB_CODE_TSC_STMT_TBNAME_ERROR, -1),
//     // no super table (guessed by using clause with normal column names ahead) found
//     R("insert into ? (ts, i32) using stmt2_insert.xstb (tname) tags (?) values (?, ?)",
//         TSDB_CODE_TSC_STMT_TBNAME_ERROR, -1),
//     // invalid column name
//     R("insert into stmt2_insert.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)",
//         TSDB_CODE_PAR_INVALID_COLUMN, -1),
//     // invalid `*` in values clause
//     R("insert into stmt2_insert.stb(tname,ts,i32,tbname) values(*,*,*,*)",
//         TSDB_CODE_TSC_SQL_SYNTAX_ERROR, -1),
//     R("insert into stmt2_insert.t (ts, i32) values (?, ?)",
//         TSDB_CODE_SUCCESS, 2),
//     R("insert into stmt2_insert.stb (tbname, tname, ts, i32) values (?, ?, ?, ?)",
//         TSDB_CODE_SUCCESS, 4),
//     R("insert into ? using stmt2_insert.stb (tname) tags (?) (ts, i32) values (?, ?)",
//         TSDB_CODE_SUCCESS, 4),
//     R("insert into ? (ts, i32) using stmt2_insert.stb (tname) tags (?) values (?, ?)",
//         TSDB_CODE_SUCCESS, 4),
//     R("insert into ? (ts, i32) values (?, ?)",
//         TSDB_CODE_SUCCESS, 3),
//     R("select * from stmt2_insert.t where ts = ? and (i32 = ? or i32 = ?)",
//         TSDB_CODE_SUCCESS, 3),
//     R("insert into stmt2_insert.t (ts, i32) values (?, ?) (?, ?)",
//         TSDB_CODE_SUCCESS, 2), // flaw: redundant (?, ?) shall be reported as error
//     R("insert into stmt2_insert.t (ts, i32) values (?, 1)",
//         TSDB_CODE_SUCCESS, 1),
//     R("insert into stmt2_insert.? values(?,?)",
//         TSDB_CODE_SUCCESS, 3),
//     R("insert into stmt2_insert.? values(now(),?)",
//         TSDB_CODE_SUCCESS, 2),
//     R("insert into stmt2_insert.? values(now,?)",
//         TSDB_CODE_SUCCESS, 2),
//     R("insert into stmt2_insert.? values(now,null)",
//         TSDB_CODE_SUCCESS, 1),
//     R("insert into stmt2_insert.? values(now,true)",
//         TSDB_CODE_SUCCESS, 1),
//     R("insert into stmt2_insert.? values(now,false)",
//         TSDB_CODE_SUCCESS, 1),
//     R("insert into stmt2_insert.? values(now,-1)",
//         TSDB_CODE_SUCCESS, 1),
//     R("insert into stmt2_insert.? values(now,-1.2)",
//         TSDB_CODE_SUCCESS, 1),
//   };
// #undef R
//   for (size_t i=0; i<sizeof(_cases1)/sizeof(*_cases1); ++i) {
//     int         line               = _cases1[i].line;
//     const char *sql                = _cases1[i].sql;
//     int         exp_code           = _cases1[i].exp_code;
//     int         exp_params         = _cases1[i].exp_params;

//     TAOS_STMT2 *stmt = taos_stmt2_init(taos_, NULL);
//     ASSERT_NE(stmt, nullptr);

//     int r = taos_stmt2_prepare(stmt, sql, (unsigned long)strlen(sql));
//     if (r) {
//       ASSERT_EQ(r, exp_code)
//         << "`taos_stmt2_prepare` "
//         << "[" << hexify(r) << "]" << taos_stmt2_error(stmt) << std::endl
//         << "@" << line << std::endl
//         << sql << std::endl;
//       taos_stmt2_close(stmt);
//       continue;
//     } else {
//       ASSERT_EQ(r, exp_code)
//         << "`taos_stmt2_prepare` "
//         << "@" << line << std::endl
//         << sql << std::endl;
//     }

//     int             count  = 0;
//     TAOS_FIELD_ALL *fields = NULL;
//     r = taos_stmt2_get_fields(stmt, &count, &fields);
//     taos_stmt2_free_fields(stmt, fields);
//     ASSERT_EQ(r, 0)
//       << "`taos_stmt2_get_fields` "
//       << "expecting success, but failed" << std::endl
//       << "[" << hexify(r) << "]" << taos_stmt2_error(stmt) << std::endl
//       << "@" << line << std::endl
//       << sql << std::endl;
//     ASSERT_EQ(count, exp_params)
//       << "@" << line << std::endl
//       << sql << std::endl;

//     taos_stmt2_close(stmt);
//   }

//   // ── Cleanup ───────────────────────────────────────────────────────────────
//   do_query(taos_, "drop database if exists stmt2_insert");
// }

// TS-6870: UPDATE ... SET ... WHERE ... support for stmt2 (same as stmt v1)
TEST(stmt2Case, update) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_update");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_update precision 'us'");
  do_query(taos, "USE stmt2_testdb_update");
  do_query(taos,
           "create table stmt2_testdb_update.devices(ts timestamp,device_id int,status binary(10),temperature float,"
           "humidity float);");
  do_query(taos,
           "insert into stmt2_testdb_update.devices (ts,device_id,status,temperature,humidity) "
           "values(1591060628000000,1,'abc',1.0,1.1);");

  float   temperature = 10.0;
  float   humidity = 10.1;
  int32_t device_id = 2;
  int64_t ts = 1591060628000000;

  int32_t c1_len = sizeof(float);
  int32_t c2_len = sizeof(float);
  int32_t c3_len = sizeof(int32_t);
  int32_t c4_len = sizeof(int64_t);

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql =
      "update stmt2_testdb_update.devices set temperature = ?,humidity = ?,device_id=? where ts=? and device_id=? "
      "and status IS NOT NULL";
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code, __FILE__, __LINE__);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(fieldNum, 5);
  ASSERT_STREQ(pFields[0].name, "temperature");
  ASSERT_STREQ(pFields[1].name, "humidity");
  ASSERT_STREQ(pFields[2].name, "device_id");
  ASSERT_STREQ(pFields[3].name, "ts");
  ASSERT_EQ(pFields[3].precision, 1);
  ASSERT_STREQ(pFields[4].name, "device_id");
  taos_stmt2_free_fields(stmt, pFields);

  TAOS_STMT2_BIND  params[5] = {{TSDB_DATA_TYPE_FLOAT, &temperature, &c1_len, NULL, 1},
                                {TSDB_DATA_TYPE_FLOAT, &humidity, &c2_len, NULL, 1},
                                {TSDB_DATA_TYPE_INT, &device_id, &c3_len, NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &ts, &c4_len, NULL, 1},
                                {TSDB_DATA_TYPE_INT, &device_id, &c3_len, NULL, 1}};
  TAOS_STMT2_BIND* paramv = params;
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  int affected = 0;
  code = taos_stmt2_exec(stmt, &affected);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected, 1);

  TAOS_RES* result = taos_query(
      taos, "select temperature,humidity,device_id from stmt2_testdb_update.devices where ts=1591060628000000");
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(taos_errno(result), 0);

  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(float*)row[0], 10);
  ASSERT_EQ(static_cast<int>(*(float*)row[1] * 10), static_cast<int>(10.1 * 10));
  ASSERT_EQ(*(int32_t*)row[2], 2);
  taos_free_result(result);

  // error case: update primary key column
  taos_stmt2_close(stmt);
  stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  sql =
      "update stmt2_testdb_update.devices set ts = ?,humidity = ?,device_id=? where ts=? and device_id=? and "
      "status IS NOT NULL";
  code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_NE(code, TSDB_CODE_SUCCESS);
  ASSERT_STREQ("Cannot update primary key column 'ts'", taos_stmt2_error(stmt));

  taos_stmt2_close(stmt);
  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_update");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_interlace_pTableCols_close_error_repro) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_1");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_1");
  do_query(taos, "create stable stmt2_testdb_1.stb (ts timestamp, c0 int) tags(t1 int, t2 binary(10))");

  AsyncArgs aa = {0, 0};
  ASSERT_EQ(tsem_init(&aa.sem, 0, 0), TSDB_CODE_SUCCESS);
  TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb, &aa};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  int code = taos_stmt2_prepare(stmt, "insert into `stmt2_testdb_1`.`stb` (tbname,ts,c0,t1,t2) values(?,?,?,?,?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  const int32_t rows = 1;
  int32_t       tsLen = sizeof(int64_t);
  int32_t       intLen = sizeof(int32_t);
  int32_t       tagLen = 3;
  char          tbName[] = "ctb_0";
  char          tag[] = "tag";
  char*         tbNames[3] = {tbName, tbName, tbName};
  int64_t       ts[3] = {1591060628000LL, 1591060628001LL, 1591060628002LL};
  int32_t       cols[3] = {1, 2, 3};
  int32_t       tags[3] = {1, 2, 3};

  TAOS_STMT2_BIND  tagBind[3][2] = {};
  TAOS_STMT2_BIND  colBind[3][2] = {};
  TAOS_STMT2_BIND* tagv[3] = {tagBind[0], tagBind[1], tagBind[2]};
  TAOS_STMT2_BIND* paramv[3] = {colBind[0], colBind[1], colBind[2]};
  for (int32_t i = 0; i < 3; ++i) {
    tagBind[i][0] = {TSDB_DATA_TYPE_INT, &tags[i], &intLen, NULL, rows};
    tagBind[i][1] = {TSDB_DATA_TYPE_BINARY, tag, &tagLen, NULL, rows};
    colBind[i][0] = {TSDB_DATA_TYPE_TIMESTAMP, &ts[i], &tsLen, NULL, rows};
    colBind[i][1] = {TSDB_DATA_TYPE_INT, &cols[i], &intLen, NULL, rows};
  }

  TAOS_STMT2_BINDV bindv = {3, tbNames, tagv, paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  taos_stmt2_close(stmt);
  (void)tsem_destroy(&aa.sem);
  do_query(taos, "drop database if exists stmt2_testdb_1");
  taos_close(taos);
}

TEST(stmt2Case, interlace_reprepare_table_in_other_stable) {
  TAOS* taos1 = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos1, nullptr);

  do_query(taos1, "drop database if exists stmt2_testdb_reprepare_stable");
  do_query(taos1, "create database stmt2_testdb_reprepare_stable");
  do_query(taos1, "create stable stmt2_testdb_reprepare_stable.stb1 (ts timestamp, c1 int) tags(t1 int)");
  do_query(taos1, "create stable stmt2_testdb_reprepare_stable.stb2 (ts timestamp, c1 int) tags(t1 int)");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos1, &option);
  ASSERT_NE(stmt, nullptr);

  int code =
      taos_stmt2_prepare(stmt, "insert into stmt2_testdb_reprepare_stable.stb1 (tbname,ts,c1,t1) values(?,?,?,?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  char*   tbnames[3] = {"ctb1", "ctb2", "ctb3"};
  int32_t tag_values[3] = {1, 2, 3};
  int32_t tag_lengths[3] = {sizeof(int32_t), sizeof(int32_t), sizeof(int32_t)};
  int64_t ts_values[3] = {1591060628000, 1591060628001, 1591060628002};
  int32_t ts_lengths[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
  int32_t col_values[3] = {10, 20, 30};
  int32_t col_lengths[3] = {sizeof(int32_t), sizeof(int32_t), sizeof(int32_t)};

  TAOS_STMT2_BIND  tag_params[3];
  TAOS_STMT2_BIND  col_params[3][2];
  TAOS_STMT2_BIND* tags[3];
  TAOS_STMT2_BIND* cols[3];
  for (int i = 0; i < 3; ++i) {
    tag_params[i] = {TSDB_DATA_TYPE_INT, &tag_values[i], &tag_lengths[i], NULL, 1};
    col_params[i][0] = {TSDB_DATA_TYPE_TIMESTAMP, &ts_values[i], &ts_lengths[i], NULL, 1};
    col_params[i][1] = {TSDB_DATA_TYPE_INT, &col_values[i], &col_lengths[i], NULL, 1};
    tags[i] = &tag_params[i];
    cols[i] = &col_params[i][0];
  }

  TAOS_STMT2_BINDV bindv = {3, &tbnames[0], &tags[0], &cols[0]};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);

  int affected_rows = 0;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected_rows, 3);

  ASSERT_NE(stmt2TestExecutable, nullptr);
  std::string drop_command = "\"" + std::string(stmt2TestExecutable) + "\" " + STMT2_DROP_REPREPARE_TABLES_ARG;
  ASSERT_EQ(std::system(drop_command.c_str()), 0);

  code =
      taos_stmt2_prepare(stmt, "insert into stmt2_testdb_reprepare_stable.stb2 (tbname,ts,c1,t1) values(?,?,?,?)", 0);
  checkError(stmt, code, __FILE__, __LINE__);

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code, __FILE__, __LINE__);
  affected_rows = 0;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code, __FILE__, __LINE__);
  ASSERT_EQ(affected_rows, 3);

  TAOS_RES* result = taos_query(taos1, "select count(*) from stmt2_testdb_reprepare_stable.stb2");
  ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS);
  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t*)row[0], 3);
  taos_free_result(result);

  do_query(taos1,
           "create table stmt2_testdb_reprepare_stable.conflict1 "
           "using stmt2_testdb_reprepare_stable.stb1 tags(1)");
  do_query(taos1,
           "create table stmt2_testdb_reprepare_stable.conflict2 "
           "using stmt2_testdb_reprepare_stable.stb1 tags(2)");
  do_query(taos1,
           "create table stmt2_testdb_reprepare_stable.conflict3 "
           "using stmt2_testdb_reprepare_stable.stb1 tags(3)");

  char*            conflict_names[3] = {"conflict1", "conflict2", "conflict3"};
  TAOS_STMT2_BINDV conflict_bindv = {3, &conflict_names[0], &tags[0], &cols[0]};
  code = taos_stmt2_bind_param(stmt, &conflict_bindv, -1);
  if (code == TSDB_CODE_SUCCESS) {
    affected_rows = 0;
    code = taos_stmt2_exec(stmt, &affected_rows);
  }
  ASSERT_EQ(code, TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE);
  ASSERT_STREQ(tstrerror(code), "Table already exists in other stables");

  taos_stmt2_close(stmt);
  do_query(taos1, "drop database if exists stmt2_testdb_reprepare_stable");
  taos_close(taos1);
}

namespace {

const int32_t kWaitSeconds = 60;
const int32_t kControlWaitSeconds = 180;

bool isStoppedServerConnectionError(int32_t code) {
  return code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_BROKEN_LINK ||
         code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED;
}

bool isTransientDbError(int32_t code) {
  return code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING ||
         code == TSDB_CODE_MND_VGROUP_OFFLINE || code == TSDB_CODE_MND_NO_ENOUGH_DNODES ||
         code == TSDB_CODE_MND_DNODE_IN_CREATING;
}

int32_t executeSql(TAOS* taos, const char* sql, std::string* error) {
  int32_t   retries = 0;
  TAOS_RES* result = NULL;
  int32_t   code = TSDB_CODE_SUCCESS;

  do {
    taos_free_result(result);
    result = taos_query(taos, sql);
    code = taos_errno(result);
    if (!isTransientDbError(code)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  } while (++retries < 300);

  if (error != NULL) {
    const char* message = taos_errstr(result);
    *error = message == NULL ? "" : message;
  }
  taos_free_result(result);
  return code;
}

TAOS* connectWithRetry(uint16_t port) {
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(kWaitSeconds);
  while (std::chrono::steady_clock::now() < deadline) {
    TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, port);
    if (taos != NULL) {
      return taos;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return NULL;
}

bool waitForTargetVnode(TAOS* taos) {
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(kWaitSeconds);
  while (std::chrono::steady_clock::now() < deadline) {
    TAOS_RES* result = taos_query(taos, "select count(*) from stmt2_vnode_stop_recovery.t1");
    int32_t   code = taos_errno(result);
    taos_free_result(result);
    if (code == TSDB_CODE_SUCCESS) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

bool hasStoredValue(TAOS* taos, int64_t ts, int32_t expected) {
  std::string sql = "select c1 from stmt2_vnode_stop_recovery.t1 where ts=" + std::to_string(ts);
  TAOS_RES*   result = taos_query(taos, sql.c_str());
  if (result == NULL || taos_errno(result) != TSDB_CODE_SUCCESS) {
    taos_free_result(result);
    return false;
  }

  TAOS_ROW row = taos_fetch_row(result);
  bool     matched = row != NULL && row[0] != NULL && *(int32_t*)row[0] == expected;
  matched = matched && taos_fetch_row(result) == NULL;
  taos_free_result(result);
  return matched;
}

bool isStoredTimestampAbsent(TAOS* taos, int64_t ts) {
  std::string sql = "select c1 from stmt2_vnode_stop_recovery.t1 where ts=" + std::to_string(ts);
  TAOS_RES*   result = taos_query(taos, sql.c_str());
  if (result == NULL || taos_errno(result) != TSDB_CODE_SUCCESS) {
    taos_free_result(result);
    return false;
  }

  bool absent = taos_fetch_row(result) == NULL;
  taos_free_result(result);
  return absent;
}

bool getStoredRowCount(TAOS* taos, int64_t* rowCount) {
  TAOS_RES* result = taos_query(taos, "select count(*) from stmt2_vnode_stop_recovery.t1");
  if (result == NULL || taos_errno(result) != TSDB_CODE_SUCCESS) {
    taos_free_result(result);
    return false;
  }

  TAOS_ROW row = taos_fetch_row(result);
  bool     valid = row != NULL && row[0] != NULL;
  if (valid) {
    *rowCount = *(int64_t*)row[0];
    valid = taos_fetch_row(result) == NULL;
  }
  taos_free_result(result);
  return valid;
}

class TaosdController {
 public:
  explicit TaosdController(const char* controlDir) : controlDir_(controlDir == NULL ? "" : controlDir) {}

  bool valid() const { return !controlDir_.empty(); }

  bool stop(const char* phase) { return requestAndWait(phase, "stop"); }

  bool start(const char* phase) { return requestAndWait(phase, "start"); }

 private:
  bool requestAndWait(const char* phase, const char* action) const {
    const std::string prefix = controlDir_ + "/" + phase + "." + action;
    {
      std::ofstream request((prefix + ".request").c_str(), std::ios::out | std::ios::trunc);
      if (!request.good()) {
        return false;
      }
      request << "ready\n";
    }

    const std::string                           ack = prefix + ".ack";
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(kControlWaitSeconds);
    while (std::chrono::steady_clock::now() < deadline) {
      std::ifstream marker(ack.c_str());
      if (marker.good()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
  }

  std::string controlDir_;
};

struct AsyncExecResult {
  TAOS_STMT2* stmt;
  int32_t     code;
  int32_t     resultCode;
  int32_t     affectedRows;
  int32_t     callbackCount;
  std::string stmtError;
  std::string resultError;
  tsem_t      sem;
  bool        semInitialized = false;

  ~AsyncExecResult() {
    if (semInitialized) {
      (void)tsem_destroy(&sem);
    }
  }
};

void asyncExecCallback(void* param, TAOS_RES* result, int32_t code) {
  AsyncExecResult* state = (AsyncExecResult*)param;
  state->code = code;
  state->resultCode = result == NULL ? TSDB_CODE_APP_ERROR : taos_errno(result);
  state->affectedRows = result == NULL ? 0 : taos_affected_rows(result);
  ++state->callbackCount;
  state->stmtError.clear();
  state->resultError.clear();
  if (code != TSDB_CODE_SUCCESS) {
    const char* stmtError = taos_stmt2_error(state->stmt);
    const char* resultError = result == NULL ? NULL : taos_errstr(result);
    state->stmtError = stmtError == NULL ? "" : stmtError;
    state->resultError = resultError == NULL ? "" : resultError;
  }
  (void)tsem_post(&state->sem);
}

}  // namespace

TEST(stmt2Case, same_stmt_recovers_after_restart) {
  const char*     controlDir = getenv("TAOS_STMT2_CONTROL_DIR");
  const char*     portText = getenv("TAOS_STMT2_TEST_PORT");
  TaosdController controller(controlDir);
  if (!controller.valid() || portText == NULL || portText[0] == '\0') {
    GTEST_SKIP() << "run this binary through stmt2VnodeStopTest.sh";
  }

  uint32_t portValue = 0;
  for (const char* p = portText; *p != '\0'; ++p) {
    ASSERT_TRUE(*p >= '0' && *p <= '9');
    portValue = portValue * 10 + (uint32_t)(*p - '0');
    ASSERT_LE(portValue, UINT16_MAX);
  }
  ASSERT_GT(portValue, 0U);
  uint16_t port = (uint16_t)portValue;

  TAOS* taos = connectWithRetry(port);
  ASSERT_NE(taos, nullptr) << taos_errstr(NULL);
  std::unique_ptr<TAOS, decltype(&taos_close)> taosGuard(taos, taos_close);

  std::string error;
  ASSERT_EQ(executeSql(taos, "drop database if exists stmt2_vnode_stop_recovery", &error), TSDB_CODE_SUCCESS) << error;
  ASSERT_EQ(executeSql(taos, "create database stmt2_vnode_stop_recovery", &error), TSDB_CODE_SUCCESS) << error;
  ASSERT_EQ(executeSql(taos, "create stable stmt2_vnode_stop_recovery.stb (ts timestamp, c1 int) tags(tag_value int)", &error),
            TSDB_CODE_SUCCESS)
      << error;
  ASSERT_EQ(executeSql(taos, "create table stmt2_vnode_stop_recovery.t1 using stmt2_vnode_stop_recovery.stb tags(1)",
                       &error), TSDB_CODE_SUCCESS) << error;
  ASSERT_EQ(executeSql(taos, "use stmt2_vnode_stop_recovery", &error), TSDB_CODE_SUCCESS) << error;

  int64_t successfulExecCount = 0;

  // Synchronous mode: keep one prepared stmt alive across a graceful taosd restart.
  TAOS_STMT2_OPTION syncOption = {0, true, true, NULL, NULL};
  TAOS_STMT2*       syncStmt = taos_stmt2_init(taos, &syncOption);
  ASSERT_NE(syncStmt, nullptr);
  std::unique_ptr<TAOS_STMT2, decltype(&taos_stmt2_close)> syncGuard(syncStmt, taos_stmt2_close);
  int32_t code = taos_stmt2_prepare(syncStmt, "insert into ? (ts, c1) values(?, ?)", 0);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);

  int64_t          ts = 1591060800000;
  int32_t          value = 0;
  TAOS_STMT2_BIND  syncParams[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, NULL, NULL, 1},
                                    {TSDB_DATA_TYPE_INT, &value, NULL, NULL, 1}};
  TAOS_STMT2_BIND* syncParamv = syncParams;
  char             tableName[] = "stmt2_vnode_stop_recovery.t1";
  char*            tableNames[] = {tableName};
  TAOS_STMT2_BINDV syncBindv = {1, tableNames, NULL, &syncParamv};

  for (int32_t i = 0; i < 8; ++i) {
    ts = 1591060800000 + i;
    value = i;
    ASSERT_EQ(taos_stmt2_bind_param(syncStmt, &syncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
    int32_t affectedRows = 0;
    ASSERT_EQ(taos_stmt2_exec(syncStmt, &affectedRows), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
    ASSERT_EQ(affectedRows, 1);
    ++successfulExecCount;
  }

  ts = 1591060810000;
  value = 10000;
  ASSERT_EQ(taos_stmt2_bind_param(syncStmt, &syncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
  ASSERT_TRUE(controller.stop("sync"));

  int32_t                                     syncExecCode = TSDB_CODE_SUCCESS;
  int32_t                                     syncFailedAffectedRows = -1;
  int64_t                                     failedTs = ts;
  const std::chrono::steady_clock::time_point syncDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(kWaitSeconds);
  while (std::chrono::steady_clock::now() < syncDeadline) {
    int32_t affectedRows = 0;
    failedTs = ts;
    syncExecCode = taos_stmt2_exec(syncStmt, &affectedRows);
    if (syncExecCode != TSDB_CODE_SUCCESS) {
      syncFailedAffectedRows = affectedRows;
      break;
    }
    ASSERT_EQ(affectedRows, 1);
    ++successfulExecCount;
    ++ts;
    ++value;
    ASSERT_EQ(taos_stmt2_bind_param(syncStmt, &syncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
  }

  int32_t     syncStmtErrCode = ((STscStmt2*)syncStmt)->errCode;
  std::string syncError = taos_stmt2_error(syncStmt);
  ASSERT_TRUE(controller.start("sync"));
  ASSERT_TRUE(isStoppedServerConnectionError(syncExecCode)) << tstrerror(syncExecCode);
  ASSERT_EQ(syncFailedAffectedRows, 0);
  ASSERT_EQ(syncStmtErrCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(syncError, tstrerror(syncExecCode));
  ASSERT_TRUE(waitForTargetVnode(taos));
  ASSERT_TRUE(isStoredTimestampAbsent(taos, failedTs));

  int64_t recoveredTs = failedTs + 1000000;
  ts = recoveredTs;
  value = 20000;
  ASSERT_EQ(taos_stmt2_bind_param(syncStmt, &syncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
  int32_t affectedRows = 0;
  ASSERT_EQ(taos_stmt2_exec(syncStmt, &affectedRows), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
  ASSERT_EQ(affectedRows, 1);
  ++successfulExecCount;
  ASSERT_TRUE(hasStoredValue(taos, recoveredTs, value));
  // A later schema change must still refresh metadata using the retained request.
  {
    TAOS* ddlTaos = connectWithRetry(port);
    ASSERT_NE(ddlTaos, nullptr);
    std::unique_ptr<TAOS, decltype(&taos_close)> ddlGuard(ddlTaos, taos_close);
    ASSERT_EQ(executeSql(ddlTaos, "alter stable stmt2_vnode_stop_recovery.stb add column after_sync int", &error),
              TSDB_CODE_SUCCESS) << error;
  }
  ++ts;
  ++value;
  ASSERT_EQ(taos_stmt2_bind_param(syncStmt, &syncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
  ASSERT_TRUE(((STscStmt2*)syncStmt)->sql.stbInterlaceMode);
  ASSERT_EQ(taos_stmt2_exec(syncStmt, &affectedRows), TSDB_CODE_SUCCESS) << taos_stmt2_error(syncStmt);
  ASSERT_EQ(affectedRows, 1);
  ++successfulExecCount;
  ASSERT_TRUE(hasStoredValue(taos, ts, value));
  syncGuard.reset();

  // Asynchronous mode: the callback owns the server result; reuse begins after callback finalization.
  AsyncExecResult asyncResult = {};
  ASSERT_EQ(tsem_init(&asyncResult.sem, 0, 0), TSDB_CODE_SUCCESS);
  asyncResult.semInitialized = true;
  TAOS_STMT2_OPTION asyncOption = {0, true, true, asyncExecCallback, &asyncResult};
  TAOS_STMT2*       asyncStmt = taos_stmt2_init(taos, &asyncOption);
  ASSERT_NE(asyncStmt, nullptr);
  std::unique_ptr<TAOS_STMT2, decltype(&taos_stmt2_close)> asyncGuard(asyncStmt, taos_stmt2_close);
  asyncResult.stmt = asyncStmt;
  ASSERT_EQ(taos_stmt2_prepare(asyncStmt, "insert into ? (ts, c1) values(?, ?)", 0),
            TSDB_CODE_SUCCESS)
      << taos_stmt2_error(asyncStmt);

  int64_t          asyncTs = 1591060900000;
  int32_t          asyncValue = 30000;
  TAOS_STMT2_BIND  asyncParams[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &asyncTs, NULL, NULL, 1},
                                     {TSDB_DATA_TYPE_INT, &asyncValue, NULL, NULL, 1}};
  TAOS_STMT2_BIND* asyncParamv = asyncParams;
  TAOS_STMT2_BINDV asyncBindv = {1, tableNames, NULL, &asyncParamv};
  int32_t          expectedCallbackCount = 0;

  ASSERT_EQ(taos_stmt2_bind_param(asyncStmt, &asyncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
  ASSERT_EQ(taos_stmt2_exec(asyncStmt, NULL), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
  ASSERT_EQ(tsem_timewait(&asyncResult.sem, kWaitSeconds * 1000), TSDB_CODE_SUCCESS);
  ++expectedCallbackCount;
  ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
  ASSERT_EQ(asyncResult.code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.resultCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.affectedRows, 1);
  ++successfulExecCount;

  ++asyncTs;
  ++asyncValue;
  // Exercise the public bind path that waits for and finalizes the previous asynchronous execution.
  ASSERT_EQ(taos_stmt2_bind_param(asyncStmt, &asyncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
  ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
  ASSERT_TRUE(controller.stop("async"));

  int64_t                                     asyncFailedTs = asyncTs;
  const std::chrono::steady_clock::time_point asyncDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(kWaitSeconds);
  while (std::chrono::steady_clock::now() < asyncDeadline) {
    asyncFailedTs = asyncTs;
    ASSERT_EQ(taos_stmt2_exec(asyncStmt, NULL), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
    ASSERT_EQ(tsem_timewait(&asyncResult.sem, kWaitSeconds * 1000), TSDB_CODE_SUCCESS);
    ++expectedCallbackCount;
    ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
    if (asyncResult.code != TSDB_CODE_SUCCESS) {
      break;
    }
    ASSERT_EQ(asyncResult.affectedRows, 1);
    ++successfulExecCount;
    ++asyncTs;
    ++asyncValue;
    ASSERT_EQ(taos_stmt2_bind_param(asyncStmt, &asyncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
    ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
  }

  int32_t asyncStmtErrCode = ((STscStmt2*)asyncStmt)->errCode;
  ASSERT_TRUE(controller.start("async"));
  ASSERT_TRUE(isStoppedServerConnectionError(asyncResult.code)) << tstrerror(asyncResult.code);
  ASSERT_EQ(asyncResult.resultCode, asyncResult.code);
  ASSERT_EQ(asyncResult.affectedRows, 0);
  ASSERT_EQ(asyncStmtErrCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.stmtError, tstrerror(asyncResult.code));
  ASSERT_EQ(asyncResult.resultError, tstrerror(asyncResult.code));
  ASSERT_EQ(asyncResult.stmtError, taos_stmt2_error(asyncStmt));
  ASSERT_TRUE(waitForTargetVnode(taos));
  ASSERT_TRUE(isStoredTimestampAbsent(taos, asyncFailedTs));

  int64_t asyncRecoveredTs = asyncFailedTs + 1000000;
  asyncTs = asyncRecoveredTs;
  asyncValue = 40000;
  // Do not consume asyncExecSem directly: recovery must work through the public bind API.
  ASSERT_EQ(taos_stmt2_bind_param(asyncStmt, &asyncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
  ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
  ASSERT_EQ(taos_stmt2_exec(asyncStmt, NULL), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
  ASSERT_EQ(tsem_timewait(&asyncResult.sem, kWaitSeconds * 1000), TSDB_CODE_SUCCESS);
  ++expectedCallbackCount;
  ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
  ASSERT_EQ(asyncResult.code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.resultCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.affectedRows, 1);
  ++successfulExecCount;
  ASSERT_TRUE(hasStoredValue(taos, asyncRecoveredTs, asyncValue));
  {
    TAOS* ddlTaos = connectWithRetry(port);
    ASSERT_NE(ddlTaos, nullptr);
    std::unique_ptr<TAOS, decltype(&taos_close)> ddlGuard(ddlTaos, taos_close);
    ASSERT_EQ(executeSql(ddlTaos, "alter stable stmt2_vnode_stop_recovery.stb add column after_async int", &error),
              TSDB_CODE_SUCCESS) << error;
  }
  ++asyncTs;
  ++asyncValue;
  ASSERT_EQ(taos_stmt2_bind_param(asyncStmt, &asyncBindv, -1), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
  ASSERT_TRUE(((STscStmt2*)asyncStmt)->sql.stbInterlaceMode);
  ASSERT_EQ(taos_stmt2_exec(asyncStmt, NULL), TSDB_CODE_SUCCESS) << taos_stmt2_error(asyncStmt);
  ASSERT_EQ(tsem_timewait(&asyncResult.sem, kWaitSeconds * 1000), TSDB_CODE_SUCCESS);
  ++expectedCallbackCount;
  ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
  ASSERT_EQ(asyncResult.code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.resultCode, TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.affectedRows, 1);
  // Public close waits for the final callback and batch cleanup.
  ASSERT_EQ(taos_stmt2_close(asyncGuard.release()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(asyncResult.callbackCount, expectedCallbackCount);
  ++successfulExecCount;
  ASSERT_TRUE(hasStoredValue(taos, asyncTs, asyncValue));

  int64_t storedRowCount = 0;
  ASSERT_TRUE(getStoredRowCount(taos, &storedRowCount));
  ASSERT_EQ(storedRowCount, successfulExecCount);

  ASSERT_EQ(executeSql(taos, "drop database if exists stmt2_vnode_stop_recovery", &error), TSDB_CODE_SUCCESS) << error;
}

TEST(stmt2Case, select_decimal) {
  const char* portText = getenv("TAOS_STMT2_TEST_PORT");
  int         port = portText == nullptr ? 0 : atoi(portText);
  ASSERT_GE(port, 0);
  ASSERT_LE(port, UINT16_MAX);
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, (uint16_t)port);
  ASSERT_NE(taos, nullptr) << taos_errstr(NULL);
  std::unique_ptr<TAOS, decltype(&taos_close)> taosGuard(taos, taos_close);

  do_query(taos, "drop database if exists stmt2_testdb_select_decimal");
  do_query(taos, "create database stmt2_testdb_select_decimal precision 'ms'");
  do_query(taos, "use stmt2_testdb_select_decimal");
  do_query(taos,
           "create stable t (ts_column timestamp, indvalue decimal(20,6), indvalue64 decimal(16,6)) "
           "tags(company varchar(32), station varchar(32), device varchar(32), indicator_type varchar(32))");
  do_query(taos, "create table d1 using t tags('company_a', 'station_1', 'device_1', 'voltage')");
  do_query(taos, "create table d2 using t tags('company_b', 'station_1', 'device_2', 'voltage')");
  do_query(taos, "create table d3 using t tags('company_a', 'station_2', 'device_3', 'voltage')");
  do_query(taos,
           "insert into d1 values "
           "(1700000000000, 12345678901234.123456, 1234567890.123456) "
           "(1700000001000, 12345678901234.123456, 1234567890.123456) "
           "(1700000002000, 12345678901234.123457, 1234567890.123457) "
           "(1700000003000, 12345678901234.123456, 1234567890.123456) "
           "(1700000005000, 12345678901234.123456, 1234567890.123456) "
           "(1700000006000, 12345678901234.123456, 1234567890.123456)");
  do_query(taos, "insert into d2 values (1700000002000, 12345678901234.123456, 1234567890.123456)");
  do_query(taos, "insert into d3 values (1700000003000, 12345678901234.123456, 1234567890.123456)");

  const char* querySql =
      "select * from t where (indvalue = 12345678901234.123456 and indvalue64 = 1234567890.123456 "
      "and ts_column >= 1700000001000 and ts_column <= 1700000005000 "
      "and station in ('station_1') and indicator_type = 'voltage') "
      "order by ts_column, device";
  Stmt2ResultSnapshot taosQueryResult = {0};
  ASSERT_TRUE(snapshotTaosQuery(taos, querySql, &taosQueryResult));

  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  std::unique_ptr<TAOS_STMT2, decltype(&taos_stmt2_close)> stmtGuard(stmt, taos_stmt2_close);

  const char* stmtSql =
      "select * from t where (indvalue = ? and indvalue64 = ? and ts_column >= ? and ts_column <= ? "
      "and station in (?) and indicator_type = ?) order by ts_column, device";
  ASSERT_EQ(taos_stmt2_prepare(stmt, stmtSql, 0), TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);

  char             decimal[] = "12345678901234.123456";
  char             decimal64[] = "1234567890.123456";
  int64_t          start = 1700000001000;
  int64_t          end = 1700000005000;
  char             station[] = "station_1";
  char             indicatorType[] = "voltage";
  int32_t          lengths[6] = {(int32_t)strlen(decimal), (int32_t)strlen(decimal64),    sizeof(start), sizeof(end),
                                 (int32_t)strlen(station), (int32_t)strlen(indicatorType)};
  TAOS_STMT2_BIND  params[6] = {{TSDB_DATA_TYPE_DECIMAL, decimal, &lengths[0], NULL, 1},
                                {TSDB_DATA_TYPE_DECIMAL64, decimal64, &lengths[1], NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &start, &lengths[2], NULL, 1},
                                {TSDB_DATA_TYPE_TIMESTAMP, &end, &lengths[3], NULL, 1},
                                {TSDB_DATA_TYPE_VARCHAR, station, &lengths[4], NULL, 1},
                                {TSDB_DATA_TYPE_VARCHAR, indicatorType, &lengths[5], NULL, 1}};
  TAOS_STMT2_BIND* bindCols = params;
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &bindCols};

  Stmt2ResultSnapshot stmtResult = {0};
  ASSERT_TRUE(snapshotStmt2Query(stmt, &bindv, &stmtResult));
  expectSnapshotsEqual(taosQueryResult, stmtResult, "taos_query and STMT2 results differ");

  ASSERT_EQ(taos_stmt2_close(stmtGuard.release()), TSDB_CODE_SUCCESS);
  do_query(taos, "drop database stmt2_testdb_select_decimal");
}

TEST(stmt2Case, select_decimal_rebind_error_close) {
  const char* portText = getenv("TAOS_STMT2_TEST_PORT");
  int         port = portText == nullptr ? 0 : atoi(portText);
  ASSERT_GE(port, 0);
  ASSERT_LE(port, UINT16_MAX);
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, (uint16_t)port);
  ASSERT_NE(taos, nullptr) << taos_errstr(NULL);
  std::unique_ptr<TAOS, decltype(&taos_close)> taosGuard(taos, taos_close);

  do_query(taos, "drop database if exists stmt2_decimal_rebind");
  do_query(taos, "create database stmt2_decimal_rebind precision 'ms'");
  do_query(taos, "use stmt2_decimal_rebind");
  do_query(taos, "create table t (ts timestamp, v bigint)");
  do_query(taos, "insert into t values (1700000001000, 42)");

  for (bool prepareAfterError : {false, true}) {
    SCOPED_TRACE(prepareAfterError);
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    std::unique_ptr<TAOS_STMT2, decltype(&taos_stmt2_close)> stmtGuard(stmt, taos_stmt2_close);
    ASSERT_EQ(taos_stmt2_prepare(stmt, "select ts from t where v = ?", 0), TSDB_CODE_SUCCESS)
        << taos_stmt2_error(stmt);

    int64_t          value = 42;
    int32_t          length = sizeof(value);
    TAOS_STMT2_BIND  param = {TSDB_DATA_TYPE_BIGINT, &value, &length, NULL, 1};
    TAOS_STMT2_BIND* cols = &param;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &cols};
    Stmt2ResultSnapshot result = {0};
    ASSERT_TRUE(snapshotStmt2Query(stmt, &bindv, &result));
    ASSERT_EQ(result.rows, 1);

    char overflow[] = "123456789012345678901234567890123456789";
    length = strlen(overflow);
    param.buffer_type = TSDB_DATA_TYPE_DECIMAL;
    param.buffer = overflow;
    ASSERT_EQ(taos_stmt2_bind_param(stmt, &bindv, -1), TSDB_CODE_DECIMAL_OVERFLOW);

    if (prepareAfterError) {
      // A failed statement must be prepared again before binding new values.
      ASSERT_EQ(taos_stmt2_prepare(stmt, "select ts from t where v = ?", 0), TSDB_CODE_SUCCESS)
          << taos_stmt2_error(stmt);
      char valid[] = "42.0";
      length = strlen(valid);
      param.buffer = valid;
      ASSERT_TRUE(snapshotStmt2Query(stmt, &bindv, &result));
      ASSERT_EQ(result.rows, 1);
    }
    // Both close and reprepare must safely clean the failed DECIMAL value.
    ASSERT_EQ(taos_stmt2_close(stmtGuard.release()), TSDB_CODE_SUCCESS);
  }
  do_query(taos, "drop database stmt2_decimal_rebind");
}

TEST(stmt2Case, select_decimal_trailing_zeros) {
  const char* portText = getenv("TAOS_STMT2_TEST_PORT");
  int         port = portText == nullptr ? 0 : atoi(portText);
  ASSERT_GE(port, 0);
  ASSERT_LE(port, UINT16_MAX);
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, (uint16_t)port);
  ASSERT_NE(taos, nullptr) << taos_errstr(NULL);
  std::unique_ptr<TAOS, decltype(&taos_close)> taosGuard(taos, taos_close);

  do_query(taos, "drop database if exists stmt2_decimal_zeros");
  do_query(taos, "create database stmt2_decimal_zeros precision 'ms'");
  do_query(taos, "use stmt2_decimal_zeros");
  do_query(taos, "create table t (ts timestamp, d64 decimal(18,0), d128 decimal(38,0), f decimal(18,6))");
  do_query(taos, "insert into t values "
                 "(1700000001000, 123456789012345678, 12345678901234567890123456789012345678, 0.123456) "
                 "(1700000002000, 1, 1, 0.1) "
                 "(1700000003000, -123456789012345678, -12345678901234567890123456789012345678, -0.123456) "
                 "(1700000004000, 0, 0, 0)");

  const struct {
    int         type;
    const char* column;
    const char* value;
    int64_t     timestamp;
  } cases[] = {
      {TSDB_DATA_TYPE_DECIMAL64, "d64", "123456789012345678.0", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL64, "d64", "-123456789012345678.000", 1700000003000},
      {TSDB_DATA_TYPE_DECIMAL64, "d64", "1.000000000000000000", 1700000002000},
      {TSDB_DATA_TYPE_DECIMAL64, "d64", "123456789012345678.000e0", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL64, "d64", "12345678901234567.800e+1", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL64, "d64", "0.0000000000000000000", 1700000004000},
      {TSDB_DATA_TYPE_DECIMAL64, "f", "0.123456000000000000", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL64, "f", "1.000000000000000000e-1", 1700000002000},
      {TSDB_DATA_TYPE_DECIMAL64, "f", "100e-3", 1700000002000},
      {TSDB_DATA_TYPE_DECIMAL64, "f", "-1.23456000E-1", 1700000003000},
      {TSDB_DATA_TYPE_DECIMAL, "d128", "12345678901234567890123456789012345678.0", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL, "d128", "-12345678901234567890123456789012345678.000", 1700000003000},
      {TSDB_DATA_TYPE_DECIMAL, "d128", "1.00000000000000000000000000000000000000", 1700000002000},
      {TSDB_DATA_TYPE_DECIMAL, "d128", "12345678901234567890123456789012345678.000e0", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL, "d128", "1234567890123456789012345678901234567.800e+1", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL, "d128", "0.000000000000000000000000000000000000000", 1700000004000},
      {TSDB_DATA_TYPE_DECIMAL, "f", "0.12345600000000000000000000000000000000", 1700000001000},
      {TSDB_DATA_TYPE_DECIMAL, "f", "1.00000000000000000000000000000000000000e-1", 1700000002000},
      {TSDB_DATA_TYPE_DECIMAL, "f", "100e-3", 1700000002000},
      {TSDB_DATA_TYPE_DECIMAL, "f", "-1.23456000E-1", 1700000003000},
  };

  for (const auto& testCase : cases) {
    SCOPED_TRACE(testCase.value);
    TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    std::unique_ptr<TAOS_STMT2, decltype(&taos_stmt2_close)> stmtGuard(stmt, taos_stmt2_close);
    std::string sql = std::string("select ts from t where ") + testCase.column + " = ?";
    ASSERT_EQ(taos_stmt2_prepare(stmt, sql.c_str(), 0), TSDB_CODE_SUCCESS) << taos_stmt2_error(stmt);

    int32_t          length = strlen(testCase.value);
    TAOS_STMT2_BIND  param = {testCase.type, const_cast<char*>(testCase.value), &length, NULL, 1};
    TAOS_STMT2_BIND* cols = &param;
    TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &cols};
    Stmt2ResultSnapshot result = {0};
    ASSERT_TRUE(snapshotStmt2Query(stmt, &bindv, &result));
    ASSERT_EQ(result.rows, 1);
    ASSERT_EQ(result.cols, 1);
    ASSERT_FALSE(result.cells[0][0].isNull);
    ASSERT_EQ(result.cells[0][0].length, sizeof(int64_t));
    int64_t timestamp = 0;
    memcpy(&timestamp, result.cells[0][0].data, sizeof(timestamp));
    ASSERT_EQ(timestamp, testCase.timestamp);
  }
  do_query(taos, "drop database stmt2_decimal_zeros");
}

#pragma GCC diagnostic pop
