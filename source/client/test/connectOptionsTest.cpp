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
#include <iostream>
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"
#include <time.h>
#include <osSleep.h> 

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "executor.h"
#include "taos.h"
#include "clientInt.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

TAOS* getConnWithGlobalOption(const char *tz){
  int code = taos_options(TSDB_OPTION_TIMEZONE, tz);
  ASSERT(code ==  0);
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != nullptr);
  return pConn;
}

TAOS* getConnWithOption(const char *tz){
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != nullptr);
  if (tz != NULL){
    int code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, tz);
    ASSERT(code == 0);
  }
  return pConn;
}

void execQuery(TAOS* pConn, const char *sql){
  TAOS_RES* pRes = taos_query(pConn, sql);
  int       code = taos_errno(pRes);
  while (code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    TAOS_RES* pRes = taos_query(pConn, sql);
    code = taos_errno(pRes);
  }
  ASSERT(code == TSDB_CODE_SUCCESS);
  taos_free_result(pRes);
}

void execQueryFail(TAOS* pConn, const char *sql){
  printf("execQueryFail: %s\n", sql);
  TAOS_RES* pRes = taos_query(pConn, sql);
#ifndef WINDOWS
  ASSERT(taos_errno(pRes) != TSDB_CODE_SUCCESS);
#endif
  taos_free_result(pRes);
}

void checkRows(TAOS* pConn, const char *sql, int32_t expectedRows){
  printf("checkRows sql:%s,rows:%d\n", sql, expectedRows);
  TAOS_RES* pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == TSDB_CODE_SUCCESS);
  TAOS_ROW    pRow = NULL;
  int rows =  0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT(rows == expectedRows);
  taos_free_result(pRes);
}

void check_timezone(TAOS* pConn, const char *sql, const char* tz){
  TAOS_RES *pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    if (strcmp((const char*)row[0], "timezone") == 0){
      ASSERT(strstr((const char*)row[1], tz) != NULL);
    }
  }
  taos_free_result(pRes);
}

void check_sql_result_partial(TAOS* pConn, const char *sql, const char* result){
  TAOS_RES *pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    ASSERT(strstr((const char*)row[0], result) != NULL);
  }
  taos_free_result(pRes);
}

int64_t get_sql_result(TAOS* pConn, const char *sql){
  int64_t ts = 0;
  TAOS_RES *pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    ts = *(int64_t*)row[0];
  }
  taos_free_result(pRes);
  return ts;
}

void check_sql_result(TAOS* pConn, const char *sql, const char* result){
  printf("check_sql_result sql:%s,result:%s\n", sql, result);
  TAOS_RES *pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes)) != NULL) {
#ifndef WINDOWS
    ASSERT (memcmp((const char*)row[0], result, strlen(result)) == 0);
#endif
  }
  taos_free_result(pRes);
}

void check_sql_result_integer(TAOS* pConn, const char *sql, int64_t result){
  printf("check_sql_result_integer sql:%s,result:%ld\n", sql, result);
  TAOS_RES *pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes)) != NULL) {
#ifndef WINDOWS
    ASSERT (*(int64_t*)row[0] == result);
#endif
  }
  taos_free_result(pRes);
}

void check_set_timezone(TAOS* optionFunc(const char *tz)){
  {
    TAOS* pConn = optionFunc("UTC-8");  // Asia/Shanghai timezone
    check_timezone(pConn, "show local variables", "UTC-8");

    execQuery(pConn, "drop database if exists db1");
    execQuery(pConn, "create database db1");
    execQuery(pConn, "create table db1.t1 (ts timestamp, v int)");

    execQuery(pConn, "insert into db1.t1 values('2023-09-16 17:00:00', 1)");
    checkRows(pConn, "select * from db1.t1 where ts == '2023-09-16 17:00:00'", 1);

    taos_close(pConn);
  }

  {
    TAOS* pConn = optionFunc("UTC+8");
    check_timezone(pConn, "show local variables", "UTC+8");
    checkRows(pConn, "select * from db1.t1 where ts == '2023-09-16 01:00:00'", 1);
    execQuery(pConn, "insert into db1.t1 values('2023-09-16 17:00:01', 1)");

    taos_close(pConn);
  }

  {
    TAOS* pConn = optionFunc("UTC+0");
    check_timezone(pConn, "show local variables", "UTC+0");
    checkRows(pConn, "select * from db1.t1 where ts == '2023-09-16 09:00:00'", 1);
    checkRows(pConn, "select * from db1.t1 where ts == '2023-09-17 01:00:01'", 1);

    taos_close(pConn);
  }
}

#define CHECK_TAOS_OPTION_POINTER(taos, option, isnull) \
  {                                                     \
    STscObj* pObj = acquireTscObj(*(int64_t*)taos);     \
    ASSERT(pObj != nullptr);                            \
    if (isnull) {                                       \
      ASSERT(pObj->optionInfo.option == nullptr);       \
    } else {                                            \
      ASSERT(pObj->optionInfo.option != nullptr);       \
    }                                                   \
  }

#define CHECK_TAOS_OPTION_APP(taos, option, val)       \
  {                                                    \
    STscObj* pObj = acquireTscObj(*(int64_t*)taos);    \
    ASSERT(pObj != nullptr);                           \
    ASSERT(strcmp(pObj->optionInfo.option, val) == 0); \
  }

#define CHECK_TAOS_OPTION_IP_ERROR(taos, option, val) \
  {                                                   \
    STscObj* pObj = acquireTscObj(*(int64_t*)taos);   \
    ASSERT(pObj != nullptr);                          \
    ASSERT(pObj->optionInfo.option == val);           \
  }

#define CHECK_TAOS_OPTION_IP(taos, option, val)     \
  {                                                 \
    STscObj* pObj = acquireTscObj(*(int64_t*)taos); \
    ASSERT(pObj != nullptr);                        \
    char ip[TD_IP_LEN] = {0};                       \
    taosInetNtoa(ip, pObj->optionInfo.option);        \
    ASSERT(strcmp(ip, val) == 0);                   \
  }

static int32_t checkUserIp(TAOS *taos, const char *ip) {
  TAOS_RES *pRes = taos_query(taos, "select user_ip from performance_schema.perf_connections");

  ASSERT(taos_errno(pRes) == 0);
    
  TAOS_ROW row = NULL;
  bool     found = false;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    if (strcmp((const char *)row[0], ip) == 0) {
      found = true;
      break;
    }
  }
  if (found == false) {
    ASSERT(0); 
  }
 
  taos_free_result(pRes);
  
  ASSERT(1);
  return 0;
}

TEST(connectionCase, setConnectionOption_Test) {
  int32_t code = taos_options_connection(NULL, TSDB_OPTION_CONNECTION_CHARSET, NULL);
  ASSERT(code != 0);
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  code = taos_options_connection(pConn, TSDB_MAX_OPTIONS_CONNECTION, NULL);
  ASSERT(code != 0);

  // test charset
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CHARSET, "");
  ASSERT(code != 0);

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CHARSET, NULL);
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, charsetCxt, true);

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CHARSET, "Asia/Shanghai");
  ASSERT(code != 0);

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CHARSET, "gbk");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, charsetCxt, false);

#ifndef WINDOWS
  // test timezone
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, "");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, timezone, false);
  check_sql_result(pConn, "select timezone()", "UTC (UTC, +0000)");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, NULL);
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, timezone, true);
  check_sql_result(pConn, "select timezone()", "Asia/Shanghai (CST, +0800)");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, "UTC");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, timezone, false);
  check_sql_result(pConn, "select timezone()", "UTC (UTC, +0000)");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, "Asia/Kolkata");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, timezone, false);
  check_sql_result(pConn, "select timezone()", "Asia/Kolkata (IST, +0530)");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_TIMEZONE, "adbc");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, timezone, false);
  check_sql_result(pConn, "select timezone()", "adbc (UTC, +0000)");
#endif

  // test user APP
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_APP, "");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, userApp, "");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_APP, NULL);
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, userApp, "");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_APP, "aaaaaaaaaaaaaaaaaaaaaabbbbbbb");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, userApp, "aaaaaaaaaaaaaaaaaaaaaab");

  // test connector info
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CONNECTOR_INFO, "");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, cInfo, "");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CONNECTOR_INFO, NULL);
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, cInfo, "");

  const char* str255 = "TDengine_ConnectOptionsTest_255_Character_String_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz_This_is_a_test_string_for_connection_options_testing_in_TDengine_database_system_with_exactly_255_characters_including_alphanumeric_and_special";
  const char* str258 = "TDengine_ConnectOptionsTest_255_Character_String_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz_This_is_a_test_string_for_connection_options_testing_in_TDengine_database_system_with_exactly_255_characters_including_alphanumeric_and_specialaaa";
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CONNECTOR_INFO, 
    str258);
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, cInfo, str255);

  // test user IP
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, "");
  //ASSERT(code != 0); // add dual later
  CHECK_TAOS_OPTION_IP_ERROR(pConn, userIp, INADDR_NONE);

  checkUserIp(pConn, "");
  
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, NULL);
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_IP_ERROR(pConn, userIp, INADDR_NONE);

  checkUserIp(pConn, "");
  

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, "aaaaaaaaaaaaaaaaaaaaaabbbbbbb");
  //ASSERT(code != 0); // add dual later
  CHECK_TAOS_OPTION_IP_ERROR(pConn, userIp, INADDR_NONE);

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, "1292.168.0.2");
  //ASSERT(code != 0); // add dual alter
  CHECK_TAOS_OPTION_IP_ERROR(pConn, userIp, INADDR_NONE);
  taosSsleep(6);
  checkUserIp(pConn, "");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, "192.168.0.2");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_IP(pConn, userIp, "192.168.0.2");

  taosSsleep(6);

  checkUserIp(pConn, "192.168.0.2");
  taosMsleep(2 * HEARTBEAT_INTERVAL);

  //test user APP and user IP
  check_sql_result_integer(pConn, "select count(*) from performance_schema.perf_connections where user_app = 'aaaaaaaaaaaaaaaaaaaaaab'", 1);
  check_sql_result_integer(pConn, "select count(*) from performance_schema.perf_connections where user_ip = '192.168.0.2'", 1);

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, "192.168.1.2");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_IP(pConn, userIp, "192.168.1.2");


  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, NULL);
  CHECK_TAOS_OPTION_IP_ERROR(pConn, userIp, INADDR_NONE);
  taosSsleep(6);
  checkUserIp(pConn, "");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_IP, "192.168.1.2");
  CHECK_TAOS_OPTION_IP(pConn, userIp, "192.168.1.2");
  taosSsleep(6);
  checkUserIp(pConn, "192.168.1.2");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_USER_APP, "user");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, userApp, "user");

  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CONNECTOR_INFO, "java-connector:3.23.3");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_APP(pConn, cInfo, "java-connector:3.23.3");

  taosMsleep(2 * HEARTBEAT_INTERVAL);

  check_sql_result_integer(pConn, "select count(*) from performance_schema.perf_connections where user_app = 'user'", 1);
  check_sql_result_integer(pConn, "select count(*) from performance_schema.perf_connections where connector_info = 'java-connector:3.23.3'", 1);
  check_sql_result_integer(pConn, "select count(*) from performance_schema.perf_connections where user_ip = '192.168.1.2'", 1);

  // test clear
  code = taos_options_connection(pConn, TSDB_OPTION_CONNECTION_CLEAR, "192.168.0.2");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConn, charsetCxt, true);

#ifndef WINDOWS
  CHECK_TAOS_OPTION_POINTER(pConn, timezone, true);
  check_sql_result(pConn, "select timezone()", "Asia/Shanghai (CST, +0800)");
#endif

  CHECK_TAOS_OPTION_APP(pConn, userApp, "");
  CHECK_TAOS_OPTION_IP_ERROR(pConn, userIp, INADDR_NONE);

  taos_close(pConn);
}

TEST(charsetCase, charset_Test) {
  // 1. build connection with different charset
  TAOS* pConnGbk = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConnGbk != nullptr);

  TAOS* pConnUTF8 = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConnUTF8 != nullptr);

  TAOS* pConnDefault = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConnDefault != nullptr);

  int32_t code = taos_options_connection(pConnGbk, TSDB_OPTION_CONNECTION_CHARSET, "gbk");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConnGbk, charsetCxt, false);

  code = taos_options_connection(pConnUTF8, TSDB_OPTION_CONNECTION_CHARSET, "UTF-8");
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConnUTF8, charsetCxt, false);

  // 2. build test string
  char sqlTag[256] = {0};
  char sqlCol[256] = {0};

  // 芬   gbk encode is 0xB7D2,     UTF-8 encode is 0xE88AAC
  // 中国 gbk encode is 0xD6D0B9FA, UTF-8 encode is 0xE4B8ADE59BBD
  char fenUtf8[32] = {0};
  char fenGbk[32] = {0};
  char zhongGbk[32] = {0};
  char zhongguoUtf8[32] = {0};
  char guoUtf8[32] = {0};
  char zhongguoGbk[32] = {0};
  snprintf(fenUtf8, sizeof(fenUtf8), "%c%c%c", 0xE8, 0x8A, 0xAC);
  snprintf(fenGbk, sizeof(fenGbk), "%c%c", 0xB7, 0xD2);
  snprintf(zhongguoUtf8, sizeof(zhongguoUtf8), "%c%c%c%c%c%c", 0xE4, 0xB8, 0xAD, 0xE5, 0x9B, 0xBD);
  snprintf(guoUtf8, sizeof(guoUtf8), "%c%c%c", 0xE5, 0x9B, 0xBD);
  snprintf(zhongguoGbk, sizeof(zhongguoGbk), "%c%c%c%c", 0xD6, 0xD0, 0xB9, 0xFA);
  snprintf(zhongGbk, sizeof(zhongGbk), "%c%c", 0xD6, 0xD0);

  // 3. create stable
  execQuery(pConnGbk, "drop database if exists db1");
  execQuery(pConnGbk, "create database db1");
  execQuery(pConnGbk, "create table db1.stb (ts timestamp, c1 nchar(32), c2 int) tags(t1 timestamp, t2 nchar(32), t3 int)");

  // 4. test tag with different charset
  snprintf(sqlTag, sizeof(sqlTag), "create table db1.ctb1 using db1.stb tags('2023-09-16 17:00:00+05:00', '%s', 1)", fenUtf8);
  execQueryFail(pConnGbk, sqlTag);

  snprintf(sqlTag, sizeof(sqlTag), "create table db1.ctb1 using db1.stb tags('2023-09-16 17:00:00+05:00', '%s', 1)", fenGbk);
  execQuery(pConnGbk, sqlTag);

  // 5. test column with different charset
  snprintf(sqlCol, sizeof(sqlCol), "insert into db1.ctb1 values(1732178775133, '%s', 1)", zhongguoUtf8);
  execQueryFail(pConnGbk, sqlCol);

  snprintf(sqlCol, sizeof(sqlCol), "insert into db1.ctb1 values(1732178775133, '%s', 1)", zhongguoGbk);
  execQuery(pConnGbk, sqlCol);

  // 6. check result with different charset
  check_sql_result(pConnGbk, "select t2 from db1.ctb1", fenGbk);
  check_sql_result(pConnUTF8, "select t2 from db1.ctb1", fenUtf8);

  check_sql_result(pConnGbk, "select c1 from db1.ctb1", zhongguoGbk);
  check_sql_result(pConnUTF8, "select c1 from db1.ctb1", zhongguoUtf8);

  // 7. test function with different charset
  // 7.1 concat
  char zhongguofenGbk[32] = {0};
  snprintf(zhongguofenGbk, sizeof(zhongguofenGbk), "%s%s", zhongguoGbk, fenGbk);
  char sql[256] = {0};
  snprintf(sql, sizeof(sql), "select concat(c1, '%s') from db1.ctb1", fenGbk);
  execQueryFail(pConnGbk, sql);
  snprintf(sql, sizeof(sql), "select concat(c1, '%s') from db1.ctb1", fenUtf8);
  check_sql_result(pConnGbk, sql, zhongguofenGbk);

  // 7.2 trim
  snprintf(sql, sizeof(sql), "select trim(LEADING c1 from '%s') from db1.ctb1", zhongguofenGbk);
  check_sql_result(pConnGbk, sql, zhongguofenGbk);
  char zhongguofenUtf8[32] = {0};
  snprintf(zhongguofenUtf8, sizeof(zhongguofenUtf8), "%s%s", zhongguoUtf8, fenUtf8);
  snprintf(sql, sizeof(sql), "select trim(LEADING c1 from '%s') from db1.ctb1", zhongguofenUtf8);
  check_sql_result(pConnGbk, sql, fenUtf8);

  check_sql_result(pConnGbk, "select char(c1) from db1.ctb1", "");

  check_sql_result_integer(pConnGbk, "select ascii(c1) from db1.ctb1", 0xE4);
  check_sql_result_integer(pConnUTF8, "select ascii(c1) from db1.ctb1", 0xE4);
  check_sql_result_integer(pConnGbk, "select LENGTH(c1) from db1.ctb1", 8);
  check_sql_result_integer(pConnUTF8, "select LENGTH(c1) from db1.ctb1", 8);
  check_sql_result_integer(pConnGbk, "select CHAR_LENGTH(c1) from db1.ctb1", 2);
  check_sql_result_integer(pConnUTF8, "select CHAR_LENGTH(c1) from db1.ctb1", 2);

  execQuery(pConnGbk, "select LOWER(c1) from db1.ctb1");
  execQuery(pConnGbk, "select UPPER(c1) from db1.ctb1");

  snprintf(sql, sizeof(sql), "select position(c1 in '%s') from db1.ctb1", zhongguofenGbk);
  check_sql_result_integer(pConnGbk, sql, 0);

  snprintf(sql, sizeof(sql), "select position('%s' in c1) from db1.ctb1", guoUtf8);
  check_sql_result_integer(pConnUTF8, sql, 2);

  snprintf(sql, sizeof(sql), "select replace(c1, '%s', 'a') from db1.ctb1", zhongguoGbk);
  execQueryFail(pConnGbk, sql);

  snprintf(sql, sizeof(sql), "select replace(c1, '%s', 'a') from db1.ctb1", zhongguoUtf8);
  check_sql_result(pConnUTF8, sql, "a");

  snprintf(sql, sizeof(sql), "%s%s", zhongguoGbk, zhongguoGbk);
  check_sql_result(pConnGbk, "select repeat(c1, 2) from db1.ctb1", sql);

  check_sql_result(pConnGbk, "select cast(c1 as binary(32)) from db1.ctb1", zhongguoUtf8);

  check_sql_result(pConnUTF8, "select substr(c1,2,1) from db1.ctb1", guoUtf8);

  snprintf(sql, sizeof(sql), "select SUBSTRING_INDEX(c1,'%s',1) from db1.ctb1", guoUtf8);
  check_sql_result(pConnGbk, sql, zhongGbk);

  // 8. test default charset
  snprintf(sqlCol, sizeof(sqlCol), "insert into db1.ctb1 values(1732178775134, '%s', 1)", zhongguoUtf8);
  execQuery(pConnDefault, sqlCol);
  check_sql_result(pConnDefault, "select c1 from db1.ctb1 where ts = 1732178775134", zhongguoUtf8);

  // 9. test json tag with different charset
  execQuery(pConnUTF8, "create table db1.jsta (ts timestamp, c1 nchar(32), c2 int) tags(t1 json)");
  snprintf(sqlCol, sizeof(sqlCol), "create table db1.jsta1 using db1.jsta tags('{\"k\":\"%s\"}')", fenUtf8);
  execQuery(pConnUTF8, sqlCol);
  snprintf(sqlCol, sizeof(sqlCol), "insert into db1.jsta1 values(1732178775133, '%s', 1)", zhongguoUtf8);
  execQuery(pConnUTF8, sqlCol);

  char resJsonTag[32] = {0};
  snprintf(resJsonTag, sizeof(resJsonTag), "{\"k\":\"%s\"}", fenGbk);
  check_sql_result(pConnGbk, "select t1 from db1.jsta1", resJsonTag);

  // 10. reset charset to default(utf-8
  code = taos_options_connection(pConnGbk, TSDB_OPTION_CONNECTION_CHARSET, NULL);
  ASSERT(code == 0);
  CHECK_TAOS_OPTION_POINTER(pConnGbk, charsetCxt, true);
  check_sql_result(pConnGbk, "select t2 from db1.ctb1 where ts = 1732178775134", fenUtf8);
  check_sql_result(pConnGbk, "select c1 from db1.ctb1 where ts = 1732178775134", zhongguoUtf8);

  taos_close(pConnGbk);
  taos_close(pConnUTF8);
  taos_close(pConnDefault);

}

TEST(charsetCase, alter_charset_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != nullptr);

  execQueryFail(pConn, "alter dnode 1 'charset gbk'");
  execQueryFail(pConn, "local 'charset gbk'");

  taos_close(pConn);
}

#ifndef WINDOWS
TEST(timezoneCase, set_timezone_Test) {
  check_set_timezone(getConnWithGlobalOption);
  check_set_timezone(getConnWithOption);
}

TEST(timezoneCase, alter_timezone_Test) {
  TAOS* pConn = getConnWithGlobalOption("UTC-8");
  check_timezone(pConn, "show local variables", "UTC-8");

  execQuery(pConn, "alter local 'timezone Asia/Kolkata'");
  check_timezone(pConn, "show local variables", "Asia/Kolkata");

  execQuery(pConn, "alter local 'timezone Asia/Shanghai'");
  check_timezone(pConn, "show local variables", "Asia/Shanghai");

  execQueryFail(pConn, "alter dnode 1 'timezone Asia/Kolkata'");

  taos_close(pConn);
}

char *tz_test[] = {
    "2023-09-16 17:00:00+",
    "2023-09-16 17:00:00+a",
    "2023-09-16 17:00:00+8",
    "2023-09-16 17:00:00+832",
    "2023-09-16 17:00:00+8323",
    "2023-09-16 17:00:00+:",
    "2023-09-16 17:00:00+8:",
    "2023-09-16 17:00:00++:",
    "2023-09-16 17:00:00+d:",
    "2023-09-16 17:00:00+09:",
    "2023-09-16 17:00:00+8f:",
    "2023-09-16 17:00:00+080:",
    "2023-09-16 17:00:00+:30",
    "2023-09-16 17:00:00+:3",
    "2023-09-16 17:00:00+:093",
    "2023-09-16 17:00:00+:-30",
    "2023-09-16 17:00:00++:-30",
    "2023-09-16 17:00:00+8:8",
    "2023-09-16 17:00:00+8:2a",
    "2023-09-16 17:00:00+8:08",
    "2023-09-16 17:00:00+8:038",
    "2023-09-16 17:00:00+08:8",
    "2023-09-16 17:00:00+09:3a",
    "2023-09-16 17:00:00+09:abc",
    "2023-09-16 17:00:00+09:001",
};

void do_insert_failed(){
  TAOS* pConn = getConnWithGlobalOption("UTC-8");

  for (unsigned int i = 0; i < sizeof (tz_test) / sizeof (tz_test[0]); ++i){
    char sql[1024] = {0};
    (void)snprintf(sql, sizeof(sql), "insert into db1.ctb1 values('%s', '%s', 1)", tz_test[i], tz_test[i]);

    execQueryFail(pConn, sql);
  }
  taos_close(pConn);
}

struct insert_params
{
  const char *tz;
  const char *tbname;
  const char *t1;
  const char *t2;
};

struct insert_params params1[] = {
    {"UTC",              "ntb", "2023-09-16 17:00:00", "2023-09-16 17:00:00+08:00"},
    {"UTC",              "ctb1", "2023-09-16 17:00:00", "2023-09-16 17:00:00+08:00"},
};

struct insert_params params2[] = {
    {"UTC+9",              "ntb", "2023-09-16 08:00:00", "2023-09-16 08:00:00-01:00"},
    {"UTC+9",              "ctb1", "2023-09-16 08:00:00", "2023-09-16 11:00:00+02:00"},
};

void do_insert(struct insert_params params){
  TAOS* pConn = getConnWithOption(params.tz);
  char sql[1024] = {0};
  (void)snprintf(sql, sizeof(sql), "insert into db1.%s values('%s', '%s', 1)", params.tbname, params.t1, params.t2);
  execQuery(pConn, sql);
  taos_close(pConn);
}

void do_select(struct insert_params params){
  TAOS* pConn = getConnWithOption(params.tz);
  char sql[1024] = {0};
  (void)snprintf(sql, sizeof(sql), "select * from db1.%s where ts == '%s' and c1 == '%s'", params.tbname, params.t1, params.t2);
  checkRows(pConn, sql, 1);
  taos_close(pConn);
}

// test insert string and integer to timestamp both normal table and child table(and tag)
TEST(timezoneCase, insert_with_timezone_Test) {
  /*
   * 1. prepare data, create db and tables
   */
  TAOS* pConn1 = getConnWithOption("UTC+2");
  execQuery(pConn1, "drop database if exists db1");
  execQuery(pConn1, "create database db1");
  execQuery(pConn1, "create table db1.ntb (ts timestamp, c1 timestamp, c2 int)");
  execQuery(pConn1, "create table db1.stb (ts timestamp, c1 timestamp, c2 int) tags(t1 timestamp, t2 timestamp, t3 int)");
  execQuery(pConn1, "create table db1.ctb1 using db1.stb tags(\"2023-09-16 17:00:00+05:00\", \"2023-09-16 17:00:00\", 1)");
  execQuery(pConn1, "create table db1.ctb2 using db1.stb tags(1732178775000, 1732178775000, 1)");
  execQuery(pConn1, "insert into db1.ntb values(1732178775133, 1732178775133, 1)");
  execQuery(pConn1, "insert into db1.ctb1 values(1732178775133, 1732178775133, 1)"); //2024-11-21 10:46:15.133+02:00
  execQuery(pConn1, "insert into db1.ctb2 values(1732178775133, 1732178775133, 1)");

  /*
   * 2. test tag and timestamp with integer format
   */
  TAOS* pConn2 = getConnWithOption("UTC-2");
  checkRows(pConn2, "select * from db1.stb where t1 == '2023-09-16 17:00:00+05:00' and t2 == '2023-09-16 21:00:00'", 1);
  checkRows(pConn2, "select * from db1.stb where t1 == '2024-11-21 16:46:15+08:00' and t2 == '2024-11-21 09:46:15+01:00'", 1);
  checkRows(pConn2, "select * from db1.ntb where ts == '2024-11-21 09:46:15.133+01:00' and c1 == '2024-11-21 10:46:15.133'", 1);
  checkRows(pConn2, "select * from db1.ctb1 where ts == '2024-11-21 09:46:15.133+01:00' and c1 == '2024-11-21 10:46:15.133'", 1);

  check_sql_result(pConn2, "select TO_ISO8601(ts) from db1.ctb1", "2024-11-21T10:46:15.133+0200");   // 2024-01-01 23:00:00+0200


  /*
   * 3. test timestamp with string format
   */
  for (unsigned int i = 0; i < sizeof (params1) / sizeof (params1[0]); ++i){
    do_insert(params1[i]);
    do_select(params1[i]);
    do_select(params2[i]);
  }

  do_insert_failed();
  /*
   * 4. test NULL timezone, use default timezone UTC-8
   */
  TAOS* pConn3 = getConnWithOption(NULL);
  checkRows(pConn3, "select * from db1.stb where t1 == '2023-09-16 20:00:00' and t2 == '2023-09-17 03:00:00'", 2);
  checkRows(pConn3, "select * from db1.stb where t1 == 1732178775000 and t2 == 1732178775000", 1);
  checkRows(pConn3, "select * from db1.ntb where ts == '2024-11-21 16:46:15.133' and c1 == '2024-11-21 16:46:15.133'", 1);
  checkRows(pConn3, "select * from db1.ctb1 where ts == '2023-09-17 01:00:00' and c1 == '2023-09-16 17:00:00'", 1);

  /*
   * 5. test multi connection with different timezone
   */
  checkRows(pConn2, "select * from db1.ctb1 where ts == '2024-11-21 09:46:15.133+01:00' and c1 == '2024-11-21 10:46:15.133'", 1);
  checkRows(pConn1, "select * from db1.ctb1 where ts == '2024-11-21 09:46:15.133+01:00' and c1 == '2024-11-21 06:46:15.133'", 1);

  taos_close(pConn1);
  taos_close(pConn2);
  taos_close(pConn3);
}

TEST(timezoneCase, func_timezone_Test) {
  TAOS* pConn = getConnWithGlobalOption("UTC+8");
  check_sql_result(pConn, "select timezone()", "UTC+8 (UTC, -0800)");
  taos_close(pConn);

  pConn = getConnWithOption("UTC-2");

  execQuery(pConn, "drop database if exists db1");
  execQuery(pConn, "create database db1");
  execQuery(pConn, "create table db1.ntb (ts timestamp, c1 binary(32), c2 int)");
  execQuery(pConn, "insert into db1.ntb values(1704142800000, '2024-01-01 23:00:00', 1)");   // 2024-01-01 23:00:00+0200

  // test timezone
  check_sql_result(pConn, "select timezone()", "UTC-2 (UTC, +0200)");

  // test timetruncate
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 23:00:00', 1d, 0))", "2024-01-01T02:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 01:00:00', 1d, 0))", "2023-12-31T02:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 01:00:00+0300', 1d, 0))", "2023-12-31T02:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 01:00:00-0300', 1d, 0))", "2024-01-01T02:00:00.000+0200");

  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 23:00:00', 1w, 0))", "2024-01-04T02:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 01:00:00', 1w, 0))", "2023-12-28T02:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 01:00:00+0300', 1w, 0))", "2023-12-28T02:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 01:00:00-0300', 1w, 0))", "2024-01-04T02:00:00.000+0200");

  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 23:00:00', 1d, 1))", "2024-01-01T00:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 01:00:00', 1d, 1))", "2024-01-01T00:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 01:00:00+0500', 1d, 1))", "2023-12-31T00:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-01 01:00:00-0300', 1d, 1))", "2024-01-01T00:00:00.000+0200");

  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 23:00:00', 1w, 1))", "2024-01-04T00:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 01:00:00', 1w, 1))", "2024-01-04T00:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 01:00:00+0500', 1w, 1))", "2023-12-28T00:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE('2024-01-04 01:00:00-0300', 1w, 1))", "2024-01-04T00:00:00.000+0200");

  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE(1704142800000, 1d, 0))", "2024-01-01T02:00:00.000+0200");   // 2024-01-01 23:00:00+0200
  check_sql_result(pConn, "select TO_ISO8601(TIMETRUNCATE(ts, 1w, 1)) from db1.ntb", "2023-12-28T00:00:00.000+0200");   // 2024-01-01 23:00:00+0200

  // TODAY
  check_sql_result_partial(pConn, "select TO_ISO8601(today())", "T00:00:00.000+0200");

  // NOW
  check_sql_result_partial(pConn, "select TO_ISO8601(now())", "+0200");

  // WEEKDAY
  check_sql_result_integer(pConn, "select WEEKDAY('2024-01-01')", 0);
  check_sql_result_integer(pConn, "select WEEKDAY('2024-01-01 03:00:00')", 0);
  check_sql_result_integer(pConn, "select WEEKDAY('2024-01-01 23:00:00+0200')", 0);
  check_sql_result_integer(pConn, "select WEEKDAY('2024-01-01 23:00:00-1100')", 1);
  check_sql_result_integer(pConn, "select WEEKDAY(1704142800000)", 0);
  check_sql_result_integer(pConn, "select WEEKDAY(ts) from db1.ntb", 1);

  // DAYOFWEEK
  check_sql_result_integer(pConn, "select DAYOFWEEK('2024-01-01')", 2);
  check_sql_result_integer(pConn, "select DAYOFWEEK('2024-01-01 03:00:00')", 2);
  check_sql_result_integer(pConn, "select DAYOFWEEK('2024-01-01 23:00:00+0200')", 2);
  check_sql_result_integer(pConn, "select DAYOFWEEK('2024-01-01 23:00:00-1100')", 3);
  check_sql_result_integer(pConn, "select DAYOFWEEK(1704142800000)", 2);
  check_sql_result_integer(pConn, "select DAYOFWEEK(ts) from db1.ntb", 3);

  // WEEK
  check_sql_result_integer(pConn, "select WEEK('2024-01-07')", 1);
  check_sql_result_integer(pConn, "select WEEK('2024-01-07 02:00:00')", 1);
  check_sql_result_integer(pConn, "select WEEK('2024-01-07 02:00:00+0200')", 1);
  check_sql_result_integer(pConn, "select WEEK('2024-01-07 02:00:00+1100')", 0);
  check_sql_result_integer(pConn, "select WEEK(1704142800000)", 0);     // 2024-01-01 23:00:00+0200
  check_sql_result_integer(pConn, "select WEEK(ts) from db1.ntb", 0);   // 2024-01-01 23:00:00+0200

  check_sql_result_integer(pConn, "select WEEK('2024-01-07', 3)", 1);
  check_sql_result_integer(pConn, "select WEEK('2024-01-07 02:00:00', 3)", 1);
  check_sql_result_integer(pConn, "select WEEK('2024-01-07 02:00:00+0200', 3)", 1);
  check_sql_result_integer(pConn, "select WEEK('2024-01-01 02:00:00+1100', 3)", 52);
  check_sql_result_integer(pConn, "select WEEK(1704142800000, 3)", 1);     // 2024-01-01 23:00:00+0200
  check_sql_result_integer(pConn, "select WEEK(ts, 3) from db1.ntb", 1);   // 2024-01-01 23:00:00+0200

  // WEEKOFYEAR
  check_sql_result_integer(pConn, "select WEEKOFYEAR('2024-01-07')", 1);
  check_sql_result_integer(pConn, "select WEEKOFYEAR('2024-01-07 02:00:00')", 1);
  check_sql_result_integer(pConn, "select WEEKOFYEAR('2024-01-07 02:00:00+0200')", 1);
  check_sql_result_integer(pConn, "select WEEKOFYEAR('2024-01-01 02:00:00+1100')", 52);
  check_sql_result_integer(pConn, "select WEEKOFYEAR(1704142800000)", 1);     // 2024-01-01 23:00:00+0200
  check_sql_result_integer(pConn, "select WEEKOFYEAR(ts) from db1.ntb", 1);   // 2024-01-01 23:00:00+0200

  // TO_ISO8601
  check_sql_result(pConn, "select TO_ISO8601(ts) from db1.ntb", "2024-01-01T23:00:00.000+0200");
  check_sql_result(pConn, "select TO_ISO8601(ts,'-08') from db1.ntb", "2024-01-01T13:00:00.000-08");
  check_sql_result(pConn, "select TO_ISO8601(1)", "1970-01-01T02:00:00.001+0200");
  check_sql_result(pConn, "select TO_ISO8601(1,'+0800')", "1970-01-01T08:00:00.001+0800");

  // TO_UNIXTIMESTAMP
  check_sql_result_integer(pConn, "select TO_UNIXTIMESTAMP(c1) from db1.ntb", 1704121200000);   // use timezone in server UTC-8
  check_sql_result_integer(pConn, "select TO_UNIXTIMESTAMP('2024-01-01T23:00:00.000+0200')", 1704142800000);
  check_sql_result_integer(pConn, "select TO_UNIXTIMESTAMP('2024-01-01T13:00:00.000-08')", 1704142800000);
  check_sql_result_integer(pConn, "select TO_UNIXTIMESTAMP('2024-01-01T23:00:00.001')", 1704142800001);

  // TO_TIMESTAMP
  check_sql_result_integer(pConn, "select TO_TIMESTAMP(c1,'yyyy-mm-dd hh24:mi:ss') from db1.ntb", 1704121200000);   // use timezone in server UTC-8
  check_sql_result_integer(pConn, "select TO_TIMESTAMP('2024-01-01 23:00:00+02:00', 'yyyy-mm-dd hh24:mi:ss tzh')", 1704142800000);
  check_sql_result_integer(pConn, "select TO_TIMESTAMP('2024-01-01T13:00:00-08', 'yyyy-mm-ddThh24:mi:ss tzh')", 1704142800000);
  check_sql_result_integer(pConn, "select TO_TIMESTAMP('2024/01/01 23:00:00', 'yyyy/mm/dd hh24:mi:ss')", 1704142800000);

  // TO_CHAR
  check_sql_result(pConn, "select TO_CHAR(ts,'yyyy-mm-dd hh24:mi:ss') from db1.ntb", "2024-01-02 05:00:00");   // use timezone in server UTC-8
  check_sql_result(pConn, "select TO_CHAR(cast(1704142800000 as timestamp), 'yyyy-mm-dd hh24:mi:ss tzh')", "2024-01-01 23:00:00 +02");
  check_sql_result(pConn, "select TO_CHAR(cast(1704142800000 as timestamp), 'yyyy-mm-dd hh24:mi:ss')", "2024-01-01 23:00:00");

  // TIMEDIFF
  check_sql_result_integer(pConn, "select TIMEDIFF(c1, '2024-01-01T23:00:00.001+02') from db1.ntb", -21600001);   // use timezone in server UTC-8
  check_sql_result_integer(pConn, "select TIMEDIFF(c1, '2024-01-01T23:00:00.001') from db1.ntb", -1);   // use timezone in server UTC-8
  check_sql_result_integer(pConn, "select TIMEDIFF('2024-01-01T23:00:00.001', '2024-01-01T13:00:00.000-08')", 1);

  // CAST
  check_sql_result_integer(pConn, "select CAST(c1 as timestamp) from db1.ntb", 1704121200000);
  check_sql_result_integer(pConn, "select CAST('2024-01-01T23:00:00.000+02' as timestamp)", 1704142800000);
  check_sql_result_integer(pConn, "select CAST('2024-01-01T23:00:00.000' as timestamp)", 1704142800000);

  taos_close(pConn);

  // hash join
  pConn = getConnWithOption("UTC+1");

  execQuery(pConn, "drop database if exists db1");
  execQuery(pConn, "create database db1");
  execQuery(pConn, "create table db1.ntb (ts timestamp, c1 binary(32), c2 int)");
  execQuery(pConn, "create table db1.ntb1 (ts timestamp, c1 binary(32), c2 int)");
  execQuery(pConn, "insert into db1.ntb values(1703987400000, '2023-12-31 00:50:00', 1)");   // 2023-12-31 00:50:00-0100
  execQuery(pConn, "insert into db1.ntb1 values(1704070200000, '2023-12-31 23:50:00', 11)");   // 2023-12-31 23:50:00-0100
  checkRows(pConn, "select a.ts,b.ts from db1.ntb a join db1.ntb1 b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1d)", 1);

  // operator +1n +1y
  check_sql_result(pConn, "select TO_ISO8601(CAST('2023-01-31T00:00:00.000-01' as timestamp) + 1n)", "2023-02-28T00:00:00.000-0100");
  check_sql_result(pConn, "select TO_ISO8601(CAST('2024-01-31T00:00:00.000-01' as timestamp) + 1n)", "2024-02-29T00:00:00.000-0100");
  check_sql_result(pConn, "select TO_ISO8601(CAST('2024-02-29T00:00:00.000-01' as timestamp) + 1y)", "2025-02-28T00:00:00.000-0100");
  check_sql_result(pConn, "select TO_ISO8601(CAST('2024-01-31T00:00:00.000-01' as timestamp) + 1y)", "2025-01-31T00:00:00.000-0100");

  check_sql_result(pConn, "select TO_ISO8601(CAST('2024-01-01T00:00:00.000+01' as timestamp) + 1n)", "2024-01-31T22:00:00.000-0100");
  check_sql_result(pConn, "select TO_ISO8601(CAST('2024-01-01T00:00:00.000+01' as timestamp) + 1y)", "2024-12-31T22:00:00.000-0100");

  // case when
  check_sql_result_integer(pConn, "select case CAST('2024-01-01T00:00:00.000+01' as timestamp) when 1704063600000 then 1 end", 1);
  check_sql_result_integer(pConn, "select case CAST('2024-01-01T00:00:00.000' as timestamp) when 1704070800000 then 1 end", 1);

  taos_close(pConn);

}

time_t time_winter = 1731323281;    // 2024-11-11 19:08:01+0800
time_t time_summer = 1731323281 - 120 * 24 * 60 * 60;

struct test_times
{
  const char *name;
  time_t  t;
  const char *timezone;
} test_tz[] = {
    {"",                 time_winter, " (UTC, +0000)"},
    {"America/New_York", time_winter, "America/New_York (EST, -0500)"},  // 2024-11-11 19:08:01+0800
    {"America/New_York", time_summer, "America/New_York (EDT, -0400)"},
    {"Asia/Kolkata",     time_winter, "Asia/Kolkata (IST, +0530)"},
    {"Asia/Shanghai",    time_winter, "Asia/Shanghai (CST, +0800)"},
    {"Europe/London",    time_winter, "Europe/London (GMT, +0000)"},
    {"Europe/London",    time_summer, "Europe/London (BST, +0100)"}
};

void timezone_str_test(const char* tz, time_t t, const char* tzStr) {
  int code = setenv("TZ", tz, 1);
  ASSERT(-1 != code);
  tzset();

  char str1[TD_TIMEZONE_LEN] = {0};
  ASSERT(taosFormatTimezoneStr(t, tz, NULL, str1) == 0);
  ASSERT_STREQ(str1, tzStr);
}

void timezone_rz_str_test(const char* tz, time_t t, const char* tzStr) {
  timezone_t sp = tzalloc(tz);
  ASSERT(sp);

  char str1[TD_TIMEZONE_LEN] = {0};
  ASSERT(taosFormatTimezoneStr(t, tz, sp, str1) == 0);
  ASSERT_STREQ(str1, tzStr);
  tzfree(sp);
}

TEST(timezoneCase, format_timezone_Test) {
  for (unsigned int i = 0; i < sizeof (test_tz) / sizeof (test_tz[0]); ++i){
    timezone_str_test(test_tz[i].name, test_tz[i].t, test_tz[i].timezone);
    timezone_str_test(test_tz[i].name, test_tz[i].t, test_tz[i].timezone);
  }
}

TEST(timezoneCase, get_tz_Test) {
  {
    char tz[TD_TIMEZONE_LEN] = {0};
    getTimezoneStr(tz);
    ASSERT_STREQ(tz, "Asia/Shanghai");

//    getTimezoneStr(tz);
//    ASSERT_STREQ(tz, "Asia/Shanghai");
//
//    getTimezoneStr(tz);
//    ASSERT_STREQ(tz, TZ_UNKNOWN);
  }
}

struct {
  const char *	env;
  time_t	expected;
} test_mk[] = {
    {"MST",	832935315},
    {"",		832910115},
    {":UTC",	832910115},
    {"UTC",	832910115},
    {"UTC0",	832910115}
};


TEST(timezoneCase, mktime_Test){
  struct tm tm;
  time_t t;

  memset (&tm, 0, sizeof (tm));
  tm.tm_isdst = 0;
  tm.tm_year  = 96;	/* years since 1900 */
  tm.tm_mon   = 4;
  tm.tm_mday  = 24;
  tm.tm_hour  =  3;
  tm.tm_min   = 55;
  tm.tm_sec   = 15;

  for (unsigned int i = 0; i < sizeof (test_mk) / sizeof (test_mk[0]); ++i)
  {
    setenv ("TZ", test_mk[i].env, 1);
    t = taosMktime (&tm, NULL);
    ASSERT (t == test_mk[i].expected);
  }
}

TEST(timezoneCase, mktime_rz_Test){
  struct tm tm;
  time_t t;

  memset (&tm, 0, sizeof (tm));
  tm.tm_isdst = 0;
  tm.tm_year  = 96;	/* years since 1900 */
  tm.tm_mon   = 4;
  tm.tm_mday  = 24;
  tm.tm_hour  =  3;
  tm.tm_min   = 55;
  tm.tm_sec   = 15;

  for (unsigned int i = 0; i < sizeof (test_mk) / sizeof (test_mk[0]); ++i)
  {
    timezone_t tz = tzalloc(test_mk[i].env);
    ASSERT(tz);
    t = taosMktime(&tm, tz);
    ASSERT (t == test_mk[i].expected);
    tzfree(tz);
  }
}

TEST(timezoneCase, localtime_performance_Test) {
  timezone_t sp = tzalloc("Asia/Shanghai");
  ASSERT(sp);

  int cnt = 1000000;
  int times = 10;
  int64_t time_localtime = 0;
  int64_t time_localtime_rz = 0;
//  int cnt = 1000000;
  for (int i = 0; i < times; ++i) {
    int64_t t1 = taosGetTimestampNs();
    for (int j = 0; j < cnt; ++j) {
      time_t t = time_winter - j;
      struct tm tm1;
      ASSERT (taosLocalTime(&t, &tm1, NULL, 0, NULL));
    }
    int64_t tmp = taosGetTimestampNs() - t1;
    printf("localtime cost:%" PRId64 " ns, run %d times", tmp, cnt);
    time_localtime += tmp/cnt;

    printf("\n");



    int64_t t2 = taosGetTimestampNs();
    for (int j = 0; j < cnt; ++j) {
      time_t t = time_winter - j;
      struct tm tm1;
      ASSERT (taosLocalTime(&t, &tm1, NULL, 0, sp));
    }
    tmp = taosGetTimestampNs() - t2;
    printf("localtime_rz cost:%" PRId64 " ns, run %d times", tmp, cnt);
    time_localtime_rz += tmp/cnt;
    printf("\n\n");
  }
  printf("average: localtime cost:%" PRId64 " ns, localtime_rz cost:%" PRId64 " ns\n", time_localtime/times, time_localtime_rz/times);
  tzfree(sp);
}
#endif

#pragma GCC diagnostic pop

