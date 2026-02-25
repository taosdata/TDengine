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

// clang-format off

#include "gtest/gtest.h"
#include <iostream>
#include "clientInt.h"
#include "osSemaphore.h"
#include "taoserror.h"
#include "tarray.h"
#include "thash.h"
#include "totp.h"

// clang-format on

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "taos.h"

namespace {

#define nDup 10
static int32_t queryDB(TAOS* taos, char* command) {
  int       i;
  TAOS_RES* pSql = NULL;
  int32_t   code = -1;

  for (i = 0; i < nDup; ++i) {
    if (NULL != pSql) {
      taos_free_result(pSql);
      pSql = NULL;
    }

    pSql = taos_query(taos, command);
    code = taos_errno(pSql);
    if (0 == code) {
      break;
    } else {
      taosMsleep(1000);
      fprintf(stderr, "%d:retry to run: %s, reason: %s\n", i, command, taos_errstr(pSql));
    }
  }

  if (code != 0) {
    fprintf(stderr, "failed to run: %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    return code;
  } else {
    fprintf(stderr, "success to run: %s\n", command);
  }

  taos_free_result(pSql);
  return code;
}

static int32_t getNumOfRows(TAOS_RES* pRes) {
  int32_t numOfRows = 0;
  TAOS_ROW pRow = NULL;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    numOfRows++;
  }
  return numOfRows;
}

void printSubResults(void* pRes, int32_t* totalRows) {
  char buf[1024] = {0};

  int32_t vgId = tmq_get_vgroup_id(pRes);
  int64_t offset = tmq_get_vgroup_offset(pRes);
  while (1) {
    TAOS_ROW row = taos_fetch_row(pRes);
    if (row == NULL) {
      break;
    }

    TAOS_FIELD* fields = taos_fetch_fields(pRes);
    if (fields == NULL) {
      std::cout << "fields is null" << std::endl;
      break;
    }
    int32_t numOfFields = taos_field_count(pRes);
    int32_t precision = taos_result_precision(pRes);
    (void)taos_print_row(buf, row, fields, numOfFields);
    *totalRows += 1;
    std::cout << "vgId:" << vgId << ", offset:" << offset << ", precision:" << precision << ", row content:" << buf
              << std::endl;
  }

  //  taos_free_result(pRes);
}

void showDB(TAOS* pConn) {
  TAOS_RES* pRes = taos_query(pConn, "show databases");
  TAOS_ROW  pRow = NULL;

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    (void)printf("%s\n", str);
  }
}

void printResult(TAOS_RES* pRes) {
  TAOS_ROW    pRow = NULL;
  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  int32_t n = 0;
  char    str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    //    int32_t* length = taos_fetch_lengths(pRes);
    //    for(int32_t i = 0; i < numOfFields; ++i) {
    //      (void)printf("(%d):%d " , i, length[i]);
    //    }
    //    (void)printf("\n");
    //
    //    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    //    (void)printf("%s\n", str);
    //    memset(str, 0, sizeof(str));
  }
}

void fetchCallback(void* param, void* res, int32_t numOfRow) {
#if 0
  (void)printf("numOfRow = %d \n", numOfRow);
  int         numFields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  TAOS       *_taos = (TAOS *)param;
  if (numOfRow > 0) {
    for (int i = 0; i < numOfRow; ++i) {
      TAOS_ROW row = taos_fetch_row(res);

      char temp[256] = {0};
      taos_print_row(temp, row, fields, numFields);
      (void)printf("%s\n", temp);
    }
    taos_fetch_rows_a(res, fetchCallback, _taos);
  } else {
    (void)printf("no more data, close the connection.\n");
//    taos_free_result(res);
//    taos_close(_taos);
//    taos_cleanup();
  }
#endif
  if (numOfRow == 0) {
    (void)printf("completed\n");
    return;
  }

  taos_fetch_raw_block_a(res, fetchCallback, param);
}

void queryCallback(void* param, void* res, int32_t code) {
  if (code != TSDB_CODE_SUCCESS) {
    (void)printf("failed to execute, reason:%s\n", taos_errstr(res));
    taos_free_result(res);
    tsem_t* sem = (tsem_t*)param;
    tsem_post(sem);
    return;
  }
  (void)printf("start to fetch data\n");
  taos_fetch_raw_block_a(res, fetchCallback, param);
  taos_free_result(res);
  tsem_t* sem = (tsem_t*)param;
  tsem_post(sem);
}

void createNewTable(TAOS* pConn, int32_t index, int32_t numOfRows, int64_t startTs, const char* pVarchar) {
  char str[1024] = {0};
  (void)sprintf(str, "create table if not exists tu%d using st2 tags(%d)", index, index);

  TAOS_RES* pRes = taos_query(pConn, str);
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create table tu, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  if (startTs == 0) {
    for (int32_t i = 0; i < numOfRows; i += 20) {
      char sql[1024] = {0};
      (void)sprintf(sql,
                    "insert into tu%d values(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)"
                    "(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)"
                    "(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)"
                    "(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)(now+%da, %d)",
                    index, i, i, i + 1, i + 1, i + 2, i + 2, i + 3, i + 3, i + 4, i + 4, i + 5, i + 5, i + 6, i + 6,
                    i + 7, i + 7, i + 8, i + 8, i + 9, i + 9, i + 10, i + 10, i + 11, i + 11, i + 12, i + 12, i + 13,
                    i + 13, i + 14, i + 14, i + 15, i + 15, i + 16, i + 16, i + 17, i + 17, i + 18, i + 18, i + 19,
                    i + 19);
      TAOS_RES* p = taos_query(pConn, sql);
      if (taos_errno(p) != 0) {
        (void)printf("failed to insert data, reason:%s\n", taos_errstr(p));
      }

      taos_free_result(p);
    }
  } else {
    for (int32_t i = 0; i < numOfRows; i += 20) {
      char sql[1024 * 50] = {0};
      (void)sprintf(
          sql,
          "insert into tu%d values(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, "
          "%d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, "
          "%d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, "
          "'%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')(%ld, %d, '%s')",
          index, startTs, i, pVarchar, startTs + 1, i + 1, pVarchar, startTs + 2, i + 2, pVarchar, startTs + 3, i + 3,
          pVarchar, startTs + 4, i + 4, pVarchar, startTs + 5, i + 5, pVarchar, startTs + 6, i + 6, pVarchar,
          startTs + 7, i + 7, pVarchar, startTs + 8, i + 8, pVarchar, startTs + 9, i + 9, pVarchar, startTs + 10,
          i + 10, pVarchar, startTs + 11, i + 11, pVarchar, startTs + 12, i + 12, pVarchar, startTs + 13, i + 13,
          pVarchar, startTs + 14, i + 14, pVarchar, startTs + 15, i + 15, pVarchar, startTs + 16, i + 16, pVarchar,
          startTs + 17, i + 17, pVarchar, startTs + 18, i + 18, pVarchar, startTs + 19, i + 19, pVarchar);
      TAOS_RES* p = taos_query(pConn, sql);
      if (taos_errno(p) != 0) {
        (void)printf("failed to insert data, reason:%s\n", taos_errstr(p));
      }

      //      startTs += 20;
      taos_free_result(p);
    }
  }
}

void* queryThread(void* arg) {
  TAOS* pConn = taos_connect("192.168.0.209", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    (void)printf("failed to connect to db, reason:%s", taos_errstr(pConn));
    return NULL;
  }

  int64_t el = 0;

  for (int32_t i = 0; i < 5; ++i) {
    int64_t   st = taosGetTimestampUs();
    TAOS_RES* pRes = taos_query(pConn,
                                "SELECT _wstart as ts,max(usage_user) FROM benchmarkcpu.host_49 WHERE  ts >= "
                                "1451618560000 AND ts < 1451622160000 INTERVAL(1m) ;");
    if (taos_errno(pRes) != 0) {
      (void)printf("failed, reason:%s\n", taos_errstr(pRes));
    } else {
      printResult(pRes);
    }

    taos_free_result(pRes);
    el += (taosGetTimestampUs() - st);
    if (i % 1000 == 0 && i != 0) {
      (void)printf("total:%d, avg time:%.2fms\n", i, el / (double)(i * 1000));
    }
  }

  taos_close(pConn);
  return NULL;
}

int32_t numOfThreads = 1;

void tmq_commit_cb_print(tmq_t* pTmq, int32_t code, void* param) {
  //  (void)printf("auto commit success, code:%d\n", code);
}

void* doConsumeData(void* param) {
  TAOS*       pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  tmq_conf_t* conf = tmq_conf_new();
  (void)tmq_conf_set(conf, "enable.auto.commit", "true");
  (void)tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
  (void)tmq_conf_set(conf, "group.id", "cgrpName41");
  (void)tmq_conf_set(conf, "td.connect.user", "root");
  (void)tmq_conf_set(conf, "td.connect.pass", "taosdata");
  (void)tmq_conf_set(conf, "auto.offset.reset", "earliest");
  (void)tmq_conf_set(conf, "experimental.snapshot.enable", "true");
  (void)tmq_conf_set(conf, "msg.with.table.name", "true");
  (void)tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);

  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  (void)tmq_list_append(topicList, "topic_t2");

  // 启动订阅
  (void)tmq_subscribe(tmq, topicList);

  tmq_list_destroy(topicList);

  TAOS_FIELD* fields = NULL;
  int32_t     numOfFields = 0;
  int32_t     precision = 0;
  int32_t     totalRows = 0;
  int32_t     msgCnt = 0;
  int32_t     timeout = 25000;

  int32_t count = 0;

  while (1) {
    TAOS_RES* pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      char buf[1024] = {0};

      const char* topicName = tmq_get_topic_name(pRes);
      const char* dbName = tmq_get_db_name(pRes);
      int32_t     vgroupId = tmq_get_vgroup_id(pRes);

      (void)printf("topic: %s\n", topicName);
      (void)printf("db: %s\n", dbName);
      (void)printf("vgroup id: %d\n", vgroupId);

      while (1) {
        TAOS_ROW row = taos_fetch_row(pRes);
        if (row == NULL) {
          break;
        }

        fields = taos_fetch_fields(pRes);
        numOfFields = taos_field_count(pRes);
        precision = taos_result_precision(pRes);
        (void)taos_print_row(buf, row, fields, numOfFields);
        totalRows += 1;
        //        (void)printf("precision: %d, row content: %s\n", precision, buf);
      }

      taos_free_result(pRes);
    } else {
      break;
    }
  }

  (void)tmq_consumer_close(tmq);
  taos_close(pConn);
  (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
  return NULL;
}

}  // namespace

void testSessionCtrl();
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    // numOfThreads = atoi(argv[1]);
    int32_t code = taosStr2int32(argv[1], &numOfThreads);
    if (code != 0) {
      return code;
    }
  }

  numOfThreads = TMAX(numOfThreads, 1);
  (void)printf("the runing threads is:%d", numOfThreads);

  return RUN_ALL_TESTS();
}

TEST(clientCase, driverInit_Test) {
  // taosInitGlobalCfg();
  //  taos_init();
}
TEST(clientCase, sessControl) { testSessionCtrl(); }

TEST(clientCase, connect_Test) {
  taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    (void)printf("failed to connect to server, reason:%s\n", taos_errstr(NULL));
  }

  TAOS_RES* pRes = taos_query(pConn, "drop database abc1");
  if (taos_errno(pRes) != 0) {
    (void)printf("error in drop db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  taos_close(pConn);
}

static int32_t base32Decode(const char *in, int32_t inLen, uint8_t *out) {
  int     buffer = 0, bits = 0;
  int32_t outLen = 0;

  for (int32_t i = 0; i < inLen; i++) {
    char c = in[i];

    if (c >= 'a' && c <= 'z') {
      c -= 'a';
    } else if (c >= 'A' && c <= 'Z') {
      c -= 'A';
    } else if (c >= '2' && c <= '7') {
      c = c - '2' + 26;
    } else if (c == '=') {
      break;  // padding character
    } else {
      return -1;  // invalid character
    }
    buffer = (buffer << 5) | c;
    bits += 5;
    if (bits >= 8) {
      out[outLen++] = (buffer >> (bits - 8)) & 0xFF;
      bits -= 8;
    }
  }

  return outLen;  // success
}

TEST(clientCase, connect_totp_Test) {
  taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    (void)printf("failed to connect to server, reason:%s\n", taos_errstr(NULL));
  }
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "create user totp_u pass 'AAbb1122'");
  if (pRes == NULL) {
    (void)printf("failed to create user, reason:%s\n", taos_errstr(NULL));
  }
  ASSERT_NE(pRes, nullptr);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create totp_secret for user totp_u");
  if (pRes == NULL) {
    (void)printf("failed to create totp secret, reason:%s\n", taos_errstr(NULL));
  }
  ASSERT_NE(pRes, nullptr);
  ASSERT_EQ(taos_errno(pRes), 0);

  char secretStr[64] = {0};
  TAOS_ROW row = taos_fetch_row(pRes);
  ASSERT_NE(row, nullptr);
  tstrncpy(secretStr, (char*)row[0], sizeof(secretStr));
  (void)printf("secret is: %s\n", secretStr);

  taos_free_result(pRes);
  taos_close(pConn);

  uint8_t secret[TSDB_TOTP_SECRET_LEN] = {0};
  int32_t secretLen = base32Decode(secretStr, (int32_t)strlen(secretStr), secret);

  pConn = taos_connect_totp("localhost", "totp_u", "AAbb1122", "123456", NULL, 0);
  ASSERT_EQ(pConn, nullptr);

  int code = taos_connect_test("localhost", "totp_u", "AAbb1122", "123456", NULL, 0);
  ASSERT_EQ(code, TSDB_CODE_MND_WRONG_TOTP_CODE);

  char totp[16] = {0};
  int totpCode = taosGenerateTotpCode(secret, secretLen, 6);
  (void)taosFormatTotp(totpCode, 6, totp, sizeof(totp));
  pConn = taos_connect_totp("localhost", "totp_u", "AAbb1122", totp, NULL, 0);
  if (pConn == NULL) {
    (void)printf("failed to connect to server via totp, reason:%s\n", taos_errstr(NULL));
  }
  ASSERT_NE(pConn, nullptr);

  pRes = taos_query(pConn, "show users");
  ASSERT_NE(pRes, nullptr);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);

  taos_close(pConn);

  totpCode = taosGenerateTotpCode(secret, secretLen, 6);
  (void)taosFormatTotp(totpCode, 6, totp, sizeof(totp));
  code = taos_connect_test("localhost", "totp_u", "AAbb1122", totp, NULL, 0);
  ASSERT_EQ(code, 0);
}

TEST(clientCase, connect_token_Test) {
  char token[TSDB_TOKEN_LEN] = {0};

  taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    (void)printf("failed to connect to server via password, reason:%s\n", taos_errstr(NULL));
  }
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "create token root1 from user root");
  if (pRes == NULL) {
    (void)printf("failed to create token, reason:%s\n", taos_errstr(NULL));
  }
  ASSERT_NE(pRes, nullptr);
  ASSERT_EQ(taos_errno(pRes), 0);

  TAOS_ROW row = taos_fetch_row(pRes);
  ASSERT_NE(row, nullptr);
  tstrncpy(token, (char*)row[0], TSDB_TOKEN_LEN);
  (void)printf("token is: %s\n", token);
  taos_free_result(pRes);
  taos_close(pConn);

  pConn = taos_connect_token("localhost", token, NULL, 0);
  if (pConn == NULL) {
    (void)printf("failed to connect to server via token, reason:%s\n", taos_errstr(NULL));
  }
  ASSERT_NE(pConn, nullptr);

  pRes = taos_query(pConn, "show users");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to show users, reason:%s\n", taos_errstr(pRes));
  }
  ASSERT_NE(pRes, nullptr);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);

  char user[TSDB_USER_LEN] = {0};
  int  len = sizeof(user);
  ASSERT_EQ(taos_get_connection_info(pConn, TSDB_CONNECTION_INFO_USER, user, &len), 0);
  user[sizeof(user) - 1] = 0;
  if (len != 4 || memcmp(user, "root", 4) != 0) {
    (void)printf("wrong user: %s, len: %d\n", user, len);
  }
  ASSERT_EQ(len, 4);
  ASSERT_EQ(memcmp(user, "root", 4), 0);

  len = sizeof(token);
  ASSERT_EQ(taos_get_connection_info(pConn, TSDB_CONNECTION_INFO_TOKEN, token, &len), 0);
  token[sizeof(token) - 1] = 0;
  if (len != 5 || memcmp(token, "root1", 5) != 0) {
    (void)printf("wrong token name: %s, len: %d\n", token, len);
  }
  ASSERT_EQ(len, 5);
  ASSERT_EQ(memcmp(token, "root1", 5), 0);

  taos_close(pConn);
}

TEST(clientCase, set_option_Test) {
  taos_set_option(NULL, "ip", "127.0.0.1");
  ASSERT_EQ(taos_errno(NULL), TSDB_CODE_INVALID_PARA);

  OPTIONS opt1 = {};
  taos_set_option(&opt1, NULL, "127.0.0.1");
  ASSERT_EQ(taos_errno(NULL), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(opt1.count, 0);

  OPTIONS opt2 = {};
  taos_set_option(&opt2, "ip", NULL);
  ASSERT_EQ(taos_errno(NULL), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(opt2.count, 0);

  OPTIONS opt3 = {};
  size_t  cap3 = sizeof(opt3.keys) / sizeof(opt3.keys[0]);
  opt3.count = (uint16_t)(cap3);
  taos_set_option(&opt3, "ip", "127.0.0.1");
  ASSERT_EQ(taos_errno(NULL), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(opt3.count, cap3);

  OPTIONS opt4 = {};
  taos_set_option(&opt4, "ip", "127.0.0.1");
  ASSERT_EQ(opt4.count, 1);
  ASSERT_EQ(opt4.keys[0], "ip");
  ASSERT_EQ(opt4.values[0], "127.0.0.1");

  OPTIONS opt5 = {};
  size_t  cap5 = sizeof(opt5.keys) / sizeof(opt5.keys[0]);
  opt5.count = (uint16_t)(cap5 - 1);
  taos_set_option(&opt5, "ip", "127.0.0.1");
  ASSERT_EQ(opt5.count, cap5);
  ASSERT_EQ(opt5.keys[cap5 - 1], "ip");
  ASSERT_EQ(opt5.values[cap5 - 1], "127.0.0.1");
}

TEST(clientCase, connect_with_Test) {
  TAOS* pConn1 = taos_connect_with(NULL);
  ASSERT_NE(pConn1, nullptr);

  TAOS_RES* pRes1 = taos_query(pConn1, "create database if not exists test_taos_connect_with");
  ASSERT_EQ(taos_errno(pRes1), TSDB_CODE_SUCCESS);
  taos_free_result(pRes1);

  OPTIONS opt2 = {};
  taos_set_option(&opt2, "ip", "127.0.0.1");
  taos_set_option(&opt2, "user", "root");
  taos_set_option(&opt2, "pass", "taosdata");
  taos_set_option(&opt2, "db", "test_taos_connect_with");
  taos_set_option(&opt2, "port", "6030");
  taos_set_option(&opt2, "charset", "UTF-8");
  taos_set_option(&opt2, "timezone", "UTC");
  taos_set_option(&opt2, "userIp", "127.0.0.1");
  taos_set_option(&opt2, "userApp", "unittest");
  taos_set_option(&opt2, "connectorInfo", "gtest");
  taos_set_option(&opt2, "unknownKey", "value");
  TAOS* pConn2 = taos_connect_with(&opt2);
  ASSERT_NE(pConn2, nullptr);
  taos_close(pConn2);

  TAOS_RES* pRes2 = taos_query(pConn1, "drop database if exists test_taos_connect_with");
  ASSERT_EQ(taos_errno(pRes2), TSDB_CODE_SUCCESS);
  taos_free_result(pRes2);

  taos_close(pConn1);

  OPTIONS opt3 = {};
  taos_set_option(&opt3, "ip", "invalid_ip");
  taos_set_option(&opt3, "ip", "127.0.0.1");
  taos_set_option(&opt3, "port", "abc");
  TAOS* pConn3 = taos_connect_with(&opt3);
  ASSERT_NE(pConn3, nullptr);
  taos_close(pConn3);

  OPTIONS opt4 = {};
  taos_set_option(&opt4, "ip", "192.168.1.1");
  taos_set_option(&opt4, "port", "6789");
  TAOS* pConn4 = taos_connect_with(&opt4);
  ASSERT_EQ(pConn4, nullptr);

  OPTIONS opt5 = {};
  taos_set_option(&opt5, "charset", "abc");
  TAOS* pConn5 = taos_connect_with(&opt5);
  ASSERT_EQ(pConn5, nullptr);
  ASSERT_NE(taos_errno(NULL), TSDB_CODE_SUCCESS);

  OPTIONS opt6 = {};
  opt6.count = 3;
  opt6.keys[0] = NULL;
  opt6.values[0] = "127.0.0.1";
  opt6.keys[1] = "ip";
  opt6.values[1] = NULL;
  opt6.keys[2] = NULL;
  opt6.values[2] = NULL;
  TAOS* pConn6 = taos_connect_with(&opt6);
  ASSERT_NE(pConn6, nullptr);
  taos_close(pConn6);
}

TEST(clientCase, connect_with_dsn_Test) {
  TAOS* pConn = taos_connect_with_dsn(NULL);
  ASSERT_EQ(pConn, nullptr);
}

TEST(clientCase, create_user_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "create user abc pass 'abc'");
  if (taos_errno(pRes) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to create user, reason:%s\n", taos_errstr(pRes));
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, create_account_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "create account aabc pass 'abc'");
  if (taos_errno(pRes) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to create user, reason:%s\n", taos_errstr(pRes));
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, drop_account_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "drop account aabc");
  if (taos_errno(pRes) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to create user, reason:%s\n", taos_errstr(pRes));
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, show_user_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "show users");
  TAOS_ROW  pRow = NULL;

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_NE(pFields, nullptr);
  int32_t numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    (void)printf("%s\n", str);
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, drop_user_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "drop user abc");
  if (taos_errno(pRes) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to create user, reason:%s\n", taos_errstr(pRes));
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, show_db_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "show databases");
  TAOS_ROW  pRow = NULL;

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_NE(pFields, nullptr);
  int32_t numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    (void)printf("%s\n", str);
  }
  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, create_db_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "create database abc1 vgroups 2");
  if (taos_errno(pRes) != 0) {
    (void)printf("error in create db, reason:%s\n", taos_errstr(pRes));
  }

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_TRUE(pFields == NULL);

  int32_t numOfFields = taos_num_fields(pRes);
  ASSERT_EQ(numOfFields, 0);

  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database abc1 vgroups 4");
  if (taos_errno(pRes) != 0) {
    (void)printf("error in create db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);
  taos_close(pConn);
}

// NOTE: create_dnode_Test and drop_dnode_Test does not
//       cooperate well, thus offline dnodes will exist
//       which will make taos fail to connect to taosd once taosd gets restarted
// TEST(clientCase, create_dnode_Test) {
//   TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
//   ASSERT_NE(pConn, nullptr);
//
//   TAOS_RES* pRes = taos_query(pConn, "create dnode abc1 port 7000");
//   if (taos_errno(pRes) != 0) {
//     (void)printf("error in create dnode, reason:%s\n", taos_errstr(pRes));
//   }
//   taos_free_result(pRes);
//
//   pRes = taos_query(pConn, "create dnode 1.1.1.1 port 9000");
//   if (taos_errno(pRes) != 0) {
//     (void)printf("failed to create dnode, reason:%s\n", taos_errstr(pRes));
//   }
//   taos_free_result(pRes);
//
//   taos_close(pConn);
// }
//
// TEST(clientCase, drop_dnode_Test) {
//   TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
//   ASSERT_NE(pConn, nullptr);
//
//   TAOS_RES* pRes = taos_query(pConn, "drop dnode 3");
//   if (taos_errno(pRes) != 0) {
//     (void)printf("error in drop dnode, reason:%s\n", taos_errstr(pRes));
//   }
//
//   TAOS_FIELD* pFields = taos_fetch_fields(pRes);
//   ASSERT_TRUE(pFields == NULL);
//
//   int32_t numOfFields = taos_num_fields(pRes);
//   ASSERT_EQ(numOfFields, 0);
//   taos_free_result(pRes);
//
//   pRes = taos_query(pConn, "drop dnode 4");
//   if (taos_errno(pRes) != 0) {
//     (void)printf("error in drop dnode, reason:%s\n", taos_errstr(pRes));
//   }
//
//   taos_free_result(pRes);
//   taos_close(pConn);
// }

TEST(clientCase, use_db_test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    (void)printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_TRUE(pFields == NULL);

  int32_t numOfFields = taos_num_fields(pRes);
  ASSERT_EQ(numOfFields, 0);

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, create_stable_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "create database if not exists abc1 vgroups 2");
  if (taos_errno(pRes) != 0) {
    (void)printf("error in create db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use abc1");
  while (taos_errno(pRes) == TSDB_CODE_MND_DB_IN_CREATING || taos_errno(pRes) == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    pRes = taos_query(pConn, "use abc1");
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists abc1.st1(ts timestamp, k int) tags(a int)");
  if (taos_errno(pRes) != 0) {
    (void)printf("error in create stable, reason:%s\n", taos_errstr(pRes));
  }

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_TRUE(pFields == NULL);

  int32_t numOfFields = taos_num_fields(pRes);
  ASSERT_EQ(numOfFields, 0);
  taos_free_result(pRes);

  taos_close(pConn);
}

TEST(clientCase, create_table_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists tm0(ts timestamp, k int)");
  ASSERT_EQ(taos_errno(pRes), 0);

  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists tm1(ts timestamp, k blob)");
  ASSERT_EQ(taos_errno(pRes), 0);

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, create_ctable_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create stable if not exists st1 (ts timestamp, k int ) tags(a int)");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create stable, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tu using st1 tags('2021-10-10 1:1:1');");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create child table tm0, reason:%s\n", taos_errstr(pRes));
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, show_stable_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "show abc1.stables");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to show stables, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    ASSERT_TRUE(false);
  }

  TAOS_ROW    pRow = NULL;
  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    (void)printf("%s\n", str);
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, show_vgroup_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "show vgroups");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to show vgroups, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    ASSERT_TRUE(false);
  }

  TAOS_ROW pRow = NULL;

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    (void)printf("%s\n", str);
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, create_multiple_tables) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "create database if not exists abc1");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create db, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    taos_close(pConn);
    return;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use abc1");
  while (taos_errno(pRes) == TSDB_CODE_MND_DB_IN_CREATING || taos_errno(pRes) == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    pRes = taos_query(pConn, "use abc1");
  }
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to use db, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    taos_close(pConn);
    return;
  }

  taos_free_result(pRes);

  pRes = taos_query(pConn, "create stable if not exists st1 (ts timestamp, k int) tags(a int)");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create stable tables, reason:%s\n", taos_errstr(pRes));
  }

  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists t_2 using st1 tags(1)");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create multiple tables, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    ASSERT_TRUE(false);
  }

  taos_free_result(pRes);
  pRes = taos_query(pConn, "create table if not exists t_3 using st1 tags(2)");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create multiple tables, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    ASSERT_TRUE(false);
  }

  TAOS_ROW    pRow = NULL;
  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    (void)printf("%s\n", str);
  }

  taos_free_result(pRes);

  for (int32_t i = 0; i < 500; i += 2) {
    char sql[512] = {0};
    (void)snprintf(sql, tListLen(sql), "create table t_x_%d using st1 tags(2) t_x_%d using st1 tags(5)", i, i + 1);
    TAOS_RES* pres = taos_query(pConn, sql);
    if (taos_errno(pres) != 0) {
      (void)printf("failed to create table %d\n, reason:%s", i, taos_errstr(pres));
    }
    taos_free_result(pres);
  }

  taos_close(pConn);
}

TEST(clientCase, show_table_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "show tables");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to show tables, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use abc1");
  taos_free_result(pRes);

  pRes = taos_query(pConn, "show tables");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to show tables, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
  }

  TAOS_ROW    pRow = NULL;
  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  int32_t count = 0;
  char    str[512] = {0};

  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    if (code > 0) {
      (void)printf("%d: %s\n", ++count, str);
    }
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, generated_request_id_test) {
  SHashObj* phash = taosHashInit(10000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);

  for (int32_t i = 0; i < 50000; ++i) {
    uint64_t v = generateRequestId();
    void*    result = taosHashGet(phash, &v, sizeof(v));
    if (result != nullptr) {
      //      (void)printf("0x%llx, index:%d\n", v, i);
    }
    ASSERT_EQ(result, nullptr);
    (void)taosHashPut(phash, &v, sizeof(v), NULL, 0);
  }

  taosHashCleanup(phash);
}

TEST(clientCase, insert_test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into t_2 values(now, 1)");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create into table t_2, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    ASSERT_TRUE(false);
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, projection_query_tables) {
#if 0
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = NULL;
  
  pRes= taos_query(pConn, "use abc1");
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select imputation(a) from (select _wstart, count(*) a from t1 where ts<='2025-3-6 11:17:44' interval(1s));");
  // pRes = taos_query(pConn, "select imputation(a) from t1 where ts<='2024-11-15 1:7:44'");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to do forecast query, reason:%s\n", taos_errstr(pRes));
  }

  TAOS_ROW    pRow = NULL;
  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    //      int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    //      (void)printf("%s\n", str);
  }

  taos_free_result(pRes);
  taos_close(pConn);
#endif
}

TEST(clientCase, tsbs_perf_test) {
  TdThread qid[20] = {0};

  for (int32_t i = 0; i < numOfThreads; ++i) {
    (void)taosThreadCreate(&qid[i], NULL, queryThread, NULL);
  }
  for (int32_t i = 0; i < numOfThreads; ++i) {
    (void)taosThreadJoin(qid[i], NULL);
  }
}

TEST(clientCase, projection_query_stables) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "explain select * from dbvg.st where tbname='ct1'");

  TAOS_ROW    pRow = NULL;
  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  int32_t numOfRows = 0;
  int32_t i = 0;
  int32_t prev = 0;

  char str[512] = {0};
  while (1) {
    pRow = taos_fetch_row(pRes);
    if (pRow == NULL) {
      break;
    }
    i += numOfRows;

    if ((i / 1000000) > prev) {
      (void)printf("%d\n", i);
      prev = i / 1000000;
    }
    //(void)printf("%d\n", i);
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(clientCase, agg_query_tables) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to use db, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    ASSERT_TRUE(false);
  }
  taos_free_result(pRes);

  int64_t st = 1685959293299;
  for (int32_t i = 0; i < 5; ++i) {
    char s[256] = {0};

    while (1) {
      (void)sprintf(s, "insert into t1 values(%ld, %d)", st + i, i);
      pRes = taos_query(pConn, s);

      int32_t ret = taos_errno(pRes);

      if (ret != 0) {
        (void)printf("failed to insert into table, reason:%s\n", taos_errstr(pRes));
      }
      taos_free_result(pRes);
      break;
    }
  }
  taos_close(pConn);
}

// --- copy the following script in the shell to setup the environment ---
//
// create database test;
// use test;
// create table m1(ts timestamp, k int) tags(a int);
// create table tm0 using m1 tags(1);
// create table tm1 using m1 tags(2);
// insert into tm0 values('2021-1-1 1:1:1.120', 1) ('2021-1-1 1:1:2.9', 2) tm1 values('2021-1-1 1:1:1.120', 11)
// ('2021-1-1 1:1:2.99', 22);

TEST(clientCase, async_api_test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to use db, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    taos_close(pConn);
    return;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tu(ts) values('2022-02-27 12:12:61')");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed, reason:%s\n", taos_errstr(pRes));
  }

  int32_t     n = 0;
  TAOS_ROW    pRow = NULL;
  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  int32_t     numOfFields = taos_num_fields(pRes);

  char str[512] = {0};
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    int32_t* length = taos_fetch_lengths(pRes);
    for (int32_t i = 0; i < numOfFields; ++i) {
      (void)printf("(%d):%d ", i, length[i]);
    }
    (void)printf("\n");

    int32_t code = taos_print_row(str, pRow, pFields, numOfFields);
    (void)printf("%s\n", str);
    (void)memset(str, 0, sizeof(str));
  }
  taos_free_result(pRes);
  tsem_t sem;
  (void)tsem_init(&sem, 0, 0);
  taos_query_a(pConn, "select count(*) from tu", queryCallback, &sem);
  tsem_wait(&sem);
  tsem_destroy(&sem);
  taos_close(pConn);
}

TEST(clientCase, update_test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "select cast(0 as timestamp)-1y");
  if (taos_errno(pRes) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to create database, code:%s", taos_errstr(pRes));
    taos_free_result(pRes);
    taos_close(pConn);
    return;
  }

  taos_free_result(pRes);

  pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != TSDB_CODE_SUCCESS) {
    (void)printf("failed to use db, code:%s", taos_errstr(pRes));
    taos_free_result(pRes);
    taos_close(pConn);
    return;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tup (ts timestamp, k int);");
  if (taos_errno(pRes) != 0) {
    (void)printf("failed to create table, reason:%s", taos_errstr(pRes));
  }

  taos_free_result(pRes);

  char s[256] = {0};
  for (int32_t i = 0; i < 10; ++i) {
    (void)sprintf(s, "insert into tup values(now+%da, %d)", i, i);
    pRes = taos_query(pConn, s);
    taos_free_result(pRes);
  }
  taos_close(pConn);
}

TEST(clientCase, sub_db_test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  //  TAOS_RES* pRes = taos_query(pConn, "create topic topic_t1 as select * from t1");
  //  if (taos_errno(pRes) != TSDB_CODE_SUCCESS) {
  //    (void)printf("failed to create topic, code:%s", taos_errstr(pRes));
  //    taos_free_result(pRes);
  //    return;
  //  }

  tmq_conf_t* conf = tmq_conf_new();
  (void)tmq_conf_set(conf, "enable.auto.commit", "true");
  (void)tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
  (void)tmq_conf_set(conf, "group.id", "cgrpNamedb");
  (void)tmq_conf_set(conf, "td.connect.user", "root");
  (void)tmq_conf_set(conf, "td.connect.pass", "taosdata");
  (void)tmq_conf_set(conf, "auto.offset.reset", "earliest");
  (void)tmq_conf_set(conf, "experimental.snapshot.enable", "false");
  (void)tmq_conf_set(conf, "msg.with.table.name", "true");
  (void)tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);

  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  (void)tmq_list_append(topicList, "topic_t1");
  //  tmq_list_append(topicList, "topic_s2");

  // 启动订阅
  (void)tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  TAOS_FIELD* fields = NULL;
  int32_t     numOfFields = 0;
  int32_t     precision = 0;
  int32_t     totalRows = 0;
  int32_t     msgCnt = 0;
  int32_t     timeout = 5000;

  int32_t count = 0;

  while (1) {
    TAOS_RES* pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      char    buf[1024];
      int32_t rows = 0;

      const char* topicName = tmq_get_topic_name(pRes);
      const char* dbName = tmq_get_db_name(pRes);
      int32_t     vgroupId = tmq_get_vgroup_id(pRes);

      (void)printf("topic: %s\n", topicName);
      (void)printf("db: %s\n", dbName);
      (void)printf("vgroup id: %d\n", vgroupId);

      if (count++ > 200) {
        (void)tmq_unsubscribe(tmq);
        break;
      }

      while (1) {
        TAOS_ROW row = taos_fetch_row(pRes);
        if (row == NULL) break;

        fields = taos_fetch_fields(pRes);
        ASSERT_NE(fields, nullptr);
        numOfFields = taos_field_count(pRes);
        precision = taos_result_precision(pRes);
        rows++;
        (void)taos_print_row(buf, row, fields, numOfFields);
        (void)printf("precision: %d, row content: %s\n", precision, buf);
      }
      taos_free_result(pRes);
    } else {
      break;
    }
  }

  (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
  taos_close(pConn);
}

TEST(clientCase, tmq_commit) {
  //  taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");

  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  tmq_conf_t* conf = tmq_conf_new();

  (void)tmq_conf_set(conf, "enable.auto.commit", "false");
  (void)tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  (void)tmq_conf_set(conf, "group.id", "group_id_2");
  (void)tmq_conf_set(conf, "td.connect.user", "root");
  (void)tmq_conf_set(conf, "td.connect.pass", "taosdata");
  (void)tmq_conf_set(conf, "auto.offset.reset", "earliest");
  (void)tmq_conf_set(conf, "msg.with.table.name", "true");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);

  char topicName[128] = "tp";
  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  (void)tmq_list_append(topicList, topicName);

  // 启动订阅
  (void)tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  int32_t totalRows = 0;
  int32_t msgCnt = 0;
  int32_t timeout = 2000;

  tmq_topic_assignment* pAssign = NULL;
  int32_t               numOfAssign = 0;

  int32_t code = tmq_get_topic_assignment(tmq, topicName, &pAssign, &numOfAssign);
  if (code != 0) {
    (void)printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign);
    (void)tmq_consumer_close(tmq);
    taos_close(pConn);
    (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
    return;
  }

  for (int i = 0; i < numOfAssign; i++) {
    tmq_topic_assignment* pa = &pAssign[i];
    std::cout << "assign i:" << i << ", vgId:" << pa->vgId << ", offset:" << pa->currentOffset << ", start:%"
              << pa->begin << ", end:%" << pa->end << std::endl;

    int64_t committed = tmq_committed(tmq, topicName, pa->vgId);
    std::cout << "committed vgId:" << pa->vgId << " committed:" << committed << std::endl;

    int64_t position = tmq_position(tmq, topicName, pa->vgId);
    std::cout << "position vgId:" << pa->vgId << ", position:" << position << std::endl;

    (void)tmq_offset_seek(tmq, topicName, pa->vgId, 1);
    position = tmq_position(tmq, topicName, pa->vgId);
    std::cout << "after seek 1, position vgId:" << pa->vgId << " position:" << position << std::endl;
  }

  while (1) {
    (void)printf("start to poll\n");
    TAOS_RES* pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      printSubResults(pRes, &totalRows);
    } else {
      break;
    }

    (void)tmq_commit_sync(tmq, pRes);
    for (int i = 0; i < numOfAssign; i++) {
      int64_t committed = tmq_committed(tmq, topicName, pAssign[i].vgId);
      std::cout << "committed vgId:" << pAssign[i].vgId << " , committed:" << committed << std::endl;
      if (committed > 0) {
        int32_t code = tmq_commit_offset_sync(tmq, topicName, pAssign[i].vgId, 4);
        (void)printf("tmq_commit_offset_sync vgId:%d, offset:4, code:%d\n", pAssign[i].vgId, code);
        int64_t committed = tmq_committed(tmq, topicName, pAssign[i].vgId);

        std::cout << "after tmq_commit_offset_sync, committed vgId:" << pAssign[i].vgId << ", committed:" << committed
                  << std::endl;
      }
    }
    if (pRes != NULL) {
      taos_free_result(pRes);
    }

    //    tmq_offset_seek(tmq, "tp", pAssign[0].vgId, pAssign[0].begin);
  }

  tmq_free_assignment(pAssign);

  (void)tmq_consumer_close(tmq);
  taos_close(pConn);
  (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}
namespace {
void doPrintInfo(tmq_topic_assignment* pa, int32_t index) {
  std::cout << "assign i:" << index << ", vgId:" << pa->vgId << ", offset:%" << pa->currentOffset << ", start:%"
            << pa->begin << ", end:%" << pa->end << std::endl;
}
}  // namespace
TEST(clientCase, td_25129) {
  //  taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");

  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  tmq_conf_t* conf = tmq_conf_new();

  (void)tmq_conf_set(conf, "enable.auto.commit", "false");
  (void)tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  (void)tmq_conf_set(conf, "group.id", "group_id_2");
  (void)tmq_conf_set(conf, "td.connect.user", "root");
  (void)tmq_conf_set(conf, "td.connect.pass", "taosdata");
  (void)tmq_conf_set(conf, "auto.offset.reset", "earliest");
  (void)tmq_conf_set(conf, "msg.with.table.name", "true");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  ASSERT_NE(tmq, nullptr);
  tmq_conf_destroy(conf);

  char topicName[128] = "tp";
  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  (void)tmq_list_append(topicList, topicName);

  // 启动订阅
  (void)tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  TAOS_FIELD* fields = NULL;
  int32_t     numOfFields = 0;
  int32_t     precision = 0;
  int32_t     totalRows = 0;
  int32_t     msgCnt = 0;
  int32_t     timeout = 2000;

  int32_t count = 0;

  tmq_topic_assignment* pAssign = NULL;
  int32_t               numOfAssign = 0;

  int32_t code = tmq_get_topic_assignment(tmq, topicName, &pAssign, &numOfAssign);
  if (code != 0) {
    (void)printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign);
    (void)tmq_consumer_close(tmq);
    taos_close(pConn);
    (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
    return;
  }

  for (int i = 0; i < numOfAssign; i++) {
    doPrintInfo(&pAssign[i], i);
  }

  //  tmq_offset_seek(tmq, "tp", pAssign[0].vgId, 4);
  tmq_free_assignment(pAssign);

  code = tmq_get_topic_assignment(tmq, topicName, &pAssign, &numOfAssign);
  if (code != 0) {
    (void)printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign);
    (void)tmq_consumer_close(tmq);
    taos_close(pConn);
    (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
    return;
  }

  for (int i = 0; i < numOfAssign; i++) {
    doPrintInfo(&pAssign[i], i);
  }

  tmq_free_assignment(pAssign);

  code = tmq_get_topic_assignment(tmq, topicName, &pAssign, &numOfAssign);
  if (code != 0) {
    (void)printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign);
    (void)tmq_consumer_close(tmq);
    taos_close(pConn);
    (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
    return;
  }

  for (int i = 0; i < numOfAssign; i++) {
    int64_t committed = tmq_committed(tmq, topicName, pAssign[i].vgId);
    doPrintInfo(&pAssign[i], i);
  }

  while (1) {
    (void)printf("start to poll\n");
    TAOS_RES* pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      char buf[128];

      const char* topicName = tmq_get_topic_name(pRes);
      //      const char* dbName = tmq_get_db_name(pRes);
      //      int32_t     vgroupId = tmq_get_vgroup_id(pRes);
      //
      //      (void)printf("topic: %s\n", topicName);
      //      (void)printf("db: %s\n", dbName);
      //      (void)printf("vgroup id: %d\n", vgroupId);

      printSubResults(pRes, &totalRows);

      code = tmq_get_topic_assignment(tmq, topicName, &pAssign, &numOfAssign);
      if (code != 0) {
        (void)printf("error occurs:%s\n", tmq_err2str(code));
        tmq_free_assignment(pAssign);
        (void)tmq_consumer_close(tmq);
        taos_close(pConn);
        (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
        return;
      }

      for (int i = 0; i < numOfAssign; i++) {
        doPrintInfo(&pAssign[i], i);
      }
    } else {
      for (int i = 0; i < numOfAssign; i++) {
        (void)tmq_offset_seek(tmq, topicName, pAssign[i].vgId, pAssign[i].currentOffset);
      }
      (void)tmq_commit_sync(tmq, pRes);
      break;
    }

    //    tmq_commit_sync(tmq, pRes);
    if (pRes != NULL) {
      taos_free_result(pRes);
      //      if ((++count) > 1) {
      //        break;
      //      }
    } else {
      break;
    }

    //    tmq_offset_seek(tmq, "tp", pAssign[0].vgId, pAssign[0].begin);
  }

  tmq_free_assignment(pAssign);

  code = tmq_get_topic_assignment(tmq, "tp", &pAssign, &numOfAssign);
  if (code != 0) {
    (void)printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign);
    (void)tmq_consumer_close(tmq);
    taos_close(pConn);
    (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
    return;
  }

  for (int i = 0; i < numOfAssign; i++) {
    doPrintInfo(&pAssign[i], i);
  }

  tmq_free_assignment(pAssign);
  (void)tmq_consumer_close(tmq);
  taos_close(pConn);
  (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}

TEST(clientCase, sub_tb_test) {
  (void)taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");

  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  tmq_conf_t* conf = tmq_conf_new();

  int32_t ts = taosGetTimestampMs() % INT32_MAX;
  char    consumerGroupid[128] = {0};
  (void)sprintf(consumerGroupid, "group_id_%d", ts);

  (void)tmq_conf_set(conf, "enable.auto.commit", "true");
  (void)tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  (void)tmq_conf_set(conf, "group.id", consumerGroupid);
  (void)tmq_conf_set(conf, "td.connect.user", "root");
  (void)tmq_conf_set(conf, "td.connect.pass", "taosdata");
  (void)tmq_conf_set(conf, "auto.offset.reset", "earliest");
  (void)tmq_conf_set(conf, "experimental.snapshot.enable", "false");
  (void)tmq_conf_set(conf, "msg.with.table.name", "true");
  (void)tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  ASSERT_NE(tmq, nullptr);
  tmq_conf_destroy(conf);

  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  (void)tmq_list_append(topicList, "t1");

  // 启动订阅
  (void)tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  TAOS_FIELD* fields = NULL;
  int32_t     numOfFields = 0;
  int32_t     precision = 0;
  int32_t     totalRows = 0;
  int32_t     msgCnt = 0;
  int32_t     timeout = 2500000;

  int32_t count = 0;

  tmq_topic_assignment* pAssign = NULL;
  int32_t               numOfAssign = 0;

  int32_t code = tmq_get_topic_assignment(tmq, "t1", &pAssign, &numOfAssign);
  if (code != 0) {
    (void)printf("error occurs:%s\n", tmq_err2str(code));
    (void)tmq_consumer_close(tmq);
    taos_close(pConn);
    (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
    return;
  }

  (void)tmq_offset_seek(tmq, "t1", pAssign[0].vgId, 4);

  code = tmq_get_topic_assignment(tmq, "t1", &pAssign, &numOfAssign);
  if (code != 0) {
    (void)printf("error occurs:%s\n", tmq_err2str(code));
    (void)tmq_consumer_close(tmq);
    taos_close(pConn);
    (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
    return;
  }

  while (1) {
    TAOS_RES* pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      char buf[128] = {0};

      const char* topicName = tmq_get_topic_name(pRes);
      //      const char* dbName = tmq_get_db_name(pRes);
      //      int32_t     vgroupId = tmq_get_vgroup_id(pRes);
      //
      //      (void)printf("topic: %s\n", topicName);
      //      (void)printf("db: %s\n", dbName);
      //      (void)printf("vgroup id: %d\n", vgroupId);

      printSubResults(pRes, &totalRows);
    } else {
      //      tmq_offset_seek(tmq, "topic_t1", pAssign[0].vgroupHandle, pAssign[0].begin);
      //      break;
    }

    (void)tmq_commit_sync(tmq, pRes);
    if (pRes != NULL) {
      taos_free_result(pRes);
      //      if ((++count) > 1) {
      //        break;
      //      }
    } else {
      break;
    }

    (void)tmq_offset_seek(tmq, "topic_t1", pAssign[0].vgId, pAssign[0].begin);
  }

  (void)tmq_consumer_close(tmq);
  taos_close(pConn);
  (void)fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}

TEST(clientCase, sub_tb_mt_test) {
  char* user = NULL;
  char* auth = NULL;
  char* ip = NULL;
  int   port = 0;
  char  key[512] = {0};
  (void)snprintf(key, sizeof(key), "%s:%s:%s:%d", user, auth, ip, port);

  (void)taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");
  TdThread qid[20] = {0};

  for (int32_t i = 0; i < 1; ++i) {
    (void)taosThreadCreate(&qid[i], NULL, doConsumeData, NULL);
  }

  for (int32_t i = 0; i < 1; ++i) {
    (void)taosThreadJoin(qid[i], NULL);
  }
}

TEST(clientCase, timezone_Test) {
  {
    // taos_options(  TSDB_OPTION_TIMEZONE, "UTC-8");
    int code = taos_options(TSDB_OPTION_TIMEZONE, "UTC-8");
    ASSERT_TRUE(code == 0);
    TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
    ASSERT_NE(pConn, nullptr);

    TAOS_RES* pRes = taos_query(pConn, "drop database if exists db_timezone");
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);

    pRes = taos_query(pConn, "create database db_timezone");
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);

    pRes = taos_query(pConn, "create table db_timezone.t1 (ts timestamp, v int)");
    while (taos_errno(pRes) == TSDB_CODE_MND_DB_IN_CREATING || taos_errno(pRes) == TSDB_CODE_MND_DB_IN_DROPPING) {
      taosMsleep(2000);
      pRes = taos_query(pConn, "create table db_timezone.t1 (ts timestamp, v int)");
    }
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);

    char sql[256] = {0};
    (void)sprintf(sql, "insert into db_timezone.t1 values('2023-09-16 17:00:00', 1)");
    pRes = taos_query(pConn, sql);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);

    pRes = taos_query(pConn, "select * from db_timezone.t1 where ts == '2023-09-16 17:00:00'");
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);

    TAOS_ROW    pRow = NULL;
    TAOS_FIELD* pFields = taos_fetch_fields(pRes);
    int32_t     numOfFields = taos_num_fields(pRes);

    char str[512] = {0};
    int  rows = 0;
    while ((pRow = taos_fetch_row(pRes)) != NULL) {
      rows++;
    }
    ASSERT_TRUE(rows == 1);

    taos_free_result(pRes);

    taos_close(pConn);
  }

  {
    // taos_options(  TSDB_OPTION_TIMEZONE, "UTC+8");
    int code = taos_options(TSDB_OPTION_TIMEZONE, "UTC+8");
    ASSERT_TRUE(code == 0);
    TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
    ASSERT_NE(pConn, nullptr);

    TAOS_RES* pRes = taos_query(pConn, "select * from db_timezone.t1 where ts == '2023-09-16 01:00:00'");
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);

    TAOS_ROW    pRow = NULL;
    TAOS_FIELD* pFields = taos_fetch_fields(pRes);
    int32_t     numOfFields = taos_num_fields(pRes);

    int  rows = 0;
    char str[512] = {0};
    while ((pRow = taos_fetch_row(pRes)) != NULL) {
      rows++;
    }
    ASSERT_TRUE(rows == 1);

    taos_free_result(pRes);

    char sql[256] = {0};
    (void)sprintf(sql, "insert into db_timezone.t1 values('2023-09-16 17:00:01', 1)");
    pRes = taos_query(pConn, sql);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);

    taos_free_result(pRes);

    taos_close(pConn);
  }

  {
    // taos_options(  TSDB_OPTION_TIMEZONE, "UTC+0");
    int code = taos_options(TSDB_OPTION_TIMEZONE, "UTC+0");
    ASSERT_TRUE(code == 0);
    TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
    ASSERT_NE(pConn, nullptr);

    TAOS_RES* pRes = taos_query(pConn, "select * from db_timezone.t1 where ts == '2023-09-16 09:00:00'");
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);

    TAOS_ROW    pRow = NULL;
    TAOS_FIELD* pFields = taos_fetch_fields(pRes);
    int32_t     numOfFields = taos_num_fields(pRes);

    int  rows = 0;
    char str[512] = {0};
    while ((pRow = taos_fetch_row(pRes)) != NULL) {
      rows++;
    }
    ASSERT_TRUE(rows == 1);
    taos_free_result(pRes);

    {
      TAOS_RES* pRes = taos_query(pConn, "select * from db_timezone.t1 where ts == '2023-09-17 01:00:01'");
      ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);

      TAOS_ROW    pRow = NULL;
      TAOS_FIELD* pFields = taos_fetch_fields(pRes);
      int32_t     numOfFields = taos_num_fields(pRes);

      int  rows = 0;
      char str[512] = {0};
      while ((pRow = taos_fetch_row(pRes)) != NULL) {
        rows++;
      }
      ASSERT_TRUE(rows == 1);
      taos_free_result(pRes);
    }

    taos_close(pConn);
  }
}

void initTestEnv(const char* database, const char* stb, TAOS** pConnect, TAOS** pUserConnect, char userBuf[]) {
  int32_t code = 0;

  const char* maxVnodeDB = "maxVnod";
  TAOS *      pRootConn = nullptr, *pUserConn = nullptr;
  char        sql[128] = {0};
  pRootConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pRootConn, nullptr);

  sprintf(sql, "drop database if exists %s", database);
  TAOS_RES* pRes = taos_query(pRootConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  sprintf(sql, "create database %s vgroups 4", database);
  pRes = taos_query(pRootConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  sprintf(sql, "create stable %s.%s (ts timestamp, v int) tags (t1 int)", database, stb);
  pRes = taos_query(pRootConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  char buf[512] = {0};
  for (int32_t i = 0; i < 10; i++) {
    char tbname[24] = {0};
    sprintf(tbname, "test_tbname_%d", i);
    sprintf(buf, "insert into %s.%s using %s.%s tags(%d) values(now, %d)", database, tbname, database, stb, i, i);
    pRes = taos_query(pRootConn, buf);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  char* tempUser = "control_user";
  sprintf(buf, "create user if not exists %s pass \'AAbb1122\'", tempUser);
  pRes = taos_query(pRootConn, buf);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  memcpy(userBuf, tempUser, strlen(tempUser));

  *pConnect = pRootConn;

  {
    sprintf(buf, "grant all on %s.* to %s", database, tempUser);
    TAOS_RES* pRes = taos_query(pRootConn, buf);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  {
    sprintf(buf, "grant use on database %s to %s", database, tempUser);
    TAOS_RES* pRes = taos_query(pRootConn, buf);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  {
    int32_t count = 10;
    bool    privilegeOk = false;
    sprintf(buf, "select * from information_schema.ins_user_privileges where user_name=\'%s\' and db_name=\'%s\'",
            tempUser, database);
    while (count--) {
      TAOS_RES* pRes = taos_query(pRootConn, buf);
      ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
      int32_t nResults = getNumOfRows(pRes);
      if (nResults >= 11) {
        privilegeOk = true;
        printf("privilege ok, waited %d, remain count:%d, nResults:%d, sql:%s\n", 10 - count, count, nResults, buf);
        break;
      }
      taos_free_result(pRes);
      printf("privilege not ok, wait 1s and retry, remain count:%d, nResults:%d, sql:%s\n", count, nResults, buf);
      taosMsleep(1000);
    }
    ASSERT_TRUE(privilegeOk);
  }

  pUserConn = taos_connect("localhost", tempUser, "AAbb1122", NULL, 0);
  ASSERT_NE(pUserConn, nullptr);
  *pUserConnect = pUserConn;
}

void testSessionPerUser() {
  int32_t code = 0;
  char*   databName = "test_session_per_user";
  char*   rstb = "stb1";
  char    userBuf[32] = {0};
  TAOS *  pRootConn = nullptr, *pUserConn = nullptr;
  initTestEnv(databName, rstb, &pRootConn, &pUserConn, userBuf);

  {
    TAOS_RES* pRes = taos_query(pRootConn, "create database if not exists tests");
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  {
    SArray* p = taosArrayInit(1, sizeof(void*));
    ASSERT_NE(p, nullptr);

    char buf[128] = {0};
    sprintf(buf, "alter user %s SESSION_PER_USER 10", userBuf);
    TAOS_RES* pRes = taos_query(pRootConn, buf);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);

    taosMsleep(6100);
    for (int32_t i = 0; i < 10; i++) {
      TAOS* pUserConn = taos_connect("localhost", userBuf, "AAbb1122", NULL, 0);
      taosArrayPush(p, &pUserConn);
    }

    {
      TAOS* pUserConn = taos_connect("localhost", userBuf, "AAbb1122", NULL, 0);
      ASSERT_EQ(pUserConn, nullptr);
    }

    for (int32_t i = 0; i < 10; i++) {
      TAOS* pUserConn = *(TAOS**)taosArrayGet(p, i);
      taos_close(pUserConn);
    }

    taosArrayDestroy(p);
    {
      taosMsleep(6200);
      TAOS* pUserConn = taos_connect("localhost", userBuf, "AAbb1122", NULL, 0);
      ASSERT_NE(pUserConn, nullptr);
      taos_close(pUserConn);
    }
  }

  {
    char buf[128] = {0};
    sprintf(buf, "alter user %s SESSION_PER_USER 2000", userBuf);
    TAOS_RES* pRes = taos_query(pRootConn, buf);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  taos_close(pRootConn);
  taos_close(pUserConn);
  // taosMsleep(6100);
}
void testSessionConnTime() {
  int32_t     code = 0;
  TAOS *      pRootConn = nullptr, *pUserConn = nullptr;
  const char* databName = "db_conn";
  const char* rstb = "db_conn_time";
  char        userBuf[32] = {0};
  initTestEnv(databName, rstb, &pRootConn, &pUserConn, userBuf);

  {
    char sql[128] = {0};
    sprintf(sql, "select * from %s.%s", databName, rstb);

    // TAOS_RES* pRes = taos_query(pUserConn, sql);
    // ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    // taos_free_result(pRes);
    code = queryDB(pUserConn, sql);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  {
    char sql[128] = {0};
    sprintf(sql, "alter user %s connect_time 1", userBuf);
    TAOS_RES* pRes = taos_query(pRootConn, sql);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  taosMsleep(61000);
  {
    char sql[128] = {0};
    sprintf(sql, "select * from %s.%s", databName, rstb);

    TAOS_RES* pRes = taos_query(pUserConn, sql);
    ASSERT_NE(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  {
    char sql[128] = {0};
    sprintf(sql, "alter user %s connect_time 10000", userBuf);
    TAOS_RES* pRes = taos_query(pRootConn, sql);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  taos_close(pRootConn);
  taos_close(pUserConn);
}

void testSessionConnIdleTime() {
  int32_t     code = 0;
  TAOS *      pRootConn = nullptr, *pUserConn = nullptr;
  const char* databName = "db_conn_idle";
  const char* rstb = "db_conn_time_idle";
  char        userBuf[32] = {0};
  initTestEnv(databName, rstb, &pRootConn, &pUserConn, userBuf);

  {
    char sql[128] = {0};
    sprintf(sql, "select * from %s.%s", databName, rstb);

    // TAOS_RES* pRes = taos_query(pUserConn, sql);
    // ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    // taos_free_result(pRes);
    code = queryDB(pUserConn, sql);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  {
    char sql[128] = {0};
    sprintf(sql, "alter user %s connect_idle_time 1", userBuf);
    TAOS_RES* pRes = taos_query(pRootConn, sql);
    taos_free_result(pRes);
  }

  taosMsleep(61000);
  {
    char sql[128] = {0};
    sprintf(sql, "select * from %s.%s", databName, rstb);

    TAOS_RES* pRes = taos_query(pUserConn, sql);
    ASSERT_NE(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  {
    char sql[128] = {0};
    sprintf(sql, "alter user %s connect_idel_time 1000", userBuf);
    TAOS_RES* pRes = taos_query(pRootConn, sql);
    taos_free_result(pRes);
  }
  taos_close(pRootConn);
  taos_close(pUserConn);
}

void testSessionMaxVnodeCall() {
  int32_t     code = 0;
  TAOS *      pRootConn = nullptr, *pUserConn = nullptr;
  const char* databName = "db_max_vnode";
  const char* rstb = "stb1";
  char        userBuf[32] = {0};
  initTestEnv(databName, rstb, &pRootConn, &pUserConn, userBuf);

  char sql[128] = {0};
  sprintf(sql, "select * from %s.%s", databName, rstb);

  // TAOS_RES* pRes = taos_query(pUserConn, sql);
  // ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  // taos_free_result(pRes);
  code = queryDB(pUserConn, sql);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  sprintf(sql, "alter user %s CALL_PER_SESSION 2", userBuf);
  TAOS_RES* pRes = taos_query(pRootConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  // taosMsleep(6100);

  // {
  //   char sql[128] = {0};
  //   sprintf(sql, "select * from %s.%s", databName, rstb);

  //   TAOS_RES* pRes = taos_query(pUserConn, sql);
  //   ASSERT_NE(taos_errno(pRes), TSDB_CODE_SUCCESS);
  //   taos_free_result(pRes);
  // }

  sprintf(sql, "alter user %s CALL_PER_SESSION 1023", userBuf);
  pRes = taos_query(pRootConn, sql);
  taos_free_result(pRes);

  taos_close(pRootConn);
  taos_close(pUserConn);
}

void testSessionConncurentCall() {
  int32_t code = 0;
  TAOS *  pRootConn = nullptr, *pUserConn = nullptr;

  const char* db = "conn_data";
  const char* stb = "conn_stp";
  char        sql[256] = {0};

  char userBuf[32] = {0};
  initTestEnv(db, stb, &pRootConn, &pUserConn, userBuf);
  sprintf(sql, "select * from %s.%s", db, stb);

  // TAOS_RES* pRes = taos_query(pUserConn, sql);
  // ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  // taos_free_result(pRes);
  code = queryDB(pUserConn, sql);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  sprintf(sql, "alter user %s VNODE_PER_CALL 2", userBuf);
  TAOS_RES* pRes = taos_query(pRootConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  taosMsleep(6100);

  sprintf(sql, "select * from %s.%s", db, stb);
  pRes = taos_query(pUserConn, sql);
  ASSERT_NE(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  sprintf(sql, "alter user %s VNODE_PER_CALL 1023", userBuf);
  pRes = taos_query(pRootConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);
  // taosMsleep(6100);
  taos_close(pRootConn);
  taos_close(pUserConn);
}

void testSessionCtrl() {
  testSessionPerUser();
  testSessionConnTime();
  testSessionConnIdleTime();
  testSessionConncurentCall();
  testSessionMaxVnodeCall();
}

#pragma GCC diagnostic pop
