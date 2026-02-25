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

#include "executor.h"
#include "taos.h"

void initEnv() {
  char  **list;
  int32_t count;
  int32_t code = taos_list_instances(NULL, &list, &count);
  if (code != TSDB_CODE_SUCCESS || count == 0) {
    return;
  }
  for (int32_t i = 0; i < count; i++) {
    code = taos_register_instance(list[i], NULL, NULL, -1);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  taos_free_instances(&list, count);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(instanceCase, normal) {
  // init env
  initEnv();
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  // taos_register_instance basic
  int64_t startTs = taosGetTimestampMs();
  int32_t code = taos_register_instance("taosadapter-1001", "taosadapter", "desc:test_instance", 100);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = taos_register_instance("taosadapter-1002", "taosadapter", "desc:test_instance", 100);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = taos_register_instance("taosc-1", "taosc", "desc:test_instance", 100);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = taos_register_instance("taosc-2", "taosc", "desc:test_instance", 100);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  int64_t endTs = taosGetTimestampMs();

  // taos_register_instance type filter
  char  **list;
  int32_t count;
  code = taos_list_instances("taosadapter", &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(list, nullptr);
  ASSERT_EQ(count, 2);
  ASSERT_EQ(strcmp(list[0], "taosadapter-1001") == 0 || strcmp(list[0], "taosadapter-1002") == 0, true);
  ASSERT_EQ(strcmp(list[1], "taosadapter-1001") == 0 || strcmp(list[1], "taosadapter-1002") == 0, true);
  taos_free_instances(&list, count);

  code = taos_list_instances("taosc", &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(list, nullptr);
  ASSERT_EQ(count, 2);
  ASSERT_EQ(strcmp(list[0], "taosc-1") == 0 || strcmp(list[0], "taosc-2") == 0, true);
  ASSERT_EQ(strcmp(list[1], "taosc-1") == 0 || strcmp(list[1], "taosc-2") == 0, true);
  taos_free_instances(&list, count);

  // taos_register_instance no type filter
  code = taos_list_instances(NULL, &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(list, nullptr);
  ASSERT_EQ(count, 4);
  for (int i = 0; i < count; i++) {
    ASSERT_EQ(strcmp(list[i], "taosadapter-1001") == 0 || strcmp(list[i], "taosadapter-1002") == 0 ||
                  strcmp(list[i], "taosc-1") == 0 || strcmp(list[i], "taosc-2") == 0,
              true);
  }
  taos_free_instances(&list, count);

  code = taos_list_instances("not_exist_type", &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(count, 0);

  // query perf_instances
  TAOS_RES *result = taos_query(taos, "select * from performance_schema.perf_instances order by id");
  ASSERT_NE(result, nullptr);
  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char *)row[0], "taosadapter-1001", 16), 0);
  ASSERT_EQ(strncmp((char *)row[1], "taosadapter", 11), 0);
  ASSERT_EQ(strncmp((char *)row[2], "desc:test_instance", 18), 0);
  ASSERT_EQ(*(int64_t *)row[3], *(int64_t *)row[4]);
  ASSERT_LE(*(int64_t *)row[3], endTs);
  ASSERT_GE(*(int64_t *)row[3], startTs);
  ASSERT_EQ(*(int *)row[5], 100);

  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char *)row[0], "taosadapter-1002", 16), 0);
  ASSERT_EQ(strncmp((char *)row[1], "taosadapter", 11), 0);
  ASSERT_EQ(strncmp((char *)row[2], "desc:test_instance", 18), 0);
  ASSERT_EQ(*(int64_t *)row[3], *(int64_t *)row[4]);
  ASSERT_LE(*(int64_t *)row[3], endTs);
  ASSERT_GE(*(int64_t *)row[3], startTs);
  ASSERT_EQ(*(int *)row[5], 100);

  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char *)row[0], "taosc-1", 5), 0);
  ASSERT_EQ(strncmp((char *)row[1], "taosc", 5), 0);
  ASSERT_EQ(strncmp((char *)row[2], "desc:test_instance", 18), 0);
  ASSERT_EQ(*(int64_t *)row[3], *(int64_t *)row[4]);
  ASSERT_LE(*(int64_t *)row[3], endTs);
  ASSERT_GE(*(int64_t *)row[3], startTs);
  ASSERT_EQ(*(int *)row[5], 100);

  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(strncmp((char *)row[0], "taosc-2", 5), 0);
  ASSERT_EQ(strncmp((char *)row[1], "taosc", 5), 0);
  ASSERT_EQ(strncmp((char *)row[2], "desc:test_instance", 18), 0);
  ASSERT_EQ(*(int64_t *)row[3], *(int64_t *)row[4]);
  ASSERT_LE(*(int64_t *)row[3], endTs);
  ASSERT_GE(*(int64_t *)row[3], startTs);
  ASSERT_EQ(*(int *)row[5], 100);
  taos_free_result(result);

  //  show instances
  result = taos_query(taos, "show instances");
  ASSERT_NE(result, nullptr);
  int cnt = 0;
  row = taos_fetch_row(result);
  while (row != nullptr) {
    ASSERT_EQ(strncmp((char *)row[0], "taosadapter-1001", 16) == 0 ||
                  strncmp((char *)row[0], "taosadapter-1002", 16) == 0 || strncmp((char *)row[0], "taosc-1", 5) == 0 ||
                  strncmp((char *)row[0], "taosc-2", 5) == 0,
              true);
    row = taos_fetch_row(result);
    cnt++;
  }
  ASSERT_EQ(cnt, 4);
  taos_free_result(result);

  result = taos_query(taos, "show instances");
  ASSERT_NE(result, nullptr);
  cnt = 0;
  row = taos_fetch_row(result);
  while (row != nullptr) {
    ASSERT_EQ(strncmp((char *)row[0], "taosadapter-1001", 16) == 0 ||
                  strncmp((char *)row[0], "taosadapter-1002", 16) == 0 || strncmp((char *)row[0], "taosc-1", 5) == 0 ||
                  strncmp((char *)row[0], "taosc-2", 5) == 0,
              true);
    row = taos_fetch_row(result);
    cnt++;
  }
  ASSERT_EQ(cnt, 4);
  taos_free_result(result);

  //  show instances like
  result = taos_query(taos, "show instances like 'taosadapter%'");
  ASSERT_NE(result, nullptr);
  cnt = 0;
  row = taos_fetch_row(result);
  while (row != nullptr) {
    ASSERT_EQ(
        strncmp((char *)row[0], "taosadapter-1001", 16) == 0 || strncmp((char *)row[0], "taosadapter-1002", 16) == 0,
        true);
    row = taos_fetch_row(result);
    cnt++;
  }
  ASSERT_EQ(cnt, 2);
  taos_free_result(result);

  result = taos_query(taos, "show instances like 'taosc%'");
  ASSERT_NE(result, nullptr);
  cnt = 0;
  row = taos_fetch_row(result);
  while (row != nullptr) {
    ASSERT_EQ(strncmp((char *)row[0], "taosc-1", 5) == 0 || strncmp((char *)row[0], "taosc-2", 5) == 0, true);
    row = taos_fetch_row(result);
    cnt++;
  }
  ASSERT_EQ(cnt, 2);
  taos_free_result(result);

  taos_close(taos);
}

TEST(instanceCase, expire) {
  initEnv();

  int32_t code = taos_register_instance("id-1", "taosadapter", "desc:test_instance", 3);
  code = taos_register_instance("id-2", "taosadapter", "desc:test_instance", 0);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  char  **list;
  int32_t count;
  code = taos_list_instances("taosadapter", &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(count, 2);
  taos_free_instances(&list, count);

  taosMsleep(10000);

  code = taos_list_instances("taosadapter", &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(strcmp(list[0], "id-2") == 0, true);
  taos_free_instances(&list, count);
}

TEST(instanceCase, update_and_delete) {
  initEnv();
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  // first register
  int64_t startTs = taosGetTimestampMs();
  int32_t code = taos_register_instance("id-1", "taosadapter", "desc:test_instance", 100);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  int64_t endTs = taosGetTimestampMs();

  char  **list;
  int32_t count;
  code = taos_list_instances("taosadapter", &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(list, nullptr);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(strcmp(list[0], "id-1") == 0, true);
  taos_free_instances(&list, count);

  int64_t   firstRegTime = 0;
  TAOS_RES *result = taos_query(taos, "select first_reg_time,last_reg_time from performance_schema.perf_instances");
  ASSERT_NE(result, nullptr);
  TAOS_ROW row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t *)row[0], *(int64_t *)row[1]);
  ASSERT_LE(*(int64_t *)row[1], endTs);
  ASSERT_GE(*(int64_t *)row[1], startTs);
  firstRegTime = *(int64_t *)row[0];
  taos_free_result(result);

  // second update
  int64_t startTs2 = taosGetTimestampMs();
  code = taos_register_instance("id-1", "taosadapter", "desc:test_instance", 100);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  int64_t endTs2 = taosGetTimestampMs();

  result = taos_query(taos, "select first_reg_time,last_reg_time from performance_schema.perf_instances");
  ASSERT_NE(result, nullptr);
  row = taos_fetch_row(result);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(*(int64_t *)row[0], firstRegTime);
  ASSERT_NE(*(int64_t *)row[1], firstRegTime);
  ASSERT_LE(*(int64_t *)row[1], endTs2);
  ASSERT_GE(*(int64_t *)row[1], startTs2);
  taos_free_result(result);

  // delect
  code = taos_register_instance("id-1", NULL, NULL, -1);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = taos_list_instances("taosadapter", &list, &count);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(count, 0);
}

#pragma GCC diagnostic pop