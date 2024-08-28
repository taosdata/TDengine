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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "taos.h"
#include "types.h"

void* consumeThreadFunc(void* param) {
  int32_t* index = (int32_t*) param;
  tmq_conf_t* conf = tmq_conf_new();
  char groupId[64] = {0};
  int64_t t = taosGetTimestampMs();
  sprintf(groupId, "group_%"PRId64"_%d", t, *index);
  tmq_conf_set(conf, "enable.auto.commit", "false");
  tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  tmq_conf_set(conf, "group.id", groupId);
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.with.table.name", "false");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);

  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  tmq_list_append(topicList, "select_d4");

  // 启动订阅
  tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  int32_t     timeout = 200;
  int32_t totalRows = 0;
  while (1) {
    printf("start to poll\n");

    TAOS_RES *pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      int32_t rows = 0;
      void* data = NULL;
      taos_fetch_raw_block(pRes, &rows, &data);

      totalRows+=rows;
      int cols = taos_num_fields(pRes);
      for(int32_t i = 0; i < cols; ++i) {
        int64_t start = taosGetTimestampUs();
        for (int32_t j = 0; j < rows; ++j) {
          //int64_t t1 = taosGetTimestampUs();
          taos_is_null(pRes, j, i);
          //int64_t t2 = taosGetTimestampUs();
          //printf("taos_is_null  gourp:%s cost %"PRId64" us\n", groupId, t2 - t1);
        }
        int64_t end = taosGetTimestampUs();
        bool* isNULL = taosMemoryCalloc(rows, sizeof(bool));
        int code = taos_is_null_by_column(pRes, i, isNULL, &rows);
        printf("taos_fetch_raw_block gourp:%s total rows:%d cost %"PRId64" us, code:%d\n", groupId, totalRows, end - start, code);
      }

      taos_free_result(pRes);
    } else {
      printf("no data\n");
      break;
    }
  }
  tmq_consumer_close(tmq);
  return NULL;
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    printf("Usage: %s <num_of_thread>\n", argv[0]);
    return 0;
  }

  int32_t numOfThread = atoi(argv[1]);
  TdThread *thread = taosMemoryCalloc(numOfThread, sizeof(TdThread));
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  int64_t t1 = taosGetTimestampUs();
  // pthread_create one thread to consume
  int32_t* paras = taosMemoryCalloc(numOfThread, sizeof(int32_t));
  for (int32_t i = 0; i < numOfThread; ++i) {
    paras[i] = i;
    taosThreadCreate(&(thread[i]), &thattr, consumeThreadFunc, (void*)(&paras[i]));
  }

  for (int32_t i = 0; i < numOfThread; i++) {
    taosThreadJoin(thread[i], NULL);
    taosThreadClear(&thread[i]);
  }

  int64_t t2 = taosGetTimestampUs();
  printf("total cost %"PRId64" us\n", t2 - t1);
  taosMemoryFree(paras);
  return 0;
}
