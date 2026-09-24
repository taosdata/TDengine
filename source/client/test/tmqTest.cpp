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

#include <array>
#include <cstring>
#include <iostream>
#include <taoserror.h>
#include <tglobal.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "../inc/clientInt.h"
#include "clientRawBlockWrite.h"
#include "clientTmq.h"
#include "taos.h"
#include "tdatablock.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

constexpr size_t kRawBlockFixedLen = sizeof(int32_t) * 5 + sizeof(uint64_t);
constexpr size_t kRawBlockSchemaLen = sizeof(int8_t) + sizeof(int32_t);
constexpr size_t kRawBlockColLenOffset = kRawBlockFixedLen + kRawBlockSchemaLen;
constexpr size_t kRawBlockDataOffset = kRawBlockColLenOffset + sizeof(int32_t);
constexpr size_t kRawBlockLen = kRawBlockDataOffset + sizeof(int32_t) + sizeof(bool);
using RawDataBlock = std::array<char, kRawBlockLen>;

TEST(testCase, driverInit_Test) {
  // taosInitGlobalCfg();
  //  taos_init();
}

TEST(testCase, supplemental_heartbeat_claim_Test) {
  tmq_t tmq = {0};
  tmq.consumerId = 0x1234;
  tmq.heartBeatIntervalMs = 1000;

  atomic_store_64(&tmq.lastHbReportTimeMs, 10000);
  ASSERT_FALSE(tmqTryClaimSupplementalHb(&tmq, 12000));
  ASSERT_EQ(atomic_load_64(&tmq.lastHbReportTimeMs), 10000);

  ASSERT_TRUE(tmqTryClaimSupplementalHb(&tmq, 12001));
  ASSERT_EQ(atomic_load_64(&tmq.lastHbReportTimeMs), 12001);

  ASSERT_FALSE(tmqTryClaimSupplementalHb(&tmq, 12001));
  ASSERT_EQ(atomic_load_64(&tmq.lastHbReportTimeMs), 12001);
}

TEST(testCase, supplemental_heartbeat_clock_rollback_Test) {
  tmq_t tmq = {0};
  tmq.consumerId = 0x1234;
  tmq.heartBeatIntervalMs = 1000;

  atomic_store_64(&tmq.lastHbReportTimeMs, 10000);
  ASSERT_FALSE(tmqTryClaimSupplementalHb(&tmq, 9000));
  ASSERT_EQ(atomic_load_64(&tmq.lastHbReportTimeMs), 9000);
}

static RawDataBlock buildRawDataBlock(int32_t version) {
  constexpr int32_t payloadLen = sizeof(int32_t);
  constexpr int32_t blockLen = static_cast<int32_t>(kRawBlockLen);

  RawDataBlock block = {};
  int32_t rows = 1;
  int32_t cols = 1;
  int32_t bytes = sizeof(int32_t);
  int32_t colLen = version == BLOCK_VERSION_1 ? htonl(payloadLen) : payloadLen;
  int32_t value = 42;

  std::memcpy(block.data(), &version, sizeof(version));
  std::memcpy(block.data() + sizeof(int32_t), &blockLen, sizeof(blockLen));
  std::memcpy(block.data() + sizeof(int32_t) * 2, &rows, sizeof(rows));
  std::memcpy(block.data() + sizeof(int32_t) * 3, &cols, sizeof(cols));
  block[kRawBlockFixedLen] = TSDB_DATA_TYPE_INT;
  std::memcpy(block.data() + kRawBlockFixedLen + sizeof(int8_t), &bytes, sizeof(bytes));
  std::memcpy(block.data() + kRawBlockColLenOffset, &colLen, sizeof(colLen));
  std::memcpy(block.data() + kRawBlockDataOffset, &value, sizeof(value));
  return block;
}

TEST(testCase, validate_raw_data_block_versions_Test) {
  RawDataBlock version1 = buildRawDataBlock(BLOCK_VERSION_1);
  ASSERT_EQ(tmqValidateRawDataBlock(version1.data(), version1.size()), TSDB_CODE_SUCCESS);

  auto version2 = buildRawDataBlock(BLOCK_VERSION_2);
  ASSERT_EQ(tmqValidateRawDataBlock(version2.data(), version2.size()), TSDB_CODE_SUCCESS);

  int32_t invalidVersion = BLOCK_VERSION_2 + 1;
  std::memcpy(version2.data(), &invalidVersion, sizeof(invalidVersion));
  ASSERT_EQ(tmqValidateRawDataBlock(version2.data(), version2.size()), TSDB_CODE_TMQ_INVALID_MSG);
}

TEST(testCase, invalid_raw_data_response_returns_error_Test) {
  auto invalidBlock = buildRawDataBlock(BLOCK_VERSION_2);
  int32_t invalidVersion = BLOCK_VERSION_2 + 1;
  std::memcpy(invalidBlock.data(), &invalidVersion, sizeof(invalidVersion));

  constexpr size_t kRetrieveLen = sizeof(SRetrieveTableRsp) + kRawBlockLen;
  constexpr size_t kRetrieveWords = (kRetrieveLen + sizeof(uint64_t) - 1) / sizeof(uint64_t);
  std::array<uint64_t, kRetrieveWords> retrieve = {};
  auto* retrieveRsp = reinterpret_cast<SRetrieveTableRsp*>(retrieve.data());
  retrieveRsp->useconds = RETRIEVE_TABLE_RSP_VERSION;
  std::memcpy(retrieveRsp->data, invalidBlock.data(), invalidBlock.size());

  void* rawData = retrieveRsp->data;
  terrno = TSDB_CODE_SUCCESS;
  ASSERT_EQ(tmqGetRawDataFromRes(retrieveRsp, kRetrieveLen, &rawData), TSDB_CODE_TMQ_INVALID_MSG);
  ASSERT_EQ(rawData, nullptr);
  ASSERT_EQ(terrno, TSDB_CODE_SUCCESS);
}

TEST(testCase, rejected_poll_response_retains_offset_Test) {
  SMqClientVg vg = {};
  tqOffsetResetToLog(&vg.offsetInfo.beginOffset, 10);
  tqOffsetResetToLog(&vg.offsetInfo.endOffset, 11);
  vg.offsetInfo.walVerBegin = 8;
  vg.offsetInfo.walVerEnd = 20;
  atomic_store_32(&vg.vgStatus, TMQ_VG_STATUS__WAIT);

  STqOffsetVal reqOffset = {};
  STqOffsetVal rspOffset = {};
  tqOffsetResetToLog(&reqOffset, 11);
  tqOffsetResetToLog(&rspOffset, 12);
  tmqUpdateVgInfo(&vg, &reqOffset, &rspOffset, 9, 21, 0x1234, true, false);

  ASSERT_EQ(vg.offsetInfo.beginOffset.version, 10);
  ASSERT_EQ(vg.offsetInfo.endOffset.version, 11);
  ASSERT_EQ(vg.offsetInfo.walVerBegin, 8);
  ASSERT_EQ(vg.offsetInfo.walVerEnd, 20);
  ASSERT_EQ(atomic_load_32(&vg.vgStatus), TMQ_VG_STATUS__IDLE);
}

TEST(testCase, create_topic_ctb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_TRUE(pFields == nullptr);

  int32_t numOfFields = taos_num_fields(pRes);
  ASSERT_EQ(numOfFields, 0);

  taos_free_result(pRes);

  // char* sql = "select * from tu";
  // pRes = tmq_create_topic(pConn, "test_ctb_topic_1", sql, strlen(sql));
  pRes = taos_query(pConn, "create test_ctb_topic_1 as select * from tu");
  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(testCase, create_topic_stb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  // taos_free_result(pRes);

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_TRUE(pFields == nullptr);

  int32_t numOfFields = taos_num_fields(pRes);
  ASSERT_EQ(numOfFields, 0);

  taos_free_result(pRes);

  // char* sql = "select * from st1";
  // pRes = tmq_create_topic(pConn, "test_stb_topic_1", sql, strlen(sql));
  pRes = taos_query(pConn, "create test_ctb_topic_1 as select * from st1");
  taos_free_result(pRes);
  taos_close(pConn);
}

#if 0
TEST(testCase, tmq_subscribe_ctb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, NULL);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg1");
  tmq_t* tmq = tmq_consumer_new(pConn, conf, NULL, 0);

  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "test_ctb_topic_1");
  tmq_subscribe(tmq, topic_list);

  while (1) {
    tmq_message_t* msg = tmq_consumer_poll(tmq, 1000);
    taos_free_result(msg);
    //printf("get msg\n");
    //if (msg == NULL) break;
  }
}

TEST(testCase, tmq_subscribe_stb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, NULL);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_t* tmq = tmq_consumer_new(pConn, conf, NULL, 0);

  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "test_stb_topic_1");
  tmq_subscribe(tmq, topic_list);
  
  int cnt = 1;
  while (1) {
    tmq_message_t* msg = tmq_consumer_poll(tmq, 1000);
    if (msg == NULL) continue;
    tmqShowMsg(msg);
    if (cnt++ % 10 == 0){
      tmq_commit(tmq, NULL, 0);
    }
    //tmq_commit(tmq, NULL, 0);
    taos_free_result(msg);
    //printf("get msg\n");
  }
}

TEST(testCase, tmq_consume_Test) {
}

TEST(testCase, tmq_commit_Test) {
}

#endif
