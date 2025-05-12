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

#ifndef _TD_TEST_BASE_H_
#define _TD_TEST_BASE_H_

#include <gtest/gtest.h>
#include "os.h"

#include "dnode.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tmsg.h"
#include "trpc.h"
#include "tthread.h"

#include "client.h"
#include "server.h"
#include "tlog.h"

class Testbase {
 public:
  void     Init(const char* path, int16_t port);
  void     Cleanup();
  void     Restart();
  void     ServerStop();
  void     ServerStart();
  void     ClientRestart();
  SRpcMsg* SendReq(tmsg_t msgType, void* pCont, int32_t contLen);
  void     InitLog(const char* path);

 private:
  TestServer server;
  TestClient client;
  int32_t    connId;

 public:
  int32_t SendShowReq(int8_t showType, const char* tb, const char* db);
  int32_t GetShowRows();

#if 0
  int32_t     GetMetaNum();
  const char* GetMetaTbName();
  int8_t      GetMetaType(int32_t col);
  int32_t     GetMetaBytes(int32_t col);
  const char* GetMetaName(int32_t col);

  int8_t      GetShowInt8(int32_t row, int32_t col);
  int16_t     GetShowInt16(int32_t row, int32_t col);
  int32_t     GetShowInt32(int32_t row, int32_t col);
  int64_t     GetShowInt64(int32_t row, int32_t col);
  int64_t     GetShowTimestamp(int32_t row, int32_t col);
  const char* GetShowBinary(int32_t row, int32_t col);
#endif

 private:
  SRetrieveMetaTableRsp* showRsp;
};

#if 0

#define CHECK_META(tbName, numOfColumns)        \
  {                                             \
    EXPECT_EQ(test.GetMetaNum(), numOfColumns); \
    EXPECT_STREQ(test.GetMetaTbName(), tbName); \
  }

#define CHECK_SCHEMA(col, type, bytes, colName)   \
  {                                               \
    EXPECT_EQ(test.GetMetaType(col), type);       \
    EXPECT_EQ(test.GetMetaBytes(col), bytes);     \
    EXPECT_STREQ(test.GetMetaName(col), colName); \
  }

#define CheckBinary(row, col, val) \
  { EXPECT_STREQ(test.GetShowBinary(row, col), val); }

#define CheckInt8(val) \
  { EXPECT_EQ(test.GetShowInt8(row, col), val); }

#define CheckInt16(val) \
  { EXPECT_EQ(test.GetShowInt16(row, col), val); }

#define CheckInt32(val) \
  { EXPECT_EQ(test.GetShowInt32row, col(), val); }

#define CheckInt64(val) \
  { EXPECT_EQ(test.GetShowInt64(row, col), val); }

#define CheckTimestamp() \
  { EXPECT_GT(test.GetShowTimestamp(row, col), 0); }

#endif

#endif /* _TD_TEST_BASE_H_ */
