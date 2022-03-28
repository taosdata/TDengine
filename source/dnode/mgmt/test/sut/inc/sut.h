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
#include "tmsg.h"
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

 private:
  void InitLog(const char* path);

 private:
  TestServer server;
  TestClient client;
  int32_t    connId;

 public:
  void SendShowMetaReq(int8_t showType, const char* db);
  void SendShowRetrieveReq();

  STableMetaRsp*     GetShowMeta();
  SRetrieveTableRsp* GetRetrieveRsp();

  int32_t     GetMetaNum();
  const char* GetMetaTbName();
  int32_t     GetMetaColId(int32_t index);
  int8_t      GetMetaType(int32_t index);
  int32_t     GetMetaBytes(int32_t index);
  const char* GetMetaName(int32_t index);

  const char* GetShowName();
  int32_t     GetShowRows();
  int8_t      GetShowInt8();
  int16_t     GetShowInt16();
  int32_t     GetShowInt32();
  int64_t     GetShowInt64();
  int64_t     GetShowTimestamp();
  const char* GetShowBinary(int32_t len);

 private:
  int64_t            showId;
  STableMetaRsp      metaRsp;
  SRetrieveTableRsp* pRetrieveRsp;
  char*              pData;
  int32_t            pos;
};

#define CHECK_META(tbName, numOfColumns)        \
  {                                             \
    EXPECT_EQ(test.GetMetaNum(), numOfColumns); \
    EXPECT_STREQ(test.GetMetaTbName(), tbName); \
  }

#define CHECK_SCHEMA(colId, type, bytes, colName)   \
  {                                                 \
    EXPECT_EQ(test.GetMetaType(colId), type);       \
    EXPECT_EQ(test.GetMetaBytes(colId), bytes);     \
    EXPECT_STREQ(test.GetMetaName(colId), colName); \
  }

#define CheckBinary(val, len) \
  { EXPECT_STREQ(test.GetShowBinary(len), val); }

#define CheckBinaryByte(b, len)                   \
  {                                               \
    char* bytes = (char*)taosMemoryCalloc(1, len);          \
    for (int32_t i = 0; i < len - 1; ++i) {       \
      bytes[i] = b;                               \
    }                                             \
    EXPECT_STREQ(test.GetShowBinary(len), bytes); \
  }

#define CheckInt8(val) \
  { EXPECT_EQ(test.GetShowInt8(), val); }

#define CheckInt16(val) \
  { EXPECT_EQ(test.GetShowInt16(), val); }

#define CheckInt32(val) \
  { EXPECT_EQ(test.GetShowInt32(), val); }

#define CheckInt64(val) \
  { EXPECT_EQ(test.GetShowInt64(), val); }

#define CheckTimestamp() \
  { EXPECT_GT(test.GetShowTimestamp(), 0); }

#define IgnoreBinary(len) \
  { test.GetShowBinary(len); }

#define IgnoreInt8() \
  { test.GetShowInt8(); }

#define IgnoreInt16() \
  { test.GetShowInt16(); }

#define IgnoreInt32() \
  { test.GetShowInt32(); }

#define IgnoreInt64() \
  { test.GetShowInt64(); }

#define IgnoreTimestamp() \
  { test.GetShowTimestamp(); }

#endif /* _TD_TEST_BASE_H_ */
