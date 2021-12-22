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
#include "taosmsg.h"
#include "tconfig.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tnote.h"
#include "trpc.h"
#include "tthread.h"
#include "ulog.h"

#include "client.h"
#include "server.h"

class Testbase {
 public:
  void     Init(const char* path, int16_t port);
  void     Cleanup();
  SRpcMsg* SendMsg(int8_t msgType, void* pCont, int32_t contLen);

 private:
  void InitLog(const char* path);

 private:
  TestServer server;
  TestClient client;
  int32_t    connId;

 public:
  void SendShowMetaMsg(int8_t showType);
  void SendShowRetrieveMsg();

  const char* GetShowName();
  int8_t      GetShowInt8();
  int16_t     GetShowInt16();
  int32_t     GetShowInt32();
  int64_t     GetShowInt64();
  int64_t     GetShowTimestamp();
  const char* GetShowBinary(int32_t len);
  int32_t     GetMetaColId(int32_t index);
  int8_t      GetMetaType(int32_t index);
  int32_t     GetMetaBytes(int32_t index);
  const char* GetMetaName(int32_t index);
  int32_t     GetMetaNum();
  const char* GetMetaTbName();

 private:
  int32_t            showId;
  STableMetaMsg*     pMeta;
  SRetrieveTableRsp* pRetrieveRsp;
  char*              pData;
  int32_t            pos;
};

#endif /* _TD_TEST_BASE_H_ */