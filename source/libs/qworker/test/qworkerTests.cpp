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
#include <tglobal.h>
#include <iostream>
#pragma GCC diagnostic ignored "-Wwrite-strings"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "tep.h"
#include "trpc.h"
#include "stub.h"
#include "addr_any.h"


namespace {


}

void __rpcSendRecv(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  SUseDbRsp *rspMsg = NULL; //todo

  return;
}


void initTestEnv() {
  static Stub stub;
  stub.set(rpcSendRecv, __rpcSendRecv);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
    for (const auto& f : result) {
      stub.set(f.second, __rpcSendRecv);
    }
  }
}

TEST(testCase, normalCase) {

}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



