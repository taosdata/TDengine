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
#include "catalog.h"
#include "tep.h"

typedef struct SAppInstInfo {
  int64_t           numOfConns;
  SCorEpSet         mgmtEp;
} SAppInstInfo;

typedef struct STscObj {
  char               user[TSDB_USER_LEN];
  char               pass[TSDB_PASSWORD_LEN];
  char               acctId[TSDB_ACCT_ID_LEN];
  char               db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  uint32_t           connId;
  uint64_t           id;       // ref ID returned by taosAddRef
//  struct SSqlObj    *sqlList;
  void              *pTransporter;
  pthread_mutex_t    mutex;     // used to protect the operation on db
  int32_t            numOfReqs; // number of sqlObj from this tscObj
  SAppInstInfo      *pAppInfo;
} STscObj;

namespace {

}

TEST(testCase, normalCase) {
  STscObj* pConn = (STscObj *)taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  char *clusterId = "cluster1";
  char *dbname = "db1";
  char *tablename = "table1";
  struct SCatalog* pCtg = NULL;
  void *mockPointer = (void *)0x1;
  SVgroupInfo vgInfo = {0};

  msgInit();
  
  int32_t code = catalogInit(NULL);
  ASSERT_EQ(code, 0);

  code = catalogGetHandle(clusterId, &pCtg);
  ASSERT_EQ(code, 0);

  code = catalogGetTableHashVgroup(pCtg, pConn->pTransporter, &pConn->pAppInfo->mgmtEp.epSet, dbname, tablename, &vgInfo);
  ASSERT_EQ(code, 0);

  taos_close(pConn);
}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



