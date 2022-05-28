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
#include <iostream>
#include "stub.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat"

#include <addr_any.h>

#pragma GCC diagnostic pop

#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif
#include "mockCatalog.h"
namespace {

void generateInformationSchema(MockCatalogService* mcs) {
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "dnodes", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("id", TSDB_DATA_TYPE_INT);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "mnodes", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("id", TSDB_DATA_TYPE_INT);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "modules", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("id", TSDB_DATA_TYPE_INT);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "qnodes", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("id", TSDB_DATA_TYPE_INT);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "user_databases", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "user_functions", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_FUNC_NAME_LEN);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "user_indexes", TSDB_SYSTEM_TABLE, 2)
                                 .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
                                 .addColumn("table_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "user_stables", TSDB_SYSTEM_TABLE, 2)
                                 .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
                                 .addColumn("stable_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "user_tables", TSDB_SYSTEM_TABLE, 2)
                                 .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
                                 .addColumn("table_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN);
    builder.done();
  }
  {
    ITableBuilder& builder =
        mcs->createTableBuilder("information_schema", "user_table_distributed", TSDB_SYSTEM_TABLE, 1)
            .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "user_users", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("user_name", TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("information_schema", "vgroups", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN);
    builder.done();
  }
}

void generatePerformanceSchema(MockCatalogService* mcs) {
  {
    ITableBuilder& builder = mcs->createTableBuilder("performance_schema", "trans", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("id", TSDB_DATA_TYPE_INT);
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("performance_schema", "streams", TSDB_SYSTEM_TABLE, 1)
                                 .addColumn("stream_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN);
    builder.done();
  }
}

/*
 * Table:t1
 *        Field        |        Type        |      DataType      |  Bytes   |
 * ==========================================================================
 *          ts         |       column       |     TIMESTAMP      |    8     |
 *          c1         |       column       |        INT         |    4     |
 *          c2         |       column       |      VARCHAR       |    20    |
 *          c3         |       column       |       BIGINT       |    8     |
 *          c4         |       column       |       DOUBLE       |    8     |
 *          c5         |       column       |       DOUBLE       |    8     |
 */
void generateTestT1(MockCatalogService* mcs) {
  ITableBuilder& builder = mcs->createTableBuilder("test", "t1", TSDB_NORMAL_TABLE, 6)
                               .setPrecision(TSDB_TIME_PRECISION_MILLI)
                               .setVgid(1)
                               .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
                               .addColumn("c1", TSDB_DATA_TYPE_INT)
                               .addColumn("c2", TSDB_DATA_TYPE_BINARY, 20)
                               .addColumn("c3", TSDB_DATA_TYPE_BIGINT)
                               .addColumn("c4", TSDB_DATA_TYPE_DOUBLE)
                               .addColumn("c5", TSDB_DATA_TYPE_DOUBLE);
  builder.done();
}

/*
 * Super Table: st1
 *        Field        |        Type        |      DataType      |  Bytes   |
 * ==========================================================================
 *          ts         |       column       |     TIMESTAMP      |    8     |
 *          c1         |       column       |        INT         |    4     |
 *          c2         |       column       |      VARCHAR       |    20    |
 *         tag1        |        tag         |        INT         |    4     |
 *         tag2        |        tag         |      VARCHAR       |    20    |
 * Child Table: st1s1, st1s2
 */
void generateTestST1(MockCatalogService* mcs) {
  ITableBuilder& builder = mcs->createTableBuilder("test", "st1", TSDB_SUPER_TABLE, 3, 2)
                               .setPrecision(TSDB_TIME_PRECISION_MILLI)
                               .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
                               .addColumn("c1", TSDB_DATA_TYPE_INT)
                               .addColumn("c2", TSDB_DATA_TYPE_BINARY, 20)
                               .addTag("tag1", TSDB_DATA_TYPE_INT)
                               .addTag("tag2", TSDB_DATA_TYPE_BINARY, 20);
  builder.done();
  mcs->createSubTable("test", "st1", "st1s1", 1);
  mcs->createSubTable("test", "st1", "st1s2", 2);
  mcs->createSubTable("test", "st1", "st1s3", 1);
}

void generateFunctions(MockCatalogService* mcs) {
  mcs->createFunction("udf1", TSDB_FUNC_TYPE_SCALAR, TSDB_DATA_TYPE_INT, tDataTypes[TSDB_DATA_TYPE_INT].bytes, 0);
  mcs->createFunction("udf2", TSDB_FUNC_TYPE_AGGREGATE, TSDB_DATA_TYPE_DOUBLE, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes,
                      8);
}

}  // namespace

int32_t __catalogGetHandle(const char* clusterId, struct SCatalog** catalogHandle) { return 0; }

int32_t __catalogGetTableMeta(struct SCatalog* pCatalog, void* pRpc, const SEpSet* pMgmtEps, const SName* pTableName,
                              STableMeta** pTableMeta) {
  return g_mockCatalogService->catalogGetTableMeta(pTableName, pTableMeta);
}

int32_t __catalogGetTableHashVgroup(struct SCatalog* pCatalog, void* pRpc, const SEpSet* pMgmtEps,
                                    const SName* pTableName, SVgroupInfo* vgInfo) {
  return g_mockCatalogService->catalogGetTableHashVgroup(pTableName, vgInfo);
}

int32_t __catalogGetTableDistVgInfo(SCatalog* pCtg, void* pRpc, const SEpSet* pMgmtEps, const SName* pTableName,
                                    SArray** pVgList) {
  return g_mockCatalogService->catalogGetTableDistVgInfo(pTableName, pVgList);
}

int32_t __catalogGetDBVgVersion(SCatalog* pCtg, const char* dbFName, int32_t* version, int64_t* dbId,
                                int32_t* tableNum) {
  return 0;
}

int32_t __catalogGetDBVgInfo(SCatalog* pCtg, void* pRpc, const SEpSet* pMgmtEps, const char* dbFName,
                             SArray** vgroupList) {
  return 0;
}

int32_t __catalogGetDBCfg(SCatalog* pCtg, void* pRpc, const SEpSet* pMgmtEps, const char* dbFName, SDbCfgInfo* pDbCfg) {
  return 0;
}

int32_t __catalogChkAuth(SCatalog* pCtg, void* pRpc, const SEpSet* pMgmtEps, const char* user, const char* dbFName,
                         AUTH_TYPE type, bool* pass) {
  *pass = true;
  return 0;
}

int32_t __catalogGetUdfInfo(SCatalog* pCtg, void* pTrans, const SEpSet* pMgmtEps, const char* funcName,
                            SFuncInfo* pInfo) {
  return g_mockCatalogService->catalogGetUdfInfo(funcName, pInfo);
}

void initMetaDataEnv() {
  g_mockCatalogService.reset(new MockCatalogService());

  static Stub stub;
  stub.set(catalogGetHandle, __catalogGetHandle);
  stub.set(catalogGetTableMeta, __catalogGetTableMeta);
  stub.set(catalogGetSTableMeta, __catalogGetTableMeta);
  stub.set(catalogGetTableHashVgroup, __catalogGetTableHashVgroup);
  stub.set(catalogGetTableDistVgInfo, __catalogGetTableDistVgInfo);
  stub.set(catalogGetDBVgVersion, __catalogGetDBVgVersion);
  stub.set(catalogGetDBVgInfo, __catalogGetDBVgInfo);
  stub.set(catalogGetDBCfg, __catalogGetDBCfg);
  stub.set(catalogChkAuth, __catalogChkAuth);
  stub.set(catalogGetUdfInfo, __catalogGetUdfInfo);
  // {
  //   AddrAny any("libcatalog.so");
  //   std::map<std::string,void*> result;
  //   any.get_global_func_addr_dynsym("^catalogGetHandle$", result);
  //   for (const auto& f : result) {
  //     stub.set(f.second, __catalogGetHandle);
  //   }
  // }
  // {
  //   AddrAny any("libcatalog.so");
  //   std::map<std::string,void*> result;
  //   any.get_global_func_addr_dynsym("^catalogGetTableMeta$", result);
  //   for (const auto& f : result) {
  //     stub.set(f.second, __catalogGetTableMeta);
  //   }
  // }
  // {
  //   AddrAny any("libcatalog.so");
  //   std::map<std::string,void*> result;
  //   any.get_global_func_addr_dynsym("^catalogGetTableHashVgroup$", result);
  //   for (const auto& f : result) {
  //     stub.set(f.second, __catalogGetTableHashVgroup);
  //   }
  // }
  // {
  //   AddrAny any("libcatalog.so");
  //   std::map<std::string,void*> result;
  //   any.get_global_func_addr_dynsym("^catalogGetTableDistVgInfo$", result);
  //   for (const auto& f : result) {
  //     stub.set(f.second, __catalogGetTableDistVgInfo);
  //   }
  // }
  // {
  //   AddrAny any("libcatalog.so");
  //   std::map<std::string,void*> result;
  //   any.get_global_func_addr_dynsym("^catalogGetDBVgVersion$", result);
  //   for (const auto& f : result) {
  //     stub.set(f.second, __catalogGetDBVgVersion);
  //   }
  // }
}

void generateMetaData() {
  generateInformationSchema(g_mockCatalogService.get());
  generatePerformanceSchema(g_mockCatalogService.get());
  generateTestT1(g_mockCatalogService.get());
  generateTestST1(g_mockCatalogService.get());
  generateFunctions(g_mockCatalogService.get());
  g_mockCatalogService->showTables();
}

void destroyMetaDataEnv() { g_mockCatalogService.reset(); }
