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

#include "systable.h"
namespace {

void generateInformationSchema(MockCatalogService* mcs) {
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_DNODES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("id", TSDB_DATA_TYPE_INT)
      .addColumn("endpoint", TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MNODES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("id", TSDB_DATA_TYPE_INT)
      .addColumn("endpoint", TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MODULES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("id", TSDB_DATA_TYPE_INT)
      .addColumn("endpoint", TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_QNODES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("id", TSDB_DATA_TYPE_INT)
      .addColumn("endpoint", TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_DATABASES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .addColumn("create_time", TSDB_DATA_TYPE_TIMESTAMP)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_FUNCTIONS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_FUNC_NAME_LEN)
      .addColumn("aggregate", TSDB_DATA_TYPE_INT)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_INDEXES, TSDB_SYSTEM_TABLE, 3)
      .addColumn("index_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .addColumn("table_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_STABLES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .addColumn("stable_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_TABLES, TSDB_SYSTEM_TABLE, 3)
      .addColumn("table_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .addColumn("stable_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_TABLE_DISTRIBUTED, TSDB_SYSTEM_TABLE, 2)
      .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .addColumn("table_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USERS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN)
      .addColumn("super", TSDB_DATA_TYPE_TINYINT)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VGROUPS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("vgroup_id", TSDB_DATA_TYPE_INT)
      .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_CONFIGS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_CONFIG_OPTION_LEN)
      .addColumn("value", TSDB_DATA_TYPE_BINARY, TSDB_CONFIG_VALUE_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_DNODE_VARIABLES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("dnode_id", TSDB_DATA_TYPE_INT)
      .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_CONFIG_OPTION_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_CLUSTER, TSDB_SYSTEM_TABLE, 2)
      .addColumn("id", TSDB_DATA_TYPE_BIGINT)
      .addColumn("name", TSDB_DATA_TYPE_BINARY, TSDB_CLUSTER_ID_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VNODES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("dnode_id", TSDB_DATA_TYPE_INT)
      .addColumn("dnode_ep", TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_TAGS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("table_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_COLS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("table_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .addColumn("db_name", TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USER_PRIVILEGES, TSDB_SYSTEM_TABLE, 2)
      .addColumn("user_name", TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN)
      .addColumn("privilege", TSDB_DATA_TYPE_BINARY, 10)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VIEWS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("view_name", TSDB_DATA_TYPE_BINARY, TSDB_VIEW_NAME_LEN)
      .addColumn("create_time", TSDB_DATA_TYPE_TIMESTAMP)
      .done();
}

void generatePerformanceSchema(MockCatalogService* mcs) {
  mcs->createTableBuilder(TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_TRANS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("id", TSDB_DATA_TYPE_INT)
      .addColumn("create_time", TSDB_DATA_TYPE_TIMESTAMP)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_STREAMS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("stream_name", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .addColumn("create_time", TSDB_DATA_TYPE_TIMESTAMP)
      .done();
  mcs->createTableBuilder(TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_CONSUMERS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("consumer_id", TSDB_DATA_TYPE_BIGINT)
      .addColumn("consumer_group", TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN)
      .done();
  mcs->createTableBuilder(TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_SUBSCRIPTIONS, TSDB_SYSTEM_TABLE, 2)
      .addColumn("vgroup_id", TSDB_DATA_TYPE_INT)
      .addColumn("consumer_id", TSDB_DATA_TYPE_BIGINT)
      .done();
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
void generateTestTables(MockCatalogService* mcs, const std::string& db) {
  mcs->createTableBuilder(db, "t1", TSDB_NORMAL_TABLE, 6)
      .setPrecision(TSDB_TIME_PRECISION_MILLI)
      .setVgid(2)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT)
      .addColumn("c2", TSDB_DATA_TYPE_BINARY, 20)
      .addColumn("c3", TSDB_DATA_TYPE_BIGINT)
      .addColumn("c4", TSDB_DATA_TYPE_DOUBLE)
      .addColumn("c5", TSDB_DATA_TYPE_DOUBLE)
      .done();
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
 *         tag3        |        tag         |     TIMESTAMP      |    8     |
 * Child Table: st1s1, st1s2
 *
 * Super Table: st2
 *        Field        |        Type        |      DataType      |  Bytes   |
 * ==========================================================================
 *          ts         |       column       |     TIMESTAMP      |    8     |
 *          c1         |       column       |        INT         |    4     |
 *          c2         |       column       |      VARCHAR       |    20    |
 *         jtag        |        tag         |        json        |    --    |
 * Child Table: st2s1, st2s2
 */
void generateTestStables(MockCatalogService* mcs, const std::string& db) {
  {
    ITableBuilder& builder = mcs->createTableBuilder(db, "st1", TSDB_SUPER_TABLE, 3, 3)
                                 .setPrecision(TSDB_TIME_PRECISION_MILLI)
                                 .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
                                 .addColumn("c1", TSDB_DATA_TYPE_INT)
                                 .addColumn("c2", TSDB_DATA_TYPE_BINARY, 20)
                                 .addTag("tag1", TSDB_DATA_TYPE_INT)
                                 .addTag("tag2", TSDB_DATA_TYPE_BINARY, 20)
                                 .addTag("tag3", TSDB_DATA_TYPE_TIMESTAMP);
    builder.done();
    mcs->createSubTable(db, "st1", "st1s1", 2);
    mcs->createSubTable(db, "st1", "st1s2", 3);
    mcs->createSubTable(db, "st1", "st1s3", 2);
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder(db, "st2", TSDB_SUPER_TABLE, 3, 1)
                                 .setPrecision(TSDB_TIME_PRECISION_MILLI)
                                 .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
                                 .addColumn("c1", TSDB_DATA_TYPE_INT)
                                 .addColumn("c2", TSDB_DATA_TYPE_BINARY, 20)
                                 .addTag("jtag", TSDB_DATA_TYPE_JSON);
    builder.done();
    mcs->createSubTable(db, "st2", "st2s1", 2);
    mcs->createSubTable(db, "st2", "st2s2", 3);
  }
}

void generateFunctions(MockCatalogService* mcs) {
  mcs->createFunction("udf1", TSDB_FUNC_TYPE_SCALAR, TSDB_DATA_TYPE_INT, tDataTypes[TSDB_DATA_TYPE_INT].bytes, 0);
  mcs->createFunction("udf2", TSDB_FUNC_TYPE_AGGREGATE, TSDB_DATA_TYPE_DOUBLE, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes,
                      8);
}

void generateDnodes(MockCatalogService* mcs) {
  mcs->createDnode(1, "host1", 7030);
  mcs->createDnode(2, "host2", 7030);
  mcs->createDnode(3, "host3", 7030);
}

void generateDatabases(MockCatalogService* mcs) {
  mcs->createDatabase(TSDB_INFORMATION_SCHEMA_DB);
  mcs->createDatabase(TSDB_PERFORMANCE_SCHEMA_DB);
  mcs->createDatabase("test");
  generateTestTables(g_mockCatalogService.get(), "test");
  generateTestStables(g_mockCatalogService.get(), "test");
  mcs->createDatabase("cache_db", false, 1);
  generateTestTables(g_mockCatalogService.get(), "cache_db");
  generateTestStables(g_mockCatalogService.get(), "cache_db");
  mcs->createDatabase("rollup_db", true);
  mcs->createDatabase("testus", false, 0, TSDB_TIME_PRECISION_NANO);
}

}  // namespace

int32_t __catalogGetHandle(const char* clusterId, struct SCatalog** catalogHandle) { return 0; }

int32_t __catalogGetTableMeta(struct SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName,
                              STableMeta** pTableMeta) {
  return g_mockCatalogService->catalogGetTableMeta(pTableName, pTableMeta);
}

int32_t __catalogGetCachedTableMeta(SCatalog* pCtg, const SName* pTableName, STableMeta** pTableMeta) {
  return g_mockCatalogService->catalogGetTableMeta(pTableName, pTableMeta, true);
}

int32_t __catalogGetTableHashVgroup(struct SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName,
                                    SVgroupInfo* vgInfo) {
  return g_mockCatalogService->catalogGetTableHashVgroup(pTableName, vgInfo);
}

int32_t __catalogGetCachedTableHashVgroup(SCatalog* pCtg, const SName* pTableName, SVgroupInfo* pVgroup, bool* exists) {
  int32_t code = g_mockCatalogService->catalogGetTableHashVgroup(pTableName, pVgroup, true);
  *exists = 0 != pVgroup->vgId;
  return code;
}

int32_t __catalogGetCachedTableVgMeta(SCatalog* pCtg, const SName* pTableName, SVgroupInfo* pVgroup,
                                      STableMeta** pTableMeta) {
  int32_t code = g_mockCatalogService->catalogGetTableMeta(pTableName, pTableMeta, true);
  if (code) return code;
  code = g_mockCatalogService->catalogGetTableHashVgroup(pTableName, pVgroup, true);
  return code;
}

int32_t __catalogGetTableDistVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName,
                                    SArray** pVgList) {
  return g_mockCatalogService->catalogGetTableDistVgInfo(pTableName, pVgList);
}

int32_t __catalogGetDBVgVersion(SCatalog* pCtg, const char* dbFName, int32_t* version, int64_t* dbId, int32_t* tableNum,
                                int64_t* stateTs) {
  return 0;
}

int32_t __catalogGetDBVgList(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SArray** pVgList) {
  return g_mockCatalogService->catalogGetDBVgList(dbFName, pVgList);
}

int32_t __catalogGetDBCfg(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SDbCfgInfo* pDbCfg) {
  return g_mockCatalogService->catalogGetDBCfg(dbFName, pDbCfg);
}

int32_t __catalogChkAuth(SCatalog* pCtg, SRequestConnInfo* pConn, SUserAuthInfo *pAuth, SUserAuthRes* pRes) {
  pRes->pass[0] = true;
  return 0;
}

int32_t __catalogChkAuthFromCache(SCatalog* pCtg, SUserAuthInfo *pAuth,        SUserAuthRes* pRes, bool* exists) {
  pRes->pass[0] = true;
  *exists = true;
  return 0;
}

int32_t __catalogGetUdfInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* funcName, SFuncInfo* pInfo) {
  return g_mockCatalogService->catalogGetUdfInfo(funcName, pInfo);
}

int32_t __catalogRefreshGetTableMeta(SCatalog* pCatalog, SRequestConnInfo* pConn, const SName* pTableName,
                                     STableMeta** pTableMeta, int32_t isSTable) {
  return g_mockCatalogService->catalogGetTableMeta(pTableName, pTableMeta);
}

int32_t __catalogRemoveTableMeta(SCatalog* pCtg, SName* pTableName) { return 0; }

int32_t __catalogRemoveViewMeta(SCatalog* pCtg, SName* pTableName) { return 0; }

int32_t __catalogGetTableIndex(SCatalog* pCtg, void* pTrans, const SEpSet* pMgmtEps, const SName* pName,
                               SArray** pRes) {
  return g_mockCatalogService->catalogGetTableIndex(pName, pRes);
}

int32_t __catalogGetDnodeList(SCatalog* pCatalog, SRequestConnInfo* pConn, SArray** pDnodeList) {
  return g_mockCatalogService->catalogGetDnodeList(pDnodeList);
}

int32_t __catalogRefreshGetTableCfg(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName,
                                    STableCfg** pCfg) {
  *pCfg = (STableCfg*)taosMemoryCalloc(1, sizeof(STableCfg));
  return 0;
}

void initMetaDataEnv() {
  g_mockCatalogService.reset(new MockCatalogService());

  static Stub stub;
  stub.set(catalogGetHandle, __catalogGetHandle);
  stub.set(catalogGetTableMeta, __catalogGetTableMeta);
  stub.set(catalogGetCachedTableMeta, __catalogGetCachedTableMeta);
  stub.set(catalogGetSTableMeta, __catalogGetTableMeta);
  stub.set(catalogGetCachedSTableMeta, __catalogGetCachedTableMeta);
  stub.set(catalogGetTableHashVgroup, __catalogGetTableHashVgroup);
  stub.set(catalogGetCachedTableHashVgroup, __catalogGetCachedTableHashVgroup);
  stub.set(catalogGetCachedTableVgMeta, __catalogGetCachedTableVgMeta);
  stub.set(catalogGetTableDistVgInfo, __catalogGetTableDistVgInfo);
  stub.set(catalogGetDBVgVersion, __catalogGetDBVgVersion);
  stub.set(catalogGetDBVgList, __catalogGetDBVgList);
  stub.set(catalogGetDBCfg, __catalogGetDBCfg);
  stub.set(catalogChkAuth, __catalogChkAuth);
  stub.set(catalogChkAuthFromCache, __catalogChkAuthFromCache);
  stub.set(catalogGetUdfInfo, __catalogGetUdfInfo);
  stub.set(catalogRefreshGetTableMeta, __catalogRefreshGetTableMeta);
  stub.set(catalogRemoveTableMeta, __catalogRemoveTableMeta);
  stub.set(catalogRemoveViewMeta, __catalogRemoveViewMeta);
  stub.set(catalogGetTableIndex, __catalogGetTableIndex);
  stub.set(catalogGetDnodeList, __catalogGetDnodeList);
  stub.set(catalogRefreshGetTableCfg, __catalogRefreshGetTableCfg);
}

void generateMetaData() {
  generateDatabases(g_mockCatalogService.get());
  generateInformationSchema(g_mockCatalogService.get());
  generatePerformanceSchema(g_mockCatalogService.get());
  generateFunctions(g_mockCatalogService.get());
  generateDnodes(g_mockCatalogService.get());
}

void destroyMetaDataEnv() { g_mockCatalogService.reset(); }
