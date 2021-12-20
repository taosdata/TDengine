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

#include "mockCatalog.h"

#include <iostream>

namespace {

void generateTestT1(MockCatalogService* mcs) {
  ITableBuilder& builder = mcs->createTableBuilder("root.test", "t1", TSDB_NORMAL_TABLE, 3)
      .setPrecision(TSDB_TIME_PRECISION_MILLI).setVgid(1).addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT).addColumn("c2", TSDB_DATA_TYPE_BINARY, 10);
  builder.done();
}

void generateTestST1(MockCatalogService* mcs) {
  ITableBuilder& builder = mcs->createTableBuilder("root.test", "st1", TSDB_SUPER_TABLE, 3, 2)
      .setPrecision(TSDB_TIME_PRECISION_MILLI).addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addTag("tag1", TSDB_DATA_TYPE_INT).addTag("tag2", TSDB_DATA_TYPE_BINARY, 10)
      .addColumn("c1", TSDB_DATA_TYPE_INT).addColumn("c2", TSDB_DATA_TYPE_BINARY, 10);
  builder.done();
  mcs->createSubTable("root.test", "st1", "st1s1", 1);
  mcs->createSubTable("root.test", "st1", "st1s2", 2);
}

}

int32_t catalogGetHandle(const char *clusterId, struct SCatalog** catalogHandle) {
  return mockCatalogService->catalogGetHandle(clusterId, catalogHandle);
}

int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta) {
  return mockCatalogService->catalogGetTableMeta(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, pTableMeta);
}

void initMetaDataEnv() {
  mockCatalogService.reset(new MockCatalogService());
}

void generateMetaData() {
  generateTestT1(mockCatalogService.get());
  generateTestST1(mockCatalogService.get());
  mockCatalogService->showTables();
}

void destroyMetaDataEnv() {
  mockCatalogService.reset();
}
