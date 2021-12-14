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
  ITableBuilder& builder = mcs->createTableBuilder("test", "t1", TSDB_NORMAL_TABLE, 3)
      .setPrecision(TSDB_TIME_PRECISION_MILLI).setVgid(1).addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT).addColumn("c2", TSDB_DATA_TYPE_BINARY, 10);
  builder.done();
}

void generateTestST1(MockCatalogService* mcs) {
  ITableBuilder& builder = mcs->createTableBuilder("test", "st1", TSDB_SUPER_TABLE, 3, 2)
      .setPrecision(TSDB_TIME_PRECISION_MILLI).addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addTag("tag1", TSDB_DATA_TYPE_INT).addTag("tag2", TSDB_DATA_TYPE_BINARY, 10)
      .addColumn("c1", TSDB_DATA_TYPE_INT).addColumn("c2", TSDB_DATA_TYPE_BINARY, 10);
  builder.done();
  mcs->createSubTable("test", "st1", "st1s1", 1);
  mcs->createSubTable("test", "st1", "st1s2", 2);
}

}

void generateMetaData(MockCatalogService* mcs) {
  generateTestT1(mcs);
  generateTestST1(mcs);
  mcs->showTables();
}

struct SCatalog* getCatalogHandle(const SEpSet* pMgmtEps) {
  return mockCatalogService->getCatalogHandle(pMgmtEps);
}

int32_t catalogGetMetaData(struct SCatalog* pCatalog, const SCatalogReq* pMetaReq, SMetaData* pMetaData) {
  return mockCatalogService->catalogGetMetaData(pCatalog, pMetaReq, pMetaData);
}
