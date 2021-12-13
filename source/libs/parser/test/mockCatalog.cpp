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

void generateMetaData(MockCatalogService* mcs) {
  {
    ITableBuilder& builder = mcs->createTableBuilder("test", "t1", TSDB_NORMAL_TABLE, MockCatalogService::numOfDataTypes)
        .setPrecision(TSDB_TIME_PRECISION_MILLI).setVgid(1).addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP);
    for (int32_t i = 0; i < MockCatalogService::numOfDataTypes; ++i) {
      if (TSDB_DATA_TYPE_NULL == tDataTypes[i].type) {
        continue;
      }
      builder = builder.addColumn("c" + std::to_string(i + 1), tDataTypes[i].type);
    }
    builder.done();
  }
  {
    ITableBuilder& builder = mcs->createTableBuilder("test", "st1", TSDB_SUPER_TABLE, MockCatalogService::numOfDataTypes, 2)
        .setPrecision(TSDB_TIME_PRECISION_MILLI).setVgid(2).addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP);
    for (int32_t i = 0; i < MockCatalogService::numOfDataTypes; ++i) {
      if (TSDB_DATA_TYPE_NULL == tDataTypes[i].type) {
        continue;
      }
      builder = builder.addColumn("c" + std::to_string(i + 1), tDataTypes[i].type);
    }
    builder.done();
  }
  mcs->showTables();
}

struct SCatalog* getCatalogHandle(const SEpSet* pMgmtEps) {
  return mockCatalogService->getCatalogHandle(pMgmtEps);
}

int32_t catalogGetMetaData(struct SCatalog* pCatalog, const SMetaReq* pMetaReq, SMetaData* pMetaData) {
  return mockCatalogService->catalogGetMetaData(pCatalog, pMetaReq, pMetaData);
}
