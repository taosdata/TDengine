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

#ifndef MOCK_CATALOG_SERVICE_H
#define MOCK_CATALOG_SERVICE_H

#include <memory>
#include <string>
#include <vector>

#define ALLOW_FORBID_FUNC

#include "catalog.h"

class ITableBuilder {
 public:
  ITableBuilder& addTag(const std::string& name, int8_t type) { return addColumn(name, type, tDataTypes[type].bytes); }

  ITableBuilder& addTag(const std::string& name, int8_t type, int32_t bytes) { return addColumn(name, type, bytes); }

  ITableBuilder& addColumn(const std::string& name, int8_t type) {
    return addColumn(name, type, tDataTypes[type].bytes);
  }

  virtual ITableBuilder& addColumn(const std::string& name, int8_t type, int32_t bytes) = 0;
  virtual ITableBuilder& setVgid(int16_t vgid) = 0;
  virtual ITableBuilder& setPrecision(uint8_t precision) = 0;
  virtual void           done() = 0;
};

struct MockTableMeta {
  ~MockTableMeta() { taosMemoryFree(schema); }

  STableMeta*              schema;
  std::vector<SVgroupInfo> vgs;
};

class MockCatalogServiceImpl;
class MockCatalogService {
 public:
  MockCatalogService();
  ~MockCatalogService();
  ITableBuilder& createTableBuilder(const std::string& db, const std::string& tbname, int8_t tableType,
                                    int32_t numOfColumns, int32_t numOfTags = 0);
  void createSubTable(const std::string& db, const std::string& stbname, const std::string& tbname, int16_t vgid);
  void showTables() const;
  std::shared_ptr<MockTableMeta> getTableMeta(const std::string& db, const std::string& tbname) const;

  int32_t catalogGetTableMeta(const SName* pTableName, STableMeta** pTableMeta) const;
  int32_t catalogGetTableHashVgroup(const SName* pTableName, SVgroupInfo* vgInfo) const;
  int32_t catalogGetTableDistVgInfo(const SName* pTableName, SArray** pVgList) const;
  int32_t catalogGetAllMeta(const SCatalogReq* pCatalogReq, SMetaData* pMetaData) const;

 private:
  std::unique_ptr<MockCatalogServiceImpl> impl_;
};

extern std::unique_ptr<MockCatalogService> g_mockCatalogService;

#endif  // MOCK_CATALOG_SERVICE_H
