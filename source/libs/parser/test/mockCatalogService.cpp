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

#include "mockCatalogService.h"

#include <iomanip>
#include <iostream>
#include <map>

#include "ttypes.h"

std::unique_ptr<MockCatalogService> mockCatalogService;

class TableBuilder : public ITableBuilder {
public:
  virtual TableBuilder& addColumn(const std::string& name, int8_t type, int32_t bytes) {
    assert(colIndex_ < meta_->tableInfo.numOfTags + meta_->tableInfo.numOfColumns);
    SSchema* col = meta_->schema + colIndex_;
    col->type = type;
    col->colId = colIndex_++;
    col->bytes = bytes;
    strcpy(col->name, name.c_str());
    return *this;
  }

  virtual TableBuilder& setVgid(int16_t vgid) {
    meta_->vgId = vgid;
    return *this;
  }

  virtual TableBuilder& setPrecision(uint8_t precision) {
    meta_->tableInfo.precision = precision;
    return *this;
  }

  virtual void done() {
    meta_->tableInfo.rowSize = rowsize_;
  }

private:
  friend class MockCatalogServiceImpl;

  static std::unique_ptr<TableBuilder> createTableBuilder(int8_t tableType, int32_t numOfColumns, int32_t numOfTags) {
    STableMeta* meta = (STableMeta*)std::calloc(1, sizeof(STableMeta) + sizeof(SSchema) * (numOfColumns + numOfTags));
    if (nullptr == meta) {
      throw std::bad_alloc();
    }
    meta->tableType = tableType;
    meta->tableInfo.numOfTags = numOfTags;
    meta->tableInfo.numOfColumns = numOfColumns;
    return std::unique_ptr<TableBuilder>(new TableBuilder(meta));
  }

  TableBuilder(STableMeta* meta) : colIndex_(0), rowsize_(0), meta_(meta) {
  }

  STableMeta* table() {
    return meta_;
  }

  int32_t colIndex_;
  int32_t rowsize_;
  STableMeta* meta_;
};

class MockCatalogServiceImpl {
public:
  static const int32_t numOfDataTypes = sizeof(tDataTypes) / sizeof(tDataTypes[0]);

  MockCatalogServiceImpl() {
  }

  struct SCatalog* getCatalogHandle(const SEpSet* pMgmtEps) {
    return (struct SCatalog*)0x01;
  }

  int32_t catalogGetMetaData(struct SCatalog* pCatalog, const SMetaReq* pMetaReq, SMetaData* pMetaData) {
    return 0;
  }

  TableBuilder& createTableBuilder(const std::string& db, const std::string& tbname, int8_t tableType, int32_t numOfColumns, int32_t numOfTags) {
    builder_ = TableBuilder::createTableBuilder(tableType, numOfColumns, numOfTags);
    meta_[db][tbname].reset(builder_->table());
    meta_[db][tbname]->uid = id_++;
    return *(builder_.get());
  }

  void showTables() const {
    // number of forward fills
    #define NOF(n) ((n) / 2)
    // number of backward fills
    #define NOB(n) ((n) % 2 ? (n) / 2 + 1 : (n) / 2)
    // center aligned
    #define CA(n, s) std::setw(NOF((n) - (s).length())) << "" << (s) << std::setw(NOB((n) - (s).length())) << "" << "|"
    // string field length
    #define SFL 20
    // string field header
    #define SH(h) CA(SFL, std::string(h))
    // string field
    #define SF(n) CA(SFL, n)
    // integer field length
    #define IFL 10
    // integer field header
    #define IH(i) CA(IFL, std::string(i))
    // integer field
    #define IF(i) CA(IFL, std::to_string(i))
    // split line
    #define SL(sn, in) std::setfill('=') << std::setw((sn) * (SFL + 1) + (in) * (IFL + 1)) << "" << std::setfill(' ')

    for (const auto& db : meta_) {
      std::cout << SH("Database") << SH("Table") << SH("Type") << SH("Precision") << IH(std::string("Vgid")) << std::endl;
      std::cout << SL(4, 1) << std::endl;
      for (const auto& table : db.second) {
        std::cout << SF(db.first) << SF(table.first) << SF(ttToString(table.second->tableType)) << SF(pToString(table.second->tableInfo.precision)) << IF(table.second->vgId) << std::endl;
        // int16_t numOfFields = table.second->tableInfo.numOfTags + table.second->tableInfo.numOfColumns;
        // for (int16_t i = 0; i < numOfFields; ++i) {
        //   const SSchema* schema = table.second->schema + i;
        //   std::cout << schema->name << " " << schema->type << " " << schema->bytes << std::endl;
        // }
      }
    }
  }

private:
  std::string ttToString(int8_t tableType) const {
    switch (tableType) {
      case TSDB_SUPER_TABLE:
        return "super table";
      case TSDB_CHILD_TABLE:
        return "child table";
      case TSDB_NORMAL_TABLE:
        return "normal table";
      default:
        return "unknown";
    }
  }

  std::string pToString(uint8_t precision) const {
    switch (precision) {
      case TSDB_TIME_PRECISION_MILLI:
        return "millisecond";
      case TSDB_TIME_PRECISION_MICRO:
        return "microsecond";
      case TSDB_TIME_PRECISION_NANO:
        return "nanosecond";
      default:
        return "unknown";
    }
  }

  uint64_t id_;
  std::unique_ptr<TableBuilder> builder_;
  std::map<std::string, std::map<std::string, std::shared_ptr<STableMeta> > > meta_;
};

MockCatalogService::MockCatalogService() : impl_(new MockCatalogServiceImpl()) {
}

MockCatalogService::~MockCatalogService() {
}

struct SCatalog* MockCatalogService::getCatalogHandle(const SEpSet* pMgmtEps) {
  return impl_->getCatalogHandle(pMgmtEps);
}

int32_t MockCatalogService::catalogGetMetaData(struct SCatalog* pCatalog, const SMetaReq* pMetaReq, SMetaData* pMetaData) {
  return impl_->catalogGetMetaData(pCatalog, pMetaReq, pMetaData);
}

ITableBuilder& MockCatalogService::createTableBuilder(const std::string& db, const std::string& tbname, int8_t tableType, int32_t numOfColumns, int32_t numOfTags) {
  return impl_->createTableBuilder(db, tbname, tableType, numOfColumns, numOfTags);
}

void MockCatalogService::showTables() const {
  impl_->showTables();
}