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

#include "tdatablock.h"
#include "tname.h"
#include "ttypes.h"

std::unique_ptr<MockCatalogService> g_mockCatalogService;

class TableBuilder : public ITableBuilder {
 public:
  virtual TableBuilder& addColumn(const std::string& name, int8_t type, int32_t bytes) {
    assert(colId_ <= schema()->tableInfo.numOfTags + schema()->tableInfo.numOfColumns);
    SSchema* col = schema()->schema + (colId_ - 1);
    col->type = type;
    col->colId = colId_++;
    col->bytes = bytes;
    strcpy(col->name, name.c_str());
    rowsize_ += bytes;
    return *this;
  }

  virtual TableBuilder& setVgid(int16_t vgid) {
    schema()->vgId = vgid;

    SVgroupInfo vgroup = {vgid, 0, 0, {0}, 0};
    addEpIntoEpSet(&vgroup.epSet, "dnode_1", 6030);
    addEpIntoEpSet(&vgroup.epSet, "dnode_2", 6030);
    addEpIntoEpSet(&vgroup.epSet, "dnode_3", 6030);
    vgroup.epSet.inUse = 0;

    meta_->vgs.emplace_back(vgroup);
    return *this;
  }

  virtual TableBuilder& setPrecision(uint8_t precision) {
    schema()->tableInfo.precision = precision;
    return *this;
  }

  virtual void done() { schema()->tableInfo.rowSize = rowsize_; }

 private:
  friend class MockCatalogServiceImpl;

  static std::unique_ptr<TableBuilder> createTableBuilder(int8_t tableType, int32_t numOfColumns, int32_t numOfTags) {
    STableMeta* meta =
        (STableMeta*)taosMemoryCalloc(1, sizeof(STableMeta) + sizeof(SSchema) * (numOfColumns + numOfTags));
    if (nullptr == meta) {
      throw std::bad_alloc();
    }
    meta->tableType = tableType;
    meta->tableInfo.numOfTags = numOfTags;
    meta->tableInfo.numOfColumns = numOfColumns;
    return std::unique_ptr<TableBuilder>(new TableBuilder(meta));
  }

  TableBuilder(STableMeta* schemaMeta) : colId_(1), rowsize_(0), meta_(new MockTableMeta()) {
    meta_->schema = schemaMeta;
  }

  STableMeta* schema() { return meta_->schema; }

  std::shared_ptr<MockTableMeta> table() { return meta_; }

  col_id_t                       colId_;
  int32_t                        rowsize_;
  std::shared_ptr<MockTableMeta> meta_;
};

class MockCatalogServiceImpl {
 public:
  static const int32_t numOfDataTypes = sizeof(tDataTypes) / sizeof(tDataTypes[0]);

  MockCatalogServiceImpl() : id_(1) {}

  int32_t catalogGetHandle() const { return 0; }

  int32_t catalogGetTableMeta(const SName* pTableName, STableMeta** pTableMeta) const {
    std::unique_ptr<STableMeta> table;

    char db[TSDB_DB_NAME_LEN] = {0};
    tNameGetDbName(pTableName, db);

    const char* tname = tNameGetTableName(pTableName);
    int32_t     code = copyTableSchemaMeta(db, tname, &table);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    *pTableMeta = table.release();
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetTableHashVgroup(const SName* pTableName, SVgroupInfo* vgInfo) const {
    vgInfo->vgId = 1;
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetTableDistVgInfo(const SName* pTableName, SArray** vgList) const {
    char db[TSDB_DB_NAME_LEN] = {0};
    tNameGetDbName(pTableName, db);
    return copyTableVgroup(db, tNameGetTableName(pTableName), vgList);
  }

  int32_t catalogGetUdfInfo(const std::string& funcName, SFuncInfo* pInfo) const {
    auto it = udf_.find(funcName);
    if (udf_.end() == it) {
      return TSDB_CODE_FAILED;
    }
    memcpy(pInfo, it->second.get(), sizeof(SFuncInfo));
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetAllMeta(const SCatalogReq* pCatalogReq, SMetaData* pMetaData) const {
    int32_t code = getAllTableMeta(pCatalogReq->pTableMeta, &pMetaData->pTableMeta);
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllTableVgroup(pCatalogReq->pTableHash, &pMetaData->pTableHash);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllDbVgroup(pCatalogReq->pDbVgroup, &pMetaData->pDbVgroup);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllDbCfg(pCatalogReq->pDbCfg, &pMetaData->pDbCfg);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllDbInfo(pCatalogReq->pDbInfo, &pMetaData->pDbInfo);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllUserAuth(pCatalogReq->pUser, &pMetaData->pUser);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllUdf(pCatalogReq->pUdf, &pMetaData->pUdfList);
    }
    return code;
  }

  TableBuilder& createTableBuilder(const std::string& db, const std::string& tbname, int8_t tableType,
                                   int32_t numOfColumns, int32_t numOfTags) {
    builder_ = TableBuilder::createTableBuilder(tableType, numOfColumns, numOfTags);
    meta_[db][tbname] = builder_->table();
    meta_[db][tbname]->schema->uid = id_++;
    return *(builder_.get());
  }

  void createSubTable(const std::string& db, const std::string& stbname, const std::string& tbname, int16_t vgid) {
    std::unique_ptr<STableMeta> table;
    if (TSDB_CODE_SUCCESS != copyTableSchemaMeta(db, stbname, &table)) {
      throw std::runtime_error("copyTableSchemaMeta failed");
    }
    meta_[db][tbname].reset(new MockTableMeta());
    meta_[db][tbname]->schema = table.release();
    meta_[db][tbname]->schema->uid = id_++;
    meta_[db][tbname]->schema->tableType = TSDB_CHILD_TABLE;

    SVgroupInfo vgroup = {vgid, 0, 0, {0}, 0};
    addEpIntoEpSet(&vgroup.epSet, "dnode_1", 6030);
    addEpIntoEpSet(&vgroup.epSet, "dnode_2", 6030);
    addEpIntoEpSet(&vgroup.epSet, "dnode_3", 6030);
    vgroup.epSet.inUse = 0;

    meta_[db][tbname]->vgs.emplace_back(vgroup);
    // super table
    meta_[db][stbname]->vgs.emplace_back(vgroup);
  }

  void showTables() const {
// number of forward fills
#define NOF(n) ((n) / 2)
// number of backward fills
#define NOB(n) ((n) % 2 ? (n) / 2 + 1 : (n) / 2)
// center aligned
#define CA(n, s)                                                                              \
  std::setw(NOF((n) - (s).length())) << "" << (s) << std::setw(NOB((n) - (s).length())) << "" \
                                     << "|"
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
      std::cout << "Databse:" << db.first << std::endl;
      std::cout << SH("Table") << SH("Type") << SH("Precision") << IH("Vgid") << IH("RowSize") << std::endl;
      std::cout << SL(3, 1) << std::endl;
      for (const auto& table : db.second) {
        const auto& schema = table.second->schema;
        std::cout << SF(table.first) << SF(ttToString(schema->tableType)) << SF(pToString(schema->tableInfo.precision))
                  << IF(schema->vgId) << IF(schema->tableInfo.rowSize) << std::endl;
      }
      std::cout << std::endl;
    }

    for (const auto& db : meta_) {
      for (const auto& table : db.second) {
        const auto& schema = table.second->schema;
        std::cout << "Table:" << table.first << std::endl;
        std::cout << SH("Field") << SH("Type") << SH("DataType") << IH("Bytes") << std::endl;
        std::cout << SL(3, 1) << std::endl;
        int16_t numOfColumns = schema->tableInfo.numOfColumns;
        int16_t numOfFields = numOfColumns + schema->tableInfo.numOfTags;
        for (int16_t i = 0; i < numOfFields; ++i) {
          const SSchema* col = schema->schema + i;
          std::cout << SF(std::string(col->name)) << SH(ftToString(i, numOfColumns)) << SH(dtToString(col->type))
                    << IF(col->bytes) << std::endl;
        }
        std::cout << std::endl;
      }
    }
  }

  void createFunction(const std::string& func, int8_t funcType, int8_t outputType, int32_t outputLen, int32_t bufSize) {
    std::shared_ptr<SFuncInfo> info(new SFuncInfo);
    strcpy(info->name, func.c_str());
    info->funcType = funcType;
    info->scriptType = TSDB_FUNC_SCRIPT_BIN_LIB;
    info->outputType = outputType;
    info->outputLen = outputLen;
    info->bufSize = bufSize;
    udf_.insert(std::make_pair(func, info));
  }

 private:
  typedef std::map<std::string, std::shared_ptr<MockTableMeta>> TableMetaCache;
  typedef std::map<std::string, TableMetaCache>                 DbMetaCache;
  typedef std::map<std::string, std::shared_ptr<SFuncInfo>>     UdfMetaCache;

  std::string toDbname(const std::string& dbFullName) const {
    std::string::size_type n = dbFullName.find(".");
    if (n == std::string::npos) {
      return dbFullName;
    }
    return dbFullName.substr(n + 1);
  }

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

  std::string dtToString(int8_t type) const { return tDataTypes[type].name; }

  std::string ftToString(int16_t colid, int16_t numOfColumns) const {
    return (0 == colid ? "column" : (colid < numOfColumns ? "column" : "tag"));
  }

  STableMeta* getTableSchemaMeta(const std::string& db, const std::string& tbname) const {
    std::shared_ptr<MockTableMeta> table = getTableMeta(db, tbname);
    return table ? table->schema : nullptr;
  }

  int32_t copyTableSchemaMeta(const std::string& db, const std::string& tbname,
                              std::unique_ptr<STableMeta>* dst) const {
    STableMeta* src = getTableSchemaMeta(db, tbname);
    if (nullptr == src) {
      return TSDB_CODE_TSC_INVALID_TABLE_NAME;
    }
    int32_t len = sizeof(STableMeta) + sizeof(SSchema) * (src->tableInfo.numOfTags + src->tableInfo.numOfColumns);
    dst->reset((STableMeta*)taosMemoryCalloc(1, len));
    if (!dst) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    memcpy(dst->get(), src, len);
    return TSDB_CODE_SUCCESS;
  }

  int32_t copyTableVgroup(const std::string& db, const std::string& tbname, SVgroupInfo* vg) const {
    std::shared_ptr<MockTableMeta> table = getTableMeta(db, tbname);
    if (table->vgs.empty()) {
      return TSDB_CODE_SUCCESS;
    }
    memcpy(vg, &(table->vgs[0]), sizeof(SVgroupInfo));
    return TSDB_CODE_SUCCESS;
  }

  int32_t copyTableVgroup(const std::string& db, const std::string& tbname, SArray** vgList) const {
    std::shared_ptr<MockTableMeta> table = getTableMeta(db, tbname);
    if (table->vgs.empty()) {
      return TSDB_CODE_SUCCESS;
    }
    *vgList = taosArrayInit(table->vgs.size(), sizeof(SVgroupInfo));
    for (const SVgroupInfo& vg : table->vgs) {
      taosArrayPush(*vgList, &vg);
    }
    return TSDB_CODE_SUCCESS;
  }

  std::shared_ptr<MockTableMeta> getTableMeta(const std::string& db, const std::string& tbname) const {
    DbMetaCache::const_iterator it = meta_.find(db);
    if (meta_.end() == it) {
      return std::shared_ptr<MockTableMeta>();
    }
    TableMetaCache::const_iterator tit = it->second.find(tbname);
    if (it->second.end() == tit) {
      return std::shared_ptr<MockTableMeta>();
    }
    return tit->second;
  }

  int32_t getAllTableMeta(SArray* pTableMetaReq, SArray** pTableMetaData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pTableMetaReq) {
      int32_t ntables = taosArrayGetSize(pTableMetaReq);
      *pTableMetaData = taosArrayInit(ntables, POINTER_BYTES);
      for (int32_t i = 0; i < ntables; ++i) {
        STableMeta* pMeta = NULL;
        code = catalogGetTableMeta((const SName*)taosArrayGet(pTableMetaReq, i), &pMeta);
        if (TSDB_CODE_SUCCESS == code) {
          taosArrayPush(*pTableMetaData, &pMeta);
        } else {
          break;
        }
      }
    }
    return code;
  }

  int32_t getAllTableVgroup(SArray* pTableVgroupReq, SArray** pTableVgroupData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pTableVgroupReq) {
      int32_t ntables = taosArrayGetSize(pTableVgroupReq);
      *pTableVgroupData = taosArrayInit(ntables, sizeof(SVgroupInfo));
      for (int32_t i = 0; i < ntables; ++i) {
        SVgroupInfo vgInfo = {0};
        code = catalogGetTableHashVgroup((const SName*)taosArrayGet(pTableVgroupReq, i), &vgInfo);
        if (TSDB_CODE_SUCCESS == code) {
          taosArrayPush(*pTableVgroupData, &vgInfo);
        } else {
          break;
        }
      }
    }
    return code;
  }

  int32_t getAllDbVgroup(SArray* pDbVgroupReq, SArray** pDbVgroupData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pDbVgroupReq) {
      int32_t ndbs = taosArrayGetSize(pDbVgroupReq);
      *pDbVgroupData = taosArrayInit(ndbs, POINTER_BYTES);
      for (int32_t i = 0; i < ndbs; ++i) {
        int64_t zeroVg = 0;
        taosArrayPush(*pDbVgroupData, &zeroVg);
      }
    }
    return code;
  }

  int32_t getAllDbCfg(SArray* pDbCfgReq, SArray** pDbCfgData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pDbCfgReq) {
      int32_t ndbs = taosArrayGetSize(pDbCfgReq);
      *pDbCfgData = taosArrayInit(ndbs, sizeof(SDbCfgInfo));
      for (int32_t i = 0; i < ndbs; ++i) {
        SDbCfgInfo dbCfg = {0};
        taosArrayPush(*pDbCfgData, &dbCfg);
      }
    }
    return code;
  }

  int32_t getAllDbInfo(SArray* pDbInfoReq, SArray** pDbInfoData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pDbInfoReq) {
      int32_t ndbs = taosArrayGetSize(pDbInfoReq);
      *pDbInfoData = taosArrayInit(ndbs, sizeof(SDbCfgInfo));
      for (int32_t i = 0; i < ndbs; ++i) {
        SDbInfo dbInfo = {0};
        taosArrayPush(*pDbInfoData, &dbInfo);
      }
    }
    return code;
  }

  int32_t getAllUserAuth(SArray* pUserAuthReq, SArray** pUserAuthData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pUserAuthReq) {
      int32_t num = taosArrayGetSize(pUserAuthReq);
      *pUserAuthData = taosArrayInit(num, sizeof(bool));
      for (int32_t i = 0; i < num; ++i) {
        bool pass = true;
        taosArrayPush(*pUserAuthData, &pass);
      }
    }
    return code;
  }

  int32_t getAllUdf(SArray* pUdfReq, SArray** pUdfData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pUdfReq) {
      int32_t num = taosArrayGetSize(pUdfReq);
      *pUdfData = taosArrayInit(num, sizeof(SFuncInfo));
      for (int32_t i = 0; i < num; ++i) {
        SFuncInfo info = {0};
        code = catalogGetUdfInfo((char*)taosArrayGet(pUdfReq, i), &info);
        if (TSDB_CODE_SUCCESS == code) {
          taosArrayPush(*pUdfData, &info);
        } else {
          break;
        }
      }
    }
    return code;
  }

  uint64_t                      id_;
  std::unique_ptr<TableBuilder> builder_;
  DbMetaCache                   meta_;
  UdfMetaCache                  udf_;
};

MockCatalogService::MockCatalogService() : impl_(new MockCatalogServiceImpl()) {}

MockCatalogService::~MockCatalogService() {}

ITableBuilder& MockCatalogService::createTableBuilder(const std::string& db, const std::string& tbname,
                                                      int8_t tableType, int32_t numOfColumns, int32_t numOfTags) {
  return impl_->createTableBuilder(db, tbname, tableType, numOfColumns, numOfTags);
}

void MockCatalogService::createSubTable(const std::string& db, const std::string& stbname, const std::string& tbname,
                                        int16_t vgid) {
  impl_->createSubTable(db, stbname, tbname, vgid);
}

void MockCatalogService::showTables() const { impl_->showTables(); }

void MockCatalogService::createFunction(const std::string& func, int8_t funcType, int8_t outputType, int32_t outputLen,
                                        int32_t bufSize) {
  impl_->createFunction(func, funcType, outputType, outputLen, bufSize);
}

int32_t MockCatalogService::catalogGetTableMeta(const SName* pTableName, STableMeta** pTableMeta) const {
  return impl_->catalogGetTableMeta(pTableName, pTableMeta);
}

int32_t MockCatalogService::catalogGetTableHashVgroup(const SName* pTableName, SVgroupInfo* vgInfo) const {
  return impl_->catalogGetTableHashVgroup(pTableName, vgInfo);
}

int32_t MockCatalogService::catalogGetTableDistVgInfo(const SName* pTableName, SArray** pVgList) const {
  return impl_->catalogGetTableDistVgInfo(pTableName, pVgList);
}

int32_t MockCatalogService::catalogGetUdfInfo(const std::string& funcName, SFuncInfo* pInfo) const {
  return impl_->catalogGetUdfInfo(funcName, pInfo);
}

int32_t MockCatalogService::catalogGetAllMeta(const SCatalogReq* pCatalogReq, SMetaData* pMetaData) const {
  return impl_->catalogGetAllMeta(pCatalogReq, pMetaData);
}
