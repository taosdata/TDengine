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
#include <set>

#include "systable.h"
#include "tdatablock.h"
#include "tmisce.h"
#include "tname.h"
#include "ttypes.h"

using std::string;

std::unique_ptr<MockCatalogService> g_mockCatalogService;

class TableBuilder : public ITableBuilder {
 public:
  virtual TableBuilder& addColumn(const string& name, int8_t type, int32_t bytes) {
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

  MockCatalogServiceImpl() : id_(1), havaCache_(true) {}

  ~MockCatalogServiceImpl() {
    for (auto& cfg : dbCfg_) {
      taosArrayDestroy(cfg.second.pRetensions);
    }
    for (auto& indexes : index_) {
      for (auto& index : indexes.second) {
        taosMemoryFree(index.expr);
      }
    }
  }

  int32_t catalogGetHandle() const { return 0; }

  int32_t catalogGetTableMeta(const SName* pTableName, STableMeta** pTableMeta, bool onlyCache = false) const {
    if (onlyCache && !havaCache_) {
      return TSDB_CODE_SUCCESS;
    }

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

  int32_t catalogGetTableHashVgroup(const SName* pTableName, SVgroupInfo* vgInfo, bool onlyCache = false) const {
    if (onlyCache && !havaCache_) {
      vgInfo->vgId = 0;
      return TSDB_CODE_SUCCESS;
    }

    vgInfo->vgId = 1;
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetTableDistVgInfo(const SName* pTableName, SArray** vgList) const {
    char db[TSDB_DB_NAME_LEN] = {0};
    tNameGetDbName(pTableName, db);
    return copyTableVgroup(db, tNameGetTableName(pTableName), vgList);
  }

  int32_t catalogGetDBVgList(const char* pDbFName, SArray** pVgList) const {
    string dbName(string(pDbFName).substr(string(pDbFName).find_last_of('.') + 1));
    if (0 == dbName.compare(TSDB_INFORMATION_SCHEMA_DB) || 0 == dbName.compare(TSDB_PERFORMANCE_SCHEMA_DB)) {
      return catalogGetAllDBVgList(pVgList);
    }
    return catalogGetDBVgListImpl(dbName, pVgList);
  }

  int32_t catalogGetDBCfg(const char* pDbFName, SDbCfgInfo* pDbCfg) const {
    string                     dbFName(pDbFName);
    DbCfgCache::const_iterator it = dbCfg_.find(dbFName.substr(string(pDbFName).find_last_of('.') + 1));
    if (dbCfg_.end() == it) {
      return TSDB_CODE_FAILED;
    }

    memcpy(pDbCfg, &(it->second), sizeof(SDbCfgInfo));
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetUdfInfo(const string& funcName, SFuncInfo* pInfo) const {
    auto it = udf_.find(funcName);
    if (udf_.end() == it) {
      return TSDB_CODE_FAILED;
    }
    memcpy(pInfo, it->second.get(), sizeof(SFuncInfo));
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetTableIndex(const SName* pTableName, SArray** pIndexes) const {
    char tbFName[TSDB_TABLE_FNAME_LEN] = {0};
    tNameExtractFullName(pTableName, tbFName);
    auto it = index_.find(tbFName);
    if (index_.end() == it) {
      return TSDB_CODE_SUCCESS;
    }
    *pIndexes = taosArrayInit(it->second.size(), sizeof(STableIndexInfo));
    for (const auto& index : it->second) {
      STableIndexInfo info;

      taosArrayPush(*pIndexes, copyTableIndexInfo(&info, &index));
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetDnodeList(SArray** pDnodes) const {
    *pDnodes = taosArrayInit(dnode_.size(), sizeof(SEpSet));
    for (const auto& dnode : dnode_) {
      taosArrayPush(*pDnodes, &dnode.second);
    }
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
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllTableIndex(pCatalogReq->pTableIndex, &pMetaData->pTableIndex);
    }
    if (TSDB_CODE_SUCCESS == code && pCatalogReq->dNodeRequired) {
      code = getAllDnodeList(&pMetaData->pDnodeList);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllTableCfg(pCatalogReq->pTableCfg, &pMetaData->pTableCfg);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllViewMeta(pCatalogReq->pView, &pMetaData->pView);
    }
    return code;
  }

  TableBuilder& createTableBuilder(const string& db, const string& tbname, int8_t tableType, int32_t numOfColumns,
                                   int32_t numOfTags) {
    builder_ = TableBuilder::createTableBuilder(tableType, numOfColumns, numOfTags);
    meta_[db][tbname] = builder_->table();
    meta_[db][tbname]->schema->uid = getNextId();
    return *(builder_.get());
  }

  void createSubTable(const string& db, const string& stbname, const string& tbname, int16_t vgid) {
    std::unique_ptr<STableMeta> table;
    if (TSDB_CODE_SUCCESS != copyTableSchemaMeta(db, stbname, &table)) {
      throw std::runtime_error("copyTableSchemaMeta failed");
    }
    meta_[db][tbname].reset(new MockTableMeta());
    meta_[db][tbname]->schema = table.release();
    meta_[db][tbname]->schema->uid = getNextId();
    meta_[db][tbname]->schema->tableType = TSDB_CHILD_TABLE;

    SVgroupInfo vgroup = {vgid, 0, 0, {0}, 0};
    genEpSet(&vgroup.epSet);

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
#define CA(n, s)                                                                                        \
  std::setw(NOF((n) - int((s).length()))) << "" << (s) << std::setw(NOB((n) - int((s).length()))) << "" \
                                          << "|"
// string field length
#define SFL 20
// string field header
#define SH(h) CA(SFL, string(h))
// string field
#define SF(n) CA(SFL, n)
// integer field length
#define IFL 10
// integer field header
#define IH(i) CA(IFL, string(i))
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
          std::cout << SF(string(col->name)) << SH(ftToString(i, numOfColumns)) << SH(dtToString(col->type))
                    << IF(col->bytes) << std::endl;
        }
        std::cout << std::endl;
      }
    }
  }

  void createFunction(const string& func, int8_t funcType, int8_t outputType, int32_t outputLen, int32_t bufSize) {
    std::shared_ptr<SFuncInfo> info(new SFuncInfo);
    strcpy(info->name, func.c_str());
    info->funcType = funcType;
    info->scriptType = TSDB_FUNC_SCRIPT_BIN_LIB;
    info->outputType = outputType;
    info->outputLen = outputLen;
    info->bufSize = bufSize;
    info->pCode = nullptr;
    info->pComment = nullptr;
    udf_.insert(std::make_pair(func, info));
  }

  void createSmaIndex(const SMCreateSmaReq* pReq) {
    STableIndexInfo info = {0};
    info.intervalUnit = pReq->intervalUnit;
    info.slidingUnit = pReq->slidingUnit;
    info.interval = pReq->interval;
    info.offset = pReq->offset;
    info.sliding = pReq->sliding;
    info.dstTbUid = getNextId();
    info.dstVgId = pReq->dstVgId;
    genEpSet(&info.epSet);
    info.expr = taosStrdup(pReq->expr);
    auto it = index_.find(pReq->stb);
    if (index_.end() == it) {
      index_.insert(std::make_pair(string(pReq->stb), std::vector<STableIndexInfo>{info}));
    } else {
      it->second.push_back(info);
    }
  }

  void createDnode(int32_t dnodeId, const string& host, int16_t port) {
    SEpSet epSet = {0};
    addEpIntoEpSet(&epSet, host.c_str(), port);
    dnode_.insert(std::make_pair(dnodeId, epSet));
  }

  void createDatabase(const string& db, bool rollup, int8_t cacheLast, int8_t precision) {
    SDbCfgInfo cfg = {0};
    if (rollup) {
      cfg.pRetensions = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SRetention));
    }
    cfg.cacheLast = cacheLast;
    cfg.precision = precision;
    dbCfg_.insert(std::make_pair(db, cfg));
  }

 private:
  typedef std::map<string, std::shared_ptr<MockTableMeta>> TableMetaCache;
  typedef std::map<string, TableMetaCache>                 DbMetaCache;
  typedef std::map<string, std::shared_ptr<SFuncInfo>>     UdfMetaCache;
  typedef std::map<string, std::vector<STableIndexInfo>>   IndexMetaCache;
  typedef std::map<int32_t, SEpSet>                        DnodeCache;
  typedef std::map<string, SDbCfgInfo>                     DbCfgCache;

  uint64_t getNextId() { return id_++; }

  void genEpSet(SEpSet* pEpSet) {
    addEpIntoEpSet(pEpSet, "dnode_1", 6030);
    addEpIntoEpSet(pEpSet, "dnode_2", 6030);
    addEpIntoEpSet(pEpSet, "dnode_3", 6030);
    pEpSet->inUse = 0;
  }

  STableIndexInfo* copyTableIndexInfo(STableIndexInfo* pDst, const STableIndexInfo* pSrc) const {
    memcpy(pDst, pSrc, sizeof(STableIndexInfo));
    pDst->expr = taosStrdup(pSrc->expr);
    return pDst;
  }

  string toDbname(const string& dbFullName) const {
    string::size_type n = dbFullName.find(".");
    if (n == string::npos) {
      return dbFullName;
    }
    return dbFullName.substr(n + 1);
  }

  string ttToString(int8_t tableType) const {
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

  string pToString(uint8_t precision) const {
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

  string dtToString(int8_t type) const { return tDataTypes[type].name; }

  string ftToString(int16_t colid, int16_t numOfColumns) const {
    return (0 == colid ? "column" : (colid < numOfColumns ? "column" : "tag"));
  }

  STableMeta* getTableSchemaMeta(const string& db, const string& tbname) const {
    std::shared_ptr<MockTableMeta> table = getTableMeta(db, tbname);
    return table ? table->schema : nullptr;
  }

  int32_t copyTableSchemaMeta(const string& db, const string& tbname, std::unique_ptr<STableMeta>* dst) const {
    STableMeta* src = getTableSchemaMeta(db, tbname);
    if (nullptr == src) {
      return TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }
    int32_t len = sizeof(STableMeta) + sizeof(SSchema) * (src->tableInfo.numOfTags + src->tableInfo.numOfColumns);
    dst->reset((STableMeta*)taosMemoryCalloc(1, len));
    if (!dst) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memcpy(dst->get(), src, len);
    return TSDB_CODE_SUCCESS;
  }

  int32_t copyTableVgroup(const string& db, const string& tbname, SVgroupInfo* vg) const {
    std::shared_ptr<MockTableMeta> table = getTableMeta(db, tbname);
    if (table->vgs.empty()) {
      return TSDB_CODE_SUCCESS;
    }
    memcpy(vg, &(table->vgs[0]), sizeof(SVgroupInfo));
    return TSDB_CODE_SUCCESS;
  }

  int32_t copyTableVgroup(const string& db, const string& tbname, SArray** vgList) const {
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

  std::shared_ptr<MockTableMeta> getTableMeta(const string& db, const string& tbname) const {
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
    if (NULL != pTableMetaReq) {
      int32_t ndbs = taosArrayGetSize(pTableMetaReq);
      *pTableMetaData = taosArrayInit(ndbs, sizeof(SMetaRes));
      for (int32_t i = 0; i < ndbs; ++i) {
        STablesReq* pReq = (STablesReq*)taosArrayGet(pTableMetaReq, i);
        int32_t     ntables = taosArrayGetSize(pReq->pTables);
        for (int32_t j = 0; j < ntables; ++j) {
          SMetaRes res = {0};
          res.code = catalogGetTableMeta((const SName*)taosArrayGet(pReq->pTables, j), (STableMeta**)&res.pRes);
          taosArrayPush(*pTableMetaData, &res);
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllTableVgroup(SArray* pTableVgroupReq, SArray** pTableVgroupData) const {
    if (NULL != pTableVgroupReq) {
      int32_t ndbs = taosArrayGetSize(pTableVgroupReq);
      *pTableVgroupData = taosArrayInit(ndbs, sizeof(SMetaRes));
      for (int32_t i = 0; i < ndbs; ++i) {
        STablesReq* pReq = (STablesReq*)taosArrayGet(pTableVgroupReq, i);
        int32_t     ntables = taosArrayGetSize(pReq->pTables);
        for (int32_t j = 0; j < ntables; ++j) {
          SMetaRes res = {0};
          res.pRes = taosMemoryCalloc(1, sizeof(SVgroupInfo));
          res.code = catalogGetTableHashVgroup((const SName*)taosArrayGet(pReq->pTables, j), (SVgroupInfo*)res.pRes);
          taosArrayPush(*pTableVgroupData, &res);
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllDbVgroup(SArray* pDbVgroupReq, SArray** pDbVgroupData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pDbVgroupReq) {
      int32_t ndbs = taosArrayGetSize(pDbVgroupReq);
      *pDbVgroupData = taosArrayInit(ndbs, sizeof(SMetaRes));
      for (int32_t i = 0; i < ndbs; ++i) {
        SMetaRes res = {0};
        taosArrayPush(*pDbVgroupData, &res);
      }
    }
    return code;
  }

  int32_t catalogGetDBVgListImpl(const string& dbName, SArray** pVgList) const {
    DbMetaCache::const_iterator it = meta_.find(dbName);
    if (meta_.end() == it) {
      return TSDB_CODE_FAILED;
    }
    std::set<int32_t> vgSet;
    *pVgList = taosArrayInit(it->second.size(), sizeof(SVgroupInfo));
    for (const auto& vgs : it->second) {
      for (const auto& vg : vgs.second->vgs) {
        if (0 == vgSet.count(vg.vgId)) {
          taosArrayPush(*pVgList, &vg);
          vgSet.insert(vg.vgId);
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetAllDBVgList(SArray** pVgList) const {
    std::set<int32_t> vgSet;
    *pVgList = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVgroupInfo));
    for (const auto& db : meta_) {
      for (const auto& vgs : db.second) {
        for (const auto& vg : vgs.second->vgs) {
          if (0 == vgSet.count(vg.vgId)) {
            taosArrayPush(*pVgList, &vg);
            vgSet.insert(vg.vgId);
          }
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllDbCfg(SArray* pDbCfgReq, SArray** pDbCfgData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pDbCfgReq) {
      int32_t ndbs = taosArrayGetSize(pDbCfgReq);
      *pDbCfgData = taosArrayInit(ndbs, sizeof(SMetaRes));
      for (int32_t i = 0; i < ndbs; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
        res.code = catalogGetDBCfg((const char*)taosArrayGet(pDbCfgReq, i), (SDbCfgInfo*)res.pRes);
        taosArrayPush(*pDbCfgData, &res);
      }
    }
    return code;
  }

  int32_t getAllDbInfo(SArray* pDbInfoReq, SArray** pDbInfoData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pDbInfoReq) {
      int32_t ndbs = taosArrayGetSize(pDbInfoReq);
      *pDbInfoData = taosArrayInit(ndbs, sizeof(SMetaRes));
      for (int32_t i = 0; i < ndbs; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SDbInfo));
        taosArrayPush(*pDbInfoData, &res);
      }
    }
    return code;
  }

  int32_t getAllUserAuth(SArray* pUserAuthReq, SArray** pUserAuthData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pUserAuthReq) {
      int32_t num = taosArrayGetSize(pUserAuthReq);
      *pUserAuthData = taosArrayInit(num, sizeof(SMetaRes));
      for (int32_t i = 0; i < num; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SUserAuthRes));
        ((SUserAuthRes*)res.pRes)->pass[0] = true;
        taosArrayPush(*pUserAuthData, &res);
      }
    }
    return code;
  }

  int32_t getAllUdf(SArray* pUdfReq, SArray** pUdfData) const {
    if (NULL != pUdfReq) {
      int32_t num = taosArrayGetSize(pUdfReq);
      *pUdfData = taosArrayInit(num, sizeof(SMetaRes));
      for (int32_t i = 0; i < num; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SFuncInfo));
        res.code = catalogGetUdfInfo((char*)taosArrayGet(pUdfReq, i), (SFuncInfo*)res.pRes);
        taosArrayPush(*pUdfData, &res);
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllTableIndex(SArray* pTableIndex, SArray** pTableIndexData) const {
    if (NULL != pTableIndex) {
      int32_t num = taosArrayGetSize(pTableIndex);
      *pTableIndexData = taosArrayInit(num, sizeof(SMetaRes));
      for (int32_t i = 0; i < num; ++i) {
        SMetaRes res = {0};
        res.code = catalogGetTableIndex((const SName*)taosArrayGet(pTableIndex, i), (SArray**)(&res.pRes));
        taosArrayPush(*pTableIndexData, &res);
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllTableCfg(SArray* pTableCfgReq, SArray** pTableCfgData) const {
    if (NULL != pTableCfgReq) {
      int32_t ntables = taosArrayGetSize(pTableCfgReq);
      *pTableCfgData = taosArrayInit(ntables, sizeof(SMetaRes));
      for (int32_t i = 0; i < ntables; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(STableCfg));
        res.code = TSDB_CODE_SUCCESS;
        taosArrayPush(*pTableCfgData, &res);
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllViewMeta(SArray* pViewMetaReq, SArray** pViewMetaData) const {
    if (NULL != pViewMetaReq) {
      int32_t nviews = taosArrayGetSize(pViewMetaReq);
      *pViewMetaData = taosArrayInit(nviews, sizeof(SMetaRes));
      for (int32_t i = 0; i < nviews; ++i) {
        SMetaRes res = {0};
        res.pRes = NULL;
        res.code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
        taosArrayPush(*pViewMetaData, &res);
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllDnodeList(SArray** pDnodes) const {
    SMetaRes res = {0};
    catalogGetDnodeList((SArray**)&res.pRes);
    *pDnodes = taosArrayInit(1, sizeof(SMetaRes));
    taosArrayPush(*pDnodes, &res);
    return TSDB_CODE_SUCCESS;
  }

  uint64_t                      id_;
  std::unique_ptr<TableBuilder> builder_;
  DbMetaCache                   meta_;
  UdfMetaCache                  udf_;
  IndexMetaCache                index_;
  DnodeCache                    dnode_;
  DbCfgCache                    dbCfg_;
  bool                          havaCache_;
};

MockCatalogService::MockCatalogService() : impl_(new MockCatalogServiceImpl()) {}

MockCatalogService::~MockCatalogService() {}

ITableBuilder& MockCatalogService::createTableBuilder(const string& db, const string& tbname, int8_t tableType,
                                                      int32_t numOfColumns, int32_t numOfTags) {
  return impl_->createTableBuilder(db, tbname, tableType, numOfColumns, numOfTags);
}

void MockCatalogService::createSubTable(const string& db, const string& stbname, const string& tbname, int16_t vgid) {
  impl_->createSubTable(db, stbname, tbname, vgid);
}

void MockCatalogService::showTables() const { impl_->showTables(); }

void MockCatalogService::createFunction(const string& func, int8_t funcType, int8_t outputType, int32_t outputLen,
                                        int32_t bufSize) {
  impl_->createFunction(func, funcType, outputType, outputLen, bufSize);
}

void MockCatalogService::createSmaIndex(const SMCreateSmaReq* pReq) { impl_->createSmaIndex(pReq); }

void MockCatalogService::createDnode(int32_t dnodeId, const string& host, int16_t port) {
  impl_->createDnode(dnodeId, host, port);
}

void MockCatalogService::createDatabase(const string& db, bool rollup, int8_t cacheLast, int8_t precision) {
  impl_->createDatabase(db, rollup, cacheLast, precision);
}

int32_t MockCatalogService::catalogGetTableMeta(const SName* pTableName, STableMeta** pTableMeta,
                                                bool onlyCache) const {
  return impl_->catalogGetTableMeta(pTableName, pTableMeta, onlyCache);
}

int32_t MockCatalogService::catalogGetTableHashVgroup(const SName* pTableName, SVgroupInfo* vgInfo,
                                                      bool onlyCache) const {
  return impl_->catalogGetTableHashVgroup(pTableName, vgInfo, onlyCache);
}

int32_t MockCatalogService::catalogGetTableDistVgInfo(const SName* pTableName, SArray** pVgList) const {
  return impl_->catalogGetTableDistVgInfo(pTableName, pVgList);
}

int32_t MockCatalogService::catalogGetDBVgList(const char* pDbFName, SArray** pVgList) const {
  return impl_->catalogGetDBVgList(pDbFName, pVgList);
}

int32_t MockCatalogService::catalogGetDBCfg(const char* pDbFName, SDbCfgInfo* pDbCfg) const {
  return impl_->catalogGetDBCfg(pDbFName, pDbCfg);
}

int32_t MockCatalogService::catalogGetUdfInfo(const string& funcName, SFuncInfo* pInfo) const {
  return impl_->catalogGetUdfInfo(funcName, pInfo);
}

int32_t MockCatalogService::catalogGetTableIndex(const SName* pTableName, SArray** pIndexes) const {
  return impl_->catalogGetTableIndex(pTableName, pIndexes);
}

int32_t MockCatalogService::catalogGetDnodeList(SArray** pDnodes) const { return impl_->catalogGetDnodeList(pDnodes); }

int32_t MockCatalogService::catalogGetAllMeta(const SCatalogReq* pCatalogReq, SMetaData* pMetaData) const {
  return impl_->catalogGetAllMeta(pCatalogReq, pMetaData);
}

void MockCatalogService::destoryTablesReq(void* p) {
  STablesReq* pRes = (STablesReq*)p;
  taosArrayDestroy(pRes->pTables);
}

void MockCatalogService::destoryCatalogReq(SCatalogReq* pReq) {
  taosArrayDestroy(pReq->pDbVgroup);
  taosArrayDestroy(pReq->pDbCfg);
  taosArrayDestroy(pReq->pDbInfo);
  taosArrayDestroyEx(pReq->pTableMeta, destoryTablesReq);
  taosArrayDestroyEx(pReq->pTableHash, destoryTablesReq);
  taosArrayDestroy(pReq->pUdf);
  taosArrayDestroy(pReq->pIndex);
  taosArrayDestroy(pReq->pUser);
  taosArrayDestroy(pReq->pTableIndex);
  taosArrayDestroy(pReq->pTableCfg);
  taosArrayDestroyEx(pReq->pView, destoryTablesReq);
  delete pReq;
}

void MockCatalogService::destoryMetaRes(void* p) {
  SMetaRes* pRes = (SMetaRes*)p;
  taosMemoryFree(pRes->pRes);
}

void MockCatalogService::destoryMetaArrayRes(void* p) {
  SMetaRes* pRes = (SMetaRes*)p;
  taosArrayDestroy((SArray*)pRes->pRes);
}

void MockCatalogService::destoryMetaData(SMetaData* pData) {
  taosArrayDestroyEx(pData->pDbVgroup, destoryMetaRes);
  taosArrayDestroyEx(pData->pDbCfg, destoryMetaRes);
  taosArrayDestroyEx(pData->pDbInfo, destoryMetaRes);
  taosArrayDestroyEx(pData->pTableMeta, destoryMetaRes);
  taosArrayDestroyEx(pData->pTableHash, destoryMetaRes);
  taosArrayDestroyEx(pData->pTableIndex, destoryMetaRes);
  taosArrayDestroyEx(pData->pUdfList, destoryMetaRes);
  taosArrayDestroyEx(pData->pIndex, destoryMetaRes);
  taosArrayDestroyEx(pData->pUser, destoryMetaRes);
  taosArrayDestroyEx(pData->pQnodeList, destoryMetaRes);
  taosArrayDestroyEx(pData->pTableCfg, destoryMetaRes);
  taosArrayDestroyEx(pData->pDnodeList, destoryMetaArrayRes);
  taosArrayDestroyEx(pData->pView, destoryMetaRes);
  taosMemoryFree(pData->pSvrVer);
  delete pData;
}
