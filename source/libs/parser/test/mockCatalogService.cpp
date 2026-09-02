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
    TD_ALWAYS_ASSERT(TSDB_CODE_SUCCESS == addEpIntoEpSet(&vgroup.epSet, "dnode_1", 6030));
    TD_ALWAYS_ASSERT(TSDB_CODE_SUCCESS == addEpIntoEpSet(&vgroup.epSet, "dnode_2", 6030));
    TD_ALWAYS_ASSERT(TSDB_CODE_SUCCESS == addEpIntoEpSet(&vgroup.epSet, "dnode_3", 6030));
    vgroup.epSet.inUse = 0;

    (void)meta_->vgs.emplace_back(vgroup);
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
    (void)tNameGetDbName(pTableName, db);

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
    (void)tNameGetDbName(pTableName, db);
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
    int32_t code = tNameExtractFullName(pTableName, tbFName);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    auto it = index_.find(tbFName);
    if (index_.end() == it) {
      return TSDB_CODE_SUCCESS;
    }
    *pIndexes = taosArrayInit(it->second.size(), sizeof(STableIndexInfo));
    for (const auto& index : it->second) {
      STableIndexInfo info;

      if (nullptr == taosArrayPush(*pIndexes, copyTableIndexInfo(&info, &index))) {
        taosArrayDestroy(*pIndexes);
        *pIndexes = nullptr;
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetDnodeList(SArray** pDnodes) const {
    *pDnodes = taosArrayInit(dnode_.size(), sizeof(SDNodeAddr));
    if (!pDnodes) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for (const auto& dnode : dnode_) {
      if (nullptr == taosArrayPush(*pDnodes, &dnode.second)) {
        taosArrayDestroy(*pDnodes);
        *pDnodes = nullptr;
        return TSDB_CODE_OUT_OF_MEMORY;
      }
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
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllVstbRefDbs(pCatalogReq->pVStbRefDbs, &pMetaData->pVStbRefDbs);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllExtSourceInfo(pCatalogReq->pExtSourceCheck, &pMetaData->pExtSourceInfo);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = getAllExtTableMeta(pCatalogReq->pExtTableMeta, &pMetaData->pExtTableMetaRsp);
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

    (void)meta_[db][tbname]->vgs.emplace_back(vgroup);
    // super table
    (void)meta_[db][stbname]->vgs.emplace_back(vgroup);
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
    SDNodeAddr dnode = {0};
    dnode.nodeId = dnodeId;
    dnode.epSet = {0};
    TD_ALWAYS_ASSERT(TSDB_CODE_SUCCESS == addEpIntoEpSet(&dnode.epSet, host.c_str(), port));
    dnode_.insert(std::make_pair(dnodeId, dnode));
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
  typedef std::map<int32_t, SDNodeAddr>                    DnodeCache;
  typedef std::map<string, SDbCfgInfo>                     DbCfgCache;

  uint64_t getNextId() { return id_++; }

  void genEpSet(SEpSet* pEpSet) {
    TD_ALWAYS_ASSERT(TSDB_CODE_SUCCESS == addEpIntoEpSet(pEpSet, "dnode_1", 6030));
    TD_ALWAYS_ASSERT(TSDB_CODE_SUCCESS == addEpIntoEpSet(pEpSet, "dnode_2", 6030));
    TD_ALWAYS_ASSERT(TSDB_CODE_SUCCESS == addEpIntoEpSet(pEpSet, "dnode_3", 6030));
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
    if (!*vgList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for (const SVgroupInfo& vg : table->vgs) {
      if (nullptr == taosArrayPush(*vgList, &vg)) {
        taosArrayDestroy(*vgList);
        *vgList = nullptr;
        return TSDB_CODE_OUT_OF_MEMORY;
      }
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
      if (!*pTableMetaData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < ndbs; ++i) {
        STablesReq* pReq = (STablesReq*)taosArrayGet(pTableMetaReq, i);
        int32_t     ntables = taosArrayGetSize(pReq->pTables);
        for (int32_t j = 0; j < ntables; ++j) {
          SMetaRes res = {0};
          res.code = catalogGetTableMeta((const SName*)taosArrayGet(pReq->pTables, j), (STableMeta**)&res.pRes);
          if (nullptr == taosArrayPush(*pTableMetaData, &res)) {
            MockCatalogService::destoryMetaRes(&res);
            taosArrayDestroyEx(*pTableMetaData, MockCatalogService::destoryMetaRes);
            *pTableMetaData = nullptr;
            return TSDB_CODE_OUT_OF_MEMORY;
          }
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllTableVgroup(SArray* pTableVgroupReq, SArray** pTableVgroupData) const {
    if (NULL != pTableVgroupReq) {
      int32_t ndbs = taosArrayGetSize(pTableVgroupReq);
      *pTableVgroupData = taosArrayInit(ndbs, sizeof(SMetaRes));
      if (!*pTableVgroupData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < ndbs; ++i) {
        STablesReq* pReq = (STablesReq*)taosArrayGet(pTableVgroupReq, i);
        int32_t     ntables = taosArrayGetSize(pReq->pTables);
        for (int32_t j = 0; j < ntables; ++j) {
          SMetaRes res = {0};
          res.pRes = taosMemoryCalloc(1, sizeof(SVgroupInfo));
          if (!res.pRes) {
            taosArrayDestroyEx(*pTableVgroupData, MockCatalogService::destoryMetaRes);
            *pTableVgroupData = nullptr;
            return TSDB_CODE_OUT_OF_MEMORY;
          }
          res.code = catalogGetTableHashVgroup((const SName*)taosArrayGet(pReq->pTables, j), (SVgroupInfo*)res.pRes);
          if (nullptr == taosArrayPush(*pTableVgroupData, &res)) {
            MockCatalogService::destoryMetaRes(&res);
            taosArrayDestroyEx(*pTableVgroupData, MockCatalogService::destoryMetaRes);
            *pTableVgroupData = nullptr;
            return TSDB_CODE_OUT_OF_MEMORY;
          }
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
      if (!*pDbVgroupData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < ndbs; ++i) {
        SMetaRes res = {0};
        if (nullptr == taosArrayPush(*pDbVgroupData, &res)) {
          taosArrayDestroyEx(*pDbVgroupData, MockCatalogService::destoryMetaRes);
          *pDbVgroupData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
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
    if (!*pVgList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for (const auto& vgs : it->second) {
      for (const auto& vg : vgs.second->vgs) {
        if (0 == vgSet.count(vg.vgId)) {
          if (nullptr == taosArrayPush(*pVgList, &vg)) {
            taosArrayDestroy(*pVgList);
            return TSDB_CODE_OUT_OF_MEMORY;
          }
          (void)vgSet.insert(vg.vgId);
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t catalogGetAllDBVgList(SArray** pVgList) const {
    std::set<int32_t> vgSet;
    *pVgList = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVgroupInfo));
    if (!*pVgList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for (const auto& db : meta_) {
      for (const auto& vgs : db.second) {
        for (const auto& vg : vgs.second->vgs) {
          if (0 == vgSet.count(vg.vgId)) {
            if (nullptr == taosArrayPush(*pVgList, &vg)) {
              taosArrayDestroy(*pVgList);
              return TSDB_CODE_OUT_OF_MEMORY;
            }
            (void)vgSet.insert(vg.vgId);
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
      if (!*pDbCfgData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < ndbs; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
        if (!res.pRes) {
          taosArrayDestroyEx(*pDbCfgData, MockCatalogService::destoryMetaRes);
          *pDbCfgData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        res.code = catalogGetDBCfg((const char*)taosArrayGet(pDbCfgReq, i), (SDbCfgInfo*)res.pRes);
        if (nullptr == taosArrayPush(*pDbCfgData, &res)) {
          MockCatalogService::destoryMetaRes(&res);
          taosArrayDestroyEx(*pDbCfgData, MockCatalogService::destoryMetaRes);
          *pDbCfgData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return code;
  }

  int32_t getAllDbInfo(SArray* pDbInfoReq, SArray** pDbInfoData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pDbInfoReq) {
      int32_t ndbs = taosArrayGetSize(pDbInfoReq);
      *pDbInfoData = taosArrayInit(ndbs, sizeof(SMetaRes));
      if (!*pDbInfoData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < ndbs; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SDbInfo));
        if (!res.pRes || (nullptr == taosArrayPush(*pDbInfoData, &res))) {
          MockCatalogService::destoryMetaRes(&res);
          taosArrayDestroyEx(*pDbInfoData, MockCatalogService::destoryMetaRes);
          *pDbInfoData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return code;
  }

  int32_t getAllUserAuth(SArray* pUserAuthReq, SArray** pUserAuthData) const {
    int32_t code = TSDB_CODE_SUCCESS;
    if (NULL != pUserAuthReq) {
      int32_t num = taosArrayGetSize(pUserAuthReq);
      *pUserAuthData = taosArrayInit(num, sizeof(SMetaRes));
      if (!*pUserAuthData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      int32_t code = TSDB_CODE_SUCCESS;
      for (int32_t i = 0; i < num; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SUserAuthRes));
        if (!res.pRes) {
          taosArrayDestroy(*pUserAuthData);
          *pUserAuthData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        ((SUserAuthRes*)res.pRes)->pass[0] = true;
        if (nullptr == taosArrayPush(*pUserAuthData, &res)) {
          MockCatalogService::destoryMetaRes(&res);
          taosArrayDestroyEx(*pUserAuthData, MockCatalogService::destoryMetaRes);
          *pUserAuthData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return code;
  }

  int32_t getAllUdf(SArray* pUdfReq, SArray** pUdfData) const {
    if (NULL != pUdfReq) {
      int32_t num = taosArrayGetSize(pUdfReq);
      *pUdfData = taosArrayInit(num, sizeof(SMetaRes));
      if (!*pUdfData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < num; ++i) {
        SMetaRes res = {0};
        res.pRes = taosMemoryCalloc(1, sizeof(SFuncInfo));
        if (!res.pRes) {
          taosArrayDestroyEx(*pUdfData, MockCatalogService::destoryMetaRes);
          *pUdfData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        res.code = catalogGetUdfInfo((char*)taosArrayGet(pUdfReq, i), (SFuncInfo*)res.pRes);
        if (nullptr == taosArrayPush(*pUdfData, &res)) {
          MockCatalogService::destoryMetaRes(&res);
          taosArrayDestroyEx(*pUdfData, MockCatalogService::destoryMetaRes);
          *pUdfData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllTableIndex(SArray* pTableIndex, SArray** pTableIndexData) const {
    if (NULL != pTableIndex) {
      int32_t num = taosArrayGetSize(pTableIndex);
      *pTableIndexData = taosArrayInit(num, sizeof(SMetaRes));
      if (!*pTableIndexData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < num; ++i) {
        SMetaRes res = {0};
        res.code = catalogGetTableIndex((const SName*)taosArrayGet(pTableIndex, i), (SArray**)(&res.pRes));
        if (nullptr == taosArrayPush(*pTableIndexData, &res)) {
          MockCatalogService::destoryMetaRes(&res);
          taosArrayDestroyEx(*pTableIndexData, MockCatalogService::destoryMetaRes);
          *pTableIndexData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllTableCfg(SArray* pTableCfgReq, SArray** pTableCfgData) const {
    if (NULL != pTableCfgReq) {
      int32_t ntables = taosArrayGetSize(pTableCfgReq);
      *pTableCfgData = taosArrayInit(ntables, sizeof(SMetaRes));
      if (!*pTableCfgData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      int32_t code = 0;
      for (int32_t i = 0; i < ntables; ++i) {
        SMetaRes res = {0};
        res.code = TSDB_CODE_SUCCESS;
        res.pRes = taosMemoryCalloc(1, sizeof(STableCfg));
        if (!res.pRes || (nullptr == taosArrayPush(*pTableCfgData, &res))) {
          taosMemoryFree(res.pRes);
          taosArrayDestroy(*pTableCfgData);
          *pTableCfgData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllViewMeta(SArray* pViewMetaReq, SArray** pViewMetaData) const {
    if (NULL != pViewMetaReq) {
      int32_t nviews = taosArrayGetSize(pViewMetaReq);
      *pViewMetaData = taosArrayInit(nviews, sizeof(SMetaRes));
      if (!*pViewMetaData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < nviews; ++i) {
        SMetaRes res = {0};
        res.pRes = nullptr;
        res.code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
        if (nullptr == taosArrayPush(*pViewMetaData, &res)) {
          taosArrayDestroyEx(*pViewMetaData, MockCatalogService::destoryMetaRes);
          *pViewMetaData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllVstbRefDbs(SArray* pVstbRefDbsReq, SArray** pVstbRefDbsMetaData) const {
    if (NULL != pVstbRefDbsReq) {
      int32_t nRefs = taosArrayGetSize(pVstbRefDbsReq);
      *pVstbRefDbsMetaData = taosArrayInit(nRefs, sizeof(SMetaRes));
      if (!*pVstbRefDbsMetaData) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      for (int32_t i = 0; i < nRefs; ++i) {
        SMetaRes res = {0};
        res.pRes = nullptr;
        res.code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
        if (nullptr == taosArrayPush(*pVstbRefDbsMetaData, &res)) {
          taosArrayDestroyEx(*pVstbRefDbsMetaData, MockCatalogService::destoryMetaRes);
          *pVstbRefDbsMetaData = nullptr;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t getAllDnodeList(SArray** pDnodes) const {
    SMetaRes res = {0};
    int32_t code = catalogGetDnodeList((SArray**)&res.pRes);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    *pDnodes = taosArrayInit(1, sizeof(SMetaRes));
    if (!*pDnodes) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (nullptr == taosArrayPush(*pDnodes, &res)) {
      MockCatalogService::destoryMetaArrayRes(&res);
      taosArrayDestroyEx(*pDnodes, MockCatalogService::destoryMetaArrayRes);
      *pDnodes = nullptr;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
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

  // Federated-query EXT registries.
  // Key for extTables_: sourceName + '\x01' + mid0 + '\x01' + mid1 + '\x01' + tableName.
  // Values are shared_ptr so the entries survive across multiple catalogGetAllMeta calls.
  std::map<std::string, std::shared_ptr<SExtSourceInfo>>                   extSources_;
  struct MockExtTableEntry {
    SExtTableMeta                                  header;
    std::vector<MockCatalogService::MockExtColDef> cols;
  };
  std::map<std::string, std::shared_ptr<MockExtTableEntry>> extTables_;

  static std::string makeExtTableKey(const std::string& src, const std::string& m0, const std::string& m1,
                                     const std::string& tbl) {
    std::string k;
    k.reserve(src.size() + m0.size() + m1.size() + tbl.size() + 4);
    k.append(src).push_back('\x01');
    k.append(m0).push_back('\x01');
    k.append(m1).push_back('\x01');
    k.append(tbl);
    return k;
  }

 public:
  void createExtSource(const string& sourceName, int8_t sourceType, const string& host, int32_t port,
                       const string& user, const string& database, const string& schemaName = "") {
    auto info = std::make_shared<SExtSourceInfo>();
    memset(info.get(), 0, sizeof(SExtSourceInfo));
    tstrncpy(info->source_name, sourceName.c_str(), TSDB_EXT_SOURCE_NAME_LEN);
    info->type = sourceType;
    tstrncpy(info->host, host.c_str(), TSDB_EXT_SOURCE_HOST_LEN);
    info->port = port;
    tstrncpy(info->user, user.c_str(), TSDB_EXT_SOURCE_USER_LEN);
    tstrncpy(info->database, database.c_str(), TSDB_EXT_SOURCE_DATABASE_LEN);
    tstrncpy(info->schema_name, schemaName.c_str(), TSDB_EXT_SOURCE_SCHEMA_LEN);
    info->meta_version = 1;
    info->create_time = 0;
    // Mock-friendly capabilities — enable everything so translator picks the EXT branch.
    info->capability.ext_can_pushdown_filter        = true;
    info->capability.ext_can_pushdown_projection    = true;
    info->capability.ext_can_pushdown_limit         = true;
    info->capability.ext_can_pushdown_agg           = true;
    info->capability.ext_can_pushdown_order         = true;
    info->capability.ext_can_pushdown_in_const_list = true;
    extSources_[sourceName] = info;
  }

  void createExtTable(const string& sourceName, const string& mid0, const string& mid1, const string& tableName,
                      const std::vector<MockCatalogService::MockExtColDef>& cols, int8_t tsPrecision) {
    auto entry = std::make_shared<MockExtTableEntry>();
    memset(&entry->header, 0, sizeof(SExtTableMeta));
    tstrncpy(entry->header.sourceName, sourceName.c_str(), TSDB_EXT_SOURCE_NAME_LEN);
    tstrncpy(entry->header.schemaName, mid1.empty() ? mid0.c_str() : mid1.c_str(), TSDB_EXT_SOURCE_SCHEMA_LEN);
    tstrncpy(entry->header.remoteTableName, tableName.c_str(), TSDB_TABLE_NAME_LEN);
    entry->header.numOfCols = (int32_t)cols.size();
    entry->header.tableType = TSDB_NORMAL_TABLE;
    entry->header.tsPrecision = tsPrecision;
    entry->header.fetched_at = 0;
    // SName.dbname uses local-side "<sourceName>" (one logical DB per ext source for routing).
    entry->header.name.type = TSDB_TABLE_NAME_T;
    entry->header.name.acctId = 1;
    tstrncpy(entry->header.name.dbname, sourceName.c_str(), TSDB_DB_NAME_LEN);
    tstrncpy(entry->header.name.tname, tableName.c_str(), TSDB_TABLE_NAME_LEN);
    entry->cols = cols;
    extTables_[makeExtTableKey(sourceName, mid0, mid1, tableName)] = entry;
  }

  // Build the EXT-source SMetaRes array, one entry per element in pCatalogReq->pExtSourceCheck,
  // index-aligned with parUtil.c:1209-1223.
  int32_t getAllExtSourceInfo(SArray* pReq, SArray** pOut) const {
    if (NULL == pReq) return TSDB_CODE_SUCCESS;
    int32_t n = (int32_t)taosArrayGetSize(pReq);
    *pOut = taosArrayInit(n, sizeof(SMetaRes));
    if (NULL == *pOut) return TSDB_CODE_OUT_OF_MEMORY;
    for (int32_t i = 0; i < n; ++i) {
      const char* sourceName = (const char*)taosArrayGet(pReq, i);
      SMetaRes res = {0};
      auto it = extSources_.find(sourceName ? sourceName : "");
      if (it == extSources_.end()) {
        res.code = TSDB_CODE_EXT_SOURCE_NOT_FOUND;
        res.pRes = NULL;
      } else {
        SExtSourceInfo* p = (SExtSourceInfo*)taosMemoryCalloc(1, sizeof(SExtSourceInfo));
        if (NULL == p) {
          taosArrayDestroyEx(*pOut, MockCatalogService::destoryMetaRes);
          *pOut = NULL;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        memcpy(p, it->second.get(), sizeof(SExtSourceInfo));
        res.code = TSDB_CODE_SUCCESS;
        res.pRes = p;
      }
      if (NULL == taosArrayPush(*pOut, &res)) {
        if (res.pRes) taosMemoryFree(res.pRes);
        taosArrayDestroyEx(*pOut, MockCatalogService::destoryMetaRes);
        *pOut = NULL;
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  // Build the EXT-table SMetaRes array, one entry per SExtTableMetaReq in pReq,
  // index-aligned with parUtil.c:1226-1247.
  int32_t getAllExtTableMeta(SArray* pReq, SArray** pOut) const {
    if (NULL == pReq) return TSDB_CODE_SUCCESS;
    int32_t n = (int32_t)taosArrayGetSize(pReq);
    *pOut = taosArrayInit(n, sizeof(SMetaRes));
    if (NULL == *pOut) return TSDB_CODE_OUT_OF_MEMORY;
    for (int32_t i = 0; i < n; ++i) {
      SExtTableMetaReq* preq = (SExtTableMetaReq*)taosArrayGet(pReq, i);
      SMetaRes res = {0};
      std::string key = makeExtTableKey(preq->sourceName, preq->rawMidSegs[0], preq->rawMidSegs[1], preq->tableName);
      auto it = extTables_.find(key);
      if (it == extTables_.end()) {
        res.code = TSDB_CODE_EXT_TABLE_NOT_EXIST;
        res.pRes = NULL;
      } else {
        const auto& entry = it->second;
        SExtTableMeta* p = (SExtTableMeta*)taosMemoryCalloc(1, sizeof(SExtTableMeta));
        if (NULL == p) {
          taosArrayDestroyEx(*pOut, MockCatalogService::destoryMetaRes);
          *pOut = NULL;
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        memcpy(p, &entry->header, sizeof(SExtTableMeta));
        if (!entry->cols.empty()) {
          p->pCols = (SExtColumnDef*)taosMemoryCalloc(entry->cols.size(), sizeof(SExtColumnDef));
          if (NULL == p->pCols) {
            taosMemoryFree(p);
            taosArrayDestroyEx(*pOut, MockCatalogService::destoryMetaRes);
            *pOut = NULL;
            return TSDB_CODE_OUT_OF_MEMORY;
          }
          for (size_t c = 0; c < entry->cols.size(); ++c) {
            const auto& src = entry->cols[c];
            SExtColumnDef& dst = p->pCols[c];
            tstrncpy(dst.colName, src.colName.c_str(), TSDB_COL_NAME_LEN);
            tstrncpy(dst.remoteColName, src.colName.c_str(), TSDB_COL_NAME_LEN);
            // Pick a reasonable extTypeName for the mocked source.
            if (src.type == TSDB_DATA_TYPE_TIMESTAMP) {
              tstrncpy(dst.extTypeName, "timestamp", sizeof(dst.extTypeName));
            } else if (src.type == TSDB_DATA_TYPE_DOUBLE || src.type == TSDB_DATA_TYPE_FLOAT) {
              tstrncpy(dst.extTypeName, "double precision", sizeof(dst.extTypeName));
            } else if (src.type == TSDB_DATA_TYPE_BIGINT || src.type == TSDB_DATA_TYPE_INT) {
              tstrncpy(dst.extTypeName, "bigint", sizeof(dst.extTypeName));
            } else {
              tstrncpy(dst.extTypeName, "varchar", sizeof(dst.extTypeName));
            }
            dst.extCharsetName[0] = '\0';
            dst.nullable = src.nullable;
            dst.isTag = false;
            dst.isPrimaryKey = src.isPrimaryKey;
          }
        }
        res.code = TSDB_CODE_SUCCESS;
        res.pRes = p;
      }
      if (NULL == taosArrayPush(*pOut, &res)) {
        if (res.pRes) {
          SExtTableMeta* p = (SExtTableMeta*)res.pRes;
          if (p->pCols) taosMemoryFree(p->pCols);
          taosMemoryFree(p);
        }
        taosArrayDestroyEx(*pOut, MockCatalogService::destoryMetaRes);
        *pOut = NULL;
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    return TSDB_CODE_SUCCESS;
  }
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

void MockCatalogService::createExtSource(const string& sourceName, int8_t sourceType, const string& host,
                                         int32_t port, const string& user, const string& database,
                                         const string& schemaName) {
  impl_->createExtSource(sourceName, sourceType, host, port, user, database, schemaName);
}

void MockCatalogService::createExtTable(const string& sourceName, const string& mid0, const string& mid1,
                                        const string& tableName,
                                        const std::vector<MockCatalogService::MockExtColDef>& cols,
                                        int8_t tsPrecision) {
  impl_->createExtTable(sourceName, mid0, mid1, tableName, cols, tsPrecision);
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
  if (nullptr == pReq) {
    return;
  }
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
  taosArrayDestroyEx(pReq->pTableTSMAs, destoryTablesReq);
  taosArrayDestroy(pReq->pExtSourceCheck);
  taosArrayDestroy(pReq->pExtTableMeta);
  taosArrayDestroyEx(pReq->pTSMAs, destoryTablesReq);
  taosArrayDestroyEx(pReq->pTableName, destoryTablesReq);
  taosArrayDestroy(pReq->pTableTag);
  taosArrayDestroy(pReq->pVStbRefDbs);
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

static void destoryExtTableMetaRes(void* p) {
  SMetaRes* pRes = (SMetaRes*)p;
  if (pRes->pRes) {
    SExtTableMeta* pMeta = (SExtTableMeta*)pRes->pRes;
    if (pMeta->pCols) taosMemoryFree(pMeta->pCols);
    taosMemoryFree(pMeta);
    pRes->pRes = NULL;
  }
}

void MockCatalogService::destoryMetaTableTSMAInfo(void* p) {
  SMetaRes* pRes = (SMetaRes*)p;
  tFreeTableTSMAInfoRsp((STableTSMAInfoRsp*)pRes->pRes);
  taosMemoryFree(pRes->pRes);
}

void MockCatalogService::destoryMetaVStbRefDbs(void* p) {
  SMetaRes* pRes = (SMetaRes*)p;
  taosArrayDestroyEx((SArray*)pRes->pRes, tDestroySVStbRefDbsRsp);
}

void MockCatalogService::destoryMetaData(SMetaData* pData) {
  if (nullptr == pData) {
    return;
  }
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
  taosArrayDestroyEx(pData->pTableTag, destoryMetaArrayRes);
  taosArrayDestroyEx(pData->pDnodeList, destoryMetaArrayRes);
  taosArrayDestroyEx(pData->pView, destoryMetaRes);
  taosArrayDestroyEx(pData->pExtSourceInfo, destoryMetaRes);
  taosArrayDestroyEx(pData->pExtTableMetaRsp, destoryExtTableMetaRes);
  taosArrayDestroyEx(pData->pTableTsmas, destoryMetaTableTSMAInfo);
  taosArrayDestroyEx(pData->pTsmas, destoryMetaTableTSMAInfo);
  taosArrayDestroyEx(pData->pVStbRefDbs, destoryMetaVStbRefDbs);
  taosMemoryFree(pData->pSvrVer);
  delete pData;
}
