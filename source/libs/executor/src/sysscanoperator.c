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

#include "executorInt.h"
#include "filter.h"
#include "functionMgt.h"
#include "querynodes.h"
#include "systable.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "index.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "storageapi.h"
#include "tcompare.h"
#include "thash.h"
#include "trpc.h"
#include "ttypes.h"

typedef int (*__optSysFilter)(void* a, void* b, int16_t dtype);
typedef int32_t (*__sys_filte)(void* pMeta, SNode* cond, SArray* result);
typedef int32_t (*__sys_check)(SNode* cond);

typedef struct SSTabFltArg {
  void*        pMeta;
  void*        pVnode;
  SStorageAPI* pAPI;
} SSTabFltArg;

typedef struct SSysTableIndex {
  int8_t  init;
  SArray* uids;
  int32_t lastIdx;
} SSysTableIndex;

typedef struct SSysTableScanInfo {
  SRetrieveMetaTableRsp* pRsp;
  SRetrieveTableReq      req;
  SEpSet                 epSet;
  tsem_t                 ready;
  SReadHandle            readHandle;
  int32_t                accountId;
  const char*            pUser;
  bool                   sysInfo;
  bool                   showRewrite;
  bool                   restore;
  SNode*                 pCondition;  // db_name filter condition, to discard data that are not in current database
  SMTbCursor*            pCur;        // cursor for iterate the local table meta store.
  SSysTableIndex*        pIdx;        // idx for local table meta
  SHashObj*              pSchema;
  SColMatchInfo          matchInfo;
  SName                  name;
  SSDataBlock*           pRes;
  int64_t                numOfBlocks;  // extract basic running information.
  SLoadRemoteDataInfo    loadInfo;
  SLimitInfo             limitInfo;
  int32_t                tbnameSlotId;
  SStorageAPI*           pAPI;
} SSysTableScanInfo;

typedef struct {
  const char* name;
  __sys_check chkFunc;
  __sys_filte fltFunc;
} SSTabFltFuncDef;

typedef struct MergeIndex {
  int idx;
  int len;
} MergeIndex;

typedef struct SBlockDistInfo {
  SSDataBlock*    pResBlock;
  STsdbReader*    pHandle;
  SReadHandle     readHandle;
  STableListInfo* pTableListInfo;
  uint64_t        uid;  // table uid
} SBlockDistInfo;

static int32_t sysChkFilter__Comm(SNode* pNode);
static int32_t sysChkFilter__DBName(SNode* pNode);
static int32_t sysChkFilter__VgroupId(SNode* pNode);
static int32_t sysChkFilter__TableName(SNode* pNode);
static int32_t sysChkFilter__CreateTime(SNode* pNode);
static int32_t sysChkFilter__Ncolumn(SNode* pNode);
static int32_t sysChkFilter__Ttl(SNode* pNode);
static int32_t sysChkFilter__STableName(SNode* pNode);
static int32_t sysChkFilter__Uid(SNode* pNode);
static int32_t sysChkFilter__Type(SNode* pNode);

static int32_t sysFilte__DbName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__VgroupId(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__TableName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__CreateTime(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Ncolumn(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Ttl(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__STableName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Uid(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Type(void* arg, SNode* pNode, SArray* result);

const SSTabFltFuncDef filterDict[] = {
    {.name = "table_name", .chkFunc = sysChkFilter__TableName, .fltFunc = sysFilte__TableName},
    {.name = "db_name", .chkFunc = sysChkFilter__DBName, .fltFunc = sysFilte__DbName},
    {.name = "create_time", .chkFunc = sysChkFilter__CreateTime, .fltFunc = sysFilte__CreateTime},
    {.name = "columns", .chkFunc = sysChkFilter__Ncolumn, .fltFunc = sysFilte__Ncolumn},
    {.name = "ttl", .chkFunc = sysChkFilter__Ttl, .fltFunc = sysFilte__Ttl},
    {.name = "stable_name", .chkFunc = sysChkFilter__STableName, .fltFunc = sysFilte__STableName},
    {.name = "vgroup_id", .chkFunc = sysChkFilter__VgroupId, .fltFunc = sysFilte__VgroupId},
    {.name = "uid", .chkFunc = sysChkFilter__Uid, .fltFunc = sysFilte__Uid},
    {.name = "type", .chkFunc = sysChkFilter__Type, .fltFunc = sysFilte__Type}};

#define SYSTAB_FILTER_DICT_SIZE (sizeof(filterDict) / sizeof(filterDict[0]))

static int32_t buildDbTableInfoBlock(bool sysInfo, const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta,
                                     size_t size, const char* dbName);

static char* SYSTABLE_IDX_COLUMN[] = {"table_name", "db_name",     "create_time",      "columns",
                                      "ttl",        "stable_name", "vgroup_id', 'uid", "type"};

static char* SYSTABLE_SPECIAL_COL[] = {"db_name", "vgroup_id"};

static int32_t        buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity);
static SSDataBlock*   buildInfoSchemaTableMetaBlock(char* tableName);
static void           destroySysScanOperator(void* param);
static int32_t        loadSysTableCallback(void* param, SDataBuf* pMsg, int32_t code);
static __optSysFilter optSysGetFilterFunc(int32_t ctype, bool* reverse, bool* equal);

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock);

static int32_t sysTableUserColsFillOneTableCols(const SSysTableScanInfo* pInfo, const char* dbname, int32_t* pNumOfRows,
                                                const SSDataBlock* dataBlock, char* tName, SSchemaWrapper* schemaRow,
                                                char* tableType);

static void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                               SFilterInfo* pFilterInfo);

int32_t sysFilte__DbName(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;
  void*        pVnode = pArg->pVnode;

  const char* db = NULL;
  pArg->pAPI->metaFn.getBasicInfo(pVnode, &db, NULL, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

  tNameGetDbName(&sn, varDataVal(dbname));
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  int ret = func(dbname, pVal->datum.p, TSDB_DATA_TYPE_VARCHAR);
  if (ret == 0) return 0;

  return -2;
}

int32_t sysFilte__VgroupId(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;
  void*        pVnode = ((SSTabFltArg*)arg)->pVnode;

  int64_t vgId = 0;
  pArg->pAPI->metaFn.getBasicInfo(pVnode, NULL, (int32_t*)&vgId, NULL, NULL);

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  int ret = func(&vgId, &pVal->datum.i, TSDB_DATA_TYPE_BIGINT);
  if (ret == 0) return 0;

  return -1;
}

int32_t sysFilte__TableName(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false, equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  SMetaFltParam param = {.suid = 0,
                         .cid = 0,
                         .type = TSDB_DATA_TYPE_VARCHAR,
                         .val = pVal->datum.p,
                         .reverse = reverse,
                         .equal = equal,
                         .filterFunc = func};
  return -1;
}

int32_t sysFilte__CreateTime(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;
  SStorageAPI* pAPI = pArg->pAPI;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false, equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  SMetaFltParam param = {.suid = 0,
                         .cid = 0,
                         .type = TSDB_DATA_TYPE_BIGINT,
                         .val = &pVal->datum.i,
                         .reverse = reverse,
                         .equal = equal,
                         .filterFunc = func};

  int32_t ret = pAPI->metaFilter.metaFilterCreateTime(pArg->pVnode, &param, result);
  return ret;
}

int32_t sysFilte__Ncolumn(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__Ttl(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__STableName(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__Uid(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__Type(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int optSysDoCompare(__compar_fn_t func, int8_t comparType, void* a, void* b) {
  int32_t cmp = func(a, b);
  switch (comparType) {
    case OP_TYPE_LOWER_THAN:
      if (cmp < 0) return 0;
      break;
    case OP_TYPE_LOWER_EQUAL: {
      if (cmp <= 0) return 0;
      break;
    }
    case OP_TYPE_GREATER_THAN: {
      if (cmp > 0) return 0;
      break;
    }
    case OP_TYPE_GREATER_EQUAL: {
      if (cmp >= 0) return 0;
      break;
    }
    case OP_TYPE_EQUAL: {
      if (cmp == 0) return 0;
      break;
    }
    default:
      return -1;
  }
  return cmp;
}

static int optSysFilterFuncImpl__LowerThan(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_LOWER_THAN, a, b);
}
static int optSysFilterFuncImpl__LowerEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_LOWER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__GreaterThan(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_GREATER_THAN, a, b);
}
static int optSysFilterFuncImpl__GreaterEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_GREATER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__Equal(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_EQUAL, a, b);
}

static int optSysFilterFuncImpl__NoEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_NOT_EQUAL, a, b);
}

static int32_t optSysTabFilte(void* arg, SNode* cond, SArray* result);
static int32_t optSysTabFilteImpl(void* arg, SNode* cond, SArray* result);
static int32_t optSysCheckOper(SNode* pOpear);
static int32_t optSysMergeRslt(SArray* mRslt, SArray* rslt);

static SSDataBlock* sysTableScanFromMNode(SOperatorInfo* pOperator, SSysTableScanInfo* pInfo, const char* name,
                                          SExecTaskInfo* pTaskInfo);
void                extractTbnameSlotId(SSysTableScanInfo* pInfo, const SScanPhysiNode* pScanNode);

static void sysTableScanFillTbName(SOperatorInfo* pOperator, const SSysTableScanInfo* pInfo, const char* name,
                                   SSDataBlock* pBlock);

__optSysFilter optSysGetFilterFunc(int32_t ctype, bool* reverse, bool* equal) {
  if (ctype == OP_TYPE_LOWER_EQUAL || ctype == OP_TYPE_LOWER_THAN) {
    *reverse = true;
  }
  if (ctype == OP_TYPE_EQUAL) {
    *equal = true;
  }
  if (ctype == OP_TYPE_LOWER_THAN)
    return optSysFilterFuncImpl__LowerThan;
  else if (ctype == OP_TYPE_LOWER_EQUAL)
    return optSysFilterFuncImpl__LowerEqual;
  else if (ctype == OP_TYPE_GREATER_THAN)
    return optSysFilterFuncImpl__GreaterThan;
  else if (ctype == OP_TYPE_GREATER_EQUAL)
    return optSysFilterFuncImpl__GreaterEqual;
  else if (ctype == OP_TYPE_EQUAL)
    return optSysFilterFuncImpl__Equal;
  else if (ctype == OP_TYPE_NOT_EQUAL)
    return optSysFilterFuncImpl__NoEqual;
  return NULL;
}

static bool sysTableIsOperatorCondOnOneTable(SNode* pCond, char* condTable) {
  SOperatorNode* node = (SOperatorNode*)pCond;
  if (node->opType == OP_TYPE_EQUAL) {
    if (nodeType(node->pLeft) == QUERY_NODE_COLUMN &&
        strcasecmp(nodesGetNameFromColumnNode(node->pLeft), "table_name") == 0 &&
        nodeType(node->pRight) == QUERY_NODE_VALUE) {
      SValueNode* pValue = (SValueNode*)node->pRight;
      if (pValue->node.resType.type == TSDB_DATA_TYPE_NCHAR || pValue->node.resType.type == TSDB_DATA_TYPE_VARCHAR) {
        char* value = nodesGetValueFromNode(pValue);
        strncpy(condTable, varDataVal(value), TSDB_TABLE_NAME_LEN);
        return true;
      }
    }
  }
  return false;
}

static bool sysTableIsCondOnOneTable(SNode* pCond, char* condTable) {
  if (pCond == NULL) {
    return false;
  }
  if (nodeType(pCond) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode* node = (SLogicConditionNode*)pCond;
    if (LOGIC_COND_TYPE_AND == node->condType) {
      SNode* pChild = NULL;
      FOREACH(pChild, node->pParameterList) {
        if (QUERY_NODE_OPERATOR == nodeType(pChild) && sysTableIsOperatorCondOnOneTable(pChild, condTable)) {
          return true;
        }
      }
    }
  }

  if (QUERY_NODE_OPERATOR == nodeType(pCond)) {
    return sysTableIsOperatorCondOnOneTable(pCond, condTable);
  }

  return false;
}

static SSDataBlock* sysTableScanUserCols(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  SSDataBlock* dataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_COLS);
  blockDataEnsureCapacity(dataBlock, pOperator->resultInfo.capacity);

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

  tNameGetDbName(&sn, varDataVal(dbname));
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->req.filterTb[0]) {
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, pInfo->req.filterTb);

    SMetaReader smrTable = {0};
    pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, 0, &pAPI->metaFn);
    int32_t code = pAPI->metaReaderFn.getTableEntryByName(&smrTable, pInfo->req.filterTb);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
      pAPI->metaReaderFn.clearReader(&smrTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    if (smrTable.me.type == TSDB_SUPER_TABLE) {
      pAPI->metaReaderFn.clearReader(&smrTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    if (smrTable.me.type == TSDB_CHILD_TABLE) {
      int64_t suid = smrTable.me.ctbEntry.suid;
      pAPI->metaReaderFn.clearReader(&smrTable);
      pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, 0, &pAPI->metaFn);
      code = pAPI->metaReaderFn.getTableEntryByUid(&smrTable, suid);
      if (code != TSDB_CODE_SUCCESS) {
        // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
        pAPI->metaReaderFn.clearReader(&smrTable);
        blockDataDestroy(dataBlock);
        pInfo->loadInfo.totalRows = 0;
        return NULL;
      }
    }

    char            typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    SSchemaWrapper* schemaRow = NULL;
    if (smrTable.me.type == TSDB_SUPER_TABLE) {
      schemaRow = &smrTable.me.stbEntry.schemaRow;
      STR_TO_VARSTR(typeName, "CHILD_TABLE");
    } else if (smrTable.me.type == TSDB_NORMAL_TABLE) {
      schemaRow = &smrTable.me.ntbEntry.schemaRow;
      STR_TO_VARSTR(typeName, "NORMAL_TABLE");
    }

    sysTableUserColsFillOneTableCols(pInfo, dbname, &numOfRows, dataBlock, tableName, schemaRow, typeName);
    pAPI->metaReaderFn.clearReader(&smrTable);

    if (numOfRows > 0) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
      numOfRows = 0;
    }
    blockDataDestroy(dataBlock);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
    setOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  }

  int32_t ret = 0;
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
  }

  if (pInfo->pSchema == NULL) {
    pInfo->pSchema = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    taosHashSetFreeFp(pInfo->pSchema, tDeleteSSchemaWrapperForHash);
  }

  if (!pInfo->pCur || !pInfo->pSchema) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    qError("sysTableScanUserCols failed since %s", terrstr(terrno));
    blockDataDestroy(dataBlock);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  int32_t restore = pInfo->restore;
  pInfo->restore = false;

  while (restore || ((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_TABLE_MAX)) == 0)) {
    if (restore) {
      restore = false;
    }

    char typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

    SSchemaWrapper* schemaRow = NULL;

    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      qDebug("sysTableScanUserCols cursor get super table");
      void* schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t));
      if (schema == NULL) {
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&pInfo->pCur->mr.me.stbEntry.schemaRow);
        taosHashPut(pInfo->pSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
      }
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      qDebug("sysTableScanUserCols cursor get child table");
      STR_TO_VARSTR(typeName, "CHILD_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.ctbEntry.suid, sizeof(int64_t));
      if (schema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, 0, &pAPI->metaFn);
        int code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserCols get meta by suid:%" PRId64 " error, code:%d", suid, code);
          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(dataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        schemaRow = schemaWrapper;
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
      }
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      qDebug("sysTableScanUserCols cursor get normal table");
      schemaRow = &pInfo->pCur->mr.me.ntbEntry.schemaRow;
      STR_TO_VARSTR(typeName, "NORMAL_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
    } else {
      qDebug("sysTableScanUserCols cursor get invalid table");
      continue;
    }

    if ((numOfRows + schemaRow->nCols) > pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
      numOfRows = 0;
      pInfo->restore = true;

      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    } else {
      sysTableUserColsFillOneTableCols(pInfo, dbname, &numOfRows, dataBlock, tableName, schemaRow, typeName);
    }
  }

  if (numOfRows > 0) {
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
    numOfRows = 0;
  }

  blockDataDestroy(dataBlock);
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  qDebug("sysTableScanUserCols get cols success, rows:%" PRIu64, pInfo->loadInfo.totalRows);

  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserTags(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  SSDataBlock* dataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TAGS);
  blockDataEnsureCapacity(dataBlock, pOperator->resultInfo.capacity);

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

  tNameGetDbName(&sn, varDataVal(dbname));
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  char condTableName[TSDB_TABLE_NAME_LEN] = {0};
  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->pCondition && sysTableIsCondOnOneTable(pInfo->pCondition, condTableName)) {
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, condTableName);

    SMetaReader smrChildTable = {0};
    pAPI->metaReaderFn.initReader(&smrChildTable, pInfo->readHandle.vnode, 0, &pAPI->metaFn);
    int32_t code = pAPI->metaReaderFn.getTableEntryByName(&smrChildTable, condTableName);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
      pAPI->metaReaderFn.clearReader(&smrChildTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    if (smrChildTable.me.type != TSDB_CHILD_TABLE) {
      pAPI->metaReaderFn.clearReader(&smrChildTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    SMetaReader smrSuperTable = {0};
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, smrChildTable.me.ctbEntry.suid);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByUid
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pAPI->metaReaderFn.clearReader(&smrChildTable);
      blockDataDestroy(dataBlock);
      return NULL;
    }

    sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &smrChildTable, dbname, tableName, &numOfRows, dataBlock);
    pAPI->metaReaderFn.clearReader(&smrSuperTable);
    pAPI->metaReaderFn.clearReader(&smrChildTable);

    if (numOfRows > 0) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
      numOfRows = 0;
    }
    blockDataDestroy(dataBlock);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
    setOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  }

  int32_t ret = 0;
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
  }

  bool blockFull = false;
  while ((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_SUPER_TABLE)) == 0) {
    if (pInfo->pCur->mr.me.type != TSDB_CHILD_TABLE) {
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

    SMetaReader smrSuperTable = {0};
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, 0, &pAPI->metaFn);
    uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
    int32_t  code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno),
             GET_TASKID(pTaskInfo));
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    if ((smrSuperTable.me.stbEntry.schemaTag.nCols + numOfRows) > pOperator->resultInfo.capacity) {
      pAPI->metaFn.cursorPrev(pInfo->pCur, TSDB_TABLE_MAX);
      blockFull = true;
    } else {
      sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &pInfo->pCur->mr, dbname, tableName, &numOfRows,
                                       dataBlock);
    }

    pAPI->metaReaderFn.clearReader(&smrSuperTable);

    if (blockFull || numOfRows >= pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        break;
      }

      blockFull = false;
    }
  }

  if (numOfRows > 0) {
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
    numOfRows = 0;
  }

  blockDataDestroy(dataBlock);
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                        SFilterInfo* pFilterInfo) {
  dataBlock->info.rows = numOfRows;
  pInfo->pRes->info.rows = numOfRows;

  relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, dataBlock->pDataBlock, false);
  doFilter(pInfo->pRes, pFilterInfo, NULL);
  blockDataCleanup(dataBlock);
}

int32_t convertTagDataToStr(char* str, int type, void* buf, int32_t bufSize, int32_t* len) {
  int32_t n = 0;

  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      n = sprintf(str, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      n = sprintf(str, (*(int8_t*)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = sprintf(str, "%d", *(int8_t*)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = sprintf(str, "%d", *(int16_t*)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = sprintf(str, "%d", *(int32_t*)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = sprintf(str, "%" PRId64, *(int64_t*)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = sprintf(str, "%.5f", GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = sprintf(str, "%.9f", GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_BINARY:
      if (bufSize < 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      memcpy(str, buf, bufSize);
      n = bufSize;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (bufSize < 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      int32_t length = taosUcs4ToMbs((TdUcs4*)buf, bufSize, str);
      if (length <= 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      n = length;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      n = sprintf(str, "%u", *(uint8_t*)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = sprintf(str, "%u", *(uint16_t*)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = sprintf(str, "%u", *(uint32_t*)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = sprintf(str, "%" PRIu64, *(uint64_t*)buf);
      break;

    default:
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (len) *len = n;

  return TSDB_CODE_SUCCESS;
}

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock) {
  char stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(stableName, (*smrSuperTable).me.name);

  int32_t numOfRows = *pNumOfRows;

  int32_t numOfTags = (*smrSuperTable).me.stbEntry.schemaTag.nCols;
  for (int32_t i = 0; i < numOfTags; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    colDataSetVal(pColInfoData, numOfRows, tableName, false);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    colDataSetVal(pColInfoData, numOfRows, dbname, false);

    // super table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    colDataSetVal(pColInfoData, numOfRows, stableName, false);

    // tag name
    char tagName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tagName, (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    colDataSetVal(pColInfoData, numOfRows, tagName, false);

    // tag type
    int8_t tagType = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].type;
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    char tagTypeStr[VARSTR_HEADER_SIZE + 32];
    int  tagTypeLen = sprintf(varDataVal(tagTypeStr), "%s", tDataTypes[tagType].name);
    if (tagType == TSDB_DATA_TYPE_VARCHAR) {
      tagTypeLen += sprintf(varDataVal(tagTypeStr) + tagTypeLen, "(%d)",
                            (int32_t)((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE));
    } else if (tagType == TSDB_DATA_TYPE_NCHAR) {
      tagTypeLen += sprintf(
          varDataVal(tagTypeStr) + tagTypeLen, "(%d)",
          (int32_t)(((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }
    varDataSetLen(tagTypeStr, tagTypeLen);
    colDataSetVal(pColInfoData, numOfRows, (char*)tagTypeStr, false);

    STagVal tagVal = {0};
    tagVal.cid = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].colId;
    char*    tagData = NULL;
    uint32_t tagLen = 0;

    if (tagType == TSDB_DATA_TYPE_JSON) {
      tagData = (char*)smrChildTable->me.ctbEntry.pTags;
    } else {
      bool exist = tTagGet((STag*)smrChildTable->me.ctbEntry.pTags, &tagVal);
      if (exist) {
        if (IS_VAR_DATA_TYPE(tagType)) {
          tagData = (char*)tagVal.pData;
          tagLen = tagVal.nData;
        } else {
          tagData = (char*)&tagVal.i64;
          tagLen = tDataTypes[tagType].bytes;
        }
      }
    }

    char* tagVarChar = NULL;
    if (tagData != NULL) {
      if (tagType == TSDB_DATA_TYPE_JSON) {
        char* tagJson = parseTagDatatoJson(tagData);
        tagVarChar = taosMemoryMalloc(strlen(tagJson) + VARSTR_HEADER_SIZE);
        memcpy(varDataVal(tagVarChar), tagJson, strlen(tagJson));
        varDataSetLen(tagVarChar, strlen(tagJson));
        taosMemoryFree(tagJson);
      } else {
        int32_t bufSize = IS_VAR_DATA_TYPE(tagType) ? (tagLen + VARSTR_HEADER_SIZE)
                                                    : (3 + DBL_MANT_DIG - DBL_MIN_EXP + VARSTR_HEADER_SIZE);
        tagVarChar = taosMemoryCalloc(1, bufSize + 1);
        int32_t len = -1;
        convertTagDataToStr(varDataVal(tagVarChar), tagType, tagData, tagLen, &len);
        varDataSetLen(tagVarChar, len);
      }
    }
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    colDataSetVal(pColInfoData, numOfRows, tagVarChar,
                  (tagData == NULL) || (tagType == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(tagData)));
    taosMemoryFree(tagVarChar);
    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

static int32_t sysTableUserColsFillOneTableCols(const SSysTableScanInfo* pInfo, const char* dbname, int32_t* pNumOfRows,
                                                const SSDataBlock* dataBlock, char* tName, SSchemaWrapper* schemaRow,
                                                char* tableType) {
  if (schemaRow == NULL) {
    qError("sysTableUserColsFillOneTableCols schemaRow is NULL");
    return TSDB_CODE_SUCCESS;
  }
  int32_t numOfRows = *pNumOfRows;

  int32_t numOfCols = schemaRow->nCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    colDataSetVal(pColInfoData, numOfRows, tName, false);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    colDataSetVal(pColInfoData, numOfRows, dbname, false);

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    colDataSetVal(pColInfoData, numOfRows, tableType, false);

    // col name
    char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(colName, schemaRow->pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    colDataSetVal(pColInfoData, numOfRows, colName, false);

    // col type
    int8_t colType = schemaRow->pSchema[i].type;
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    char colTypeStr[VARSTR_HEADER_SIZE + 32];
    int  colTypeLen = sprintf(varDataVal(colTypeStr), "%s", tDataTypes[colType].name);
    if (colType == TSDB_DATA_TYPE_VARCHAR) {
      colTypeLen += sprintf(varDataVal(colTypeStr) + colTypeLen, "(%d)",
                            (int32_t)(schemaRow->pSchema[i].bytes - VARSTR_HEADER_SIZE));
    } else if (colType == TSDB_DATA_TYPE_NCHAR) {
      colTypeLen += sprintf(varDataVal(colTypeStr) + colTypeLen, "(%d)",
                            (int32_t)((schemaRow->pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }
    varDataSetLen(colTypeStr, colTypeLen);
    colDataSetVal(pColInfoData, numOfRows, (char*)colTypeStr, false);

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    colDataSetVal(pColInfoData, numOfRows, (const char*)&schemaRow->pSchema[i].bytes, false);

    for (int32_t j = 6; j <= 8; ++j) {
      pColInfoData = taosArrayGet(dataBlock->pDataBlock, j);
      colDataSetNULL(pColInfoData, numOfRows);
    }
    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* buildInfoSchemaTableMetaBlock(char* tableName) {
  size_t               size = 0;
  const SSysTableMeta* pMeta = NULL;
  getInfosDbMeta(&pMeta, &size);

  int32_t index = 0;
  for (int32_t i = 0; i < size; ++i) {
    if (strcmp(pMeta[i].name, tableName) == 0) {
      index = i;
      break;
    }
  }

  SSDataBlock* pBlock = createDataBlock();
  for (int32_t i = 0; i < pMeta[index].colNum; ++i) {
    SColumnInfoData colInfoData =
        createColumnInfoData(pMeta[index].schema[i].type, pMeta[index].schema[i].bytes, i + 1);
    blockDataAppendColInfo(pBlock, &colInfoData);
  }

  return pBlock;
}

int32_t buildDbTableInfoBlock(bool sysInfo, const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta, size_t size,
                              const char* dbName) {
  char    n[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t numOfRows = p->info.rows;

  for (int32_t i = 0; i < size; ++i) {
    const SSysTableMeta* pm = &pSysDbTableMeta[i];
    if (!sysInfo && pm->sysInfo) {
      continue;
    }

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);

    STR_TO_VARSTR(n, pm->name);
    colDataSetVal(pColInfoData, numOfRows, n, false);

    // database name
    STR_TO_VARSTR(n, dbName);
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    colDataSetVal(pColInfoData, numOfRows, n, false);

    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    colDataSetNULL(pColInfoData, numOfRows);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    colDataSetVal(pColInfoData, numOfRows, (char*)&pm->colNum, false);

    for (int32_t j = 4; j <= 8; ++j) {
      pColInfoData = taosArrayGet(p->pDataBlock, j);
      colDataSetNULL(pColInfoData, numOfRows);
    }

    STR_TO_VARSTR(n, "SYSTEM_TABLE");

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    colDataSetVal(pColInfoData, numOfRows, n, false);

    numOfRows += 1;
  }

  return numOfRows;
}

int32_t buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity) {
  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  blockDataEnsureCapacity(p, capacity);

  size_t               size = 0;
  const SSysTableMeta* pSysDbTableMeta = NULL;

  getInfosDbMeta(&pSysDbTableMeta, &size);
  p->info.rows = buildDbTableInfoBlock(pInfo->sysInfo, p, pSysDbTableMeta, size, TSDB_INFORMATION_SCHEMA_DB);

  getPerfDbMeta(&pSysDbTableMeta, &size);
  p->info.rows = buildDbTableInfoBlock(pInfo->sysInfo, p, pSysDbTableMeta, size, TSDB_PERFORMANCE_SCHEMA_DB);

  pInfo->pRes->info.rows = p->info.rows;
  relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
  blockDataDestroy(p);

  return pInfo->pRes->info.rows;
}

static SSDataBlock* sysTableBuildUserTablesByUids(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SSysTableScanInfo* pInfo = pOperator->info;

  SSysTableIndex* pIdx = pInfo->pIdx;
  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  int ret = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

  tNameGetDbName(&sn, varDataVal(dbname));
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);

  char    n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t i = pIdx->lastIdx;
  for (; i < taosArrayGetSize(pIdx->uids); i++) {
    tb_uid_t* uid = taosArrayGet(pIdx->uids, i);

    SMetaReader mr = {0};
    pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, 0, &pAPI->metaFn);
    ret = pAPI->metaReaderFn.getTableEntryByUid(&mr, *uid);
    if (ret < 0) {
      pAPI->metaReaderFn.clearReader(&mr);
      continue;
    }
    STR_TO_VARSTR(n, mr.me.name);

    // table name
    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
    colDataSetVal(pColInfoData, numOfRows, n, false);

    // database name
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    colDataSetVal(pColInfoData, numOfRows, dbname, false);

    // vgId
    pColInfoData = taosArrayGet(p->pDataBlock, 6);
    colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);

    int32_t tableType = mr.me.type;
    if (tableType == TSDB_CHILD_TABLE) {
      // create time
      int64_t ts = mr.me.ctbEntry.btime;
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      colDataSetVal(pColInfoData, numOfRows, (char*)&ts, false);

      SMetaReader mr1 = {0};
      pAPI->metaReaderFn.initReader(&mr1, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);

      int64_t suid = mr.me.ctbEntry.suid;
      int32_t code = pAPI->metaReaderFn.getTableEntryByUid(&mr1, suid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
               suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
        pAPI->metaReaderFn.clearReader(&mr1);
        pAPI->metaReaderFn.clearReader(&mr);
        T_LONG_JMP(pTaskInfo->env, terrno);
      }
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      colDataSetVal(pColInfoData, numOfRows, (char*)&mr1.me.stbEntry.schemaRow.nCols, false);

      // super table name
      STR_TO_VARSTR(n, mr1.me.name);
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      colDataSetVal(pColInfoData, numOfRows, n, false);
      pAPI->metaReaderFn.clearReader(&mr1);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      if (mr.me.ctbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, mr.me.ctbEntry.comment);
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else if (mr.me.ctbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.uid, false);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.ctbEntry.ttlDays, false);

      STR_TO_VARSTR(n, "CHILD_TABLE");

    } else if (tableType == TSDB_NORMAL_TABLE) {
      // create time
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.btime, false);

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schemaRow.nCols, false);

      // super table name
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      colDataSetNULL(pColInfoData, numOfRows);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      if (mr.me.ntbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, mr.me.ntbEntry.comment);
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else if (mr.me.ntbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.uid, false);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.ntbEntry.ttlDays, false);

      STR_TO_VARSTR(n, "NORMAL_TABLE");
      // impl later
    }

    pAPI->metaReaderFn.clearReader(&mr);

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    colDataSetVal(pColInfoData, numOfRows, n, false);

    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

      blockDataCleanup(p);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    }
  }

  if (numOfRows > 0) {
    p->info.rows = numOfRows;
    pInfo->pRes->info.rows = numOfRows;

    relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  if (i >= taosArrayGetSize(pIdx->uids)) {
    setOperatorCompleted(pOperator);
  } else {
    pIdx->lastIdx = i + 1;
  }

  blockDataDestroy(p);

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableBuildUserTables(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;
  int8_t         firstMetaCursor = 0;

  SSysTableScanInfo* pInfo = pOperator->info;
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    firstMetaCursor = 1;
  }
  if (!firstMetaCursor) {
    pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0);
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

  tNameGetDbName(&sn, varDataVal(dbname));
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);

  char n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

  int32_t ret = 0;
  while ((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_SUPER_TABLE)) == 0) {
    STR_TO_VARSTR(n, pInfo->pCur->mr.me.name);

    // table name
    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
    colDataSetVal(pColInfoData, numOfRows, n, false);

    // database name
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    colDataSetVal(pColInfoData, numOfRows, dbname, false);

    // vgId
    pColInfoData = taosArrayGet(p->pDataBlock, 6);
    colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);

    int32_t tableType = pInfo->pCur->mr.me.type;
    if (tableType == TSDB_CHILD_TABLE) {
      // create time
      int64_t ts = pInfo->pCur->mr.me.ctbEntry.btime;
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      colDataSetVal(pColInfoData, numOfRows, (char*)&ts, false);

      SMetaReader mr = {0};
      pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);

      uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      int32_t  code = pAPI->metaReaderFn.getTableEntryByUid(&mr, suid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
               suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
        pAPI->metaReaderFn.clearReader(&mr);
        pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
        pInfo->pCur = NULL;
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.stbEntry.schemaRow.nCols, false);

      // super table name
      STR_TO_VARSTR(n, mr.me.name);
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      colDataSetVal(pColInfoData, numOfRows, n, false);
      pAPI->metaReaderFn.clearReader(&mr);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      if (pInfo->pCur->mr.me.ctbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ctbEntry.comment);
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else if (pInfo->pCur->mr.me.ctbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ctbEntry.ttlDays, false);

      STR_TO_VARSTR(n, "CHILD_TABLE");
    } else if (tableType == TSDB_NORMAL_TABLE) {
      // create time
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.btime, false);

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schemaRow.nCols, false);

      // super table name
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      colDataSetNULL(pColInfoData, numOfRows);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      if (pInfo->pCur->mr.me.ntbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ntbEntry.comment);
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else if (pInfo->pCur->mr.me.ntbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        colDataSetVal(pColInfoData, numOfRows, comment, false);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.ttlDays, false);

      STR_TO_VARSTR(n, "NORMAL_TABLE");
    }

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    colDataSetVal(pColInfoData, numOfRows, n, false);

    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

      blockDataCleanup(p);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        break;
      }
    }
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    p->info.rows = numOfRows;
    pInfo->pRes->info.rows = numOfRows;

    relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  blockDataDestroy(p);

  // todo temporarily free the cursor here, the true reason why the free is not valid needs to be found
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserTables(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  SNode* pCondition = pInfo->pCondition;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
  if (pInfo->readHandle.mnd != NULL) {
    buildSysDbTableInfo(pInfo, pOperator->resultInfo.capacity);
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

    setOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  } else {
    if (pInfo->showRewrite == false) {
      if (pCondition != NULL && pInfo->pIdx == NULL) {
        SSTabFltArg arg = {
            .pMeta = pInfo->readHandle.vnode, .pVnode = pInfo->readHandle.vnode, .pAPI = &pTaskInfo->storageAPI};

        SSysTableIndex* idx = taosMemoryMalloc(sizeof(SSysTableIndex));
        idx->init = 0;
        idx->uids = taosArrayInit(128, sizeof(int64_t));
        idx->lastIdx = 0;

        pInfo->pIdx = idx;  // set idx arg

        int flt = optSysTabFilte(&arg, pCondition, idx->uids);
        if (flt == 0) {
          pInfo->pIdx->init = 1;
          SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
          return blk;
        } else if ((flt == -1) || (flt == -2)) {
          qDebug("%s failed to get sys table info by idx, scan sys table one by one", GET_TASKID(pTaskInfo));
        }
      } else if (pCondition != NULL && (pInfo->pIdx != NULL && pInfo->pIdx->init == 1)) {
        SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
        return blk;
      }
    }

    return sysTableBuildUserTables(pOperator);
  }
  return NULL;
}

static SSDataBlock* sysTableScanUserSTables(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  pInfo->pRes->info.rows = 0;
  pOperator->status = OP_EXEC_DONE;

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static int32_t getSysTableDbNameColId(const char* pTable) {
  // if (0 == strcmp(TSDB_INS_TABLE_INDEXES, pTable)) {
  //   return 1;
  // }
  return TSDB_INS_USER_STABLES_DBNAME_COLID;
}

static EDealRes getDBNameFromConditionWalker(SNode* pNode, void* pContext) {
  int32_t   code = TSDB_CODE_SUCCESS;
  ENodeType nType = nodeType(pNode);

  switch (nType) {
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* node = (SOperatorNode*)pNode;
      if (OP_TYPE_EQUAL == node->opType) {
        *(int32_t*)pContext = 1;
        return DEAL_RES_CONTINUE;
      }

      *(int32_t*)pContext = 0;
      return DEAL_RES_IGNORE_CHILD;
    }
    case QUERY_NODE_COLUMN: {
      if (1 != *(int32_t*)pContext) {
        return DEAL_RES_CONTINUE;
      }

      SColumnNode* node = (SColumnNode*)pNode;
      if (getSysTableDbNameColId(node->tableName) == node->colId) {
        *(int32_t*)pContext = 2;
        return DEAL_RES_CONTINUE;
      }

      *(int32_t*)pContext = 0;
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_VALUE: {
      if (2 != *(int32_t*)pContext) {
        return DEAL_RES_CONTINUE;
      }

      SValueNode* node = (SValueNode*)pNode;
      char*       dbName = nodesGetValueFromNode(node);
      strncpy(pContext, varDataVal(dbName), varDataLen(dbName));
      *((char*)pContext + varDataLen(dbName)) = 0;
      return DEAL_RES_END;  // stop walk
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static void getDBNameFromCondition(SNode* pCondition, const char* dbName) {
  if (NULL == pCondition) {
    return;
  }
  nodesWalkExpr(pCondition, getDBNameFromConditionWalker, (char*)dbName);
}

static SSDataBlock* doSysTableScan(SOperatorInfo* pOperator) {
  // build message and send to mnode to fetch the content of system tables.
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  char               dbName[TSDB_DB_NAME_LEN] = {0};

  if (isTaskKilled(pOperator->pTaskInfo)) {
    setOperatorCompleted(pOperator);
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  const char* name = tNameGetTableName(&pInfo->name);
  if (pInfo->showRewrite) {
    getDBNameFromCondition(pInfo->pCondition, dbName);
    if (strncasecmp(name, TSDB_INS_TABLE_COMPACTS, TSDB_TABLE_FNAME_LEN) != 0 &&
        strncasecmp(name, TSDB_INS_TABLE_COMPACT_DETAILS, TSDB_TABLE_FNAME_LEN) != 0) {
      sprintf(pInfo->req.db, "%d.%s", pInfo->accountId, dbName);
    }
  } else if (strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0) {
    getDBNameFromCondition(pInfo->pCondition, dbName);
    if (dbName[0]) sprintf(pInfo->req.db, "%d.%s", pInfo->accountId, dbName);
    sysTableIsCondOnOneTable(pInfo->pCondition, pInfo->req.filterTb);
  }

  SSDataBlock* pBlock = NULL;
  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0) {
    pBlock = sysTableScanUserTables(pOperator);
  } else if (strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0) {
    pBlock = sysTableScanUserTags(pOperator);
  } else if (strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->readHandle.mnd == NULL) {
    pBlock = sysTableScanUserCols(pOperator);
  } else if (strncasecmp(name, TSDB_INS_TABLE_STABLES, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->showRewrite &&
             IS_SYS_DBNAME(dbName)) {
    pBlock = sysTableScanUserSTables(pOperator);
  } else {  // load the meta from mnode of the given epset
    pBlock = sysTableScanFromMNode(pOperator, pInfo, name, pTaskInfo);
  }

  sysTableScanFillTbName(pOperator, pInfo, name, pBlock);
  if (pBlock != NULL) {
    bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
    if (limitReached) {
      setOperatorCompleted(pOperator);
    }

    return pBlock->info.rows > 0 ? pBlock : NULL;
  } else {
    return NULL;
  }
}

static void sysTableScanFillTbName(SOperatorInfo* pOperator, const SSysTableScanInfo* pInfo, const char* name,
                                   SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return;
  }

  if (pInfo->tbnameSlotId != -1) {
    SColumnInfoData* pColumnInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, pInfo->tbnameSlotId);
    char             varTbName[TSDB_TABLE_FNAME_LEN - 1 + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(varTbName, name);

    colDataSetNItems(pColumnInfoData, 0, varTbName, pBlock->info.rows, true);
  }

  doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL);
}

static SSDataBlock* sysTableScanFromMNode(SOperatorInfo* pOperator, SSysTableScanInfo* pInfo, const char* name,
                                          SExecTaskInfo* pTaskInfo) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  while (1) {
    int64_t startTs = taosGetTimestampUs();
    tstrncpy(pInfo->req.tb, tNameGetTableName(&pInfo->name), tListLen(pInfo->req.tb));
    tstrncpy(pInfo->req.user, pInfo->pUser, tListLen(pInfo->req.user));

    int32_t contLen = tSerializeSRetrieveTableReq(NULL, 0, &pInfo->req);
    char*   buf1 = taosMemoryCalloc(1, contLen);
    tSerializeSRetrieveTableReq(buf1, contLen, &pInfo->req);

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }

    int32_t msgType = (strcasecmp(name, TSDB_INS_TABLE_DNODE_VARIABLES) == 0) ? TDMT_DND_SYSTABLE_RETRIEVE
                                                                              : TDMT_MND_SYSTABLE_RETRIEVE;

    pMsgSendInfo->param = pOperator;
    pMsgSendInfo->msgInfo.pData = buf1;
    pMsgSendInfo->msgInfo.len = contLen;
    pMsgSendInfo->msgType = msgType;
    pMsgSendInfo->fp = loadSysTableCallback;
    pMsgSendInfo->requestId = pTaskInfo->id.queryId;

    int64_t transporterId = 0;
    int32_t code =
        asyncSendMsgToServer(pInfo->readHandle.pMsgCb->clientRpc, &pInfo->epSet, &transporterId, pMsgSendInfo);
    tsem_wait(&pInfo->ready);

    if (pTaskInfo->code) {
      qError("%s load meta data from mnode failed, totalRows:%" PRIu64 ", code:%s", GET_TASKID(pTaskInfo),
             pInfo->loadInfo.totalRows, tstrerror(pTaskInfo->code));
      return NULL;
    }

    SRetrieveMetaTableRsp* pRsp = pInfo->pRsp;
    pInfo->req.showId = pRsp->handle;

    if (pRsp->numOfRows == 0 || pRsp->completed) {
      pOperator->status = OP_EXEC_DONE;
      qDebug("%s load meta data from mnode completed, rowsOfSource:%d, totalRows:%" PRIu64, GET_TASKID(pTaskInfo),
             pRsp->numOfRows, pInfo->loadInfo.totalRows);

      if (pRsp->numOfRows == 0) {
        taosMemoryFree(pRsp);
        return NULL;
      }
    }

    char* pStart = pRsp->data;
    extractDataBlockFromFetchRsp(pInfo->pRes, pRsp->data, pInfo->matchInfo.pList, &pStart);
    updateLoadRemoteInfo(&pInfo->loadInfo, pRsp->numOfRows, pRsp->compLen, startTs, pOperator);

    // todo log the filter info
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    taosMemoryFree(pRsp);
    if (pInfo->pRes->info.rows > 0) {
      return pInfo->pRes;
    } else if (pOperator->status == OP_EXEC_DONE) {
      return NULL;
    }
  }
}

SOperatorInfo* createSysTableScanOperatorInfo(void* readHandle, SSystemTableScanPhysiNode* pScanPhyNode,
                                              const char* pUser, SExecTaskInfo* pTaskInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SSysTableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SSysTableScanInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SScanPhysiNode*     pScanNode = &pScanPhyNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;

  int32_t num = 0;
  code = extractColMatchInfo(pScanNode->pScanCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  extractTbnameSlotId(pInfo, pScanNode);

  pInfo->pAPI = &pTaskInfo->storageAPI;

  pInfo->accountId = pScanPhyNode->accountId;
  pInfo->pUser = taosStrdup((void*)pUser);
  pInfo->sysInfo = pScanPhyNode->sysInfo;
  pInfo->showRewrite = pScanPhyNode->showRewrite;
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  pInfo->pCondition = pScanNode->node.pConditions;
  code = filterInitFromNode(pScanNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initLimitInfo(pScanPhyNode->scan.node.pLimit, pScanPhyNode->scan.node.pSlimit, &pInfo->limitInfo);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  tNameAssign(&pInfo->name, &pScanNode->tableName);
  const char* name = tNameGetTableName(&pInfo->name);

  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0) {
    pInfo->readHandle = *(SReadHandle*)readHandle;
  } else {
    tsem_init(&pInfo->ready, 0, 0);
    pInfo->epSet = pScanPhyNode->mgmtEpSet;
    pInfo->readHandle = *(SReadHandle*)readHandle;
  }

  setOperatorInfo(pOperator, "SysTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doSysTableScan, NULL, destroySysScanOperator,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroySysScanOperator(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void extractTbnameSlotId(SSysTableScanInfo* pInfo, const SScanPhysiNode* pScanNode) {
  pInfo->tbnameSlotId = -1;
  if (pScanNode->pScanPseudoCols != NULL) {
    SNode* pNode = NULL;
    FOREACH(pNode, pScanNode->pScanPseudoCols) {
      STargetNode* pTargetNode = NULL;
      if (nodeType(pNode) == QUERY_NODE_TARGET) {
        pTargetNode = (STargetNode*)pNode;
        SNode* expr = pTargetNode->pExpr;
        if (nodeType(expr) == QUERY_NODE_FUNCTION) {
          SFunctionNode* pFuncNode = (SFunctionNode*)expr;
          if (pFuncNode->funcType == FUNCTION_TYPE_TBNAME) {
            pInfo->tbnameSlotId = pTargetNode->slotId;
          }
        }
      }
    }
  }
}

void destroySysScanOperator(void* param) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)param;
  tsem_destroy(&pInfo->ready);
  blockDataDestroy(pInfo->pRes);

  const char* name = tNameGetTableName(&pInfo->name);
  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 || pInfo->pCur != NULL) {
    if (pInfo->pAPI->metaFn.closeTableMetaCursor != NULL) {
      pInfo->pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    }

    pInfo->pCur = NULL;
  }

  if (pInfo->pIdx) {
    taosArrayDestroy(pInfo->pIdx->uids);
    taosMemoryFree(pInfo->pIdx);
    pInfo->pIdx = NULL;
  }

  if (pInfo->pSchema) {
    taosHashCleanup(pInfo->pSchema);
    pInfo->pSchema = NULL;
  }

  taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFreeClear(pInfo->pUser);

  taosMemoryFreeClear(param);
}

int32_t loadSysTableCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SOperatorInfo*     operator=(SOperatorInfo*) param;
  SSysTableScanInfo* pScanResInfo = (SSysTableScanInfo*)operator->info;
  if (TSDB_CODE_SUCCESS == code) {
    pScanResInfo->pRsp = pMsg->pData;

    SRetrieveMetaTableRsp* pRsp = pScanResInfo->pRsp;
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->handle = htobe64(pRsp->handle);
    pRsp->compLen = htonl(pRsp->compLen);
  } else {
    operator->pTaskInfo->code = rpcCvtErrCode(code);
    if (operator->pTaskInfo->code != code) {
      qError("load systable rsp received, error:%s, cvted error:%s", tstrerror(code),
             tstrerror(operator->pTaskInfo->code));
    } else {
      qError("load systable rsp received, error:%s", tstrerror(code));
    }
  }

  tsem_post(&pScanResInfo->ready);
  return TSDB_CODE_SUCCESS;
}

static int32_t sysChkFilter__Comm(SNode* pNode) {
  // impl
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  EOperatorType  opType = pOper->opType;
  if (opType != OP_TYPE_EQUAL && opType != OP_TYPE_LOWER_EQUAL && opType != OP_TYPE_LOWER_THAN &&
      opType != OP_TYPE_GREATER_EQUAL && opType != OP_TYPE_GREATER_THAN) {
    return -1;
  }
  return 0;
}

static int32_t sysChkFilter__DBName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;

  if (pOper->opType != OP_TYPE_EQUAL && pOper->opType != OP_TYPE_NOT_EQUAL) {
    return -1;
  }

  SValueNode* pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }

  return 0;
}
static int32_t sysChkFilter__VgroupId(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__TableName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__CreateTime(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_TIMESTAMP_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}

static int32_t sysChkFilter__Ncolumn(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Ttl(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__STableName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Uid(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Type(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t optSysTabFilteImpl(void* arg, SNode* cond, SArray* result) {
  if (optSysCheckOper(cond) != 0) return -1;

  SOperatorNode* pNode = (SOperatorNode*)cond;

  int8_t i = 0;
  for (; i < SYSTAB_FILTER_DICT_SIZE; i++) {
    if (strcmp(filterDict[i].name, ((SColumnNode*)(pNode->pLeft))->colName) == 0) {
      break;
    }
  }
  if (i >= SYSTAB_FILTER_DICT_SIZE) return -1;

  if (filterDict[i].chkFunc(cond) != 0) return -1;

  return filterDict[i].fltFunc(arg, cond, result);
}

static int32_t optSysCheckOper(SNode* pOpear) {
  if (nodeType(pOpear) != QUERY_NODE_OPERATOR) return -1;

  SOperatorNode* pOper = (SOperatorNode*)pOpear;
  if (pOper->opType < OP_TYPE_GREATER_THAN || pOper->opType > OP_TYPE_NOT_EQUAL) {
    return -1;
  }

  if (nodeType(pOper->pLeft) != QUERY_NODE_COLUMN || nodeType(pOper->pRight) != QUERY_NODE_VALUE) {
    return -1;
  }
  return 0;
}

static FORCE_INLINE int optSysBinarySearch(SArray* arr, int s, int e, uint64_t k) {
  uint64_t v;
  int32_t  m;
  while (s <= e) {
    m = s + (e - s) / 2;
    v = *(uint64_t*)taosArrayGet(arr, m);
    if (v >= k) {
      e = m - 1;
    } else {
      s = m + 1;
    }
  }
  return s;
}

void optSysIntersection(SArray* in, SArray* out) {
  int32_t sz = (int32_t)taosArrayGetSize(in);
  if (sz <= 0) {
    return;
  }
  MergeIndex* mi = taosMemoryCalloc(sz, sizeof(MergeIndex));
  for (int i = 0; i < sz; i++) {
    SArray* t = taosArrayGetP(in, i);
    mi[i].len = (int32_t)taosArrayGetSize(t);
    mi[i].idx = 0;
  }

  SArray* base = taosArrayGetP(in, 0);
  for (int i = 0; i < taosArrayGetSize(base); i++) {
    uint64_t tgt = *(uint64_t*)taosArrayGet(base, i);
    bool     has = true;
    for (int j = 1; j < taosArrayGetSize(in); j++) {
      SArray* oth = taosArrayGetP(in, j);
      int     mid = optSysBinarySearch(oth, mi[j].idx, mi[j].len - 1, tgt);
      if (mid >= 0 && mid < mi[j].len) {
        uint64_t val = *(uint64_t*)taosArrayGet(oth, mid);
        has = (val == tgt ? true : false);
        mi[j].idx = mid;
      } else {
        has = false;
      }
    }
    if (has == true) {
      taosArrayPush(out, &tgt);
    }
  }
  taosMemoryFreeClear(mi);
}

static int tableUidCompare(const void* a, const void* b) {
  int64_t u1 = *(int64_t*)a;
  int64_t u2 = *(int64_t*)b;
  if (u1 == u2) {
    return 0;
  }
  return u1 < u2 ? -1 : 1;
}

static int32_t optSysMergeRslt(SArray* mRslt, SArray* rslt) {
  // TODO, find comm mem from mRslt
  for (int i = 0; i < taosArrayGetSize(mRslt); i++) {
    SArray* arslt = taosArrayGetP(mRslt, i);
    taosArraySort(arslt, tableUidCompare);
  }
  optSysIntersection(mRslt, rslt);
  return 0;
}

static int32_t optSysSpecialColumn(SNode* cond) {
  SOperatorNode* pOper = (SOperatorNode*)cond;
  SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
  for (int i = 0; i < sizeof(SYSTABLE_SPECIAL_COL) / sizeof(SYSTABLE_SPECIAL_COL[0]); i++) {
    if (0 == strcmp(pCol->colName, SYSTABLE_SPECIAL_COL[i])) {
      return 1;
    }
  }
  return 0;
}

static int32_t optSysTabFilte(void* arg, SNode* cond, SArray* result) {
  int ret = -1;
  if (nodeType(cond) == QUERY_NODE_OPERATOR) {
    ret = optSysTabFilteImpl(arg, cond, result);
    if (ret == 0) {
      SOperatorNode* pOper = (SOperatorNode*)cond;
      SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
      if (0 == strcmp(pCol->colName, "create_time")) {
        return 0;
      }
      return -1;
    }
    return ret;
  }

  if (nodeType(cond) != QUERY_NODE_LOGIC_CONDITION || ((SLogicConditionNode*)cond)->condType != LOGIC_COND_TYPE_AND) {
    return ret;
  }

  SLogicConditionNode* pNode = (SLogicConditionNode*)cond;
  SNodeList*           pList = (SNodeList*)pNode->pParameterList;

  int32_t len = LIST_LENGTH(pList);

  bool    hasIdx = false;
  bool    hasRslt = true;
  SArray* mRslt = taosArrayInit(len, POINTER_BYTES);

  SListCell* cell = pList->pHead;
  for (int i = 0; i < len; i++) {
    if (cell == NULL) break;

    SArray* aRslt = taosArrayInit(16, sizeof(int64_t));

    ret = optSysTabFilteImpl(arg, cell->pNode, aRslt);
    if (ret == 0) {
      // has index
      hasIdx = true;
      if (optSysSpecialColumn(cell->pNode) == 0) {
        taosArrayPush(mRslt, &aRslt);
      } else {
        // db_name/vgroup not result
        taosArrayDestroy(aRslt);
      }
    } else if (ret == -2) {
      // current vg
      hasIdx = true;
      hasRslt = false;
      taosArrayDestroy(aRslt);
      break;
    } else {
      taosArrayDestroy(aRslt);
    }
    cell = cell->pNext;
  }
  if (hasRslt && hasIdx) {
    optSysMergeRslt(mRslt, result);
  }

  for (int i = 0; i < taosArrayGetSize(mRslt); i++) {
    SArray* aRslt = taosArrayGetP(mRslt, i);
    taosArrayDestroy(aRslt);
  }
  taosArrayDestroy(mRslt);
  if (hasRslt == false) {
    return -2;
  }
  if (hasRslt && hasIdx) {
    cell = pList->pHead;
    for (int i = 0; i < len; i++) {
      if (cell == NULL) break;
      SOperatorNode* pOper = (SOperatorNode*)cell->pNode;
      SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
      if (0 == strcmp(pCol->colName, "create_time")) {
        return 0;
      }
      cell = cell->pNext;
    }
    return -1;
  }
  return -1;
}

static int32_t doGetTableRowSize(SReadHandle* pHandle, uint64_t uid, int32_t* rowLen, const char* idstr) {
  *rowLen = 0;

  SMetaReader mr = {0};
  pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, 0, &pHandle->api.metaFn);
  int32_t code = pHandle->api.metaReaderFn.getTableEntryByUid(&mr, uid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", uid, tstrerror(terrno), idstr);
    pHandle->api.metaReaderFn.clearReader(&mr);
    return terrno;
  }

  if (mr.me.type == TSDB_SUPER_TABLE) {
    int32_t numOfCols = mr.me.stbEntry.schemaRow.nCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.stbEntry.schemaRow.pSchema[i].bytes;
    }
  } else if (mr.me.type == TSDB_CHILD_TABLE) {
    uint64_t suid = mr.me.ctbEntry.suid;
    tDecoderClear(&mr.coder);
    code = pHandle->api.metaReaderFn.getTableEntryByUid(&mr, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno), idstr);
      pHandle->api.metaReaderFn.clearReader(&mr);
      return terrno;
    }

    int32_t numOfCols = mr.me.stbEntry.schemaRow.nCols;

    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.stbEntry.schemaRow.pSchema[i].bytes;
    }
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {
    int32_t numOfCols = mr.me.ntbEntry.schemaRow.nCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.ntbEntry.schemaRow.pSchema[i].bytes;
    }
  }

  pHandle->api.metaReaderFn.clearReader(&mr);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doBlockInfoScan(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SBlockDistInfo* pBlockScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;

  STableBlockDistInfo blockDistInfo = {.minRows = INT_MAX, .maxRows = INT_MIN};
  int32_t code = doGetTableRowSize(&pBlockScanInfo->readHandle, pBlockScanInfo->uid, (int32_t*)&blockDistInfo.rowSize,
                                   GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  pAPI->tsdReader.tsdReaderGetDataBlockDistInfo(pBlockScanInfo->pHandle, &blockDistInfo);
  blockDistInfo.numOfInmemRows = (int32_t)pAPI->tsdReader.tsdReaderGetNumOfInMemRows(pBlockScanInfo->pHandle);

  SSDataBlock* pBlock = pBlockScanInfo->pResBlock;

  int32_t          slotId = pOperator->exprSupp.pExprInfo->base.resSchema.slotId;
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t len = tSerializeBlockDistInfo(NULL, 0, &blockDistInfo);
  char*   p = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE);
  tSerializeBlockDistInfo(varDataVal(p), len, &blockDistInfo);
  varDataSetLen(p, len);

  colDataSetVal(pColInfo, 0, p, false);
  taosMemoryFree(p);

  // make the valgrind happy that all memory buffer has been initialized already.
  if (slotId != 0) {
    SColumnInfoData* p1 = taosArrayGet(pBlock->pDataBlock, 0);
    int64_t          v = 0;
    colDataSetInt64(p1, 0, &v);
  }

  pBlock->info.rows = 1;
  pOperator->status = OP_EXEC_DONE;
  return pBlock;
}

static void destroyBlockDistScanOperatorInfo(void* param) {
  SBlockDistInfo* pDistInfo = (SBlockDistInfo*)param;
  blockDataDestroy(pDistInfo->pResBlock);
  pDistInfo->readHandle.api.tsdReader.tsdReaderClose(pDistInfo->pHandle);
  tableListDestroy(pDistInfo->pTableListInfo);
  taosMemoryFreeClear(param);
}

static int32_t initTableblockDistQueryCond(uint64_t uid, SQueryTableDataCond* pCond) {
  memset(pCond, 0, sizeof(SQueryTableDataCond));

  pCond->order = TSDB_ORDER_ASC;
  pCond->numOfCols = 1;
  pCond->colList = taosMemoryCalloc(1, sizeof(SColumnInfo));
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t));
  if (pCond->colList == NULL || pCond->pSlotList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pCond->colList->colId = 1;
  pCond->colList->type = TSDB_DATA_TYPE_TIMESTAMP;
  pCond->colList->bytes = sizeof(TSKEY);

  pCond->pSlotList[0] = 0;

  pCond->twindows = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pCond->suid = uid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createDataBlockInfoScanOperator(SReadHandle* readHandle, SBlockDistScanPhysiNode* pBlockScanNode,
                                               STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo) {
  SBlockDistInfo* pInfo = taosMemoryCalloc(1, sizeof(SBlockDistInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->pResBlock = createDataBlockFromDescNode(pBlockScanNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pInfo->pResBlock, 1);

  {
    SQueryTableDataCond cond = {0};
    int32_t             code = initTableblockDistQueryCond(pBlockScanNode->suid, &cond);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    pInfo->pTableListInfo = pTableListInfo;
    size_t num = tableListGetSize(pTableListInfo);
    void*  pList = tableListGetInfo(pTableListInfo, 0);

    code = readHandle->api.tsdReader.tsdReaderOpen(readHandle->vnode, &cond, pList, num, pInfo->pResBlock,
                                                   (void**)&pInfo->pHandle, pTaskInfo->id.str, NULL);
    cleanupQueryTableDataCond(&cond);
    if (code != 0) {
      goto _error;
    }
  }

  pInfo->readHandle = *readHandle;
  pInfo->uid = (pBlockScanNode->suid != 0) ? pBlockScanNode->suid : pBlockScanNode->uid;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pBlockScanNode->pScanPseudoCols, NULL, &numOfCols);
  int32_t    code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfCols, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "DataBlockDistScanOperator", QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doBlockInfoScan, NULL, destroyBlockDistScanOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  return pOperator;

_error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}
