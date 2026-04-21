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
#include "geosWrapper.h"
#include "nodes.h"
#include "osMemPool.h"
#include "querynodes.h"
#include "systable.h"
#include "tcommon.h"
#include "tdef.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "index.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "storageapi.h"
#include "tref.h"
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

typedef struct {
  char vDbName[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE];
  char vStbName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE];
  char vTableName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE];

  char vColName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE];

  char    refDbName[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE];
  char    refStbName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE];
  char    refTableName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE];
  char    refColName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE];
  int8_t  type;
  int8_t  isValid;
  int32_t errCode;
  char    errMsg[64 + VARSTR_HEADER_SIZE];

} SVirtualTableRefInfo;

typedef struct SSysTableScanInfo {
  SRetrieveMetaTableRsp* pRsp;
  SRetrieveTableReq      req;
  SEpSet                 epSet;
  tsem_t                 ready;
  int64_t                self;      // ref ID in sysTableScanRefPool (for callback safety)
  int32_t                rspCode;   // error code set by the RPC callback
  SReadHandle            readHandle;
  const char*            pUser;
  int32_t                accountId;
  bool                   sysInfo;
  bool                   showRewrite;
  bool                   restore;
  bool                   skipFilterTable;
  union {
    uint16_t privInfo;
    struct {
      uint16_t privLevel : 3;  // user privilege level
      uint16_t privInfoBasic : 1;
      uint16_t privInfoPrivileged : 1;
      uint16_t privInfoAudit : 1;
      uint16_t privInfoSec : 1;
      uint16_t privPerfBasic : 1;
      uint16_t privPerfPrivileged : 1;
      uint16_t reserved1 : 7;
    };
  };
  SNode*              pCondition;  // db_name filter condition, to discard data that are not in current database
  SMTbCursor*         pCur;        // cursor for iterate the local table meta store.
  SSysTableIndex*     pIdx;        // idx for local table meta
  SHashObj*           pSchema;
  SColMatchInfo       matchInfo;
  SName               name;
  SSDataBlock*        pRes;
  int64_t             numOfBlocks;  // extract basic running information.
  SLoadRemoteDataInfo loadInfo;
  SLimitInfo          limitInfo;
  int32_t             tbnameSlotId;
  STableListInfo*     pTableListInfo;
  SReadHandle*        pHandle;
  SStorageAPI*        pAPI;

  // file set iterate
  struct SFileSetReader* pFileSetReader;
  SHashObj*              pExtSchema;

  // for virtual supertable scan
  STableListInfo* pSubTableListInfo;
  SArray*         pVtbRefReqs;   // SArray<SSysTableScanVtbRefReq> used by layered vc-cols lookup
  int32_t         vtbRefReqIdx;  // next request index in pVtbRefReqs
} SSysTableScanInfo;

// Lightweight wrapper passed as RPC callback param; stores only the ref ID so
// the callback can safely acquire the SSysTableScanInfo from the ref pool.
typedef struct SSysTableScanCbParam {
  int64_t sysTableScanId;
} SSysTableScanCbParam;

// Per-file ref pool used to decouple the callback lifetime from the operator
// lifetime, following the same pattern as fetchObjRefPool in exchangeoperator.c.
static int32_t      sysTableScanRefPool = -1;
static TdThreadOnce sysTableScanRefPoolOnce = PTHREAD_ONCE_INIT;

static void doDestroySysTableScanInfo(void* param);

static void cleanupSysTableScanRefPool(void) {
  int32_t ref = atomic_val_compare_exchange_32(&sysTableScanRefPool, sysTableScanRefPool, 0);
  taosCloseRef(ref);
}

static void initSysTableScanRefPool(void) {
  sysTableScanRefPool = taosOpenRef(64, doDestroySysTableScanInfo);
  (void)atexit(cleanupSysTableScanRefPool);
}

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

typedef struct {
  int8_t   type;
  tb_uid_t uid;
} STableId;

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

static int32_t buildDbTableInfoBlock(const SSysTableScanInfo* pInfo, const SSDataBlock* p,
                                     const SSysTableMeta* pSysDbTableMeta, size_t size, const char* dbName,
                                     int64_t* pRows);

static char* SYSTABLE_SPECIAL_COL[] = {"db_name", "vgroup_id"};

static int32_t        buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity);
static SSDataBlock*   buildInfoSchemaTableMetaBlock(char* tableName);
static void           destroySysScanOperator(void* param);
static int32_t        loadSysTableCallback(void* param, SDataBuf* pMsg, int32_t code);
static __optSysFilter optSysGetFilterFunc(int32_t ctype, bool* reverse, bool* equal);

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock,
                                                uint64_t reqId, SExecTaskInfo* pTaskInfo);

static int32_t sysTableUserColsFillOneTableCols(const char* dbname, int32_t* pNumOfRows, const SSDataBlock* dataBlock,
                                                char* tName, SSchemaWrapper* schemaRow, SExtSchema* extSchemaRow,
                                                char* tableType, SColRefWrapper* colRef);

static int32_t sysTableUserColsFillOneVirtualTableCols(const SSysTableScanInfo* pInfo, const char* dbname,
                                                       int32_t* pNumOfRows, const SSDataBlock* dataBlock, char* tName,
                                                       char* stName, SSchemaWrapper* schemaRow, char* tableType,
                                                       SColRefWrapper* colRef, tb_uid_t uid, int32_t vgId);

static int32_t sysTableUserColsFillOneVirtualTableCol(const char* dbname, int32_t* pNumOfRows,
                                                      const SSDataBlock* dataBlock, char* tName, char* stName,
                                                      SSchemaWrapper* schemaRow, SColRefWrapper* colRef, tb_uid_t uid,
                                                      int32_t vgId, int32_t colIdx);
static int32_t sysTableScanApplyVtbRefReqParam(SOperatorInfo* pOperator);
static SSDataBlock* sysTableScanUserVcColsByReqs(SOperatorInfo* pOperator);


// static int32_t sysTableFillOneVirtualTableRef(const SSysTableScanInfo* pInfo, const char* dbname, int32_t*
// pNumOfRows,
//                                               const SSDataBlock* dataBlock, char* tName, char* stName,
//                                               SSchemaWrapper* schemaRow, char* tableType, SColRefWrapper* colRef,
//                                               tb_uid_t uid, int32_t vgId);

static int32_t sysTableFillOneVirtualTableRefImpl(const SSysTableScanInfo* pInfo, SExecTaskInfo* pTaskInfo,
                                                  const char* dbname, int32_t* pNumOfRows, const SSDataBlock* dataBlock,
                                                  SSchemaWrapper* schemaRow, SColRefWrapper* pRefCol,
                                                  SVirtualTableRefInfo* pRef);

static void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                               SFilterInfo* pFilterInfo, SExecTaskInfo* pTaskInfo);

static int32_t vnodeEstimateRawDataSize(SOperatorInfo* pOperator, SDbSizeStatisInfo* pStatisInfo);

static int32_t vtbRefResolveSrcColumnChain(const SSysTableScanInfo* pInfo, SExecTaskInfo* pTaskInfo, const char* refDbName,
                                           const char* refTableName, const char* refColName, SHashObj* pDbVgInfoCache,
                                           SHashObj* pTableCache, int32_t localVgId, SHashObj* pSeenRefs,
                                           int32_t depth, int32_t* pErrCode);

int32_t sysFilte__DbName(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;
  void*        pVnode = pArg->pVnode;

  const char* db = NULL;
  pArg->pAPI->metaFn.getBasicInfo(pVnode, &db, NULL, NULL, NULL);

  SName   sn = {0};
  char    dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  code = tNameGetDbName(&sn, varDataVal(dbname));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }
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
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_LOWER_THAN, a, b);
}
static int optSysFilterFuncImpl__LowerEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_LOWER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__GreaterThan(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_GREATER_THAN, a, b);
}
static int optSysFilterFuncImpl__GreaterEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_GREATER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__Equal(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_EQUAL, a, b);
}

static int optSysFilterFuncImpl__NoEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
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
        tstrncpy(condTable, varDataVal(value), TSDB_TABLE_NAME_LEN);
        return true;
      }
    }
  }
  return false;
}

static bool sysTableIsOperatorCondOnOneVTableName(SNode* pCond, char* condTable) {
  SOperatorNode* node = (SOperatorNode*)pCond;
  if (node->opType == OP_TYPE_EQUAL) {
    if (nodeType(node->pLeft) == QUERY_NODE_COLUMN &&
        strcasecmp(nodesGetNameFromColumnNode(node->pLeft), "virtual_table_name") == 0 &&
        nodeType(node->pRight) == QUERY_NODE_VALUE) {
      SValueNode* pValue = (SValueNode*)node->pRight;
      if (pValue->node.resType.type == TSDB_DATA_TYPE_NCHAR || pValue->node.resType.type == TSDB_DATA_TYPE_VARCHAR) {
        char* value = nodesGetValueFromNode(pValue);
        tstrncpy(condTable, varDataVal(value), TSDB_TABLE_NAME_LEN);
        return true;
      }
    }
  }
  return false;
}

static bool sysTableIsCondOnOneVTableName(SNode* pCond, char* condTable) {
  if (pCond == NULL) {
    return false;
  }
  if (nodeType(pCond) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode* node = (SLogicConditionNode*)pCond;
    if (LOGIC_COND_TYPE_AND == node->condType) {
      SNode* pChild = NULL;
      FOREACH(pChild, node->pParameterList) {
        if (QUERY_NODE_OPERATOR == nodeType(pChild) && sysTableIsOperatorCondOnOneVTableName(pChild, condTable)) {
          return true;
        }
      }
    }
  }

  if (QUERY_NODE_OPERATOR == nodeType(pCond)) {
    return sysTableIsOperatorCondOnOneVTableName(pCond, condTable);
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

static SSDataBlock* doOptimizeTableNameFilter(SOperatorInfo* pOperator, SSDataBlock* dataBlock, char* dbname) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;

  char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(tableName, pInfo->req.filterTb);

  SMetaReader smrTable = {0};
  SMetaReader smrSuperTable = {0};
  bool        hasSuperTable = false;
  pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  int32_t code = pAPI->metaReaderFn.getTableEntryByName(&smrTable, pInfo->req.filterTb);
  if (code != TSDB_CODE_SUCCESS) {
    // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
    pAPI->metaReaderFn.clearReader(&smrTable);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  if (smrTable.me.type == TSDB_SUPER_TABLE) {
    pAPI->metaReaderFn.clearReader(&smrTable);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  if (smrTable.me.type == TSDB_CHILD_TABLE) {
    int64_t suid = smrTable.me.ctbEntry.suid;
    pAPI->metaReaderFn.clearReader(&smrTable);
    pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrTable, suid);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
      pAPI->metaReaderFn.clearReader(&smrTable);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }
  }

  char            typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  SSchemaWrapper* schemaRow = NULL;
  SExtSchema*     extSchemaRow = smrTable.me.pExtSchemas;
  SColRefWrapper* colRef = NULL;
  if (smrTable.me.type == TSDB_SUPER_TABLE) {
    schemaRow = &smrTable.me.stbEntry.schemaRow;
    STR_TO_VARSTR(typeName, "CHILD_TABLE");
  } else if (smrTable.me.type == TSDB_NORMAL_TABLE) {
    schemaRow = &smrTable.me.ntbEntry.schemaRow;
    STR_TO_VARSTR(typeName, "NORMAL_TABLE");
  } else if (smrTable.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
    schemaRow = &smrTable.me.ntbEntry.schemaRow;
    colRef = &smrTable.me.colRef;
    STR_TO_VARSTR(typeName, "VIRTUAL_NORMAL_TABLE");
  } else if (smrTable.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    hasSuperTable = true;
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, smrTable.me.ctbEntry.suid);
    if (code != TSDB_CODE_SUCCESS) {
      pAPI->metaReaderFn.clearReader(&smrTable);
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }
    schemaRow = &smrSuperTable.me.stbEntry.schemaRow;
    extSchemaRow = smrSuperTable.me.pExtSchemas;
    colRef = &smrTable.me.colRef;
    STR_TO_VARSTR(typeName, "VIRTUAL_CHILD_TABLE");
  }

  code = sysTableUserColsFillOneTableCols(dbname, &numOfRows, dataBlock, tableName, schemaRow, extSchemaRow, typeName,
                                          colRef);
  if (code != TSDB_CODE_SUCCESS) {
    if (hasSuperTable) {
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
    }
    pAPI->metaReaderFn.clearReader(&smrTable);
    pInfo->loadInfo.totalRows = 0;
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  if (hasSuperTable) {
    pAPI->metaReaderFn.clearReader(&smrSuperTable);
  }
  pAPI->metaReaderFn.clearReader(&smrTable);

  if (numOfRows > 0) {
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  setOperatorCompleted(pOperator);

  qDebug("get cols success, total rows:%" PRIu64 ", current:%" PRId64 " %s", pInfo->loadInfo.totalRows,
         pInfo->pRes->info.rows, GET_TASKID(pTaskInfo));
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* doOptimizeVTableNameFilter(SOperatorInfo* pOperator, SSDataBlock* dataBlock, char* dbname) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  int32_t            numOfRows = 0;

  char vtableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(vtableName, pInfo->req.filterTb);

  SVirtualTableRefInfo* pVtableRefInfo = taosMemoryCalloc(1, sizeof(SVirtualTableRefInfo));
  if (pVtableRefInfo == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, terrstr());
    pTaskInfo->code = terrno;
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  memcpy(pVtableRefInfo->vDbName, dbname, TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE);

  SMetaReader smrTable = {0};
  SMetaReader smrSuperTable = {0};
  bool        smrTableInited = false;
  bool        smrSuperTableInited = false;

  pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  smrTableInited = true;
  code = pAPI->metaReaderFn.getTableEntryByName(&smrTable, pInfo->req.filterTb);
  QUERY_CHECK_CODE(code, lino, _end);

  if (smrTable.me.type != TSDB_VIRTUAL_NORMAL_TABLE && smrTable.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
    pAPI->metaReaderFn.clearReader(&smrTable);
    smrTableInited = false;
    code = TSDB_CODE_SUCCESS;
    goto _end;
  }

  SSchemaWrapper* schemaRow = NULL;
  SColRefWrapper* colRef = NULL;

  if (smrTable.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
    schemaRow = &smrTable.me.ntbEntry.schemaRow;
    colRef = &smrTable.me.colRef;
    STR_TO_VARSTR(pVtableRefInfo->vStbName, smrTable.me.name);
    STR_TO_VARSTR(pVtableRefInfo->vTableName, smrTable.me.name);
    // Release the META_READER_LOCK before calling validateSrcTableColRef,
    // which may send RPCs that could deadlock if the lock is held.
    // The data (schemaRow, colRef) remains valid until clearReader is called.
    pAPI->metaReaderFn.readerReleaseLock(&smrTable);
  } else if (smrTable.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
    colRef = &smrTable.me.colRef;
    STR_TO_VARSTR(pVtableRefInfo->vTableName, smrTable.me.name);

    int64_t suid = smrTable.me.ctbEntry.suid;
    // Release lock but keep data valid (colRef still points to smrTable internal data)
    pAPI->metaReaderFn.readerReleaseLock(&smrTable);

    // Get super table info for virtual child table
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    smrSuperTableInited = true;
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
    QUERY_CHECK_CODE(code, lino, _end);

    STR_TO_VARSTR(pVtableRefInfo->vStbName, smrSuperTable.me.name);
    schemaRow = &smrSuperTable.me.stbEntry.schemaRow;

    // Release lock but keep data valid for sysTableFillOneVirtualTableRefImpl
    pAPI->metaReaderFn.readerReleaseLock(&smrSuperTable);
  }

  if (schemaRow != NULL && schemaRow->pSchema == NULL) {
    qWarn("doOptimizeVTableNameFilter: vstb schema pSchema is NULL for table %s, returning empty", pInfo->req.filterTb);
    schemaRow = NULL;
  }

  if (schemaRow == NULL) {
    goto _end;
  }

  code = sysTableFillOneVirtualTableRefImpl(pInfo, pTaskInfo, dbname, &numOfRows, dataBlock, schemaRow, colRef,
                                            pVtableRefInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  if (numOfRows > 0) {
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  setOperatorCompleted(pOperator);

  qDebug("get virtual table ref success, total rows:%" PRIu64 ", current:%" PRId64 " %s", pInfo->loadInfo.totalRows,
         pInfo->pRes->info.rows, GET_TASKID(pTaskInfo));

_end:
  if (smrTableInited) {
    pAPI->metaReaderFn.clearReader(&smrTable);
  }
  if (smrSuperTableInited) {
    pAPI->metaReaderFn.clearReader(&smrSuperTable);
  }
  taosMemoryFreeClear(pVtableRefInfo);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

int32_t doExtractDbName(char* dbname, SSysTableScanInfo* pInfo, SStorageAPI* pAPI) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SName       sn = {0};
  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* sysTableScanUserCols(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;
  int32_t            ret = 0;
  char               dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  SSDataBlock*       pDataBlock = NULL;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  pDataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_COLS);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pDataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->pVtbRefReqs != NULL) {
    blockDataDestroy(pDataBlock);
    return sysTableScanUserVcColsByReqs(pOperator);
  }

  code = doExtractDbName(dbname, pInfo, pAPI);
  QUERY_CHECK_CODE(code, lino, _end);

  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->req.filterTb[0]) {
    SSDataBlock* p = doOptimizeTableNameFilter(pOperator, pDataBlock, dbname);
    blockDataDestroy(pDataBlock);
    return p;
  }

  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
  } else {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 0);
    if (code != 0) {
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pInfo->pSchema == NULL) {
    pInfo->pSchema = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    taosHashSetFreeFp(pInfo->pSchema, tDeleteSSchemaWrapperForHash);
  }
  if (pInfo->pExtSchema == NULL) {
    pInfo->pExtSchema = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  }

  if (!pInfo->pCur || !pInfo->pSchema || !pInfo->pExtSchema) {
    qError("sysTableScanUserCols failed since %s", terrstr());
    blockDataDestroy(pDataBlock);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  while (((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_TABLE_MAX)) == 0)) {
    char typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

    SSchemaWrapper* schemaRow = NULL;
    SExtSchema*     extSchemaRow = NULL;
    SColRefWrapper* colRef = NULL;

    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      qDebug("sysTableScanUserCols cursor get super table, %s", GET_TASKID(pTaskInfo));
      void* schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t));
      if (schema == NULL) {
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&pInfo->pCur->mr.me.stbEntry.schemaRow);
        if (pInfo->pCur->mr.me.stbEntry.schemaRow.pSchema) {
          QUERY_CHECK_NULL(schemaWrapper, code, lino, _end, terrno);
        }
        code = taosHashPut(pInfo->pSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        QUERY_CHECK_CODE(code, lino, _end);
      }

      void* pExtSchema = taosHashGet(pInfo->pExtSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t));
      if (pExtSchema == NULL) {
        SExtSchema* pExtSchema = pInfo->pCur->mr.me.pExtSchemas;
        code = taosHashPut(pInfo->pExtSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t), pExtSchema,
                           pInfo->pCur->mr.me.stbEntry.schemaRow.nCols * sizeof(SExtSchema));
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        QUERY_CHECK_CODE(code, lino, _end);
      }

      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      qDebug("sysTableScanUserCols cursor get child table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(typeName, "CHILD_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &suid, sizeof(int64_t));
      void*   pExtSchema = taosHashGet(pInfo->pExtSchema, &suid, sizeof(int64_t));
      if (schema != NULL && pExtSchema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
        extSchemaRow = (SExtSchema*)pExtSchema;
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        if (smrSuperTable.me.stbEntry.schemaRow.pSchema) {
          if (schemaWrapper == NULL) {
            code = terrno;
            lino = __LINE__;
            pAPI->metaReaderFn.clearReader(&smrSuperTable);
            goto _end;
          }
        }
        code = taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        SExtSchema* pExtSchema = smrSuperTable.me.pExtSchemas;
        code = taosHashPut(pInfo->pExtSchema, &suid, sizeof(int64_t), pExtSchema,
                           smrSuperTable.me.stbEntry.schemaRow.nCols * sizeof(SExtSchema));
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        schemaRow = schemaWrapper;
        extSchemaRow = taosHashGet(pInfo->pExtSchema, &suid, sizeof(int64_t));
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      qDebug("sysTableScanUserCols cursor get normal table, %s", GET_TASKID(pTaskInfo));
      schemaRow = &pInfo->pCur->mr.me.ntbEntry.schemaRow;
      extSchemaRow = pInfo->pCur->mr.me.pExtSchemas;
      STR_TO_VARSTR(typeName, "NORMAL_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
      qDebug("sysTableScanUserCols cursor get virtual normal table, %s", GET_TASKID(pTaskInfo));
      schemaRow = &pInfo->pCur->mr.me.ntbEntry.schemaRow;
      extSchemaRow = pInfo->pCur->mr.me.pExtSchemas;
      STR_TO_VARSTR(typeName, "VIRTUAL_NORMAL_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
      colRef = &pInfo->pCur->mr.me.colRef;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
      qDebug("sysTableScanUserCols cursor get virtual child table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(typeName, "VIRTUAL_CHILD_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

      colRef = &pInfo->pCur->mr.me.colRef;
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &suid, sizeof(int64_t));
      void*   pExtSchema = taosHashGet(pInfo->pExtSchema, &suid, sizeof(int64_t));
      if (schema != NULL && pExtSchema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
        extSchemaRow = (SExtSchema*)pExtSchema;
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        if (smrSuperTable.me.stbEntry.schemaRow.pSchema) {
          if (schemaWrapper == NULL) {
            code = terrno;
            lino = __LINE__;
            pAPI->metaReaderFn.clearReader(&smrSuperTable);
            goto _end;
          }
        }
        code = taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        SExtSchema* pExtSchema = smrSuperTable.me.pExtSchemas;
        code = taosHashPut(pInfo->pExtSchema, &suid, sizeof(int64_t), pExtSchema,
                           smrSuperTable.me.stbEntry.schemaRow.nCols * sizeof(SExtSchema));
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        schemaRow = schemaWrapper;
        extSchemaRow = taosHashGet(pInfo->pExtSchema, &suid, sizeof(int64_t));
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      qDebug("sysTableScanUserCols cursor get invalid table, %s", GET_TASKID(pTaskInfo));
      continue;
    }

    if ((numOfRows + schemaRow->nCols) > pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        break;
      }
    }
    // if pInfo->pRes->info.rows == 0, also need to add the meta to pDataBlock
    code = sysTableUserColsFillOneTableCols(dbname, &numOfRows, pDataBlock, tableName, schemaRow, extSchemaRow,
                                            typeName, colRef);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  blockDataDestroy(pDataBlock);
  pDataBlock = NULL;
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  qDebug("get cols success, rows:%" PRIu64 " %s", pInfo->loadInfo.totalRows, GET_TASKID(pTaskInfo));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pDataBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static bool virtualChildTableNeedCollect(STableListInfo* pTableListInfo, tb_uid_t tableUid) {
  for (int32_t i = 0; i < taosArrayGetSize(pTableListInfo->pTableList); i++) {
    tb_uid_t* childUid = taosArrayGet(pTableListInfo->pTableList, i);
    if (childUid == NULL) {
      return false;
    }
    if (*childUid == tableUid) {
      return true;
    }
  }
  return false;
}

static SSDataBlock* sysTableScanUserVcCols(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;
  int32_t            ret = 0;
  char               dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  SSDataBlock*       pDataBlock = NULL;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  pDataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_VC_COLS);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pDataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->pVtbRefReqs != NULL) {
    blockDataDestroy(pDataBlock);
    return sysTableScanUserVcColsByReqs(pOperator);
  }

  code = doExtractDbName(dbname, pInfo, pAPI);
  QUERY_CHECK_CODE(code, lino, _end);

  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->req.filterTb[0]) {
    SSDataBlock* p = doOptimizeTableNameFilter(pOperator, pDataBlock, dbname);
    blockDataDestroy(pDataBlock);
    return p;
  }

  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
  } else {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 0);
    if (code != 0) {
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pInfo->pSchema == NULL) {
    pInfo->pSchema = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    taosHashSetFreeFp(pInfo->pSchema, tDeleteSSchemaWrapperForHash);
  }

  if (!pInfo->pCur || !pInfo->pSchema) {
    qError("sysTableScanUserVcCols failed since %s", terrstr());
    blockDataDestroy(pDataBlock);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  while (((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_TABLE_MAX)) == 0)) {
    char typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

    SSchemaWrapper* schemaRow = NULL;
    SColRefWrapper* colRef = NULL;

    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
      qDebug("sysTableScanUserVcCols cursor get virtual normal table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(typeName, "VIRTUAL_NORMAL_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
      STR_TO_VARSTR(stableName, pInfo->pCur->mr.me.name);

      colRef = &pInfo->pCur->mr.me.colRef;
      schemaRow = &pInfo->pCur->mr.me.ntbEntry.schemaRow;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
      qDebug("sysTableScanUserVcCols cursor get virtual child table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(typeName, "VIRTUAL_CHILD_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
      if (!virtualChildTableNeedCollect(pInfo->pSubTableListInfo, pInfo->pCur->mr.me.uid)) {
        qDebug("skip virtual child table:%s uid:%" PRId64 " %s", varDataVal(tableName), pInfo->pCur->mr.me.uid,
               GET_TASKID(pTaskInfo));
        continue;
      }
      colRef = &pInfo->pCur->mr.me.colRef;
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.ctbEntry.suid, sizeof(int64_t));
      if (schema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserVcCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        STR_TO_VARSTR(stableName, smrSuperTable.me.name);
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserVcCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        STR_TO_VARSTR(stableName, smrSuperTable.me.name);
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        if (smrSuperTable.me.stbEntry.schemaRow.pSchema) {
          if (schemaWrapper == NULL) {
            code = terrno;
            lino = __LINE__;
            pAPI->metaReaderFn.clearReader(&smrSuperTable);
            goto _end;
          }
        }
        code = taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        schemaRow = schemaWrapper;
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      qDebug("sysTableScanUserVcCols cursor get invalid table, %s", GET_TASKID(pTaskInfo));
      continue;
    }

    {
      int32_t nTagRefs = (colRef != NULL) ? colRef->nTagRefs : 0;
      if ((numOfRows + schemaRow->nCols + nTagRefs) > pOperator->resultInfo.capacity) {
        relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
        numOfRows = 0;

        if (pInfo->pRes->info.rows > 0) {
          pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
          break;
        }
      }
    }
    // if pInfo->pRes->info.rows == 0, also need to add the meta to pDataBlock
    code = sysTableUserColsFillOneVirtualTableCols(pInfo, dbname, &numOfRows, pDataBlock, tableName, stableName,
                                                   schemaRow, typeName, colRef, pInfo->pCur->mr.me.uid,
                                                   pOperator->pTaskInfo->id.vgId);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  blockDataDestroy(pDataBlock);
  pDataBlock = NULL;
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  qDebug("get cols success, rows:%" PRIu64 " %s", pInfo->loadInfo.totalRows, GET_TASKID(pTaskInfo));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pDataBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanVirtualTableRef(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;
  int32_t            ret = 0;
  char               dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  SSDataBlock*       pDataBlock = NULL;

  // skip mnd read
  if (pInfo->readHandle.mnd != NULL) {
    setOperatorCompleted(pOperator);
    return NULL;
  }

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  pDataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_VIRTUAL_TABLES_REFERENCING);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pDataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->pCondition && sysTableIsCondOnOneVTableName(pInfo->pCondition, pInfo->req.filterTb)) {
    code = doExtractDbName(dbname, pInfo, pAPI);
    QUERY_CHECK_CODE(code, lino, _end);

    SSDataBlock* p = doOptimizeVTableNameFilter(pOperator, pDataBlock, dbname);
    pInfo->pRes = p;
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  }

  SVirtualTableRefInfo* pVtableRefInfo = taosMemoryCalloc(1, sizeof(SVirtualTableRefInfo));
  if (pVtableRefInfo == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, terrstr());
    pTaskInfo->code = terrno;
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  code = doExtractDbName(pVtableRefInfo->vDbName, pInfo, pAPI);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
  } else {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 0);
    if (code != 0) {
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pInfo->pSchema == NULL) {
    pInfo->pSchema = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    taosHashSetFreeFp(pInfo->pSchema, tDeleteSSchemaWrapperForHash);
  }

  if (!pInfo->pCur || !pInfo->pSchema) {
    qError("sysTableScanUserVcCols failed since %s", terrstr());
    blockDataDestroy(pDataBlock);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  while (((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_TABLE_MAX)) == 0)) {
    SSchemaWrapper* schemaRow = NULL;
    SColRefWrapper* colRef = NULL;

    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
      qDebug("sysTableScanUserVcCols cursor get virtual normal table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(pVtableRefInfo->vTableName, pInfo->pCur->mr.me.name);
      STR_TO_VARSTR(pVtableRefInfo->vStbName, pInfo->pCur->mr.me.name);

      colRef = &pInfo->pCur->mr.me.colRef;
      schemaRow = &pInfo->pCur->mr.me.ntbEntry.schemaRow;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
      qDebug("sysTableScanUserVcCols cursor get virtual child table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(pVtableRefInfo->vTableName, pInfo->pCur->mr.me.name);

      colRef = &pInfo->pCur->mr.me.colRef;
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.ctbEntry.suid, sizeof(int64_t));
      if (schema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserVcCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        STR_TO_VARSTR(pVtableRefInfo->vStbName, smrSuperTable.me.name);
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserVcCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        STR_TO_VARSTR(pVtableRefInfo->vStbName, smrSuperTable.me.name);
        bool hasSchema = (smrSuperTable.me.stbEntry.schemaRow.pSchema != NULL);
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        if (schemaWrapper == NULL) {
          if (!hasSchema) {
            qWarn("sysTableScanVirtualTableRef: vstb suid:%" PRId64
                  " has no schema, skipping virtual child table %s", suid, pInfo->pCur->mr.me.name);
            continue;
          }
          code = terrno;
          lino = __LINE__;
          goto _end;
        }
        code = taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code != TSDB_CODE_SUCCESS) {
          tDeleteSchemaWrapper(schemaWrapper);
          QUERY_CHECK_CODE(code, lino, _end);
        }

        schemaRow = schemaWrapper;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      qDebug("sysTableScanUserVcCols cursor get invalid table, %s", GET_TASKID(pTaskInfo));
      continue;
    }

    if ((numOfRows + schemaRow->nCols) > pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        break;
      }
    }

    // if pInfo->pRes->info.rows == 0, also need to add the meta to pDataBlock
    code = sysTableFillOneVirtualTableRefImpl(pInfo, pTaskInfo, dbname, &numOfRows, pDataBlock, schemaRow, colRef,
                                              pVtableRefInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  blockDataDestroy(pDataBlock);
  pDataBlock = NULL;
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  qDebug("get cols success, rows:%" PRIu64 " %s", pInfo->loadInfo.totalRows, GET_TASKID(pTaskInfo));

_end:

  taosMemoryFreeClear(pVtableRefInfo);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pDataBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserTags(SOperatorInfo* pOperator) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;
  SSDataBlock*   dataBlock = NULL;

  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  dataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TAGS);
  QUERY_CHECK_NULL(dataBlock, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(dataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  char condTableName[TSDB_TABLE_NAME_LEN] = {0};
  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->pCondition && sysTableIsCondOnOneTable(pInfo->pCondition, condTableName)) {
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, condTableName);

    SMetaReader smrChildTable = {0};
    pAPI->metaReaderFn.initReader(&smrChildTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByName(&smrChildTable, condTableName);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
      pAPI->metaReaderFn.clearReader(&smrChildTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    if (smrChildTable.me.type != TSDB_CHILD_TABLE && smrChildTable.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
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

    code = sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &smrChildTable, dbname, tableName, &numOfRows,
                                            dataBlock, pTaskInfo->id.queryId, pTaskInfo);

    pAPI->metaReaderFn.clearReader(&smrSuperTable);
    pAPI->metaReaderFn.clearReader(&smrChildTable);

    QUERY_CHECK_CODE(code, lino, _end);

    if (numOfRows > 0) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
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
    QUERY_CHECK_NULL(pInfo->pCur, code, lino, _end, terrno);
  } else {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 0);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  while ((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_SUPER_TABLE)) == 0) {
    if (pInfo->pCur->mr.me.type != TSDB_CHILD_TABLE && pInfo->pCur->mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

    SMetaReader smrSuperTable = {0};
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
    uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno),
             GET_TASKID(pTaskInfo));
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      blockDataDestroy(dataBlock);
      dataBlock = NULL;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    if ((smrSuperTable.me.stbEntry.schemaTag.nCols + numOfRows) > pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        break;
      }
    }

    // if pInfo->pRes->info.rows == 0, also need to add the meta to pDataBlock
    code = sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &pInfo->pCur->mr, dbname, tableName, &numOfRows,
                                            dataBlock, pTaskInfo->id.queryId, pTaskInfo);

    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      blockDataDestroy(dataBlock);
      dataBlock = NULL;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }
    pAPI->metaReaderFn.clearReader(&smrSuperTable);
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  blockDataDestroy(dataBlock);
  dataBlock = NULL;
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(dataBlock);
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                        SFilterInfo* pFilterInfo, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  dataBlock->info.rows = numOfRows;
  pInfo->pRes->info.rows = numOfRows;

  code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, dataBlock->pDataBlock, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = doFilter(pInfo->pRes, pFilterInfo, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  blockDataCleanup(dataBlock);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

int32_t convertTagDataToStr(char* str, int32_t strBuffLen, int type, void* buf, int32_t bufSize, int32_t* len) {
  int32_t n = 0;

  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      n = snprintf(str, strBuffLen, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      n = snprintf(str, strBuffLen, (*(int8_t*)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = snprintf(str, strBuffLen, "%d", *(int8_t*)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = snprintf(str, strBuffLen, "%d", *(int16_t*)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = snprintf(str, strBuffLen, "%d", *(int32_t*)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = snprintf(str, strBuffLen, "%" PRId64, *(int64_t*)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = snprintf(str, strBuffLen, "%.5f", GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = snprintf(str, strBuffLen, "%.9f", GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
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

      int32_t length = taosUcs4ToMbs((TdUcs4*)buf, bufSize, str, NULL);
      if (length <= 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      n = length;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      n = snprintf(str, strBuffLen, "%u", *(uint8_t*)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = snprintf(str, strBuffLen, "%u", *(uint16_t*)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = snprintf(str, strBuffLen, "%u", *(uint32_t*)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = snprintf(str, strBuffLen, "%" PRIu64, *(uint64_t*)buf);
      break;

    default:
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (len) *len = n;

  return TSDB_CODE_SUCCESS;
}

static int32_t sysTableGetGeomText(char* iGeom, int32_t nGeom, char** output, int32_t* nOutput) {
#ifdef USE_GEOS
  int32_t code = 0;
  char*   outputWKT = NULL;

  if (nGeom == 0) {
    if (!(*output = taosStrdup(""))) code = terrno;
    *nOutput = 0;
    return code;
  }

  if (TSDB_CODE_SUCCESS != (code = initCtxAsText()) ||
      TSDB_CODE_SUCCESS != (code = doAsText(iGeom, nGeom, &outputWKT))) {
    qError("geo text for systable failed:%s", getGeosErrMsg(code));
    *output = NULL;
    *nOutput = 0;
    return code;
  }

  *output = outputWKT;
  *nOutput = strlen(outputWKT);

  return code;
#else
  TAOS_RETURN(TSDB_CODE_OPS_NOT_SUPPORT);
#endif
}

static int32_t vtbRefGetDbVgInfo(void* clientRpc, SEpSet* pEpSet, int32_t acctId, const char* dbName, uint64_t reqId,
                                 SExecTaskInfo* pTaskInfo, SDBVgInfo** ppVgInfo);
static int32_t vtbRefGetVgId(SDBVgInfo* dbInfo, const char* dbFName, const char* tbName, int32_t* pVgId,
                             SEpSet* pEpSet);

static int32_t sysTagsExtractFromTagData(const STag* pTag, const SSchema* pSrcSchema,
                                          char** ppTagData, uint32_t* pTagLen, bool* pResolved) {
  STagVal srcVal = {.cid = pSrcSchema->colId};
  bool    exists = tTagGet(pTag, &srcVal);
  if (!exists) return TSDB_CODE_SUCCESS;

  if (IS_VAR_DATA_TYPE(pSrcSchema->type)) {
    if (srcVal.pData != NULL && srcVal.nData > 0) {
      *ppTagData = taosMemoryMalloc(srcVal.nData);
      if (*ppTagData) {
        memcpy(*ppTagData, srcVal.pData, srcVal.nData);
        *pTagLen = srcVal.nData;
        *pResolved = true;
      }
    }
  } else {
    *ppTagData = taosMemoryMalloc(tDataTypes[pSrcSchema->type].bytes);
    if (*ppTagData) {
      memcpy(*ppTagData, &srcVal.i64, tDataTypes[pSrcSchema->type].bytes);
      *pTagLen = tDataTypes[pSrcSchema->type].bytes;
      *pResolved = true;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sysTagsFetchRemoteCfg(const SSysTableScanInfo* pInfo, int32_t acctId,
                                     const char* refDbName, const char* refTableName,
                                     uint64_t reqId, SExecTaskInfo* pTaskInfo, STableCfgRsp* pCfgRsp) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SDBVgInfo* pDbVgInfo = NULL;
  void*      clientRpc = pInfo->readHandle.pMsgCb->clientRpc;

  code = vtbRefGetDbVgInfo(clientRpc, (SEpSet*)&pInfo->epSet, acctId, refDbName, reqId, pTaskInfo, &pDbVgInfo);
  if (code != TSDB_CODE_SUCCESS || pDbVgInfo == NULL) {
    qDebug("sysTagsFetchRemoteCfg: failed to get db vg info for %s, code=%s", refDbName, tstrerror(code));
    return TSDB_CODE_SUCCESS;
  }

  char    dbFName[TSDB_DB_FNAME_LEN] = {0};
  SEpSet  vnodeEpSet = {0};
  int32_t vgId = 0;
  (void)snprintf(dbFName, sizeof(dbFName), "%d.%s", acctId, refDbName);

  code = vtbRefGetVgId(pDbVgInfo, dbFName, refTableName, &vgId, &vnodeEpSet);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("sysTagsFetchRemoteCfg: failed to get vgId for %s.%s, code=%s", refDbName, refTableName, tstrerror(code));
    freeVgInfo(pDbVgInfo);
    return TSDB_CODE_SUCCESS;
  }
  freeVgInfo(pDbVgInfo);

  STableCfgReq req = {0};
  req.header.vgId = vgId;
  tstrncpy(req.dbFName, dbFName, sizeof(req.dbFName));
  tstrncpy(req.tbName, refTableName, sizeof(req.tbName));

  int32_t contLen = tSerializeSTableCfgReq(NULL, 0, &req);
  char*   buf = rpcMallocCont(contLen);
  if (buf == NULL) return TSDB_CODE_SUCCESS;
  if (tSerializeSTableCfgReq(buf, contLen, &req) < 0) {
    rpcFreeCont(buf);
    return TSDB_CODE_SUCCESS;
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_VND_TABLE_CFG,
      .pCont = buf,
      .contLen = contLen,
      .info.ahandle = (void*)0x9527,
      .info.notFreeAhandle = 1,
  };

  SRpcMsg rpcRsp = {0};
  code = rpcSendRecv(clientRpc, &vnodeEpSet, &rpcMsg, &rpcRsp);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("sysTagsFetchRemoteCfg: rpcSendRecv failed for %s.%s vgId %d, code=%s",
           refDbName, refTableName, vgId, tstrerror(code));
    return TSDB_CODE_SUCCESS;
  }

  if (rpcRsp.code != TSDB_CODE_SUCCESS || rpcRsp.pCont == NULL || rpcRsp.contLen <= 0) {
    qDebug("sysTagsFetchRemoteCfg: table %s.%s vgId %d returned %s",
           refDbName, refTableName, vgId, tstrerror(rpcRsp.code));
    rpcFreeCont(rpcRsp.pCont);
    return TSDB_CODE_SUCCESS;
  }

  code = tDeserializeSTableCfgRsp(rpcRsp.pCont, rpcRsp.contLen, pCfgRsp);
  rpcFreeCont(rpcRsp.pCont);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("sysTagsFetchRemoteCfg: deserialize failed for %s.%s vgId %d", refDbName, refTableName, vgId);
    return TSDB_CODE_SUCCESS;
  }

  qDebug("sysTagsFetchRemoteCfg: got cfg for %s.%s from vgId %d, numOfTags=%d",
         refDbName, refTableName, vgId, pCfgRsp->numOfTags);
  return TSDB_CODE_SUCCESS;
}

static int32_t sysTagsResolveRefTagVal(const SSysTableScanInfo* pInfo, const SColRef* pRef,
                                       int8_t dstTagType, char** ppTagData, uint32_t* pTagLen,
                                       bool* pResolved, uint64_t reqId, SExecTaskInfo* pTaskInfo) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SStorageAPI* pAPI = pInfo->pAPI;

  *pResolved = false;
  *ppTagData = NULL;
  *pTagLen = 0;

  // Step 1: Try local vnode resolution
  SMetaReader srcTable = {0};
  pAPI->metaReaderFn.initReader(&srcTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  code = pAPI->metaReaderFn.getTableEntryByName(&srcTable, pRef->refTableName);
  pAPI->metaReaderFn.readerReleaseLock(&srcTable);

  if (code == TSDB_CODE_SUCCESS && srcTable.me.type == TSDB_CHILD_TABLE &&
      srcTable.me.ctbEntry.pTags != NULL) {
    SMetaReader srcSuper = {0};
    pAPI->metaReaderFn.initReader(&srcSuper, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByUid(&srcSuper, srcTable.me.ctbEntry.suid);
    pAPI->metaReaderFn.readerReleaseLock(&srcSuper);

    if (code == TSDB_CODE_SUCCESS) {
      const SSchema* pSrcSchema = NULL;
      for (int32_t j = 0; j < srcSuper.me.stbEntry.schemaTag.nCols; ++j) {
        if (strcmp(srcSuper.me.stbEntry.schemaTag.pSchema[j].name, pRef->refColName) == 0) {
          pSrcSchema = &srcSuper.me.stbEntry.schemaTag.pSchema[j];
          break;
        }
      }
      if (pSrcSchema != NULL) {
        code = sysTagsExtractFromTagData((STag*)srcTable.me.ctbEntry.pTags, pSrcSchema,
                                         ppTagData, pTagLen, pResolved);
      }
    }
    pAPI->metaReaderFn.clearReader(&srcSuper);
    pAPI->metaReaderFn.clearReader(&srcTable);
    if (*pResolved) return TSDB_CODE_SUCCESS;
    code = TSDB_CODE_SUCCESS;
  } else {
    pAPI->metaReaderFn.clearReader(&srcTable);
  }

  // Step 2: Source table not on local vnode - try remote fetch via RPC
  void* clientRpc = (pInfo->readHandle.pMsgCb) ? pInfo->readHandle.pMsgCb->clientRpc : NULL;
  if (clientRpc == NULL) {
    qDebug("sysTagsResolveRefTagVal: no clientRpc for %s.%s", pRef->refDbName, pRef->refTableName);
    return TSDB_CODE_SUCCESS;
  }

  STableCfgRsp cfgRsp = {0};
  code = sysTagsFetchRemoteCfg(pInfo, pInfo->accountId,
                               pRef->refDbName, pRef->refTableName, reqId, pTaskInfo, &cfgRsp);
  if (code != TSDB_CODE_SUCCESS || cfgRsp.pTags == NULL) {
    tFreeSTableCfgRsp(&cfgRsp);
    return TSDB_CODE_SUCCESS;
  }

  STag*    pRemoteTag = (STag*)cfgRsp.pTags;
  SSchema* pSchemas = cfgRsp.pSchemas;
  int32_t  numOfCols = cfgRsp.numOfColumns;
  int32_t  numOfTags = cfgRsp.numOfTags;

  for (int32_t t = 0; t < numOfTags; t++) {
    SSchema* pTagSchema = &pSchemas[numOfCols + t];
    if (strcmp(pTagSchema->name, pRef->refColName) == 0) {
      code = sysTagsExtractFromTagData(pRemoteTag, pTagSchema, ppTagData, pTagLen, pResolved);
      break;
    }
  }

  tFreeSTableCfgRsp(&cfgRsp);
  return code;
}

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock,
                                                uint64_t reqId, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char    stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(stableName, (*smrSuperTable).me.name);

  int32_t numOfRows = *pNumOfRows;

  bool     isVirtualChild = (smrChildTable->me.type == TSDB_VIRTUAL_CHILD_TABLE);
  SColRef* pTagRefs = isVirtualChild ? smrChildTable->me.colRef.pTagRef : NULL;
  int32_t  nTagRefs = isVirtualChild ? smrChildTable->me.colRef.nTagRefs : 0;

  int32_t numOfTags = (*smrSuperTable).me.stbEntry.schemaTag.nCols;
  for (int32_t i = 0; i < numOfTags; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tableName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // super table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, stableName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // tag name
    char tagName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tagName, (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tagName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // tag type
    int8_t tagType = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].type;

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    int32_t tagStrBufflen = 32;
    char    tagTypeStr[VARSTR_HEADER_SIZE + 32];
    int     tagTypeLen = snprintf(varDataVal(tagTypeStr), tagStrBufflen, "%s", tDataTypes[tagType].name);
    tagStrBufflen -= tagTypeLen;
    if (tagStrBufflen <= 0) {
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (tagType == TSDB_DATA_TYPE_NCHAR) {
      tagTypeLen += snprintf(
          varDataVal(tagTypeStr) + tagTypeLen, tagStrBufflen, "(%d)",
          (int32_t)(((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    } else if (IS_VAR_DATA_TYPE(tagType)) {
      if (IS_STR_DATA_BLOB(tagType)) {
        code = TSDB_CODE_BLOB_NOT_SUPPORT_TAG;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      tagTypeLen += snprintf(varDataVal(tagTypeStr) + tagTypeLen, tagStrBufflen, "(%d)",
                             (int32_t)((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE));
    }
    varDataSetLen(tagTypeStr, tagTypeLen);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)tagTypeStr, false);
    QUERY_CHECK_CODE(code, lino, _end);

    STagVal tagVal = {0};
    tagVal.cid = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].colId;
    char*    tagData = NULL;
    uint32_t tagLen = 0;
    bool     tagDataFromRemote = false;

    SColRef* pMatchedRef = NULL;
    if (pTagRefs != NULL) {
      col_id_t curColId = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].colId;
      for (int32_t r = 0; r < nTagRefs; ++r) {
        if (pTagRefs[r].id == curColId) {
          pMatchedRef = &pTagRefs[r];
          break;
        }
      }
    }

    if (pMatchedRef != NULL && pMatchedRef->hasRef) {
      bool resolved = false;
      code = sysTagsResolveRefTagVal(pInfo, pMatchedRef, tagType, &tagData, &tagLen, &resolved, reqId, pTaskInfo);
      if (code == TSDB_CODE_SUCCESS && resolved) {
        tagDataFromRemote = true;
      }
      code = TSDB_CODE_SUCCESS;
    } else {
      if (tagType == TSDB_DATA_TYPE_JSON) {
        tagData = (char*)smrChildTable->me.ctbEntry.pTags;
      } else {
        bool exist = tTagGet((STag*)smrChildTable->me.ctbEntry.pTags, &tagVal);
        if (exist) {
          if (tagType == TSDB_DATA_TYPE_GEOMETRY) {
            code = sysTableGetGeomText(tagVal.pData, tagVal.nData, &tagData, &tagLen);
            QUERY_CHECK_CODE(code, lino, _end);
          } else if (tagType == TSDB_DATA_TYPE_VARBINARY) {
            code = taosAscii2Hex(tagVal.pData, tagVal.nData, (void**)&tagData, &tagLen);
            if (code < 0) {
              qError("varbinary for systable failed since %s", tstrerror(code));
            }
          } else if (IS_VAR_DATA_TYPE(tagType)) {
            tagData = (char*)tagVal.pData;
            tagLen = tagVal.nData;
          } else {
            tagData = (char*)&tagVal.i64;
            tagLen = tDataTypes[tagType].bytes;
          }
        }
      }
    }

    char* tagVarChar = NULL;
    if (tagData != NULL) {
      if (IS_STR_DATA_BLOB(tagType)) {
        code = TSDB_CODE_BLOB_NOT_SUPPORT_TAG;
        goto _end;
      }

      if (tagType == TSDB_DATA_TYPE_JSON) {
        char* tagJson = NULL;
        parseTagDatatoJson(tagData, &tagJson, NULL);
        if (tagJson == NULL) {
          code = terrno;
          goto _end;
        }
        tagVarChar = taosMemoryMalloc(strlen(tagJson) + VARSTR_HEADER_SIZE);
        QUERY_CHECK_NULL(tagVarChar, code, lino, _end, terrno);
        memcpy(varDataVal(tagVarChar), tagJson, strlen(tagJson));
        varDataSetLen(tagVarChar, strlen(tagJson));
        taosMemoryFree(tagJson);
      } else {
        int32_t bufSize = IS_VAR_DATA_TYPE(tagType) ? (tagLen + VARSTR_HEADER_SIZE)
                                                    : (3 + DBL_MANT_DIG - DBL_MIN_EXP + VARSTR_HEADER_SIZE);
        tagVarChar = taosMemoryCalloc(1, bufSize + 1);
        QUERY_CHECK_NULL(tagVarChar, code, lino, _end, terrno);
        int32_t len = -1;
        if (tagLen > 0)
          convertTagDataToStr(varDataVal(tagVarChar), bufSize + 1 - VARSTR_HEADER_SIZE, tagType, tagData, tagLen, &len);
        else
          len = 0;
        varDataSetLen(tagVarChar, len);
      }
    }
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tagVarChar,
                         (tagData == NULL) || (tagType == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(tagData)));
    QUERY_CHECK_CODE(code, lino, _end);

    if (tagDataFromRemote) {
      taosMemoryFreeClear(tagData);
    } else {
      if (tagType == TSDB_DATA_TYPE_GEOMETRY || tagType == TSDB_DATA_TYPE_VARBINARY) taosMemoryFreeClear(tagData);
    }
    taosMemoryFree(tagVarChar);
    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t sysTableUserColsFillOneTableCols(const char* dbname, int32_t* pNumOfRows, const SSDataBlock* dataBlock,
                                                char* tName, SSchemaWrapper* schemaRow, SExtSchema* extSchemaRow,
                                                char* tableType, SColRefWrapper* colRef) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
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
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tableType, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col name
    char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(colName, schemaRow->pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, colName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col type
    int8_t colType = schemaRow->pSchema[i].type;
    SDataType colDataType = {0};
    if (IS_DECIMAL_TYPE(colType)) {
      schemaToRefDataType(&schemaRow->pSchema[i], extSchemaRow ? extSchemaRow[i].typeMod : 0, &colDataType);
    }
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    int32_t colStrBufflen = 32;
    char    colTypeStr[VARSTR_HEADER_SIZE + 32];
    int     colTypeLen = snprintf(varDataVal(colTypeStr), colStrBufflen, "%s", tDataTypes[colType].name);
    colStrBufflen -= colTypeLen;
    if (colStrBufflen <= 0) {
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (colType == TSDB_DATA_TYPE_VARCHAR) {
      colTypeLen += snprintf(varDataVal(colTypeStr) + colTypeLen, colStrBufflen, "(%d)",
                             (int32_t)(schemaRow->pSchema[i].bytes - VARSTR_HEADER_SIZE));
    } else if (colType == TSDB_DATA_TYPE_NCHAR) {
      colTypeLen += snprintf(varDataVal(colTypeStr) + colTypeLen, colStrBufflen, "(%d)",
                             (int32_t)((schemaRow->pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    } else if (IS_DECIMAL_TYPE(colType)) {
      if (colDataType.precision > 0) {
        colTypeLen += snprintf(varDataVal(colTypeStr) + colTypeLen, sizeof(colTypeStr) - colTypeLen - VARSTR_HEADER_SIZE,
                               "(%d,%d)", colDataType.precision, colDataType.scale);
      }
    }
    varDataSetLen(colTypeStr, colTypeLen);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)colTypeStr, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col length
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (const char*)&schemaRow->pSchema[i].bytes, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col precision
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 6);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (IS_DECIMAL_TYPE(colType) && colDataType.precision > 0) {
      int32_t precision = colDataType.precision;
      code = colDataSetVal(pColInfoData, numOfRows, (const char*)&precision, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      colDataSetNULL(pColInfoData, numOfRows);
    }

    // col scale
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (IS_DECIMAL_TYPE(colType) && colDataType.precision > 0) {
      int32_t scale = colDataType.scale;
      code = colDataSetVal(pColInfoData, numOfRows, (const char*)&scale, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      colDataSetNULL(pColInfoData, numOfRows);
    }

    // col nullable
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, numOfRows);

    // col data source
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (!colRef || !colRef->pColRef[i].hasRef) {
      colDataSetNULL(pColInfoData, numOfRows);
    } else {
      char refColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      char tmpColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN] = {0};

      TSlice refColNameBuf = {0};
      sliceInit(&refColNameBuf, tmpColName, sizeof(tmpColName));

      QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, colRef->pColRef[i].refDbName, strlen(colRef->pColRef[i].refDbName)),
                       lino, _end);
      QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, ".", 1), lino, _end);

      QUERY_CHECK_CODE(
          sliceAppend(&refColNameBuf, colRef->pColRef[i].refTableName, strlen(colRef->pColRef[i].refTableName)), lino,
          _end);

      QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, ".", 1), lino, _end);
      QUERY_CHECK_CODE(
          sliceAppend(&refColNameBuf, colRef->pColRef[i].refColName, strlen(colRef->pColRef[i].refColName)), lino,
          _end);

      STR_TO_VARSTR(refColName, tmpColName);

      code = colDataSetVal(pColInfoData, numOfRows, (char*)refColName, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // col id
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 10);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (const char*)&schemaRow->pSchema[i].colId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/*
 * Fill one `ins_vc_cols` row for one virtual-table column.
 *
 * @param dbname     database name in varstr format
 * @param pNumOfRows current output row counter
 * @param dataBlock  temporary system-table block
 * @param tName      virtual table name in varstr format
 * @param stName     virtual stable name in varstr format
 * @param schemaRow  schema wrapper of the virtual table column source
 * @param colRef     virtual table column reference wrapper
 * @param uid        virtual table uid
 * @param vgId       vnode id of the current virtual table
 * @param colIdx     target column index inside schemaRow/colRef
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t sysTableUserColsFillOneVirtualTableCol(const char* dbname, int32_t* pNumOfRows,
                                                      const SSDataBlock* dataBlock, char* tName, char* stName,
                                                      SSchemaWrapper* schemaRow, SColRefWrapper* colRef, tb_uid_t uid,
                                                      int32_t vgId, int32_t colIdx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (schemaRow == NULL) {
    qError("sysTableUserColsFillOneVirtualTableCol schemaRow is NULL");
    return TSDB_CODE_SUCCESS;
  }
  if (colIdx < 0 || colIdx >= schemaRow->nCols) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t          numOfRows = *pNumOfRows;
  SColumnInfoData* pColInfoData = NULL;

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, tName, false);
  QUERY_CHECK_CODE(code, lino, _end);

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, stName, false);
  QUERY_CHECK_CODE(code, lino, _end);

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
  QUERY_CHECK_CODE(code, lino, _end);

  char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(colName, schemaRow->pSchema[colIdx].name);
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, colName, false);
  QUERY_CHECK_CODE(code, lino, _end);

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&uid, false);
  QUERY_CHECK_CODE(code, lino, _end);

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  if (!colRef || !colRef->pColRef[colIdx].hasRef) {
    colDataSetNULL(pColInfoData, numOfRows);
  } else {
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&colRef->pColRef[colIdx].id, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 6);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  if (!colRef || !colRef->pColRef[colIdx].hasRef) {
    colDataSetNULL(pColInfoData, numOfRows);
  } else {
    char   refColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char   tmpColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN] = {0};
    TSlice refColNameBuf = {0};

    sliceInit(&refColNameBuf, tmpColName, sizeof(tmpColName));
    QUERY_CHECK_CODE(
        sliceAppend(&refColNameBuf, colRef->pColRef[colIdx].refDbName, strlen(colRef->pColRef[colIdx].refDbName)),
        lino, _end);
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, ".", 1), lino, _end);
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, colRef->pColRef[colIdx].refTableName,
                                 strlen(colRef->pColRef[colIdx].refTableName)),
                     lino, _end);
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, ".", 1), lino, _end);
    QUERY_CHECK_CODE(
        sliceAppend(&refColNameBuf, colRef->pColRef[colIdx].refColName, strlen(colRef->pColRef[colIdx].refColName)),
        lino, _end);
    STR_TO_VARSTR(refColName, tmpColName);

    code = colDataSetVal(pColInfoData, numOfRows, (char*)refColName, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 7);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
  QUERY_CHECK_CODE(code, lino, _end);

  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 8);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  int32_t refVersion = colRef ? colRef->version : 0;
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&refVersion, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col type: 0=column ref, 1=tag ref
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 9);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  int32_t colType = 0;
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&colType, false);
  QUERY_CHECK_CODE(code, lino, _end);

  *pNumOfRows = numOfRows + 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// Fill one tag-ref row for ins_vc_cols. Same schema as column-ref rows but with colType=1.
static int32_t sysTableUserColsFillOneVirtualTableTagRef(const char* dbname, int32_t* pNumOfRows,
                                                         const SSDataBlock* dataBlock, char* tName, char* stName,
                                                         SColRef* pTagRef, tb_uid_t uid, int32_t vgId,
                                                         int32_t refVersion) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pTagRef == NULL || !pTagRef->hasRef) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t          numOfRows = *pNumOfRows;
  SColumnInfoData* pColInfoData = NULL;

  // 0: tableName
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, tName, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 1: stableName
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, stName, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 2: dbName
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 3: colName (tag name)
  char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(colName, pTagRef->colName);
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, colName, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 4: uid
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&uid, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 5: colId
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&pTagRef->id, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 6: refColName (format "db.table.col")
  {
    char   refColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char   tmpColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN] = {0};
    TSlice refColNameBuf = {0};

    sliceInit(&refColNameBuf, tmpColName, sizeof(tmpColName));
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, pTagRef->refDbName, strlen(pTagRef->refDbName)), lino, _end);
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, ".", 1), lino, _end);
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, pTagRef->refTableName, strlen(pTagRef->refTableName)), lino, _end);
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, ".", 1), lino, _end);
    QUERY_CHECK_CODE(sliceAppend(&refColNameBuf, pTagRef->refColName, strlen(pTagRef->refColName)), lino, _end);
    STR_TO_VARSTR(refColName, tmpColName);

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 6);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)refColName, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // 7: vgId
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 7);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 8: refVersion
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 8);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&refVersion, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // 9: colType = 1 (tag ref)
  pColInfoData = taosArrayGet(dataBlock->pDataBlock, 9);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  int32_t colType = 1;
  code = colDataSetVal(pColInfoData, numOfRows, (char*)&colType, false);
  QUERY_CHECK_CODE(code, lino, _end);

  *pNumOfRows = numOfRows + 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t sysTableUserColsFillOneVirtualTableCols(const SSysTableScanInfo* pInfo, const char* dbname,
                                                       int32_t* pNumOfRows, const SSDataBlock* dataBlock, char* tName,
                                                       char* stName, SSchemaWrapper* schemaRow, char* tableType,
                                                       SColRefWrapper* colRef, tb_uid_t uid, int32_t vgId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  TAOS_UNUSED(pInfo);
  TAOS_UNUSED(tableType);

  if (schemaRow == NULL) {
    qError("sysTableUserColsFillOneTableCols schemaRow is NULL");
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < schemaRow->nCols; ++i) {
    code = sysTableUserColsFillOneVirtualTableCol(dbname, pNumOfRows, dataBlock, tName, stName, schemaRow, colRef,
                                                  uid, vgId, i);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // Emit tag-ref rows (colType=1) for DynQueryCtrl to resolve referenced tags.
  if (colRef != NULL && colRef->pTagRef != NULL) {
    int32_t     refVersion = colRef->version;
    for (int32_t i = 0; i < colRef->nTagRefs; ++i) {
      if (colRef->pTagRef[i].hasRef) {
        code = sysTableUserColsFillOneVirtualTableTagRef(dbname, pNumOfRows, dataBlock, tName, stName,
                                                         &colRef->pTagRef[i], uid, vgId, refVersion);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/*
 * Release layered vc-cols request state cached on one systable scan operator.
 *
 * @param pInfo systable scan runtime info
 *
 * @return none
 */
static void destroySysTableScanVtbRefReqs(SSysTableScanInfo* pInfo) {
  if (pInfo->pVtbRefReqs != NULL) {
    taosArrayDestroy(pInfo->pVtbRefReqs);
    pInfo->pVtbRefReqs = NULL;
  }
  pInfo->vtbRefReqIdx = 0;
}

/*
 * Find the schema index for one requested virtual-table column.
 *
 * @param schemaRow schema wrapper to search
 * @param colName   requested column name
 * @param pColIdx   output schema index
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t findVirtualTableColIndex(SSchemaWrapper* schemaRow, const char* colName, int32_t* pColIdx) {
  for (int32_t i = 0; i < schemaRow->nCols; ++i) {
    if (strcmp(schemaRow->pSchema[i].name, colName) == 0) {
      *pColIdx = i;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_PAR_INVALID_REF_COLUMN;
}

/*
 * Fill one requested virtual-table column row for layered vc-cols lookup.
 *
 * @param pOperator dyn-layer systable scan operator
 * @param pReq      one requested table/column lookup
 * @param dataBlock temporary result block
 * @param pNumOfRows current output row counter
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t sysTableScanFillRequestedVirtualTableCol(SOperatorInfo* pOperator, const SSysTableScanVtbRefReq* pReq,
                                                        const SSDataBlock* dataBlock, int32_t* pNumOfRows) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SMetaReader        smrTable = {0};
  SMetaReader        smrSuperTable = {0};
  bool               smrTableInited = false;
  bool               smrSuperTableInited = false;
  SSchemaWrapper*    schemaRow = NULL;
  SColRefWrapper*    colRef = NULL;
  int32_t            colIdx = -1;
  char               dbname[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  char               tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  char               stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

  pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  smrTableInited = true;
  code = pAPI->metaReaderFn.getTableEntryByName(&smrTable, pReq->tbName);
  QUERY_CHECK_CODE(code, lino, _return);

  if (smrTable.me.type != TSDB_VIRTUAL_NORMAL_TABLE && smrTable.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
    goto _return;
  }

  STR_TO_VARSTR(dbname, pReq->dbName);
  STR_TO_VARSTR(tableName, pReq->tbName);

  if (smrTable.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
    schemaRow = &smrTable.me.ntbEntry.schemaRow;
    colRef = &smrTable.me.colRef;
    STR_TO_VARSTR(stableName, smrTable.me.name);
  } else {
    int64_t suid = smrTable.me.ctbEntry.suid;

    colRef = &smrTable.me.colRef;
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    smrSuperTableInited = true;
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
    QUERY_CHECK_CODE(code, lino, _return);

    schemaRow = &smrSuperTable.me.stbEntry.schemaRow;
    STR_TO_VARSTR(stableName, smrSuperTable.me.name);
  }

  QUERY_CHECK_NULL(schemaRow, code, lino, _return, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  QUERY_CHECK_NULL(schemaRow->pSchema, code, lino, _return, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  QUERY_CHECK_NULL(colRef, code, lino, _return, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  code = findVirtualTableColIndex(schemaRow, pReq->colName, &colIdx);
  if (code == TSDB_CODE_SUCCESS && colIdx >= 0) {
    code = sysTableUserColsFillOneVirtualTableCol(dbname, pNumOfRows, dataBlock, tableName, stableName, schemaRow,
                                                  colRef, smrTable.me.uid, pReq->vgId, colIdx);
    QUERY_CHECK_CODE(code, lino, _return);
  } else {
    // Column not found in column schema - check tag refs
    code = TSDB_CODE_SUCCESS;
    if (colRef != NULL && colRef->pTagRef != NULL) {
      for (int32_t i = 0; i < colRef->nTagRefs; ++i) {
        if (colRef->pTagRef[i].hasRef && strcmp(colRef->pTagRef[i].colName, pReq->colName) == 0) {
          code = sysTableUserColsFillOneVirtualTableTagRef(dbname, pNumOfRows, dataBlock, tableName, stableName,
                                                           &colRef->pTagRef[i], smrTable.me.uid, pReq->vgId,
                                                           colRef->version);
          QUERY_CHECK_CODE(code, lino, _return);
          break;
        }
      }
    }
  }

_return:
  if (smrSuperTableInited) {
    pAPI->metaReaderFn.clearReader(&smrSuperTable);
  }
  if (smrTableInited) {
    pAPI->metaReaderFn.clearReader(&smrTable);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, tb:%s, col:%s, vgId:%d", __func__, lino, tstrerror(code), pReq->tbName,
           pReq->colName, pReq->vgId);
  }
  return code;
}

/*
 * Apply layered vc-cols get-param to the current systable scan operator.
 *
 * @param pOperator systable scan operator
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t sysTableScanApplyVtbRefReqParam(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SSysTableScanInfo*         pInfo = pOperator->info;
  SSysTableScanOperatorParam* pParam = NULL;
  const char*                name = tNameGetTableName(&pInfo->name);

  if (pOperator->pOperatorGetParam == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  QUERY_CHECK_CONDITION(pOperator->pOperatorGetParam->opType == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, code, lino,
                        _return, TSDB_CODE_INVALID_PARA)
  QUERY_CHECK_CONDITION(strncasecmp(name, TSDB_INS_TABLE_VC_COLS, TSDB_TABLE_FNAME_LEN) == 0, code, lino, _return,
                        TSDB_CODE_INVALID_PARA)

  if (pOperator->status == OP_EXEC_DONE && pOperator->fpSet.resetStateFn != NULL) {
    code = pOperator->fpSet.resetStateFn(pOperator);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  destroySysTableScanVtbRefReqs(pInfo);
  pParam = (SSysTableScanOperatorParam*)pOperator->pOperatorGetParam->value;
  if (pParam != NULL && pParam->pVtbRefReqs != NULL) {
    pInfo->pVtbRefReqs = taosArrayDup(pParam->pVtbRefReqs, NULL);
    QUERY_CHECK_NULL(pInfo->pVtbRefReqs, code, lino, _return, terrno)
  }

_return:
  freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
  pOperator->pOperatorGetParam = NULL;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/*
 * Scan `ins_vc_cols` with layered request params instead of full vnode cursor iteration.
 *
 * @param pOperator systable scan operator
 *
 * @return result block on success, otherwise NULL
 */
static SSDataBlock* sysTableScanUserVcColsByReqs(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  SSDataBlock*       pDataBlock = NULL;
  int32_t            numOfRows = 0;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  pDataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_VC_COLS);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);
  code = blockDataEnsureCapacity(pDataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  while (pInfo->vtbRefReqIdx < taosArrayGetSize(pInfo->pVtbRefReqs)) {
    SSysTableScanVtbRefReq* pReq = taosArrayGet(pInfo->pVtbRefReqs, pInfo->vtbRefReqIdx++);
    QUERY_CHECK_NULL(pReq, code, lino, _end, terrno);

    qDebug("vcColsByReqs: idx=%d, tbName=%s, colName=%s, vgId=%d, taskVgId=%d",
           pInfo->vtbRefReqIdx - 1, pReq->tbName, pReq->colName, pReq->vgId, pTaskInfo->id.vgId);

    if (pReq->vgId != pTaskInfo->id.vgId) {
      continue;
    }

    code = sysTableScanFillRequestedVirtualTableCol(pOperator, pReq, pDataBlock, &numOfRows);
    QUERY_CHECK_CODE(code, lino, _end);

    if (numOfRows >= pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, NULL, pTaskInfo);
      numOfRows = 0;
      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    }
  }

  if (numOfRows > 0) {
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, NULL, pTaskInfo);
    numOfRows = 0;
  }

  if (pInfo->vtbRefReqIdx >= taosArrayGetSize(pInfo->pVtbRefReqs)) {
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  blockDataDestroy(pDataBlock);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

// ===================== Virtual Table Reference Validation =====================

// Context for async RPC used during virtual table reference validation.
// Shared between caller and callback via atomic refCount to prevent UAF on timeout.
typedef struct SVtbRefValidateCtx {
  tsem_t            ready;
  int32_t           rspCode;
  void*             pRsp;
  int32_t           rspLen;
  // refCount starts at 1 (waiter only). It is bumped to 2 immediately before
  // asyncSendMsgToServer (waiter + callback) so the callback can never access a
  // freed struct even if the waiter exits early. If the send fails,
  // asyncSendMsgToServer does NOT invoke fp, so we roll back to 1 and the
  // waiter's decRef at _return frees the struct.
  volatile int32_t  refCount;
} SVtbRefValidateCtx;

static void vtbRefValidateCtxDecRef(SVtbRefValidateCtx* pCtx) {
  if (atomic_sub_fetch_32(&pCtx->refCount, 1) == 0) {
    taosMemoryFree(pCtx->pRsp);
    TAOS_UNUSED(tsem_destroy(&pCtx->ready));
    taosMemoryFree(pCtx);
  }
}

// ===================== Table Schema Cache for Validation =====================

// Cached schema for a single table (used to avoid repeated meta reads)
typedef struct SVtbRefSchemaCache {
  SSchemaWrapper schemaRow;  // Data column schema
  SSchemaWrapper schemaTag;  // Tag schema (optional)
  bool           hasTagSchema;
  bool           ownsSchema;     // true if schema is deep-copied (remote tables), false if shallow (local tables)
  SHashObj*      pColNameIndex;  // Column name hash index for O(1) lookup
} SVtbRefSchemaCache;

// Cache entry for a single table
typedef struct SVtbRefTableCacheEntry {
  int32_t             errCode;       // Table validation result (TSDB_CODE_SUCCESS or error)
  int8_t              tableType;     // ETableType
  SVtbRefSchemaCache* pSchemaCache;  // Schema cache (NULL if errCode != SUCCESS)
  SColRefWrapper      colRef;        // Ref metadata for virtual tables
} SVtbRefTableCacheEntry;

static bool vtbRefIsVirtualTableType(int8_t tableType) {
  return tableType == TSDB_VIRTUAL_NORMAL_TABLE || tableType == TSDB_VIRTUAL_CHILD_TABLE;
}

static int32_t vtbRefCopyColRefs(const SColRef* pSrc, int32_t numOfRefs, SColRef** ppDst) {
  if (ppDst == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppDst = NULL;
  if (pSrc == NULL || numOfRefs <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SColRef* pDst = taosMemoryMalloc(numOfRefs * sizeof(SColRef));
  if (pDst == NULL) {
    return terrno;
  }

  TAOS_MEMCPY(pDst, pSrc, numOfRefs * sizeof(SColRef));
  *ppDst = pDst;
  return TSDB_CODE_SUCCESS;
}

static void vtbRefFreeColRefWrapper(SColRefWrapper* pColRef) {
  if (pColRef == NULL) {
    return;
  }

  taosMemoryFreeClear(pColRef->pColRef);
  taosMemoryFreeClear(pColRef->pTagRef);
  pColRef->nCols = 0;
  pColRef->nTagRefs = 0;
  pColRef->version = 0;
}

static int32_t vtbRefCopyColRefWrapper(const SColRefWrapper* pSrc, SColRefWrapper* pDst) {
  if (pDst == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  TAOS_MEMSET(pDst, 0, sizeof(*pDst));
  if (pSrc == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = vtbRefCopyColRefs(pSrc->pColRef, pSrc->nCols, &pDst->pColRef);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = vtbRefCopyColRefs(pSrc->pTagRef, pSrc->nTagRefs, &pDst->pTagRef);
  if (code != TSDB_CODE_SUCCESS) {
    vtbRefFreeColRefWrapper(pDst);
    return code;
  }

  pDst->nCols = pSrc->nCols;
  pDst->nTagRefs = pSrc->nTagRefs;
  pDst->version = pSrc->version;
  return TSDB_CODE_SUCCESS;
}

static int32_t vtbRefCopyColRefWrapperFromMetaRsp(const STableMetaRsp* pMetaRsp, SColRefWrapper* pDst) {
  if (pDst == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  TAOS_MEMSET(pDst, 0, sizeof(*pDst));
  if (pMetaRsp == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = vtbRefCopyColRefs(pMetaRsp->pColRefs, pMetaRsp->numOfColRefs, &pDst->pColRef);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = vtbRefCopyColRefs(pMetaRsp->pTagRefs, pMetaRsp->numOfTagRefs, &pDst->pTagRef);
  if (code != TSDB_CODE_SUCCESS) {
    vtbRefFreeColRefWrapper(pDst);
    return code;
  }

  pDst->nCols = pMetaRsp->numOfColRefs;
  pDst->nTagRefs = pMetaRsp->numOfTagRefs;
  pDst->version = pMetaRsp->rversion;
  return TSDB_CODE_SUCCESS;
}

static int32_t vtbRefBuildColNameIndex(SSchema* pSchemas, int32_t numOfCols, int32_t numOfTags, SHashObj** ppIndex) {
  int32_t code = 0;
  int32_t totalCols = numOfCols + numOfTags;
  SHashObj* pIndex = taosHashInit(totalCols > 0 ? totalCols : 8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                  true, HASH_NO_LOCK);
  if (pIndex == NULL) {
    return terrno;
  }
  for (int32_t i = 0; i < totalCols; ++i) {
    code = taosHashPut(pIndex, pSchemas[i].name, strlen(pSchemas[i].name), &i, sizeof(int32_t));
    if (code != TSDB_CODE_SUCCESS) {
      taosHashCleanup(pIndex);
      return code;
    }   
  }
  *ppIndex = pIndex;
  return TSDB_CODE_SUCCESS;
}

static SVtbRefSchemaCache* vtbRefCreateSchemaCache(ETableType type, SMetaReader* pReader) {
  SVtbRefSchemaCache* pCache = taosMemoryCalloc(1, sizeof(SVtbRefSchemaCache));
  if (pCache == NULL) {
    return NULL;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t numOfCols = 0;
  int32_t numOfTags = 0;
  SSchema* pSrcSchemas = NULL;
  SSchema* pSrcTagSchemas = NULL;

  if (type == TSDB_NORMAL_TABLE || type == TSDB_VIRTUAL_NORMAL_TABLE) {
    numOfCols = pReader->me.ntbEntry.schemaRow.nCols;
    pSrcSchemas = pReader->me.ntbEntry.schemaRow.pSchema;
    pCache->hasTagSchema = false;
  } else if (type == TSDB_CHILD_TABLE || type == TSDB_SUPER_TABLE || type == TSDB_VIRTUAL_CHILD_TABLE) {
    numOfCols = pReader->me.stbEntry.schemaRow.nCols;
    numOfTags = pReader->me.stbEntry.schemaTag.nCols;
    pSrcSchemas = pReader->me.stbEntry.schemaRow.pSchema;
    pSrcTagSchemas = pReader->me.stbEntry.schemaTag.pSchema;
    pCache->hasTagSchema = true;
  } else {
    taosMemoryFree(pCache);
    return NULL;
  }

  // Allocate and copy schemas (deep copy to own the memory)
  int32_t totalCols = numOfCols + numOfTags;
  SSchema* pAllSchemas = taosMemoryMalloc(totalCols * sizeof(SSchema));
  if (pAllSchemas == NULL) {
    taosMemoryFree(pCache);
    return NULL;
  }

  // Copy column schemas
  if (pSrcSchemas != NULL && numOfCols > 0) {
    memcpy(pAllSchemas, pSrcSchemas, numOfCols * sizeof(SSchema));
  }

  // Copy tag schemas
  if (pSrcTagSchemas != NULL && numOfTags > 0) {
    memcpy(pAllSchemas + numOfCols, pSrcTagSchemas, numOfTags * sizeof(SSchema));
  }

  pCache->schemaRow.nCols = numOfCols;
  pCache->schemaRow.pSchema = pAllSchemas;
  pCache->schemaTag.nCols = numOfTags;
  pCache->schemaTag.pSchema = (numOfTags > 0) ? pAllSchemas + numOfCols : NULL;
  pCache->ownsSchema = true;

  code = vtbRefBuildColNameIndex(pAllSchemas, numOfCols, numOfTags, &pCache->pColNameIndex);

  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pAllSchemas);
    taosMemoryFree(pCache);
    return NULL;
  }

  return pCache;
}

static void vtbRefFreeSchemaCache(SVtbRefSchemaCache* pCache) {
  if (pCache == NULL) {
    return;
  }
  if (pCache->ownsSchema) {
    taosMemoryFree(pCache->schemaRow.pSchema);
    // schemaTag.pSchema points into the same memory block as schemaRow.pSchema
    // Only free once to avoid double-free
    pCache->schemaTag.pSchema = NULL;
  }
  taosHashCleanup(pCache->pColNameIndex);
  taosMemoryFree(pCache);
}

// Free cache entry contents only (for hash table freeFp callback).
// The struct itself is part of the hash node and freed by the hash table.
static void vtbRefCleanupTableCacheEntryContents(void* p) {
  SVtbRefTableCacheEntry* pEntry = (SVtbRefTableCacheEntry*)p;
  if (pEntry != NULL) {
    vtbRefFreeSchemaCache(pEntry->pSchemaCache);
    vtbRefFreeColRefWrapper(&pEntry->colRef);
    pEntry->pSchemaCache = NULL;
  }
}

// Free a standalone (non-hash-owned) cache entry: contents + the struct itself.
static void vtbRefFreeTableCacheEntry(void* p) {
  SVtbRefTableCacheEntry* pEntry = (SVtbRefTableCacheEntry*)p;
  if (pEntry != NULL) {
    vtbRefFreeSchemaCache(pEntry->pSchemaCache);
    vtbRefFreeColRefWrapper(&pEntry->colRef);
    taosMemoryFree(pEntry);
  }
}

static bool vtbRefCheckColumnInCache(const SVtbRefSchemaCache* pCache, const char* colName) {
  if (pCache == NULL || colName == NULL || pCache->pColNameIndex == NULL) {
    return false;
  }
  return taosHashGet(pCache->pColNameIndex, colName, strlen(colName)) != NULL;
}

static const SColRef* vtbRefFindColumnRefInEntry(const SVtbRefTableCacheEntry* pEntry, const char* colName) {
  if (pEntry == NULL || colName == NULL || !vtbRefIsVirtualTableType(pEntry->tableType) || pEntry->colRef.pColRef == NULL ||
      pEntry->colRef.nCols <= 0) {
    return NULL;
  }

  if (pEntry->pSchemaCache == NULL || pEntry->pSchemaCache->pColNameIndex == NULL) {
    return NULL;
  }

  int32_t* pIndex = taosHashGet(pEntry->pSchemaCache->pColNameIndex, colName, strlen(colName));
  if (pIndex == NULL || *pIndex < 0 || *pIndex >= pEntry->colRef.nCols) {
    return NULL;
  }

  return &pEntry->colRef.pColRef[*pIndex];
}

static int32_t vtbRefCreateSchemaCacheFromMetaRsp(STableMetaRsp* pMetaRsp, SVtbRefSchemaCache** ppCache) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pMetaRsp == NULL || pMetaRsp->pSchemas == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  SVtbRefSchemaCache* pCache = taosMemoryCalloc(1, sizeof(SVtbRefSchemaCache));
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  int32_t numOfCols = pMetaRsp->numOfColumns;
  int32_t numOfTags = pMetaRsp->numOfTags;
  int32_t totalCols = numOfCols + numOfTags;

  SSchema* pAllSchemas = taosMemoryMalloc(totalCols * sizeof(SSchema));
  if (pAllSchemas == NULL) {
    taosMemoryFree(pCache);
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }
  memcpy(pAllSchemas, pMetaRsp->pSchemas, totalCols * sizeof(SSchema));

  pCache->schemaRow.nCols = numOfCols;
  pCache->schemaRow.pSchema = pAllSchemas;
  pCache->schemaTag.nCols = numOfTags;
  pCache->schemaTag.pSchema = (numOfTags > 0) ? pAllSchemas + numOfCols : NULL;
  pCache->hasTagSchema = (numOfTags > 0);
  pCache->ownsSchema = true;
  code = vtbRefBuildColNameIndex(pAllSchemas, numOfCols, numOfTags, &pCache->pColNameIndex);

  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pAllSchemas);
    taosMemoryFree(pCache);
    return code;
  }

  *ppCache = pCache;
  return TSDB_CODE_SUCCESS;
}

static void vtbRefBuildRemoteCacheKey(char* buf, int32_t size, const char* dbName, const char* tableName) {
  snprintf(buf, size, "%s.%s", dbName, tableName);
}

static SVtbRefTableCacheEntry* vtbRefGetRemoteCacheEntry(SHashObj* pTableCache, const char* dbName,
                                                         const char* tableName) {
  char key[TSDB_DB_FNAME_LEN + TSDB_TABLE_NAME_LEN + 2];
  vtbRefBuildRemoteCacheKey(key, sizeof(key), dbName, tableName);
  return (SVtbRefTableCacheEntry*)taosHashGet(pTableCache, key, strlen(key));
}

static int32_t vtbRefPutRemoteCacheEntry(SHashObj* pTableCache, const char* dbName, const char* tableName,
                                         SVtbRefTableCacheEntry* pEntry) {
  char key[TSDB_DB_FNAME_LEN + TSDB_TABLE_NAME_LEN + 2];
  vtbRefBuildRemoteCacheKey(key, sizeof(key), dbName, tableName);
  int32_t code = taosHashPut(pTableCache, key, strlen(key), pEntry, sizeof(SVtbRefTableCacheEntry));

  return (code == TSDB_CODE_DUP_KEY) ? TSDB_CODE_SUCCESS : code;
}

// Callback for async RPC responses during validation
static int32_t vtbRefValidateCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SVtbRefValidateCtx* pCtx = (SVtbRefValidateCtx*)param;
  if (TSDB_CODE_SUCCESS == code) {
    pCtx->pRsp = pMsg->pData;
    pCtx->rspLen = (int32_t)pMsg->len;
    pCtx->rspCode = TSDB_CODE_SUCCESS;
  } else {
    pCtx->rspCode = rpcCvtErrCode(code);
    pCtx->pRsp = NULL;
    pCtx->rspLen = 0;
    taosMemoryFree(pMsg->pData);
  }
  taosMemoryFree(pMsg->pEpSet);
  int32_t res = tsem_post(&pCtx->ready);
  if (res != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(res));
  }
  // Release the callback's reference; if the waiter already exited (timeout /
  // error), this may free pCtx — safe because we no longer access it.
  vtbRefValidateCtxDecRef(pCtx);
  return TSDB_CODE_SUCCESS;
}

// Fetch DB vgroup info from MNode via RPC
static int32_t vtbRefGetDbVgInfo(void* clientRpc, SEpSet* pEpSet, int32_t acctId, const char* dbName, uint64_t reqId,
                                 SExecTaskInfo* pTaskInfo, SDBVgInfo** ppVgInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SVtbRefValidateCtx* pCtx = NULL;
  SUseDbReq          usedbReq = {0};
  SUseDbRsp          usedbRsp = {0};
  SUseDbOutput       output = {0};
  char*              buf = NULL;

  // Heap-allocate context; refCount starts at 1 (waiter only). It is bumped to
  // 2 (waiter + callback) immediately before asyncSendMsgToServer so that the
  // callback can never access a freed struct even if the waiter exits early.
  // If the send fails asyncSendMsgToServer does NOT invoke fp, so we roll back
  // the extra reference synchronously.
  pCtx = taosMemoryCalloc(1, sizeof(SVtbRefValidateCtx));
  QUERY_CHECK_NULL(pCtx, code, lino, _return, terrno);
  atomic_store_32(&pCtx->refCount, 1);

  code = tsem_init(&pCtx->ready, 0, 0);
  QUERY_CHECK_CODE(code, lino, _return);

  // Build full db name: "acctId.dbName"
  (void)snprintf(usedbReq.db, sizeof(usedbReq.db), "%d.%s", acctId, dbName);

  int32_t contLen = tSerializeSUseDbReq(NULL, 0, &usedbReq);
  buf = taosMemoryCalloc(1, contLen);
  QUERY_CHECK_NULL(buf, code, lino, _return, terrno);

  int32_t tempRes = tSerializeSUseDbReq(buf, contLen, &usedbReq);
  if (tempRes < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  QUERY_CHECK_NULL(pMsgSendInfo, code, lino, _return, terrno);

  pMsgSendInfo->param = pCtx;
  pMsgSendInfo->msgInfo.pData = buf;
  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgType = TDMT_MND_GET_DB_INFO;
  pMsgSendInfo->fp = vtbRefValidateCallback;
  pMsgSendInfo->requestId = reqId;

  // Bump refCount for the callback before sending; if send fails we roll back.
  int32_t newCount = atomic_add_fetch_32(&pCtx->refCount, 1);  // now 2: waiter + callback
  TAOS_UNUSED(newCount);
  code = asyncSendMsgToServer(clientRpc, pEpSet, NULL, pMsgSendInfo);
  if (code != TSDB_CODE_SUCCESS) {
    // asyncSendMsgToServer freed pMsgSendInfo without calling fp; the callback
    // will never fire, so roll back the reference we just added.
    newCount = atomic_sub_fetch_32(&pCtx->refCount, 1);  // back to 1: waiter only
    TAOS_UNUSED(newCount);
    buf = NULL;
    QUERY_CHECK_CODE(code, lino, _return);
  }
  buf = NULL;  // ownership transferred to pMsgSendInfo

  code = qSemWait((qTaskInfo_t)pTaskInfo, &pCtx->ready);
  QUERY_CHECK_CODE(code, lino, _return);

  if (pCtx->rspCode != TSDB_CODE_SUCCESS) {
    code = pCtx->rspCode;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  // Parse the response
  SUseDbRsp* pRsp = taosMemoryMalloc(sizeof(SUseDbRsp));
  QUERY_CHECK_NULL(pRsp, code, lino, _return, terrno);

  code = tDeserializeSUseDbRsp(pCtx->pRsp, pCtx->rspLen, pRsp);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pRsp);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  code = queryBuildUseDbOutput(&output, pRsp);
  tFreeSUsedbRsp(pRsp);
  taosMemoryFree(pRsp);
  QUERY_CHECK_CODE(code, lino, _return);

  *ppVgInfo = output.dbVgroup;
  output.dbVgroup = NULL;  // ownership transferred

_return:
  // Release the waiter's reference; if the callback has already fired this
  // may free pCtx, otherwise the callback will free it when it completes.
  if (pCtx) vtbRefValidateCtxDecRef(pCtx);
  taosMemoryFree(buf);
  if (output.dbVgroup) {
    freeVgInfo(output.dbVgroup);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int vtbRefVgInfoComp(const void* lp, const void* rp) {
  SVgroupInfo* pLeft = (SVgroupInfo*)lp;
  SVgroupInfo* pRight = (SVgroupInfo*)rp;
  if (pLeft->hashBegin < pRight->hashBegin) return -1;
  if (pLeft->hashBegin > pRight->hashBegin) return 1;
  return 0;
}

static int vtbRefHashValueComp(const void* lp, const void* rp) {
  uint32_t*    key = (uint32_t*)lp;
  SVgroupInfo* pVg = (SVgroupInfo*)rp;
  if (*key < pVg->hashBegin) return -1;
  if (*key > pVg->hashEnd) return 1;
  return 0;
}

// Calculate vgId for a table within a database's vgroup info
static int32_t vtbRefGetVgId(SDBVgInfo* dbInfo, const char* dbFName, const char* tbName, int32_t* pVgId,
                             SEpSet* pEpSet) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // Build vgArray if not already built
  if (dbInfo->vgHash && NULL == dbInfo->vgArray) {
    int32_t vgSize = taosHashGetSize(dbInfo->vgHash);
    dbInfo->vgArray = taosArrayInit(vgSize, sizeof(SVgroupInfo));
    QUERY_CHECK_NULL(dbInfo->vgArray, code, lino, _return, terrno);

    void* pIter = taosHashIterate(dbInfo->vgHash, NULL);
    while (pIter) {
      if (NULL == taosArrayPush(dbInfo->vgArray, pIter)) {
        taosHashCancelIterate(dbInfo->vgHash, pIter);
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _return);
      }
      pIter = taosHashIterate(dbInfo->vgHash, pIter);
    }

    taosArraySort(dbInfo->vgArray, vtbRefVgInfoComp);
  }

  int32_t vgNum = (int32_t)taosArrayGetSize(dbInfo->vgArray);
  if (vgNum <= 0) {
    qError("db vgroup cache invalid, db:%s, vgroup number:%d", dbFName, vgNum);
    code = TSDB_CODE_TSC_DB_NOT_SELECTED;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  // Calculate hash value for the table
  char tbFullName[TSDB_TABLE_FNAME_LEN];
  (void)snprintf(tbFullName, sizeof(tbFullName), "%s.%s", dbFName, tbName);
  uint32_t hashValue = taosGetTbHashVal(tbFullName, (int32_t)strlen(tbFullName), dbInfo->hashMethod, dbInfo->hashPrefix,
                                        dbInfo->hashSuffix);

  // Binary search for the vgroup
  SVgroupInfo* vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, vtbRefHashValueComp, TD_EQ);

  if (NULL == vgInfo) {
    qError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, dbFName, vgNum);
    code = TSDB_CODE_CTG_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  *pVgId = vgInfo->vgId;
  if (pEpSet) {
    *pEpSet = vgInfo->epSet;
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// Fetch table schema from a specific vnode via RPC
static int32_t vtbRefFetchTableSchema(void* clientRpc, SEpSet* pVnodeEpSet, int32_t acctId, const char* dbName,
                                      const char* tbName, int32_t vgId, uint64_t reqId,
                                      SExecTaskInfo* pTaskInfo, STableMetaRsp* pMetaRsp) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SVtbRefValidateCtx* pCtx = NULL;
  char*               buf = NULL;

  // Heap-allocate context; refCount starts at 1 (waiter only). It is bumped to
  // 2 (waiter + callback) immediately before asyncSendMsgToServer. If the send
  // fails asyncSendMsgToServer does NOT invoke fp, so we roll back the extra
  // reference synchronously.
  pCtx = taosMemoryCalloc(1, sizeof(SVtbRefValidateCtx));
  QUERY_CHECK_NULL(pCtx, code, lino, _return, terrno);
  atomic_store_32(&pCtx->refCount, 1);

  code = tsem_init(&pCtx->ready, 0, 0);
  QUERY_CHECK_CODE(code, lino, _return);

  // Build the table info request
  STableInfoReq infoReq = {0};
  infoReq.header.vgId = vgId;
  infoReq.option = REQ_OPT_TBNAME;
  (void)snprintf(infoReq.dbFName, sizeof(infoReq.dbFName), "%d.%s", acctId, dbName);
  tstrncpy(infoReq.tbName, tbName, TSDB_TABLE_NAME_LEN);

  int32_t contLen = tSerializeSTableInfoReq(NULL, 0, &infoReq);
  buf = taosMemoryCalloc(1, contLen);
  QUERY_CHECK_NULL(buf, code, lino, _return, terrno);

  int32_t tempRes = tSerializeSTableInfoReq(buf, contLen, &infoReq);
  if (tempRes < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  QUERY_CHECK_NULL(pMsgSendInfo, code, lino, _return, terrno);

  pMsgSendInfo->param = pCtx;
  pMsgSendInfo->msgInfo.pData = buf;
  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgType = TDMT_VND_TABLE_META;
  pMsgSendInfo->fp = vtbRefValidateCallback;
  pMsgSendInfo->requestId = reqId;

  // Bump refCount for the callback before sending; if send fails we roll back.
  int32_t newCount = atomic_add_fetch_32(&pCtx->refCount, 1);  // now 2: waiter + callback
  TAOS_UNUSED(newCount);
  code = asyncSendMsgToServer(clientRpc, pVnodeEpSet, NULL, pMsgSendInfo);
  if (code != TSDB_CODE_SUCCESS) {
    // asyncSendMsgToServer freed pMsgSendInfo (via destroySendMsgInfo) without
    // calling fp; the callback will never fire, so roll back the extra ref.
    newCount = atomic_sub_fetch_32(&pCtx->refCount, 1);  // back to 1: waiter only
    TAOS_UNUSED(newCount);
    buf = NULL;
    QUERY_CHECK_CODE(code, lino, _return);
  }
  buf = NULL;  // ownership transferred to pMsgSendInfo, will be freed by destroySendMsgInfo

  code = qSemWait((qTaskInfo_t)pTaskInfo, &pCtx->ready);
  QUERY_CHECK_CODE(code, lino, _return);

  if (pCtx->rspCode != TSDB_CODE_SUCCESS) {
    code = pCtx->rspCode;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  // Deserialize table meta response
  code = tDeserializeSTableMetaRsp(pCtx->pRsp, pCtx->rspLen, pMetaRsp);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  // Release waiter's reference; callback will free pCtx if it's last.
  if (pCtx) vtbRefValidateCtxDecRef(pCtx);
  taosMemoryFree(buf);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, db:%s, tb:%s", __func__, lino, tstrerror(code), dbName, tbName);
  }
  return code;
}

// Check if a column exists in a table schema
static bool vtbRefColExistsInSchema(SSchema* pSchemas, int32_t numOfCols, int32_t numOfTags, const char* colName) {
  int32_t totalCols = numOfCols + numOfTags;
  for (int32_t j = 0; j < totalCols; ++j) {
    if (strcmp(pSchemas[j].name, colName) == 0) {
      return true;
    }
  }
  return false;
}

// Get table schema from local vnode meta and cache it
// Returns: TSDB_CODE_SUCCESS with pEntry filled, or error code
static int32_t vtbRefGetTableSchemaLocal(const SSysTableScanInfo* pInfo, SStorageAPI* pAPI, const char* refTableName,
                                         SHashObj* pTableCache, SVtbRefTableCacheEntry** ppEntry) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaReader srcReader = {0};

  // Check cache first
  SVtbRefTableCacheEntry* pEntry = taosHashGet(pTableCache, refTableName, strlen(refTableName));
  if (pEntry != NULL) {
    *ppEntry = pEntry;
    return TSDB_CODE_SUCCESS;
  }

  // Create new cache entry
  pEntry = taosMemoryCalloc(1, sizeof(SVtbRefTableCacheEntry));
  if (pEntry == NULL) {
    return terrno;
  }

  pAPI->metaReaderFn.initReader(&srcReader, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
  code = pAPI->metaReaderFn.getTableEntryByName(&srcReader, refTableName);

  if (code != TSDB_CODE_SUCCESS) {
    pAPI->metaReaderFn.clearReader(&srcReader);
    pEntry->errCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    pEntry->pSchemaCache = NULL;
  } else {
    ETableType tableType = srcReader.me.type;
    pEntry->tableType = tableType;
    if (tableType == TSDB_CHILD_TABLE || tableType == TSDB_VIRTUAL_CHILD_TABLE) {
      int64_t suid = srcReader.me.ctbEntry.suid;
      if (tableType == TSDB_VIRTUAL_CHILD_TABLE) {
        code = vtbRefCopyColRefWrapper(&srcReader.me.colRef, &pEntry->colRef);
        if (code != TSDB_CODE_SUCCESS) {
          pAPI->metaReaderFn.clearReader(&srcReader);
          vtbRefFreeTableCacheEntry(pEntry);
          return code;
        }
      }
      pAPI->metaReaderFn.clearReader(&srcReader);
      pAPI->metaReaderFn.initReader(&srcReader, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
      code = pAPI->metaReaderFn.getTableEntryByUid(&srcReader, suid);
      if (code != TSDB_CODE_SUCCESS) {
        pAPI->metaReaderFn.clearReader(&srcReader);
        pEntry->errCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
        pEntry->pSchemaCache = NULL;
      } else {
        pEntry->pSchemaCache = vtbRefCreateSchemaCache(tableType, &srcReader);
        pEntry->errCode = (pEntry->pSchemaCache != NULL) ? TSDB_CODE_SUCCESS : terrno;
        pAPI->metaReaderFn.clearReader(&srcReader);
      }
    } else if (tableType == TSDB_NORMAL_TABLE || tableType == TSDB_SUPER_TABLE || tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
      pEntry->pSchemaCache = vtbRefCreateSchemaCache(tableType, &srcReader);
      if (pEntry->pSchemaCache != NULL && tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
        code = vtbRefCopyColRefWrapper(&srcReader.me.colRef, &pEntry->colRef);
        if (code != TSDB_CODE_SUCCESS) {
          pAPI->metaReaderFn.clearReader(&srcReader);
          vtbRefFreeTableCacheEntry(pEntry);
          return code;
        }
      }
      pEntry->errCode = (pEntry->pSchemaCache != NULL) ? TSDB_CODE_SUCCESS : terrno;
      pAPI->metaReaderFn.clearReader(&srcReader);
    } else {
      pAPI->metaReaderFn.clearReader(&srcReader);
      pEntry->errCode = TSDB_CODE_PAR_INVALID_REF_COLUMN;
      pEntry->pSchemaCache = NULL;
    }
  }

  // Add to cache
  code = taosHashPut(pTableCache, refTableName, strlen(refTableName), pEntry, sizeof(SVtbRefTableCacheEntry));
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_DUP_KEY) {
    vtbRefFreeTableCacheEntry(pEntry);
    return code;
  }

  if (code == TSDB_CODE_DUP_KEY) {
    // Another thread inserted first - free our new entry completely
    vtbRefFreeTableCacheEntry(pEntry);
  } else {
    // Success - free original struct only, schema cache owned by hash table copy
    taosMemoryFree(pEntry);
  }

  // Return pointer to hash table's entry (not the freed original)
  *ppEntry = (SVtbRefTableCacheEntry*)taosHashGet(pTableCache, refTableName, strlen(refTableName));
  return TSDB_CODE_SUCCESS;
}

// Try to validate source table and column using local vnode meta
static int32_t vtbRefValidateLocal(const SSysTableScanInfo* pInfo, SStorageAPI* pAPI, const char* refTableName,
                                   const char* refColName, int32_t* pErrCode) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaReader srcReader = {0};

  pAPI->metaReaderFn.initReader(&srcReader, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
  code = pAPI->metaReaderFn.getTableEntryByName(&srcReader, refTableName);

  if (code != TSDB_CODE_SUCCESS) {
    // Table not found locally - could be on another vnode or deleted
    pAPI->metaReaderFn.clearReader(&srcReader);
    *pErrCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
    return TSDB_CODE_SUCCESS;  // Not a fatal error, just table not on this vnode
  }

  // Table found locally, check column existence
  SSchemaWrapper* pSchemaRow = NULL;
  SSchemaWrapper* pTagSchema = NULL;
  if (srcReader.me.type == TSDB_NORMAL_TABLE) {
    pSchemaRow = &srcReader.me.ntbEntry.schemaRow;
  } else if (srcReader.me.type == TSDB_CHILD_TABLE) {
    // For child table, we need to get the super table schema
    int64_t suid = srcReader.me.ctbEntry.suid;
    pAPI->metaReaderFn.clearReader(&srcReader);

    pAPI->metaReaderFn.initReader(&srcReader, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByUid(&srcReader, suid);
    if (code != TSDB_CODE_SUCCESS) {
      pAPI->metaReaderFn.clearReader(&srcReader);
      *pErrCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
      return TSDB_CODE_SUCCESS;
    }
    pSchemaRow = &srcReader.me.stbEntry.schemaRow;
    pTagSchema = &srcReader.me.stbEntry.schemaTag;
  } else if (srcReader.me.type == TSDB_SUPER_TABLE) {
    pSchemaRow = &srcReader.me.stbEntry.schemaRow;
    pTagSchema = &srcReader.me.stbEntry.schemaTag;
  } else {
    pAPI->metaReaderFn.clearReader(&srcReader);
    *pErrCode = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    return TSDB_CODE_SUCCESS;
  }

  // Check column existence in schema
  bool found = false;
  for (int32_t j = 0; j < pSchemaRow->nCols; ++j) {
    if (strcmp(pSchemaRow->pSchema[j].name, refColName) == 0) {
      found = true;
      break;
    }
  }
  if (!found && pTagSchema) {
    for (int32_t j = 0; j < pTagSchema->nCols; ++j) {
      if (strcmp(pTagSchema->pSchema[j].name, refColName) == 0) {
        found = true;
        break;
      }
    }
  }

  pAPI->metaReaderFn.clearReader(&srcReader);
  *pErrCode = found ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_INVALID_REF_COLUMN;
  return TSDB_CODE_SUCCESS;
}

static int32_t vtbRefValidateRemote(void* clientRpc, SEpSet* pMnodeEpSet, int32_t acctId, const char* refDbName,
                                    const char* refTableName, const char* refColName, uint64_t reqId,
                                    SExecTaskInfo* pTaskInfo, SHashObj* pDbVgInfoCache,
                                    SHashObj* pTableCache, int32_t localVgId, int32_t* pErrCode) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SDBVgInfo*    pDbVgInfo = NULL;
  STableMetaRsp metaRsp = {0};
  bool          metaRspInited = false;

  // Step 1: Get DB vgroup info (with caching)
  SDBVgInfo** ppCached = (SDBVgInfo**)taosHashGet(pDbVgInfoCache, refDbName, strlen(refDbName));
  if (ppCached) {
    pDbVgInfo = *ppCached;
  } else {
    code = vtbRefGetDbVgInfo(clientRpc, pMnodeEpSet, acctId, refDbName, reqId, pTaskInfo, &pDbVgInfo);
    if (code != TSDB_CODE_SUCCESS) {
      // DB doesn't exist or network error
      *pErrCode = TSDB_CODE_MND_DB_NOT_EXIST;
      code = TSDB_CODE_SUCCESS;  // not a fatal error for validation
      goto _return;
    }
    code = taosHashPut(pDbVgInfoCache, refDbName, strlen(refDbName), &pDbVgInfo, POINTER_BYTES);
    if (code == TSDB_CODE_DUP_KEY) {
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _return);
  }

  // Step 2: Calculate vgId for the source table
  int32_t vgId = 0;
  SEpSet  vnodeEpSet = {0};
  char    dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)snprintf(dbFName, sizeof(dbFName), "%d.%s", acctId, refDbName);

  code = vtbRefGetVgId(pDbVgInfo, dbFName, refTableName, &vgId, &vnodeEpSet);
  if (code != TSDB_CODE_SUCCESS) {
    *pErrCode = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    code = TSDB_CODE_SUCCESS;
    goto _return;
  }

  // If the target vnode is the same as the local vnode, the table was already
  // checked locally (and not found). Skip the RPC to avoid self-deadlock.
  if (vgId == localVgId) {
    *pErrCode = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    code = TSDB_CODE_SUCCESS;
    goto _return;
  }

  // Step 3: Fetch table schema from the target vnode
  code = vtbRefFetchTableSchema(clientRpc, &vnodeEpSet, acctId, refDbName, refTableName, vgId, reqId, pTaskInfo, &metaRsp);
  metaRspInited = true;
  if (code != TSDB_CODE_SUCCESS) {
    // Table doesn't exist on the target vnode
    *pErrCode = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    code = TSDB_CODE_SUCCESS;
    goto _return;
  }

  bool found = vtbRefColExistsInSchema(metaRsp.pSchemas, metaRsp.numOfColumns, metaRsp.numOfTags, refColName);
  *pErrCode = found ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_INVALID_REF_COLUMN;

  if (pTableCache != NULL) {
    SVtbRefTableCacheEntry* pNewEntry = taosMemoryCalloc(1, sizeof(SVtbRefTableCacheEntry));
    if (pNewEntry != NULL) {
      int32_t cacheCode = vtbRefCreateSchemaCacheFromMetaRsp(&metaRsp, &pNewEntry->pSchemaCache);
      if (cacheCode == TSDB_CODE_SUCCESS) {
        pNewEntry->errCode = TSDB_CODE_SUCCESS;
        int32_t putCode = vtbRefPutRemoteCacheEntry(pTableCache, refDbName, refTableName, pNewEntry);
        if (putCode == TSDB_CODE_SUCCESS) {
          taosMemoryFree(pNewEntry);  // Free struct only, schema cache owned by hash table
        } else {
          vtbRefFreeTableCacheEntry(pNewEntry);  // Free both on failure
        }
      } else {
        taosMemoryFree(pNewEntry);
      }
    }
  }

_return:
  if (metaRspInited) {
    tFreeSTableMetaRsp(&metaRsp);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}


static int32_t vtbRefResolveEntryColumn(const SSysTableScanInfo* pInfo, SExecTaskInfo* pTaskInfo,
                                        const SVtbRefTableCacheEntry* pEntry, const char* refColName, SHashObj* pDbVgInfoCache,
                                        SHashObj* pTableCache, int32_t localVgId, SHashObj* pSeenRefs,
                                        int32_t depth, int32_t* pErrCode) {
  if (pErrCode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pEntry == NULL) {
    *pErrCode = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    return TSDB_CODE_SUCCESS;
  }

  if (pEntry->errCode != TSDB_CODE_SUCCESS) {
    *pErrCode = (pEntry->errCode == TSDB_CODE_TDB_TABLE_NOT_EXIST) ? TSDB_CODE_PAR_TABLE_NOT_EXIST : pEntry->errCode;
    return TSDB_CODE_SUCCESS;
  }

  if (pEntry->pSchemaCache == NULL || !vtbRefCheckColumnInCache(pEntry->pSchemaCache, refColName)) {
    *pErrCode = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    return TSDB_CODE_SUCCESS;
  }

  if (!vtbRefIsVirtualTableType(pEntry->tableType)) {
    *pErrCode = TSDB_CODE_SUCCESS;
    return TSDB_CODE_SUCCESS;
  }

  const SColRef* pRef = vtbRefFindColumnRefInEntry(pEntry, refColName);
  if (pRef == NULL || !pRef->hasRef) {
    *pErrCode = TSDB_CODE_SUCCESS;
    return TSDB_CODE_SUCCESS;
  }

  if (pRef->refDbName[0] == 0 || pRef->refTableName[0] == 0 || pRef->refColName[0] == 0) {
    *pErrCode = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    return TSDB_CODE_SUCCESS;
  }

  return vtbRefResolveSrcColumnChain(pInfo, pTaskInfo, pRef->refDbName, pRef->refTableName, pRef->refColName, pDbVgInfoCache,
                                     pTableCache, localVgId, pSeenRefs, depth + 1, pErrCode);
}

static int32_t vtbRefResolveSrcColumnChain(const SSysTableScanInfo* pInfo, SExecTaskInfo* pTaskInfo, const char* refDbName,
                                           const char* refTableName, const char* refColName, SHashObj* pDbVgInfoCache,
                                           SHashObj* pTableCache, int32_t localVgId, SHashObj* pSeenRefs,
                                           int32_t depth, int32_t* pErrCode) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  char                     refKey[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 3] = {0};
  int8_t                   seenMark = 1;
  bool                     addedSeen = false;
  SVtbRefTableCacheEntry*  pEntry = NULL;
  SStorageAPI*             pAPI = &pTaskInfo->storageAPI;

  if (pErrCode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (depth >= TSDB_MAX_VTABLE_REF_DEPTH) {
    *pErrCode = TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED;
    return TSDB_CODE_SUCCESS;
  }

  (void)snprintf(refKey, sizeof(refKey), "%s.%s.%s", refDbName, refTableName, refColName);
  if (taosHashGet(pSeenRefs, refKey, strlen(refKey)) != NULL) {
    *pErrCode = TSDB_CODE_VTABLE_INVALID_REF_COLUMN;
    return TSDB_CODE_SUCCESS;
  }

  code = taosHashPut(pSeenRefs, refKey, strlen(refKey), &seenMark, sizeof(seenMark));
  if (code == TSDB_CODE_DUP_KEY) {
    *pErrCode = TSDB_CODE_VTABLE_INVALID_REF_COLUMN;
    return TSDB_CODE_SUCCESS;
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  addedSeen = true;

  code = vtbRefGetTableSchemaLocal(pInfo, pAPI, refTableName, pTableCache, &pEntry);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  if (pEntry != NULL && pEntry->errCode == TSDB_CODE_SUCCESS) {
    code = vtbRefResolveEntryColumn(pInfo, pTaskInfo, pEntry, refColName, pDbVgInfoCache, pTableCache, localVgId,
                                    pSeenRefs, depth, pErrCode);
    goto _return;
  }

  if (pEntry != NULL && pEntry->errCode != TSDB_CODE_TDB_TABLE_NOT_EXIST) {
    *pErrCode = pEntry->errCode;
    goto _return;
  }

  if (pInfo->readHandle.pMsgCb == NULL || pInfo->readHandle.pMsgCb->clientRpc == NULL) {
    *pErrCode = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    goto _return;
  }

  pEntry = NULL;
  code = vtbRefValidateRemote(pInfo->readHandle.pMsgCb->clientRpc, (SEpSet*)&pInfo->epSet, pInfo->accountId,
                              refDbName, refTableName, refColName, pTaskInfo->id.queryId,
                              pTaskInfo, pDbVgInfoCache, pTableCache, localVgId, pErrCode);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

_return:
  if (addedSeen) {
    TAOS_UNUSED(taosHashRemove(pSeenRefs, refKey, strlen(refKey)));
  }
  return code;
}

static int32_t validateSrcTableColRef(const SSysTableScanInfo* pInfo, SExecTaskInfo* pTaskInfo, SSchemaWrapper* pSchema,
                                      SColRefWrapper* pColRef, SArray* pResult) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  if (pSchema == NULL || pColRef == NULL || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t localVgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, &localVgId, NULL, NULL);

  SHashObj* pDbVgInfoCache = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pDbVgInfoCache == NULL) {
    return terrno;
  }

  SHashObj* pTableCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pTableCache == NULL) {
    taosHashCleanup(pDbVgInfoCache);
    return terrno;
  }
  taosHashSetFreeFp(pTableCache, vtbRefCleanupTableCacheEntryContents);

  SHashObj* pSeenRefs = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pSeenRefs == NULL) {
    taosHashCleanup(pTableCache);
    taosHashCleanup(pDbVgInfoCache);
    return terrno;
  }

  for (int32_t i = 0; i < pSchema->nCols; ++i) {
    int32_t errCode = TSDB_CODE_SUCCESS;

    if (i == 0 || pColRef == NULL || i >= pColRef->nCols || !pColRef->pColRef[i].hasRef) {
      if (NULL == taosArrayPush(pResult, &errCode)) {
        code = terrno;
        goto _cleanup;
      }
      continue;
    }

    const char* refDbName = pColRef->pColRef[i].refDbName;
    const char* refTableName = pColRef->pColRef[i].refTableName;
    const char* refColName = pColRef->pColRef[i].refColName;

    SVtbRefTableCacheEntry* pEntry = NULL;
    code = vtbRefGetTableSchemaLocal(pInfo, pAPI, refTableName, pTableCache, &pEntry);
    QUERY_CHECK_CODE(code, lino, _cleanup);

    if (pEntry != NULL && pEntry->errCode == TSDB_CODE_SUCCESS && pEntry->pSchemaCache != NULL) {
      errCode = vtbRefCheckColumnInCache(pEntry->pSchemaCache, refColName) ? TSDB_CODE_SUCCESS
                                                                           : TSDB_CODE_PAR_INVALID_REF_COLUMN;
    } else if (pEntry != NULL && pEntry->errCode == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
      if (pInfo->readHandle.pMsgCb && pInfo->readHandle.pMsgCb->clientRpc) {
        SVtbRefTableCacheEntry* pRemoteEntry = vtbRefGetRemoteCacheEntry(pTableCache, refDbName, refTableName);
        if (pRemoteEntry != NULL && pRemoteEntry->errCode == TSDB_CODE_SUCCESS && pRemoteEntry->pSchemaCache != NULL) {
          errCode = vtbRefCheckColumnInCache(pRemoteEntry->pSchemaCache, refColName) ? TSDB_CODE_SUCCESS
                                                                                     : TSDB_CODE_PAR_INVALID_REF_COLUMN;
        } else if (pRemoteEntry != NULL && pRemoteEntry->errCode != TSDB_CODE_SUCCESS) {
          errCode = pRemoteEntry->errCode;
        } else {
          errCode = TSDB_CODE_SUCCESS;
          code = vtbRefValidateRemote(pInfo->readHandle.pMsgCb->clientRpc, (SEpSet*)&pInfo->epSet, pInfo->accountId,
                                      refDbName, refTableName, refColName, pTaskInfo->id.queryId,
                                      pTaskInfo, pDbVgInfoCache, pTableCache, localVgId, &errCode);
          QUERY_CHECK_CODE(code, lino, _cleanup);
        }
      } else {
        errCode = TSDB_CODE_TDB_TABLE_NOT_EXIST;
      }
    } else if (pEntry != NULL) {
      errCode = pEntry->errCode;
    } else {
      errCode = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    }

    if (NULL == taosArrayPush(pResult, &errCode)) {
      code = terrno;
      goto _cleanup;
    }
  }

_cleanup:
  // Free table cache - freeFp handles contents, taosHashCleanup frees nodes
  taosHashCleanup(pTableCache);

  // Free seen refs cache
    taosHashCleanup(pSeenRefs);

  // Free db vg info cache
  if (pDbVgInfoCache) {
    void* pIter = taosHashIterate(pDbVgInfoCache, NULL);
    while (pIter) {
      SDBVgInfo** ppVgInfo = (SDBVgInfo**)pIter;
      if (*ppVgInfo) {
        freeVgInfo(*ppVgInfo);
      }
      pIter = taosHashIterate(pDbVgInfoCache, pIter);
    }
    taosHashCleanup(pDbVgInfoCache);
  }

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t getErrMsgFromCode(int32_t code, char* errMsg, int32_t cap) {
  if (errMsg == NULL || cap <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (code != 0) {
    char buf[128] = {0};
    snprintf(buf, sizeof(buf), "%s", tstrerror(code));
    STR_TO_VARSTR(errMsg, buf);
  } else {
    STR_TO_VARSTR(errMsg, "");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sysTableFillOneVirtualTableRefImpl(const SSysTableScanInfo* pInfo, SExecTaskInfo* pTaskInfo,
                                                  const char* dbname, int32_t* pNumOfRows, const SSDataBlock* dataBlock,
                                                  SSchemaWrapper* schemaRow, SColRefWrapper* pColRef,
                                                  SVirtualTableRefInfo* pRef) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (schemaRow == NULL || schemaRow->pSchema == NULL) {
    qError("sysTableFillOneVirtualTableRefImpl schemaRow or pSchema is NULL");
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t numOfRows = *pNumOfRows;

  int32_t numOfCols = schemaRow->nCols;

  SArray* pResult = taosArrayInit(numOfCols, sizeof(int32_t));
  if (pResult == NULL) {
    QUERY_CHECK_CODE(code = terrno, lino, _end);
  }
  // Only validate if pColRef is valid, otherwise just fill with error codes
  if (pColRef != NULL) {
    code = validateSrcTableColRef(pInfo, pTaskInfo, schemaRow, pColRef, pResult);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    // Fill pResult with success codes for all columns (columns without refs)
    for (int32_t i = 0; i < numOfCols; ++i) {
      int32_t errCode = TSDB_CODE_SUCCESS;
      if (NULL == taosArrayPush(pResult, &errCode)) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

  for (int32_t i = 1; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // Check if this column has a valid reference
    bool hasValidRef = (pColRef != NULL && i < pColRef->nCols && pColRef->pColRef[i].hasRef);

    // virtual db name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, pRef->vDbName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // virtual stable name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, pRef->vStbName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // virtual table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, pRef->vTableName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // virtual col name
    char vColName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(vColName, schemaRow->pSchema[i].name);

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, vColName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // src db name
    char db[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (hasValidRef) {
      STR_TO_VARSTR(db, pColRef->pColRef[i].refDbName);
    } else {
      STR_TO_VARSTR(db, "");
    }

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, db, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // src table name
    char srcTableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (hasValidRef) {
      STR_TO_VARSTR(srcTableName, pColRef->pColRef[i].refTableName);
    } else {
      STR_TO_VARSTR(srcTableName, "");
    }

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, srcTableName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // src col name
    char srcColName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (hasValidRef) {
      STR_TO_VARSTR(srcColName, pColRef->pColRef[i].refColName);
    } else {
      STR_TO_VARSTR(srcColName, "");
    }

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 6);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, srcColName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // type (col ref type: 0=column, 1=tag)
    int32_t colType = 0;  // default: column reference
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&colType, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // err_code
    int32_t* pColErrCode = (int32_t*)taosArrayGet(pResult, i);
    int64_t  errCodeVal = pColErrCode ? (int64_t)(*pColErrCode) : 0;
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&errCodeVal, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // err_msg
    char    errMsg[TSDB_SHOW_VALIDATE_VIRTUAL_TABLE_ERROR + VARSTR_HEADER_SIZE] = {0};
    int32_t colErrCode = pColErrCode ? *pColErrCode : 0;
    code = getErrMsgFromCode(colErrCode, errMsg, sizeof(errMsg));
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, errMsg, false);
    QUERY_CHECK_CODE(code, lino, _end);
    ++numOfRows;
  }
  *pNumOfRows = numOfRows;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosArrayDestroy(pResult);
  return code;
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

  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    terrno = code;
    return NULL;
  }

  for (int32_t i = 0; i < pMeta[index].colNum; ++i) {
    SColumnInfoData colInfoData =
        createColumnInfoData(pMeta[index].schema[i].type, pMeta[index].schema[i].bytes, i + 1);
    code = blockDataAppendColInfo(pBlock, &colInfoData);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      blockDataDestroy(pBlock);
      pBlock = NULL;
      terrno = code;
      break;
    }
  }

  return pBlock;
}

int32_t buildDbTableInfoBlock(const SSysTableScanInfo* pInfo, const SSDataBlock* p,
                              const SSysTableMeta* pSysDbTableMeta, size_t size, const char* dbName, int64_t* pRows) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char    n[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t numOfRows = p->info.rows;

  for (int32_t i = 0; i < size; ++i) {
    const SSysTableMeta* pm = &pSysDbTableMeta[i];
    if (!pInfo->sysInfo && pm->sysInfo) {
      continue;
    }
#ifdef TD_ENTERPRISE
    if (dbName[0] == 'i') {
      if (pm->privCat == PRIV_CAT_BASIC) {
        if (pInfo->privInfoBasic == 0) continue;
      } else if (pm->privCat == PRIV_CAT_PRIVILEGED) {
        if (pInfo->privInfoPrivileged == 0) continue;
      } else if (pm->privCat == PRIV_CAT_SECURITY) {
        if (pInfo->privInfoSec == 0) continue;
      } else if (pm->privCat == PRIV_CAT_AUDIT) {
        if (pInfo->privInfoAudit == 0) continue;
      }
    } else if (dbName[0] == 'p') {
      if (pm->privCat == PRIV_CAT_BASIC) {
        if (pInfo->privPerfBasic == 0) continue;
      } else if (pm->privCat == PRIV_CAT_PRIVILEGED) {
        if (pInfo->privPerfPrivileged == 0) continue;
      }
    }
#endif

    if (strcmp(pm->name, TSDB_INS_TABLE_USERS_FULL) == 0) {
      continue;
    }

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    STR_TO_VARSTR(n, pm->name);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    STR_TO_VARSTR(n, dbName);
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, numOfRows);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&pm->colNum, false);
    QUERY_CHECK_CODE(code, lino, _end);

    for (int32_t j = 4; j <= 8; ++j) {
      pColInfoData = taosArrayGet(p->pDataBlock, j);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);
    }

    STR_TO_VARSTR(n, "SYSTEM_TABLE");

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    numOfRows += 1;
  }

  *pRows = numOfRows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  size_t               size = 0;
  const SSysTableMeta* pSysDbTableMeta = NULL;

  getInfosDbMeta(&pSysDbTableMeta, &size);
  code = buildDbTableInfoBlock(pInfo, p, pSysDbTableMeta, size, TSDB_INFORMATION_SCHEMA_DB, &p->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);
  getPerfDbMeta(&pSysDbTableMeta, &size);
  code = buildDbTableInfoBlock(pInfo, p, pSysDbTableMeta, size, TSDB_PERFORMANCE_SCHEMA_DB, &p->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  pInfo->pRes->info.rows = p->info.rows;
  code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
  QUERY_CHECK_CODE(code, lino, _end);

  blockDataDestroy(p);
  p = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(p);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doSetUserTableMetaInfo(SStoreMetaReader* pMetaReaderFn, SStoreMeta* pMetaFn, void* pVnode,
                                      SMetaReader* pMReader, int64_t uid, const char* dbname, int32_t vgId,
                                      SSDataBlock* p, int32_t rowIndex, const char* idStr) {
  char    n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t lino = 0;
  int32_t code = pMetaReaderFn->getTableEntryByUid(pMReader, uid);
  if (code < 0) {
    qError("failed to get table meta, uid:%" PRId64 ", code:%s, %s", uid, tstrerror(terrno), idStr);
    return code;
  }

  STR_TO_VARSTR(n, pMReader->me.name);

  // table name
  SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

  code = colDataSetVal(pColInfoData, rowIndex, n, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // database name
  pColInfoData = taosArrayGet(p->pDataBlock, 1);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, rowIndex, dbname, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // vgId
  pColInfoData = taosArrayGet(p->pDataBlock, 6);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, rowIndex, (char*)&vgId, false);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t tableType = pMReader->me.type;
  if (tableType == TSDB_CHILD_TABLE) {
    // create time
    int64_t ts = pMReader->me.ctbEntry.btime;
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&ts, false);
    QUERY_CHECK_CODE(code, lino, _end);

    SMetaReader mr1 = {0};
    pMetaReaderFn->initReader(&mr1, pVnode, META_READER_NOLOCK, pMetaFn);

    int64_t suid = pMReader->me.ctbEntry.suid;
    code = pMetaReaderFn->getTableEntryByUid(&mr1, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pMReader->me.name, suid,
             tstrerror(code), idStr);
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&mr1.me.stbEntry.schemaRow.nCols, false);
    if (code != 0) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // super table name
    STR_TO_VARSTR(n, mr1.me.name);
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, n, false);
    pMetaReaderFn->clearReader(&mr1);
    QUERY_CHECK_CODE(code, lino, _end);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (pMReader->me.ctbEntry.commentLen > 0) {
      char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, pMReader->me.ctbEntry.comment);
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pMReader->me.ctbEntry.commentLen == 0) {
      char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, "");
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      colDataSetNULL(pColInfoData, rowIndex);
    }

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ctbEntry.ttlDays, false);
    QUERY_CHECK_CODE(code, lino, _end);

    STR_TO_VARSTR(n, "CHILD_TABLE");

  } else if (tableType == TSDB_NORMAL_TABLE) {
    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.btime, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.schemaRow.nCols, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // super table name
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (pMReader->me.ntbEntry.commentLen > 0) {
      char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, pMReader->me.ntbEntry.comment);
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pMReader->me.ntbEntry.commentLen == 0) {
      char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, "");
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      colDataSetNULL(pColInfoData, rowIndex);
    }

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.ttlDays, false);
    QUERY_CHECK_CODE(code, lino, _end);

    STR_TO_VARSTR(n, "NORMAL_TABLE");
    // impl later
  } else if (tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.btime, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.schemaRow.nCols, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // super table name
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    STR_TO_VARSTR(n, "VIRTUAL_NORMAL_TABLE");
    // impl later
  } else if (tableType == TSDB_VIRTUAL_CHILD_TABLE) {
    // create time
    int64_t ts = pMReader->me.ctbEntry.btime;
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&ts, false);
    QUERY_CHECK_CODE(code, lino, _end);

    SMetaReader mr1 = {0};
    pMetaReaderFn->initReader(&mr1, pVnode, META_READER_NOLOCK, pMetaFn);

    int64_t suid = pMReader->me.ctbEntry.suid;
    code = pMetaReaderFn->getTableEntryByUid(&mr1, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pMReader->me.name, suid,
             tstrerror(code), idStr);
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&mr1.me.stbEntry.schemaRow.nCols, false);
    if (code != 0) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // super table name
    STR_TO_VARSTR(n, mr1.me.name);
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, n, false);
    pMetaReaderFn->clearReader(&mr1);
    QUERY_CHECK_CODE(code, lino, _end);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    STR_TO_VARSTR(n, "VIRTUAL_CHILD_TABLE");
  }

  pColInfoData = taosArrayGet(p->pDataBlock, 9);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, rowIndex, n, false);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  qError("%s failed at line %d since %s, %s", __func__, lino, tstrerror(code), idStr);
  return code;
}

static SSDataBlock* sysTableBuildUserTablesByUids(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SSysTableIndex*    pIdx = pInfo->pIdx;
  SSDataBlock*       p = NULL;
  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  int ret = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t i = pIdx->lastIdx;
  for (; i < taosArrayGetSize(pIdx->uids); i++) {
    tb_uid_t* uid = taosArrayGet(pIdx->uids, i);
    QUERY_CHECK_NULL(uid, code, lino, _end, terrno);

    SMetaReader mr = {0};
    pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);

    code = doSetUserTableMetaInfo(&pAPI->metaReaderFn, &pAPI->metaFn, pInfo->readHandle.vnode, &mr, *uid, dbname, vgId,
                                  p, numOfRows, GET_TASKID(pTaskInfo));

    pAPI->metaReaderFn.clearReader(&mr);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

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

    code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  if (i >= taosArrayGetSize(pIdx->uids)) {
    setOperatorCompleted(pOperator);
  } else {
    pIdx->lastIdx = i + 1;
  }

  blockDataDestroy(p);
  p = NULL;

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableBuildUserTables(SOperatorInfo* pOperator) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;
  int8_t         firstMetaCursor = 0;
  SSDataBlock*   p = NULL;

  SSysTableScanInfo* pInfo = pOperator->info;
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    QUERY_CHECK_NULL(pInfo->pCur, code, lino, _end, terrno);
    firstMetaCursor = 1;
  }
  if (!firstMetaCursor) {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 1);
    if (code != 0) {
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  char n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

  int32_t ret = 0;
  while ((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_SUPER_TABLE)) == 0) {
    STR_TO_VARSTR(n, pInfo->pCur->mr.me.name);

    // table name
    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // vgId
    pColInfoData = taosArrayGet(p->pDataBlock, 6);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    int32_t tableType = pInfo->pCur->mr.me.type;
    if (tableType == TSDB_CHILD_TABLE) {
      // create time
      int64_t ts = pInfo->pCur->mr.me.ctbEntry.btime;
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&ts, false);
      QUERY_CHECK_CODE(code, lino, _end);

      SMetaReader mr = {0};
      pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);

      uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      code = pAPI->metaReaderFn.getTableEntryByUid(&mr, suid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
               suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
        pAPI->metaReaderFn.clearReader(&mr);
        pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
        pInfo->pCur = NULL;
        blockDataDestroy(p);
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      if (isTsmaResSTb(mr.me.name)) {
        pAPI->metaReaderFn.clearReader(&mr);
        continue;
      }

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.stbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      STR_TO_VARSTR(n, mr.me.name);
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, n, false);
      QUERY_CHECK_CODE(code, lino, _end);
      pAPI->metaReaderFn.clearReader(&mr);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      if (pInfo->pCur->mr.me.ctbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ctbEntry.comment);
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (pInfo->pCur->mr.me.ctbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ctbEntry.ttlDays, false);
      QUERY_CHECK_CODE(code, lino, _end);

      STR_TO_VARSTR(n, "CHILD_TABLE");
    } else if (tableType == TSDB_NORMAL_TABLE) {
      // create time
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.btime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      if (pInfo->pCur->mr.me.ntbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ntbEntry.comment);
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (pInfo->pCur->mr.me.ntbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.ttlDays, false);
      QUERY_CHECK_CODE(code, lino, _end);

      STR_TO_VARSTR(n, "NORMAL_TABLE");
    } else if (tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
      // create time
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.btime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      STR_TO_VARSTR(n, "VIRTUAL_NORMAL_TABLE");
    } else if (tableType == TSDB_VIRTUAL_CHILD_TABLE) {
      // create time
      int64_t ts = pInfo->pCur->mr.me.ctbEntry.btime;
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&ts, false);
      QUERY_CHECK_CODE(code, lino, _end);

      SMetaReader mr = {0};
      pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);

      uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      code = pAPI->metaReaderFn.getTableEntryByUid(&mr, suid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
               suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
        pAPI->metaReaderFn.clearReader(&mr);
        pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
        pInfo->pCur = NULL;
        blockDataDestroy(p);
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      if (isTsmaResSTb(mr.me.name)) {
        pAPI->metaReaderFn.clearReader(&mr);
        continue;
      }

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.stbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      STR_TO_VARSTR(n, mr.me.name);
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, n, false);
      QUERY_CHECK_CODE(code, lino, _end);
      pAPI->metaReaderFn.clearReader(&mr);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      STR_TO_VARSTR(n, "VIRTUAL_CHILD_TABLE");
    }

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

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

    code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  blockDataDestroy(p);
  p = NULL;

  // todo temporarily free the cursor here, the true reason why the free is not valid needs to be found
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static int32_t buildVgDiskUsage(SOperatorInfo* pOperator, SDbSizeStatisInfo* pStaticsInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            vgId = 0;
  const char*        db = NULL;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &pStaticsInfo->dbname, &vgId, NULL, NULL);

  pStaticsInfo->vgId = vgId;

  code = pAPI->metaFn.getDBSize(pInfo->readHandle.vnode, pStaticsInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = vnodeEstimateRawDataSize(pOperator, pStaticsInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  pStaticsInfo->memSize = pStaticsInfo->memSize >> 10;
  pStaticsInfo->l1Size = pStaticsInfo->l1Size >> 10;
  pStaticsInfo->l2Size = pStaticsInfo->l2Size >> 10;
  pStaticsInfo->l3Size = pStaticsInfo->l3Size >> 10;
  pStaticsInfo->cacheSize = pStaticsInfo->cacheSize >> 10;
  pStaticsInfo->walSize = pStaticsInfo->walSize >> 10;
  pStaticsInfo->metaSize = pStaticsInfo->metaSize >> 10;
  pStaticsInfo->rawDataSize = pStaticsInfo->rawDataSize >> 10;
  pStaticsInfo->ssSize = pStaticsInfo->ssSize >> 10;

_end:
  return code;
}

static int8_t shouldEstimateRawDataSize(SOperatorInfo* pOperator) {
  int32_t lino = 0;
  size_t  size = 0;
  int32_t index = 0;

  const SSysTableMeta* pMeta = NULL;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  SSysTableScanInfo* pInfo = pOperator->info;
  getInfosDbMeta(&pMeta, &size);

  for (int32_t i = 0; i < size; ++i) {
    if (strcmp(pMeta[i].name, TSDB_INS_DISK_USAGE) == 0) {
      index = i;
      break;
    }
  }
  if (index >= size) {
    return 1;
  }
  const SSysTableMeta* pTgtMeta = &pMeta[index];
  int32_t              colNum = pTgtMeta->colNum;
  SColumnInfoData      colInfoData =
      createColumnInfoData(pTgtMeta->schema[colNum - 1].type, pTgtMeta->schema[colNum - 1].bytes, colNum);
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->matchInfo.pList); i++) {
    SColMatchItem* pItem = taosArrayGet(pInfo->matchInfo.pList, i);
    if (pItem->colId == colInfoData.info.colId) {
      return 1;
    }
  }
  return 0;
}
static SSDataBlock* sysTableBuildVgUsage(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SDbSizeStatisInfo  staticsInfo = {.estimateRawData = 1};

  char*        buf = NULL;
  SSDataBlock* p = NULL;

  const char* db = NULL;
  int32_t     numOfCols = 0;
  int32_t     numOfRows = 0;

  // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
  if (pInfo->readHandle.mnd != NULL) {
    setOperatorCompleted(pOperator);
    return NULL;
  }
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    if (pInfo->pCur == NULL) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SSDataBlock* pBlock = pInfo->pRes;

  if (!pInfo->showRewrite) {
    staticsInfo.estimateRawData = shouldEstimateRawDataSize(pOperator);
  }

  code = buildVgDiskUsage(pOperator, &staticsInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->showRewrite) {
    SSDataBlock*      pBlock = pInfo->pRes;
    SDBBlockUsageInfo usageInfo = {0};
    int32_t           len = tSerializeBlockDbUsage(NULL, 0, &usageInfo);

    usageInfo.dataInDiskSize = staticsInfo.l1Size + staticsInfo.l2Size + staticsInfo.l3Size;
    usageInfo.walInDiskSize = staticsInfo.walSize;
    usageInfo.rawDataSize = staticsInfo.rawDataSize;

    buf = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE);
    QUERY_CHECK_NULL(buf, code, lino, _end, terrno);

    int32_t tempRes = tSerializeBlockDbUsage(varDataVal(buf), len, &usageInfo);
    if (tempRes != len) {
      QUERY_CHECK_CODE(TSDB_CODE_INVALID_MSG, lino, _end);
    }

    varDataSetLen(buf, len);

    int32_t          slotId = 1;
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
    code = colDataSetVal(pColInfo, 0, buf, false);
    QUERY_CHECK_CODE(code, lino, _end);
    taosMemoryFreeClear(buf);
    if (slotId != 0) {
      SColumnInfoData* p1 = taosArrayGet(pBlock->pDataBlock, 0);
      QUERY_CHECK_NULL(p1, code, lino, _end, terrno);
    }

    pBlock->info.rows = 1;
    pOperator->status = OP_EXEC_DONE;
    pInfo->pRes->info.rows = pBlock->info.rows;
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    SName sn = {0};
    char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    code = tNameFromString(&sn, staticsInfo.dbname, T_NAME_ACCT | T_NAME_DB);
    QUERY_CHECK_CODE(code, lino, _end);

    code = tNameGetDbName(&sn, varDataVal(dbname));
    QUERY_CHECK_CODE(code, lino, _end);

    varDataSetLen(dbname, strlen(varDataVal(dbname)));

    p = buildInfoSchemaTableMetaBlock(TSDB_INS_DISK_USAGE);
    QUERY_CHECK_NULL(p, code, lino, _end, terrno);

    code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.vgId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.walSize, false);  // wal
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.l1Size, false);  // l1_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.l2Size, false);  // l2_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.l3Size, false);  // l3_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.cacheSize, false);  // cache_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.metaSize, false);  // meta_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.ssSize, false);  // ss_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.rawDataSize, false);  // estimate_size
    QUERY_CHECK_CODE(code, lino, _end);
    numOfRows += 1;

    if (numOfRows > 0) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    blockDataDestroy(p);
    p = NULL;

    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
    setOperatorCompleted(pOperator);
  }
_end:
  taosMemoryFree(buf);
  if (pInfo->pCur) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserTables(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  SNode* pCondition = pInfo->pCondition;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
  if (pInfo->readHandle.mnd != NULL) {
    code = buildSysDbTableInfo(pInfo, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

    setOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  } else {
    if (pInfo->showRewrite == false) {
      if (pCondition != NULL && pInfo->pIdx == NULL) {
        SSTabFltArg arg = {
            .pMeta = pInfo->readHandle.vnode, .pVnode = pInfo->readHandle.vnode, .pAPI = &pTaskInfo->storageAPI};

        SSysTableIndex* idx = taosMemoryMalloc(sizeof(SSysTableIndex));
        QUERY_CHECK_NULL(idx, code, lino, _end, terrno);
        idx->init = 0;
        idx->uids = taosArrayInit(128, sizeof(int64_t));
        QUERY_CHECK_NULL(idx->uids, code, lino, _end, terrno);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
}
static SSDataBlock* sysTableScanUsage(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  SNode* pCondition = pInfo->pCondition;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  return sysTableBuildVgUsage(pOperator);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
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

static int32_t doSetQueryFileSetRow() {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // TODO

_exit:
  return code;
}

// ---------------------------------------------------------------------------
// ins_table_fixed_distributed: per-vnode block distribution scan handler
// ---------------------------------------------------------------------------

// forward declarations for static functions defined later in this file
static int32_t doGetTableRowSize(SReadHandle* pHandle, uint64_t uid, int32_t* rowLen, const char* idstr);
static int32_t initTableblockDistQueryCond(uint64_t uid, SQueryTableDataCond* pCond);
static int32_t buildTableListInfo(SOperatorInfo* pOperator, STableId* id, STableListInfo** ppTableListInfo);

static SSDataBlock* sysTableBuildTableFixedDist(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SSDataBlock*       p = NULL;
  void*              pHandle = NULL;
  STableListInfo*    pTableListInfo = NULL;

  // ---- extract db_name & vgroup_id from vnode ----
  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);
  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  // ---- extract table_name from WHERE condition ----
  // Note: after planner's setConditionsSlotId, column userAlias may be empty.
  // We must check colName directly instead of using sysTableIsCondOnOneTable
  // which relies on nodesGetNameFromColumnNode (returns userAlias).
  char condTableName[TSDB_TABLE_NAME_LEN] = {0};
  bool foundTableName = false;
  if (pInfo->pCondition != NULL) {
    SNode* pCond = pInfo->pCondition;
    // helper: check a single operator node for table_name = 'xxx'
    #define CHECK_OP_FOR_TABLENAME(opNode)                                              \
      do {                                                                              \
        SOperatorNode* _op = (SOperatorNode*)(opNode);                                  \
        if (_op->opType == OP_TYPE_EQUAL &&                                             \
            nodeType(_op->pLeft) == QUERY_NODE_COLUMN &&                                \
            nodeType(_op->pRight) == QUERY_NODE_VALUE) {                                \
          SColumnNode* _col = (SColumnNode*)_op->pLeft;                                 \
          const char* _name = _col->colName[0] ? _col->colName                         \
                            : (_col->node.userAlias[0] ? _col->node.userAlias : "");    \
          if (strcasecmp(_name, "table_name") == 0) {                                   \
            SValueNode* _val = (SValueNode*)_op->pRight;                                \
            if (_val->node.resType.type == TSDB_DATA_TYPE_NCHAR ||                      \
                _val->node.resType.type == TSDB_DATA_TYPE_VARCHAR) {                    \
              char* _v = nodesGetValueFromNode((SValueNode*)_op->pRight);               \
              tstrncpy(condTableName, varDataVal(_v), TSDB_TABLE_NAME_LEN);             \
              foundTableName = true;                                                    \
            }                                                                           \
          }                                                                             \
        }                                                                               \
      } while (0)

    if (nodeType(pCond) == QUERY_NODE_OPERATOR) {
      CHECK_OP_FOR_TABLENAME(pCond);
    } else if (nodeType(pCond) == QUERY_NODE_LOGIC_CONDITION) {
      SLogicConditionNode* pLogic = (SLogicConditionNode*)pCond;
      if (LOGIC_COND_TYPE_AND == pLogic->condType) {
        SNode* pChild = NULL;
        FOREACH(pChild, pLogic->pParameterList) {
          if (QUERY_NODE_OPERATOR == nodeType(pChild)) {
            CHECK_OP_FOR_TABLENAME(pChild);
            if (foundTableName) break;
          }
        }
      }
    }
    #undef CHECK_OP_FOR_TABLENAME
  }
  if (!foundTableName) {
    qDebug("ins_table_fixed_distributed: table_name condition not found, returning empty");
    setOperatorCompleted(pOperator);
    return NULL;
  }

  // ---- look up the table to get uid & type ----
  SMetaReader smr = {0};
  pAPI->metaReaderFn.initReader(&smr, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  code = pAPI->metaReaderFn.getTableEntryByName(&smr, condTableName);
  if (code != TSDB_CODE_SUCCESS) {
    pAPI->metaReaderFn.clearReader(&smr);
    qDebug("ins_table_fixed_distributed: table '%s' not found in this vnode, returning empty", condTableName);
    setOperatorCompleted(pOperator);
    return NULL;
  }

  STableId tableId = {0};
  tb_uid_t suid = 0;  // supertable uid for TSDB reader context
  if (smr.me.type == TSDB_SUPER_TABLE) {
    tableId.type = TSDB_SUPER_TABLE;
    tableId.uid = smr.me.uid;
    suid = smr.me.uid;
  } else if (smr.me.type == TSDB_CHILD_TABLE) {
    // Single child table scan — only scan this child's blocks, not all siblings.
    // Use the child's own uid for the table list, and suid for reader context.
    tableId.type = TSDB_NORMAL_TABLE;
    tableId.uid = smr.me.uid;
    suid = smr.me.ctbEntry.suid;
  } else {
    tableId.type = TSDB_NORMAL_TABLE;
    tableId.uid = smr.me.uid;
  }
  uint64_t rowSizeUid = smr.me.uid;  // uid used for doGetTableRowSize
  pAPI->metaReaderFn.clearReader(&smr);

  // ---- build table list for TSDB reader ----
  code = buildTableListInfo(pOperator, &tableId, &pTableListInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SQueryTableDataCond cond = {0};
  code = initTableblockDistQueryCond(suid, &cond);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t numTables = 0;
  code = tableListGetSize(pTableListInfo, &numTables);
  QUERY_CHECK_CODE(code, lino, _end);

  void* pList = tableListGetInfo(pTableListInfo, 0);
  code = pInfo->readHandle.api.tsdReader.tsdReaderOpen(pInfo->readHandle.vnode, &cond, pList, numTables, NULL,
                                                       &pHandle, pTaskInfo->id.str, NULL);
  cleanupQueryTableDataCond(&cond);
  QUERY_CHECK_CODE(code, lino, _end);

  // ---- collect block distribution info ----
  STableBlockDistInfo blockDistInfo = {.minRows = INT_MAX, .maxRows = INT_MIN};
  code = doGetTableRowSize(&pInfo->readHandle, rowSizeUid, (int32_t*)&blockDistInfo.rowSize, GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _end);

  code = pAPI->tsdReader.tsdReaderGetDataBlockDistInfo(pHandle, &blockDistInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = pAPI->tsdReader.tsdReaderGetNumOfInMemRows(pHandle, &blockDistInfo.numOfInmemRows);
  QUERY_CHECK_CODE(code, lino, _end);

  pInfo->readHandle.api.tsdReader.tsdReaderClose(pHandle);
  pHandle = NULL;

  // ---- build output data block (1 row) ----
  p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLE_FIXED_DISTRIBUTED);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);
  code = blockDataEnsureCapacity(p, 1);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t          colIdx = 0;
  SColumnInfoData* pColInfo = NULL;

  // col 0: db_name (VARCHAR)
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, dbname, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 1: table_name (VARCHAR)
  char tblNameVar[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(tblNameVar, condTableName);
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, tblNameVar, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 2: vgroup_id (INT)
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&vgId, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 3: total_blocks (BIGINT)
  int64_t totalBlocks = (int64_t)blockDistInfo.numOfBlocks;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&totalBlocks, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 4: total_size (BIGINT)
  int64_t totalSize = (int64_t)blockDistInfo.totalSize;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&totalSize, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 5: average_size (DOUBLE)
  double avgSize = (totalBlocks > 0) ? ((double)totalSize / totalBlocks) : 0.0;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&avgSize, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 6: compression_ratio (DOUBLE)
  int64_t totalRows = (int64_t)blockDistInfo.totalRows;
  double  compRatio = 0.0;
  if (totalRows > 0 && blockDistInfo.rowSize > 0) {
    compRatio = (double)totalSize * 100.0 / ((double)blockDistInfo.rowSize * (double)totalRows);
  }
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&compRatio, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 7: block_rows (BIGINT)
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&totalRows, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 8: min_rows (INT)
  int32_t minRows = (blockDistInfo.minRows == INT_MAX) ? 0 : blockDistInfo.minRows;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&minRows, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 9: max_rows (INT)
  int32_t maxRows = (blockDistInfo.maxRows == INT_MIN) ? 0 : blockDistInfo.maxRows;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&maxRows, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 10: avg_rows (DOUBLE)
  double avgRows = (totalBlocks > 0) ? ((double)totalRows / totalBlocks) : 0.0;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&avgRows, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 11: in_mem_rows (BIGINT)
  int64_t inMemRows = (int64_t)blockDistInfo.numOfInmemRows;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&inMemRows, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 12: stt_rows (BIGINT)
  int64_t sttRows = (int64_t)blockDistInfo.numOfSttRows;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&sttRows, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 13: total_tables (BIGINT)
  int64_t totalTables = (int64_t)blockDistInfo.numOfTables;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&totalTables, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 14: total_filesets (BIGINT)
  int64_t totalFilesets = (int64_t)blockDistInfo.numOfFiles;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&totalFilesets, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 15: total_vgroups (BIGINT) — always 1 per vnode row
  int64_t totalVgroups = 1;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&totalVgroups, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // col 16: row_size (INT)
  int32_t rowSize = (int32_t)blockDistInfo.rowSize;
  pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, 0, (char*)&rowSize, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // cols 17-24: block_dist_64 .. block_dist_other (BIGINT x8)
  for (int32_t i = 0; i < 8; ++i) {
    int64_t histVal = (int64_t)blockDistInfo.blockRowsHistoFixed[i];
    pColInfo = taosArrayGet(p->pDataBlock, colIdx++);
    QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
    code = colDataSetVal(pColInfo, 0, (char*)&histVal, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  p->info.rows = 1;
  pInfo->pRes->info.rows = 1;

  code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  blockDataDestroy(p);
  p = NULL;
  tableListDestroy(pTableListInfo);
  setOperatorCompleted(pOperator);

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;

_end:
  if (pHandle != NULL) {
    pInfo->readHandle.api.tsdReader.tsdReaderClose(pHandle);
  }
  blockDataDestroy(p);
  tableListDestroy(pTableListInfo);

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
}

static SSDataBlock* sysTableScanTableFixedDist(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSysTableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pInfo->readHandle.mnd != NULL) {
    return NULL;
  }

  return sysTableBuildTableFixedDist(pOperator);
}

static SSDataBlock* sysTableBuildUserFileSets(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SSDataBlock*       p = NULL;

  // open cursor if not opened
  if (pInfo->pFileSetReader == NULL) {
    code = pAPI->tsdReader.fileSetReaderOpen(pInfo->readHandle.vnode, &pInfo->pFileSetReader);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_FILESETS);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  char    n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t ret = 0;

  // loop to query each entry
  for (;;) {
    int32_t ret = pAPI->tsdReader.fileSetReadNext(pInfo->pFileSetReader);
    if (ret) {
      if (ret == TSDB_CODE_NOT_FOUND) {
        // no more scan entry
        setOperatorCompleted(pOperator);
        pAPI->tsdReader.fileSetReaderClose(&pInfo->pFileSetReader);
        break;
      } else {
        code = ret;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    // fill the data block
    {
      SColumnInfoData* pColInfoData;
      int32_t          index = 0;

      // db_name
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // vgroup_id
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // fileset_id
      int32_t filesetId = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "fileset_id", &filesetId);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&filesetId, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // start_time
      int64_t startTime = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "start_time", &startTime);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&startTime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // end_time
      int64_t endTime = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "end_time", &endTime);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&endTime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // total_size
      int64_t totalSize = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "total_size", &totalSize);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&totalSize, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // last_compact
      int64_t lastCompact = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "last_compact_time", &lastCompact);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&lastCompact, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // should_compact
      bool shouldCompact = false;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "should_compact", &shouldCompact);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&shouldCompact, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // // details
      // const char* details = NULL;
      // code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "details", &details);
      // QUERY_CHECK_CODE(code, lino, _end);
      // pColInfoData = taosArrayGet(p->pDataBlock, index++);
      // QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      // code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
      // QUERY_CHECK_CODE(code, lino, _end);
    }

    // check capacity
    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

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

    code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  blockDataDestroy(p);
  p = NULL;

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    pAPI->tsdReader.fileSetReaderClose(&pInfo->pFileSetReader);
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserFileSets(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSysTableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SNode*             pCondition = pInfo->pCondition;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pInfo->readHandle.mnd != NULL) {
    // do nothing on mnode
    qTrace("This operator do nothing on mnode, task id:%s", GET_TASKID(pTaskInfo));
    return NULL;
  } else {
#if 0
    if (pInfo->showRewrite == false) {
      if (pCondition != NULL && pInfo->pIdx == NULL) {
        SSTabFltArg arg = {
            .pMeta = pInfo->readHandle.vnode, .pVnode = pInfo->readHandle.vnode, .pAPI = &pTaskInfo->storageAPI};

        SSysTableIndex* idx = taosMemoryMalloc(sizeof(SSysTableIndex));
        QUERY_CHECK_NULL(idx, code, lino, _end, terrno);
        idx->init = 0;
        idx->uids = taosArrayInit(128, sizeof(int64_t));
        QUERY_CHECK_NULL(idx->uids, code, lino, _end, terrno);
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
#endif

    return sysTableBuildUserFileSets(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
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
      tstrncpy((char*)pContext, varDataVal(dbName), TSDB_DB_NAME_LEN);
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

static int32_t doSysTableScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  // build message and send to mnode to fetch the content of system tables.
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  char               dbName[TSDB_DB_NAME_LEN] = {0};
  int32_t            code = TSDB_CODE_SUCCESS;

  if (pOperator->pOperatorGetParam != NULL) {
    code = sysTableScanApplyVtbRefReqParam(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  while (1) {
    if (isTaskKilled(pOperator->pTaskInfo)) {
      setOperatorCompleted(pOperator);
      (*ppRes) = NULL;
      break;
    }

    blockDataCleanup(pInfo->pRes);

    const char* name = tNameGetTableName(&pInfo->name);
    if (pInfo->showRewrite) {
      getDBNameFromCondition(pInfo->pCondition, dbName);
      if (strncasecmp(name, TSDB_INS_TABLE_COMPACTS, TSDB_TABLE_FNAME_LEN) != 0 &&
          strncasecmp(name, TSDB_INS_TABLE_SCANS, TSDB_TABLE_FNAME_LEN) != 0 &&
          strncasecmp(name, TSDB_INS_TABLE_COMPACT_DETAILS, TSDB_TABLE_FNAME_LEN) != 0 &&
          strncasecmp(name, TSDB_INS_TABLE_SSMIGRATES, TSDB_TABLE_FNAME_LEN) != 0 &&
          strncasecmp(name, TSDB_INS_TABLE_SCAN_DETAILS, TSDB_TABLE_FNAME_LEN) != 0 &&
          strncasecmp(name, TSDB_INS_TABLE_TRANSACTION_DETAILS, TSDB_TABLE_FNAME_LEN) != 0) {
        TAOS_UNUSED(snprintf(pInfo->req.db, sizeof(pInfo->req.db), "%d.%s", pInfo->accountId, dbName));
      }
    } else if (strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0) {
      getDBNameFromCondition(pInfo->pCondition, dbName);
      if (dbName[0]) TAOS_UNUSED(snprintf(pInfo->req.db, sizeof(pInfo->req.db), "%d.%s", pInfo->accountId, dbName));
      (void)sysTableIsCondOnOneTable(pInfo->pCondition, pInfo->req.filterTb);
    }
    bool         filter = true;
    SSDataBlock* pBlock = NULL;
    if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUserTables(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUserTags(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->readHandle.mnd == NULL) {
      pBlock = sysTableScanUserCols(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_VC_COLS, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->readHandle.mnd == NULL) {
      pBlock = sysTableScanUserVcCols(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_STABLES, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->showRewrite &&
               IS_SYS_DBNAME(dbName)) {
      pBlock = sysTableScanUserSTables(pOperator);
    } else if (strncasecmp(name, TSDB_INS_DISK_USAGE, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUsage(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_FILESETS, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUserFileSets(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_TABLE_FIXED_DISTRIBUTED, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanTableFixedDist(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_VIRTUAL_TABLES_REFERENCING, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanVirtualTableRef(pOperator);
    } else {  // load the meta from mnode of the given epset
      pBlock = sysTableScanFromMNode(pOperator, pInfo, name, pTaskInfo);
    }

    /* record input rows before filter */
    pOperator->cost.inputRows += (pBlock == NULL) ? 0 : pBlock->info.rows;
    if (!pInfo->skipFilterTable && pInfo->pVtbRefReqs == NULL) {
      sysTableScanFillTbName(pOperator, pInfo, name, pBlock);
    }
    if (pBlock != NULL) {
      bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
      if (limitReached) {
        setOperatorCompleted(pOperator);
      }

      if (pBlock->info.rows == 0) {
        continue;
      }
      (*ppRes) = pBlock;
    } else {
      (*ppRes) = NULL;
    }
    break;
  }

  if (pTaskInfo->code) {
    qError("%s failed since %s", __func__, tstrerror(pTaskInfo->code));
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }
  return pTaskInfo->code;
}

static void sysTableScanFillTbName(SOperatorInfo* pOperator, const SSysTableScanInfo* pInfo, const char* name,
                                   SSDataBlock* pBlock) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pBlock == NULL) {
    return;
  }

  if (pInfo->tbnameSlotId != -1) {
    SColumnInfoData* pColumnInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, pInfo->tbnameSlotId);
    QUERY_CHECK_NULL(pColumnInfoData, code, lino, _end, terrno);
    char varTbName[TSDB_TABLE_FNAME_LEN - 1 + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(varTbName, name);

    code = colDataSetNItems(pColumnInfoData, 0, varTbName, pBlock->info.rows, 1, true);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static SSDataBlock* sysTableScanFromMNode(SOperatorInfo* pOperator, SSysTableScanInfo* pInfo, const char* name,
                                          SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  while (1) {
    int64_t startTs = taosGetTimestampUs();
    tstrncpy(pInfo->req.tb, tNameGetTableName(&pInfo->name), tListLen(pInfo->req.tb));
    tstrncpy(pInfo->req.user, pInfo->pUser, tListLen(pInfo->req.user));

    int32_t contLen = tSerializeSRetrieveTableReq(NULL, 0, &pInfo->req);
    char*   buf1 = taosMemoryCalloc(1, contLen);
    if (!buf1) {
      return NULL;
    }
    int32_t tempRes = tSerializeSRetrieveTableReq(buf1, contLen, &pInfo->req);
    if (tempRes < 0) {
      code = terrno;
      taosMemoryFree(buf1);
      return NULL;
    }

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = terrno;
      taosMemoryFree(buf1);
      return NULL;
    }

    int32_t msgType = (strcasecmp(name, TSDB_INS_TABLE_DNODE_VARIABLES) == 0) ? TDMT_DND_SYSTABLE_RETRIEVE
                                                                              : TDMT_MND_SYSTABLE_RETRIEVE;

    // Allocate a lightweight wrapper that holds only the ref ID; the callback
    // frees it via paramFreeFp = taosAutoMemoryFree after the callback returns.
    SSysTableScanCbParam* pWrapper = taosMemoryCalloc(1, sizeof(SSysTableScanCbParam));
    if (!pWrapper) {
      pTaskInfo->code = terrno;
      taosMemoryFree(buf1);
      taosMemoryFree(pMsgSendInfo);
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }
    pWrapper->sysTableScanId = pInfo->self;

    // reset rspCode from the previous iteration
    pInfo->rspCode = 0;

    pMsgSendInfo->param = pWrapper;
    pMsgSendInfo->paramFreeFp = taosAutoMemoryFree;
    pMsgSendInfo->msgInfo.pData = buf1;
    pMsgSendInfo->msgInfo.len = contLen;
    pMsgSendInfo->msgType = msgType;
    pMsgSendInfo->fp = loadSysTableCallback;
    pMsgSendInfo->requestId = pTaskInfo->id.queryId;

    code = asyncSendMsgToServer(pInfo->readHandle.pMsgCb->clientRpc, &pInfo->epSet, NULL, pMsgSendInfo);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // Block this worker thread until the response arrives.  qSemWait notifies
    // the worker pool and waits, then re-acquires on wake-up.
    code = qSemWait((qTaskInfo_t)pTaskInfo, &pInfo->ready);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s tsem_wait failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (pInfo->rspCode != TSDB_CODE_SUCCESS) {
      qError("%s load meta data from mnode failed, totalRows:%" PRIu64 ", code:%s", GET_TASKID(pTaskInfo),
             pInfo->loadInfo.totalRows, tstrerror(pInfo->rspCode));
      pTaskInfo->code = pInfo->rspCode;
      return NULL;
    }

    SRetrieveMetaTableRsp* pRsp = pInfo->pRsp;
    pInfo->req.showId = pRsp->handle;

    if (pRsp->numOfRows == 0 || pRsp->completed) {
      pOperator->status = OP_EXEC_DONE;
      qDebug("%s load meta data from mnode completed, rowsOfSource:%d, totalRows:%" PRIu64, GET_TASKID(pTaskInfo),
             pRsp->numOfRows, pInfo->loadInfo.totalRows);

      if (pRsp->numOfRows == 0) {
        taosMemoryFreeClear(pInfo->pRsp);
        return NULL;
      }
    }

    char* pStart = pRsp->data;
    code = extractDataBlockFromFetchRsp(pInfo->pRes, pRsp->data, pInfo->matchInfo.pList, &pStart, false);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      taosMemoryFreeClear(pInfo->pRsp);
      T_LONG_JMP(pTaskInfo->env, code);
    }
    updateLoadRemoteInfo(&pInfo->loadInfo, pRsp->numOfRows, pRsp->compLen, startTs, pOperator);
    // todo log the filter info
    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      taosMemoryFreeClear(pInfo->pRsp);
      T_LONG_JMP(pTaskInfo->env, code);
    }
    taosMemoryFreeClear(pInfo->pRsp);
    if (pInfo->pRes->info.rows > 0) {
      return pInfo->pRes;
    } else if (pOperator->status == OP_EXEC_DONE) {
      return NULL;
    }
  }
}

static int32_t resetSysTableScanOperState(SOperatorInfo* pOper) {
  SSysTableScanInfo* pInfo = pOper->info;

  SSystemTableScanPhysiNode* pScanPhyNode = (SSystemTableScanPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;
  blockDataEmpty(pInfo->pRes);

  if (pInfo->name.type == TSDB_TABLE_NAME_T) {
    const char* name = tNameGetTableName(&pInfo->name);
    if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_VC_COLS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_VIRTUAL_TABLES_REFERENCING, TSDB_TABLE_FNAME_LEN) == 0 ||
        pInfo->pCur != NULL) {
      if (pInfo->pAPI != NULL && pInfo->pAPI->metaFn.closeTableMetaCursor != NULL) {
        pInfo->pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      }

      pInfo->pCur = NULL;
    }
  } else {
    qError("pInfo->name is not initialized");
  }

  initLimitInfo(pScanPhyNode->scan.node.pLimit, pScanPhyNode->scan.node.pSlimit, &pInfo->limitInfo);
  pInfo->loadInfo.totalRows = 0;

  if (pScanPhyNode->scan.virtualStableScan) {
    SExecTaskInfo* pTaskInfo = pOper->pTaskInfo;
    tableListDestroy(pInfo->pSubTableListInfo);
    pInfo->pSubTableListInfo = tableListCreate();
    if (!pInfo->pSubTableListInfo) {
      pTaskInfo->code = terrno;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }

    bool savedDynamicOp = pScanPhyNode->scan.node.dynamicOp;
    pScanPhyNode->scan.node.dynamicOp = false;
    int32_t code = createScanTableListInfo((SScanPhysiNode*)pScanPhyNode, NULL, false, &pInfo->readHandle,
                                           pInfo->pSubTableListInfo, NULL, NULL, pTaskInfo, NULL);
    pScanPhyNode->scan.node.dynamicOp = savedDynamicOp;
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      tableListDestroy(pInfo->pSubTableListInfo);
      return code;
    }
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

  if (pInfo->pExtSchema) {
    taosHashCleanup(pInfo->pExtSchema);
    pInfo->pExtSchema = NULL;
  }
  pInfo->readHandle.mnd = NULL;
  destroySysTableScanVtbRefReqs(pInfo);

  return 0;
}

int32_t createSysTableScanOperatorInfo(void* readHandle, SSystemTableScanPhysiNode* pScanPhyNode,
                                       STableListInfo* pTableListInfo, const char* pUser, SExecTaskInfo* pTaskInfo,
                                       SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSysTableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SSysTableScanInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pScanPhyNode;
  SScanPhysiNode*     pScanNode = &pScanPhyNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t num = 0;
  code = extractColMatchInfo(pScanNode->pScanCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  extractTbnameSlotId(pInfo, pScanNode);

  pInfo->pAPI = &pTaskInfo->storageAPI;

  pInfo->accountId = pScanPhyNode->accountId;
  pInfo->pUser = taosStrdup((void*)pUser);
  QUERY_CHECK_NULL(pInfo->pUser, code, lino, _error, terrno);
  pInfo->privInfo = pScanPhyNode->privInfo;
  pInfo->sysInfo = pScanPhyNode->sysInfo;
  pInfo->showRewrite = pScanPhyNode->showRewrite;
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  pInfo->pCondition = pScanNode->node.pConditions;

  tNameAssign(&pInfo->name, &pScanNode->tableName);
  const char* name = tNameGetTableName(&pInfo->name);
  if (pInfo->showRewrite == false) {
    code = filterInitFromNode(pScanNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
  } else {
    if (strncasecmp(name, TSDB_INS_DISK_USAGE, TSDB_TABLE_FNAME_LEN) == 0) {
      pInfo->skipFilterTable = true;
      code = filterInitFromNode(NULL, &pOperator->exprSupp.pFilterInfo, 0, pTaskInfo->pStreamRuntimeInfo);
    } else {
      code = filterInitFromNode(pScanNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                                pTaskInfo->pStreamRuntimeInfo);
    }
  }
  QUERY_CHECK_CODE(code, lino, _error);

  initLimitInfo(pScanPhyNode->scan.node.pLimit, pScanPhyNode->scan.node.pSlimit, &pInfo->limitInfo);
  // since max column changed from 4096 -> 32767, we set the initial result size to 32K
  initResultSizeInfo(&pOperator->resultInfo, 32768);
  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_FILESETS, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TABLE_FIXED_DISTRIBUTED, TSDB_TABLE_FNAME_LEN) == 0) {
    pInfo->readHandle = *(SReadHandle*)readHandle;
    pInfo->epSet = pScanPhyNode->mgmtEpSet;
  } else {
    if (tsem_init(&pInfo->ready, 0, 0) != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_FAILED;
      goto _error;
    }
    pInfo->epSet = pScanPhyNode->mgmtEpSet;
    pInfo->readHandle = *(SReadHandle*)readHandle;

    // Register pInfo in the per-file ref pool so that loadSysTableCallback can
    // safely acquire/release it even after the operator has been destroyed.
    (void)taosThreadOnce(&sysTableScanRefPoolOnce, initSysTableScanRefPool);
    int64_t refId = taosAddRef(sysTableScanRefPool, pInfo);
    if (refId < 0) {
      qError("%s failed to add ref for sysTableScan since %s", GET_TASKID(pTaskInfo), tstrerror(terrno));
      code = terrno;
      goto _error;
    }
    pInfo->self = refId;
  }

  pInfo->pSubTableListInfo = pTableListInfo;

  setOperatorInfo(pOperator, "SysTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  if (pScanNode->node.dynamicOp) {
    pOperator->dynamicTask = true;
  }
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doSysTableScanNext, NULL, destroySysScanOperator,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetSysTableScanOperState);

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroySysScanOperator(pInfo);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
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

// doDestroySysTableScanInfo: actual teardown for SSysTableScanInfo.
// For operators that use the ref pool (MNode path, self > 0), this function
// is the pool destructor invoked automatically when the last ref is dropped
// via taosRemoveRef / taosReleaseRef — do NOT call it directly on those.
// For local-scan operators (self == 0), destroySysScanOperator calls it
// directly since there is no ref pool involved.
static void doDestroySysTableScanInfo(void* param) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)param;
  int32_t            code = tsem_destroy(&pInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  blockDataDestroy(pInfo->pRes);

  if (pInfo->name.type == TSDB_TABLE_NAME_T) {
    const char* name = tNameGetTableName(&pInfo->name);
    if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_VC_COLS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_VIRTUAL_TABLES_REFERENCING, TSDB_TABLE_FNAME_LEN) == 0 ||
        pInfo->pCur != NULL) {
      if (pInfo->pAPI != NULL && pInfo->pAPI->metaFn.closeTableMetaCursor != NULL) {
        pInfo->pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      }

      pInfo->pCur = NULL;
    }
  } else {
    qError("pInfo->name is not initialized");
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
  if (pInfo->pExtSchema) {
    taosHashCleanup(pInfo->pExtSchema);
    pInfo->pExtSchema = NULL;
  }
  destroySysTableScanVtbRefReqs(pInfo);
  tableListDestroy(pInfo->pSubTableListInfo);

  taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFreeClear(pInfo->pUser);

  taosMemoryFreeClear(param);
}

// destroySysScanOperator: operator destroy callback.  For operators that use
// the ref pool (MNode path), we just remove our reference — actual cleanup
// happens inside doDestroySysTableScanInfo when all refs are released.  For
// local-scan operators (no ref pool entry), do the teardown inline.
static void destroySysScanOperator(void* param) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)param;
  if (pInfo->self > 0) {
    // MNode path: remove the operator's own ref; the pool calls
    // doDestroySysTableScanInfo when all refs (operator + any in-flight
    // callbacks) are dropped.
    int32_t refCode = taosRemoveRef(sysTableScanRefPool, pInfo->self);
    if (refCode != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(refCode));
    }
  } else {
    // Local scan path: no ref pool — destroy directly.
    doDestroySysTableScanInfo(pInfo);
  }
}

static int32_t loadSysTableCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SSysTableScanCbParam* pWrapper = (SSysTableScanCbParam*)param;

  // Acquire the SSysTableScanInfo from the ref pool.  If it returns NULL the
  // operator has already been destroyed — discard the response safely.
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)taosAcquireRef(sysTableScanRefPool, pWrapper->sysTableScanId);
  if (pInfo == NULL) {
    // Operator is gone; free the response payload and bail out.
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pInfo->pRsp = pMsg->pData;

    SRetrieveMetaTableRsp* pRsp = pInfo->pRsp;
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->handle = htobe64(pRsp->handle);
    pRsp->compLen = htonl(pRsp->compLen);
  } else {
    int32_t cvtCode = rpcCvtErrCode(code);
    if (cvtCode != code) {
      qError("load systable rsp received, error:%s, cvted error:%s", tstrerror(code), tstrerror(cvtCode));
    } else {
      qError("load systable rsp received, error:%s", tstrerror(code));
    }
    pInfo->rspCode = cvtCode;
    taosMemoryFree(pMsg->pData);
  }
  taosMemoryFree(pMsg->pEpSet);

  // Release our acquired ref BEFORE posting the semaphore.
  // If we post first, the waiter can race ahead: task completes → taosRemoveRef
  // drops the count to 1, then doDestroyTask frees the task memory pool (which
  // owns pInfo).  Our subsequent taosReleaseRef would then drop the count to 0
  // and call doDestroySysTableScanInfo on already-freed memory.
  // By releasing first (count 2→1, destructor not triggered), pInfo remains
  // valid for the tsem_post call below, and doDestroySysTableScanInfo is
  // called only later, inside destroySysScanOperator, when pInfo is still live.
  int32_t refCode = taosReleaseRef(sysTableScanRefPool, pWrapper->sysTableScanId);
  if (refCode != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(refCode));
  }

  int32_t res = tsem_post(&pInfo->ready);
  if (res != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(res));
  }
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

int32_t optSysIntersection(SArray* in, SArray* out) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  MergeIndex* mi = NULL;
  int32_t     sz = (int32_t)taosArrayGetSize(in);
  if (sz <= 0) {
    goto _end;
  }
  mi = taosMemoryCalloc(sz, sizeof(MergeIndex));
  QUERY_CHECK_NULL(mi, code, lino, _end, terrno);
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
      void* tmp = taosArrayPush(out, &tgt);
      if (!tmp) {
        code = terrno;
        goto _end;
      }
    }
  }

_end:
  taosMemoryFreeClear(mi);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  return optSysIntersection(mRslt, rslt);
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
  int ret = TSDB_CODE_FAILED;
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
  if (!mRslt) {
    return terrno;
  }

  SListCell* cell = pList->pHead;
  for (int i = 0; i < len; i++) {
    if (cell == NULL) break;

    SArray* aRslt = taosArrayInit(16, sizeof(int64_t));
    if (!aRslt) {
      return terrno;
    }

    ret = optSysTabFilteImpl(arg, cell->pNode, aRslt);
    if (ret == 0) {
      // has index
      hasIdx = true;
      if (optSysSpecialColumn(cell->pNode) == 0) {
        void* tmp = taosArrayPush(mRslt, &aRslt);
        if (!tmp) {
          return TSDB_CODE_FAILED;
        }
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
    int32_t code = optSysMergeRslt(mRslt, result);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
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
      if (nodeType(pOper->pLeft) == QUERY_NODE_COLUMN && 0 == strcmp(pCol->colName, "create_time")) {
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
  pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, META_READER_LOCK, &pHandle->api.metaFn);
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

static int32_t doBlockInfoScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SBlockDistInfo* pBlockScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;

  STableBlockDistInfo blockDistInfo = {.minRows = INT_MAX, .maxRows = INT_MIN};
  code = doGetTableRowSize(&pBlockScanInfo->readHandle, pBlockScanInfo->uid, (int32_t*)&blockDistInfo.rowSize,
                           GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _end);

  code = pAPI->tsdReader.tsdReaderGetDataBlockDistInfo(pBlockScanInfo->pHandle, &blockDistInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = (int32_t)pAPI->tsdReader.tsdReaderGetNumOfInMemRows(pBlockScanInfo->pHandle, &blockDistInfo.numOfInmemRows);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* pBlock = pBlockScanInfo->pResBlock;

  int32_t          slotId = pOperator->exprSupp.pExprInfo->base.resSchema.slotId;
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, slotId);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  int32_t len = tSerializeBlockDistInfo(NULL, 0, &blockDistInfo);
  char*   p = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  int32_t tempRes = tSerializeBlockDistInfo(varDataVal(p), len, &blockDistInfo);
  if (tempRes < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  varDataSetLen(p, len);

  code = colDataSetVal(pColInfo, 0, p, false);
  QUERY_CHECK_CODE(code, lino, _end);

  taosMemoryFree(p);

  // make the valgrind happy that all memory buffer has been initialized already.
  if (slotId != 0) {
    SColumnInfoData* p1 = taosArrayGet(pBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(p1, code, lino, _end, terrno);
    int64_t v = 0;
    colDataSetInt64(p1, 0, &v);
  }

  pBlock->info.rows = 1;
  pOperator->status = OP_EXEC_DONE;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = pBlock;
  return code;
}

static void destroyBlockDistScanOperatorInfo(void* param) {
  SBlockDistInfo* pDistInfo = (SBlockDistInfo*)param;
  blockDataDestroy(pDistInfo->pResBlock);
  if (pDistInfo->readHandle.api.tsdReader.tsdReaderClose != NULL) {
    pDistInfo->readHandle.api.tsdReader.tsdReaderClose(pDistInfo->pHandle);
  }
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
    taosMemoryFree(pCond->colList);
    taosMemoryFree(pCond->pSlotList);
    return terrno;
  }

  pCond->colList->colId = 1;
  pCond->colList->type = TSDB_DATA_TYPE_TIMESTAMP;
  pCond->colList->bytes = sizeof(TSKEY);
  pCond->colList->pk = 0;

  pCond->pSlotList[0] = 0;

  pCond->twindows = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pCond->suid = uid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;

  return TSDB_CODE_SUCCESS;
}

int32_t createDataBlockInfoScanOperator(SReadHandle* readHandle, SBlockDistScanPhysiNode* pBlockScanNode,
                                        STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t         code = 0;
  int32_t         lino = 0;
  SBlockDistInfo* pInfo = taosMemoryCalloc(1, sizeof(SBlockDistInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pInfo->pResBlock = createDataBlockFromDescNode(pBlockScanNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pResBlock, code, lino, _error, terrno);
  code = blockDataEnsureCapacity(pInfo->pResBlock, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  {
    SQueryTableDataCond cond = {0};
    code = initTableblockDistQueryCond(pBlockScanNode->suid, &cond);
    QUERY_CHECK_CODE(code, lino, _error);

    pInfo->pTableListInfo = pTableListInfo;
    int32_t num = 0;
    code = tableListGetSize(pTableListInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    void* pList = tableListGetInfo(pTableListInfo, 0);

    code = readHandle->api.tsdReader.tsdReaderOpen(readHandle->vnode, &cond, pList, num, pInfo->pResBlock,
                                                   (void**)&pInfo->pHandle, pTaskInfo->id.str, NULL);
    cleanupQueryTableDataCond(&cond);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pInfo->readHandle = *readHandle;
  pInfo->uid = (pBlockScanNode->suid != 0) ? pBlockScanNode->suid : pBlockScanNode->uid;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pBlockScanNode->pScanPseudoCols, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfCols, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "DataBlockDistScanOperator", QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doBlockInfoScanNext, NULL, destroyBlockDistScanOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo) {
    pInfo->pTableListInfo = NULL;
    destroyBlockDistScanOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}

static int32_t buildTableListInfo(SOperatorInfo* pOperator, STableId* id, STableListInfo** ppTableListInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            line = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SReadHandle*       pReadHandle = &pInfo->readHandle;
  SArray*            pList = NULL;

  STableListInfo* pTableListInfo = tableListCreate();
  QUERY_CHECK_NULL(ppTableListInfo, code, line, _end, terrno);

  if (id->type == TSDB_SUPER_TABLE) {
    pList = taosArrayInit(4, sizeof(uint64_t));
    QUERY_CHECK_NULL(pList, code, line, _end, terrno);

    code = pReadHandle->api.metaFn.getChildTableList(pReadHandle->vnode, id->uid, pList);
    QUERY_CHECK_CODE(code, line, _end);

    size_t num = taosArrayGetSize(pList);
    for (int32_t i = 0; i < num; ++i) {
      uint64_t* id = taosArrayGet(pList, i);
      if (id == NULL) {
        continue;
      }
      code = tableListAddTableInfo(pTableListInfo, *id, 0);
      QUERY_CHECK_CODE(code, line, _end);
    }
    taosArrayDestroy(pList);
    pList = NULL;

  } else if (id->type == TSDB_NORMAL_TABLE) {
    code = tableListAddTableInfo(pTableListInfo, id->uid, 0);
    QUERY_CHECK_CODE(code, line, _end);
  }
  *ppTableListInfo = pTableListInfo;
  return code;
_end:
  taosArrayDestroy(pList);
  tableListDestroy(pTableListInfo);
  return code;
}
static int32_t vnodeEstimateDataSizeByUid(SOperatorInfo* pOperator, STableId* id, SDbSizeStatisInfo* pStaticInfo) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             line = 0;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*        pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo*  pInfo = pOperator->info;
  SQueryTableDataCond cond = {0};

  SReadHandle* pReadHandle = &pInfo->readHandle;

  STableListInfo* pTableListInfo = NULL;
  code = buildTableListInfo(pOperator, id, &pTableListInfo);
  QUERY_CHECK_CODE(code, line, _end);

  tb_uid_t tbId = id->type == TSDB_SUPER_TABLE ? id->uid : 0;

  code = initTableblockDistQueryCond(tbId, &cond);
  QUERY_CHECK_CODE(code, line, _end);

  pInfo->pTableListInfo = pTableListInfo;

  int32_t num = 0;
  code = tableListGetSize(pTableListInfo, &num);
  QUERY_CHECK_CODE(code, line, _end);

  void* pList = tableListGetInfo(pTableListInfo, 0);

  code = pReadHandle->api.tsdReader.tsdReaderOpen(pReadHandle->vnode, &cond, pList, num, NULL, (void**)&pInfo->pHandle,
                                                  pTaskInfo->id.str, NULL);
  cleanupQueryTableDataCond(&cond);
  QUERY_CHECK_CODE(code, line, _end);

  STableBlockDistInfo blockDistInfo = {.minRows = INT_MAX, .maxRows = INT_MIN};
  code = doGetTableRowSize(pReadHandle, id->uid, (int32_t*)&blockDistInfo.rowSize, GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, line, _end);

  code = pAPI->tsdReader.tsdReaderGetDataBlockDistInfo(pInfo->pHandle, &blockDistInfo);
  QUERY_CHECK_CODE(code, line, _end);

  code = pAPI->tsdReader.tsdReaderGetNumOfInMemRows(pInfo->pHandle, &blockDistInfo.numOfInmemRows);
  QUERY_CHECK_CODE(code, line, _end);

  int64_t rawDiskSize = 0, rawCacheSize = 0;
  rawDiskSize = (blockDistInfo.totalRows + blockDistInfo.numOfSttRows) * blockDistInfo.rowSize;
  rawCacheSize = blockDistInfo.numOfInmemRows * blockDistInfo.rowSize;
  pStaticInfo->rawDataSize += rawDiskSize;
  pStaticInfo->cacheSize += rawCacheSize;

  if (pInfo->pHandle != NULL) {
    pReadHandle->api.tsdReader.tsdReaderClose(pInfo->pHandle);
    pInfo->pHandle = NULL;
  }

  tableListDestroy(pInfo->pTableListInfo);
  pInfo->pTableListInfo = NULL;
  return code;
_end:

  if (pInfo->pHandle != NULL) {
    pReadHandle->api.tsdReader.tsdReaderClose(pInfo->pHandle);
    pInfo->pHandle = NULL;
  }

  tableListDestroy(pInfo->pTableListInfo);
  pInfo->pTableListInfo = NULL;
  if (code) {
    pTaskInfo->code = code;
    return code;
  }
  cleanupQueryTableDataCond(&cond);
  return code;
}

static int32_t vnodeEstimateRawDataSizeImpl(SOperatorInfo* pOperator, SArray* pTableList,
                                            SDbSizeStatisInfo* pStaticInfo) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;

  int32_t rowLen = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < taosArrayGetSize(pTableList); i++) {
    STableId* id = (STableId*)taosArrayGet(pTableList, i);
    code = vnodeEstimateDataSizeByUid(pOperator, id, pStaticInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return code;
}

static int32_t vnodeEstimateRawDataSize(SOperatorInfo* pOperator, SDbSizeStatisInfo* pStaticInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  int32_t line = 0;

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;
  int32_t            ret = 0;
  if (pStaticInfo->estimateRawData == 0) {
    pStaticInfo->rawDataSize = 0;
    return code;
  }

  if (pStaticInfo->estimateRawData == 0) {
    pStaticInfo->rawDataSize = 0;
    return code;
  }

  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    if (pInfo->pCur == NULL) {
      TAOS_CHECK_GOTO(terrno, &line, _exit);
    }
  }

  SArray* pIdList = taosArrayInit(16, sizeof(STableId));
  if (pIdList == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _exit);
  }

  while (((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_CHILD_TABLE)) == 0)) {
    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      STableId id = {.type = TSDB_SUPER_TABLE, .uid = pInfo->pCur->mr.me.uid};
      if (taosArrayPush(pIdList, &id) == NULL) {
        TAOS_CHECK_GOTO(terrno, &line, _exit);
      }
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      STableId id = {.type = TSDB_NORMAL_TABLE, .uid = pInfo->pCur->mr.me.uid};
      if (taosArrayPush(pIdList, &id) == NULL) {
        TAOS_CHECK_GOTO(terrno, &line, _exit);
      }
    }
  }
  if (pInfo->pCur) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
  }

  code = vnodeEstimateRawDataSizeImpl(pOperator, pIdList, pStaticInfo);

_exit:
  if (pInfo->pCur) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
    pTaskInfo->code = code;
  }

  taosArrayDestroy(pIdList);
  return code;
}
