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

#ifndef TDENGINE_CLIENTSTMT_H
#define TDENGINE_CLIENTSTMT_H

#ifdef __cplusplus
extern "C" {
#endif
#include "catalog.h"

typedef enum {
  STMT_TYPE_INSERT = 1,
  STMT_TYPE_MULTI_INSERT,
  STMT_TYPE_QUERY,
} STMT_TYPE;

typedef enum {
  STMT_INIT = 1,
  STMT_PREPARE,
  STMT_SETTBNAME,
  STMT_SETTAGS,
  STMT_FETCH_FIELDS,
  STMT_BIND,
  STMT_BIND_COL,
  STMT_ADD_BATCH,
  STMT_EXECUTE,
  STMT_MAX,
} STMT_STATUS;

#define STMT_TABLE_COLS_NUM 1000 

typedef struct SStmtTableCache {
  STableDataCxt *pDataCtx;
  void          *boundTags;
} SStmtTableCache;

typedef struct SStmtQueryResInfo {
  TAOS_FIELD *fields;
  TAOS_FIELD *userFields;
  uint32_t    numOfCols;
  int32_t     precision;
} SStmtQueryResInfo;

typedef struct SStmtBindInfo {
  bool     needParse;
  bool     inExecCache;
  uint64_t tbUid;
  uint64_t tbSuid;
  int32_t  tbVgId;
  int32_t  sBindRowNum;
  int32_t  sBindLastIdx;
  int8_t   tbType;
  bool     tagsCached;
  void    *boundTags;
  char     tbName[TSDB_TABLE_FNAME_LEN];
  char     tbFName[TSDB_TABLE_FNAME_LEN];
  char     stbFName[TSDB_TABLE_FNAME_LEN];
  SName    sname;

  char     statbName[TSDB_TABLE_FNAME_LEN];
} SStmtBindInfo;

typedef struct SStmtAsyncParam {
  STableColsData *pTbData;
  void*           pStmt;
} SStmtAsyncParam;

typedef struct SStmtExecInfo {
  int32_t        affectedRows;
  SRequestObj   *pRequest;
  SHashObj      *pBlockHash;
  STableDataCxt *pCurrBlock;
  SSubmitTbData *pCurrTbData;
} SStmtExecInfo;

typedef struct SStmtAPI2 {
  int               questions; // # of question-marks found by `qScanSql`

  int               nr_tags;   // # of tags
  int               nr_cols;   // # of cols

  TAOS_FIELD_E     *params;        // local cache for param-meta-info
  size_t            cap_params;

  TAOS_MULTI_BIND  *mbs_from_app;
  int               nr_mbs_from_app;

  TAOS_MULTI_BIND  *mbs_cache;     // used during batch submit
  size_t            cap_mbs;

  char             *conv_buf;      // used during param conversion
  size_t            cap_conv_buf;

  TAOS_RES         *res_from_taos_query;

  uint8_t           scanned:1;         // set after `qScanSql`
  uint8_t           is_insert:1;
  uint8_t           tbname_required:1;

  // NOTE: only valid when tbname_required is set
  // NOTE: bypass tag-related routines when it's set
  uint8_t           without_using_clause:1;

  uint8_t           prepared:1;        // set when taos_stmt_prepare2 succeed

  uint8_t           use_res_from_taos_query:1;
} SStmtAPI2;

typedef struct SStmtSQLInfo {
  bool              stbInterlaceMode;
  STMT_TYPE         type;
  STMT_STATUS       status;
  uint64_t          suid; 
  uint64_t          runTimes;
  SHashObj         *pTableCache;  // SHash<SStmtTableCache>
  SQuery           *pQuery;
  char             *sqlStr;
  int32_t           sqlLen;
  SArray           *nodeList;
  SStmtQueryResInfo queryRes;
  bool              autoCreateTbl;
  SHashObj         *pVgHash;
  SBindInfo        *pBindInfo;

  SStbInterlaceInfo siInfo;
} SStmtSQLInfo;

typedef struct SStmtStatInfo {
  int64_t ctgGetTbMetaNum;
  int64_t getCacheTbInfo;
  int64_t parseSqlNum;
  int64_t bindDataNum;
  int64_t setTbNameUs;
  int64_t bindDataUs1;
  int64_t bindDataUs2;
  int64_t bindDataUs3;
  int64_t bindDataUs4;
  int64_t addBatchUs;
  int64_t execWaitUs;
  int64_t execUseUs;
} SStmtStatInfo;

typedef struct SStmtQNode {
  bool                 restoreTbCols;
  STableColsData       tblData;
  struct SStmtQNode*   next;
} SStmtQNode;

typedef struct SStmtQueue {
  bool        stopQueue;
  SStmtQNode* head;
  SStmtQNode* tail;
  uint64_t    qRemainNum;
} SStmtQueue;

typedef enum {
  STMT_API_UNKNOWN      = 0,
  STMT_API_PREPARE,
  STMT_API_PREPARE2,
} STMT_API_TYPE;

typedef struct STscStmt {
  STscObj          *taos;
  SCatalog         *pCatalog;
  int32_t           affectedRows;
  uint32_t          seqId;
  uint32_t          seqIds[STMT_MAX];
  bool              bindThreadInUse;
  TdThread          bindThread;
  TAOS_STMT_OPTIONS options;
  bool              stbInterlaceMode;
  SStmtQueue        queue;

  SStmtSQLInfo      sql;
  SStmtExecInfo     exec;
  SStmtBindInfo     bInfo;

  int64_t           reqid;
  int32_t           errCode;

  SStmtStatInfo     stat;

  STMT_API_TYPE     api_type;
  SStmtAPI2         api2;
} STscStmt;

extern char *gStmtStatusStr[];

#define STMT_LOG_SEQ(n)                                                                 \
  do {                                                                                  \
    (pStmt)->seqId++;                                                                   \
    (pStmt)->seqIds[n]++;                                                               \
    STMT_DLOG("the %dth:%d %s", (pStmt)->seqIds[n], (pStmt)->seqId, gStmtStatusStr[n]); \
  } while (0)

#define STMT_STATUS_NE(S) (pStmt->sql.status != STMT_##S)
#define STMT_STATUS_EQ(S) (pStmt->sql.status == STMT_##S)

#define STMT_ERR_RET(c)               \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      pStmt->errCode = _code;         \
      return _code;                   \
    }                                 \
  } while (0)
#define STMT_RET(c)                   \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      pStmt->errCode = _code;         \
    }                                 \
    return _code;                     \
  } while (0)
#define STMT_ERR_JRET(c)             \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      pStmt->errCode = code;         \
      goto _return;                  \
    }                                \
  } while (0)
#define STMT_ERRI_JRET(c)            \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)
#define STMT_ERR_RET_IMUTABLY(c)      \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)


#define STMT_FLOG(param, ...) qFatal("stmt:%p " param, pStmt, __VA_ARGS__)
#define STMT_ELOG(param, ...) qError("stmt:%p " param, pStmt, __VA_ARGS__)
#define STMT_DLOG(param, ...) qDebug("stmt:%p " param, pStmt, __VA_ARGS__)

#define STMT_ELOG_E(param) qError("stmt:%p " param, pStmt)
#define STMT_DLOG_E(param) qDebug("stmt:%p " param, pStmt)

TAOS_STMT  *stmtInit(STscObj* taos, int64_t reqid, TAOS_STMT_OPTIONS* pOptions);
int         stmtClose(TAOS_STMT *stmt);
int         stmtExec(TAOS_STMT *stmt);
const char *stmtErrstr(TAOS_STMT *stmt);
int         stmtAffectedRows(TAOS_STMT *stmt);
int         stmtAffectedRowsOnce(TAOS_STMT *stmt);
int         stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length, STMT_API_TYPE api_type);
int         stmtSetTbName(TAOS_STMT *stmt, const char *tbName);
int         stmtSetTbTags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags);
int         stmtGetTagFields(TAOS_STMT *stmt, int *nums, TAOS_FIELD_E **fields);
int         stmtGetColFields(TAOS_STMT *stmt, int *nums, TAOS_FIELD_E **fields);
int         stmtIsInsert(TAOS_STMT *stmt, int *insert);
int         stmtGetParamNum(TAOS_STMT *stmt, int *nums);
int         stmtGetParam(TAOS_STMT *stmt, int idx, int *type, int *bytes);
int         stmtAddBatch(TAOS_STMT *stmt);
TAOS_RES   *stmtUseResult(TAOS_STMT *stmt);
int         stmtBindBatch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int32_t colIdx);

int         stmtPrepare2(TAOS_STMT *stmt, const char *sql, unsigned long length);
int         stmtPostPrepare2(TAOS_STMT *stmt);
int         stmtGetParams2(TAOS_STMT *stmt, TAOS_FIELD_E *params, int nr_params, int *nr_real);
int         stmtBindParams2(TAOS_STMT *stmt, TAOS_MULTI_BIND *mbs, int nr_mbs);

int stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length, STMT_API_TYPE api_type);
int stmt_num_params(TAOS_STMT *stmt, int *nums);
int stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags);
int stmt_set_tbname(TAOS_STMT *stmt, const char *name);
int stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags);
int stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
int stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
int stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
int stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
int stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx);
int stmt_add_batch(TAOS_STMT *stmt);

TAOS_RES* taosQueryImplWithConnId(int64_t connId, const char* sql, bool validateOnly, int8_t source);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTSTMT_H
