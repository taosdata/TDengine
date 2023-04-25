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
  int32_t  sBindRowNum;
  int32_t  sBindLastIdx;
  int8_t   tbType;
  bool     tagsCached;
  void    *boundTags;
  char     tbName[TSDB_TABLE_FNAME_LEN];
  char     tbFName[TSDB_TABLE_FNAME_LEN];
  char     stbFName[TSDB_TABLE_FNAME_LEN];
  SName    sname;
} SStmtBindInfo;

typedef struct SStmtExecInfo {
  int32_t        affectedRows;
  SRequestObj   *pRequest;
  SHashObj      *pBlockHash;
  STableDataCxt *pCurrBlock;
  SSubmitTbData *pCurrTbData;
} SStmtExecInfo;

typedef struct SStmtSQLInfo {
  STMT_TYPE         type;
  STMT_STATUS       status;
  uint64_t          runTimes;
  SHashObj         *pTableCache;  // SHash<SStmtTableCache>
  SQuery           *pQuery;
  char             *sqlStr;
  int32_t           sqlLen;
  SArray           *nodeList;
  SStmtQueryResInfo queryRes;
  bool              autoCreateTbl;
  SHashObj         *pVgHash;
} SStmtSQLInfo;

typedef struct STscStmt {
  STscObj  *taos;
  SCatalog *pCatalog;
  int32_t   affectedRows;
  uint32_t  seqId;
  uint32_t  seqIds[STMT_MAX];

  SStmtSQLInfo  sql;
  SStmtExecInfo exec;
  SStmtBindInfo bInfo;

  int64_t reqid;
  int32_t errCode;
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


#define STMT_ELOG(param, ...) qError("stmt:%p " param, pStmt, __VA_ARGS__)
#define STMT_DLOG(param, ...) qDebug("stmt:%p " param, pStmt, __VA_ARGS__)

#define STMT_ELOG_E(param) qError("stmt:%p " param, pStmt)
#define STMT_DLOG_E(param) qDebug("stmt:%p " param, pStmt)

TAOS_STMT  *stmtInit(STscObj *taos, int64_t reqid);
int         stmtClose(TAOS_STMT *stmt);
int         stmtExec(TAOS_STMT *stmt);
const char *stmtErrstr(TAOS_STMT *stmt);
int         stmtAffectedRows(TAOS_STMT *stmt);
int         stmtAffectedRowsOnce(TAOS_STMT *stmt);
int         stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
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

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTSTMT_H
