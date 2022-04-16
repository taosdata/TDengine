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

typedef enum {
  STMT_TYPE_INSERT = 1,
  STMT_TYPE_MULTI_INSERT,
  STMT_TYPE_QUERY
} STMT_TYPE;

typedef enum {
  STMT_INIT = 1,
  STMT_PREPARE,
  STMT_SETTBNAME,
  STMT_FETCH_TAG_FIELDS,
  STMT_FETCH_COL_FIELDS,
  STMT_BIND,
  STMT_BIND_COL,
  STMT_ADD_BATCH,
  STMT_EXECUTE
} STMT_STATUS;

typedef struct SStmtTableCache {
  STableDataBlocks* pDataBlock;
  void*             boundTags;
} SStmtTableCache;

typedef struct SStmtBindInfo {
  bool         needParse;
  uint64_t     tbUid;
  uint64_t     tbSuid;
  int8_t       tbType;
  void*        boundTags;  
  char*        tbName;
  SName        sname;
} SStmtBindInfo;

typedef struct SStmtExecInfo {
  SRequestObj* pRequest;
  SHashObj*    pVgHash;
  SHashObj*    pBlockHash;
} SStmtExecInfo;

typedef struct SStmtSQLInfo {
  STMT_TYPE    type;
  STMT_STATUS  status;
  bool         autoCreate;
  uint64_t     runTimes;
  SHashObj*    pTableCache;   //SHash<SStmtTableCache>
  SQuery*      pQuery;
  char*        sqlStr;
  int32_t      sqlLen;
} SStmtSQLInfo;

typedef struct STscStmt {
  STscObj*      taos;
  SCatalog*     pCatalog;

  SStmtSQLInfo  sql;
  SStmtExecInfo exec;
  SStmtBindInfo bind;
} STscStmt;


#define STMT_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define STMT_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define STMT_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define STMT_SWITCH_STATUS(_stmt, _newstatus, _errcode)               \
  do {                                                                \
    switch (_newstatus) {                                             \
      case STMT_INIT:                                                 \
        if ((_stmt)->sql.status != 0) return (_errcode);              \
        break;                                                        \
      case STMT_PREPARE:                                              \
        if ((_stmt)->sql.status != STMT_INIT) STMT_ERR_RET(_errcode); \
        break;                                                        \
      case STMT_SETTBNAME:                                            \
        break;                                                        \
      default:                                                        \
        STMT_ERR_RET(_errcode);                                       \
        break;                                                        \
    }                                                                 \
                                                                      \
    (_stmt)->sql.status = _newstatus;                                 \
  } while (0)


TAOS_STMT *stmtInit(TAOS *taos);
int stmtClose(TAOS_STMT *stmt);
int stmtExec(TAOS_STMT *stmt);
char *stmtErrstr(TAOS_STMT *stmt);
int stmtAffectedRows(TAOS_STMT *stmt);
int stmtPrepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
int stmtSetTbNameTags(TAOS_STMT *stmt, const char *name, TAOS_BIND *tags);
int stmtIsInsert(TAOS_STMT *stmt, int *insert);
int stmtGetParamNum(TAOS_STMT *stmt, int *nums);
int stmtAddBatch(TAOS_STMT *stmt);
TAOS_RES *stmtUseResult(TAOS_STMT *stmt);
int stmtBindBatch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTSTMT_H
