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
  STMT_TYPE_QUERY,
} STMT_TYPE;

typedef struct STscStmt {
  STMT_TYPE type;
  //int16_t  last;
  //STscObj* taos;
  //SSqlObj* pSql;
  //SMultiTbStmt mtb;
  //SNormalStmt normal;

  //int numOfRows;
} STscStmt;

#define STMT_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define STMT_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define STMT_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

TAOS_STMT *stmtInit(TAOS *taos);
int stmtClose(TAOS_STMT *stmt);
int stmtExec(TAOS_STMT *stmt);
char *stmtErrstr(TAOS_STMT *stmt);
int stmtAffectedRows(TAOS_STMT *stmt);
int stmtBind(TAOS_STMT *stmt, TAOS_BIND *bind);
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
