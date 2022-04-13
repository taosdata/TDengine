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
  int16_t  last;
  STscObj* taos;
  SSqlObj* pSql;
  SMultiTbStmt mtb;
  SNormalStmt normal;

  int numOfRows;
} STscStmt;

#define SCH_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define SCH_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define SCH_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTSTMT_H
