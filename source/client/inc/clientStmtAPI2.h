
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

#ifndef TDENGINE_CLIENTSTMTAPI2_H
#define TDENGINE_CLIENTSTMTAPI2_H

#include "clientStmt.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DBGE(fmt, ...) if (0) fprintf(stderr, "%s[%d]:%s():" fmt "\n", __FILE__, __LINE__, __func__, ##__VA_ARGS__)

void stmtExec2ClearRes(STscStmt *pStmt);
int stmtKeepMbs(STscStmt *pStmt, int nr);
void stmtReleasePrepareInfo2(STscStmt *pStmt);
void stmtUnbindParams2(STscStmt *pStmt);
int stmtReallocParams(TAOS_STMT *stmt);

int stmtGetParamNumInternal(TAOS_STMT* stmt, int* nums);
int stmtGetParam2(TAOS_STMT* stmt, int idx, int* type, int* bytes);

int stmtDoExec1(TAOS_STMT* stmt, int64_t *affectedRows);
int stmtExec1(TAOS_STMT* stmt);
int stmtExec2(TAOS_STMT *stmt);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTSTMTAPI2_H

