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

#ifndef TDENGINE_TSCSUBQUERY_H
#define TDENGINE_TSCSUBQUERY_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tscUtil.h"
#include "tsclient.h"

void tscFetchDatablockForSubquery(SSqlObj* pSql);

void tscSetupOutputColumnIndex(SSqlObj* pSql);
void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code);

SJoinSupporter* tscCreateJoinSupporter(SSqlObj* pSql, int32_t index);

void tscHandleMasterJoinQuery(SSqlObj* pSql);

int32_t tscHandleMasterSTableQuery(SSqlObj *pSql);

int32_t tscHandleMultivnodeInsert(SSqlObj *pSql);

int32_t tscHandleInsertRetry(SSqlObj* parent, SSqlObj* child);

void tscBuildResFromSubqueries(SSqlObj *pSql);
TAOS_ROW doSetResultRowData(SSqlObj *pSql);

char *getArithmeticInputSrc(void *param, const char *name, int32_t colId);

void tscLockByThread(int64_t *lockedBy);

void tscUnlockByThread(int64_t *lockedBy);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCSUBQUERY_H
