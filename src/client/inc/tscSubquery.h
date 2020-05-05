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

#ifndef TDENGINE_TSCJOINPROCESS_H
#define TDENGINE_TSCJOINPROCESS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tscUtil.h"
#include "tsclient.h"

void tscFetchDatablockFromSubquery(SSqlObj* pSql);

void tscSetupOutputColumnIndex(SSqlObj* pSql);
int32_t tscLaunchSecondPhaseSubqueries(SSqlObj* pSql);
void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code);

SJoinSupporter* tscCreateJoinSupporter(SSqlObj* pSql, SSubqueryState* pState, int32_t index);
void tscDestroyJoinSupporter(SJoinSupporter* pSupporter);

int32_t tscHandleMasterJoinQuery(SSqlObj* pSql);

int32_t tscHandleMasterSTableQuery(SSqlObj *pSql);

int32_t tscHandleMultivnodeInsert(SSqlObj *pSql);

void tscBuildResFromSubqueries(SSqlObj *pSql);
void **doSetResultRowData(SSqlObj *pSql, bool finalResult);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCJOINPROCESS_H
