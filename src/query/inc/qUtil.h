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
#ifndef TDENGINE_QUERYUTIL_H
#define TDENGINE_QUERYUTIL_H

void clearTimeWindowResBuf(SQueryRuntimeEnv* pRuntimeEnv, SWindowResult* pOneOutputRes);
void copyTimeWindowResBuf(SQueryRuntimeEnv* pRuntimeEnv, SWindowResult* dst, const SWindowResult* src);

int32_t initWindowResInfo(SWindowResInfo* pWindowResInfo, SQueryRuntimeEnv* pRuntimeEnv, int32_t size,
                          int32_t threshold, int16_t type);

void    cleanupTimeWindowInfo(SWindowResInfo* pWindowResInfo, int32_t numOfCols);
void    resetTimeWindowInfo(SQueryRuntimeEnv* pRuntimeEnv, SWindowResInfo* pWindowResInfo);
void    clearFirstNTimeWindow(SQueryRuntimeEnv *pRuntimeEnv, int32_t num);

void    clearClosedTimeWindow(SQueryRuntimeEnv* pRuntimeEnv);
int32_t numOfClosedTimeWindow(SWindowResInfo* pWindowResInfo);
void    closeTimeWindow(SWindowResInfo* pWindowResInfo, int32_t slot);
void    closeAllTimeWindow(SWindowResInfo* pWindowResInfo);
void    removeRedundantWindow(SWindowResInfo *pWindowResInfo, TSKEY lastKey, int32_t order);
SWindowResult *getWindowResult(SWindowResInfo *pWindowResInfo, int32_t slot);

#define curTimeWindow(_winres)  ((_winres)->curIndex)
bool isWindowResClosed(SWindowResInfo *pWindowResInfo, int32_t slot);

void createQueryResultInfo(SQuery *pQuery, SWindowResult *pResultRow, bool isSTableQuery, SPosInfo *posInfo);

char *getPosInResultPage(SQueryRuntimeEnv *pRuntimeEnv, int32_t columnIndex, SWindowResult *pResult);

__filter_func_t *getRangeFilterFuncArray(int32_t type);
__filter_func_t *getValueFilterFuncArray(int32_t type);

bool supportPrefilter(int32_t type);

#endif  // TDENGINE_QUERYUTIL_H
