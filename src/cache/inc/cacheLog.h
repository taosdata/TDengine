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

#ifndef TDENGINE_CACHE_LOG_H
#define TDENGINE_CACHE_LOG_H

#include "tlog.h"

extern int32_t cacheDebugFlag;

#define cacheFatal(...) do { if (cacheDebugFlag & DEBUG_FATAL) { taosPrintLog("CAE FATAL ", 255, __VA_ARGS__); }}     while(0)
#define cacheError(...) do { if (cacheDebugFlag & DEBUG_ERROR) { taosPrintLog("CAE ERROR ", 255, __VA_ARGS__); }}     while(0)
#define cacheWarn(...)  do { if (cacheDebugFlag & DEBUG_WARN)  { taosPrintLog("CAE WARN ", 255, __VA_ARGS__); }}      while(0)
#define cacheInfo(...)  do { if (cacheDebugFlag & DEBUG_INFO)  { taosPrintLog("CAE ", 255, __VA_ARGS__); }}           while(0)
#define cacheDebug(...) do { if (cacheDebugFlag & DEBUG_DEBUG) { taosPrintLog("CAE ", cacheDebugFlag, __VA_ARGS__); }} while(0)
#define cacheTrace(...) do { if (cacheDebugFlag & DEBUG_TRACE) { taosPrintLog("CAE ", cacheDebugFlag, __VA_ARGS__); }} while(0)

#endif /* TDENGINE_CACHE_LOG_H */