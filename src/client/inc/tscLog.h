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

#ifndef TDENGINE_TSCLOG_H
#define TDENGINE_TSCLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern uint32_t cDebugFlag;
extern int8_t  tscEmbedded;

#define tscFatal(...)  do { if (cDebugFlag & DEBUG_FATAL) { taosPrintLog("TSC FATAL ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }} while(0)
#define tscError(...)  do { if (cDebugFlag & DEBUG_ERROR) { taosPrintLog("TSC ERROR ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }} while(0)
#define tscWarn(...)   do { if (cDebugFlag & DEBUG_WARN)  { taosPrintLog("TSC WARN ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }}  while(0)
#define tscInfo(...)   do { if (cDebugFlag & DEBUG_INFO)  { taosPrintLog("TSC ", tscEmbedded ? 255 : cDebugFlag, __VA_ARGS__); }} while(0)
#define tscDebug(...)  do { if (cDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSC ", cDebugFlag, __VA_ARGS__); }} while(0)
#define tscTrace(...)  do { if (cDebugFlag & DEBUG_TRACE) { taosPrintLog("TSC ", cDebugFlag, __VA_ARGS__); }} while(0)
#define tscDebugL(...) do { if (cDebugFlag & DEBUG_DEBUG) { taosPrintLongString("TSC ", cDebugFlag, __VA_ARGS__); }} while(0)

#ifdef __cplusplus
}
#endif

#endif
