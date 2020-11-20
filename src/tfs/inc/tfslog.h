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

#ifndef TD_TFSLOG_H
#define TD_TFSLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#define fFatal(...) { if (fsDebugFlag & DEBUG_FATAL) { taosPrintLog("FS FATAL ", 255, __VA_ARGS__); }}
#define fError(...) { if (fsDebugFlag & DEBUG_ERROR) { taosPrintLog("FS ERROR ", 255, __VA_ARGS__); }}
#define fWarn(...)  { if (fsDebugFlag & DEBUG_WARN)  { taosPrintLog("FS WARN ", 255, __VA_ARGS__); }}
#define fInfo(...)  { if (fsDebugFlag & DEBUG_INFO)  { taosPrintLog("FS ", 255, __VA_ARGS__); }}
#define fDebug(...) { if (fsDebugFlag & DEBUG_DEBUG) { taosPrintLog("FS ", cqDebugFlag, __VA_ARGS__); }}
#define fTrace(...) { if (fsDebugFlag & DEBUG_TRACE) { taosPrintLog("FS ", cqDebugFlag, __VA_ARGS__); }}

#ifdef __cplusplus
}
#endif

#endif