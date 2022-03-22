
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

#ifndef _TD_DND_MAIN_H_
#define _TD_DND_MAIN_H_

#include "dnode.h"

#include "taoserror.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tlog.h"
#include "version.h"

#ifdef __cplusplus
extern "C" {
#endif

#define dFatal(...) { if (dDebugFlag & DEBUG_FATAL) { taosPrintLog("DND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define dError(...) { if (dDebugFlag & DEBUG_ERROR) { taosPrintLog("DND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define dWarn(...)  { if (dDebugFlag & DEBUG_WARN)  { taosPrintLog("DND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define dInfo(...)  { if (dDebugFlag & DEBUG_INFO)  { taosPrintLog("DND ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define dDebug(...) { if (dDebugFlag & DEBUG_DEBUG) { taosPrintLog("DND ", DEBUG_DEBUG, dDebugFlag, __VA_ARGS__); }}
#define dTrace(...) { if (dDebugFlag & DEBUG_TRACE) { taosPrintLog("DND ", DEBUG_TRACE, dDebugFlag, __VA_ARGS__); }}

void      dndDumpCfg();
void      dndPrintVersion();
void      dndGenerateGrant();
SDnodeOpt dndGetOpt();

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MAIN_H_*/
