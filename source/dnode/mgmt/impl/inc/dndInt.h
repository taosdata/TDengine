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

#ifndef _TD_DND_INT_H_
#define _TD_DND_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "cJSON.h"
#include "monitor.h"
#include "tcache.h"
#include "tcrc32c.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "thash.h"
#include "tlockfree.h"
#include "tlog.h"
#include "tmsg.h"
#include "tqueue.h"
#include "trpc.h"
#include "tthread.h"
#include "ttime.h"
#include "tworker.h"

#include "dnode.h"

#include "bnode.h"
#include "mnode.h"
#include "qnode.h"
#include "snode.h"
#include "vnode.h"
#include "tfs.h"

#define dFatal(...) { if (dDebugFlag & DEBUG_FATAL) { taosPrintLog("DND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define dError(...) { if (dDebugFlag & DEBUG_ERROR) { taosPrintLog("DND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define dWarn(...)  { if (dDebugFlag & DEBUG_WARN)  { taosPrintLog("DND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define dInfo(...)  { if (dDebugFlag & DEBUG_INFO)  { taosPrintLog("DND ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define dDebug(...) { if (dDebugFlag & DEBUG_DEBUG) { taosPrintLog("DND ", DEBUG_DEBUG, dDebugFlag, __VA_ARGS__); }}
#define dTrace(...) { if (dDebugFlag & DEBUG_TRACE) { taosPrintLog("DND ", DEBUG_TRACE, dDebugFlag, __VA_ARGS__); }}

typedef enum { DND_STAT_INIT, DND_STAT_RUNNING, DND_STAT_STOPPED } EStat;
typedef enum { DND_WORKER_SINGLE, DND_WORKER_MULTI } EWorkerType;
typedef enum { DND_ENV_INIT = 0, DND_ENV_READY = 1, DND_ENV_CLEANUP = 2 } EEnvStat;
typedef void (*DndMsgFp)(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEps);

EStat       dndGetStat(SDnode *pDnode);
void        dndSetStat(SDnode *pDnode, EStat stat);
const char *dndStatStr(EStat stat);

void dndReportStartup(SDnode *pDnode, char *pName, char *pDesc);
void dndGetStartup(SDnode *pDnode, SStartupReq *pStartup);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_INT_H_*/
