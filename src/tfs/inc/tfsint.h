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

#ifndef TD_TFSINT_H
#define TD_TFSINT_H

#include "tlog.h"
#include "tglobal.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int fsDebugFlag;

// For debug purpose
#define fFatal(...) { if (fsDebugFlag & DEBUG_FATAL) { taosPrintLog("TFS FATAL ", 255, __VA_ARGS__); }}
#define fError(...) { if (fsDebugFlag & DEBUG_ERROR) { taosPrintLog("TFS ERROR ", 255, __VA_ARGS__); }}
#define fWarn(...)  { if (fsDebugFlag & DEBUG_WARN)  { taosPrintLog("TFS WARN ", 255, __VA_ARGS__); }}
#define fInfo(...)  { if (fsDebugFlag & DEBUG_INFO)  { taosPrintLog("TFS ", 255, __VA_ARGS__); }}
#define fDebug(...) { if (fsDebugFlag & DEBUG_DEBUG) { taosPrintLog("TFS ", cqDebugFlag, __VA_ARGS__); }}
#define fTrace(...) { if (fsDebugFlag & DEBUG_TRACE) { taosPrintLog("TFS ", cqDebugFlag, __VA_ARGS__); }}

// tdisk.c
typedef struct SDisk SDisk;

SDisk *tfsNewDisk(int level, int id, char *dir);
void   tfsFreeDisk(SDisk *pDisk);
int    tfsUpdateDiskInfo(SDisk *pDisk);

const char *tfsDiskDir(SDisk *pDisk);

// ttier.c
#define TSDB_MAX_DISK_PER_TIER 16

typedef struct STier {
  int    level;
  int    ndisk;
  SDisk *disks[TSDB_MAX_DISK_PER_TIER];
} STier;

#define DISK_AT_TIER(pTier, id) ((pTier)->disks[id])

void   tfsInitTier(STier *pTier, int level);
void   tfsDestroyTier(STier *pTier);
SDisk *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg);
int    tfsUpdateTierInfo(STier *pTier);

// tfs.c
void tfsIncFileAt(int level, int id);
void tfsDecFileAt(int level, int id);
int  tfsLock();
int  tfsUnLock();
bool tfsIsLocked();
int  tfsLevels();

// tfcntl.c

#ifdef __cplusplus
}
#endif

#endif