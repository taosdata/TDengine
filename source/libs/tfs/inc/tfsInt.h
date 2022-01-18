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

#ifndef _TD_TFS_INT_H_
#define _TD_TFS_INT_H_

#include "os.h"

#include "taosdef.h"
#include "taoserror.h"
#include "tcoding.h"
#include "tfs.h"
#include "tglobal.h"
#include "thash.h"
#include "tlog.h"

extern int32_t fsDebugFlag;

// For debug purpose
#define fFatal(...) { if (fsDebugFlag & DEBUG_FATAL) { taosPrintLog("TFS FATAL ", 255, __VA_ARGS__); }}
#define fError(...) { if (fsDebugFlag & DEBUG_ERROR) { taosPrintLog("TFS ERROR ", 255, __VA_ARGS__); }}
#define fWarn(...)  { if (fsDebugFlag & DEBUG_WARN)  { taosPrintLog("TFS WARN ", 255, __VA_ARGS__); }}
#define fInfo(...)  { if (fsDebugFlag & DEBUG_INFO)  { taosPrintLog("TFS ", 255, __VA_ARGS__); }}
#define fDebug(...) { if (fsDebugFlag & DEBUG_DEBUG) { taosPrintLog("TFS ", cqDebugFlag, __VA_ARGS__); }}
#define fTrace(...) { if (fsDebugFlag & DEBUG_TRACE) { taosPrintLog("TFS ", cqDebugFlag, __VA_ARGS__); }}

// Global Definitions
#define TFS_MIN_DISK_FREE_SIZE 50 * 1024 * 1024

typedef struct SDisk {
  int32_t   level;
  int32_t   id;
  char     *path;
  SDiskSize size;
} SDisk;

typedef struct STier {
  pthread_spinlock_t lock;
  int32_t            level;
  int16_t            nextid;       // next disk id to allocate
  int16_t            ndisk;        // # of disks mounted to this tier
  int16_t            nAvailDisks;  // # of Available disks
  SDisk             *disks[TSDB_MAX_DISKS_PER_TIER];
  SDiskSize          size;
} STier;

#define TIER_LEVEL(pt) ((pt)->level)
#define TIER_NDISKS(pt) ((pt)->ndisk)
#define TIER_SIZE(pt) ((pt)->tmeta.size)
#define TIER_FREE_SIZE(pt) ((pt)->tmeta.free)

#define DISK_AT_TIER(pt, id) ((pt)->disks[id])
#define DISK_DIR(pd) ((pd)->path)

SDisk  *tfsNewDisk(int32_t level, int32_t id, const char *dir);
SDisk  *tfsFreeDisk(SDisk *pDisk);
int32_t tfsUpdateDiskSize(SDisk *pDisk);

int32_t tfsInitTier(STier *pTier, int32_t level);
void    tfsDestroyTier(STier *pTier);
SDisk  *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg);
void    tfsUpdateTierSize(STier *pTier);
int32_t tfsAllocDiskOnTier(STier *pTier);
void    tfsPosNextId(STier *pTier);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TFS_INT_H_*/
