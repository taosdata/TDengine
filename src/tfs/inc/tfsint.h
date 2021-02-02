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
#include "tfs.h"
#include "tcoding.h"

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

// Global Definitions
#define TFS_MIN_DISK_FREE_SIZE 50 * 1024 * 1024

// tdisk.c ======================================================
typedef struct {
  int64_t size;
  int64_t free;
} SDiskMeta;

typedef struct SDisk {
  int       level;
  int       id;
  char      dir[TSDB_FILENAME_LEN];
  SDiskMeta dmeta;
} SDisk;

#define DISK_LEVEL(pd) ((pd)->level)
#define DISK_ID(pd) ((pd)->id)
#define DISK_DIR(pd) ((pd)->dir)
#define DISK_META(pd) ((pd)->dmeta)
#define DISK_SIZE(pd) ((pd)->dmeta.size)
#define DISK_FREE_SIZE(pd) ((pd)->dmeta.free)

SDisk *tfsNewDisk(int level, int id, const char *dir);
SDisk *tfsFreeDisk(SDisk *pDisk);
int    tfsUpdateDiskInfo(SDisk *pDisk);

// ttier.c ======================================================
typedef struct {
  int64_t size;
  int64_t free;
  int16_t nAvailDisks;  // # of Available disks
} STierMeta;
typedef struct STier {
  pthread_spinlock_t lock;
  int                level;
  int16_t            ndisk;   // # of disks mounted to this tier
  int16_t            nextid;  // next disk id to allocate
  STierMeta          tmeta;
  SDisk *            disks[TSDB_MAX_DISKS_PER_TIER];
} STier;

#define TIER_LEVEL(pt) ((pt)->level)
#define TIER_NDISKS(pt) ((pt)->ndisk)
#define TIER_SIZE(pt) ((pt)->tmeta.size)
#define TIER_FREE_SIZE(pt) ((pt)->tmeta.free)
#define TIER_AVAIL_DISKS(pt) ((pt)->tmeta.nAvailDisks)
#define DISK_AT_TIER(pt, id) ((pt)->disks[id])

int    tfsInitTier(STier *pTier, int level);
void   tfsDestroyTier(STier *pTier);
SDisk *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg);
void   tfsUpdateTierInfo(STier *pTier, STierMeta *pTierMeta);
int    tfsAllocDiskOnTier(STier *pTier);
void   tfsGetTierMeta(STier *pTier, STierMeta *pTierMeta);
void   tfsPosNextId(STier *pTier);

#ifdef __cplusplus
}
#endif

#endif