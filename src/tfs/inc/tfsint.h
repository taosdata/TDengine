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

// tdisk.c ======================================================
typedef struct {
  int32_t nfiles;
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
#define DISK_NFILES(pd) ((pd)->dmeta.nfiles)

SDisk *tfsNewDisk(int level, int id, const char *dir);
SDisk *tfsFreeDisk(SDisk *pDisk);
void   tfsUpdateDiskInfo(SDisk *pDisk);

// ttier.c ======================================================
typedef struct {
  int64_t size;
  int64_t free;
} STierMeta;
typedef struct STier {
  int       level;
  int32_t   ndisk;
  STierMeta tmeta;
  SDisk *   disks[TSDB_MAX_DISKS_PER_TIER];
} STier;

#define TIER_LEVEL(pt) ((pt)->level)
#define TIER_NDISKS(pt) ((pt)->ndisk)
#define TIER_SIZE(pt) ((pt)->tmeta.size)
#define TIER_FREE_SIZE(pt) ((pt)->tmeta.free)
#define DISK_AT_TIER(pt, id) ((pt)->disks[id])

void   tfsInitTier(STier *pTier, int level);
void   tfsDestroyTier(STier *pTier);
SDisk *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg);
void   tfsUpdateTierInfo(STier *pTier);

#ifdef __cplusplus
}
#endif

#endif