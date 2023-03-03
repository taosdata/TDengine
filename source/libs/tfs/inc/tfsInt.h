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
#include "thash.h"
#include "tlog.h"

// For debug purpose
// clang-format off
#define fFatal(...) { if (fsDebugFlag & DEBUG_FATAL) { taosPrintLog("TFS FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define fError(...) { if (fsDebugFlag & DEBUG_ERROR) { taosPrintLog("TFS ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define fWarn(...)  { if (fsDebugFlag & DEBUG_WARN)  { taosPrintLog("TFS WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define fInfo(...)  { if (fsDebugFlag & DEBUG_INFO)  { taosPrintLog("TFS ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define fDebug(...) { if (fsDebugFlag & DEBUG_DEBUG) { taosPrintLog("TFS ", DEBUG_DEBUG, fsDebugFlag, __VA_ARGS__); }}
#define fTrace(...) { if (fsDebugFlag & DEBUG_TRACE) { taosPrintLog("TFS ", DEBUG_TRACE, fsDebugFlag, __VA_ARGS__); }}
// clang-format on

typedef struct {
  int32_t   level;
  int32_t   id;
  char     *path;
  SDiskSize size;
} STfsDisk;

typedef struct {
  TdThreadSpinlock lock;
  int32_t          level;
  int32_t          nextid;       // next disk id to allocate
  int32_t          ndisk;        // # of disks mounted to this tier
  int32_t          nAvailDisks;  // # of Available disks
  STfsDisk        *disks[TFS_MAX_DISKS_PER_TIER];
  SDiskSize        size;
} STfsTier;

typedef struct {
  STfsDisk *pDisk;
} SDiskIter;

typedef struct STfsDir {
  SDiskIter iter;
  SDiskID   did;
  char      dirName[TSDB_FILENAME_LEN];
  STfsFile  tfile;
  TdDirPtr  pDir;
  STfs     *pTfs;
} STfsDir;

typedef struct STfs {
  TdThreadSpinlock lock;
  SDiskSize        size;
  int32_t          nlevel;
  STfsTier         tiers[TFS_MAX_TIERS];
  SHashObj        *hash;  // name to did map
} STfs;

STfsDisk *tfsNewDisk(int32_t level, int32_t id, const char *dir);
STfsDisk *tfsFreeDisk(STfsDisk *pDisk);
int32_t   tfsUpdateDiskSize(STfsDisk *pDisk);

int32_t   tfsInitTier(STfsTier *pTier, int32_t level);
void      tfsDestroyTier(STfsTier *pTier);
STfsDisk *tfsMountDiskToTier(STfsTier *pTier, SDiskCfg *pCfg);
void      tfsUpdateTierSize(STfsTier *pTier);
int32_t   tfsAllocDiskOnTier(STfsTier *pTier);
void      tfsPosNextId(STfsTier *pTier);

#define tfsLockTier(pTier)   taosThreadSpinLock(&(pTier)->lock)
#define tfsUnLockTier(pTier) taosThreadSpinUnlock(&(pTier)->lock)

#define tfsLock(pTfs)   taosThreadSpinLock(&(pTfs)->lock)
#define tfsUnLock(pTfs) taosThreadSpinUnlock(&(pTfs)->lock)

#define TFS_TIER_AT(pTfs, level) (&(pTfs)->tiers[level])
#define TFS_DISK_AT(pTfs, did)   ((pTfs)->tiers[(did).level].disks[(did).id])
#define TFS_PRIMARY_DISK(pTfs)   ((pTfs)->tiers[0].disks[0])

#define TMPNAME_LEN (TSDB_FILENAME_LEN * 2 + 32)

#ifdef __cplusplus
}
#endif

#endif /*_TD_TFS_INT_H_*/
