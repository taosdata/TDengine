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

#define _DEFAULT_SOURCE
#include "os.h"
#include "hash.h"
#include "tlog.h"
#include "tutil.h"
#include "storageTier.h"

STierInfo tsStorageDiskTier;

int32_t storageAllocDiskTier() {
  memset(&tsStorageDiskTier, 0, sizeof(STierInfo));
  pthread_mutex_init(&tsStorageDiskTier.tierMutex, NULL);

  tsStorageDiskTier.diskHash = taosInitStrHash(TSDB_MAX_TIER_MOUNT * TSDB_MAX_TIER, sizeof(SDiskID), taosHashString);
  if (tsStorageDiskTier.diskHash == NULL) {
    uError("failed to init disk tier hash");
    return -1;
  }

  return 0;
}

SDisk *storageGetDiskByID(TIERID tid, DISKID did) {
  if (tid >= TSDB_MAX_TIER || did >= tsStorageDiskTier.tiers[tid].numOfDisks) {
    return NULL;
  }

  return tsStorageDiskTier.tiers[tid].disks[did];
}

SDisk *storageGetDiskByPath(char *path) {
  char dPath[TSDB_FILENAME_LEN] = "\0";
  strcpy(dPath, path);

  SDiskID *diskId = (SDiskID *)taosGetStrHashData(tsStorageDiskTier.diskHash, dPath);
  if (diskId == NULL) return NULL;

  return storageGetDiskByID(diskId->tid, diskId->did);
}

int32_t storageGetDiskInfo(SDisk *disk) {
  assert(disk != NULL);

  struct statvfs diskStat;
  if (statvfs(disk->path, &diskStat) < 0) {
    uError("failed to get disk info, path: %s numOfFiles: %d", disk->path, disk->numOfFiles);
    return -1;
  }

  disk->availableSpace = diskStat.f_bfree * diskStat.f_bsize;
  return 0;
}

int32_t storageAddMountPoint(char *path, TIERID tierid) {
  SDisk *   disk = NULL;
  wordexp_t full_path;
  SDiskID   diskId;
  int32_t   old_uDebugFlag = uDebugFlag;

  uDebugFlag = 131;

  // -1 means tid = 0 and did = 0

  if (tierid < -1 || tierid >= TSDB_MAX_TIER) {
    uError("Invalid tier level %d path %s", tierid, path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  if (tierid == -1) {
    if (storageGetDiskByID(0, 0) != NULL) {
      uError("Failed to add path %s since tid 0 and did 0 disk already there", path);
      uDebugFlag = old_uDebugFlag;
      return -1;
    }

    tierid = 0;
  }

  // Remove trailing slashes in path string
  if (strcmp(path, "/") != 0) {
    size_t path_len = strlen(path) - 1;
    for (; path_len >= 1; path_len--) {
      if (path[path_len] == '/') {
        path[path_len] = '\0';
        continue;
      } else
        break;
    }
  }

  // check if path is a valid path (if path exists and if it already added in tiers)
  if (wordexp(path, &full_path, 0) != 0) {
    uError("Invalid path %s", path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  mkdir(full_path.we_wordv[0], 0755);

  if (access(full_path.we_wordv[0], W_OK | R_OK) != 0) {
    uError("No R/W rights to path %s", path);
    wordfree(&full_path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  if (tsStorageDiskTier.tiers[tierid].numOfDisks >= TSDB_MAX_TIER_MOUNT) {
    uError("tier %s is full, failed to add mount point %s", tierid, path);
    wordfree(&full_path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  disk = (SDisk *)calloc(1, sizeof(SDisk));
  if (disk == NULL) {
    uError("failed to allocate disk memory, tierid: %d path: %s", tierid, path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }
  strcpy(disk->path, full_path.we_wordv[0]);
  wordfree(&full_path);

  if (taosGetStrHashData(tsStorageDiskTier.diskHash, disk->path) != NULL) {
    uError("failed to add path %s to tier %d since it is already there", path, tierid);
    tfree(disk);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  diskId = (SDiskID){.tid = tierid, .did = tsStorageDiskTier.tiers[tierid].numOfDisks};

  taosAddStrHash(tsStorageDiskTier.diskHash, disk->path, (char *)(&diskId));

  if (tsStorageDiskTier.tiers[tierid].numOfDisks == 0) tsStorageDiskTier.numOfTiers++;

  disk->diskId = diskId;
  storageGetDiskInfo(disk);

  tsStorageDiskTier.tiers[tierid].disks[tsStorageDiskTier.tiers[tierid].numOfDisks++] = disk;

  uDebug("disk %s is added to tsStorageDiskTier, tid: %d did: %d", path, tierid, diskId.did);

  uDebugFlag = old_uDebugFlag;

  return 0;
}

void storageUpdateTierDiskInfo(TIERID tierid) {
  STier *tier;
  tier = tsStorageDiskTier.tiers + tierid;

  for (int32_t i = 0; i < tier->numOfDisks; i++) storageGetDiskInfo(tier->disks[i]);
}

DISKID storageAllocDiskOnTier(TIERID tierid) {
  DISKID  did = -1;
  int32_t numOfFiles = INT_MAX;
  SDisk **disks = NULL;

  if (tierid < 0 || tierid >= TSDB_MAX_TIER || tsStorageDiskTier.tiers[tierid].numOfDisks == 0) return -1;

  disks = tsStorageDiskTier.tiers[tierid].disks;

  storageUpdateTierDiskInfo(tierid);

  pthread_mutex_lock(&tsStorageDiskTier.tierMutex);

  for (DISKID i = 0; i < tsStorageDiskTier.tiers[tierid].numOfDisks; i++) {
    if (disks[i]->numOfFiles < numOfFiles) {
      did = i;
      numOfFiles = disks[i]->numOfFiles;
    }
  }

  __sync_fetch_and_add(&(disks[did]->numOfFiles), 1);

  pthread_mutex_unlock(&tsStorageDiskTier.tierMutex);

  uDebug("Allocate disk tier %d did %d", tierid, did);

  return did;
}

bool storageValidTierInfo() {
  if (tsStorageDiskTier.numOfTiers == 0) {
    if (storageAddMountPoint(tsDirectory, 0) < 0) return false;
    return true;
  }

  if (storageGetDiskByID(0, 0) == NULL) return false;

  for (int32_t i = 0; i < tsStorageDiskTier.numOfTiers; i++) {
    if (tsStorageDiskTier.tiers[i].numOfDisks == 0) {
      uError("tier %d has %d disks", i, tsStorageDiskTier.tiers[i].numOfDisks) return false;
    }
  }

  return true;
}

SDisk *storageGetDiskFromHeadFile(char *headFile) {
  char dpath[TSDB_FILENAME_LEN] = "\0";
  char path[TSDB_FILENAME_LEN] = "\0";

  /* if (access(headFile, F_OK) != 0) return NULL; */

  if (readlink(headFile, dpath, TSDB_FILENAME_LEN) < 0) return NULL;

  for (int32_t i = 0; i < 3; i++) dirname(dpath);

  strcpy(path, dpath);

  return storageGetDiskByPath(path);
}
