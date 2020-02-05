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

#include "os.h"

#include "shash.h"
#include "tlog.h"
#include "ttier.h"
#include "tutil.h"

STierInfo diskTier;

int taosInitTier() {
  memset(&diskTier, 0, sizeof(STierInfo));
  pthread_mutex_init(&diskTier.tierMutex, NULL);

  // TODO : Change the taosHashString function here to be a correct hash
  // function
  diskTier.diskHash = taosInitStrHash(TSDB_MAX_TIER_MOUNT * TSDB_MAX_TIER, sizeof(SDiskID), taosHashString);
  if (diskTier.diskHash == NULL) {
    pError("failed to init disk tier hash");
    return -1;
  }

  return 0;
}

void dnodeCleanupStorageClusterImp() {
  taosCleanUpStrHash(diskTier.diskHash);
  for (int8_t tierid = 0; tierid < diskTier.numOfTiers; tierid++)
    for (int8_t did = 0; did < diskTier.tiers[tierid].numOfDisks; did++) tfree(diskTier.tiers[tierid].disks[did]);

  pthread_mutex_destroy(&(diskTier.tierMutex));
}

SDisk *taosGetDiskByID(TIERID tid, DISKID did) {
  if (tid >= TSDB_MAX_TIER || did >= diskTier.tiers[tid].numOfDisks) return NULL;

  return diskTier.tiers[tid].disks[did];
}

SDisk *taosGetDiskByPath(char *path) {
  char dPath[TSDB_FILENAME_LEN] = "\0";

  strcpy(dPath, path);

  SDiskID *diskId = (SDiskID *)taosGetStrHashData(diskTier.diskHash, dPath);

  if (diskId == NULL) return NULL;

  return taosGetDiskByID(diskId->tid, diskId->did);
}

int getDiskInfo(SDisk *disk) {
  assert(disk != NULL);

  struct statvfs diskStat;

  if (statvfs(disk->path, &diskStat) < 0) {
    pError("failed to get disk info, path: %s numOfFiles: %d", disk->path, disk->numOfFiles);
    return -1;
  }

  disk->availableSpace = diskStat.f_bfree * diskStat.f_bsize;

  return 0;
}

int taosAddMountPoint(char *path, TIERID tierid) {
  SDisk *   disk = NULL;
  wordexp_t full_path;
  SDiskID   diskId;
  int       old_uDebugFlag = uDebugFlag;

  // Supress UTL debug information
  uDebugFlag = 131;

  // -1 means tid = 0 and did = 0

  if (tierid < -1 || tierid >= TSDB_MAX_TIER) {
    pError("Invalid tier level %d path %s", tierid, path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  if (tierid == -1) {
    if (taosGetDiskByID(0, 0) != NULL) {
      pError("Failed to add path %s since tid 0 and did 0 disk already there", path);
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

  // check if path is a valid path (if path exists and if it already added in
  // tiers)
  if (wordexp(path, &full_path, 0) != 0) {
    pError("Invalid path %s", path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  mkdir(full_path.we_wordv[0], 0755);

  if (access(full_path.we_wordv[0], W_OK | R_OK) != 0) {
    pError("No R/W rights to path %s", path);
    wordfree(&full_path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  if (diskTier.tiers[tierid].numOfDisks >= TSDB_MAX_TIER_MOUNT) {
    pError("tier %s is full, failed to add mount point %s", tierid, path);
    wordfree(&full_path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  disk = (SDisk *)calloc(1, sizeof(SDisk));
  if (disk == NULL) {
    pError("failed to allocate disk memory, tierid: %d path: %s", tierid, path);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }
  strcpy(disk->path, full_path.we_wordv[0]);
  wordfree(&full_path);

  if (taosGetStrHashData(diskTier.diskHash, disk->path) != NULL) {
    pError("failed to add path %s to tier %d since it is already there", path, tierid);
    tfree(disk);
    uDebugFlag = old_uDebugFlag;
    return -1;
  }

  diskId = (SDiskID){.tid = tierid, .did = diskTier.tiers[tierid].numOfDisks};

  taosAddStrHash(diskTier.diskHash, disk->path, (char *)(&diskId));

  if (diskTier.tiers[tierid].numOfDisks == 0) diskTier.numOfTiers++;

  disk->diskId = diskId;
  getDiskInfo(disk);

  diskTier.tiers[tierid].disks[diskTier.tiers[tierid].numOfDisks++] = disk;

  pTrace("disk %s is added to diskTier, tid: %d did: %d", path, tierid, diskId.did);

  uDebugFlag = old_uDebugFlag;

  return 0;
}

void updateTierDiskInfo(TIERID tierid) {
  STier *tier;
  tier = diskTier.tiers + tierid;

  for (int i = 0; i < tier->numOfDisks; i++) getDiskInfo(tier->disks[i]);
}

DISKID taosAllocDiskOnTier(TIERID tierid) {
  DISKID  did = -1;
  int32_t numOfFiles = INT_MAX;
  SDisk **disks = NULL;

  if (tierid < 0 || tierid >= TSDB_MAX_TIER || diskTier.tiers[tierid].numOfDisks == 0) return -1;

  disks = diskTier.tiers[tierid].disks;

  updateTierDiskInfo(tierid);

  pthread_mutex_lock(&diskTier.tierMutex);

  for (DISKID i = 0; i < diskTier.tiers[tierid].numOfDisks; i++) {
    if (disks[i]->numOfFiles < numOfFiles) {
      did = i;
      numOfFiles = disks[i]->numOfFiles;
    }
  }

  __sync_fetch_and_add(&(disks[did]->numOfFiles), 1);

  pthread_mutex_unlock(&diskTier.tierMutex);

  pTrace("Allocate disk tier %d did %d", tierid, did);

  return did;
}

bool taosValidTierInfo() {
  if (diskTier.numOfTiers == 0) {
    if (taosAddMountPoint(tsDirectory, 0) < 0) return false;
    return true;
  }

  if (taosGetDiskByID(0, 0) == NULL) return false;

  for (int i = 0; i < diskTier.numOfTiers; i++) {
    if (diskTier.tiers[i].numOfDisks == 0) {
      pError("tier %d has %d disks", i, diskTier.tiers[i].numOfDisks) return false;
    }
  }

  return true;
}

SDisk *taosGetDiskFromHeadFile(char *headFile) {
  char dpath[TSDB_FILENAME_LEN] = "\0";
  char path[TSDB_FILENAME_LEN] = "\0";

  /* if (access(headFile, F_OK) != 0) return NULL; */

  if (readlink(headFile, dpath, TSDB_FILENAME_LEN) < 0) return NULL;

  for (int i = 0; i < 3; i++) dirname(dpath);

  strcpy(path, dpath);

  return taosGetDiskByPath(path);
}

void taosPrintTierInfo() {
  char optionBuffer[128];
  char blank[TSDB_CFG_PRINT_LEN];
  int  optionLen;
  int  blankLen;

  for (int i = 0; i < diskTier.numOfTiers; ++i) {
    STier *tier = &diskTier.tiers[i];
    for (int j = 0; j < tier->numOfDisks; ++j) {
      SDisk *disk = tier->disks[j];

      optionLen = sprintf(optionBuffer, "disk%d-%d", disk->diskId.tid, disk->diskId.did);
      blankLen = TSDB_CFG_PRINT_LEN - optionLen;
      blankLen = blankLen < 0 ? 0 : blankLen;
      memset(blank, ' ', TSDB_CFG_PRINT_LEN);
      blank[blankLen] = 0;
      pPrint(" %s:%s%s", optionBuffer, blank, disk->path);
    }
  }
}