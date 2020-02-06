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
#include "tlog.h"
#include "tutil.h"
#include "shash.h"
#include "tglobalcfg.h"
#include "dnode.h"
#include "mstorageTier.h"
#include "mstorageSystem.h"

int32_t mstorageInitSystem() {
  dnodeInitStorage = mstorageInitStorage;
  dnodeCleanupStorage = mstorageCleanupStorage;
}

int32_t mstorageInitStorage() {
  char   fileName[128];
  SDisk *disk = NULL;

  for (TIERID tid = 0; tid < diskTier.numOfTiers; tid++) {
    for (DISKID did = 0; did < diskTier.tiers[tid].numOfDisks; did++) {
      disk = taosGetDiskByID(tid, did);
      assert(disk != NULL);

      if (tid == 0 && did == 0) {
        sprintf(fileName, "%s/tsdb", disk->path);
        mkdir(fileName, 0755);
      }
      sprintf(fileName, "%s/data", disk->path);
      mkdir(fileName, 0755);
    }
  }

  disk = taosGetDiskByID(0, 0);
  if (disk == NULL) {
    return -1;
  }

  sprintf(mgmtDirectory, "%s/mgmt", disk->path);
  sprintf(tsDirectory, "%s/tsdb", disk->path);
  dnodeCheckDbRunning(disk->path);

  return 0;
}

void mstorageCleanupStorage() {
  taosCleanUpStrHash(diskTier.diskHash);
  for (int8_t tierid = 0; tierid < diskTier.numOfTiers; tierid++)
    for (int8_t did = 0; did < diskTier.tiers[tierid].numOfDisks; did++) {
      tfree(diskTier.tiers[tierid].disks[did]);
    }

  pthread_mutex_destroy(&(diskTier.tierMutex));
}