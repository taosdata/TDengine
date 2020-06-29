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
#include "taoserror.h"
#include "taosdef.h"
#include "tlog.h"
#include "tutil.h"
#include "hash.h"
#include "tglobal.h"
#include "dnode.h"
#include "storage.h"
#include "storageTier.h"

static int32_t storageInitTiers();
static void storageCleanupTiers();
static bool storageReadTiersInfo();
static void storagePrintTiersInfo();

void storageInit() {
  dnodeInitStorage     = storageInitTiers;
  dnodeCleanupStorage  = storageCleanupTiers;
  tsReadStorageConfig  = storageReadTiersInfo;
  tsPrintStorageConfig = storagePrintTiersInfo;
}

static int32_t storageInitTiers() {
  char   fileName[128];
  SDisk *disk = NULL;

  for (TIERID tid = 0; tid < tsStorageDiskTier.numOfTiers; tid++) {
    for (DISKID did = 0; did < tsStorageDiskTier.tiers[tid].numOfDisks; did++) {
      disk = storageGetDiskByID(tid, did);
      assert(disk != NULL);

      if (tid == 0 && did == 0) {
        sprintf(fileName, "%s/tsdb", disk->path);
        mkdir(fileName, 0755);
      }
      sprintf(fileName, "%s/data", disk->path);
      mkdir(fileName, 0755);
    }
  }

  disk = storageGetDiskByID(0, 0);
  if (disk == NULL) {
    return -1;
  }

  sprintf(tsMgmtDirectory, "%s/mgmt", disk->path);
  sprintf(tsDirectory, "%s/tsdb", disk->path);

  dnodeCheckDataDirOpenned(disk->path);

  return 0;
}

static void storageCleanupTiers() {
  taosCleanUpStrHash(tsStorageDiskTier.diskHash);
  for (int8_t tierid = 0; tierid < tsStorageDiskTier.numOfTiers; tierid++)
    for (int8_t did = 0; did < tsStorageDiskTier.tiers[tierid].numOfDisks; did++) {
      tfree(tsStorageDiskTier.tiers[tierid].disks[did]);
    }

  pthread_mutex_destroy(&(tsStorageDiskTier.tierMutex));
}

static bool storageReadTiersInfo() {
  if (!tscEmbedded) return true;

  char   path[TSDB_FILENAME_LEN] = "\0";
  TIERID tid;

  if (storageAllocDiskTier() < 0) {
    return false;
  }

  FILE *  fp;
  char *  line, *option, *value, *value1;
  size_t  len;
  int32_t olen, vlen, vlen1;
  char    fileName[TSDB_FILENAME_LEN*2];

  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
  } else {
    line = NULL;
    while (!feof(fp)) {
      tfree(line);
      line = option = value = NULL;
      len = olen = vlen = 0;

      getline(&line, &len, fp);
      if (line == NULL) break;

      paGetToken(line, &option, &olen);
      if (olen == 0) continue;
      option[olen] = 0;

      paGetToken(option + olen + 1, &value, &vlen);
      if (vlen == 0) continue;
      value[vlen] = 0;

      // For dataDir, the format is:
      // dataDir    /mnt/disk1    0
      paGetToken(value + vlen + 1, &value1, &vlen1);
      if (strncasecmp(option, "dataDir", 7) == 0) {
        if (vlen1 == 0)
          tid = -1;
        else
          tid = (TIERID)atoi(value1);

        memset(path, 0, TSDB_FILENAME_LEN);
        memcpy(path, value, vlen);
        path[vlen] = '\0';

        tsExpandFilePath("dataDir", path);
        storageAddMountPoint(path, tid);
        strcpy(dataDir, path);
      }
    }

    tfree(line);
    fclose(fp);
  }

  if (tscEmbedded) {
    if (!storageValidTierInfo()) {
      return false;
    }

  }

  return true;
}

static void storagePrintTiersInfo() {
  if (tscEmbedded) return;

  char optionBuffer[128];
  char blank[TSDB_CFG_PRINT_LEN];
  int32_t  optionLen;
  int32_t  blankLen;

  for (int32_t i = 0; i < tsStorageDiskTier.numOfTiers; ++i) {
    STier *tier = &tsStorageDiskTier.tiers[i];
    for (int32_t j = 0; j < tier->numOfDisks; ++j) {
      SDisk *disk = tier->disks[j];

      optionLen = sprintf(optionBuffer, "disk%d-%d", disk->diskId.tid, disk->diskId.did);
      blankLen = TSDB_CFG_PRINT_LEN - optionLen;
      blankLen = blankLen < 0 ? 0 : blankLen;
      memset(blank, ' ', TSDB_CFG_PRINT_LEN);
      blank[blankLen] = 0;
      uInfo(" %s:%s%s", optionBuffer, blank, disk->path);
    }
  }
}
