#ifndef TDENGINE_TTIER_H
#define TDENGINE_TTIER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdint.h>

#include "taosdef.h"

#define TIERID int8_t
#define DISKID int8_t

#define TSDB_MAX_TIER 3
#define TSDB_MAX_TIER_MOUNT 16
#define TSDB_FREE_DISK_LIMIT 268435456  // 256M

typedef struct {
  TIERID tid;  // tier ID
  DISKID did;  // disk ID
} SDiskID;

typedef struct {
  SDiskID diskId;
  int32_t numOfFiles;  // number of files on this disk
  char    path[TSDB_FILENAME_LEN];
  int64_t availableSpace;  // available spaces on this mount point in bytes.
} SDisk;

typedef struct {
  int    numOfDisks;
  SDisk *disks[TSDB_MAX_TIER_MOUNT];
} STier;

typedef struct {
  pthread_mutex_t tierMutex;
  int8_t          numOfTiers;
  STier           tiers[TSDB_MAX_TIER];
  void *          diskHash;  // Hash list to decide if a disk is added more than one time.
} STierInfo;

extern STierInfo diskTier;

bool taosValidTierInfo();

int taosInitTier();

int taosAddMountPoint(char *path, TIERID tierid);

DISKID taosAllocDiskOnTier(TIERID tierid);

SDisk *taosGetDiskByID(TIERID tid, DISKID did);

SDisk *taosGetDiskByPath(char *path);

SDisk *taosGetDiskFromHeadFile(char *headFile);

void taosPrintTierInfo();

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTIER_H
