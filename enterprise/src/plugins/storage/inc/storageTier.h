#ifndef TDENGINE_STORAGE_TIER_H
#define TDENGINE_STORAGE_TIER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "taosdef.h"

#define TIERID int8_t
#define DISKID int8_t

#define TSDB_MAX_TIER 3
#define TSDB_MAX_TIER_MOUNT 16

typedef struct {
  TIERID tid;  // tier ID
  DISKID did;  // disk ID
} SDiskID;

typedef struct {
  SDiskID diskId;
  int32_t numOfFiles;      // number of files on this disk
  int64_t availableSpace;  // available spaces on this mount point in bytes.
  char    path[TSDB_FILENAME_LEN];
} SDisk;

typedef struct {
  int    numOfDisks;
  SDisk *disks[TSDB_MAX_TIER_MOUNT];
} STier;

typedef struct {
  int8_t numOfTiers;
  STier  tiers[TSDB_MAX_TIER];
  void  *diskHash;  // Hash list to decide if a disk is added more than one time.
  pthread_mutex_t tierMutex;
} STierInfo;

extern STierInfo tsStorageDiskTier;

bool    storageValidTierInfo();
int32_t storageAllocDiskTier();
int32_t storageAddMountPoint(char *path, TIERID tierid);
SDisk * storageGetDiskByID(TIERID tid, DISKID did);

#ifdef __cplusplus
}
#endif

#endif
