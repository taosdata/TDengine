#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// #include "taosdef.h"
// #include "disk.h"
#include "tsdb.h"
#include "tsdbCache.h"
#include "tsdbFile.h"
#include "tsdbMeta.h"
#include "tutil.h"
#include "tskiplist.h"

#define TSDB_DEFAULT_PRECISION TSDB_PRECISION_MILLI  // default precision
#define IS_VALID_PRECISION(precision) (((precision) >= TSDB_PRECISION_MILLI) && ((precision) <= TSDB_PRECISION_NANO))
#define TSDB_MIN_ID 0
#define TSDB_MAX_ID INT_MAX
#define TSDB_MIN_TABLES 10
#define TSDB_MAX_TABLES 100000
#define TSDB_DEFAULT_TABLES 1000
#define TSDB_DEFAULT_DAYS_PER_FILE 10
#define TSDB_MIN_DAYS_PER_FILE 1
#define TSDB_MAX_DAYS_PER_FILE 60
#define TSDB_DEFAULT_MIN_ROW_FBLOCK 100
#define TSDB_MIN_MIN_ROW_FBLOCK 10
#define TSDB_MAX_MIN_ROW_FBLOCK 1000
#define TSDB_DEFAULT_MAX_ROW_FBLOCK 4096
#define TSDB_MIN_MAX_ROW_FBLOCK 200
#define TSDB_MAX_MAX_ROW_FBLOCK 10000
#define TSDB_DEFAULT_KEEP 3650
#define TSDB_MIN_KEEP 1
#define TSDB_MAX_KEEP INT_MAX
#define TSDB_DEFAULT_CACHE_SIZE (16 * 1024 * 1024)  // 16M
#define TSDB_MIN_CACHE_SIZE (4 * 1024 * 1024)       // 4M
#define TSDB_MAX_CACHE_SIZE (1024 * 1024 * 1024)    // 1G

enum { TSDB_REPO_STATE_ACTIVE, TSDB_REPO_STATE_CLOSED, TSDB_REPO_STATE_CONFIGURING };

typedef struct _tsdb_repo {
  char *rootDir;
  // TSDB configuration
  STsdbCfg config;

  // The meter meta handle of this TSDB repository
  STsdbMeta *tsdbMeta;

  // The cache Handle
  STsdbCache *tsdbCache;

  // Disk tier handle for multi-tier storage
  void *diskTier;

  // File Store
  void *tsdbFiles;

  pthread_mutex_t tsdbMutex;

  // A limiter to monitor the resources used by tsdb
  void *limiter;

  int8_t state;

} STsdbRepo;

static int32_t tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg);
static int32_t tsdbSetRepoEnv(STsdbRepo *pRepo);
static int32_t tsdbDestroyRepoEnv(STsdbRepo *pRepo);
static int     tsdbOpenMetaFile(char *tsdbDir);
static int     tsdbRecoverRepo(int fd, STsdbCfg *pCfg);
static int32_t tsdbInsertDataToTable(tsdb_repo_t *repo, SSubmitBlock *pBlock);

#define TSDB_GET_TABLE_BY_ID(pRepo, sid) (((STSDBRepo *)pRepo)->pTableList)[sid]
#define TSDB_GET_TABLE_BY_NAME(pRepo, name)
#define TSDB_IS_REPO_ACTIVE(pRepo) ((pRepo)->state == TSDB_REPO_STATE_ACTIVE)
#define TSDB_IS_REPO_CLOSED(pRepo) ((pRepo)->state == TSDB_REPO_STATE_CLOSED)

STsdbCfg *tsdbCreateDefaultCfg() {
  STsdbCfg *pCfg = (STsdbCfg *)malloc(sizeof(STsdbCfg));
  if (pCfg == NULL) return NULL;

  pCfg->precision = -1;
  pCfg->tsdbId = 0;
  pCfg->maxTables = -1;
  pCfg->daysPerFile = -1;
  pCfg->minRowsPerFileBlock = -1;
  pCfg->maxRowsPerFileBlock = -1;
  pCfg->keep = -1;
  pCfg->maxCacheSize = -1;

  return pCfg;
}

void tsdbFreeCfg(STsdbCfg *pCfg) {
  if (pCfg != NULL) free(pCfg);
}

tsdb_repo_t *tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg, void *limiter) {
  if (rootDir == NULL) return NULL;

  if (access(rootDir, F_OK | R_OK | W_OK) == -1) return NULL;

  if (tsdbCheckAndSetDefaultCfg(pCfg) < 0) {
    return NULL;
  }

  STsdbRepo *pRepo = (STsdbRepo *)malloc(sizeof(STsdbRepo));
  if (pRepo == NULL) {
    return NULL;
  }

  pRepo->rootDir = strdup(rootDir);
  pRepo->config = *pCfg;
  pRepo->limiter = limiter;

  pRepo->tsdbMeta = tsdbCreateMeta(pCfg->maxTables);
  if (pRepo->tsdbMeta == NULL) {
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }

  pRepo->tsdbCache = tsdbCreateCache(5);
  if (pRepo->tsdbCache == NULL) {
    free(pRepo->rootDir);
    tsdbFreeMeta(pRepo->tsdbMeta);
    free(pRepo);
    return NULL;
  }

  // Create the Meta data file and data directory
  if (tsdbSetRepoEnv(pRepo) < 0) {
    free(pRepo->rootDir);
    tsdbFreeMeta(pRepo->tsdbMeta);
    tsdbFreeCache(pRepo->tsdbCache);
    free(pRepo);
    return NULL;
  }

  pRepo->state = TSDB_REPO_STATE_ACTIVE;

  return (tsdb_repo_t *)pRepo;
}

int32_t tsdbDropRepo(tsdb_repo_t *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  pRepo->state = TSDB_REPO_STATE_CLOSED;

  // Free the metaHandle
  tsdbFreeMeta(pRepo->tsdbMeta);

  // Free the cache
  tsdbFreeCache(pRepo->tsdbCache);

  // Destroy the repository info
  tsdbDestroyRepoEnv(pRepo);

  free(pRepo->rootDir);
  free(pRepo);

  return 0;
}

tsdb_repo_t *tsdbOpenRepo(char *tsdbDir) {
  if (access(tsdbDir, F_OK | W_OK | R_OK) < 0) {
    return NULL;
  }

  STsdbRepo *pRepo = (STsdbRepo *)malloc(sizeof(STsdbRepo));
  if (pRepo == NULL) {
    return NULL;
  }

  int fd = tsdbOpenMetaFile(tsdbDir);
  if (fd < 0) {
    free(pRepo);
    return NULL;
  }

  if (tsdbRecoverRepo(fd, &(pRepo->config)) < 0) {
    close(fd);
    free(pRepo);
    return NULL;
  }

  pRepo->tsdbCache = tsdbCreateCache(5);
  if (pRepo->tsdbCache == NULL) {
    // TODO: deal with error
    return NULL;
  }

  pRepo->rootDir = strdup(tsdbDir);
  pRepo->state = TSDB_REPO_STATE_ACTIVE;

  return (tsdb_repo_t *)pRepo;
}

static int32_t tsdbFlushCache(STsdbRepo *pRepo) {
  // TODO
  return 0;
}

int32_t tsdbCloseRepo(tsdb_repo_t *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  if (pRepo == NULL) return 0;

  pRepo->state = TSDB_REPO_STATE_CLOSED;

  tsdbFlushCache(pRepo);

  tsdbFreeMeta(pRepo->tsdbMeta);

  tsdbFreeCache(pRepo->tsdbCache);

  return 0;
}

int32_t tsdbConfigRepo(tsdb_repo_t *repo, STsdbCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  pRepo->config = *pCfg;
  // TODO
  return 0;
}

STsdbRepoInfo *tsdbGetStatus(tsdb_repo_t *pRepo) {
  // TODO
  return NULL;
}

int32_t tsdbCreateTable(tsdb_repo_t *repo, STableCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  return tsdbCreateTableImpl(pRepo->tsdbMeta, pCfg);
}

int32_t tsdbAlterTable(tsdb_repo_t *pRepo, STableCfg *pCfg) {
  // TODO
  return 0;
}

int32_t tsdbDropTable(tsdb_repo_t *repo, STableId tableId) {
  // TODO
  if (repo == NULL) return -1;
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  return tsdbDropTableImpl(pRepo->tsdbMeta, tableId);
}

STableInfo *tsdbGetTableInfo(tsdb_repo_t *pRepo, STableId tableId) {
  // TODO
  return NULL;
}

// TODO: need to return the number of data inserted
int32_t tsdbInsertData(tsdb_repo_t *repo, SSubmitMsg *pMsg) {
  SSubmitBlock *pBlock = (SSubmitBlock *)pMsg->data;

  for (int i = 0; i < pMsg->numOfTables; i++) {  // Loop to deal with the submit message
    if (tsdbInsertDataToTable(repo, pBlock) < 0) {
      return -1;
    }
    pBlock = (SSubmitBlock *)(((char *)pBlock) + sizeof(SSubmitBlock) + pBlock->len);
  }

  return 0;
}

// Check the configuration and set default options
static int32_t tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg) {
  // Check precision
  if (pCfg->precision == -1) {
    pCfg->precision = TSDB_DEFAULT_PRECISION;
  } else {
    if (!IS_VALID_PRECISION(pCfg->precision)) return -1;
  }

  // Check tsdbId
  if (pCfg->tsdbId < 0) return -1;

  // Check MaxTables
  if (pCfg->maxTables == -1) {
    pCfg->maxTables = TSDB_DEFAULT_TABLES;
  } else {
    if (pCfg->maxTables < TSDB_MIN_TABLES || pCfg->maxTables > TSDB_MAX_TABLES) return -1;
  }

  // Check daysPerFile
  if (pCfg->daysPerFile == -1) {
    pCfg->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  } else {
    if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) return -1;
  }

  // Check minRowsPerFileBlock and maxRowsPerFileBlock
  if (pCfg->minRowsPerFileBlock == -1) {
    pCfg->minRowsPerFileBlock = TSDB_DEFAULT_MIN_ROW_FBLOCK;
  } else {
    if (pCfg->minRowsPerFileBlock < TSDB_MIN_MIN_ROW_FBLOCK || pCfg->minRowsPerFileBlock > TSDB_MAX_MIN_ROW_FBLOCK)
      return -1;
  }

  if (pCfg->maxRowsPerFileBlock == -1) {
    pCfg->maxRowsPerFileBlock = TSDB_DEFAULT_MAX_ROW_FBLOCK;
  } else {
    if (pCfg->maxRowsPerFileBlock < TSDB_MIN_MAX_ROW_FBLOCK || pCfg->maxRowsPerFileBlock > TSDB_MAX_MAX_ROW_FBLOCK)
      return -1;
  }

  if (pCfg->minRowsPerFileBlock > pCfg->maxRowsPerFileBlock) return -1;

  // Check keep
  if (pCfg->keep == -1) {
    pCfg->keep = TSDB_DEFAULT_KEEP;
  } else {
    if (pCfg->keep < TSDB_MIN_KEEP || pCfg->keep > TSDB_MAX_KEEP) return -1;
  }

  // Check maxCacheSize
  if (pCfg->maxCacheSize == -1) {
    pCfg->maxCacheSize = TSDB_DEFAULT_CACHE_SIZE;
  } else {
    if (pCfg->maxCacheSize < TSDB_MIN_CACHE_SIZE || pCfg->maxCacheSize > TSDB_MAX_CACHE_SIZE) return -1;
  }

  return 0;
}

static int32_t tsdbSetRepoEnv(STsdbRepo *pRepo) {
  char *metaFname = tsdbGetFileName(pRepo->rootDir, "tsdb", TSDB_FILE_TYPE_META);

  int fd = open(metaFname, O_WRONLY | O_CREAT);
  if (fd < 0) {
    return -1;
  }

  if (write(fd, (void *)(&(pRepo->config)), sizeof(STsdbCfg)) < 0) {
    return -1;
  }

  // Create the data file
  char *dirName = calloc(1, strlen(pRepo->rootDir) + strlen("tsdb") + 2);
  if (dirName == NULL) {
    return -1;
  }

  sprintf(dirName, "%s/%s", pRepo->rootDir, "tsdb");
  if (mkdir(dirName, 0755) < 0) {
    free(dirName);
    return -1;
  }

  free(dirName);

  return 0;
}

static int32_t tsdbDestroyRepoEnv(STsdbRepo *pRepo) {
  char fname[128];
  if (pRepo == NULL) return 0;
  char *dirName = calloc(1, strlen(pRepo->rootDir) + strlen("tsdb") + 2);
  if (dirName == NULL) {
    return -1;
  }

  sprintf(dirName, "%s/%s", pRepo->rootDir, "tsdb");

  DIR *dir = opendir(dirName);
  if (dir == NULL) return -1;

  struct dirent *dp;
  while ((dp = readdir(dir)) != NULL) {
    if ((strcmp(dp->d_name, ".") == 0) || (strcmp(dp->d_name, "..") == 0)) continue;
    sprintf(fname, "%s/%s", pRepo->rootDir, dp->d_name);
    remove(fname);
  }

  closedir(dir);

  rmdir(dirName);

  char *metaFname = tsdbGetFileName(pRepo->rootDir, "tsdb", TSDB_FILE_TYPE_META);
  remove(metaFname);

  return 0;
}

static int tsdbOpenMetaFile(char *tsdbDir) {
  // TODO
  return 0;
}

static int tsdbRecoverRepo(int fd, STsdbCfg *pCfg) {
  // TODO: read tsdb configuration from file
  // recover tsdb meta
  return 0;
}

static int32_t tdInsertRowToTable(STsdbRepo *pRepo, SDataRow row, STable *pTable) {
  // TODO
  int32_t level = 0;
  int32_t headSize = 0;

  tSkipListRandNodeInfo(pTable->content.pData, &level, &headSize);

  // Copy row into the memory
  SSkipListNode *pNode = tsdbAllocFromCache(pRepo->tsdbCache, headSize + dataRowLen(row));
  if (pNode == NULL) {
    // TODO: deal with allocate failure
  }

  pNode->level = level;
  tdDataRowCpy(SL_GET_NODE_DATA(pNode), row);

  // Insert the skiplist node into the data
  tsdbInsertRowToTableImpl(pNode, pTable);

  return 0;
}

static int32_t tsdbInsertDataToTable(tsdb_repo_t *repo, SSubmitBlock *pBlock) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  STable *pTable = tsdbIsValidTableToInsert(pRepo->tsdbMeta, pBlock->tableId);
  if (pTable == NULL) {
    return -1;
  }

  SDataRows     rows = pBlock->data;
  SDataRowsIter rDataIter, *pIter;
  pIter = &rDataIter;
  SDataRow row;

  tdInitSDataRowsIter(rows, pIter);
  while ((row = tdDataRowsNext(pIter)) != NULL) {
    if (tdInsertRowToTable(pRepo, row, pTable) < 0) {
      // TODO: deal with the error here
    }
  }

  return 0;
}