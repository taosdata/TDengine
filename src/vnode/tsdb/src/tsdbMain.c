#include <stdio.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>

// #include "taosdef.h"
// #include "disk.h"
#include "tsdb.h"
#include "tsdbCache.h"
#include "tsdbFile.h"
#include "tsdbMeta.h"

enum { TSDB_REPO_STATE_ACTIVE, TSDB_REPO_STATE_CLOSED, TSDB_REPO_STATE_CONFIGURING };

typedef struct _tsdb_repo {
  char *rootDir;
  // TSDB configuration
  STsdbCfg config;

  // The meter meta handle of this TSDB repository
  STsdbMeta *tsdbMeta;

  // The cache Handle
  SCacheHandle *tsdbCache;

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

#define TSDB_GET_TABLE_BY_ID(pRepo, sid) (((STSDBRepo *)pRepo)->pTableList)[sid]
#define TSDB_GET_TABLE_BY_NAME(pRepo, name)
#define TSDB_IS_REPO_ACTIVE(pRepo) ((pRepo)->state == TSDB_REPO_STATE_ACTIVE)
#define TSDB_IS_REPO_CLOSED(pRepo) ((pRepo)->state == TSDB_REPO_STATE_CLOSED)

tsdb_repo_t *tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg, void *limiter) {

  if (rootDir == NULL) return NULL;

  if (access(rootDir, F_OK|R_OK|W_OK) == -1) return NULL;

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

int32_t tsdbDropTable(tsdb_repo_t *pRepo, STableId tid) {
  return 0;
}

STableInfo *tsdbGetTableInfo(tsdb_repo_t *pRepo, STableId tid) {
  // TODO
  return NULL;
}

int32_t tsdbInsertData(tsdb_repo_t *repo, STableId tableId, char *pData) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  tsdbInsertDataImpl(pRepo->tsdbMeta, tableId, pData);

  return 0;
}

// Check the configuration and set default options
static int32_t tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg) {
  // TODO
  return 0;
}

static int32_t tsdbSetRepoEnv(STsdbRepo *pRepo) {
  char *metaFname = tsdbGetFileName(pRepo->rootDir, "tsdb", TSDB_FILE_TYPE_META);

  int fd = open(metaFname, O_WRONLY|O_CREAT);
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

  sprintf(dirName, "%s/%s", pRepo->rootDir, dirName);
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