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

#define TSDB_CFG_FILE_NAME "CONFIG"
#define TSDB_DATA_DIR_NAME "data"
#define TSDB_DEFAULT_FILE_BLOCK_ROW_OPTION 0.7

enum { TSDB_REPO_STATE_ACTIVE, TSDB_REPO_STATE_CLOSED, TSDB_REPO_STATE_CONFIGURING };

typedef struct _tsdb_repo {
  char *rootDir;
  // TSDB configuration
  STsdbCfg config;

  // The meter meta handle of this TSDB repository
  STsdbMeta *tsdbMeta;

  // The cache Handle
  STsdbCache *tsdbCache;

  // The TSDB file handle
  STsdbFileH *tsdbFileH;

  // Disk tier handle for multi-tier storage
  void *diskTier;

  pthread_mutex_t mutex;

  int commit;
  pthread_t commitThread;

  // A limiter to monitor the resources used by tsdb
  void *limiter;

  int8_t state;

} STsdbRepo;

static int32_t tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg);
static int32_t tsdbSetRepoEnv(STsdbRepo *pRepo);
static int32_t tsdbDestroyRepoEnv(STsdbRepo *pRepo);
static int     tsdbOpenMetaFile(char *tsdbDir);
static int32_t tsdbInsertDataToTable(tsdb_repo_t *repo, SSubmitBlk *pBlock);
static int32_t tsdbRestoreCfg(STsdbRepo *pRepo, STsdbCfg *pCfg);
static int32_t tsdbGetDataDirName(STsdbRepo *pRepo, char *fname);
static void *  tsdbCommitToFile(void *arg);

#define TSDB_GET_TABLE_BY_ID(pRepo, sid) (((STSDBRepo *)pRepo)->pTableList)[sid]
#define TSDB_GET_TABLE_BY_NAME(pRepo, name)
#define TSDB_IS_REPO_ACTIVE(pRepo) ((pRepo)->state == TSDB_REPO_STATE_ACTIVE)
#define TSDB_IS_REPO_CLOSED(pRepo) ((pRepo)->state == TSDB_REPO_STATE_CLOSED)

/**
 * Set the default TSDB configuration
 */
void tsdbSetDefaultCfg(STsdbCfg *pCfg) {
  if (pCfg == NULL) return;

  pCfg->precision = -1;
  pCfg->tsdbId = 0;
  pCfg->maxTables = -1;
  pCfg->daysPerFile = -1;
  pCfg->minRowsPerFileBlock = -1;
  pCfg->maxRowsPerFileBlock = -1;
  pCfg->keep = -1;
  pCfg->maxCacheSize = -1;
}

/**
 * Create a configuration for TSDB default
 * @return a pointer to a configuration. the configuration object 
 *         must call tsdbFreeCfg to free memory after usage
 */
STsdbCfg *tsdbCreateDefaultCfg() {
  STsdbCfg *pCfg = (STsdbCfg *)malloc(sizeof(STsdbCfg));
  if (pCfg == NULL) return NULL;

  tsdbSetDefaultCfg(pCfg);

  return pCfg;
}

void tsdbFreeCfg(STsdbCfg *pCfg) {
  if (pCfg != NULL) free(pCfg);
}

/**
 * Create a new TSDB repository
 * @param rootDir the TSDB repository root directory
 * @param pCfg the TSDB repository configuration, upper layer need to free the pointer
 * @param limiter the limitation tracker will implement in the future, make it void now
 *
 * @return a TSDB repository handle on success, NULL for failure
 */
tsdb_repo_t *tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg, void *limiter /* TODO */) {
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

  // Create the environment files and directories
  if (tsdbSetRepoEnv(pRepo) < 0) {
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }

  // Initialize meta
  STsdbMeta *pMeta = tsdbInitMeta(rootDir, pCfg->maxTables);
  if (pMeta == NULL) {
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }
  pRepo->tsdbMeta = pMeta;

  // Initialize cache
  STsdbCache *pCache = tsdbInitCache(pCfg->maxCacheSize, -1);
  if (pCache == NULL) {
    free(pRepo->rootDir);
    tsdbFreeMeta(pRepo->tsdbMeta);
    free(pRepo);
    return NULL;
  }
  pRepo->tsdbCache = pCache;

  // Initialize file handle
  char dataDir[128] = "\0";
  tsdbGetDataDirName(pRepo, dataDir);
  pRepo->tsdbFileH =
      tsdbInitFile(dataDir, pCfg->daysPerFile, pCfg->keep, pCfg->minRowsPerFileBlock, pCfg->maxRowsPerFileBlock, pCfg->maxTables);
  if (pRepo->tsdbFileH == NULL) {
    free(pRepo->rootDir);
    tsdbFreeCache(pRepo->tsdbCache);
    tsdbFreeMeta(pRepo->tsdbMeta);
    free(pRepo);
    return NULL;
  }

  pRepo->state = TSDB_REPO_STATE_ACTIVE;

  return (tsdb_repo_t *)pRepo;
}

/**
 * Close and free all resources taken by the repository
 * @param repo the TSDB repository handle. The interface will free the handle too, so upper
 *              layer do NOT need to free the repo handle again.
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
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

/**
 * Open an existing TSDB storage repository
 * @param tsdbDir the existing TSDB root directory
 *
 * @return a TSDB repository handle on success, NULL for failure and the error number is set
 */
tsdb_repo_t *tsdbOpenRepo(char *tsdbDir) {
  if (access(tsdbDir, F_OK | W_OK | R_OK) < 0) {
    return NULL;
  }

  STsdbRepo *pRepo = (STsdbRepo *)malloc(sizeof(STsdbRepo));
  if (pRepo == NULL) {
    return NULL;
  }

  pRepo->rootDir = strdup(tsdbDir);

  tsdbRestoreCfg(pRepo, &(pRepo->config));

  pRepo->tsdbMeta = tsdbInitMeta(tsdbDir, pRepo->config.maxTables);
  if (pRepo->tsdbMeta == NULL) {
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }

  pRepo->tsdbCache = tsdbInitCache(pRepo->config.maxCacheSize, -1);
  if (pRepo->tsdbCache == NULL) {
    tsdbFreeMeta(pRepo->tsdbMeta);
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }

  pRepo->state = TSDB_REPO_STATE_ACTIVE;

  return (tsdb_repo_t *)pRepo;
}

static int32_t tsdbFlushCache(STsdbRepo *pRepo) {
  // TODO
  return 0;
}

/**
 * Close a TSDB repository. Only free memory resources, and keep the files.
 * @param repo the opened TSDB repository handle. The interface will free the handle too, so upper
 *              layer do NOT need to free the repo handle again.
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbCloseRepo(tsdb_repo_t *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  if (pRepo == NULL) return 0;

  pRepo->state = TSDB_REPO_STATE_CLOSED;

  tsdbFlushCache(pRepo);

  tsdbFreeMeta(pRepo->tsdbMeta);

  tsdbFreeCache(pRepo->tsdbCache);

  return 0;
}

/**
 * Change the configuration of a repository
 * @param pCfg the repository configuration, the upper layer should free the pointer
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbConfigRepo(tsdb_repo_t *repo, STsdbCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  pRepo->config = *pCfg;
  // TODO
  return 0;
}

int32_t tsdbTriggerCommit(tsdb_repo_t *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  if (pthread_mutex_lock(&(pRepo->mutex)) < 0) return -1;
  if (pRepo->commit) return 0;
  pRepo->commit = 1;
  // Loop to move pData to iData
  for (int i = 0; i < pRepo->config.maxTables; i++) {
    STable *pTable = pRepo->tsdbMeta->tables[i];
    if (pTable != NULL && pTable->mem != NULL) {
      pTable->imem = pTable->mem;
      pTable->mem = NULL;
    }
  }
  // TODO: Loop to move mem to imem
  pRepo->tsdbCache->imem = pRepo->tsdbCache->mem;
  pRepo->tsdbCache->mem = NULL;

  pthread_create(&(pRepo->commitThread), NULL, tsdbCommitToFile, (void *)repo);
  pthread_mutex_unlock(&(pRepo->mutex));

  pthread_join(pRepo->commitThread, NULL);

  return 0;
}

/**
 * Get the TSDB repository information, including some statistics
 * @param pRepo the TSDB repository handle
 * @param error the error number to set when failure occurs
 *
 * @return a info struct handle on success, NULL for failure and the error number is set. The upper
 *         layers should free the info handle themselves or memory leak will occur
 */
STsdbRepoInfo *tsdbGetStatus(tsdb_repo_t *pRepo) {
  // TODO
  return NULL;
}

int tsdbCreateTable(tsdb_repo_t *repo, STableCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  return tsdbCreateTableImpl(pRepo->tsdbMeta, pCfg);
}

int tsdbAlterTable(tsdb_repo_t *pRepo, STableCfg *pCfg) {
  // TODO
  return 0;
}

int tsdbDropTable(tsdb_repo_t *repo, STableId tableId) {
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
  SSubmitMsgIter msgIter;

  tsdbInitSubmitMsgIter(pMsg, &msgIter);
  SSubmitBlk *pBlock;
  while ((pBlock = tsdbGetSubmitMsgNext(&msgIter)) != NULL) {
    if (tsdbInsertDataToTable(repo, pBlock) < 0) {
      return -1;
    }
  }

  return 0;
}

/**
 * Initialize a table configuration
 */
int tsdbInitTableCfg(STableCfg *config, TSDB_TABLE_TYPE type, int64_t uid, int32_t tid) {
  if (config == NULL) return -1;
  if (type != TSDB_NORMAL_TABLE && type != TSDB_CHILD_TABLE) return -1;

  memset((void *)config, 0, sizeof(STableCfg));

  config->type = type;
  config->superUid = TSDB_INVALID_SUPER_TABLE_ID;
  config->tableId.uid = uid;
  config->tableId.tid = tid;
  return 0;
}

/**
 * Set the super table UID of the created table
 */
int tsdbTableSetSuperUid(STableCfg *config, int64_t uid) {
  if (config->type != TSDB_CHILD_TABLE) return -1;
  if (uid == TSDB_INVALID_SUPER_TABLE_ID) return -1;

  config->superUid = uid;
  return 0;
}

/**
 * Set the table schema in the configuration
 * @param config the configuration to set
 * @param pSchema the schema to set
 * @param dup use the schema directly or duplicate one for use
 * 
 * @return 0 for success and -1 for failure
 */
int tsdbTableSetSchema(STableCfg *config, STSchema *pSchema, bool dup) {
  if (dup) {
    config->schema = tdDupSchema(pSchema);
  } else {
    config->schema = pSchema;
  }
  return 0;
}

/**
 * Set the table schema in the configuration
 * @param config the configuration to set
 * @param pSchema the schema to set
 * @param dup use the schema directly or duplicate one for use
 * 
 * @return 0 for success and -1 for failure
 */
int tsdbTableSetTagSchema(STableCfg *config, STSchema *pSchema, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) return -1;

  if (dup) {
    config->tagSchema = tdDupSchema(pSchema);
  } else {
    config->tagSchema = pSchema;
  }
  return 0;
}

int tsdbTableSetTagValue(STableCfg *config, SDataRow row, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) return -1;

  if (dup) {
    config->tagValues = tdDataRowDup(row);
  } else {
    config->tagValues = row;
  }

  return 0;
}

void tsdbClearTableCfg(STableCfg *config) {
  if (config->schema) tdFreeSchema(config->schema);
  if (config->tagSchema) tdFreeSchema(config->tagSchema);
  if (config->tagValues) tdFreeDataRow(config->tagValues);
}

int tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pBlock->len <= 0) return -1;
  pIter->totalLen = pBlock->len;
  pIter->len = 0;
  pIter->row = (SDataRow)(pBlock->data);
  return 0;
}

SDataRow tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter) {
  SDataRow row = pIter->row;
  if (row == NULL) return NULL;

  pIter->len += dataRowLen(row);
  if (pIter->len >= pIter->totalLen) {
    pIter->row = NULL;
  } else {
    pIter->row = (char *)row + dataRowLen(row);
  }

  return row;
}

int tsdbInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL || pIter == NULL) return -1;

  pIter->totalLen = pMsg->length;
  pIter->len = TSDB_SUBMIT_MSG_HEAD_SIZE;
  if (pMsg->length <= TSDB_SUBMIT_MSG_HEAD_SIZE) {
    pIter->pBlock = NULL;
  } else {
    pIter->pBlock = pMsg->blocks;
  }

  return 0;
}

SSubmitBlk *tsdbGetSubmitMsgNext(SSubmitMsgIter *pIter) {
  SSubmitBlk *pBlock = pIter->pBlock;
  if (pBlock == NULL) return NULL;

  pIter->len = pIter->len + sizeof(SSubmitBlk) + pBlock->len;
  if (pIter->len >= pIter->totalLen) {
    pIter->pBlock = NULL;
  } else {
    pIter->pBlock = (SSubmitBlk *)((char *)pBlock + pBlock->len + sizeof(SSubmitBlk));
  }

  return pBlock;
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

  // Check maxTables
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

static int32_t tsdbGetCfgFname(STsdbRepo *pRepo, char *fname) {
  if (pRepo == NULL) return -1;
  sprintf(fname, "%s/%s", pRepo->rootDir, TSDB_CFG_FILE_NAME);
  return 0;
}

static int32_t tsdbSaveConfig(STsdbRepo *pRepo) {
  char fname[128] = "\0"; // TODO: get rid of the literal 128

  if (tsdbGetCfgFname(pRepo, fname) < 0) return -1;

  int fd = open(fname, O_WRONLY | O_CREAT, 0755);
  if (fd < 0) {
    return -1;
  }

  if (write(fd, (void *)(&(pRepo->config)), sizeof(STsdbCfg)) < 0) {
    return -1;
  }

  close(fd);
  return 0;
}

static int32_t tsdbRestoreCfg(STsdbRepo *pRepo, STsdbCfg *pCfg) {
  char fname[128] = "\0";

  if (tsdbGetCfgFname(pRepo, fname) < 0) return -1;

  int fd = open(fname, O_RDONLY);
  if (fd < 0) {
    return -1;
  }

  if (read(fd, (void *)pCfg, sizeof(STsdbCfg)) < sizeof(STsdbCfg)) {
    close(fd);
    return -1;
  }

  close(fd);

  return 0;
}

static int32_t tsdbGetDataDirName(STsdbRepo *pRepo, char *fname) {
  if (pRepo == NULL || pRepo->rootDir == NULL) return -1;
  sprintf(fname, "%s/%s", pRepo->rootDir, TSDB_DATA_DIR_NAME);
  return 0;
}

static int32_t tsdbSetRepoEnv(STsdbRepo *pRepo) {
  if (tsdbSaveConfig(pRepo) < 0) return -1;

  char dirName[128] = "\0";
  if (tsdbGetDataDirName(pRepo, dirName) < 0) return -1;

  if (mkdir(dirName, 0755) < 0) {
    return -1;
  }

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

  return 0;
}

static int tsdbOpenMetaFile(char *tsdbDir) {
  // TODO
  return 0;
}

static int32_t tdInsertRowToTable(STsdbRepo *pRepo, SDataRow row, STable *pTable) {
  // TODO
  int32_t level = 0;
  int32_t headSize = 0;

  if (pTable->mem == NULL) {
    pTable->mem = (SMemTable *)calloc(1, sizeof(SMemTable));
    if (pTable->mem == NULL) return -1;
    pTable->mem->pData = tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP], 0, 0, getTupleKey);
    pTable->mem->keyFirst = INT64_MAX;
    pTable->mem->keyLast = 0;
  }

  tSkipListRandNodeInfo(pTable->mem->pData, &level, &headSize);

  TSKEY key = dataRowKey(row);
  // Copy row into the memory
  SSkipListNode *pNode = tsdbAllocFromCache(pRepo->tsdbCache, headSize + dataRowLen(row), key);
  if (pNode == NULL) {
    // TODO: deal with allocate failure
  }

  pNode->level = level;
  dataRowCpy(SL_GET_NODE_DATA(pNode), row);

  // Insert the skiplist node into the data
  tSkipListPut(pTable->mem->pData, pNode);
  if (key > pTable->mem->keyLast) pTable->mem->keyLast = key;
  if (key < pTable->mem->keyFirst) pTable->mem->keyFirst = key;
  pTable->mem->numOfPoints++;

  return 0;
}

static int32_t tsdbInsertDataToTable(tsdb_repo_t *repo, SSubmitBlk *pBlock) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  STable *pTable = tsdbIsValidTableToInsert(pRepo->tsdbMeta, pBlock->tableId);
  if (pTable == NULL) return -1;

  SSubmitBlkIter blkIter;
  SDataRow row;

  tsdbInitSubmitBlkIter(pBlock, &blkIter);
  while ((row = tsdbGetSubmitBlkNext(&blkIter)) != NULL) {
    if (tdInsertRowToTable(pRepo, row, pTable) < 0) {
      return -1;
    }
  }

  return 0;
}

static int tsdbReadRowsFromCache(SSkipListIterator *pIter, TSKEY minKey, TSKEY maxKey, int maxRowsToRead, void *dst) {
  int numOfRows = 0;
  do {
    SSkipListNode *node = tSkiplistIterGet(pIter);
    SDataRow       row = SL_GET_NODE_DATA(node);
    if (dataRowKey(row) > maxKey) break;
  } while (tSkipListIterNext(pIter));
  return numOfRows;
}

// Commit to file
static void *tsdbCommitToFile(void *arg) {
  // TODO
  STsdbRepo * pRepo = (STsdbRepo *)arg;
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbCache *pCache = pRepo->tsdbCache;
  STsdbRepo * pCfg = &(pRepo->config);
  if (pCache->imem == NULL) return;

  int                 sfid = tsdbGetKeyFileId(pCache->imem->keyFirst);
  int                 efid = tsdbGetKeyFileId(pCache->imem->keyLast);
  SSkipListIterator **iters = (SSkipListIterator **)calloc(pCfg->maxTables, sizeof(SSkipListIterator *));
  if (iters == NULL) {
    // TODO: deal with the error
    return NULL;
  }

  for (int fid = sfid; fid <= efid; fid++) {
    TSKEY minKey = 0, maxKey = 0;
    tsdbGetKeyRangeOfFileId(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

    for (int tid = 0; tid < pCfg->maxTables; tid++) {
      STable *pTable = pMeta->tables[tid];
      if (pTable == NULL || pTable->imem == NULL) continue;
      if (iters[tid] == NULL) {  // create table iterator
        iters[tid] = tSkipListCreateIter(pTable->imem);
        // TODO: deal with the error
        if (iters[tid] == NULL) break;
        if (!tSkipListIterNext(iters[tid])) {
          // assert(0);
        }
      }

      // Loop the iterator
      // tsdbReadRowsFromCache();
    }
  }

  // Free the iterator
  for (int tid = 0; tid < pCfg->maxTables; tid++) {
    if (iters[tid] != NULL) tSkipListDestroyIter(iters[tid]);
  }

  free(iters);

  return NULL;
}