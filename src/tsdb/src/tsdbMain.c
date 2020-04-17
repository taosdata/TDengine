#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <tlog.h>
#include <unistd.h>

// #include "taosdef.h"
// #include "disk.h"
#include "os.h"
#include "talgo.h"
#include "tsdb.h"
#include "tsdbMain.h"

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
#define TSDB_MAX_LAST_FILE_SIZE (1024 * 1024 * 10) // 10M

enum { TSDB_REPO_STATE_ACTIVE, TSDB_REPO_STATE_CLOSED, TSDB_REPO_STATE_CONFIGURING };

static int32_t tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg);
static int32_t tsdbSetRepoEnv(STsdbRepo *pRepo);
static int32_t tsdbDestroyRepoEnv(STsdbRepo *pRepo);
// static int     tsdbOpenMetaFile(char *tsdbDir);
static int32_t tsdbInsertDataToTable(tsdb_repo_t *repo, SSubmitBlk *pBlock);
static int32_t tsdbRestoreCfg(STsdbRepo *pRepo, STsdbCfg *pCfg);
static int32_t tsdbGetDataDirName(STsdbRepo *pRepo, char *fname);
static void *  tsdbCommitData(void *arg);
static int     tsdbCommitToFile(STsdbRepo *pRepo, int fid, SSkipListIterator **iters, SDataCols *pCols);
static int     tsdbHasDataInRange(SSkipListIterator *pIter, TSKEY minKey, TSKEY maxKey);
static int     tsdbHasDataToCommit(SSkipListIterator **iters, int nIters, TSKEY minKey, TSKEY maxKey);
static int tsdbWriteBlockToFileImpl(SFile *pFile, SDataCols *pCols, int pointsToWrite, int64_t *offset, int32_t *len,
                                    int64_t uid);

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
int32_t tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg, void *limiter /* TODO */) {

  if (mkdir(rootDir, 0755) != 0) {
    if (errno == EACCES) {
      return TSDB_CODE_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_SERV_NO_DISKSPACE;
    } else if (errno == EEXIST) {
    } else {
      return TSDB_CODE_VG_INIT_FAILED;
    }
  }

  if (access(rootDir, F_OK | R_OK | W_OK) == -1) return -1;

  if (tsdbCheckAndSetDefaultCfg(pCfg) < 0) {
    return -1;
  }

  STsdbRepo *pRepo = (STsdbRepo *)malloc(sizeof(STsdbRepo));
  if (pRepo == NULL) {
    return -1;
  }

  pRepo->rootDir = strdup(rootDir);
  pRepo->config = *pCfg;
  pRepo->limiter = limiter;

  // Create the environment files and directories
  int32_t code = tsdbSetRepoEnv(pRepo);
  
  free(pRepo->rootDir);
  free(pRepo);
  return code;
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
tsdb_repo_t *tsdbOpenRepo(char *tsdbDir, STsdbAppH *pAppH) {
  char dataDir[128] = "\0";
  if (access(tsdbDir, F_OK | W_OK | R_OK) < 0) {
    return NULL;
  }

  STsdbRepo *pRepo = (STsdbRepo *)calloc(1, sizeof(STsdbRepo));
  if (pRepo == NULL) {
    return NULL;
  }

  pRepo->rootDir = strdup(tsdbDir);

  tsdbRestoreCfg(pRepo, &(pRepo->config));
  if (pAppH) pRepo->appH = *pAppH;

  pRepo->tsdbMeta = tsdbInitMeta(tsdbDir, pRepo->config.maxTables);
  if (pRepo->tsdbMeta == NULL) {
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }

  pRepo->tsdbCache = tsdbInitCache(pRepo->config.maxCacheSize, -1, (tsdb_repo_t *)pRepo);
  if (pRepo->tsdbCache == NULL) {
    tsdbFreeMeta(pRepo->tsdbMeta);
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }

  tsdbGetDataDirName(pRepo, dataDir);
  pRepo->tsdbFileH = tsdbInitFileH(dataDir, pRepo->config.maxTables);
  if (pRepo->tsdbFileH == NULL) {
    tsdbFreeCache(pRepo->tsdbCache);
    tsdbFreeMeta(pRepo->tsdbMeta);
    free(pRepo->rootDir);
    free(pRepo);
    return NULL;
  }

  pRepo->state = TSDB_REPO_STATE_ACTIVE;

  return (tsdb_repo_t *)pRepo;
}

// static int32_t tsdbFlushCache(STsdbRepo *pRepo) {
//   // TODO
//   return 0;
// }

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
  tsdbLockRepo(repo);
  if (pRepo->commit) {
    tsdbUnLockRepo(repo);
    return -1;
  }
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
  pRepo->tsdbCache->curBlock = NULL;
  tsdbUnLockRepo(repo);

  tsdbCommitData((void *)repo);

  tsdbCloseFileH(pRepo->tsdbFileH);

  tsdbFreeMeta(pRepo->tsdbMeta);

  tsdbFreeCache(pRepo->tsdbCache);

  tfree(pRepo->rootDir);
  tfree(pRepo);

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
  
  tsdbLockRepo(repo);
  if (pRepo->commit) {
    tsdbUnLockRepo(repo);
    return -1;
  }
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
  pRepo->tsdbCache->curBlock = NULL;
  tsdbUnLockRepo(repo);

  // TODO: here should set as detached or use join for memory leak
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  pthread_create(&(pRepo->commitThread), &thattr, tsdbCommitData, (void *)repo);

  return 0;
}

int32_t tsdbLockRepo(tsdb_repo_t *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  return pthread_mutex_lock(&(pRepo->mutex));
}

int32_t tsdbUnLockRepo(tsdb_repo_t *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  return pthread_mutex_unlock(&(pRepo->mutex));
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
  SSubmitBlk *pBlock = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  
  while ((pBlock = tsdbGetSubmitMsgNext(&msgIter)) != NULL) {
    if ((code = tsdbInsertDataToTable(repo, pBlock)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}

/**
 * Initialize a table configuration
 */
int tsdbInitTableCfg(STableCfg *config, ETableType type, int64_t uid, int32_t tid) {
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

  pMsg->length = htonl(pMsg->length);
  pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);
  pMsg->compressed = htonl(pMsg->compressed);
  
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
  
  pBlock->len = htonl(pBlock->len);
  pBlock->numOfRows = htons(pBlock->numOfRows);
  pBlock->uid = htobe64(pBlock->uid);
  pBlock->tid = htonl(pBlock->tid);
  
  pBlock->sversion = htonl(pBlock->sversion);
  pBlock->padding = htonl(pBlock->padding);
  
  pIter->len = pIter->len + sizeof(SSubmitBlk) + pBlock->len;
  if (pIter->len >= pIter->totalLen) {
    pIter->pBlock = NULL;
  } else {
    pIter->pBlock = (SSubmitBlk *)((char *)pBlock + pBlock->len + sizeof(SSubmitBlk));
  }

  return pBlock;
}

STsdbMeta* tsdbGetMeta(tsdb_repo_t* pRepo) {
  STsdbRepo *tsdb = (STsdbRepo *)pRepo;
  return tsdb->tsdbMeta;
}

STsdbFileH* tsdbGetFile(tsdb_repo_t* pRepo) {
  STsdbRepo* tsdb = (STsdbRepo*) pRepo;
  return tsdb->tsdbFileH;
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
  char fname[260];
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

// static int tsdbOpenMetaFile(char *tsdbDir) {
//   // TODO
//   return 0;
// }

static int32_t tdInsertRowToTable(STsdbRepo *pRepo, SDataRow row, STable *pTable) {
  // TODO
  int32_t level = 0;
  int32_t headSize = 0;

  if (pTable->mem == NULL) {
    pTable->mem = (SMemTable *)calloc(1, sizeof(SMemTable));
    if (pTable->mem == NULL) return -1;
    pTable->mem->pData = tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP], 0, 0, 0, getTupleKey);
    pTable->mem->keyFirst = INT64_MAX;
    pTable->mem->keyLast = 0;
  }

  tSkipListNewNodeInfo(pTable->mem->pData, &level, &headSize);

  TSKEY key = dataRowKey(row);
  // printf("insert:%lld, size:%d\n", key, pTable->mem->numOfPoints);
  
  // Copy row into the memory
  SSkipListNode *pNode = tsdbAllocFromCache(pRepo->tsdbCache, headSize + dataRowLen(row), key);
  if (pNode == NULL) {
    // TODO: deal with allocate failure
  }

  pNode->level = level;
  dataRowCpy(SL_GET_NODE_DATA(pNode), row);

  // Insert the skiplist node into the data
  if (pTable->mem == NULL) {
    pTable->mem = (SMemTable *)calloc(1, sizeof(SMemTable));
    if (pTable->mem == NULL) return -1;
    pTable->mem->pData = tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP], 0, 0, 0, getTupleKey);
    pTable->mem->keyFirst = INT64_MAX;
    pTable->mem->keyLast = 0;
  }
  tSkipListPut(pTable->mem->pData, pNode);
  if (key > pTable->mem->keyLast) pTable->mem->keyLast = key;
  if (key < pTable->mem->keyFirst) pTable->mem->keyFirst = key;
  
  pTable->mem->numOfPoints = tSkipListGetSize(pTable->mem->pData);

  return 0;
}

static int32_t tsdbInsertDataToTable(tsdb_repo_t *repo, SSubmitBlk *pBlock) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  STableId tableId = {.uid = pBlock->uid, .tid = pBlock->tid};
  STable *pTable = tsdbIsValidTableToInsert(pRepo->tsdbMeta, tableId);
  if (pTable == NULL) {
    dError("failed to get table for insert, uid:%" PRIu64 ", tid:%d", tableId.uid, tableId.tid);
    return TSDB_CODE_INVALID_TABLE_ID;
  }

  SSubmitBlkIter blkIter;
  SDataRow row;

  tsdbInitSubmitBlkIter(pBlock, &blkIter);
  while ((row = tsdbGetSubmitBlkNext(&blkIter)) != NULL) {
    if (tdInsertRowToTable(pRepo, row, pTable) < 0) {
      return -1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int tsdbReadRowsFromCache(SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead, SDataCols *pCols) {
  int numOfRows = 0;

  do {
    if (numOfRows >= maxRowsToRead) break;

    SSkipListNode *node = tSkipListIterGet(pIter);
    if (node == NULL) break;

    SDataRow row = SL_GET_NODE_DATA(node);
    if (dataRowKey(row) > maxKey) break;

    tdAppendDataRowToDataCol(row, pCols);
    numOfRows++;
  } while (tSkipListIterNext(pIter));

  return numOfRows;
}

static void tsdbDestroyTableIters(SSkipListIterator **iters, int maxTables) {
  if (iters == NULL) return;

  for (int tid = 0; tid < maxTables; tid++) {
    if (iters[tid] == NULL) continue;
    tSkipListDestroyIter(iters[tid]);
  }

  free(iters);
}

static SSkipListIterator **tsdbCreateTableIters(STsdbMeta *pMeta, int maxTables) {
  SSkipListIterator **iters = (SSkipListIterator **)calloc(maxTables, sizeof(SSkipListIterator *));
  if (iters == NULL) return NULL;

  for (int tid = 0; tid < maxTables; tid++) {
    STable *pTable = pMeta->tables[tid];
    if (pTable == NULL || pTable->imem == NULL) continue;

    iters[tid] = tSkipListCreateIter(pTable->imem->pData);
    if (iters[tid] == NULL) {
      tsdbDestroyTableIters(iters, maxTables);
      return NULL;
    }

    if (!tSkipListIterNext(iters[tid])) {
      // No data in this iterator
      tSkipListDestroyIter(iters[tid]);
      iters[tid] = NULL;
    }
  }

  return iters;
}

static void tsdbFreeMemTable(SMemTable *pMemTable) {
  if (pMemTable) {
    tSkipListDestroy(pMemTable->pData);
    free(pMemTable);
  }
}

// Commit to file
static void *tsdbCommitData(void *arg) {
  // TODO
  printf("Starting to commit....\n");
  STsdbRepo * pRepo = (STsdbRepo *)arg;
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbCache *pCache = pRepo->tsdbCache;
  STsdbCfg * pCfg = &(pRepo->config);
  if (pCache->imem == NULL) return NULL;

  // Create the iterator to read from cache
  SSkipListIterator **iters = tsdbCreateTableIters(pMeta, pCfg->maxTables);
  if (iters == NULL) {
    // TODO: deal with the error
    return NULL;
  }

  // Create a data column buffer for commit
  SDataCols *pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock);
  if (pDataCols == NULL) {
    // TODO: deal with the error
    return NULL;
  }

  int sfid = tsdbGetKeyFileId(pCache->imem->keyFirst, pCfg->daysPerFile, pCfg->precision);
  int efid = tsdbGetKeyFileId(pCache->imem->keyLast, pCfg->daysPerFile, pCfg->precision);

  for (int fid = sfid; fid <= efid; fid++) {
    if (tsdbCommitToFile(pRepo, fid, iters, pDataCols) < 0) {
      // TODO: deal with the error here
      // assert(0);
    }
  }

  tdFreeDataCols(pDataCols);
  tsdbDestroyTableIters(iters, pCfg->maxTables);

  tsdbLockRepo(arg);
  tdListMove(pCache->imem->list, pCache->pool.memPool);
  tdListFree(pCache->imem->list);
  free(pCache->imem);
  pCache->imem = NULL;
  pRepo->commit = 0;
  // TODO: free the skiplist
  for (int i = 0; i < pCfg->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable && pTable->imem) {
      tsdbFreeMemTable(pTable->imem);
      pTable->imem = NULL;
    }
  }
  tsdbUnLockRepo(arg);

  return NULL;
}

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SSkipListIterator **iters, SDataCols *pCols) {
  int isNewLastFile = 0;

  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  STsdbCfg *  pCfg = &pRepo->config;
  SFile       hFile, lFile;
  SFileGroup *pGroup = NULL;
  SCompIdx *  pIndices = NULL;
  SCompInfo * pCompInfo = NULL;
  // size_t      compInfoSize = 0;
  // SCompBlock  compBlock;
  // SCompBlock *pBlock = &compBlock;

  TSKEY minKey = 0, maxKey = 0;
  tsdbGetKeyRangeOfFileId(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

  // Check if there are data to commit to this file
  int hasDataToCommit = tsdbHasDataToCommit(iters, pCfg->maxTables, minKey, maxKey);
  if (!hasDataToCommit) return 0;  // No data to commit, just return

  // TODO: make it more flexible
  pCompInfo = (SCompInfo *)malloc(sizeof(SCompInfo) + sizeof(SCompBlock) * 1000);

  // Create and open files for commit
  tsdbGetDataDirName(pRepo, dataDir);
  if (tsdbCreateFGroup(pFileH, dataDir, fid, pCfg->maxTables) < 0) { /* TODO */
  }
  pGroup = tsdbOpenFilesForCommit(pFileH, fid);
  if (pGroup == NULL) { /* TODO */
  }
  tsdbCreateFile(dataDir, fid, ".h", pCfg->maxTables, &hFile, 1, 1);
  tsdbOpenFile(&hFile, O_RDWR);
  if (0 /*pGroup->files[TSDB_FILE_TYPE_LAST].size > TSDB_MAX_LAST_FILE_SIZE*/) {
    // TODO: make it not to write the last file every time
    tsdbCreateFile(dataDir, fid, ".l", pCfg->maxTables, &lFile, 0, 0);
    isNewLastFile = 1;
  }

  // Load the SCompIdx
  pIndices = (SCompIdx *)malloc(sizeof(SCompIdx) * pCfg->maxTables);
  if (pIndices == NULL) { /* TODO*/
  }
  if (tsdbLoadCompIdx(pGroup, (void *)pIndices, pCfg->maxTables) < 0) { /* TODO */
  }

  lseek(hFile.fd, TSDB_FILE_HEAD_SIZE + sizeof(SCompIdx) * pCfg->maxTables, SEEK_SET);

  // Loop to commit data in each table
  for (int tid = 0; tid < pCfg->maxTables; tid++) {
    STable *           pTable = pMeta->tables[tid];
    SSkipListIterator *pIter = iters[tid];
    SCompIdx *         pIdx = &pIndices[tid];

    int nNewBlocks = 0;

    if (pTable == NULL || pIter == NULL) continue;

    /* If no new data to write for this table, just write the old data to new file
     * if there are.
     */
    if (!tsdbHasDataInRange(pIter, minKey, maxKey)) {
      // has old data
      if (pIdx->len > 0) {  
        goto _table_over;
        // if (isNewLastFile && pIdx->hasLast) {
        if (0) {
          // need to move the last block to new file
          if ((pCompInfo = (SCompInfo *)realloc((void *)pCompInfo, pIdx->len)) == NULL) { /* TODO */
          }
          if (tsdbLoadCompBlocks(pGroup, pIdx, (void *)pCompInfo) < 0) { /* TODO */
          }

          tdInitDataCols(pCols, tsdbGetTableSchema(pMeta, pTable));

          SCompBlock *pTBlock = TSDB_COMPBLOCK_AT(pCompInfo, pIdx->numOfSuperBlocks);
          int nBlocks = 0;

          TSDB_COMPBLOCK_GET_START_AND_SIZE(pCompInfo, pTBlock, nBlocks);

          SCompData tBlock;
          int64_t toffset;
          int32_t tlen;
          tsdbLoadDataBlock(&pGroup->files[TSDB_FILE_TYPE_LAST], pTBlock, nBlocks, pCols, &tBlock);

          tsdbWriteBlockToFileImpl(&lFile, pCols, pCols->numOfPoints, &toffset, &tlen, pTable->tableId.uid);
          pTBlock = TSDB_COMPBLOCK_AT(pCompInfo, pIdx->numOfSuperBlocks);
          pTBlock->offset = toffset;
          pTBlock->len = tlen;
          pTBlock->numOfPoints = pCols->numOfPoints;
          pTBlock->numOfSubBlocks = 1;

          pIdx->offset = lseek(hFile.fd, 0, SEEK_CUR);
          if (nBlocks > 1) {
            pIdx->len -= (sizeof(SCompBlock) * nBlocks);
          }
          write(hFile.fd, (void *)pCompInfo, pIdx->len);
        } else {
          pIdx->offset = lseek(hFile.fd, 0, SEEK_CUR);
          sendfile(pGroup->files[TSDB_FILE_TYPE_HEAD].fd, hFile.fd, NULL, pIdx->len);
          hFile.info.size += pIdx->len;
        }
      }
      continue;
    }

    pCompInfo->delimiter = TSDB_FILE_DELIMITER;
    pCompInfo->checksum = 0;
    pCompInfo->uid = pTable->tableId.uid;

    // Load SCompBlock part if neccessary
    // int isCompBlockLoaded = 0;
    if (0) {
    // if (pIdx->offset > 0) {
      if (pIdx->hasLast || tsdbHasDataInRange(pIter, minKey, pIdx->maxKey)) {
        // has last block || cache key overlap with commit key
        pCompInfo = (SCompInfo *)realloc((void *)pCompInfo, pIdx->len + sizeof(SCompBlock) * 100);
        if (tsdbLoadCompBlocks(pGroup, pIdx, (void *)pCompInfo) < 0) { /* TODO */
        }
        // if (pCompInfo->uid == pTable->tableId.uid) isCompBlockLoaded = 1;
      } else {
        // TODO: No need to load the SCompBlock part, just sendfile the SCompBlock part
        // and write those new blocks to it
      }
    }

    tdInitDataCols(pCols, tsdbGetTableSchema(pMeta, pTable));

    int maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5;
    while (1) {
      tsdbReadRowsFromCache(pIter, maxKey, maxRowsToRead, pCols);
      if (pCols->numOfPoints == 0) break;

      int pointsWritten = pCols->numOfPoints;
      // TODO: all write to the end of .data file
      int64_t toffset = 0;
      int32_t tlen = 0;
      tsdbWriteBlockToFileImpl(&pGroup->files[TSDB_FILE_TYPE_DATA], pCols, pCols->numOfPoints, &toffset, &tlen, pTable->tableId.uid);

      // Make the compBlock
      SCompBlock *pTBlock = pCompInfo->blocks + nNewBlocks++;
      pTBlock->offset = toffset;
      pTBlock->len = tlen;
      pTBlock->keyFirst = dataColsKeyFirst(pCols);
      pTBlock->keyLast = dataColsKeyLast(pCols);
      pTBlock->last = 0;
      pTBlock->algorithm = 0;
      pTBlock->numOfPoints = pCols->numOfPoints;
      pTBlock->sversion = pTable->sversion;
      pTBlock->numOfSubBlocks = 1;
      pTBlock->numOfCols = pCols->numOfCols;

      if (dataColsKeyLast(pCols) > pIdx->maxKey) pIdx->maxKey = dataColsKeyLast(pCols);

      tdPopDataColsPoints(pCols, pointsWritten);
      maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5 - pCols->numOfPoints;
    }


_table_over:
    // Write the SCompBlock part
    pIdx->offset = lseek(hFile.fd, 0, SEEK_END);
    if (pIdx->len > 0) {
      int bytes = tsendfile(hFile.fd, pGroup->files[TSDB_FILE_TYPE_HEAD].fd, NULL, pIdx->len);
      if (bytes < pIdx->len) {
        printf("Failed to send file, reason: %s\n", strerror(errno));
      }
      if (nNewBlocks > 0) {
        write(hFile.fd, (void *)(pCompInfo->blocks), sizeof(SCompBlock) * nNewBlocks);
        pIdx->len += (sizeof(SCompBlock) * nNewBlocks);
      }
    } else {
      if (nNewBlocks > 0) {
        write(hFile.fd, (void *)pCompInfo, sizeof(SCompInfo) + sizeof(SCompBlock) * nNewBlocks);
        pIdx->len += sizeof(SCompInfo) + sizeof(SCompBlock) * nNewBlocks;
      }
    }

    pIdx->checksum = 0;
    pIdx->numOfSuperBlocks += nNewBlocks;
    pIdx->hasLast = 0;
  }

  // Write the SCompIdx part
  if (lseek(hFile.fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) {/* TODO */}
  if (write(hFile.fd, (void *)pIndices, sizeof(SCompIdx) * pCfg->maxTables) < 0) {/* TODO */}

  // close the files
  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
    tsdbCloseFile(&pGroup->files[type]);
  }
  tsdbCloseFile(&hFile);
  if (isNewLastFile) tsdbCloseFile(&lFile);
  // TODO: replace the .head and .last file
  rename(hFile.fname, pGroup->files[TSDB_FILE_TYPE_HEAD].fname);
  pGroup->files[TSDB_FILE_TYPE_HEAD].info = hFile.info;
  if (isNewLastFile) {
    rename(lFile.fname, pGroup->files[TSDB_FILE_TYPE_LAST].fname);
    pGroup->files[TSDB_FILE_TYPE_LAST].info = lFile.info;
  }

  if (pIndices) free(pIndices);
  if (pCompInfo) free(pCompInfo);

  return 0;
}

static int tsdbHasDataInRange(SSkipListIterator *pIter, TSKEY minKey, TSKEY maxKey) {
  if (pIter == NULL) return 0;

  SSkipListNode *node = tSkipListIterGet(pIter);
  if (node == NULL) return 0;

  SDataRow row = SL_GET_NODE_DATA(node);
  if (dataRowKey(row) >= minKey && dataRowKey(row) <= maxKey) return 1;

  return 0;
}

static int tsdbHasDataToCommit(SSkipListIterator **iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    SSkipListIterator *pIter = iters[i];
    if (tsdbHasDataInRange(pIter, minKey, maxKey)) return 1;
  }
  return 0;
}

static int tsdbWriteBlockToFileImpl(SFile *pFile, SDataCols *pCols, int pointsToWrite, int64_t *offset, int32_t *len, int64_t uid) {
  size_t     size = sizeof(SCompData) + sizeof(SCompCol) * pCols->numOfCols;
  SCompData *pCompData = (SCompData *)malloc(size);
  if (pCompData == NULL) return -1;

  pCompData->delimiter = TSDB_FILE_DELIMITER;
  pCompData->uid = uid;
  pCompData->numOfCols = pCols->numOfCols;

  *offset = lseek(pFile->fd, 0, SEEK_END);
  *len = size;

  int toffset = size;
  for (int iCol = 0; iCol < pCols->numOfCols; iCol++) {
    SCompCol *pCompCol = pCompData->cols + iCol;
    SDataCol *pDataCol = pCols->cols + iCol;
    
    pCompCol->colId = pDataCol->colId;
    pCompCol->type = pDataCol->type;
    pCompCol->offset = toffset;

    // TODO: add compression
    pCompCol->len = TYPE_BYTES[pCompCol->type] * pointsToWrite;
    toffset += pCompCol->len;
  }

  // Write the block
  if (write(pFile->fd, (void *)pCompData, size) < 0) goto _err;
  *len += size;
  for (int iCol = 0; iCol < pCols->numOfCols; iCol++) {
    SDataCol *pDataCol = pCols->cols + iCol;
    SCompCol *pCompCol = pCompData->cols + iCol;
    if (write(pFile->fd, pDataCol->pData, pCompCol->len) < 0) goto _err;
    *len += pCompCol->len;
  }

  tfree(pCompData);
  return 0;

_err:
  tfree(pCompData);
  return -1;
}

static int compareKeyBlock(const void *arg1, const void *arg2) {
  TSKEY key = *(TSKEY *)arg1;
  SCompBlock *pBlock = (SCompBlock *)arg2;

  if (key < pBlock->keyFirst) {
    return -1;
  } else if (key > pBlock->keyLast) {
    return 1;
  }

  return 0;
}

int tsdbWriteBlockToFile(STsdbRepo *pRepo, SFileGroup *pGroup, SCompIdx *pIdx, SCompInfo *pCompInfo, SDataCols *pCols, SCompBlock *pCompBlock, SFile *lFile, int64_t uid) {
  STsdbCfg * pCfg = &(pRepo->config);
  SFile *    pFile = NULL;
  int        numOfPointsToWrite = 0;
  int64_t    offset = 0;
  int32_t    len = 0;

  memset((void *)pCompBlock, 0, sizeof(SCompBlock));

  if (pCompInfo == NULL) {
    // Just append the data block to .data or .l or .last file
    numOfPointsToWrite = pCols->numOfPoints;
    if (pCols->numOfPoints > pCfg->minRowsPerFileBlock) {  // Write to .data file
      pFile = &(pGroup->files[TSDB_FILE_TYPE_DATA]);
    } else {  // Write to .last or .l file
      pCompBlock->last = 1;
      if (lFile) {
        pFile = lFile;
      } else {
        pFile = &(pGroup->files[TSDB_FILE_TYPE_LAST]);
      }
    }
    tsdbWriteBlockToFileImpl(pFile, pCols, numOfPointsToWrite, &offset, &len, uid);
    pCompBlock->offset = offset;
    pCompBlock->len = len;
    pCompBlock->algorithm = 2;  // TODO : add to configuration
    pCompBlock->sversion = pCols->sversion;
    pCompBlock->numOfPoints = pCols->numOfPoints;
    pCompBlock->numOfSubBlocks = 1;
    pCompBlock->numOfCols = pCols->numOfCols;
    pCompBlock->keyFirst = dataColsKeyFirst(pCols);
    pCompBlock->keyLast = dataColsKeyLast(pCols);
  } else {
    // Need to merge the block to either the last block or the other block
    TSKEY       keyFirst = dataColsKeyFirst(pCols);
    SCompBlock *pMergeBlock = NULL;

    // Search the block to merge in
    void *ptr = taosbsearch((void *)&keyFirst, (void *)(pCompInfo->blocks), sizeof(SCompBlock), pIdx->numOfSuperBlocks,
                            compareKeyBlock, TD_GE);
    if (ptr == NULL) {
      // No block greater or equal than the key, but there are data in the .last file, need to merge the last file block
      // and merge the data
      pMergeBlock = TSDB_COMPBLOCK_AT(pCompInfo, pIdx->numOfSuperBlocks - 1);
    } else {
      pMergeBlock = (SCompBlock *)ptr;
    }

    if (pMergeBlock->last) {
      if (pMergeBlock->last + pCols->numOfPoints > pCfg->minRowsPerFileBlock) {
        // Need to load the data from .last and combine data in pCols to write to .data file

      } else { // Just append the block to .last or .l file
        if (lFile) {
          // read the block from .last file and merge with pCols, write to .l file

        } else {
          // tsdbWriteBlockToFileImpl();
        }
      }
    } else { // The block need to merge in .data file

    }

  }

  return numOfPointsToWrite;
}
