#include "os.h"
#include "tulog.h"
#include "talgo.h"
#include "tsdb.h"
#include "tsdbMain.h"
#include "tscompression.h"

#define TSDB_DEFAULT_PRECISION TSDB_PRECISION_MILLI  // default precision
#define IS_VALID_PRECISION(precision) (((precision) >= TSDB_PRECISION_MILLI) && ((precision) <= TSDB_PRECISION_NANO))
#define TSDB_DEFAULT_COMPRESSION TWO_STAGE_COMP
#define IS_VALID_COMPRESSION(compression) (((compression) >= NO_COMPRESSION) && ((compression) <= TWO_STAGE_COMP))
#define TSDB_MIN_ID 0
#define TSDB_MAX_ID INT_MAX
#define TSDB_MIN_TABLES 4
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
static int32_t tsdbInsertDataToTable(TsdbRepoT *repo, SSubmitBlk *pBlock);
static int32_t tsdbRestoreCfg(STsdbRepo *pRepo, STsdbCfg *pCfg);
static int32_t tsdbGetDataDirName(STsdbRepo *pRepo, char *fname);
static void *  tsdbCommitData(void *arg);
static int     tsdbCommitToFile(STsdbRepo *pRepo, int fid, SSkipListIterator **iters, SRWHelper *pHelper,
                                SDataCols *pDataCols);
static TSKEY   tsdbNextIterKey(SSkipListIterator *pIter);
static int     tsdbHasDataToCommit(SSkipListIterator **iters, int nIters, TSKEY minKey, TSKEY maxKey);
// static int tsdbWriteBlockToFileImpl(SFile *pFile, SDataCols *pCols, int pointsToWrite, int64_t *offset, int32_t *len,
//                                     int64_t uid);

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
  pCfg->compression = TWO_STAGE_COMP;
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
int32_t tsdbDropRepo(TsdbRepoT *repo) {
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
TsdbRepoT *tsdbOpenRepo(char *tsdbDir, STsdbAppH *pAppH) {
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

  pRepo->tsdbCache = tsdbInitCache(pRepo->config.maxCacheSize, -1, (TsdbRepoT *)pRepo);
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

  return (TsdbRepoT *)pRepo;
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
int32_t tsdbCloseRepo(TsdbRepoT *repo) {
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
int32_t tsdbConfigRepo(TsdbRepoT *repo, STsdbCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  pRepo->config = *pCfg;
  // TODO
  return 0;
}

int32_t tsdbTriggerCommit(TsdbRepoT *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  if (pRepo->appH.walCallBack) pRepo->appH.walCallBack(pRepo->appH.appH);
  
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

int32_t tsdbLockRepo(TsdbRepoT *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  return pthread_mutex_lock(&(pRepo->mutex));
}

int32_t tsdbUnLockRepo(TsdbRepoT *repo) {
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
STsdbRepoInfo *tsdbGetStatus(TsdbRepoT *pRepo) {
  // TODO
  return NULL;
}

int tsdbCreateTable(TsdbRepoT *repo, STableCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  return tsdbCreateTableImpl(pRepo->tsdbMeta, pCfg);
}

int tsdbAlterTable(TsdbRepoT *pRepo, STableCfg *pCfg) {
  // TODO
  return 0;
}

int tsdbDropTable(TsdbRepoT *repo, STableId tableId) {
  if (repo == NULL) return -1;
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  return tsdbDropTableImpl(pRepo->tsdbMeta, tableId);
}

STableInfo *tsdbGetTableInfo(TsdbRepoT *pRepo, STableId tableId) {
  // TODO
  return NULL;
}

// TODO: need to return the number of data inserted
int32_t tsdbInsertData(TsdbRepoT *repo, SSubmitMsg *pMsg) {
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
  config->name = NULL;
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

int tsdbTableSetName(STableCfg *config, char *name, bool dup) {
  if (dup) {
    config->name = strdup(name);
    if (config->name == NULL) return -1;
  } else {
    config->name = name;
  }

  return 0;
}

int tsdbTableSetSName(STableCfg *config, char *sname, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) return -1;

  if (dup) {
    config->sname = strdup(sname);
    if (config->sname == NULL) return -1;
  } else {
    config->sname = sname;
  }
  return 0;
}

void tsdbClearTableCfg(STableCfg *config) {
  if (config->schema) tdFreeSchema(config->schema);
  if (config->tagSchema) tdFreeSchema(config->tagSchema);
  if (config->tagValues) tdFreeDataRow(config->tagValues);
  tfree(config->name);
  tfree(config->sname);
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

STsdbMeta* tsdbGetMeta(TsdbRepoT* pRepo) {
  STsdbRepo *tsdb = (STsdbRepo *)pRepo;
  return tsdb->tsdbMeta;
}

STsdbFileH* tsdbGetFile(TsdbRepoT* pRepo) {
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

  // Check compression
  if (pCfg->compression == -1) {
    pCfg->compression = TSDB_DEFAULT_COMPRESSION;
  } else {
    if (!IS_VALID_COMPRESSION(pCfg->compression)) return -1;
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

static int32_t tsdbInsertDataToTable(TsdbRepoT *repo, SSubmitBlk *pBlock) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  STableId tableId = {.uid = pBlock->uid, .tid = pBlock->tid};
  STable *pTable = tsdbIsValidTableToInsert(pRepo->tsdbMeta, tableId);
  if (pTable == NULL) {
    uError("failed to get table for insert, uid:%" PRIu64 ", tid:%d", tableId.uid, tableId.tid);
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
  ASSERT(maxRowsToRead > 0);
  if (pIter == NULL) return 0;

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
    if (iters[tid] == NULL) goto _err;

    if (!tSkipListIterNext(iters[tid])) goto _err;
  }

  return iters;

  _err:
  tsdbDestroyTableIters(iters, maxTables);
  return NULL;
}

static void tsdbFreeMemTable(SMemTable *pMemTable) {
  if (pMemTable) {
    tSkipListDestroy(pMemTable->pData);
    free(pMemTable);
  }
}

// Commit to file
static void *tsdbCommitData(void *arg) {
  printf("Starting to commit....\n");
  STsdbRepo * pRepo = (STsdbRepo *)arg;
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbCache *pCache = pRepo->tsdbCache;
  STsdbCfg *  pCfg = &(pRepo->config);
  SDataCols * pDataCols = NULL;
  SRWHelper   whelper = {0};
  if (pCache->imem == NULL) return NULL;

  // Create the iterator to read from cache
  SSkipListIterator **iters = tsdbCreateTableIters(pMeta, pCfg->maxTables);
  if (iters == NULL) {
    // TODO: deal with the error
    return NULL;
  }

  if (tsdbInitWriteHelper(&whelper, pRepo) < 0) goto _exit;
  if ((pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock)) == NULL) goto _exit;

  int sfid = tsdbGetKeyFileId(pCache->imem->keyFirst, pCfg->daysPerFile, pCfg->precision);
  int efid = tsdbGetKeyFileId(pCache->imem->keyLast, pCfg->daysPerFile, pCfg->precision);

  // Loop to commit to each file
  for (int fid = sfid; fid <= efid; fid++) {
    if (tsdbCommitToFile(pRepo, fid, iters, &whelper, pDataCols) < 0) {
      ASSERT(false);
      goto _exit;
    }
  }

_exit:
  tdFreeDataCols(pDataCols);
  tsdbDestroyTableIters(iters, pCfg->maxTables);
  tsdbDestroyHelper(&whelper);

  tsdbLockRepo(arg);
  tdListMove(pCache->imem->list, pCache->pool.memPool);
  tdListFree(pCache->imem->list);
  free(pCache->imem);
  pCache->imem = NULL;
  pRepo->commit = 0;
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

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SSkipListIterator **iters, SRWHelper *pHelper, SDataCols *pDataCols) {

  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  STsdbCfg *  pCfg = &pRepo->config;
  SFileGroup *pGroup = NULL;

  TSKEY minKey = 0, maxKey = 0;
  tsdbGetKeyRangeOfFileId(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

  // Check if there are data to commit to this file
  int hasDataToCommit = tsdbHasDataToCommit(iters, pCfg->maxTables, minKey, maxKey);
  if (!hasDataToCommit) return 0;  // No data to commit, just return

  // Create and open files for commit
  tsdbGetDataDirName(pRepo, dataDir);
  if ((pGroup = tsdbCreateFGroup(pFileH, dataDir, fid, pCfg->maxTables)) == NULL) goto _err;

  // Open files for write/read
  if (tsdbSetAndOpenHelperFile(pHelper, pGroup) < 0) goto _err;

  // Loop to commit data in each table
  for (int tid = 0; tid < pCfg->maxTables; tid++) {
    STable *           pTable = pMeta->tables[tid];
    if (pTable == NULL) continue;

    SSkipListIterator *pIter = iters[tid];

    // Set the helper and the buffer dataCols object to help to write this table
    tsdbSetHelperTable(pHelper, pTable, pRepo);
    tdInitDataCols(pDataCols, tsdbGetTableSchema(pMeta, pTable));

    // Loop to write the data in the cache to files. If no data to write, just break the loop 
    int maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5;
    int nLoop = 0;
    while (true) {
      int rowsRead = tsdbReadRowsFromCache(pIter, maxKey, maxRowsToRead, pDataCols);
      assert(rowsRead >= 0);
      if (pDataCols->numOfPoints == 0) break;
      nLoop++;

      ASSERT(dataColsKeyFirst(pDataCols) >= minKey && dataColsKeyFirst(pDataCols) <= maxKey);
      ASSERT(dataColsKeyLast(pDataCols) >= minKey && dataColsKeyLast(pDataCols) <= maxKey);

      int rowsWritten = tsdbWriteDataBlock(pHelper, pDataCols);
      ASSERT(rowsWritten != 0);
      if (rowsWritten < 0) goto _err;
      ASSERT(rowsWritten <= pDataCols->numOfPoints);

      tdPopDataColsPoints(pDataCols, rowsWritten);
      maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5 - pDataCols->numOfPoints;
    }

    ASSERT(pDataCols->numOfPoints == 0);

    // Move the last block to the new .l file if neccessary
    if (tsdbMoveLastBlockIfNeccessary(pHelper) < 0) goto _err;

    // Write the SCompBlock part
    if (tsdbWriteCompInfo(pHelper) < 0) goto _err;
 
  }

  if (tsdbWriteCompIdx(pHelper) < 0) goto _err;

  tsdbCloseHelperFile(pHelper, 0);
  // TODO: make it atomic with some methods
  pGroup->files[TSDB_FILE_TYPE_HEAD] = pHelper->files.headF;
  pGroup->files[TSDB_FILE_TYPE_DATA] = pHelper->files.dataF;
  pGroup->files[TSDB_FILE_TYPE_LAST] = pHelper->files.lastF;

  return 0;

  _err:
  ASSERT(false);
  tsdbCloseHelperFile(pHelper, 1);
  return -1;
}

/**
 * Return the next iterator key.
 *
 * @return the next key if iter has
 *         -1 if iter not
 */
static TSKEY tsdbNextIterKey(SSkipListIterator *pIter) {
  if (pIter == NULL) return -1;

  SSkipListNode *node = tSkipListIterGet(pIter);
  if (node == NULL) return -1;

  SDataRow row = SL_GET_NODE_DATA(node);
  return dataRowKey(row);
}

static int tsdbHasDataToCommit(SSkipListIterator **iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  TSKEY nextKey;
  for (int i = 0; i < nIters; i++) {
    SSkipListIterator *pIter = iters[i];
    nextKey = tsdbNextIterKey(pIter);
    if (nextKey > 0 && (nextKey >= minKey && nextKey <= maxKey)) return 1;
  }
  return 0;
}