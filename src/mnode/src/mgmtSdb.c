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
#include "taosdef.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tglobalcfg.h"
#include "tlog.h"
#include "trpc.h"
#include "tutil.h"
#include "hashint.h"
#include "hashstr.h"
#include "mgmtSdb.h"

#define abs(x) (((x) < 0) ? -(x) : (x))
#define SDB_MAX_PEERS 4
#define SDB_DELIMITER 0xFFF00F00
#define SDB_ENDCOMMIT 0xAFFFAAAF

typedef struct {
  uint64_t swVersion;
  int16_t  sdbFileVersion;
  char     reserved[2];
  TSCKSUM  checkSum;
} SSdbHeader;

typedef struct _SSdbTable {
  SSdbHeader  header;
  char        tableName[TSDB_DB_NAME_LEN];
  char        fileName[TSDB_FILENAME_LEN];
  ESdbKeyType keyType;
  int32_t     tableId;
  int32_t     hashSessions;
  int32_t     maxRowSize;
  int32_t     autoIndex;
  int32_t     fd;
  int64_t     numOfRows;
  int64_t     version;
  int64_t     fileSize;
  void *      iHandle;
  int32_t (*insertFp)(SSdbOperDesc *pDesc);
  int32_t (*deleteFp)(SSdbOperDesc *pOper);
  int32_t (*updateFp)(SSdbOperDesc *pOper);
  int32_t (*decodeFp)(SSdbOperDesc *pOper);
  int32_t (*encodeFp)(SSdbOperDesc *pOper);
  int32_t (*destroyFp)(SSdbOperDesc *pOper);
  pthread_mutex_t mutex;
} SSdbTable;

typedef struct {
  int64_t version;
  int64_t offset;
  int32_t rowSize;
  void *  row;
} SRowMeta;

typedef struct {
  int32_t delimiter;
  int32_t rowSize;
  int64_t version;
  char    data[];
} SRowHead;

typedef enum {
  SDB_FORWARD_TYPE_INSERT,
  SDB_FORWARD_TYPE_DELETE,
  SDB_FORWARD_TYPE_UPDATE
} ESdbForwardType;

typedef struct {
  ESdbForwardType type;
  int32_t tableId;
  int64_t version;
  int32_t rowSize;
  void *  rowData;
} SForwardMsg;

extern char   version[];
const int16_t sdbFileVersion = 2;
int32_t (*mpeerForwardRequestFp)(SForwardMsg *forwardMsg) = NULL;

static SSdbTable *sdbTableList[10] = {0};
static int32_t    sdbNumOfTables = 0;
static uint64_t   sdbVersion = 0;

static void *(*sdbInitIndexFp[])(int32_t maxRows, int32_t dataSize) = {sdbOpenStrHash, sdbOpenIntHash};
static void *(*sdbAddIndexFp[])(void *handle, void *key, void *data) = {sdbAddStrHash, sdbAddIntHash};
static void (*sdbDeleteIndexFp[])(void *handle, void *key) = {sdbDeleteStrHash, sdbDeleteIntHash};
static void *(*sdbGetIndexFp[])(void *handle, void *key) = {sdbGetStrHashData, sdbGetIntHashData};
static void (*sdbCleanUpIndexFp[])(void *handle) = {sdbCloseStrHash, sdbCloseIntHash};
static void *(*sdbFetchRowFp[])(void *handle, void *ptr, void **ppRow) = {sdbFetchStrHashData, sdbFetchIntHashData};

uint64_t sdbGetVersion() { return sdbVersion; }
int64_t sdbGetId(void *handle) { return ((SSdbTable *)handle)->version; }
int64_t sdbGetNumOfRows(void *handle) { return ((SSdbTable *)handle)->numOfRows; }

static char *sdbGetkeyStr(SSdbTable *pTable, void *row) {
  static char str[16];
  switch (pTable->keyType) {
    case SDB_KEY_TYPE_STRING:
      return (char *)row;
    case SDB_KEY_TYPE_AUTO:
      sprintf(str, "%d", *(int32_t *)row);
      return str;
    default:
      return "unknown";
  }
}

static int32_t sdbForwardDbReqToPeer(SForwardMsg *forwardMsg) {
  if (mpeerForwardRequestFp) {
    return mpeerForwardRequestFp(forwardMsg);
  } else {
    return 0;
  }
}

static void sdbFinishCommit(SSdbTable *pTable) {
  uint32_t sdbEcommit = SDB_ENDCOMMIT;
  off_t offset = lseek(pTable->fd, 0, SEEK_END);
  assert(offset == pTable->fileSize);
  twrite(pTable->fd, &sdbEcommit, sizeof(sdbEcommit));
  pTable->fileSize += sizeof(sdbEcommit);
}

static int32_t sdbOpenSdbFile(SSdbTable *pTable) {
  struct stat fstat, ofstat;
  uint64_t    size;
  char *      dirc = NULL;
  char *      basec = NULL;
  union {
    char     cversion[64];
    uint64_t iversion;
  } swVersion;

  memcpy(swVersion.cversion, version, sizeof(uint64_t));

  // check sdb.db and .sdb.db status
  char fn[TSDB_FILENAME_LEN] = "\0";
  dirc = strdup(pTable->fileName);
  basec = strdup(pTable->fileName);
  sprintf(fn, "%s/.%s", dirname(dirc), basename(basec));
  tfree(dirc);
  tfree(basec);
  if (stat(fn, &ofstat) == 0) {  // .sdb.db file exists
    if (stat(pTable->fileName, &fstat) == 0) {
      remove(fn);
    } else {
      remove(pTable->fileName);
      rename(fn, pTable->fileName);
    }
  }

  pTable->fd = open(pTable->fileName, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (pTable->fd < 0) {
    sdbError("table:%s, failed to open file:%s", pTable->tableName, pTable->fileName);
    return -1;
  }

  pTable->fileSize = 0;
  stat(pTable->fileName, &fstat);
  size = sizeof(pTable->header);

  if (fstat.st_size == 0) {
    pTable->header.swVersion = swVersion.iversion;
    pTable->header.sdbFileVersion = sdbFileVersion;
    if (taosCalcChecksumAppend(0, (uint8_t *)(&pTable->header), size) < 0) {
      sdbError("table:%s, failed to get file header checksum, file:%s", pTable->tableName, pTable->fileName);
      tclose(pTable->fd);
      return -1;
    }
    twrite(pTable->fd, &(pTable->header), size);
    pTable->fileSize += size;
    sdbFinishCommit(pTable);
  } else {
    uint32_t sdbEcommit = 0;
    off_t    offset = lseek(pTable->fd, -(sizeof(sdbEcommit)), SEEK_END);
    while (offset > 0) {
      read(pTable->fd, &sdbEcommit, sizeof(sdbEcommit));
      if (sdbEcommit == SDB_ENDCOMMIT) {
        ftruncate(pTable->fd, offset + sizeof(sdbEcommit));
        break;
      }
      offset = lseek(pTable->fd, -(sizeof(sdbEcommit) + 1), SEEK_CUR);
    }
    lseek(pTable->fd, 0, SEEK_SET);

    ssize_t tsize = read(pTable->fd, &(pTable->header), size);
    if (tsize < size) {
      sdbError("table:%s, failed to read sdb file header, file:%s", pTable->tableName, pTable->fileName);
      tclose(pTable->fd);
      return -1;
    }

    if (pTable->header.swVersion != swVersion.iversion) {
      sdbWarn("table:%s, sdb file:%s version not match software version", pTable->tableName, pTable->fileName);
    }

    if (!taosCheckChecksumWhole((uint8_t *)(&pTable->header), size)) {
      sdbError("table:%s, sdb file header is broken since checksum mismatch, file:%s", pTable->tableName, pTable->fileName);
      tclose(pTable->fd);
      return -1;
    }

    pTable->fileSize += size;
    // skip end commit symbol
    lseek(pTable->fd, sizeof(sdbEcommit), SEEK_CUR);
    pTable->fileSize += sizeof(sdbEcommit);
  }

  pTable->numOfRows = 0;

  return pTable->fd;
}

static int32_t sdbInitTableByFile(SSdbTable *pTable) {
  sdbTrace("table:%s, open sdb file:%s for read", pTable->tableName, pTable->fileName);
  if (sdbOpenSdbFile(pTable) < 0) {
    sdbError("table:%s, failed to open sdb file:%s for read", pTable->tableName, pTable->fileName);
    return -1;
  }
  
  int32_t total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)malloc(total_size);
  if (rowHead == NULL) {
    sdbError("table:%s, failed to allocate row head memory, sdb:%s", pTable->tableName, pTable->tableName);
    return -1;
  }

  int32_t numOfChanged = 0;
  int32_t maxAutoIndex = 0;
  while (1) {
    memset(rowHead, 0, total_size);

    int32_t bytes = read(pTable->fd, rowHead, sizeof(SRowHead));
    if (bytes < 0) {
      sdbError("table:%s, failed to read sdb file:%s", pTable->tableName, pTable->fileName);
      tfree(rowHead);
      return -1;
    }

    if (bytes == 0) break;

    if (bytes < sizeof(SRowHead) || rowHead->delimiter != SDB_DELIMITER) {
      pTable->fileSize++;
      lseek(pTable->fd, -(bytes - 1), SEEK_CUR);
      continue;
    }

    if (rowHead->rowSize < 0 || rowHead->rowSize > pTable->maxRowSize) {
      sdbError("table:%s, error row size in sdb filesize:%d, version:%d rowSize:%d maxRowSize:%d", pTable->tableName,
               pTable->fileSize, rowHead->version, rowHead->rowSize, pTable->maxRowSize);
      pTable->fileSize += sizeof(SRowHead);
      continue;
    }

    bytes = read(pTable->fd, rowHead->data, rowHead->rowSize + sizeof(TSCKSUM));
    if (bytes < rowHead->rowSize + sizeof(TSCKSUM)) {
      // TODO: Here may cause pTable->fileSize not end of the file
      sdbError("table:%s, failed to read sdb file, version:%d rowSize:%d", pTable->tableName, rowHead->version,
               rowHead->rowSize);
      break;
    }

    int32_t real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
    if (!taosCheckChecksumWhole((uint8_t *)rowHead, real_size)) {
      sdbError("table:%s, error sdb checksum, version:%d, skip", pTable->tableName, rowHead->version);
      pTable->fileSize += real_size;
      continue;
    }
    
    if (pTable->keyType == SDB_KEY_TYPE_AUTO) {
      maxAutoIndex = MAX(maxAutoIndex, *(int32_t *) rowHead->data);
    }

    pTable->version = MAX(pTable->version, abs(rowHead->version));

    void *pMetaRow = sdbGetRow(pTable, rowHead->data);
    if (pMetaRow == NULL) {
      if (rowHead->version < 0) {
        sdbError("table:%s, error sdb negative version:%d, record:%s, skip", pTable->tableName, rowHead->version,
                 sdbGetkeyStr(pTable, rowHead->data));
      } else {
        SRowMeta rowMeta;
        rowMeta.version = rowHead->version;
        rowMeta.offset = pTable->fileSize;
        rowMeta.rowSize = rowHead->rowSize;
        SSdbOperDesc oper = {
          .table = pTable,
          .rowData = rowHead->data,
          .rowSize = rowHead->rowSize
        };
        int32_t code = (*pTable->decodeFp)(&oper);
        if (code == TSDB_CODE_SUCCESS) {
          rowMeta.row = oper.pObj;
          (*sdbAddIndexFp[pTable->keyType])(pTable->iHandle, rowMeta.row, &rowMeta);
          pTable->numOfRows++;
          sdbTrace("table:%s, read new record:%s, numOfRows:%d version:%" PRId64 ,
                   pTable->tableName, sdbGetkeyStr(pTable, rowHead->data), pTable->numOfRows, pTable->version);
        } else {
          sdbError("table:%s, failed to decode record:%s, numOfRows:%d version:%" PRId64 ,
                   pTable->tableName, sdbGetkeyStr(pTable, rowHead->data), pTable->numOfRows, pTable->version);
        }
      }
    } else {
      if (rowHead->version < 0) {
        SSdbOperDesc oper = {
          .table = pTable,
          .pObj = pMetaRow
        };
        (*pTable->destroyFp)(&oper);
        (*sdbDeleteIndexFp[pTable->keyType])(pTable->iHandle, rowHead->data);
        pTable->numOfRows--;
        sdbTrace("table:%s, read deleted record:%s, numOfRows:%d version:%" PRId64 ,
                 pTable->tableName, sdbGetkeyStr(pTable, rowHead->data), pTable->numOfRows, pTable->version);
      } else { 
        SRowMeta rowMeta;
        rowMeta.version = rowHead->version;
        rowMeta.offset = pTable->fileSize;
        rowMeta.rowSize = rowHead->rowSize;
        SSdbOperDesc oper = {
          .table = pTable,
          .rowData = rowHead->data,
          .rowSize = rowHead->rowSize,
           .pObj = pMetaRow
        };
        
        (*pTable->destroyFp)(&oper);
        (*sdbDeleteIndexFp[pTable->keyType])(pTable->iHandle, rowHead->data);
        
        int32_t code = (*pTable->decodeFp)(&oper);
        if (code == TSDB_CODE_SUCCESS) {
          rowMeta.row = oper.pObj;
          (*sdbAddIndexFp[pTable->keyType])(pTable->iHandle, rowMeta.row, &rowMeta);
          sdbTrace("table:%s, read updated record:%s, numOfRows:%d version:%" PRId64 ,
                   pTable->tableName, sdbGetkeyStr(pTable, rowHead->data), pTable->numOfRows, pTable->version);
        } else {
          sdbError("table:%s, failed to decode record:%s, numOfRows:%d version:%" PRId64 ,
                   pTable->tableName, sdbGetkeyStr(pTable, rowHead->data), pTable->numOfRows, pTable->version);
        }
      }
      numOfChanged++;
    }

    pTable->fileSize += real_size;
    pTable->fileSize += 4;
    lseek(pTable->fd, 4, SEEK_CUR);
  }

  void *pNode = NULL;
  while (1) {
    SRowMeta * pMeta;
    pNode = (*sdbFetchRowFp[pTable->keyType])(pTable->iHandle, pNode, (void **)&pMeta);
    if (pMeta == NULL) break;

    SSdbOperDesc oper = {
      .pObj = pMeta->row,
      .table = pTable,
      .version = pMeta->version,
    };

    int32_t code = (*pTable->insertFp)(&oper);
    if (code != TSDB_CODE_SUCCESS) {
      sdbError("table:%s, failed to insert record:%s", pTable->tableName, sdbGetkeyStr(pTable, rowHead->data));
    }    
  }

  sdbVersion += pTable->version;
  
  if (pTable->keyType == SDB_KEY_TYPE_AUTO) {
    pTable->autoIndex = maxAutoIndex;
  }

  tfree(rowHead);
  return 0;
}

void *sdbOpenTable(SSdbTableDesc *pDesc) {
  SSdbTable *pTable = (SSdbTable *)calloc(1, sizeof(SSdbTable));
  if (pTable == NULL) return NULL;

  pTable->keyType = pDesc->keyType;
  pTable->hashSessions = pDesc->hashSessions;
  pTable->maxRowSize = pDesc->maxRowSize;
  pTable->insertFp = pDesc->insertFp;
  pTable->deleteFp = pDesc->deleteFp;
  pTable->updateFp = pDesc->updateFp;
  pTable->encodeFp = pDesc->encodeFp;
  pTable->decodeFp = pDesc->decodeFp;
  pTable->destroyFp = pDesc->destroyFp;
  strcpy(pTable->tableName, pDesc->tableName);
  sprintf(pTable->fileName, "%s/%s.db", tsMnodeDir, pTable->tableName);

  if (sdbInitIndexFp[pTable->keyType] != NULL) {
    pTable->iHandle = (*sdbInitIndexFp[pTable->keyType])(pTable->maxRowSize, sizeof(SRowMeta));
  }

  pthread_mutex_init(&pTable->mutex, NULL);

  if (sdbInitTableByFile(pTable) < 0) return NULL;

  pTable->tableId = sdbNumOfTables++;
  sdbTableList[pTable->tableId] = pTable;

  sdbTrace("table:%s, is initialized, numOfRows:%d numOfTables:%d version:%" PRId64 " sdbversion:%" PRId64,
           pTable->tableName, pTable->numOfRows, sdbNumOfTables, pTable->version, sdbVersion);

  return pTable;
}

static SRowMeta *sdbGetRowMeta(void *handle, void *key) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta;

  if (handle == NULL) return NULL;

  pMeta = (*sdbGetIndexFp[pTable->keyType])(pTable->iHandle, key);

  return pMeta;
}

void *sdbGetRow(void *handle, void *key) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta;

  if (handle == NULL) return NULL;

  pthread_mutex_lock(&pTable->mutex);
  pMeta = (*sdbGetIndexFp[pTable->keyType])(pTable->iHandle, key);
  pthread_mutex_unlock(&pTable->mutex);

  if (pMeta == NULL) {
    return NULL;
  }

  return pMeta->row;
}

int32_t sdbInsertRow(SSdbOperDesc *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) {
    sdbError("sdb tables is null");
    return TSDB_CODE_OTHERS;
  }

  if (sdbGetRow(pTable, pOper->pObj)) {
    sdbError("table:%s, failed to insert record:%s, already exist", pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj));
    return TSDB_CODE_ALREADY_THERE;
  }

  pOper->maxRowSize = pTable->maxRowSize;
  pthread_mutex_lock(&pTable->mutex);

  if (pOper->type == SDB_OPER_TYPE_GLOBAL) {
    SForwardMsg forward = {
      .type = SDB_FORWARD_TYPE_INSERT,
      .tableId = pTable->tableId,
      .version = pTable->version + 1,
      .rowSize = pOper->rowSize,
      .rowData = pOper->rowData,
    };

    if (sdbForwardDbReqToPeer(&forward) != 0) {
      sdbError("table:%s, failed to forward record:%s version:%" PRId64 " sdbversion:%" PRId64, 
               pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj), pOper->version, sdbVersion);
      pthread_mutex_unlock(&pTable->mutex);
      return TSDB_CODE_OTHERS;
    } 
  }

  int32_t total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)calloc(1, total_size);
  if (rowHead == NULL) {
    pthread_mutex_unlock(&pTable->mutex);
    sdbError("table:%s, failed to allocate row head memory for record:%s version:%" PRId64 " sdbversion:%" PRId64, 
              pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj), pOper->version, sdbVersion);
    return -1;
  }
  
  if (pTable->keyType == SDB_KEY_TYPE_AUTO) {
    *((uint32_t *)pOper->pObj) = ++pTable->autoIndex;
  }
  pTable->version++;
  sdbVersion++;

  pOper->rowData = rowHead->data;
  (*pTable->encodeFp)(pOper);
  rowHead->rowSize = pOper->rowSize;
  rowHead->delimiter = SDB_DELIMITER;
  rowHead->version = pTable->version;
  assert(rowHead->rowSize > 0 && rowHead->rowSize <= pTable->maxRowSize);

  int32_t real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
  if (taosCalcChecksumAppend(0, (uint8_t *)rowHead, real_size) < 0) {
    sdbError("table:%s, failed to get checksum while inserting", pTable->tableName);
    pTable->version--;
    sdbVersion--;
    pthread_mutex_unlock(&pTable->mutex);
    tfree(rowHead);
    return -1;
  }

  twrite(pTable->fd, rowHead, real_size);
  pTable->fileSize += real_size;
  sdbFinishCommit(pTable);
  tfree(rowHead);
 
  // update in SDB layer
  SRowMeta rowMeta;
  rowMeta.version = pTable->version;
  rowMeta.offset = pTable->fileSize;
  rowMeta.rowSize = pOper->rowSize;
  rowMeta.row = pOper->pObj;
  (*sdbAddIndexFp[pTable->keyType])(pTable->iHandle, pOper->pObj, &rowMeta);

  pTable->numOfRows++;  
  
  pthread_mutex_unlock(&pTable->mutex);

  sdbTrace("table:%s, inserte record:%s, sdbversion:%" PRId64 " version:%" PRId64 " rowSize:%d numOfRows:%d fileSize:%" PRId64,
          pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj), sdbVersion, pTable->version, pOper->rowSize, pTable->numOfRows, pTable->fileSize);

  (*pTable->insertFp)(pOper);

  return 0;
}

// row here can be object or null-terminated string
int32_t sdbDeleteRow(SSdbOperDesc *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  SRowMeta *pMeta = sdbGetRowMeta(pTable, pOper->pObj);
  if (pMeta == NULL) {
    sdbTrace("table:%s, record is not there, delete failed", pTable->tableName);
    return -1;
  }

  void * pMetaRow = pMeta->row;
  assert(pMetaRow != NULL);

  pthread_mutex_lock(&pTable->mutex);

  if (pOper->type == SDB_OPER_TYPE_GLOBAL) {
    SForwardMsg forward = {
      .type = SDB_FORWARD_TYPE_DELETE,
      .tableId = pTable->tableId,
      .version = pTable->version + 1,
      .rowSize = pMeta->rowSize,
      .rowData = pMeta->row,
    };

   if (sdbForwardDbReqToPeer(&forward) != 0) {
      sdbError("table:%s, failed to delete record", pTable->tableName);
      pthread_mutex_unlock(&pTable->mutex);
      return -1;
    } 
  }

  int32_t total_size = sizeof(SRowHead) + pMeta->rowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)calloc(1, total_size);
  if (rowHead == NULL) {
    sdbError("failed to allocate row head memory, sdb:%s", pTable->tableName);
    pthread_mutex_unlock(&pTable->mutex);
    return -1;
  }

  pTable->version++;
  sdbVersion++;

  int32_t rowSize = 0;
  switch (pTable->keyType) {
    case SDB_KEY_TYPE_STRING:
      rowSize = strlen((char *)pOper->pObj) + 1;
      break;
    case SDB_KEY_TYPE_AUTO:
      rowSize = sizeof(uint64_t);
      break;
    default:
      return -1;
  }

  rowHead->delimiter = SDB_DELIMITER;
  rowHead->rowSize = rowSize;
  rowHead->version = -(pTable->version);
  memcpy(rowHead->data, pOper->pObj, rowSize);
  int32_t real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
  if (taosCalcChecksumAppend(0, (uint8_t *)rowHead, real_size) < 0) {
    sdbError("failed to get checksum while inserting, sdb:%s", pTable->tableName);
    pTable->version--;
    sdbVersion--;
    pthread_mutex_unlock(&pTable->mutex);
    tfree(rowHead);
    return -1;
  }

  twrite(pTable->fd, rowHead, real_size);
  pTable->fileSize += real_size;
  sdbFinishCommit(pTable);

  tfree(rowHead);

  sdbTrace("table:%s, delete record:%s, sdbversion:%" PRId64 " version:%" PRId64 " numOfRows:%d",
          pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj), sdbVersion, pTable->version, pTable->numOfRows);
     
  // Delete from current layer
  (*sdbDeleteIndexFp[pTable->keyType])(pTable->iHandle, pOper->pObj);

  pTable->numOfRows--;

  pthread_mutex_unlock(&pTable->mutex);

  (*pTable->deleteFp)(pOper);
  (*pTable->destroyFp)(pOper);

  return 0;
}

// row here can be the object or the string info (encoded string)
int32_t sdbUpdateRow(SSdbOperDesc *pOper) {
  SSdbTable *pTable = (SSdbTable *)pOper->table;
  if (pTable == NULL) return -1;

  SRowMeta *pMeta = sdbGetRowMeta(pTable, pOper->pObj);
  if (pMeta == NULL) {
    sdbError("table:%s, failed to update record:%s, record is not there, sdbversion:%" PRId64 " version:%" PRId64,
            pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj), sdbVersion, pTable->version);
    return -1;
  }

  void *pMetaRow = pMeta->row;
  assert(pMetaRow != NULL);

  pthread_mutex_lock(&pTable->mutex);

  if (pOper->type == SDB_OPER_TYPE_GLOBAL) {
    SForwardMsg forward = {
      .type = SDB_FORWARD_TYPE_UPDATE,
      .tableId = pTable->tableId,
      .version = pTable->version + 1,
      .rowSize = pOper->rowSize,
      .rowData = pOper->rowData,
    };
    if (sdbForwardDbReqToPeer(&forward) != 0) {
      sdbError("table:%s, failed to update record", pTable->tableName);
      pthread_mutex_unlock(&pTable->mutex);
      return -1;
    } 
  }

  int32_t total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)calloc(1, total_size);
  if (rowHead == NULL) {
    sdbError("table:%s, failed to allocate row head memory", pTable->tableName);
    return -1;
  }
  
  if (pMetaRow != pOper->pObj) {
    memcpy(rowHead->data, pOper->rowData, pOper->rowSize);
    rowHead->rowSize = pOper->rowSize;
  } else {
    SSdbOperDesc oper = {
      .table = pTable,
      .rowData = rowHead->data,
      .maxRowSize = pTable->maxRowSize,
      .pObj = pOper->pObj
    };
    (*pTable->encodeFp)(&oper);
    rowHead->rowSize = oper.rowSize;
  }

  pTable->version++;
  sdbVersion++;

  int32_t real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
  rowHead->delimiter = SDB_DELIMITER;
  rowHead->version = pTable->version;
  if (taosCalcChecksumAppend(0, (uint8_t *)rowHead, real_size) < 0) {
    sdbError("table:%s, failed to get checksum, version:%d", pTable->tableName, rowHead->version);
    pTable->version--;
    sdbVersion--;
    pthread_mutex_unlock(&pTable->mutex);
    tfree(rowHead);
    return -1;
  }
  
  twrite(pTable->fd, rowHead, real_size);
  pTable->fileSize += real_size;
  sdbFinishCommit(pTable);
  
  sdbTrace("table:%s, update record:%s, sdbversion:%" PRId64 " version:%" PRId64 " numOfRows:%" PRId64,
          pTable->tableName, sdbGetkeyStr(pTable, pOper->pObj), sdbVersion, pTable->version, pTable->numOfRows);

  pMeta->version = pTable->version;
  pMeta->offset = pTable->fileSize;
  pMeta->rowSize = rowHead->rowSize;
  
  pthread_mutex_unlock(&pTable->mutex);

  (*pTable->updateFp)(pOper);  // update in upper layer

  tfree(rowHead);

  return 0;
}

void sdbCloseTable(void *handle) {
  SSdbTable *pTable = (SSdbTable *)handle;
  void *     pNode = NULL;
  void *     row;

  if (pTable == NULL) return;

  while (1) {
    pNode = sdbFetchRow(handle, pNode, &row);
    if (row == NULL) break;

    SSdbOperDesc oper = {
      .table = pTable,
      .rowData = row,
    };
    (*pTable->destroyFp)(&oper);
  }

  if (sdbCleanUpIndexFp[pTable->keyType]) (*sdbCleanUpIndexFp[pTable->keyType])(pTable->iHandle);

  if (pTable->fd) tclose(pTable->fd);

  pthread_mutex_destroy(&pTable->mutex);

  sdbNumOfTables--;
  sdbTrace("table:%s, is closed, version:%" PRId64 " numOfTables:%d", pTable->tableName, pTable->version, sdbNumOfTables);

  tfree(pTable);
}

void *sdbFetchRow(void *handle, void *pNode, void **ppRow) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta;

  *ppRow = NULL;
  if (pTable == NULL) return NULL;

  pNode = (*sdbFetchRowFp[pTable->keyType])(pTable->iHandle, pNode, (void **)&pMeta);
  if (pMeta == NULL) return NULL;

  *ppRow = pMeta->row;

  return pNode;
}
