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
#include "tutil.h"
#include "tchecksum.h"
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
  char     reserved[6];
  TSCKSUM  checkSum;
} SSdbHeader;

typedef struct _SSdbTable {
  SSdbHeader header;
  int        maxRows;
  int        dbId;
  int32_t    maxRowSize;
  char       name[TSDB_DB_NAME_LEN];
  char       fn[128];
  int        keyType;
  uint32_t   autoIndex;
  int64_t    numOfRows;
  int64_t    id;
  int64_t    size;
  void *     iHandle;
  int        fd;
  void *(*appTool)(char, void *, char *, int, int *);
  pthread_mutex_t mutex;
} SSdbTable;

typedef struct {
  int64_t id;
  int64_t offset;
  int     rowSize;
  void *  row;
} SRowMeta;

typedef struct {
  int32_t delimiter;
  int32_t rowSize;
  int64_t id;
  char    data[];
} SRowHead;

typedef struct {
  uint8_t  dbId;
  char     type;
  uint64_t version;
  short    dataLen;
  char     data[];
} SForwardMsg;

extern char   version[];
const int16_t sdbFileVersion = 2;
int32_t (*mpeerForwardRequestFp)(SSdbTable *pTable, char type, void *cont, int32_t contLen) = NULL;

static SSdbTable *sdbTableList[10] = {0};
static int32_t    sdbNumOfTables = 0;
static uint64_t   sdbVersion = 0;

static void *(*sdbInitIndexFp[])(int32_t maxRows, int32_t dataSize) = {sdbOpenStrHash, sdbOpenIntHash};
static void *(*sdbAddIndexFp[])(void *handle, void *key, void *data) = {sdbAddStrHash, sdbAddIntHash};
static void (*sdbDeleteIndexFp[])(void *handle, void *key) = {sdbDeleteStrHash, sdbDeleteIntHash};
static void *(*sdbGetIndexFp[])(void *handle, void *key) = {sdbGetStrHashData, sdbGetIntHashData};
static void (*sdbCleanUpIndexFp[])(void *handle) = {sdbCloseStrHash, sdbCloseIntHash};
static void *(*sdbFetchRowFp[])(void *handle, void *ptr, void **ppRow) = {sdbFetchStrHashData, sdbFetchIntHashData};

void sdbResetTable(SSdbTable *pTable);
void sdbSaveSnapShot(void *handle);

uint64_t sdbGetVersion() { return sdbVersion; }
int64_t sdbGetId(void *handle) { return ((SSdbTable *)handle)->id; }
int64_t sdbGetNumOfRows(void *handle) { return ((SSdbTable *)handle)->numOfRows; }

static int32_t sdbForwardDbReqToPeer(SSdbTable *pTable, char type, char *data, int32_t dataLen) {
  if (mpeerForwardRequestFp) {
    return mpeerForwardRequestFp(pTable, type, data, dataLen);
  } else {
    return 0;
  }
}

static void sdbFinishCommit(void *handle) {
  SSdbTable *pTable = (SSdbTable *)handle;
  uint32_t   sdbEcommit = SDB_ENDCOMMIT;

  off_t offset = lseek(pTable->fd, 0, SEEK_END);
  assert(offset == pTable->size);
  twrite(pTable->fd, &sdbEcommit, sizeof(sdbEcommit));
  pTable->size += sizeof(sdbEcommit);
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
  dirc = strdup(pTable->fn);
  basec = strdup(pTable->fn);
  sprintf(fn, "%s/.%s", dirname(dirc), basename(basec));
  tfree(dirc);
  tfree(basec);
  if (stat(fn, &ofstat) == 0) {  // .sdb.db file exists
    if (stat(pTable->fn, &fstat) == 0) {
      remove(fn);
    } else {
      remove(pTable->fn);
      rename(fn, pTable->fn);
    }
  }

  pTable->fd = open(pTable->fn, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (pTable->fd < 0) {
    sdbError("failed to open file:%s", pTable->fn);
    return -1;
  }

  pTable->size = 0;
  stat(pTable->fn, &fstat);
  size = sizeof(pTable->header);

  if (fstat.st_size == 0) {
    pTable->header.swVersion = swVersion.iversion;
    pTable->header.sdbFileVersion = sdbFileVersion;
    if (taosCalcChecksumAppend(0, (uint8_t *)(&pTable->header), size) < 0) {
      sdbError("failed to get file header checksum, file:%s", pTable->fn);
      tclose(pTable->fd);
      return -1;
    }
    twrite(pTable->fd, &(pTable->header), size);
    pTable->size += size;
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
      sdbError("failed to read sdb file header, file:%s", pTable->fn);
      tclose(pTable->fd);
      return -1;
    }

    if (pTable->header.swVersion != swVersion.iversion) {
      sdbWarn("sdb file:%s version not match software version", pTable->fn);
    }

    if (!taosCheckChecksumWhole((uint8_t *)(&pTable->header), size)) {
      sdbError("sdb file header is broken since checksum mismatch, file:%s", pTable->fn);
      tclose(pTable->fd);
      return -1;
    }

    pTable->size += size;
    // skip end commit symbol
    lseek(pTable->fd, sizeof(sdbEcommit), SEEK_CUR);
    pTable->size += sizeof(sdbEcommit);
  }

  pTable->numOfRows = 0;

  return pTable->fd;
}

static int32_t sdbInitTableByFile(SSdbTable *pTable) {
  SRowMeta rowMeta;
  int32_t  numOfDels = 0;
  int32_t  bytes = 0;
  int64_t  oldId = 0;
  void *   pMetaRow = NULL;
  int32_t  total_size = 0;
  int32_t  real_size = 0;
  int32_t  maxAutoIndex = 0;

  oldId = pTable->id;
  if (sdbOpenSdbFile(pTable) < 0) return -1;

  total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)malloc(total_size);
  if (rowHead == NULL) {
    sdbError("failed to allocate row head memory, sdb:%s", pTable->name);
    return -1;
  }

  sdbTrace("open sdb file:%s for read", pTable->fn);

  // Loop to read sdb file row by row
  while (1) {
    memset(rowHead, 0, total_size);

    bytes = read(pTable->fd, rowHead, sizeof(SRowHead));
    if (bytes < 0) {
      sdbError("failed to read sdb file:%s", pTable->fn);
      goto sdb_exit1;
    }

    if (bytes == 0) break;

    if (bytes < sizeof(SRowHead) || rowHead->delimiter != SDB_DELIMITER) {
      pTable->size++;
      lseek(pTable->fd, -(bytes - 1), SEEK_CUR);
      continue;
    }

    if (rowHead->rowSize < 0 || rowHead->rowSize > pTable->maxRowSize) {
      sdbError("error row size in sdb file:%s, id:%d rowSize:%d maxRowSize:%d",
              pTable->fn, rowHead->id, rowHead->rowSize, pTable->maxRowSize);
      pTable->size += sizeof(SRowHead);
      continue;
    }

    // sdbTrace("%s id:%ld rowSize:%d", pTable->name, rowHead->id,
    // rowHead->rowSize);

    bytes = read(pTable->fd, rowHead->data, rowHead->rowSize + sizeof(TSCKSUM));
    if (bytes < rowHead->rowSize + sizeof(TSCKSUM)) {
      // TODO: Here may cause pTable->size not end of the file
      sdbError("failed to read sdb file:%s id:%d rowSize:%d", pTable->fn, rowHead->id, rowHead->rowSize);
      break;
    }

    real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
    if (!taosCheckChecksumWhole((uint8_t *)rowHead, real_size)) {
      sdbError("error sdb checksum, sdb:%s  id:%d, skip", pTable->name, rowHead->id);
      pTable->size += real_size;
      continue;
    }

    // Check if the the object exists already

    pMetaRow = sdbGetRow(pTable, rowHead->data);
    if (pMetaRow == NULL) {  // New object
      if (rowHead->id < 0) {
        /* assert(0); */
        sdbError("error sdb negative id:%d, sdb:%s, skip", rowHead->id, pTable->name);
      } else {
        rowMeta.id = rowHead->id;
        // TODO: Get rid of the rowMeta.offset and rowSize
        rowMeta.offset = pTable->size;
        rowMeta.rowSize = rowHead->rowSize;
        rowMeta.row = (*(pTable->appTool))(SDB_TYPE_DECODE, NULL, rowHead->data, rowHead->rowSize, NULL);
        (*sdbAddIndexFp[pTable->keyType])(pTable->iHandle, rowMeta.row, &rowMeta);
        if (pTable->keyType == SDB_KEYTYPE_AUTO) {
          pTable->autoIndex++;
          maxAutoIndex = MAX(maxAutoIndex, *(int32_t*)rowHead->data);
        }
        pTable->numOfRows++;
      }
    } else {                  // already exists
      if (pTable->keyType == SDB_KEYTYPE_AUTO) {
        pTable->autoIndex++;
        maxAutoIndex = MAX(maxAutoIndex, *(int32_t *) rowHead->data);
      }

      if (rowHead->id < 0) {  // Delete the object
        (*sdbDeleteIndexFp[pTable->keyType])(pTable->iHandle, rowHead->data);
        (*(pTable->appTool))(SDB_TYPE_DESTROY, pMetaRow, NULL, 0, NULL);
        pTable->numOfRows--;
        numOfDels++;
      } else {  // Reset the object TODO: is it possible to merge reset and
                // update ??
        //(*(pTable->appTool))(SDB_TYPE_RESET, pMetaRow, rowHead->data, rowHead->rowSize, NULL);
      }
      numOfDels++;
    }

    pTable->size += real_size;
    if (pTable->id < abs(rowHead->id)) pTable->id = abs(rowHead->id);
    
    //TODO: check this valid
    pTable->size += 4;
    lseek(pTable->fd, 4, SEEK_CUR);
  }

  if (pTable->keyType == SDB_KEYTYPE_AUTO) {
    pTable->autoIndex = maxAutoIndex;
  }

  sdbVersion += (pTable->id - oldId);
  if (numOfDels > pTable->maxRows / 4) sdbSaveSnapShot(pTable);

  tfree(rowHead);
  return 0;

sdb_exit1:
  tfree(rowHead);
  return -1;
}

void *sdbOpenTable(int32_t maxRows, int32_t maxRowSize, char *name, uint8_t keyType, char *directory,
                   void *(*appTool)(char, void *, char *, int32_t, int32_t *)) {
  SSdbTable *pTable = (SSdbTable *)malloc(sizeof(SSdbTable));
  if (pTable == NULL) return NULL;
  memset(pTable, 0, sizeof(SSdbTable));

  strcpy(pTable->name, name);
  pTable->keyType = keyType;
  pTable->maxRows = maxRows;
  pTable->maxRowSize = maxRowSize;
  pTable->appTool = appTool;
  sprintf(pTable->fn, "%s/%s.db", directory, pTable->name);

  if (sdbInitIndexFp[keyType] != NULL) pTable->iHandle = (*sdbInitIndexFp[keyType])(maxRows, sizeof(SRowMeta));

  pthread_mutex_init(&pTable->mutex, NULL);

  if (sdbInitTableByFile(pTable) < 0) return NULL;

  pTable->dbId = sdbNumOfTables++;
  sdbTableList[pTable->dbId] = pTable;

  sdbTrace("table:%s is initialized, numOfRows:%d, numOfTables:%d", pTable->name, pTable->numOfRows, sdbNumOfTables);

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

  if (pMeta == NULL) return NULL;

  return pMeta->row;
}

// row here must be encoded string (rowSize > 0) or the object it self (rowSize = 0)
int32_t sdbInsertRow(void *handle, void *row, ESdbOper oper) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta   rowMeta;
  void *     pObj = NULL;
  int32_t    total_size = 0;
  int32_t    real_size = 0;

  if (pTable == NULL) {
    sdbError("sdb tables is null");
    return -1;
  }
  
  if (sdbGetRow(handle, row)) {
    switch (pTable->keyType) {
      case SDB_KEYTYPE_STRING:
        sdbError("table:%s, failed to insert record:%s sdbVersion:%" PRId64 " id:%" PRId64 , pTable->name, (char *)row, sdbVersion, pTable->id);
        break;
      case SDB_KEYTYPE_AUTO:
        sdbError("table:%s, failed to insert record:%d sdbVersion:%" PRId64 " id:%" PRId64, pTable->name, *(int32_t *)row, sdbVersion, pTable->id);
        break;
      default:
        sdbError("table:%s, failed to insert record sdbVersion:%" PRId64 " id:%" PRId64, pTable->name, sdbVersion, pTable->id);
        break;
    }
    return -1;
  }

  total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)malloc(total_size);
  if (rowHead == NULL) {
    sdbError("table:%s, failed to allocate row head memory", pTable->name);
    return -1;
  }
  memset(rowHead, 0, total_size);

  if (oper == SDB_OPER_GLOBAL) {
    pObj = row;
  } else {
    pObj = (*(pTable->appTool))(SDB_TYPE_DECODE, NULL, row, 0, NULL);
  } 
   
  pthread_mutex_lock(&pTable->mutex);

  if (oper == SDB_OPER_GLOBAL) {
    if (sdbForwardDbReqToPeer(pTable, SDB_TYPE_INSERT, rowHead->data, rowHead->rowSize) != 0) {
      sdbError("table:%s, failed to insert record", pTable->name);
      pthread_mutex_unlock(&pTable->mutex);
      tfree(rowHead);
      return -1;
    } 
  }

  if (oper == SDB_OPER_GLOBAL || oper == SDB_OPER_LOCAL) {
    (*(pTable->appTool))(SDB_TYPE_ENCODE, pObj, rowHead->data, pTable->maxRowSize, &(rowHead->rowSize));
    assert(rowHead->rowSize > 0 && rowHead->rowSize <= pTable->maxRowSize);

    real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
    rowHead->delimiter = SDB_DELIMITER;
    rowHead->id = pTable->id + 1;
    if (taosCalcChecksumAppend(0, (uint8_t *)rowHead, real_size) < 0) {
      sdbError("table:%s, failed to get checksum while inserting", pTable->name);
      pthread_mutex_unlock(&pTable->mutex);
      tfree(rowHead);
      return -1;
    }

    // update in SDB layer
    rowMeta.id = pTable->id;
    rowMeta.offset = pTable->size;
    rowMeta.rowSize = rowHead->rowSize;
    rowMeta.row = pObj;
    (*sdbAddIndexFp[pTable->keyType])(pTable->iHandle, pObj, &rowMeta);

    twrite(pTable->fd, rowHead, real_size);
    pTable->size += real_size;
    sdbFinishCommit(pTable);

    switch (pTable->keyType) {
      case SDB_KEYTYPE_STRING:
        sdbTrace("table:%s, a record is inserted:%s, sdbVersion:%" PRId64 " id:%" PRId64 " rowSize:%d numOfRows:%d fileSize:%" PRId64,
                pTable->name, (char *)row, sdbVersion, rowHead->id, rowHead->rowSize, pTable->numOfRows, pTable->size);
        break;
      case SDB_KEYTYPE_AUTO:
        sdbTrace("table:%s, a record is inserted:%d, sdbVersion:%" PRId64 " id:%" PRId64 " rowSize:%d numOfRows:%d fileSize:%" PRId64,
                pTable->name, *(int32_t *)row, sdbVersion, rowHead->id, rowHead->rowSize, pTable->numOfRows, pTable->size);
        break;
      default:
        sdbTrace("table:%s, a record is inserted, sdbVersion:%" PRId64 " id:%" PRId64 " rowSize:%d numOfRows:%d fileSize:%" PRId64,
                pTable->name, sdbVersion, rowHead->id, rowHead->rowSize, pTable->numOfRows, pTable->size);
        break;
    }
  } 

  if (pTable->keyType == SDB_KEYTYPE_AUTO) {
    *((uint32_t *)pObj) = ++pTable->autoIndex;
  }

  pTable->numOfRows++;  
  pTable->id++;
  sdbVersion++;

  pthread_mutex_unlock(&pTable->mutex);

  (*pTable->appTool)(SDB_TYPE_INSERT, pObj, NULL, 0, NULL);

  tfree(rowHead);

  return 0;
}

// row here can be object or null-terminated string
int32_t sdbDeleteRow(void *handle, void *row, ESdbOper oper) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta = NULL;
  void *     pMetaRow = NULL;
  SRowHead * rowHead = NULL;
  int32_t    rowSize = 0;
  int32_t    total_size = 0;

  if (pTable == NULL) return -1;

  pMeta = sdbGetRowMeta(handle, row);
  if (pMeta == NULL) {
    sdbTrace("table:%s, record is not there, delete failed", pTable->name);
    return -1;
  }

  pMetaRow = pMeta->row;
  assert(pMetaRow != NULL);

  switch (pTable->keyType) {
    case SDB_KEYTYPE_STRING:
      rowSize = strlen((char *)row) + 1;
      break;
    case SDB_KEYTYPE_AUTO:
      rowSize = sizeof(uint64_t);
      break;
    default:
      return -1;
  }

  total_size = sizeof(SRowHead) + rowSize + sizeof(TSCKSUM);
  rowHead = (SRowHead *)malloc(total_size);
  if (rowHead == NULL) {
    sdbError("failed to allocate row head memory, sdb:%s", pTable->name);
    return -1;
  }
  memset(rowHead, 0, total_size);

  pthread_mutex_lock(&pTable->mutex);

  if (oper == SDB_OPER_GLOBAL) {
   if (sdbForwardDbReqToPeer(pTable, SDB_TYPE_DELETE, (char *)row, rowSize) == 0) {
      sdbError("table:%s, failed to delete record", pTable->name);
      pthread_mutex_unlock(&pTable->mutex);
      tfree(rowHead);
      return -1;
    } 
  }

  rowHead->delimiter = SDB_DELIMITER;
  rowHead->rowSize = rowSize;
  rowHead->id = -(pTable->id);
  memcpy(rowHead->data, row, rowSize);
  if (taosCalcChecksumAppend(0, (uint8_t *)rowHead, total_size) < 0) {
    sdbError("failed to get checksum while inserting, sdb:%s", pTable->name);
    pthread_mutex_unlock(&pTable->mutex);
    tfree(rowHead);
    return -1;
  }

  twrite(pTable->fd, rowHead, total_size);
  pTable->size += total_size;
  sdbFinishCommit(pTable);

  switch (pTable->keyType) {
    case SDB_KEYTYPE_STRING:
      sdbTrace("table:%s, a record is deleted:%s, sdbVersion:%" PRId64 " id:%" PRId64 " numOfRows:%d",
              pTable->name, (char *)row, sdbVersion, pTable->id, pTable->numOfRows);
      break;
    case SDB_KEYTYPE_AUTO:
      sdbTrace("table:%s, a record is deleted:%d, sdbVersion:%" PRId64 " id:%" PRId64 " numOfRows:%d",
              pTable->name, *(int32_t *)row, sdbVersion, pTable->id, pTable->numOfRows);
      break;
    default:
      sdbTrace("table:%s, a record is deleted, sdbVersion:%" PRId64 " id:%" PRId64 " numOfRows:%d",
              pTable->name, sdbVersion, pTable->id, pTable->numOfRows);
      break;
  }

  // Delete from current layer
  (*sdbDeleteIndexFp[pTable->keyType])(pTable->iHandle, row);

  pTable->numOfRows--;
  pTable->id++;
  sdbVersion++;

  pthread_mutex_unlock(&pTable->mutex);

  tfree(rowHead);

  (*pTable->appTool)(SDB_TYPE_DELETE, pMetaRow, NULL, 0, NULL);

  return 0;
}

// row here can be the object or the string info (encoded string)
int32_t sdbUpdateRow(void *handle, void *row, int32_t updateSize, ESdbOper oper) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta = NULL;
  int32_t    total_size = 0;
  int32_t    real_size = 0;

  if (pTable == NULL || row == NULL) return -1;
  pMeta = sdbGetRowMeta(handle, row);
  if (pMeta == NULL) {
    switch (pTable->keyType) {
      case SDB_KEYTYPE_STRING:
        sdbError("table:%s, failed to update record:%s, record is not there, sdbVersion:%" PRId64 " id:%" PRId64,
                pTable->name, (char *) row, sdbVersion, pTable->id);
        break;
      case SDB_KEYTYPE_AUTO:
        sdbError("table:%s, failed to update record:%d, record is not there, sdbVersion:%" PRId64 " id:%" PRId64,
                pTable->name, *(int32_t *) row, sdbVersion, pTable->id);
        break;
      default:
        sdbError("table:%s, failed to update record, record is not there, sdbVersion:%" PRId64 " id:%" PRId64,
                pTable->name, sdbVersion, pTable->id);
        break;
    }
    return -1;
  }

  void *pMetaRow = pMeta->row;
  assert(pMetaRow != NULL);

  total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)malloc(total_size);
  if (rowHead == NULL) {
    sdbError("failed to allocate row head memory, sdb:%s", pTable->name);
    return -1;
  }
  memset(rowHead, 0, total_size);

  pthread_mutex_lock(&pTable->mutex);

  if (oper == SDB_OPER_GLOBAL) {
    if (sdbForwardDbReqToPeer(pTable, SDB_TYPE_UPDATE, rowHead->data, rowHead->rowSize) == 0) {
      sdbError("table:%s, failed to update record", pTable->name);
      pthread_mutex_unlock(&pTable->mutex);
      tfree(rowHead);
      return -1;
    } 
  }

  if (pMetaRow != row) {
    memcpy(rowHead->data, row, updateSize);
    rowHead->rowSize = updateSize;
  } else {
    (*(pTable->appTool))(SDB_TYPE_ENCODE, pMetaRow, rowHead->data, pTable->maxRowSize, &(rowHead->rowSize));
  }

  real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);

  // write to the new position
  rowHead->delimiter = SDB_DELIMITER;
  rowHead->id = pTable->id;
  if (taosCalcChecksumAppend(0, (uint8_t *)rowHead, real_size) < 0) {
    sdbError("failed to get checksum, sdb:%s id:%d", pTable->name, rowHead->id);
    pthread_mutex_unlock(&pTable->mutex);
    tfree(rowHead);
    return -1;
  }
  
  twrite(pTable->fd, rowHead, real_size);

  pMeta->id = pTable->id;
  pMeta->offset = pTable->size;
  pMeta->rowSize = rowHead->rowSize;
  pTable->size += real_size;

  sdbFinishCommit(pTable);

  switch (pTable->keyType) {
    case SDB_KEYTYPE_STRING:
      sdbTrace("table:%s, a record is updated:%s, sdbVersion:%" PRId64 " id:%" PRId64 " numOfRows:%" PRId64,
              pTable->name, (char *)row, sdbVersion, pTable->id, pTable->numOfRows);
      break;
    case SDB_KEYTYPE_AUTO:
      sdbTrace("table:%s, a record is updated:%d, sdbVersion:%" PRId64 " id:%" PRId64 " numOfRows:%" PRId64,
              pTable->name, *(int32_t *)row, sdbVersion, pTable->id, pTable->numOfRows);
      break;
    default:
      sdbTrace("table:%s, a record is updated, sdbVersion:%" PRId64 " id:%" PRId64 " numOfRows:%" PRId64, pTable->name, sdbVersion,
                pTable->id, pTable->numOfRows);
      break;
  }

  pTable->id++;
  sdbVersion++;

  pthread_mutex_unlock(&pTable->mutex);

  (*(pTable->appTool))(SDB_TYPE_UPDATE, pMetaRow, row, updateSize, NULL);  // update in upper layer

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
    (*(pTable->appTool))(SDB_TYPE_DESTROY, row, NULL, 0, NULL);
  }

  if (sdbCleanUpIndexFp[pTable->keyType]) (*sdbCleanUpIndexFp[pTable->keyType])(pTable->iHandle);

  if (pTable->fd) tclose(pTable->fd);

  pthread_mutex_destroy(&pTable->mutex);

  sdbNumOfTables--;
  sdbTrace("table:%s is closed, id:%" PRId64 " numOfTables:%d", pTable->name, pTable->id, sdbNumOfTables);

  tfree(pTable);
}

void sdbResetTable(SSdbTable *pTable) {
  /* SRowHead rowHead; */
  SRowMeta  rowMeta;
  int32_t       bytes;
  int32_t       total_size = 0;
  int32_t       real_size = 0;
  SRowHead *rowHead = NULL;
  void *    pMetaRow = NULL;
  int64_t   oldId = pTable->id;
  int32_t       oldNumOfRows = pTable->numOfRows;

  if (sdbOpenSdbFile(pTable) < 0) return;
  pTable->numOfRows = oldNumOfRows;

  total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  rowHead = (SRowHead *)malloc(total_size);
  if (rowHead == NULL) {
    sdbError("failed to allocate row head memory for reset, sdb:%s", pTable->name);
    return;
  }

  sdbPrint("open sdb file:%s for reset table", pTable->fn);

  while (1) {
    memset(rowHead, 0, total_size);

    bytes = read(pTable->fd, rowHead, sizeof(SRowHead));
    if (bytes < 0) {
      sdbError("failed to read sdb file:%s", pTable->fn);
      tfree(rowHead);
      return;
    }

    if (bytes == 0) break;

    if (bytes < sizeof(SRowHead) || rowHead->delimiter != SDB_DELIMITER) {
      pTable->size++;
      lseek(pTable->fd, -(bytes - 1), SEEK_CUR);
      continue;
    }

    if (rowHead->rowSize < 0 || rowHead->rowSize > pTable->maxRowSize) {
      sdbError("error row size in sdb file:%s for reset, id:%d rowSize:%d maxRowSize:%d",
               pTable->fn, rowHead->id, rowHead->rowSize, pTable->maxRowSize);
      pTable->size += sizeof(SRowHead);
      continue;
    }

    bytes = read(pTable->fd, rowHead->data, rowHead->rowSize + sizeof(TSCKSUM));
    if (bytes < rowHead->rowSize + sizeof(TSCKSUM)) {
      sdbError("failed to read sdb file:%s for reset, id:%d  rowSize:%d", pTable->fn, rowHead->id, rowHead->rowSize);
      break;
    }

    real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
    if (!taosCheckChecksumWhole((uint8_t *)rowHead, real_size)) {
      sdbError("error sdb checksum, sdb:%s  id:%d, skip", pTable->name, rowHead->id);
      pTable->size += real_size;
      continue;
    }

    if (abs(rowHead->id) > oldId) {  // not operated
      pMetaRow = sdbGetRow(pTable, rowHead->data);
      if (pMetaRow == NULL) {  // New object
        if (rowHead->id < 0) {
          sdbError("error sdb negative id:%d, sdb:%s, skip", rowHead->id, pTable->name);
        } else {
          rowMeta.id = rowHead->id;
          // TODO:Get rid of the rowMeta.offset and rowSize
          rowMeta.offset = pTable->size;
          rowMeta.rowSize = rowHead->rowSize;
          rowMeta.row = (*(pTable->appTool))(SDB_TYPE_DECODE, NULL, rowHead->data, rowHead->rowSize, NULL);
          (*sdbAddIndexFp[pTable->keyType])(pTable->iHandle, rowMeta.row, &rowMeta);
          pTable->numOfRows++;

          (*pTable->appTool)(SDB_TYPE_INSERT, rowMeta.row, NULL, 0, NULL);
        }
      } else {                  // already exists
        if (rowHead->id < 0) {  // Delete the object
          (*sdbDeleteIndexFp[pTable->keyType])(pTable->iHandle, rowHead->data);
          (*(pTable->appTool))(SDB_TYPE_DESTROY, pMetaRow, NULL, 0, NULL);
          pTable->numOfRows--;
        } else {  // update the object
          (*(pTable->appTool))(SDB_TYPE_UPDATE, pMetaRow, rowHead->data, rowHead->rowSize, NULL);
        }
      }
    }

    pTable->size += real_size;
    if (pTable->id < abs(rowHead->id)) pTable->id = abs(rowHead->id);
  }

  sdbVersion += (pTable->id - oldId);
  
  tfree(rowHead);

  sdbPrint("table:%s is updated, sdbVerion:%" PRId64 " id:%" PRId64, pTable->name, sdbVersion, pTable->id);
}

// TODO:A problem here :use snapshot file to sync another node will cause problem
void sdbSaveSnapShot(void *handle) {
  SSdbTable *pTable = (SSdbTable *)handle;
  SRowMeta * pMeta;
  void *     pNode = NULL;
  int32_t        total_size = 0;
  int32_t        real_size = 0;
  int32_t        size = 0;
  int32_t        numOfRows = 0;
  uint32_t   sdbEcommit = SDB_ENDCOMMIT;
  char *     dirc = NULL;
  char *     basec = NULL;
  /* char       action = SDB_TYPE_INSERT; */

  if (pTable == NULL) return;

  sdbTrace("Table:%s, save the snapshop", pTable->name);

  char fn[128] = "\0";
  dirc = strdup(pTable->fn);
  basec = strdup(pTable->fn);
  sprintf(fn, "%s/.%s", dirname(dirc), basename(basec));
  int32_t fd = open(fn, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  tfree(dirc);
  tfree(basec);

  total_size = sizeof(SRowHead) + pTable->maxRowSize + sizeof(TSCKSUM);
  SRowHead *rowHead = (SRowHead *)malloc(total_size);
  if (rowHead == NULL) {
    sdbError("failed to allocate memory while saving SDB snapshot, sdb:%s", pTable->name);
    return;
  }
  memset(rowHead, 0, size);

  // Write the header
  twrite(fd, &(pTable->header), sizeof(SSdbHeader));
  size += sizeof(SSdbHeader);
  twrite(fd, &sdbEcommit, sizeof(sdbEcommit));
  size += sizeof(sdbEcommit);

  while (1) {
    pNode = (*sdbFetchRowFp[pTable->keyType])(pTable->iHandle, pNode, (void **)&pMeta);
    if (pMeta == NULL) break;

    rowHead->delimiter = SDB_DELIMITER;
    rowHead->id = pMeta->id;
    (*(pTable->appTool))(SDB_TYPE_ENCODE, pMeta->row, rowHead->data, pTable->maxRowSize, &(rowHead->rowSize));
    real_size = sizeof(SRowHead) + rowHead->rowSize + sizeof(TSCKSUM);
    if (taosCalcChecksumAppend(0, (uint8_t *)rowHead, real_size) < 0) {
      sdbError("failed to get checksum while save sdb %s snapshot", pTable->name);
      tfree(rowHead);
      return;
    }

    /* write(fd, &action, sizeof(action)); */
    /* size += sizeof(action); */
    twrite(fd, rowHead, real_size);
    size += real_size;
    twrite(fd, &sdbEcommit, sizeof(sdbEcommit));
    size += sizeof(sdbEcommit);
    numOfRows++;
  }

  tfree(rowHead);

  // Remove the old file
  tclose(pTable->fd);
  remove(pTable->fn);
  // Rename the .sdb.db file to sdb.db file
  rename(fn, pTable->fn);
  pTable->fd = fd;
  pTable->size = size;
  pTable->numOfRows = numOfRows;

  fdatasync(pTable->fd);
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
