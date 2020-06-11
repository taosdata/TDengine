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

#include "vnode.h"
#include "vnodeUtil.h"
#include "vnodeStatus.h"

extern void         vnodeGetHeadTname(char *nHeadName, char *nLastName, int vnode, int fileId);
extern int          vnodeReadColumnToMem(int fd, SCompBlock *pBlock, SField **fields, int col, char *data, int dataSize,
                                         char *temp, char *buffer, int bufferSize);
extern int          vnodeSendShellSubmitRspMsg(SShellObj *pObj, int code, int numOfPoints);
extern void         vnodeGetHeadDataLname(char *headName, char *dataName, char *lastName, int vnode, int fileId);
extern int          vnodeCreateEmptyCompFile(int vnode, int fileId);
extern int          vnodeUpdateFreeSlot(SVnodeObj *pVnode);
extern SCacheBlock *vnodeGetFreeCacheBlock(SVnodeObj *pVnode);
extern int          vnodeCreateNeccessaryFiles(SVnodeObj *pVnode);

#define KEY_AT_INDEX(payload, step, idx) (*(TSKEY *)((char *)(payload) + (step) * (idx)))
typedef struct {
  void *     signature;
  SShellObj *pShell;
  SMeterObj *pObj;
  int        retry;
  TSKEY      firstKey;
  TSKEY      lastKey;
  int        importedRows;
  int        commit;  // start to commit if it is set to 1

  int   slot;  // slot/block to start writing the import data
  int   pos;   // pos to start writing the import data in the slot/block
  TSKEY key;

  // only for file
  int     numOfPoints;
  int64_t offset;  // offset in data file
  char *  payload;
  char *  opayload;  // allocated space for payload from client
  int     rows;
} SImportInfo;

typedef struct {
  // in .head file
  SCompHeader *pHeader;
  size_t       pHeaderSize;

  SCompInfo   compInfo;
  SCompBlock *pBlocks;
  // in .data file
  int     blockId;
  uint8_t blockLoadState;

  SField *pField;
  size_t  pFieldSize;

  SData *data[TSDB_MAX_COLUMNS];
  char * buffer;

  char *temp;

  char * tempBuffer;
  size_t tempBufferSize;
  // Variables for sendfile
  int64_t compInfoOffset;
  int64_t nextNo0Offset;  // next sid whose compInfoOffset > 0
  int64_t hfSize;
  int64_t driftOffset;

  int oldNumOfBlocks;
  int newNumOfBlocks;
  int last;
} SImportHandle;

typedef struct {
  int   slot;
  int   pos;
  int   oslot;  // old slot
  TSKEY nextKey;
} SBlockIter;

typedef struct {
  int64_t spos;
  int64_t epos;
  int64_t totalRows;
  char *  offset[];
} SMergeBuffer;

int vnodeImportData(SMeterObj *pObj, SImportInfo *pImport);

int vnodeFindKeyInCache(SImportInfo *pImport, int order) {
  SMeterObj * pObj = pImport->pObj;
  int         code = 0;
  SQuery      query;
  SCacheInfo *pInfo = (SCacheInfo *)pObj->pCache;

  TSKEY key = order ? pImport->firstKey : pImport->lastKey;
  memset(&query, 0, sizeof(query));
  query.order.order = order;
  query.skey = key;
  query.ekey = order ? pImport->lastKey : pImport->firstKey;
  vnodeSearchPointInCache(pObj, &query);

  if (query.slot < 0) {
    pImport->slot = pInfo->commitSlot;
    if (pInfo->commitPoint >= pObj->pointsPerBlock) pImport->slot = (pImport->slot + 1) % pInfo->maxBlocks;
    pImport->pos = 0;
    pImport->key = 0;
    dTrace("vid:%d sid:%d id:%s, key:%" PRId64 ", import to head of cache", pObj->vnode, pObj->sid, pObj->meterId, key);
    code = 0;
  } else {
    pImport->slot = query.slot;
    pImport->pos = query.pos;
    pImport->key = query.key;

    if (key != query.key) {
      if (order == 0) {
        // since pos is the position which has smaller key, data shall be imported after it
        pImport->pos++;
        if (pImport->pos >= pObj->pointsPerBlock) {
          pImport->slot = (pImport->slot + 1) % pInfo->maxBlocks;
          pImport->pos = 0;
        }
      } else {
        if (pImport->pos < 0) pImport->pos = 0;
      }
    }
    code = 0;
  }

  return code;
}

void vnodeGetValidDataRange(int vnode, TSKEY now, TSKEY *minKey, TSKEY *maxKey) {
  SVnodeObj *pVnode = vnodeList + vnode;

  int64_t delta = pVnode->cfg.daysPerFile * tsMsPerDay[(uint8_t)pVnode->cfg.precision];
  int     fid = now / delta;
  *minKey = (fid - pVnode->maxFiles + 1) * delta;
  *maxKey = (fid + 2) * delta - 1;
  return;
}

int vnodeImportPoints(SMeterObj *pObj, char *cont, int contLen, char source, void *param, int sversion,
                      int *pNumOfPoints, TSKEY now) {
  SSubmitMsg *pSubmit = (SSubmitMsg *)cont;
  SVnodeObj * pVnode = vnodeList + pObj->vnode;
  int         rows = 0;
  char *      payload = NULL;
  int         code = TSDB_CODE_SUCCESS;
  SCachePool *pPool = (SCachePool *)(pVnode->pCachePool);
  SShellObj * pShell = (SShellObj *)param;
  TSKEY       firstKey, lastKey;

  payload = pSubmit->payLoad;

  rows = htons(pSubmit->numOfRows);
  assert(rows > 0);
  int expectedLen = rows * pObj->bytesPerPoint + sizeof(pSubmit->numOfRows);
  if (expectedLen != contLen) {
    dError("vid:%d sid:%d id:%s, invalid import, expected:%d, contLen:%d", pObj->vnode, pObj->sid, pObj->meterId,
           expectedLen, contLen);
    return TSDB_CODE_WRONG_MSG_SIZE;
  }

  // Check timestamp context.
  TSKEY minKey = 0, maxKey = 0;
  firstKey = KEY_AT_INDEX(payload, pObj->bytesPerPoint, 0);
  lastKey = KEY_AT_INDEX(payload, pObj->bytesPerPoint, rows - 1);
  assert(firstKey <= lastKey);
  vnodeGetValidDataRange(pObj->vnode, now, &minKey, &maxKey);
  if (firstKey < minKey || firstKey > maxKey || lastKey < minKey || lastKey > maxKey) {
    dError(
        "vid:%d sid:%d id:%s, invalid timestamp to import, rows:%d firstKey: %" PRId64 " lastKey: %" PRId64 " minAllowedKey:%" PRId64 " "
        "maxAllowedKey:%" PRId64,
        pObj->vnode, pObj->sid, pObj->meterId, rows, firstKey, lastKey, minKey, maxKey);
    return TSDB_CODE_TIMESTAMP_OUT_OF_RANGE;
  }
    // forward to peers
  if (pShell && pVnode->cfg.replications > 1) {
    code = vnodeForwardToPeer(pObj, cont, contLen, TSDB_ACTION_IMPORT, sversion);
    if (code != 0) return code;
  }

  if (pVnode->cfg.commitLog && source != TSDB_DATA_SOURCE_LOG) {
    if (pVnode->logFd < 0) return TSDB_CODE_INVALID_COMMIT_LOG;
    code = vnodeWriteToCommitLog(pObj, TSDB_ACTION_IMPORT, cont, contLen, sversion);
    if (code != 0) return code;
  }

  /*
   * The timestamp of all records in a submit payload are always in ascending order, guaranteed by client, so here only
   * the first key.
   */
  if (firstKey > pObj->lastKey) {  // Just call insert
    code = vnodeInsertPoints(pObj, cont, contLen, TSDB_DATA_SOURCE_LOG, NULL, sversion, pNumOfPoints, now);
  } else {  // trigger import
    if (sversion != pObj->sversion) {
      dError("vid:%d sid:%d id:%s, invalid sversion, expected:%d received:%d", pObj->vnode, pObj->sid, pObj->meterId,
             pObj->sversion, sversion);
      return TSDB_CODE_OTHERS;
    }
  
    // check the table status for perform import historical data
    if ((code = vnodeSetMeterInsertImportStateEx(pObj, TSDB_METER_STATE_IMPORTING)) != TSDB_CODE_SUCCESS) {
      return code;
    }
    
    SImportInfo import = {0};

    dTrace("vid:%d sid:%d id:%s, try to import %d rows data, firstKey:%" PRId64 ", lastKey:%" PRId64 ", object lastKey:%" PRId64,
           pObj->vnode, pObj->sid, pObj->meterId, rows, firstKey, lastKey, pObj->lastKey);
    
    import.firstKey = firstKey;
    import.lastKey = lastKey;
    import.pObj = pObj;
    import.pShell = pShell;
    import.payload = payload;
    import.rows = rows;

    // FIXME: mutex here seems meaningless and num here still can be changed
    int32_t num = 0;
    pthread_mutex_lock(&pVnode->vmutex);
    num = pObj->numOfQueries;
    pthread_mutex_unlock(&pVnode->vmutex);

    int32_t commitInProcess = 0;

    pthread_mutex_lock(&pPool->vmutex);
    if (((commitInProcess = pPool->commitInProcess) == 1) || num > 0) {
      // mutual exclusion with read (need to change here)
      pthread_mutex_unlock(&pPool->vmutex);
      vnodeClearMeterState(pObj, TSDB_METER_STATE_IMPORTING);
      return TSDB_CODE_ACTION_IN_PROGRESS;

    } else {
      pPool->commitInProcess = 1;
      pthread_mutex_unlock(&pPool->vmutex);
      code = vnodeImportData(pObj, &import);
      *pNumOfPoints = import.importedRows;
    }
    pVnode->version++;
    vnodeClearMeterState(pObj, TSDB_METER_STATE_IMPORTING);
  }
  
  return code;
}

/* Function to search keys in a range
 *
 * Assumption: keys in payload are in ascending order
 *
 * @payload: data records, key in ascending order
 * @step:    bytes each record takes
 * @rows:    number of data records
 * @skey:    range start (included)
 * @ekey:    range end (included)
 * @srows:   rtype, start index of records
 * @nrows:   rtype, number of records in range
 *
 * @rtype:   0 means find data in the range
 *          -1 means find no data in the range
 */
static int vnodeSearchKeyInRange(char *payload, int step, int rows, TSKEY skey, TSKEY ekey, int *srow, int *nrows) {
  if (rows <= 0 || KEY_AT_INDEX(payload, step, 0) > ekey || KEY_AT_INDEX(payload, step, rows - 1) < skey || skey > ekey)
    return -1;

  int left = 0;
  int right = rows - 1;
  int mid;

  // Binary search the first key in payload >= skey
  do {
    mid = (left + right) / 2;
    if (skey < KEY_AT_INDEX(payload, step, mid)) {
      right = mid;
    } else if (skey > KEY_AT_INDEX(payload, step, mid)) {
      left = mid + 1;
    } else {
      break;
    }
  } while (left < right);

  if (skey <= KEY_AT_INDEX(payload, step, mid)) {
    *srow = mid;
  } else {
    if (mid + 1 >= rows) {
      return -1;
    } else {
      *srow = mid + 1;
    }
  }

  assert(skey <= KEY_AT_INDEX(payload, step, *srow));

  *nrows = 0;
  for (int i = *srow; i < rows; i++) {
    if (KEY_AT_INDEX(payload, step, i) <= ekey) {
      (*nrows)++;
    } else {
      break;
    }
  }

  if (*nrows == 0) return -1;

  return 0;
}

int vnodeOpenMinFilesForImport(int vnode, int fid) {
  char        dname[TSDB_FILENAME_LEN] = "\0";
  SVnodeObj * pVnode = vnodeList + vnode;
  struct stat filestat;
  int         minFileSize;

  minFileSize = TSDB_FILE_HEADER_LEN + sizeof(SCompHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM);

  vnodeGetHeadDataLname(pVnode->cfn, dname, pVnode->lfn, vnode, fid);

  // Open .head file
  pVnode->hfd = open(pVnode->cfn, O_RDONLY);
  if (pVnode->hfd < 0) {
    dError("vid:%d, failed to open head file:%s, reason:%s", vnode, pVnode->cfn, strerror(errno));
    taosLogError("vid:%d, failed to open head file:%s, reason:%s", vnode, pVnode->cfn, strerror(errno));
    goto _error_open;
  }

  fstat(pVnode->hfd, &filestat);
  if (filestat.st_size < minFileSize) {
    dError("vid:%d, head file:%s is corrupted", vnode, pVnode->cfn);
    taosLogError("vid:%d, head file:%s corrupted", vnode, pVnode->cfn);
    goto _error_open;
  }

  // Open .data file
  pVnode->dfd = open(dname, O_RDWR);
  if (pVnode->dfd < 0) {
    dError("vid:%d, failed to open data file:%s, reason:%s", vnode, dname, strerror(errno));
    taosLogError("vid:%d, failed to open data file:%s, reason:%s", vnode, dname, strerror(errno));
    goto _error_open;
  }

  fstat(pVnode->dfd, &filestat);
  if (filestat.st_size < TSDB_FILE_HEADER_LEN) {
    dError("vid:%d, data file:%s corrupted", vnode, dname);
    taosLogError("vid:%d, data file:%s corrupted", vnode, dname);
    goto _error_open;
  }

  // Open .last file
  pVnode->lfd = open(pVnode->lfn, O_RDWR);
  if (pVnode->lfd < 0) {
    dError("vid:%d, failed to open last file:%s, reason:%s", vnode, pVnode->lfn, strerror(errno));
    taosLogError("vid:%d, failed to open last file:%s, reason:%s", vnode, pVnode->lfn, strerror(errno));
    goto _error_open;
  }

  fstat(pVnode->lfd, &filestat);
  if (filestat.st_size < TSDB_FILE_HEADER_LEN) {
    dError("vid:%d, last file:%s corrupted", vnode, pVnode->lfn);
    taosLogError("vid:%d, last file:%s corrupted", vnode, pVnode->lfn);
    goto _error_open;
  }

  return 0;

_error_open:
  if (pVnode->hfd > 0) close(pVnode->hfd);
  pVnode->hfd = 0;

  if (pVnode->dfd > 0) close(pVnode->dfd);
  pVnode->dfd = 0;

  if (pVnode->lfd > 0) close(pVnode->lfd);
  pVnode->lfd = 0;

  return -1;
}

/* Function to open .t file and sendfile the first part
 */
int vnodeOpenTempFilesForImport(SImportHandle *pHandle, SMeterObj *pObj, int fid) {
  char        dHeadName[TSDB_FILENAME_LEN] = "\0";
  SVnodeObj * pVnode = vnodeList + pObj->vnode;
  struct stat filestat;
  int         sid;

  // cfn: .head
  if (readlink(pVnode->cfn, dHeadName, TSDB_FILENAME_LEN) < 0) return -1;

  size_t len = strlen(dHeadName);
  // switch head name
  switch (dHeadName[len - 1]) {
    case '0':
      dHeadName[len - 1] = '1';
      break;
    case '1':
      dHeadName[len - 1] = '0';
      break;
    default:
      dError("vid: %d, fid: %d, head target filename not end with 0 or 1", pVnode->vnode, fid);
      return -1;
  }

  vnodeGetHeadTname(pVnode->nfn, NULL, pVnode->vnode, fid);
  if (symlink(dHeadName, pVnode->nfn) < 0) return -1;

  pVnode->nfd = open(pVnode->nfn, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (pVnode->nfd < 0) {
    dError("vid:%d, failed to open new head file:%s, reason:%s", pVnode->vnode, pVnode->nfn, strerror(errno));
    taosLogError("vid:%d, failed to open new head file:%s, reason:%s", pVnode->vnode, pVnode->nfn, strerror(errno));
    return -1;
  }

  fstat(pVnode->hfd, &filestat);
  pHandle->hfSize = filestat.st_size;

  // Find the next sid whose compInfoOffset > 0
  for (sid = pObj->sid + 1; sid < pVnode->cfg.maxSessions; sid++) {
    if (pHandle->pHeader[sid].compInfoOffset > 0) break;
  }

  pHandle->nextNo0Offset = (sid == pVnode->cfg.maxSessions) ? pHandle->hfSize : pHandle->pHeader[sid].compInfoOffset;

  // FIXME: sendfile the original part
  // TODO: Here, we need to take the deleted table case in consideration, this function
  // just assume the case is handled before calling this function
  if (pHandle->pHeader[pObj->sid].compInfoOffset > 0) {
    pHandle->compInfoOffset = pHandle->pHeader[pObj->sid].compInfoOffset;
  } else {
    pHandle->compInfoOffset = pHandle->nextNo0Offset;
  }

  assert(pHandle->compInfoOffset <= pHandle->hfSize);

  lseek(pVnode->hfd, 0, SEEK_SET);
  lseek(pVnode->nfd, 0, SEEK_SET);
  if (tsendfile(pVnode->nfd, pVnode->hfd, NULL, pHandle->compInfoOffset) < 0) {
    return -1;
  }

  // Leave a SCompInfo space here
  lseek(pVnode->nfd, sizeof(SCompInfo), SEEK_CUR);

  return 0;
}

typedef enum { DATA_LOAD_TIMESTAMP = 0x1, DATA_LOAD_OTHER_DATA = 0x2 } DataLoadMod;

/* Function to load a block data at the requirement of mod
 */
static int vnodeLoadNeededBlockData(SMeterObj *pObj, SImportHandle *pHandle, int blockId, uint8_t loadMod, int *code) {
  size_t      size;
  SCompBlock *pBlock = pHandle->pBlocks + blockId;
  *code = TSDB_CODE_SUCCESS;

  SVnodeObj *pVnode = vnodeList + pObj->vnode;

  int dfd = pBlock->last ? pVnode->lfd : pVnode->dfd;

  if (pHandle->blockId != blockId) {
    pHandle->blockId = blockId;
    pHandle->blockLoadState = 0;
  }

  if (pHandle->blockLoadState == 0){ // Reload pField
    size = sizeof(SField) * pBlock->numOfCols + sizeof(TSCKSUM);
    if (pHandle->pFieldSize < size) {
      pHandle->pField = (SField *)realloc((void *)(pHandle->pField), size);
      if (pHandle->pField == NULL) {
        dError("vid: %d, sid: %d, meterId: %s, failed to allocate memory, size: %ul", pObj->vnode, pObj->sid,
               pObj->meterId, size);
        *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
        return -1;
      }
      pHandle->pFieldSize = size;
    }

    lseek(dfd, pBlock->offset, SEEK_SET);
    if (read(dfd, (void *)(pHandle->pField), pHandle->pFieldSize) < 0) {
      dError("vid:%d sid:%d meterId:%s, failed to read data file, size:%zu reason:%s", pVnode->vnode, pObj->sid,
             pObj->meterId, pHandle->pFieldSize, strerror(errno));
      *code = TSDB_CODE_FILE_CORRUPTED;
      return -1;
    }

    if (!taosCheckChecksumWhole((uint8_t *)(pHandle->pField), pHandle->pFieldSize)) {
      dError("vid:%d sid:%d meterId:%s, data file %s is broken since checksum mismatch", pVnode->vnode, pObj->sid,
             pObj->meterId, pVnode->lfn);
      *code = TSDB_CODE_FILE_CORRUPTED;
      return -1;
    }
  }

  {  // Allocate necessary buffer
    size = pObj->bytesPerPoint * pObj->pointsPerFileBlock +
           (sizeof(SData) + EXTRA_BYTES + sizeof(TSCKSUM)) * pObj->numOfColumns;
    if (pHandle->buffer == NULL) {
      pHandle->buffer = malloc(size);
      if (pHandle->buffer == NULL) {
        dError("vid: %d, sid: %d, meterId: %s, failed to allocate memory, size: %ul", pObj->vnode, pObj->sid,
               pObj->meterId, size);
        *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
        return -1;
      }

      // TODO: Init data
      pHandle->data[0] = (SData *)(pHandle->buffer);
      for (int col = 1; col < pObj->numOfColumns; col++) {
        pHandle->data[col] = (SData *)((char *)(pHandle->data[col - 1]) + sizeof(SData) + EXTRA_BYTES +
                                       sizeof(TSCKSUM) + pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes);
      }
    }

    if (pHandle->temp == NULL) {
      pHandle->temp = malloc(size);
      if (pHandle->temp == NULL) {
        dError("vid: %d, sid: %d, meterId: %s, failed to allocate memory, size: %ul", pObj->vnode, pObj->sid,
               pObj->meterId, size);
        *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
        return -1;
      }
    }

    if (pHandle->tempBuffer == NULL) {
      pHandle->tempBufferSize = pObj->maxBytes * pObj->pointsPerFileBlock + EXTRA_BYTES + sizeof(TSCKSUM);
      pHandle->tempBuffer = malloc(pHandle->tempBufferSize);
      if (pHandle->tempBuffer == NULL) {
        dError("vid: %d, sid: %d, meterId: %s, failed to allocate memory, size: %ul", pObj->vnode, pObj->sid,
               pObj->meterId, pHandle->tempBufferSize);
        *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
        return -1;
      }
    }
  }

  if ((loadMod & DATA_LOAD_TIMESTAMP) &&
      (~(pHandle->blockLoadState & DATA_LOAD_TIMESTAMP))) {  // load only timestamp part
    if (vnodeReadColumnToMem(dfd, pBlock, &(pHandle->pField), PRIMARYKEY_TIMESTAMP_COL_INDEX,
                             pHandle->data[PRIMARYKEY_TIMESTAMP_COL_INDEX]->data, sizeof(TSKEY) * pBlock->numOfPoints,
                             pHandle->temp, pHandle->tempBuffer, pHandle->tempBufferSize) < 0) {
      *code = TSDB_CODE_FILE_CORRUPTED;
      return -1;
    }

    pHandle->blockLoadState |= DATA_LOAD_TIMESTAMP;
  }

  if ((loadMod & DATA_LOAD_OTHER_DATA) && (~(pHandle->blockLoadState & DATA_LOAD_OTHER_DATA))) {  // load other columns
    for (int col = 1; col < pBlock->numOfCols; col++) {
      if (vnodeReadColumnToMem(dfd, pBlock, &(pHandle->pField), col, pHandle->data[col]->data,
                               pBlock->numOfPoints * pObj->schema[col].bytes, pHandle->temp, pHandle->tempBuffer,
                               pHandle->tempBufferSize) < 0) {
        *code = TSDB_CODE_FILE_CORRUPTED;
        return -1;
      }
    }

    pHandle->blockLoadState |= DATA_LOAD_OTHER_DATA;
  }

  return 0;
}

static int vnodeCloseImportFiles(SMeterObj *pObj, SImportHandle *pHandle) {
  SVnodeObj *pVnode = vnodeList + pObj->vnode;
  char       dpath[TSDB_FILENAME_LEN] = "\0";
  SCompInfo  compInfo;

#ifdef _ALPINE
  off_t    offset = 0;
#else
  __off_t  offset = 0;
#endif

  if (pVnode->nfd > 0) {
    offset = lseek(pVnode->nfd, 0, SEEK_CUR);
    assert(offset == pHandle->nextNo0Offset + pHandle->driftOffset);

    {  // Write the SCompInfo part
      compInfo.uid = pObj->uid;
      compInfo.last = pHandle->last;
      compInfo.numOfBlocks = pHandle->newNumOfBlocks + pHandle->oldNumOfBlocks;
      compInfo.delimiter = TSDB_VNODE_DELIMITER;
      taosCalcChecksumAppend(0, (uint8_t *)(&compInfo), sizeof(SCompInfo));

      lseek(pVnode->nfd, pHandle->compInfoOffset, SEEK_SET);
      if (twrite(pVnode->nfd, (void *)(&compInfo), sizeof(SCompInfo)) < 0) {
        dError("vid:%d sid:%d meterId:%s, failed to wirte SCompInfo, reason:%s", pObj->vnode, pObj->sid, pObj->meterId,
               strerror(errno));
        return -1;
      }
    }

    // Write the rest of the SCompBlock part
    if (pHandle->hfSize > pHandle->nextNo0Offset) {
      lseek(pVnode->nfd, 0, SEEK_END);
      lseek(pVnode->hfd, pHandle->nextNo0Offset, SEEK_SET);
      if (tsendfile(pVnode->nfd, pVnode->hfd, NULL, pHandle->hfSize - pHandle->nextNo0Offset) < 0) {
        dError("vid:%d sid:%d meterId:%s, failed to sendfile, size:%" PRId64 ", reason:%s", pObj->vnode, pObj->sid,
               pObj->meterId, pHandle->hfSize - pHandle->nextNo0Offset, strerror(errno));
        return -1;
      }
    }

    // Write SCompHeader part
    pHandle->pHeader[pObj->sid].compInfoOffset = pHandle->compInfoOffset;
    for (int sid = pObj->sid + 1; sid < pVnode->cfg.maxSessions; ++sid) {
      if (pHandle->pHeader[sid].compInfoOffset > 0) {
        pHandle->pHeader[sid].compInfoOffset += pHandle->driftOffset;
      }
    }

    taosCalcChecksumAppend(0, (uint8_t *)(pHandle->pHeader), pHandle->pHeaderSize);
    lseek(pVnode->nfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
    if (twrite(pVnode->nfd, (void *)(pHandle->pHeader), pHandle->pHeaderSize) < 0) {
      dError("vid:%d sid:%d meterId:%s, failed to wirte SCompHeader part, size:%zu, reason:%s", pObj->vnode, pObj->sid,
             pObj->meterId, pHandle->pHeaderSize, strerror(errno));
      return -1;
    }
  }

  // Close opened files
  close(pVnode->dfd);
  pVnode->dfd = 0;

  close(pVnode->hfd);
  pVnode->hfd = 0;

  close(pVnode->lfd);
  pVnode->lfd = 0;

  if (pVnode->nfd > 0) {
    close(pVnode->nfd);
    pVnode->nfd = 0;

    readlink(pVnode->cfn, dpath, TSDB_FILENAME_LEN);
    rename(pVnode->nfn, pVnode->cfn);
    remove(dpath);
  }

  return 0;
}

static void vnodeConvertRowsToCols(SMeterObj *pObj, const char *payload, int rows, SData *data[], int rowOffset) {
  int sdataRow;
  int offset;

  for (int row = 0; row < rows; ++row) {
    sdataRow = row + rowOffset;
    offset = 0;
    for (int col = 0; col < pObj->numOfColumns; ++col) {
      memcpy(data[col]->data + sdataRow * pObj->schema[col].bytes, payload + pObj->bytesPerPoint * row + offset,
             pObj->schema[col].bytes);

      offset += pObj->schema[col].bytes;
    }
  }
}

static int vnodeMergeDataIntoFile(SImportInfo *pImport, const char *payload, int rows, int fid) {
  SMeterObj *   pObj = (SMeterObj *)(pImport->pObj);
  SVnodeObj *   pVnode = vnodeList + pObj->vnode;
  SImportHandle importHandle;
  size_t        size = 0;
  SData *       data[TSDB_MAX_COLUMNS];
  char *        buffer = NULL;
  SData *       cdata[TSDB_MAX_COLUMNS];
  char *        cbuffer = NULL;
  SCompBlock    compBlock;
  TSCKSUM       checksum = 0;
  int           pointsImported = 0;
  int           code = TSDB_CODE_SUCCESS;
  SCachePool *  pPool = (SCachePool *)pVnode->pCachePool;
  SCacheInfo *  pInfo = (SCacheInfo *)(pObj->pCache);
  TSKEY         lastKeyImported = 0;

  TSKEY delta = pVnode->cfg.daysPerFile * tsMsPerDay[(uint8_t)pVnode->cfg.precision];
  TSKEY minFileKey = fid * delta;
  TSKEY maxFileKey = minFileKey + delta - 1;
  TSKEY firstKey = KEY_AT_INDEX(payload, pObj->bytesPerPoint, 0);
  TSKEY lastKey = KEY_AT_INDEX(payload, pObj->bytesPerPoint, rows - 1);

  assert(firstKey >= minFileKey && firstKey <= maxFileKey && lastKey >= minFileKey && lastKey <= maxFileKey);

  // create neccessary files
  pVnode->commitFirstKey = firstKey;
  if (vnodeCreateNeccessaryFiles(pVnode) < 0) return TSDB_CODE_OTHERS;

  assert(pVnode->commitFileId == fid);

  // Open least files to import .head(hfd) .data(dfd) .last(lfd)
  if (vnodeOpenMinFilesForImport(pObj->vnode, fid) < 0) return TSDB_CODE_FILE_CORRUPTED;

  memset(&importHandle, 0, sizeof(SImportHandle));

  {  // Load SCompHeader part from .head file
    importHandle.pHeaderSize = sizeof(SCompHeader) * pVnode->cfg.maxSessions + sizeof(TSCKSUM);
    importHandle.pHeader = (SCompHeader *)malloc(importHandle.pHeaderSize);
    if (importHandle.pHeader == NULL) {
      dError("vid: %d, sid: %d, meterId: %s, failed to allocate memory, size: %ul", pObj->vnode, pObj->sid,
             pObj->meterId, importHandle.pHeaderSize);
      code = TSDB_CODE_SERV_OUT_OF_MEMORY;
      goto _error_merge;
    }

    lseek(pVnode->hfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
    if (read(pVnode->hfd, (void *)(importHandle.pHeader), importHandle.pHeaderSize) < importHandle.pHeaderSize) {
      dError("vid: %d, sid: %d, meterId: %s, fid: %d failed to read SCompHeader part, reason:%s", pObj->vnode,
             pObj->sid, pObj->meterId, fid, strerror(errno));
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _error_merge;
    }

    if (!taosCheckChecksumWhole((uint8_t *)(importHandle.pHeader), importHandle.pHeaderSize)) {
      dError("vid: %d, sid: %d, meterId: %s, fid: %d SCompHeader part is broken", pObj->vnode, pObj->sid, pObj->meterId,
             fid);
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _error_merge;
    }
  }

  {  // Initialize data[] and cdata[], which is used to hold data to write to data file
    size = pObj->bytesPerPoint * pVnode->cfg.rowsInFileBlock + (sizeof(SData) + EXTRA_BYTES + sizeof(TSCKSUM)) * pObj->numOfColumns;

    buffer = (char *)malloc(size);
    if (buffer == NULL) {
      dError("vid: %d, sid: %d, meterId: %s, failed to allocate memory, size: %ul", pObj->vnode, pObj->sid,
             pObj->meterId, size);
      code = TSDB_CODE_SERV_OUT_OF_MEMORY;
      goto _error_merge;
    }

    cbuffer = (char *)malloc(size);
    if (cbuffer == NULL) {
      dError("vid: %d, sid: %d, meterId: %s, failed to allocate memory, size: %ul", pObj->vnode, pObj->sid,
             pObj->meterId, size);
      code = TSDB_CODE_SERV_OUT_OF_MEMORY;
      goto _error_merge;
    }

    data[0] = (SData *)buffer;
    cdata[0] = (SData *)cbuffer;

    for (int col = 1; col < pObj->numOfColumns; col++) {
      data[col] = (SData *)((char *)data[col - 1] + sizeof(SData) + EXTRA_BYTES + sizeof(TSCKSUM) +
                            pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes);
      cdata[col] = (SData *)((char *)cdata[col - 1] + sizeof(SData) + EXTRA_BYTES + sizeof(TSCKSUM) +
                             pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes);
    }
  }

  if (importHandle.pHeader[pObj->sid].compInfoOffset == 0) {  // No data in this file, just write it
  _write_empty_point:
    if (vnodeOpenTempFilesForImport(&importHandle, pObj, fid) < 0) {
      code = TSDB_CODE_OTHERS;
      goto _error_merge;
    }
    importHandle.oldNumOfBlocks = 0;
    importHandle.driftOffset += sizeof(SCompInfo);
    lastKeyImported = lastKey;

    for (int rowsWritten = 0; rowsWritten < rows;) {
      int rowsToWrite = MIN(pVnode->cfg.rowsInFileBlock, (rows - rowsWritten) /* the rows left */);
      vnodeConvertRowsToCols(pObj, payload + rowsWritten * pObj->bytesPerPoint, rowsToWrite, data, 0);
      pointsImported += rowsToWrite;

      compBlock.last = 1;
      if (vnodeWriteBlockToFile(pObj, &compBlock, data, cdata, rowsToWrite) < 0) {
        // TODO: deal with ERROR here
      }

      importHandle.last = compBlock.last;

      checksum = taosCalcChecksum(checksum, (uint8_t *)(&compBlock), sizeof(SCompBlock));
      twrite(pVnode->nfd, &compBlock, sizeof(SCompBlock));
      importHandle.newNumOfBlocks++;
      importHandle.driftOffset += sizeof(SCompBlock);

      rowsWritten += rowsToWrite;
    }
    twrite(pVnode->nfd, &checksum, sizeof(TSCKSUM));
    importHandle.driftOffset += sizeof(TSCKSUM);
  } else {  // Else if there are old data in this file.
    {       // load SCompInfo and SCompBlock part
      lseek(pVnode->hfd, importHandle.pHeader[pObj->sid].compInfoOffset, SEEK_SET);
      if (read(pVnode->hfd, (void *)(&(importHandle.compInfo)), sizeof(SCompInfo)) < sizeof(SCompInfo)) {
        dError("vid:%d sid:%d meterId:%s, failed to read .head file, reason:%s", pVnode->vnode, pObj->sid,
               pObj->meterId, strerror(errno));
        code = TSDB_CODE_FILE_CORRUPTED;
        goto _error_merge;
      }

      if ((importHandle.compInfo.delimiter != TSDB_VNODE_DELIMITER) ||
          (!taosCheckChecksumWhole((uint8_t *)(&(importHandle.compInfo)), sizeof(SCompInfo)))) {
        dError("vid:%d sid:%d meterId:%s, .head file %s is broken, delemeter:%x", pVnode->vnode, pObj->sid,
               pObj->meterId, pVnode->cfn, importHandle.compInfo.delimiter);
        code = TSDB_CODE_FILE_CORRUPTED;
        goto _error_merge;
      }

      // Check the context of SCompInfo part
      if (importHandle.compInfo.uid != pObj->uid) {  // The data belongs to the other meter
        goto _write_empty_point;
      }

      importHandle.oldNumOfBlocks = importHandle.compInfo.numOfBlocks;
      importHandle.last = importHandle.compInfo.last;

      size = sizeof(SCompBlock) * importHandle.compInfo.numOfBlocks + sizeof(TSCKSUM);
      importHandle.pBlocks = (SCompBlock *)malloc(size);
      if (importHandle.pBlocks == NULL) {
        dError("vid:%d sid:%d meterId:%s, failed to allocate importHandle.pBlock, size:%ul", pVnode->vnode, pObj->sid,
               pObj->meterId, size);
        code = TSDB_CODE_SERV_OUT_OF_MEMORY;
        goto _error_merge;
      }

      if (read(pVnode->hfd, (void *)(importHandle.pBlocks), size) < size) {
        dError("vid:%d sid:%d meterId:%s, failed to read importHandle.pBlock, reason:%s", pVnode->vnode, pObj->sid,
               pObj->meterId, strerror(errno));
        code = TSDB_CODE_FILE_CORRUPTED;
        goto _error_merge;
      }

      if (!taosCheckChecksumWhole((uint8_t *)(importHandle.pBlocks), size)) {
        dError("vid:%d sid:%d meterId:%s, pBlock part is broken in %s", pVnode->vnode, pObj->sid, pObj->meterId,
               pVnode->cfn);
        code = TSDB_CODE_FILE_CORRUPTED;
        goto _error_merge;
      }
    }

    /* Now we have _payload_, we have _importHandle.pBlocks_, just merge payload into the importHandle.pBlocks
     *
     * Input: payload, pObj->bytesPerBlock, rows, importHandle.pBlocks
     */
    {
      int        payloadIter = 0;
      SBlockIter blockIter = {0, 0, 0, 0};

      while (1) {
        if (payloadIter >= rows) {  // payload end, break
          // write the remaining blocks to the file
          if (pVnode->nfd > 0) {
            int blocksLeft = importHandle.compInfo.numOfBlocks - blockIter.oslot;
            if (blocksLeft > 0) {
              checksum = taosCalcChecksum(checksum, (uint8_t *)(importHandle.pBlocks + blockIter.oslot),
                                          sizeof(SCompBlock) * blocksLeft);
              if (twrite(pVnode->nfd, (void *)(importHandle.pBlocks + blockIter.oslot),
                         sizeof(SCompBlock) * blocksLeft) < 0) {
                dError("vid:%d sid:%d meterId:%s, failed to write %s file, size:%ul, reason:%s", pVnode->vnode,
                       pObj->sid, pObj->meterId, pVnode->nfn, sizeof(SCompBlock) * blocksLeft, strerror(errno));
                code = TSDB_CODE_OTHERS;
                goto _error_merge;
              }
            }

            if (twrite(pVnode->nfd, (void *)(&checksum), sizeof(TSCKSUM)) < 0) {
              dError("vid:%d sid:%d meterId:%s, failed to write %s file, size:%ul, reason:%s", pVnode->vnode, pObj->sid,
                     pObj->meterId, pVnode->nfn, sizeof(TSCKSUM), strerror(errno));
              code = TSDB_CODE_OTHERS;
              goto _error_merge;
            }
          }
          break;
        }

        if (blockIter.slot >= importHandle.compInfo.numOfBlocks) {  // blocks end, break
          // Should never come here
          assert(false);
        }

        TSKEY key = KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter);

        {  // Binary search the (slot, pos) which is >= key as well as nextKey
          int   left = blockIter.slot;
          int   right = importHandle.compInfo.numOfBlocks - 1;
          TSKEY minKey = importHandle.pBlocks[left].keyFirst;
          TSKEY maxKey = importHandle.pBlocks[right].keyLast;

          assert(minKey <= maxKey);

          if (key < minKey) {  // Case 1. write just ahead the blockIter.slot
            blockIter.slot = left;
            blockIter.pos = 0;
            blockIter.nextKey = minKey;
          } else if (key > maxKey) {                 // Case 2. write to the end
            if (importHandle.pBlocks[right].last) {  // Case 2.1 last block in .last file, need to merge
              assert(importHandle.last != 0);
              importHandle.last = 0;
              blockIter.slot = right;
              blockIter.pos = importHandle.pBlocks[right].numOfPoints;
            } else {  // Case 2.2 just write after the last block
              blockIter.slot = right + 1;
              blockIter.pos = 0;
            }
            blockIter.nextKey = maxFileKey + 1;
          } else {  // Case 3. need to search the block for slot and pos
            if (key == minKey || key == maxKey) {
              if (tsAffectedRowsMod) pointsImported++;
              payloadIter++;
              continue;
            }

            // Here: minKey < key < maxKey

            int   mid;
            TSKEY blockMinKey;
            TSKEY blockMaxKey;

            // Binary search the slot
            do {
              mid = (left + right) / 2;
              blockMinKey = importHandle.pBlocks[mid].keyFirst;
              blockMaxKey = importHandle.pBlocks[mid].keyLast;

              assert(blockMinKey <= blockMaxKey);

              if (key < blockMinKey) {
                right = mid;
              } else if (key > blockMaxKey) {
                left = mid + 1;
              } else { /* blockMinKey <= key <= blockMaxKey */
                break;
              }
            } while (left < right);

            if (key == blockMinKey || key == blockMaxKey) {  // duplicate key
              if (tsAffectedRowsMod) pointsImported++;
              payloadIter++;
              continue;
            }

            // Get the slot
            if (key > blockMaxKey) { /* pos = 0 or pos = ? */
              blockIter.slot = mid + 1;
            } else { /* key < blockMinKey (pos = 0) || (key > blockMinKey && key < blockMaxKey) (pos=?) */
              blockIter.slot = mid;
            }

            // Get the pos
            assert(blockIter.slot < importHandle.compInfo.numOfBlocks);

            if (key == importHandle.pBlocks[blockIter.slot].keyFirst ||
                key == importHandle.pBlocks[blockIter.slot].keyLast) {
              if (tsAffectedRowsMod) pointsImported++;
              payloadIter++;
              continue;
            }

            assert(key < importHandle.pBlocks[blockIter.slot].keyLast);

            /* */
            if (key < importHandle.pBlocks[blockIter.slot].keyFirst) {
              blockIter.pos = 0;
              blockIter.nextKey = importHandle.pBlocks[blockIter.slot].keyFirst;
            } else {
              SCompBlock *pBlock = importHandle.pBlocks + blockIter.slot;
              if (pBlock->sversion != pObj->sversion) { /*TODO*/
              }
              if (vnodeLoadNeededBlockData(pObj, &importHandle, blockIter.slot, DATA_LOAD_TIMESTAMP, &code) < 0) {
                goto _error_merge;
              }
              int pos = (*vnodeSearchKeyFunc[pObj->searchAlgorithm])(
                  importHandle.data[PRIMARYKEY_TIMESTAMP_COL_INDEX]->data, pBlock->numOfPoints, key, TSQL_SO_ASC);
              assert(pos != 0);
              if (KEY_AT_INDEX(importHandle.data[PRIMARYKEY_TIMESTAMP_COL_INDEX]->data, sizeof(TSKEY), pos) == key) {
                if (tsAffectedRowsMod) pointsImported++;
                payloadIter++;
                continue;
              }

              blockIter.pos = pos;
              blockIter.nextKey = (blockIter.slot + 1 < importHandle.compInfo.numOfBlocks)
                                      ? importHandle.pBlocks[blockIter.slot + 1].keyFirst
                                      : maxFileKey + 1;
              // Need to merge with this block
              if (importHandle.pBlocks[blockIter.slot].last) {  // this is to merge with the last block
                assert((blockIter.slot == (importHandle.compInfo.numOfBlocks - 1)));
                importHandle.last = 0;
              }
            }
          }
        }

        int aslot = MIN(blockIter.slot, importHandle.compInfo.numOfBlocks - 1);
        int64_t sversion = importHandle.pBlocks[aslot].sversion;
        if (sversion != pObj->sversion) {
          code = TSDB_CODE_OTHERS;
          goto _error_merge;
        }

        // Open the new .t file if not opened yet.
        if (pVnode->nfd <= 0) {
          if (vnodeOpenTempFilesForImport(&importHandle, pObj, fid) < 0) {
            code = TSDB_CODE_OTHERS;
            goto _error_merge;
          }
        }

        if (blockIter.slot > blockIter.oslot) {  // write blocks in range [blockIter.oslot, blockIter.slot) to .t file
          checksum = taosCalcChecksum(checksum, (uint8_t *)(importHandle.pBlocks + blockIter.oslot),
                                      sizeof(SCompBlock) * (blockIter.slot - blockIter.oslot));
          if (twrite(pVnode->nfd, (void *)(importHandle.pBlocks + blockIter.oslot),
                     sizeof(SCompBlock) * (blockIter.slot - blockIter.oslot)) < 0) {
            dError("vid:%d sid:%d meterId:%s, failed to write %s file, size:%ul, reason:%s", pVnode->vnode, pObj->sid,
                   pObj->meterId, pVnode->nfn, sizeof(SCompBlock) * (blockIter.slot - blockIter.oslot),
                   strerror(errno));
            code = TSDB_CODE_OTHERS;
            goto _error_merge;
          }

          blockIter.oslot = blockIter.slot;
        }

        if (blockIter.pos == 0) {  // No need to merge
          // copy payload part to data
          int rowOffset = 0;
          for (; payloadIter < rows; rowOffset++) {
            if (KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter) >= blockIter.nextKey) break;

            vnodeConvertRowsToCols(pObj, payload + pObj->bytesPerPoint * payloadIter, 1, data, rowOffset);
            pointsImported++;
            lastKeyImported = KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter);
            payloadIter++;
          }

          // write directly to .data file
          compBlock.last = 0;
          if (vnodeWriteBlockToFile(pObj, &compBlock, data, cdata, rowOffset) < 0) {
            // TODO: Deal with the ERROR here
          }

          checksum = taosCalcChecksum(checksum, (uint8_t *)(&compBlock), sizeof(SCompBlock));
          if (twrite(pVnode->nfd, &compBlock, sizeof(SCompBlock)) < 0) {
            // TODO : deal with the ERROR here
          }
          importHandle.newNumOfBlocks++;
          importHandle.driftOffset += sizeof(SCompBlock);
        } else {  // Merge block and payload from payloadIter

          if (vnodeLoadNeededBlockData(pObj, &importHandle, blockIter.slot,
                                       DATA_LOAD_TIMESTAMP | DATA_LOAD_OTHER_DATA, &code) < 0) {  // Load neccessary blocks
            goto _error_merge;
          }

          importHandle.oldNumOfBlocks--;
          importHandle.driftOffset -= sizeof(SCompBlock);

          int rowOffset = blockIter.pos;  // counter for data

          // Copy the front part
          for (int col = 0; col < pObj->numOfColumns; col++) {
            memcpy((void *)(data[col]->data), (void *)(importHandle.data[col]->data),
                   pObj->schema[col].bytes * blockIter.pos);
          }

          // Merge part
          while (1) {
            if (rowOffset >= pVnode->cfg.rowsInFileBlock) {  // data full in a block to commit
              compBlock.last = 0;
              if (vnodeWriteBlockToFile(pObj, &compBlock, data, cdata, rowOffset) < 0) {
                // TODO : deal with the ERROR here
              }

              checksum = taosCalcChecksum(checksum, (uint8_t *)(&compBlock), sizeof(SCompBlock));
              if (twrite(pVnode->nfd, (void *)(&compBlock), sizeof(SCompBlock)) < 0) {
                dError("vid:%d sid:%d meterId:%s, failed to write %s file, size:%ul, reason:%s", pVnode->vnode,
                       pObj->sid, pObj->meterId, pVnode->nfn, sizeof(SCompBlock), strerror(errno));
                goto _error_merge;
              }
              importHandle.newNumOfBlocks++;
              importHandle.driftOffset += sizeof(SCompBlock);
              rowOffset = 0;
            }

            if ((payloadIter >= rows || KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter) >= blockIter.nextKey) &&
                blockIter.pos >= importHandle.pBlocks[blockIter.slot].numOfPoints)
              break;

            if (payloadIter >= rows ||
                KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter) >= blockIter.nextKey) {  // payload end
              for (int col = 0; col < pObj->numOfColumns; col++) {
                memcpy(data[col]->data + rowOffset * pObj->schema[col].bytes,
                       importHandle.data[col]->data + pObj->schema[col].bytes * blockIter.pos, pObj->schema[col].bytes);
              }
              blockIter.pos++;
              rowOffset++;
            } else if (blockIter.pos >= importHandle.pBlocks[blockIter.slot].numOfPoints) {  // block end
              vnodeConvertRowsToCols(pObj, payload + pObj->bytesPerPoint * payloadIter, 1, data, rowOffset);
              pointsImported++;
              lastKeyImported = KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter);
              payloadIter++;
              rowOffset++;
            } else {
              if (KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter) ==
                  KEY_AT_INDEX(importHandle.data[PRIMARYKEY_TIMESTAMP_COL_INDEX]->data, sizeof(TSKEY),
                               blockIter.pos)) {  // duplicate key
                if (tsAffectedRowsMod) pointsImported++;
                payloadIter++;
                continue;
              } else if (KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter) <
                         KEY_AT_INDEX(importHandle.data[PRIMARYKEY_TIMESTAMP_COL_INDEX]->data, sizeof(TSKEY),
                                      blockIter.pos)) {
                vnodeConvertRowsToCols(pObj, payload + pObj->bytesPerPoint * payloadIter, 1, data, rowOffset);
                pointsImported++;
                lastKeyImported = KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter);
                payloadIter++;
                rowOffset++;
              } else {
                for (int col = 0; col < pObj->numOfColumns; col++) {
                  memcpy(data[col]->data + rowOffset * pObj->schema[col].bytes,
                         importHandle.data[col]->data + pObj->schema[col].bytes * blockIter.pos,
                         pObj->schema[col].bytes);
                }
                blockIter.pos++;
                rowOffset++;
              }
            }
          }
          if (rowOffset > 0) {  // data full in a block to commit
            compBlock.last = 0;
            if (vnodeWriteBlockToFile(pObj, &compBlock, data, cdata, rowOffset) < 0) {
              // TODO : deal with the ERROR here
            }

            checksum = taosCalcChecksum(checksum, (uint8_t *)(&compBlock), sizeof(SCompBlock));
            if (twrite(pVnode->nfd, (void *)(&compBlock), sizeof(SCompBlock)) < 0) {
              dError("vid:%d sid:%d meterId:%s, failed to write %s file, size:%ul, reason:%s", pVnode->vnode, pObj->sid,
                     pObj->meterId, pVnode->nfn, sizeof(SCompBlock), strerror(errno));
              goto _error_merge;
            }
            importHandle.newNumOfBlocks++;
            importHandle.driftOffset += sizeof(SCompBlock);
            rowOffset = 0;
          }

          blockIter.slot++;
          blockIter.oslot = blockIter.slot;
        }
      }
    }
  }

  // Write the SCompInfo part
  if (vnodeCloseImportFiles(pObj, &importHandle) < 0) {
    code = TSDB_CODE_OTHERS;
    goto _error_merge;
  }

  pImport->importedRows += pointsImported;

  pthread_mutex_lock(&(pPool->vmutex));
  if (pInfo->numOfBlocks > 0) {
    int   slot = (pInfo->currentSlot - pInfo->numOfBlocks + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
    TSKEY firstKeyInCache = *((TSKEY *)(pInfo->cacheBlocks[slot]->offset[0]));

    // data may be in commited cache, cache shall be released
    if (lastKeyImported > firstKeyInCache) {
      while (slot != pInfo->commitSlot) {
        SCacheBlock *pCacheBlock = pInfo->cacheBlocks[slot];
        vnodeFreeCacheBlock(pCacheBlock);
        slot = (slot + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
      }

      if (pInfo->commitPoint == pObj->pointsPerBlock) {
        if (pInfo->cacheBlocks[pInfo->commitSlot]->pMeterObj == pObj) {
          vnodeFreeCacheBlock(pInfo->cacheBlocks[pInfo->commitSlot]);
        }
      }
    }
  }
  pthread_mutex_unlock(&(pPool->vmutex));

  // TODO: free the allocated memory
  tfree(buffer);
  tfree(cbuffer);
  tfree(importHandle.pHeader);
  tfree(importHandle.pBlocks);
  tfree(importHandle.pField);
  tfree(importHandle.buffer);
  tfree(importHandle.temp);
  tfree(importHandle.tempBuffer);

  return code;

_error_merge:
  tfree(buffer);
  tfree(cbuffer);
  tfree(importHandle.pHeader);
  tfree(importHandle.pBlocks);
  tfree(importHandle.pField);
  tfree(importHandle.buffer);
  tfree(importHandle.temp);
  tfree(importHandle.tempBuffer);

  close(pVnode->dfd);
  pVnode->dfd = 0;

  close(pVnode->hfd);
  pVnode->hfd = 0;

  close(pVnode->lfd);
  pVnode->lfd = 0;

  if (pVnode->nfd > 0) {
    close(pVnode->nfd);
    pVnode->nfd = 0;
    remove(pVnode->nfn);
  }

  return code;
}

#define FORWARD_ITER(iter, step, slotLimit, posLimit) \
  {                                                   \
    if ((iter.pos) + (step) < (posLimit)) {           \
      (iter.pos) = (iter.pos) + (step);               \
    } else {                                          \
      (iter.pos) = 0;                                 \
      (iter.slot) = ((iter.slot) + 1) % (slotLimit);  \
    }                                                 \
  }

int isCacheEnd(SBlockIter iter, SMeterObj *pMeter) {
  SCacheInfo *pInfo = (SCacheInfo *)(pMeter->pCache);
  int         slot = 0;
  int         pos = 0;

  if (pInfo->cacheBlocks[pInfo->currentSlot]->numOfPoints == pMeter->pointsPerBlock) {
    slot = (pInfo->currentSlot + 1) % (pInfo->maxBlocks);
    pos = 0;
  } else {
    slot = pInfo->currentSlot;
    pos = pInfo->cacheBlocks[pInfo->currentSlot]->numOfPoints;
  }
  return ((iter.slot == slot) && (iter.pos == pos));
}

static void vnodeFlushMergeBuffer(SMergeBuffer *pBuffer, SBlockIter *pWriteIter, SBlockIter *pCacheIter,
                                  SMeterObj *pObj, SCacheInfo *pInfo, int checkBound) {
  // Function to flush the merge buffer data to cache
  if (pWriteIter->pos == pObj->pointsPerBlock) {
    pWriteIter->pos = 0;
    pWriteIter->slot = (pWriteIter->slot + 1) % pInfo->maxBlocks;
  }

  while (pBuffer->spos != pBuffer->epos) {
    if (checkBound && pWriteIter->slot == pCacheIter->slot && pWriteIter->pos == pCacheIter->pos) break;
    for (int col = 0; col < pObj->numOfColumns; col++) {
      memcpy(pInfo->cacheBlocks[pWriteIter->slot]->offset[col] + pObj->schema[col].bytes * pWriteIter->pos,
             pBuffer->offset[col] + pObj->schema[col].bytes * pBuffer->spos, pObj->schema[col].bytes);
    }

    if (pWriteIter->pos + 1 < pObj->pointsPerBlock) {
      (pWriteIter->pos)++;
    } else {
      pInfo->cacheBlocks[pWriteIter->slot]->numOfPoints = pWriteIter->pos + 1;
      pWriteIter->slot = (pWriteIter->slot + 1) % pInfo->maxBlocks;
      pWriteIter->pos = 0;
    }

    pBuffer->spos = (pBuffer->spos + 1) % pBuffer->totalRows;
  }

  if ((!checkBound) && pWriteIter->pos != 0) {
    pInfo->cacheBlocks[pWriteIter->slot]->numOfPoints = pWriteIter->pos;
  }
}

int vnodeImportDataToCache(SImportInfo *pImport, const char *payload, const int rows) {
  SMeterObj *   pObj = pImport->pObj;
  SVnodeObj *   pVnode = vnodeList + pObj->vnode;
  int           code = -1;
  SCacheInfo *  pInfo = (SCacheInfo *)(pObj->pCache);
  int           payloadIter;
  SCachePool *  pPool = (SCachePool *)(pVnode->pCachePool);
  int           isCacheIterEnd = 0;
  int           spayloadIter = 0;
  int           isAppendData = 0;
  int           rowsImported = 0;
  int           totalRows = 0;
  size_t        size = 0;
  SMergeBuffer *pBuffer = NULL;

  TSKEY firstKey = KEY_AT_INDEX(payload, pObj->bytesPerPoint, 0);
  TSKEY lastKey = KEY_AT_INDEX(payload, pObj->bytesPerPoint, rows - 1);

  assert(firstKey <= lastKey && firstKey > pObj->lastKeyOnFile);

  // TODO: make this condition less strict
  if (pObj->freePoints < rows || pObj->freePoints < (pObj->pointsPerBlock << 1)) {  // No free room to hold the data
    dError("vid:%d sid:%d id:%s, import failed, cache is full, freePoints:%d", pObj->vnode, pObj->sid, pObj->meterId,
           pObj->freePoints);
    pImport->importedRows = 0;
    pImport->commit = 1;
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    return code;
  }

  if (pInfo->numOfBlocks == 0) {
    if (vnodeAllocateCacheBlock(pObj) < 0) {
      pImport->importedRows = 0;
      pImport->commit = 1;
      code = TSDB_CODE_ACTION_IN_PROGRESS;
      return code;
    }
  }

  // Find the first importable record from payload
  pImport->lastKey = lastKey;
  for (payloadIter = 0; payloadIter < rows; payloadIter++) {
    TSKEY key = KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter);
    if (key == pObj->lastKey) {
      if (tsAffectedRowsMod) rowsImported++;
      continue;
    }
    if (key > pObj->lastKey) {  // Just as insert
      pImport->slot = pInfo->currentSlot;
      pImport->pos = pInfo->cacheBlocks[pImport->slot]->numOfPoints;
      isCacheIterEnd = 1;
      break;
    } else {
      pImport->firstKey = key;
      if (vnodeFindKeyInCache(pImport, 1) < 0) {
        goto _exit;
      }

      if (pImport->firstKey != pImport->key) break;
      if (tsAffectedRowsMod) rowsImported++;
    }
  }

  if (payloadIter == rows) {
    pImport->importedRows += rowsImported;
    code = 0;
    goto _exit;
  }

  spayloadIter = payloadIter;
  if (pImport->pos == pObj->pointsPerBlock) assert(isCacheIterEnd);

  // Allocate a new merge buffer work as buffer
  totalRows = pObj->pointsPerBlock + rows - payloadIter + 1;
  size = sizeof(SMergeBuffer) + sizeof(char *) * pObj->numOfColumns + pObj->bytesPerPoint * totalRows;
  pBuffer = (SMergeBuffer *)malloc(size);
  if (pBuffer == NULL) {
    dError("vid:%d sid:%d meterId:%s, failed to allocate memory, size:%d", pObj->vnode, pObj->sid, pObj->meterId, size);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }
  pBuffer->spos = 0;
  pBuffer->epos = 0;
  pBuffer->totalRows = totalRows;
  pBuffer->offset[0] = (char *)pBuffer + sizeof(SMergeBuffer) + sizeof(char *) * pObj->numOfColumns;
  for (int col = 1; col < pObj->numOfColumns; col++) {
    pBuffer->offset[col] = pBuffer->offset[col - 1] + pObj->schema[col - 1].bytes * totalRows;
  }

  // TODO: take pImport->pos = pObj->pointsPerBlock into consideration
  {                                                              // Do the merge staff
    SBlockIter cacheIter = {pImport->slot, pImport->pos, 0, 0};  // Iter to traverse old cache data
    SBlockIter writeIter = {pImport->slot, pImport->pos, 0, 0};  // Iter to write data to cache
    int        availPoints = pObj->pointsPerBlock - pInfo->cacheBlocks[pInfo->currentSlot]->numOfPoints;

    assert(availPoints >= 0);

    while (1) {
      if ((payloadIter >= rows) && isCacheIterEnd) break;

      if ((pBuffer->epos + 1) % pBuffer->totalRows == pBuffer->spos) {  // merge buffer is full, flush
        vnodeFlushMergeBuffer(pBuffer, &writeIter, &cacheIter, pObj, pInfo, 1);
      }

      TSKEY payloadKey = (payloadIter < rows) ? KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter) : INT64_MAX;
      TSKEY cacheKey = (isCacheIterEnd) ? INT64_MAX : KEY_AT_INDEX(pInfo->cacheBlocks[cacheIter.slot]->offset[0], sizeof(TSKEY), cacheIter.pos);

      if (cacheKey < payloadKey) {  // if (payload end || (cacheIter not end && payloadKey > blockKey)), consume cache
        for (int col = 0; col < pObj->numOfColumns; col++) {
          memcpy(pBuffer->offset[col] + pObj->schema[col].bytes * pBuffer->epos,
                 pInfo->cacheBlocks[cacheIter.slot]->offset[col] + pObj->schema[col].bytes * cacheIter.pos,
                 pObj->schema[col].bytes);
        }
        FORWARD_ITER(cacheIter, 1, pInfo->maxBlocks, pObj->pointsPerBlock);
        isCacheIterEnd = isCacheEnd(cacheIter, pObj);
      } else if (cacheKey > payloadKey) {  // cacheIter end || (payloadIter not end && payloadKey < blockKey), consume payload
        if (availPoints == 0) {                      // Need to allocate a new cache block
          pthread_mutex_lock(&(pPool->vmutex));
          // TODO: Need to check if there are enough slots to hold a new one
          SCacheBlock *pNewBlock = vnodeGetFreeCacheBlock(pVnode);
          if (pNewBlock == NULL) {  // Failed to allocate a new cache block, need to commit and loop over the remaining cache records
            pthread_mutex_unlock(&(pPool->vmutex));
            payloadIter = rows;
            code = TSDB_CODE_ACTION_IN_PROGRESS;
            pImport->commit = 1;
            continue;
          }
          
          assert(pInfo->numOfBlocks <= pInfo->maxBlocks);
          if (pInfo->numOfBlocks == pInfo->maxBlocks) {
            vnodeFreeCacheBlock(pInfo->cacheBlocks[(pInfo->currentSlot + 1) % pInfo->maxBlocks]);
          }

          pNewBlock->pMeterObj = pObj;
          pNewBlock->offset[0] = (char *)pNewBlock + sizeof(SCacheBlock) + sizeof(char *) * pObj->numOfColumns;
          for (int col = 1; col < pObj->numOfColumns; col++)
            pNewBlock->offset[col] = pNewBlock->offset[col - 1] + pObj->schema[col - 1].bytes * pObj->pointsPerBlock;

          int newSlot = writeIter.slot;
          if (newSlot != ((pInfo->currentSlot + 1) % pInfo->maxBlocks)) {
            newSlot = (newSlot + 1) % pInfo->maxBlocks;
          }
          pInfo->blocks++;
          int tblockId = pInfo->blocks;

          if ((writeIter.slot != pInfo->currentSlot) && (writeIter.slot != ((pInfo->currentSlot + 1) % pInfo->maxBlocks))) {
            for (int tslot = pInfo->currentSlot; tslot != writeIter.slot;) {
              int nextSlot = (tslot + 1) % pInfo->maxBlocks;
              pInfo->cacheBlocks[nextSlot] = pInfo->cacheBlocks[tslot];
              pInfo->cacheBlocks[nextSlot]->slot = nextSlot;
              pInfo->cacheBlocks[nextSlot]->blockId = tblockId--;
              tslot = (tslot - 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
            }
          }

          int index = pNewBlock->index;
          if (cacheIter.slot == writeIter.slot && cacheIter.slot != ((pInfo->currentSlot + 1) % pInfo->maxBlocks)) {
            pNewBlock->numOfPoints = pInfo->cacheBlocks[cacheIter.slot]->numOfPoints;
            int pointsLeft = pInfo->cacheBlocks[cacheIter.slot]->numOfPoints - cacheIter.pos;
            if (pointsLeft > 0) {
              for (int col = 0; col < pObj->numOfColumns; col++) {
                memcpy((void *)(pNewBlock->offset[col] + pObj->schema[col].bytes*cacheIter.pos),
                       pInfo->cacheBlocks[cacheIter.slot]->offset[col] + pObj->schema[col].bytes * cacheIter.pos,
                       pObj->schema[col].bytes * pointsLeft);
              }
            }
          }
          pNewBlock->blockId = tblockId;
          pNewBlock->slot = newSlot;
          pNewBlock->index = index;
          pInfo->cacheBlocks[newSlot] = pNewBlock;
          pInfo->numOfBlocks++;
          pInfo->unCommittedBlocks++;
          pInfo->currentSlot = (pInfo->currentSlot + 1) % pInfo->maxBlocks;
          pthread_mutex_unlock(&(pPool->vmutex));
          cacheIter.slot = (cacheIter.slot + 1) % pInfo->maxBlocks;
          // move a cache of data forward
          availPoints = pObj->pointsPerBlock;
        }

        int offset = 0;
        for (int col = 0; col < pObj->numOfColumns; col++) {
          memcpy(pBuffer->offset[col] + pObj->schema[col].bytes * pBuffer->epos,
                 payload + pObj->bytesPerPoint * payloadIter + offset, pObj->schema[col].bytes);
          offset += pObj->schema[col].bytes;
        }
        if (spayloadIter == payloadIter) {// update pVnode->firstKey
          pthread_mutex_lock(&(pVnode->vmutex));
          if (KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter) < pVnode->firstKey) pVnode->firstKey = firstKey;
          pthread_mutex_unlock(&(pVnode->vmutex));
        }
        if (isCacheIterEnd) {
          pObj->lastKey = KEY_AT_INDEX(payload, pObj->bytesPerPoint, payloadIter);
          if (!isAppendData) isAppendData = 1;
        }

        rowsImported++;
        availPoints--;
        payloadIter++;

      } else {
        if (tsAffectedRowsMod) rowsImported++;
        payloadIter++;
        continue;
      }
      pBuffer->epos = (pBuffer->epos + 1) % pBuffer->totalRows;
    }

    if (pBuffer->spos != pBuffer->epos) { // Flush the remaining data in the merge buffer
      vnodeFlushMergeBuffer(pBuffer, &writeIter, &cacheIter, pObj, pInfo, 0);
    } else {
      // Should never come here
      assert(false);
    }

    if (isAppendData) {
      pthread_mutex_lock(&(pVnode->vmutex));
      if (pObj->lastKey > pVnode->lastKey) pVnode->lastKey = pObj->lastKey;
      pthread_mutex_unlock(&(pVnode->vmutex));
    }
  }
  pImport->importedRows += rowsImported;
  atomic_fetch_sub_32(&(pObj->freePoints), rowsImported);

  code = TSDB_CODE_SUCCESS;

_exit:
  tfree(pBuffer);
  return code;
}

int vnodeImportDataToFiles(SImportInfo *pImport, char *payload, const int rows) {
  int code = 0;
  // TODO : Check the correctness of pObj and pVnode
  SMeterObj *pObj = (SMeterObj *)(pImport->pObj);
  SVnodeObj *pVnode = vnodeList + pObj->vnode;

  int64_t delta = pVnode->cfg.daysPerFile * tsMsPerDay[(uint8_t)pVnode->cfg.precision];
  int     sfid = KEY_AT_INDEX(payload, pObj->bytesPerPoint, 0) / delta;
  int     efid = KEY_AT_INDEX(payload, pObj->bytesPerPoint, rows - 1) / delta;

  for (int fid = sfid; fid <= efid; fid++) {
    TSKEY skey = fid * delta;
    TSKEY ekey = skey + delta - 1;
    int   srow = 0, nrows = 0;

    if (vnodeSearchKeyInRange(payload, pObj->bytesPerPoint, rows, skey, ekey, &srow, &nrows) < 0) continue;

    assert(nrows > 0);

    dTrace("vid:%d sid:%d meterId:%s, %d rows of data will be imported to file %d, srow:%d firstKey:%" PRId64 " lastKey:%" PRId64,
           pObj->vnode, pObj->sid, pObj->meterId, nrows, fid, srow, KEY_AT_INDEX(payload, pObj->bytesPerPoint, srow),
           KEY_AT_INDEX(payload, pObj->bytesPerPoint, (srow + nrows - 1)));

    code = vnodeMergeDataIntoFile(pImport, payload + (srow * pObj->bytesPerPoint), nrows, fid);
    if (code != TSDB_CODE_SUCCESS) break;
  }

  return code;
}

// TODO : add offset in pShell to make it avoid repeatedly deal with messages
int vnodeImportData(SMeterObj *pObj, SImportInfo *pImport) {
  int         code = 0;
  int         srow = 0, nrows = 0;
  SVnodeObj * pVnode = vnodeList + pObj->vnode;
  SCachePool *pPool = (SCachePool *)(pVnode->pCachePool);

  // 1. import data in range (pObj->lastKeyOnFile, INT64_MAX) into cache
  if (vnodeSearchKeyInRange(pImport->payload, pObj->bytesPerPoint, pImport->rows, pObj->lastKeyOnFile + 1, INT64_MAX,
                            &srow, &nrows) >= 0) {
    assert(nrows > 0);
    code = vnodeImportDataToCache(pImport, pImport->payload + pObj->bytesPerPoint * srow, nrows);
    if (pImport->commit) {  // Need to commit now
      pPool->commitInProcess = 0;
      vnodeProcessCommitTimer(pVnode, NULL);
      return code;
    }

    if (code != TSDB_CODE_SUCCESS) return code;
  }

  // 2. import data (0, pObj->lastKeyOnFile) into files
  if (vnodeSearchKeyInRange(pImport->payload, pObj->bytesPerPoint, pImport->rows, 0, pObj->lastKeyOnFile - 1, &srow,
                            &nrows) >= 0) {
    assert(nrows > 0);
    code = vnodeImportDataToFiles(pImport, pImport->payload + pObj->bytesPerPoint * srow, nrows);
  }

  pPool->commitInProcess = 0;

  return code;
}
