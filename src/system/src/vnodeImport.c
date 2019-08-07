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

#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "trpc.h"
#include "ttimer.h"
#include "vnode.h"
#include "vnodeMgmt.h"
#include "vnodeShell.h"
#include "vnodeShell.h"
#include "vnodeUtil.h"
#pragma GCC diagnostic ignored "-Wpointer-sign"
#pragma GCC diagnostic ignored "-Wint-conversion"

typedef struct {
  SCompHeader *headList;
  SCompInfo    compInfo;
  int          last;            // 0:last block in data file, 1:not the last block
  int          newBlocks;
  int          oldNumOfBlocks;
  int64_t      compInfoOffset;  // offset for compInfo in head file
  int64_t      leftOffset;      // copy from this offset to end of head file
  int64_t      hfdSize;         // old head file size
} SHeadInfo;

typedef struct {
  void      *signature;
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
  int     fileId;
  int64_t offset;  // offset in data file
  SData  *sdata[TSDB_MAX_COLUMNS];
  char   *buffer;
  char   *payload;
  char   *opayload;  // allocated space for payload from client
  int     rows;
} SImportInfo;

int vnodeImportData(SMeterObj *pObj, SImportInfo *pImport);

int vnodeGetImportStartPart(SMeterObj *pObj, char *payload, int rows, TSKEY key1) {
  int i;

  for (i = 0; i < rows; ++i) {
    TSKEY key = *((TSKEY *)(payload + i * pObj->bytesPerPoint));
    if (key >= key1) break;
  }

  return i;
}

int vnodeGetImportEndPart(SMeterObj *pObj, char *payload, int rows, char **pStart, TSKEY key0) {
  int i;

  for (i = 0; i < rows; ++i) {
    TSKEY key = *((TSKEY *)(payload + i * pObj->bytesPerPoint));
    if (key > key0) break;
  }

  *pStart = payload + i * pObj->bytesPerPoint;
  return rows - i;
}

int vnodeCloseFileForImport(SMeterObj *pObj, SHeadInfo *pHinfo) {
  SVnodeObj *pVnode = &vnodeList[pObj->vnode];
  SVnodeCfg *pCfg = &pVnode->cfg;
  TSCKSUM    chksum = 0;

  if (pHinfo->newBlocks == 0 || pHinfo->compInfoOffset == 0) return 0;

  if (pHinfo->oldNumOfBlocks == 0) write(pVnode->nfd, &chksum, sizeof(TSCKSUM));

  int leftSize = pHinfo->hfdSize - pHinfo->leftOffset;
  if (leftSize > 0) {
    lseek(pVnode->hfd, pHinfo->leftOffset, SEEK_SET);
    sendfile(pVnode->nfd, pVnode->hfd, NULL, leftSize);
  }

  pHinfo->compInfo.numOfBlocks += pHinfo->newBlocks;
  int offset = (pHinfo->compInfo.numOfBlocks - pHinfo->oldNumOfBlocks) * sizeof(SCompBlock);
  if (pHinfo->oldNumOfBlocks == 0) offset += sizeof(SCompInfo) + sizeof(TSCKSUM);

  pHinfo->headList[pObj->sid].compInfoOffset = pHinfo->compInfoOffset;
  for (int sid = pObj->sid + 1; sid < pCfg->maxSessions; ++sid) {
    if (pHinfo->headList[sid].compInfoOffset) pHinfo->headList[sid].compInfoOffset += offset;
  }

  lseek(pVnode->nfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  int tmsize = sizeof(SCompHeader) * pCfg->maxSessions + sizeof(TSCKSUM);
  taosCalcChecksumAppend(0, (uint8_t *)pHinfo->headList, tmsize);
  write(pVnode->nfd, pHinfo->headList, tmsize);

  int   size = pHinfo->compInfo.numOfBlocks * sizeof(SCompBlock);
  char *buffer = malloc(size);
  lseek(pVnode->nfd, pHinfo->compInfoOffset + sizeof(SCompInfo), SEEK_SET);
  read(pVnode->nfd, buffer, size);
  SCompBlock *pBlock = (SCompBlock *)(buffer + (pHinfo->compInfo.numOfBlocks - 1) * sizeof(SCompBlock));

  pHinfo->compInfo.uid = pObj->uid;
  pHinfo->compInfo.delimiter = TSDB_VNODE_DELIMITER;
  pHinfo->compInfo.last = pBlock->last;

  taosCalcChecksumAppend(0, (uint8_t *)(&pHinfo->compInfo), sizeof(SCompInfo));
  lseek(pVnode->nfd, pHinfo->compInfoOffset, SEEK_SET);
  write(pVnode->nfd, &pHinfo->compInfo, sizeof(SCompInfo));

  chksum = taosCalcChecksum(0, (uint8_t *)buffer, size);
  lseek(pVnode->nfd, pHinfo->compInfoOffset + sizeof(SCompInfo) + size, SEEK_SET);
  write(pVnode->nfd, &chksum, sizeof(TSCKSUM));
  free(buffer);

  vnodeCloseCommitFiles(pVnode);

  return 0;
}

int vnodeProcessLastBlock(SImportInfo *pImport, SHeadInfo *pHinfo, SData *data[]) {
  SMeterObj *pObj = pImport->pObj;
  SVnodeObj *pVnode = &vnodeList[pObj->vnode];
  SCompBlock lastBlock;
  int        code = 0;

  if (pHinfo->compInfo.last == 0) return 0;

  // read into memory
  uint64_t offset =
      pHinfo->compInfoOffset + (pHinfo->compInfo.numOfBlocks - 1) * sizeof(SCompBlock) + sizeof(SCompInfo);
  lseek(pVnode->hfd, offset, SEEK_SET);
  read(pVnode->hfd, &lastBlock, sizeof(SCompBlock));
  assert(lastBlock.last);

  if (lastBlock.sversion != pObj->sversion) {
    lseek(pVnode->lfd, lastBlock.offset, SEEK_SET);
    lastBlock.offset = lseek(pVnode->dfd, 0, SEEK_END);
    sendfile(pVnode->dfd, pVnode->lfd, NULL, lastBlock.len);

    lastBlock.last = 0;
    lseek(pVnode->hfd, offset, SEEK_SET);
    write(pVnode->hfd, &lastBlock, sizeof(SCompBlock));
  } else {
    vnodeReadLastBlockToMem(pObj, &lastBlock, data);
    pHinfo->compInfo.numOfBlocks--;
    code = lastBlock.numOfPoints;
    pImport->slot--;
  }

  return code;
}

int vnodeOpenFileForImport(SImportInfo *pImport, char *payload, SHeadInfo *pHinfo, SData *data[]) {
  SMeterObj  *pObj = pImport->pObj;
  SVnodeObj  *pVnode = &vnodeList[pObj->vnode];
  SVnodeCfg  *pCfg = &pVnode->cfg;
  TSKEY       firstKey = *((TSKEY *)payload);
  struct stat filestat;
  int         sid, rowsBefore = 0;

  if (pVnode->nfd <= 0 || firstKey > pVnode->commitLastKey) {
    if (pVnode->nfd > 0) vnodeCloseFileForImport(pObj, pHinfo);

    pVnode->commitFirstKey = firstKey;
    if (vnodeOpenCommitFiles(pVnode, pObj->sid) < 0) return -1;

    fstat(pVnode->hfd, &filestat);
    pHinfo->hfdSize = filestat.st_size;
    pHinfo->newBlocks = 0;
    pHinfo->last = 1;  // by default, new blockes are at the end of block list

    lseek(pVnode->hfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
    read(pVnode->hfd, pHinfo->headList, sizeof(SCompHeader) * pCfg->maxSessions);

    if (pHinfo->headList[pObj->sid].compInfoOffset > 0) {
      lseek(pVnode->hfd, pHinfo->headList[pObj->sid].compInfoOffset, SEEK_SET);
      if (read(pVnode->hfd, &pHinfo->compInfo, sizeof(SCompInfo)) != sizeof(SCompInfo)) {
        dError("vid:%d sid:%d, failed to read compInfo from file:%s", pObj->vnode, pObj->sid, pVnode->cfn);
        return -1;
      }

      pHinfo->compInfoOffset = pHinfo->headList[pObj->sid].compInfoOffset;
      pHinfo->leftOffset = pHinfo->headList[pObj->sid].compInfoOffset + sizeof(SCompInfo);
    } else {
      memset(&pHinfo->compInfo, 0, sizeof(SCompInfo));
      pHinfo->compInfo.uid = pObj->uid;

      for (sid = pObj->sid + 1; sid < pCfg->maxSessions; ++sid)
        if (pHinfo->headList[sid].compInfoOffset > 0) break;

      pHinfo->compInfoOffset = (sid == pCfg->maxSessions) ? pHinfo->hfdSize : pHinfo->headList[sid].compInfoOffset;
      pHinfo->leftOffset = pHinfo->compInfoOffset;
    }

    pHinfo->oldNumOfBlocks = pHinfo->compInfo.numOfBlocks;
    lseek(pVnode->hfd, 0, SEEK_SET);
    lseek(pVnode->nfd, 0, SEEK_SET);
    sendfile(pVnode->nfd, pVnode->hfd, NULL, pHinfo->compInfoOffset);
    write(pVnode->nfd, &pHinfo->compInfo, sizeof(SCompInfo));
    if (pHinfo->headList[pObj->sid].compInfoOffset > 0) lseek(pVnode->hfd, sizeof(SCompInfo), SEEK_CUR);

    if (pVnode->commitFileId < pImport->fileId) {
      if (pHinfo->compInfo.numOfBlocks > 0)
        pHinfo->leftOffset += pHinfo->compInfo.numOfBlocks * sizeof(SCompBlock) + sizeof(TSCKSUM);

      rowsBefore = vnodeProcessLastBlock(pImport, pHinfo, data);

      // copy all existing compBlockInfo
      lseek(pVnode->hfd, pHinfo->compInfoOffset + sizeof(SCompInfo), SEEK_SET);
      if (pHinfo->compInfo.numOfBlocks > 0)
        sendfile(pVnode->nfd, pVnode->hfd, NULL, pHinfo->compInfo.numOfBlocks * sizeof(SCompBlock));

    } else if (pVnode->commitFileId == pImport->fileId) {
      int slots = pImport->pos ? pImport->slot + 1 : pImport->slot;
      pHinfo->leftOffset += slots * sizeof(SCompBlock);

      // check if last block is at last file, if it is, read into memory
      if (pImport->pos == 0 && pHinfo->compInfo.numOfBlocks > 0 && pImport->slot == pHinfo->compInfo.numOfBlocks &&
          pHinfo->compInfo.last) {
        rowsBefore = vnodeProcessLastBlock(pImport, pHinfo, data);
      }

      // this block will be replaced by new blocks
      if (pImport->pos > 0) pHinfo->compInfo.numOfBlocks--;

      if (pImport->slot > 0) {
        lseek(pVnode->hfd, pHinfo->compInfoOffset + sizeof(SCompInfo), SEEK_SET);
        sendfile(pVnode->nfd, pVnode->hfd, NULL, pImport->slot * sizeof(SCompBlock));
      }

      if (pImport->slot < pHinfo->compInfo.numOfBlocks)
        pHinfo->last = 0;  // new blocks are not at the end of block list

    } else {
      // nothing

      pHinfo->last = 0;  // new blocks are not at the end of block list
    }
  }

  return rowsBefore;
}

extern int vnodeSendShellSubmitRspMsg(SShellObj *pObj, int code, int numOfPoints);
int vnodeImportToFile(SImportInfo *pImport);

void vnodeProcessImportTimer(void *param, void *tmrId) {
  SImportInfo *pImport = (SImportInfo *)param;
  if (pImport == NULL || pImport->signature != param) {
    dError("import timer is messed up, signature:%p", pImport);
    return;
  }

  SMeterObj  *pObj = pImport->pObj;
  SVnodeObj  *pVnode = &vnodeList[pObj->vnode];
  SCachePool *pPool = (SCachePool *)pVnode->pCachePool;
  SShellObj  *pShell = pImport->pShell;

  pImport->retry++;

  //slow query will block the import operation
  int32_t state = vnodeSetMeterState(pObj, TSDB_METER_STATE_IMPORTING);
  if (state >= TSDB_METER_STATE_DELETING) {
    dError("vid:%d sid:%d id:%s, meter is deleted, failed to import, state:%d",
           pObj->vnode, pObj->sid, pObj->meterId, state);
    return;
  }

  int32_t num = 0;
  pthread_mutex_lock(&pVnode->vmutex);
  num = pObj->numOfQueries;
  pthread_mutex_unlock(&pVnode->vmutex);

  //if the num == 0, it will never be increased before state is set to TSDB_METER_STATE_READY
  int32_t commitInProcess = 0;
  pthread_mutex_lock(&pPool->vmutex);
  if (((commitInProcess = pPool->commitInProcess) == 1) || num > 0 || state != TSDB_METER_STATE_READY) {
    pthread_mutex_unlock(&pPool->vmutex);
    vnodeClearMeterState(pObj, TSDB_METER_STATE_IMPORTING);

    if (pImport->retry < 1000) {
      dTrace("vid:%d sid:%d id:%s, import failed, retry later. commit in process or queries on it, or not ready."
             "commitInProcess:%d, numOfQueries:%d, state:%d", pObj->vnode, pObj->sid, pObj->meterId,
             commitInProcess, num, state);

      taosTmrStart(vnodeProcessImportTimer, 10, pImport, vnodeTmrCtrl);
      return;
    } else {
      pShell->code = TSDB_CODE_TOO_SLOW;
    }
  } else {
    pPool->commitInProcess = 1;
    pthread_mutex_unlock(&pPool->vmutex);
    int code = vnodeImportData(pObj, pImport);
    if (pShell) {
      pShell->code = code;
      pShell->numOfTotalPoints += pImport->importedRows;
    }
  }

  vnodeClearMeterState(pObj, TSDB_METER_STATE_IMPORTING);

  pVnode->version++;

  // send response back to shell
  if (pShell) {
    pShell->count--;
    if (pShell->count <= 0) vnodeSendShellSubmitRspMsg(pImport->pShell, pShell->code, pShell->numOfTotalPoints);
  }

  pImport->signature = NULL;
  free(pImport->opayload);
  free(pImport);
}

int vnodeImportToFile(SImportInfo *pImport) {
  SMeterObj  *pObj = pImport->pObj;
  SVnodeObj  *pVnode = &vnodeList[pObj->vnode];
  SVnodeCfg  *pCfg = &pVnode->cfg;
  SHeadInfo   headInfo;
  int         code = 0, col;
  SCompBlock  compBlock;
  char       *payload = pImport->payload;
  int         rows = pImport->rows;
  SCachePool *pPool = (SCachePool *)pVnode->pCachePool;

  TSKEY lastKey = *((TSKEY *)(payload + pObj->bytesPerPoint * (rows - 1)));
  TSKEY firstKey = *((TSKEY *)payload);
  memset(&headInfo, 0, sizeof(headInfo));
  headInfo.headList = malloc(sizeof(SCompHeader) * pCfg->maxSessions + sizeof(TSCKSUM));

  SData *cdata[TSDB_MAX_COLUMNS];
  char * buffer1 =
      malloc(pObj->bytesPerPoint * pCfg->rowsInFileBlock + (sizeof(SData) + EXTRA_BYTES) * pObj->numOfColumns);
  cdata[0] = (SData *)buffer1;

  SData *data[TSDB_MAX_COLUMNS];
  char * buffer2 =
      malloc(pObj->bytesPerPoint * pCfg->rowsInFileBlock + (sizeof(SData) + EXTRA_BYTES) * pObj->numOfColumns);
  data[0] = (SData *)buffer2;

  for (col = 1; col < pObj->numOfColumns; ++col) {
    cdata[col] = (SData *)(((char *)cdata[col - 1]) + sizeof(SData) + EXTRA_BYTES +
                           pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes);
    data[col] = (SData *)(((char *)data[col - 1]) + sizeof(SData) + EXTRA_BYTES +
                          pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes);
  }

  int     rowsBefore = 0;
  int     rowsRead = 0;
  int     rowsUnread = 0;
  int     leftRows = rows;  // left number of rows of imported data
  int     row, rowsToWrite;
  int64_t offset[TSDB_MAX_COLUMNS];

  if (pImport->pos > 0) {
    for (col = 0; col < pObj->numOfColumns; ++col)
      memcpy(data[col]->data, pImport->sdata[col]->data, pImport->pos * pObj->schema[col].bytes);

    rowsBefore = pImport->pos;
    rowsRead = pImport->pos;
    rowsUnread = pImport->numOfPoints - pImport->pos;
  }

  dTrace("vid:%d sid:%d id:%s, %d rows data will be imported to file, firstKey:%ld lastKey:%ld",
         pObj->vnode, pObj->sid, pObj->meterId, rows, firstKey, lastKey);
  do {
    if (leftRows > 0) {
      code = vnodeOpenFileForImport(pImport, payload, &headInfo, data);
      if (code < 0) goto _exit;
      if (code > 0) {
        rowsBefore = code;
        code = 0;
      };
    } else {
      // if payload is already imported, rows unread shall still be processed
      rowsBefore = 0;
    }

    int rowsToProcess = pObj->pointsPerFileBlock - rowsBefore;
    if (rowsToProcess > leftRows) rowsToProcess = leftRows;

    for (col = 0; col < pObj->numOfColumns; ++col) {
      offset[col] = data[col]->data + rowsBefore * pObj->schema[col].bytes;
    }

    row = 0;
    if (leftRows > 0) {
      for (row = 0; row < rowsToProcess; ++row) {
        if (*((TSKEY *)payload) > pVnode->commitLastKey) break;

        for (col = 0; col < pObj->numOfColumns; ++col) {
          memcpy((void *)offset[col], payload, pObj->schema[col].bytes);
          payload += pObj->schema[col].bytes;
          offset[col] += pObj->schema[col].bytes;
        }
      }
    }

    leftRows -= row;
    rowsToWrite = rowsBefore + row;
    rowsBefore = 0;

    if (leftRows == 0 && rowsUnread > 0) {
      // copy the unread
      int rowsToCopy = pObj->pointsPerFileBlock - rowsToWrite;
      if (rowsToCopy > rowsUnread) rowsToCopy = rowsUnread;

      for (col = 0; col < pObj->numOfColumns; ++col) {
        int bytes = pObj->schema[col].bytes;
        memcpy(data[col]->data + rowsToWrite * bytes, pImport->sdata[col]->data + rowsRead * bytes, rowsToCopy * bytes);
      }

      rowsRead += rowsToCopy;
      rowsUnread -= rowsToCopy;
      rowsToWrite += rowsToCopy;
    }

    for (col = 0; col < pObj->numOfColumns; ++col) {
      data[col]->len = rowsToWrite * pObj->schema[col].bytes;
    }

    compBlock.last = headInfo.last;
    vnodeWriteBlockToFile(pObj, &compBlock, data, cdata, rowsToWrite);
    write(pVnode->nfd, &compBlock, sizeof(SCompBlock));

    rowsToWrite = 0;
    headInfo.newBlocks++;

  } while (leftRows > 0 || rowsUnread > 0);

  if (compBlock.keyLast > pObj->lastKeyOnFile)
    pObj->lastKeyOnFile = compBlock.keyLast;

  vnodeCloseFileForImport(pObj, &headInfo);
  dTrace("vid:%d sid:%d id:%s, %d rows data are imported to file", pObj->vnode, pObj->sid, pObj->meterId, rows);

  SCacheInfo *pInfo = (SCacheInfo *)pObj->pCache;
  pthread_mutex_lock(&pPool->vmutex);

  if (pInfo->numOfBlocks > 0) {
    int   slot = (pInfo->currentSlot - pInfo->numOfBlocks + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
    TSKEY firstKeyInCache = *((TSKEY *)(pInfo->cacheBlocks[slot]->offset[0]));

    // data may be in commited cache, cache shall be released
    if (lastKey > firstKeyInCache) {
      while (slot != pInfo->commitSlot) {
        SCacheBlock *pCacheBlock = pInfo->cacheBlocks[slot];
        vnodeFreeCacheBlock(pCacheBlock);
        slot = (slot + 1 + pInfo->maxBlocks) % pInfo->maxBlocks;
      }

      // last slot, the uncommitted slots shall be shifted
      SCacheBlock *pCacheBlock = pInfo->cacheBlocks[slot];
      int          points = pCacheBlock->numOfPoints - pInfo->commitPoint;
      if (points > 0) {
        for (int col = 0; col < pObj->numOfColumns; ++col) {
          int size = points * pObj->schema[col].bytes;
          memmove(pCacheBlock->offset[col], pCacheBlock->offset[col] + pObj->schema[col].bytes * pInfo->commitPoint, size);
        }
      }

      if (pInfo->commitPoint != pObj->pointsPerBlock) {
        // commit point shall be set to 0 if last block is not full
        pInfo->commitPoint = 0;
        pCacheBlock->numOfPoints = points;
        if (slot == pInfo->currentSlot) {
          __sync_fetch_and_add(&pObj->freePoints, pInfo->commitPoint);
        }
      } else {
        // if last block is full and committed
        SCacheBlock *pCacheBlock = pInfo->cacheBlocks[slot];
        if (pCacheBlock->pMeterObj == pObj) {
          vnodeFreeCacheBlock(pCacheBlock);
        }
      }
    }
  }

  if (lastKey > pObj->lastKeyOnFile) pObj->lastKeyOnFile = lastKey;

  pthread_mutex_unlock(&pPool->vmutex);

_exit:
  tfree(headInfo.headList);
  tfree(buffer1);
  tfree(buffer2);
  tfree(pImport->buffer);

  return code;
}

int vnodeImportToCache(SImportInfo *pImport, char *payload, int rows) {
  SMeterObj  *pObj = pImport->pObj;
  SVnodeObj  *pVnode = &vnodeList[pObj->vnode];
  SVnodeCfg  *pCfg = &pVnode->cfg;
  int         code = -1;
  SCacheInfo *pInfo = (SCacheInfo *)pObj->pCache;
  int         slot, pos, row, col, points, tpoints;

  char *data[TSDB_MAX_COLUMNS], *current[TSDB_MAX_COLUMNS];
  int   slots = pInfo->unCommittedBlocks + 1;
  int   trows = slots * pObj->pointsPerBlock + rows;  // max rows in buffer
  int   tsize = (trows / pObj->pointsPerBlock + 1) * pCfg->cacheBlockSize;
  TSKEY firstKey = *((TSKEY *)payload);
  TSKEY lastKey = *((TSKEY *)(payload + pObj->bytesPerPoint * (rows - 1)));

  if (pObj->freePoints < rows || pObj->freePoints < (pObj->pointsPerBlock << 1)) {
    dError("vid:%d sid:%d id:%s, import failed, cache is full, freePoints:%d", pObj->vnode, pObj->sid, pObj->meterId,
           pObj->freePoints);
    pImport->importedRows = 0;
    pImport->commit = 1;
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    return code;
  }

  dTrace("vid:%d sid:%d id:%s, %d rows data will be imported to cache, firstKey:%ld lastKey:%ld",
      pObj->vnode, pObj->sid, pObj->meterId, rows, firstKey, lastKey);

  pthread_mutex_lock(&(pVnode->vmutex));
  if (firstKey < pVnode->firstKey) pVnode->firstKey = firstKey;
  pthread_mutex_unlock(&(pVnode->vmutex));

  char *buffer = malloc(tsize);  // buffer to hold unCommitted data plus import data
  data[0] = buffer;
  current[0] = data[0];
  for (col = 1; col < pObj->numOfColumns; ++col) {
    data[col] = data[col - 1] + trows * pObj->schema[col - 1].bytes;
    current[col] = data[col];
  }

  // write import data into buffer first
  for (row = 0; row < rows; ++row) {
    for (col = 0; col < pObj->numOfColumns; ++col) {
      memcpy(current[col], payload, pObj->schema[col].bytes);
      payload += pObj->schema[col].bytes;
      current[col] += pObj->schema[col].bytes;
    }
  }

  // copy the overwritten data into buffer
  tpoints = rows;
  pos = pImport->pos;
  slot = pImport->slot;
  while (1) {
    points = pInfo->cacheBlocks[slot]->numOfPoints - pos;
    for (col = 0; col < pObj->numOfColumns; ++col) {
      int size = points * pObj->schema[col].bytes;
      memcpy(current[col], pInfo->cacheBlocks[slot]->offset[col] + pos * pObj->schema[col].bytes, size);
      current[col] += size;
    }
    pos = 0;
    tpoints += points;

    if (slot == pInfo->currentSlot) break;
    slot = (slot + 1) % pInfo->maxBlocks;
  }

  for (col = 0; col < pObj->numOfColumns; ++col) current[col] = data[col];
  pos = pImport->pos;

  // write back to existing slots first
  slot = pImport->slot;
  while (1) {
    points = (tpoints > pObj->pointsPerBlock - pos) ? pObj->pointsPerBlock - pos : tpoints;
    SCacheBlock *pCacheBlock = pInfo->cacheBlocks[slot];
    for (col = 0; col < pObj->numOfColumns; ++col) {
      int size = points * pObj->schema[col].bytes;
      memcpy(pCacheBlock->offset[col] + pos * pObj->schema[col].bytes, current[col], size);
      current[col] += size;
    }
    pCacheBlock->numOfPoints = points + pos;
    pos = 0;
    tpoints -= points;

    if (slot == pInfo->currentSlot) break;
    slot = (slot + 1) % pInfo->maxBlocks;
  }

  // allocate new cache block if there are still data left
  while (tpoints > 0) {
    pImport->commit = vnodeAllocateCacheBlock(pObj);
    if (pImport->commit < 0) goto _exit;
    points = (tpoints > pObj->pointsPerBlock) ? pObj->pointsPerBlock : tpoints;
    SCacheBlock *pCacheBlock = pInfo->cacheBlocks[pInfo->currentSlot];
    for (col = 0; col < pObj->numOfColumns; ++col) {
      int size = points * pObj->schema[col].bytes;
      memcpy(pCacheBlock->offset[col] + pos * pObj->schema[col].bytes, current[col], size);
      current[col] += size;
    }
    tpoints -= points;
    pCacheBlock->numOfPoints = points;
  }

  code = 0;
  __sync_fetch_and_sub(&pObj->freePoints, rows);
  dTrace("vid:%d sid:%d id:%s, %d rows data are imported to cache", pObj->vnode, pObj->sid, pObj->meterId, rows);

_exit:
  free(buffer);
  return code;
}

int vnodeFindKeyInFile(SImportInfo *pImport, int order) {
  SMeterObj    *pObj = pImport->pObj;
  SVnodeObj    *pVnode = &vnodeList[pObj->vnode];
  int           code = -1;
  SQuery        query;
  SColumnFilter colList[TSDB_MAX_COLUMNS] = {0};

  TSKEY key = order ? pImport->firstKey : pImport->lastKey;
  memset(&query, 0, sizeof(query));
  query.order.order = order;
  query.skey = key;
  query.ekey = order ? INT64_MAX : 0;
  query.colList = colList;
  query.numOfCols = pObj->numOfColumns;

  for (int16_t i = 0; i < pObj->numOfColumns; ++i) {
    colList[i].data.colId = pObj->schema[i].colId;
    colList[i].data.bytes = pObj->schema[i].bytes;
    colList[i].data.type = pObj->schema[i].type;

    colList[i].colIdx = i;
    colList[i].colIdxInBuf = i;
  }

  int ret = vnodeSearchPointInFile(pObj, &query);

  if (ret >= 0) {
    if (query.slot < 0) {
      pImport->slot = 0;
      pImport->pos = 0;
      pImport->key = 0;
      pImport->fileId = pVnode->fileId - pVnode->numOfFiles + 1;
      dTrace("vid:%d sid:%d id:%s, import to head of file", pObj->vnode, pObj->sid, pObj->meterId);
      code = 0;
    } else if (query.slot >= 0) {
      code = 0;
      pImport->slot = query.slot;
      pImport->pos = query.pos;
      pImport->key = query.key;
      pImport->fileId = query.fileId;
      SCompBlock *pBlock = &query.pBlock[query.slot];
      pImport->numOfPoints = pBlock->numOfPoints;

      if (pImport->key != key) {
        if (order == 0) {
          pImport->pos++;

          if (pImport->pos >= pBlock->numOfPoints) {
            pImport->slot++;
            pImport->pos = 0;
          }
        } else {
          if (pImport->pos < 0) pImport->pos = 0;
        }
      }

      if (pImport->key != key && pImport->pos > 0) {
        if ( pObj->sversion != pBlock->sversion ) {
          dError("vid:%d sid:%d id:%s, import sversion not matached, expected:%d received:%d", pObj->vnode, pObj->sid,
                 pBlock->sversion, pObj->sversion);
          code = TSDB_CODE_OTHERS;
        } else {
          pImport->offset = pBlock->offset;

          pImport->buffer =
              malloc(pObj->bytesPerPoint * pVnode->cfg.rowsInFileBlock + sizeof(SData) * pObj->numOfColumns);
          pImport->sdata[0] = (SData *)pImport->buffer;
          for (int col = 1; col < pObj->numOfColumns; ++col)
            pImport->sdata[col] = (SData *)(((char *)pImport->sdata[col - 1]) + sizeof(SData) +
                                          pObj->pointsPerFileBlock * pObj->schema[col - 1].bytes);

          code = vnodeReadCompBlockToMem(pObj, &query, pImport->sdata);
          if (code < 0) {
            code = -code;
            tfree(pImport->buffer);
          }
        }
      }
    }
  } else {
    dError("vid:%d sid:%d id:%s, file is corrupted, import failed", pObj->vnode, pObj->sid, pObj->meterId);
    code = -ret;
  }

  tclose(query.hfd);
  tclose(query.dfd);
  tclose(query.lfd);
  vnodeFreeFields(&query);
  tfree(query.pBlock);

  return code;
}

int vnodeFindKeyInCache(SImportInfo *pImport, int order) {
  SMeterObj  *pObj = pImport->pObj;
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
    dTrace("vid:%d sid:%d id:%s, key:%ld, import to head of cache", pObj->vnode, pObj->sid, pObj->meterId, key);
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

int vnodeImportStartToCache(SImportInfo *pImport, char *payload, int rows) {
  int        code = 0;
  SMeterObj *pObj = pImport->pObj;

  code = vnodeFindKeyInCache(pImport, 1);
  if (code != 0) return code;

  if (pImport->key != pImport->firstKey) {
    rows = vnodeGetImportStartPart(pObj, payload, rows, pImport->key);
    pImport->importedRows = rows;
    code = vnodeImportToCache(pImport, payload, rows);
  } else {
    dTrace("vid:%d sid:%d id:%s, data is already imported to cache", pObj->vnode, pObj->sid, pObj->meterId);
  }

  return code;
}

int vnodeImportStartToFile(SImportInfo *pImport, char *payload, int rows) {
  int        code = 0;
  SMeterObj *pObj = pImport->pObj;

  code = vnodeFindKeyInFile(pImport, 1);
  if (code != 0) return code;

  if (pImport->key != pImport->firstKey) {
    pImport->payload = payload;
    pImport->rows = vnodeGetImportStartPart(pObj, payload, rows, pImport->key);
    pImport->importedRows = pImport->rows;
    code = vnodeImportToFile(pImport);
  } else {
    dTrace("vid:%d sid:%d id:%s, data is already imported to file", pObj->vnode, pObj->sid, pObj->meterId);
  }

  return code;
}

int vnodeImportWholeToFile(SImportInfo *pImport, char *payload, int rows) {
  int        code = 0;
  SMeterObj *pObj = pImport->pObj;

  code = vnodeFindKeyInFile(pImport, 0);
  if (code != 0) return code;

  if (pImport->key != pImport->lastKey) {
    pImport->payload = payload;
    pImport->rows = vnodeGetImportEndPart(pObj, payload, rows, &pImport->payload, pImport->key);
    pImport->importedRows = pImport->rows;
    code = vnodeImportToFile(pImport);
  } else {
    code = vnodeImportStartToFile(pImport, payload, rows);
  }

  return code;
}

int vnodeImportWholeToCache(SImportInfo *pImport, char *payload, int rows) {
  int        code = 0;
  SMeterObj *pObj = pImport->pObj;

  code = vnodeFindKeyInCache(pImport, 0);
  if (code != 0) return code;

  if (pImport->key != pImport->lastKey) {
    char *pStart;
    if ( pImport->key < pObj->lastKeyOnFile ) pImport->key = pObj->lastKeyOnFile;
    rows = vnodeGetImportEndPart(pObj, payload, rows, &pStart, pImport->key);
    pImport->importedRows = rows;
    code = vnodeImportToCache(pImport, pStart, rows);
  } else {
    if (pImport->firstKey > pObj->lastKeyOnFile) {
      code = vnodeImportStartToCache(pImport, payload, rows);
    } else if (pImport->firstKey < pObj->lastKeyOnFile) {
      code = vnodeImportStartToFile(pImport, payload, rows);
    } else {  // firstKey == pObj->lastKeyOnFile
      dTrace("vid:%d sid:%d id:%s, data is already there", pObj->vnode, pObj->sid, pObj->meterId);
    }
  }

  return code;
}

int vnodeImportPoints(SMeterObj *pObj, char *cont, int contLen, char source, void *param, int sversion,
                      int *pNumOfPoints) {
  SSubmitMsg *pSubmit = (SSubmitMsg *)cont;
  SVnodeObj  *pVnode = &vnodeList[pObj->vnode];
  int         rows;
  char       *payload;
  int         code = TSDB_CODE_ACTION_IN_PROGRESS;
  SCachePool *pPool = (SCachePool *)pVnode->pCachePool;
  SShellObj  *pShell = (SShellObj *)param;
  int         pointsImported = 0;

  rows = htons(pSubmit->numOfRows);
  int expectedLen = rows * pObj->bytesPerPoint + sizeof(pSubmit->numOfRows);
  if (expectedLen != contLen) {
    dError("vid:%d sid:%d id:%s, invalid import, expected:%d, contLen:%d", pObj->vnode, pObj->sid, pObj->meterId,
           expectedLen, contLen);
    return TSDB_CODE_WRONG_MSG_SIZE;
  }

  if (sversion != pObj->sversion) {
    dError("vid:%d sid:%d id:%s, invalid sversion, expected:%d received:%d", pObj->vnode, pObj->sid, pObj->meterId,
           pObj->sversion, sversion);
    return TSDB_CODE_OTHERS;
  }

  payload = pSubmit->payLoad;
  int firstId = (*(TSKEY *)payload)/pVnode->cfg.daysPerFile/tsMsPerDay[pVnode->cfg.precision];
  int lastId  = (*(TSKEY *)(payload+pObj->bytesPerPoint*(rows-1)))/pVnode->cfg.daysPerFile/tsMsPerDay[pVnode->cfg.precision];
  int cfile = taosGetTimestamp(pVnode->cfg.precision)/pVnode->cfg.daysPerFile/tsMsPerDay[pVnode->cfg.precision];
  if ((firstId <= cfile - pVnode->maxFiles) || (firstId > cfile + 1) || (lastId <= cfile - pVnode->maxFiles) || (lastId > cfile + 1)) {
    dError("vid:%d sid:%d id:%s, invalid timestamp to import, firstKey: %ld lastKey: %ld",
        pObj->vnode, pObj->sid, pObj->meterId, *(TSKEY *)(payload), *(TSKEY *)(payload+pObj->bytesPerPoint*(rows-1)));
    return TSDB_CODE_TIMESTAMP_OUT_OF_RANGE;
  }

  if ( pVnode->cfg.commitLog && source != TSDB_DATA_SOURCE_LOG) {
    if (pVnode->logFd < 0) return TSDB_CODE_INVALID_COMMIT_LOG;
    code = vnodeWriteToCommitLog(pObj, TSDB_ACTION_IMPORT, cont, contLen, sversion);
    if (code != 0) return code;
  }

  if (*((TSKEY *)(pSubmit->payLoad + (rows - 1) * pObj->bytesPerPoint)) > pObj->lastKey) {
    vnodeClearMeterState(pObj, TSDB_METER_STATE_IMPORTING);
    vnodeSetMeterState(pObj, TSDB_METER_STATE_INSERT);
    code = vnodeInsertPoints(pObj, cont, contLen, TSDB_DATA_SOURCE_LOG, NULL, pObj->sversion, &pointsImported);

    if (pShell) {
      pShell->code = code;
      pShell->numOfTotalPoints += pointsImported;
    }

    vnodeClearMeterState(pObj, TSDB_METER_STATE_INSERT);
  } else {
    SImportInfo *pNew, import;

    dTrace("vid:%d sid:%d id:%s, import %d rows data", pObj->vnode, pObj->sid, pObj->meterId, rows);
    memset(&import, 0, sizeof(import));
    import.firstKey = *((TSKEY *)(payload));
    import.lastKey = *((TSKEY *)(pSubmit->payLoad + (rows - 1) * pObj->bytesPerPoint));
    import.pObj = pObj;
    import.pShell = pShell;
    import.payload = payload;
    import.rows = rows;

    int32_t num = 0;
    pthread_mutex_lock(&pVnode->vmutex);
    num = pObj->numOfQueries;
    pthread_mutex_unlock(&pVnode->vmutex);

    int32_t commitInProcess = 0;

    pthread_mutex_lock(&pPool->vmutex);
    if (((commitInProcess = pPool->commitInProcess) == 1) || num > 0) {
      pthread_mutex_unlock(&pPool->vmutex);

      pNew = (SImportInfo *)malloc(sizeof(SImportInfo));
      memcpy(pNew, &import, sizeof(SImportInfo));
      pNew->signature = pNew;
      int payloadLen = contLen - sizeof(SSubmitMsg);
      pNew->payload = malloc(payloadLen);
      pNew->opayload = pNew->payload;
      memcpy(pNew->payload, payload, payloadLen);

      dTrace("vid:%d sid:%d id:%s, import later, commit in process:%d, numOfQueries:%d", pObj->vnode, pObj->sid,
             pObj->meterId, commitInProcess, pObj->numOfQueries);

      taosTmrStart(vnodeProcessImportTimer, 10, pNew, vnodeTmrCtrl);
      return 0;
    } else {
      pPool->commitInProcess = 1;
      pthread_mutex_unlock(&pPool->vmutex);
      int code = vnodeImportData(pObj, &import);
      if (pShell) {
        pShell->code = code;
        pShell->numOfTotalPoints += import.importedRows;
      }
    }
  }

  pVnode->version++;

  if (pShell) {
    pShell->count--;
    if (pShell->count <= 0) vnodeSendShellSubmitRspMsg(pShell, pShell->code, pShell->numOfTotalPoints);
  }

  return 0;
}

//todo abort from the procedure if the meter is going to be dropped
int vnodeImportData(SMeterObj *pObj, SImportInfo *pImport) {
  int code = 0;

  if (pImport->lastKey > pObj->lastKeyOnFile) {
    code = vnodeImportWholeToCache(pImport, pImport->payload, pImport->rows);
  } else if (pImport->lastKey < pObj->lastKeyOnFile) {
    code = vnodeImportWholeToFile(pImport, pImport->payload, pImport->rows);
  } else {  // lastKey == pObj->lastkeyOnFile
    code = vnodeImportStartToFile(pImport, pImport->payload, pImport->rows);
  }

  SVnodeObj  *pVnode = &vnodeList[pObj->vnode];
  SCachePool *pPool = (SCachePool *)pVnode->pCachePool;
  pPool->commitInProcess = 0;

  if (pImport->commit) vnodeProcessCommitTimer(pVnode, NULL);

  return code;
}
