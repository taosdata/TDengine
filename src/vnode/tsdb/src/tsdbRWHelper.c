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
#include "tsdbMain.h"

#define adjustMem(ptr, size, expectedSize)    \
  do {                                        \
    if ((size) < (expectedSize)) {            \
      (ptr) = realloc((void *)(ptr), (expectedSize)); \
      if ((ptr) == NULL) return -1;           \
      (size) = (expectedSize);                \
    }                                         \
  } while (0)

// Local function definitions
static int  tsdbCheckHelperCfg(SHelperCfg *pCfg);
static void tsdbInitHelperFile(SHelperFile *pHFile);
static int  tsdbInitHelperRead(SRWHelper *pHelper);
static int  tsdbInitHelperWrite(SRWHelper *pHelper);
static void tsdbClearHelperFile(SHelperFile *pHFile);
static void tsdbDestroyHelperRead(SRWHelper *pHelper);
static void tsdbDestroyHelperWrite(SRWHelper *pHelper);
static void tsdbClearHelperRead(SRWHelper *pHelper);
static void tsdbClearHelperWrite(SRWHelper *pHelper);
static bool tsdbShouldCreateNewLast(SRWHelper *pHelper);
static int tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, int rowsToWrite, SCompBlock *pCompBlock,
                                bool isLast);
static int compareKeyBlock(const void *arg1, const void *arg2);
static int tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols);
static int nRowsLEThan(SDataCols *pDataCols, int maxKey);
static int tsdbGetRowsCanBeMergedWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols);

int tsdbInitHelper(SRWHelper *pHelper, SHelperCfg *pCfg) {
  if (pHelper == NULL || pCfg == NULL || tsdbCheckHelperCfg(pCfg) < 0) return -1;

  memset((void *)pHelper, 0, sizeof(*pHelper));

  pHelper->config = *pCfg;

  tsdbInitHelperFile(&(pHelper->files));

  if (tsdbInitHelperRead(pHelper) < 0) goto _err;

  if ((TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) && tsdbInitHelperWrite(pHelper) < 0) goto _err;

  pHelper->state = TSDB_HELPER_CLEAR_STATE;

  return 0;

_err:
  tsdbDestroyHelper(pHelper);
  return -1;
}

void tsdbDestroyHelper(SRWHelper *pHelper) {
  if (pHelper == NULL) return;

  tsdbClearHelperFile(&(pHelper->files));
  tsdbDestroyHelperRead(pHelper);
  tsdbDestroyHelperWrite(pHelper);
}

void tsdbClearHelper(SRWHelper *pHelper) {
  if (pHelper == NULL) return;
  tsdbClearHelperFile(&(pHelper->files));
  tsdbClearHelperRead(pHelper);
  tsdbClearHelperWrite(pHelper);
}

int tsdbSetHelperFile(SRWHelper *pHelper, SFileGroup *pGroup) {
  // TODO: reset the helper object

  pHelper->files.fid = pGroup->fileId;

  pHelper->files.headF = pGroup->files[TSDB_FILE_TYPE_HEAD];
  pHelper->files.dataF = pGroup->files[TSDB_FILE_TYPE_DATA];
  pHelper->files.lastF = pGroup->files[TSDB_FILE_TYPE_LAST];

  if (TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) {
    char *fnameDup = strdup(pHelper->files.headF.fname);
    if (fnameDup == NULL) return -1;
    char *dataDir = dirname(fnameDup);

    tsdbGetFileName(dataDir, pHelper->files.fid, ".h", pHelper->files.nHeadF.fname);
    tsdbGetFileName(dataDir, pHelper->files.fid, ".l", pHelper->files.nLastF.fname);
    free((void *)fnameDup);
  }
  return 0;
}

int  tsdbOpenHelperFile(SRWHelper *pHelper) {
  // TODO: check if the file is set
  {}

  if (TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) {
    if (tsdbOpenFile(&(pHelper->files.headF), O_RDONLY) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDWR) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDWR) < 0) goto _err;
    // TODO: need to write head and compIdx part
    if (tsdbOpenFile(&(pHelper->files.nHeadF), O_WRONLY | O_CREAT) < 0) goto _err;
    if (tsdbShouldCreateNewLast(pHelper)) {
      if (tsdbOpenFile(&(pHelper->files.nLastF), O_WRONLY | O_CREAT) < 0) goto _err;
    }
  } else {
    if (tsdbOpenFile(&(pHelper->files.headF), O_RDONLY) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDONLY) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDONLY) < 0) goto _err;
  }

  return 0;

_err:
  tsdbCloseHelperFile(pHelper, true);
  return -1;
}

int tsdbCloseHelperFile(SRWHelper *pHelper, bool hasError) {
  if (pHelper->files.headF.fd > 0) {
    close(pHelper->files.headF.fd);
    pHelper->files.headF.fd = -1;
  }
  if (pHelper->files.dataF.fd > 0) {
    close(pHelper->files.dataF.fd);
    pHelper->files.dataF.fd = -1;
  }
  if (pHelper->files.lastF.fd > 0) {
    close(pHelper->files.lastF.fd);
    pHelper->files.lastF.fd = -1;
  }
  if (pHelper->files.nHeadF.fd > 0) {
    close(pHelper->files.nHeadF.fd);
    pHelper->files.nHeadF.fd = -1;
    if (hasError) remove(pHelper->files.nHeadF.fname);
  }
  
  if (pHelper->files.nLastF.fd > 0) {
    close(pHelper->files.nLastF.fd);
    pHelper->files.nLastF.fd = -1;
    if (hasError) remove(pHelper->files.nLastF.fname);
  }
  return 0;
}

void tsdbSetHelperTable(SRWHelper *pHelper, SHelperTable *pHelperTable, STSchema *pSchema) {
  // TODO: check if it is available to set the table

  pHelper->tableInfo = *pHelperTable;
  // TODO: Set the pDataCols according to schema

  // TODO: set state
}

int tsdbWriteDataBlock(SRWHelper *pHelper, SDataCols *pDataCols) {
  ASSERT(TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER);
  ASSERT(helperHasState(pHelper, TSDB_HELPER_FILE_SET) && helperHasState(pHelper, TSDB_HELPER_FILE_OPEN));
  SCompBlock compBlock;
  int        rowsToWrite = 0;
  TSKEY      keyFirst = dataColsKeyFirst(pDataCols);

  // Load SCompIdx part if not loaded yet
  if ((!helperHasState(pHelper, TSDB_HELPER_IDX_LOAD)) && (tsdbLoadCompIdx(pHelper, NULL) < 0)) goto _err;

  // Load the SCompInfo part if neccessary
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  if ((pIdx->offset > 0) && (pIdx->hasLast || dataColsKeyFirst(pDataCols) <= pIdx->maxKey)) {
    if (tsdbLoadCompInfo(pHelper, NULL) < 0) goto _err;
  }

  SCompIdx *pWIdx = pHelper->pWCompIdx + pHelper->tableInfo.tid;

  if (!pIdx->hasLast && keyFirst > pIdx->maxKey) {
    // Just need to append as a super block
    rowsToWrite = pDataCols->numOfPoints;
    SFile *pWFile = NULL;
    bool   isLast = false;

    if (rowsToWrite > pHelper->config.minRowsPerFileBlock) {
      pWFile = &(pHelper->files.dataF);
    } else {
      isLast = true;
      pWFile = (pHelper->files.nLastF.fd > 0) ? &(pHelper->files.nLastF) : &(pHelper->files.nLastF);
    }

    if (tsdbWriteBlockToFile(pHelper, pWFile, pDataCols, rowsToWrite, &compBlock, isLast) < 0) goto _err;

    // TODO: may need to reallocate the memory
    pHelper->pCompInfo->blocks[pHelper->blockIter++] = compBlock;

    pIdx->hasLast = compBlock.last;
    pIdx->numOfSuperBlocks++;
    pIdx->maxKey = dataColsKeyLast(pDataCols);
    // pIdx->len = ??????
  } else { // (pIdx->hasLast) OR (keyFirst <= pIdx->maxKey)
    if (keyFirst > pIdx->maxKey) {
      int blkIdx = pIdx->numOfSuperBlocks - 1;
      ASSERT(pIdx->hasLast && pHelper->pCompInfo->blocks[blkIdx].last);

      // Need to merge with the last block
      if (tsdbMergeDataWithBlock(pHelper, blkIdx, pDataCols) < 0) goto _err;
    } else {
      // Find the first block greater or equal to the block
      SCompBlock *pCompBlock = taosbsearch((void *)(&keyFirst), (void *)(pHelper->pCompInfo->blocks),
                                           pIdx->numOfSuperBlocks, sizeof(SCompBlock), compareKeyBlock, TD_GE);
      if (pCompBlock == NULL) {
        if (tsdbMergeDataWithBlock(pHelper, pIdx->numOfSuperBlocks-1, pDataCols) < 0) goto _err;
      } else {
        if (compareKeyBlock((void *)(&keyFirst), (void *)pCompBlock) == 0) {
          SCompBlock *pNextBlock = NULL;
          TSKEY keyLimit = (pNextBlock == NULL) ? INT_MAX : (pNextBlock->keyFirst - 1);
          rowsToWrite =
              MIN(nRowsLEThan(pDataCols, keyLimit), pHelper->config.maxRowsPerFileBlock - pCompBlock->numOfPoints);
          
          if (tsdbMergeDataWithBlock(pHelper, pCompBlock-pHelper->pCompInfo->blocks, pDataCols) < 0) goto _err;
        } else {
          // There options: 1. merge with previous block
          //                2. commit as one block
          //                3. merge with current block
          int nRows1 = INT_MAX;
          int nRows2 = nRowsLEThan(pDataCols, pCompBlock->keyFirst);
          int nRows3 = MIN(nRowsLEThan(pDataCols, (pCompBlock + 1)->keyFirst), (pHelper->config.maxRowsPerFileBlock - pCompBlock->numOfPoints));

          // TODO: find the block with max rows can merge
          if (tsdbMergeDataWithBlock(pHelper, pCompBlock, pDataCols) < 0) goto _err;
        }
      }
    }
  }

  return rowsToWrite;

  _err:
  return -1;
}

int tsdbMoveLastBlockIfNeccessary(SRWHelper *pHelper) {
  // TODO
  return 0;
}

int tsdbWriteCompInfo(SRWHelper *pHelper) {
  // TODO
  return 0;
}

int tsdbWriteCompIdx(SRWHelper *pHelper) {
  // TODO
  return 0;
}

int tsdbLoadCompIdx(SRWHelper *pHelper, void *target) {
  // TODO: check helper state
  ASSERT(!helperHasState(pHelper, TSDB_HELPER_IDX_LOAD));

  int fd = pHelper->files.headF.fd;

  if (lseek(fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) return -1;
  if (tread(fd, pHelper->pCompIdx, pHelper->compIdxSize) < pHelper->compIdxSize) return -1;
  // TODO: check the checksum

  if (target) memcpy(target, pHelper->pCompIdx, pHelper->compIdxSize);
  helperSetState(pHelper, TSDB_HELPER_IDX_LOAD);

  return 0;
}

int tsdbLoadCompInfo(SRWHelper *pHelper, void *target) {
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;

  ASSERT(pIdx->offset > 0);

  int fd = pHelper->files.headF.fd;

  if (lseek(fd, pIdx->offset, SEEK_SET) < 0) return -1;
  ASSERT(pIdx->len > 0);

  adjustMem(pHelper->pCompInfo, pHelper->compInfoSize, pIdx->len);
  if (tread(fd, (void *)(pHelper->pCompInfo), pIdx->len) < 0) return -1;
  // TODO: check the checksum

  // TODO: think about when target has no space for the content
  if (target) memcpy(target, (void *)(pHelper->pCompInfo), pIdx->len);

  helperSetState(pHelper, TSDB_HELPER_INFO_LOAD);

  return 0;
}

int tsdbLoadCompData(SRWHelper *pHelper, int blkIdx, void *target) {
  // TODO
  return 0;
}

int tsdbLoadBlockDataCols(SRWHelper *pHelper, SDataCols *pDataCols, int32_t *colIds, int numOfColIds) {
  // TODO
  return 0;
}

int tsdbLoadBlockData(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  // TODO
  return 0;
}

static int tsdbCheckHelperCfg(SHelperCfg *pCfg) {
  // TODO
  return 0;
}

static void tsdbInitHelperFile(SHelperFile *pHFile) {
  pHFile->fid = -1;
  pHFile->headF.fd = -1;
  pHFile->dataF.fd = -1;
  pHFile->lastF.fd = -1;
  pHFile->nHeadF.fd = -1;
  pHFile->nLastF.fd = -1;
}

static void tsdbClearHelperFile(SHelperFile *pHFile) {
  pHFile->fid = -1;
  if (pHFile->headF.fd > 0) {
    close(pHFile->headF.fd);
    pHFile->headF.fd = -1;
  }
  if (pHFile->dataF.fd > 0) {
    close(pHFile->dataF.fd);
    pHFile->dataF.fd = -1;
  }
  if (pHFile->lastF.fd > 0) {
    close(pHFile->lastF.fd);
    pHFile->lastF.fd = -1;
  }
  if (pHFile->nHeadF.fd > 0) {
    close(pHFile->nHeadF.fd);
    pHFile->nHeadF.fd = -1;
  }
  if (pHFile->nLastF.fd > 0) {
    close(pHFile->nLastF.fd);
    pHFile->nLastF.fd = -1;
  }

}

static int tsdbInitHelperRead(SRWHelper *pHelper) {
  SHelperCfg *pCfg = &(pHelper->config);

  pHelper->compIdxSize = pCfg->maxTables * sizeof(SCompIdx);
  if ((pHelper->pCompIdx = (SCompIdx *)malloc(pHelper->compIdxSize)) == NULL) return -1;

  return 0;
}

static void tsdbDestroyHelperRead(SRWHelper *pHelper) {
  tfree(pHelper->pCompIdx);
  pHelper->compIdxSize = 0;

  tfree(pHelper->pCompInfo);
  pHelper->compInfoSize = 0;

  tfree(pHelper->pCompData);
  pHelper->compDataSize = 0;

  tdFreeDataCols(pHelper->pDataCols[0]);
  tdFreeDataCols(pHelper->pDataCols[1]);
}

static int tsdbInitHelperWrite(SRWHelper *pHelper) {
  SHelperCfg *pCfg = &(pHelper->config);

  pHelper->wCompIdxSize = pCfg->maxTables * sizeof(SCompIdx);
  if ((pHelper->pWCompIdx = (SCompIdx *)malloc(pHelper->wCompIdxSize)) == NULL) return -1;

  return 0;
}

static void tsdbDestroyHelperWrite(SRWHelper *pHelper) {
  tfree(pHelper->pWCompIdx);
  pHelper->wCompIdxSize = 0;

  tfree(pHelper->pWCompInfo);
  pHelper->wCompInfoSize = 0;

  tfree(pHelper->pWCompData);
  pHelper->wCompDataSize = 0;
}

static void tsdbClearHelperRead(SRWHelper *pHelper) {
  // TODO
}

static void tsdbClearHelperWrite(SRWHelper *pHelper) {
  // TODO
}

static bool tsdbShouldCreateNewLast(SRWHelper *pHelper) {
  // TODO
  return 0;
}

static int tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, int rowsToWrite, SCompBlock *pCompBlock,
                                bool isLast) {
  ASSERT(rowsToWrite > 0 && rowsToWrite <= pDataCols->numOfPoints);

  int64_t offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) goto _err;

  SCompData *pCompData = (SCompData *)malloc(sizeof(SCompData) + sizeof(SCompCol) * pDataCols->numOfCols);
  if (pCompData == NULL) goto _err;

  int nColsNotAllNull = 0;
  int32_t toffset;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + nColsNotAllNull;

    if (0) {
      // TODO: all data are NULL
      continue;
    }

    pCompCol->colId = pDataCol->colId;
    pCompCol->type = pDataCol->type;
    pCompCol->len = pDataCol->len;
    pCompCol->offset = toffset;
    nColsNotAllNull++;
    toffset += pCompCol->len;
  }

  pCompData->delimiter = TSDB_FILE_DELIMITER;
  pCompData->uid = pHelper->tableInfo.uid;
  pCompData->numOfCols = nColsNotAllNull;

  size_t tsize = sizeof(SCompData) + sizeof(SCompCol) * nColsNotAllNull;
  if (twrite(pFile->fd, (void *)pCompData, tsize) < tsize) goto _err;
  for (int i = 0; i < pDataCols->numOfCols; i++) {
    SDataCol *pDataCol = pCompData->cols + i;
    SCompCol *pCompCol = NULL;
    if (twrite(pFile->fd, (void *)(pDataCol->pData), pCompCol->len) < pCompCol->len) goto _err;
  }

  pCompBlock->last = isLast;
  pCompBlock->offset = offset;
  // pCOmpBlock->algorithm = ;
  pCompBlock->numOfPoints = rowsToWrite;
  pCompBlock->sversion = pHelper->tableInfo.sversion;
  // pCompBlock->len = ;
  // pCompBlock->numOfSubBlocks = ;
  pCompBlock->numOfCols = nColsNotAllNull;
  // pCompBlock->keyFirst = ;
  // pCompBlock->keyLast = ;

  return 0;

  _err:
  return -1;
}

// static int compareKeyBlock(const void *arg1, const void *arg2);

// /**
//  * Init a read-write helper object for read or write usage.
//  */
// int tsdbInitHelper(SRWHelper *pHelper, int maxTables, tsdb_rwhelper_t type, int maxRowSize, int maxRows,
//                      int maxCols) {
//   if (pHelper == NULL) return -1;

//   memset((void *)pHelper, 0, sizeof(SRWHelper));
//   for (int ftype = TSDB_RW_HEADF; ftype <= TSDB_RW_LF; ftype++) {
//     pHelper->files[ftype] = -1;
//   }

//   // Set type
//   pHelper->type = type;

//   // Set global configuration
//   pHelper->maxTables = maxTables;
//   pHelper->maxRowSize = maxRowSize;
//   pHelper->maxRows = maxRows;
//   pHelper->maxCols = maxCols;

//   // Allocate SCompIdx part memory
//   pHelper->compIdxSize = sizeof(SCompIdx) * maxTables;
//   pHelper->pCompIdx = (SCompIdx *)malloc(pHelper->compIdxSize);
//   if (pHelper->pCompIdx == NULL) goto _err;

//   pHelper->compDataSize = sizeof(SCompData) + sizeof(SCompCol) * maxCols;
//   pHelper->pCompData = (SCompData *)malloc(pHelper->compDataSize);

//   pHelper->pDataCols = tdNewDataCols(maxRowSize, maxCols, maxRows);
//   if (pHelper->pDataCols == NULL) goto _err;

//   return 0;

// _err:
//   tsdbDestroyHelper(pHelper);
//   return -1;
// }

// void tsdbResetHelper(SRWHelper *pHelper) {
//   if (pHelper->headF.fd > 0) {
//     close(pHelper->headF.fd);
//     pHelper->headF.fd = -1;
//   }
//   if (pHelper->dataF.fd > 0) {
//     close(pHelper->dataF.fd);
//     pHelper->dataF.fd = -1;
//   }
//   if (pHelper->lastF.fd > 0) {
//     close(pHelper->lastF.fd);
//     pHelper->lastF.fd = -1;
//   }
//   if (pHelper->hF.fd > 0) {
//     close(pHelper->hF.fd);
//     pHelper->hF.fd = -1;
//   }
//   if (pHelper->lF.fd > 0) {
//     close(pHelper->lF.fd);
//     pHelper->lF.fd = -1;
//   }
  
//   pHelper->state = 0;
//   tdResetDataCols(pHelper->pDataCols);
// }

// int tsdbDestroyHelper(SRWHelper *pHelper) {
//   if (pHelper->headF.fd > 0) close(pHelper->headF.fd);
//   if (pHelper->dataF.fd > 0) close(pHelper->dataF.fd);
//   if (pHelper->lastF.fd > 0) close(pHelper->lastF.fd);
//   if (pHelper->hF.fd > 0) close(pHelper->hF.fd);
//   if (pHelper->lF.fd > 0) close(pHelper->lF.fd);

//   if (pHelper->pCompIdx) free(pHelper->pCompIdx);
//   if (pHelper->pCompInfo) free(pHelper->pCompInfo);
//   if (pHelper->pCompData) free(pHelper->pCompData);
//   memset((void *)pHelper, 0, sizeof(SRWHelper));
//   return 0;
// }

// int tsdbSetHelperFile(SRWHelper *pHelper, SFileGroup *pGroup) {
//   if (pHelper->state != 0) return -1;

//   pHelper->fid = pGroup->fileId;

//   pHelper->headF = pGroup->files[TSDB_FILE_TYPE_HEAD];
//   pHelper->headF.fd = -1;
//   pHelper->dataF = pGroup->files[TSDB_FILE_TYPE_DATA];
//   pHelper->dataF.fd = -1;
//   pHelper->lastF = pGroup->files[TSDB_FILE_TYPE_LAST];
//   pHelper->lastF.fd = -1;

//   if (pHelper->mode == TSDB_WRITE_HELPER) {
//     char *fnameCpy = strdup(pHelper->headF.fname);
//     if (fnameCpy == NULL) return -1;
//     char *dataDir = dirname(fnameCpy);

//     memset((void *)(&pHelper->hF), 0, sizeof(SFile));
//     memset((void *)(&pHelper->lF), 0, sizeof(SFile));
//     pHelper->hF.fd = -1;
//     pHelper->lF.fd = -1;

//     tsdbGetFileName(dataDir, pHelper->fid, ".h", pHelper->hF.fname);
//     tsdbGetFileName(dataDir, pHelper->fid, ".l", pHelper->lF.fname);
//     free((char *)fnameCpy);
//   }

//   TSDB_SET_RWHELPER_STATE(pHelper, TSDB_RWHELPER_FILE_SET);

//   return 0;
// }

// static int tsdbNeedToCreateNewLastFile() {
//   // TODO
//   return 0;
// }

// int tsdbCloseHelperFile(SRWHelper *pHelper, int hasErr) {
//   int ret = 0;
//   if (pHelper->headF.fd > 0) {
//     close(pHelper->headF.fd);
//     pHelper->headF.fd = -1;
//   }
//   if (pHelper->dataF.fd > 0) {
//     close(pHelper->dataF.fd);
//     pHelper->dataF.fd = -1;
//   }
//   if (pHelper->lastF.fd > 0) {
//     close(pHelper->lastF.fd);
//     pHelper->lastF.fd = -1;
//   }
//   if (pHelper->hF.fd > 0) {
//     close(pHelper->hF.fd);
//     pHelper->hF.fd = -1;
//     if (hasErr) remove(pHelper->hF.fname);
//   }
//   if (pHelper->lF.fd > 0) {
//     close(pHelper->lF.fd);
//     pHelper->lF.fd = -1;
//     if (hasErr) remove(pHelper->hF.fname);
//   }
//   return 0;
// }

// int tsdbOpenHelperFile(SRWHelper *pHelper) {
//   if (pHelper->state != TSDB_RWHELPER_FILE_SET) return -1;

//   if (pHelper->mode == TSDB_READ_HELPER) {  // The read helper
//     if (tsdbOpenFile(&pHelper->headF, O_RDONLY) < 0) goto _err;
//     if (tsdbOpenFile(&pHelper->dataF, O_RDONLY) < 0) goto _err;
//     if (tsdbOpenFile(&pHelper->lastF, O_RDONLY) < 0) goto _err;
//   } else {
//     if (tsdbOpenFile(&pHelper->headF, O_RDONLY) < 0) goto _err;
//     if (tsdbOpenFile(&pHelper->dataF, O_RDWR) < 0) goto _err;
//     if (tsdbOpenFile(&pHelper->lastF, O_RDWR) < 0) goto _err;
//     // Open .h and .l file
//     if (tsdbOpenFile(&pHelper->hF, O_WRONLY | O_CREAT) < 0) goto _err;
//     if (tsdbNeedToCreateNewLastFile()) {
//       if (tsdbOpenFile(&pHelper->lF, O_WRONLY | O_CREAT) < 0) goto _err;
//     }
//   }

//   TSDB_SET_RWHELPER_STATE(pHelper, TSDB_RWHELPER_FILE_OPENED);

//   return 0;
// _err:
//   tsdbCloseHelperFile(pHelper, 1);
//   return -1;
// }

// int tsdbLoadCompIdx(SRWHelper *pHelper) {
//   if (pHelper->state != (TSDB_RWHELPER_FILE_SET | TSDB_RWHELPER_FILE_OPENED)) return -1;

//   if (lseek(pHelper->headF.fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) return -1;
//   if (tread(pHelper->headF.fd, (void *)(pHelper->pCompIdx), pHelper->compIdxSize) < pHelper->compIdxSize) return -1;

//   TSDB_SET_RWHELPER_STATE(pHelper, TSDB_RWHELPER_COMPIDX_LOADED);

//   return 0;
// }

// int tsdbSetHelperTable(SRWHelper *pHelper, int32_t tid, int64_t uid, STSchema *pSchema) {
//   // TODO: add some check information
//   pHelper->tid = tid;
//   pHelper->uid = uid;

//   tdInitDataCols(pHelper->pDataCols, pSchema);

//   TSDB_SET_RWHELPER_STATE(pHelper, TSDB_RWHELPER_TABLE_SET);

//   return 0;
// }

// int tsdbLoadCompBlocks(SRWHelper *pHelper) {
//   SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tid;
//   if (pIdx->offset <= 0) return 0;

//   if (lseek(pHelper->headF.fd, pIdx->offset, SEEK_SET) < 0) return -1;
//   if (pHelper->compInfoSize < pIdx->len) {
//     pHelper->pCompInfo = (SCompInfo *)realloc((void *)(pHelper->pCompInfo), pIdx->len);
//     if (pHelper->pCompInfo == NULL) return -1;
//     pHelper->compInfoSize = pIdx->len;
//   }

//   if (tread(pHelper->headF.fd, (void *)(pHelper->pCompInfo), pIdx->len) < pIdx->len) return -1;

//   TSDB_SET_RWHELPER_STATE(pHelper, TSDB_RWHELPER_COMPBLOCK_LOADED);

//   return 0;
// }

// int tsdbRWHelperSetBlockIdx(SRWHelper *pHelper, int blkIdx) {
//   SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tid;
//   if (blkIdx > pIdx->numOfSuperBlocks) return -1;

//   pHelper->blkIdx = blkIdx;

//   TSDB_SET_RWHELPER_STATE(pHelper, TSDB_RWHELPER_BLOCKIDX_SET);

//   return 0;
// }

// int tsdbRWHelperLoadCompData(SRWHelper *pHelper) {
//   SCompBlock *pBlock = pHelper->pCompInfo->blocks + pHelper->blkIdx;

//   if (pBlock->numOfSubBlocks == 1) {  // Only one super block
//     size_t size = sizeof(SCompData) + sizeof(SCompCol) * pBlock->numOfCols;
//     if (size > pHelper->compDataSize) {
//       pHelper->pCompData = (SCompData *)realloc((void *)pHelper->pCompData, size);
//       if (pHelper->pCompData == NULL) return -1;
//       pHelper->compDataSize = size;
//     }

//     if (lseek(pHelper->dataF.fd, pBlock->offset, SEEK_SET) < 0) return -1;
//     if (tread(pHelper->dataF.fd, (void *)(pHelper->pCompData), size) < size) return -1;
//   } else {  // TODO: More sub blocks
//   }

//   TSDB_SET_RWHELPER_STATE(pHelper, TSDB_RWHELPER_COMPCOL_LOADED);

//   return 0;
// }


// static int compColIdCompCol(const void *arg1, const void *arg2) {
//   int       colId = *(int *)arg1;
//   SCompCol *pCompCol = (SCompCol *)arg2;

//   return (int)(colId - pCompCol->colId);
// }

// static int compColIdDataCol(const void *arg1, const void *arg2) {
//   int colId = *(int *)arg1;
//   SDataCol *pDataCol = (SDataCol *)arg2;

//   return (int)(colId - pDataCol->colId);
// }

// int tsdbRWHelperLoadColData(SRWHelper *pHelper, int colId) {
//   SCompBlock *pBlock = pHelper->pCompInfo->blocks + pHelper->blkIdx;

//   if (pBlock->numOfSubBlocks == 1) {  // only one super block
//     SCompCol *pCompCol = bsearch((void *)(&colId), (void *)(pHelper->pCompData->cols), pBlock->numOfCols, compColIdCompCol, compColIdCompCol);
//     if (pCompCol == NULL) return 0;  // No data to read from this block , but we still return 0

//     SDataCol *pDataCol = bsearch((void *)(&colId), (void *)(pHelper->pDataCols->cols), pHelper->pDataCols->numOfCols, sizeof(SDataCol), compColIdDataCol);
//     assert(pDataCol != NULL);

//     int fd = (pBlock->last) ? pHelper->lastF.fd : pHelper->dataF.fd;
//     if (lseek(fd, pBlock->offset + pCompCol->offset, SEEK_SET) < 0) return -1;
//     if (tread(fd, (void *)pDataCol->pData, pCompCol->len) < pCompCol->len) return -1;
//     pDataCol->len = pCompCol->len;
//   } else {
//     // TODO: more than 1 blocks
//   }
//   return 0;
// }

// int tsdbRWHelperLoadBlockData(SRWHelper *pHelper, int blkIdx) {
//   SCompBlock *pBlock = pHelper->pCompInfo->blocks + pHelper->blkIdx;

//   if (pBlock->numOfSubBlocks == 1) {
//     for (int i = 0; i < pHelper->pDataCols->numOfCols; i++) {
//       if (tsdbRWHelperLoadBlockData(pHelper, pHelper->pDataCols->cols[i].colId) < 0) return -1;
//     }
//   } else {
//     // TODO: more than 1 block of data
//   }
//   return 0;
// }

// int tsdbRWHelperCopyCompBlockPart(SRWHelper *pHelper) {
//   // TODO
//   return 0;
// }

// int tsdbRWHelperCopyDataBlockPart(SRWHelper *pHelper ) {
//   // TODO
//   return 0;
// }

// int tsdbRWHelperWriteCompIdx(SRWHelper *pHelper) {
//   // TODO
//   if (lseek(pHelper->hF.fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) return -1;
//   if (twrite(pHelper->hF.fd, (void *)(pHelper->pCompIdx), pHelper->compIdxSize) < pHelper->compIdxSize) return -1;

//   return 0;
// }

// /**
//  * Load the data block from file
//  *
//  * @return  0 for success
//  *         -1 for failure
//  */
// int tsdbLoadDataBlock(SRWHelper *pHelper, int bldIdx) {
//   SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tid;
//   if (pIdx->)
  
//   return 0;
// }

// /**
//  * Append the block to a file, either .data
//  */
// int tsdbAppendBlockToFile(SRWHelper *pHelper, tsdb_rw_file_t toFile, SDataCols *pDataCols, SCompBlock *pCompBlock, bool isSuper) {
//   SFile *pFile = pHelper->files + toFile;

//   int64_t offset = lseek(pFile->fd, 0, SEEK_END);
//   if (*offset < 0) return -1;

//   SCompData *pCompData = (SCompData *)malloc(sizeof(SCompData) + sizeof(SCompCol) * pDataCols->numOfCols);
//   if (pCompData == NULL) return -1;


//   int numOfNotAllNullCols = 0;
//   int32_t toffset = 0;
//   for (int i = 0; i < pDataCols->numOfCols; i++) {
//     SDataCol *pDataCol = pDataCols->cols + i;
//     SCompCol *pCompCol = pCompData->cols + numOfNotAllNullCols;

//     if (0 /* All data in this column are NULL value */) {
//       continue;
//     }
//     pCompCol->colId = pDataCol->colId;
//     pCompCol->type = pDataCol->type;
//     pCompCol->len = pDataCol->len;
//     // pCompCol->offset = toffset;
//     numOfNotAllNullCols++;
//     // toffset += pDataCol->len;
//   }

//   pCompData->delimiter = TSDB_FILE_DELIMITER;
//   pCompData->numOfCols = numOfNotAllNullCols;
//   pCompData->uid = pHelper->uid;
  
//   size_t tsize = sizeof(SCompData) + sizeof(SCompCol) * numOfNotAllNullCols;
//   if (twrite(pFile->fd, (void *)pCompData, tsize) < 0) return -1;
//   for (int i = 0; i < numOfNotAllNullCols; i++) {
//     SCompCol *pCompCol = pCompData->cols + i;
//     SDataCol *pDataCol = NULL; // bsearch()
//     tassert(pDataCol != NULL);
//     if (twrite(pFile->fd, (void *)(pDataCol->pData), pDataCol->len) < pDataCol->len) return -1;
//   }

//   pCompBlock->last = (toFile == TSDB_RW_DATAF) ? 0 : 1;
//   pCompBlock->offset = offset;
//   pCompBlock->algorithm = pHelper->compression;
//   pCompBlock->numOfPoints = pDataCols->numOfPoints;
//   pCompBlock->sversion = pHelper->sversion;
//   // pCompBlock->len = ;
//   pCompBlock->numOfSubBlocks = isSuper ? 1 : 0;
//   pCompBlock->numOfCols = numOfNotAllNullCols;
//   pCompBlock->keyFirst = dataColsKeyFirst(pDataCols);
//   pCompBlock->keyLast = dataColsKeyLast(pDataCols);

//   return 0;
// }

// /**
//  * Write the whole or part of the cached data block to file.
//  *
//  * There are four options:
//  * 1. Append the whole block as a SUPER-BLOCK at the end
//  * 2. Append part/whole block as a SUPER-BLOCK and insert in the middle
//  * 3. Append part/whole block as a SUB-BLOCK
//  * 4. Merge part/whole block as a SUPER-BLOCK
//  */
// int tsdbWriteDataBlock(SRWHelper *pHelper, SDataCols *pDataCols) {
//   tassert(pHelper->type == TSDB_WRITE_HELPER);

//   int        rowsWritten = 0;
//   SCompBlock compBlock;

//   SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tid;
//   // if ((no old data) OR (no last block AND cached first key is larger than the max key))
//   if ((pIdx->offset == 0) || (pIdx->hasLast && dataColsKeyFirst(pDataCols) > pIdx->maxKey)) {
//     // Append the whole block as a SUPER-BLOCK at the end
//     if (pDataCols->numOfPoints >= pHelper->minRowPerFileBlock) {
//       if (tsdbAppendBlockToFile(pHelper, TSDB_RW_DATAF, pDataCols, &compBlock, true) < 0) goto _err;
//     } else {
//       tsdb_rw_file_t ftype = (pHelper->files[TSDB_RW_LF].fd > 0) ? TSDB_RW_LF : TSDB_RW_LASTF;
//       if (tsdbAppendBlockToFile(pHelper, ftype, pDataCols, &compBlock, true) < 0) goto _err;
//     }
//     // Copy the compBlock part to the end
//     if (IS_COMPBLOCK_LOADED(pHelper)) {

//     } else {

//     }

//     pIdx->hasLast = compBlock.last;
//     pIdx->len += sizeof(compBlock);
//     pIdx->numOfSuperBlocks++;
//     pIdx->maxKey = compBlock.keyLast;

//     rowsWritten = pDataCols->numOfPoints;
//   } else {
//     // Need to find a block to merge with
//     int blkIdx = 0;
//     // if (has last block AND cached Key is larger than the max Key)
//     if (pIdx->hasLast && dataColsKeyFirst(pDataCols) > pIdx->maxKey) {
//       blkIdx = pIdx->numOfSuperBlocks - 1;
//       rowsWritten = tsdbMergeDataWithBlock(pHelper, pDataCols, blkIdx);
//       if (rowsWritten < 0) goto _err;
//     } else {
//       ASSERT(IS_COMPBLOCK_LOADED(pHelper));
//       // SCompBlock *pMergeBlock = taosbsearch();
//     }
//   }

//   return numOfPointsWritten;

// _err:
//   return -1;
// }

static int compareKeyBlock(const void *arg1, const void *arg2) {
  TSKEY       key = *(TSKEY *)arg1;
  SCompBlock *pBlock = (SCompBlock *)arg2;

  if (key < pBlock->keyFirst) {
    return -1;
  } else if (key > pBlock->keyLast) {
    return 1;
  }

  return 0;
}

static int nRowsLEThan(SDataCols *pDataCols, int maxKey) {
  return 0;
}

static int tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  int        rowsWritten = 0;
  TSKEY      keyFirst = dataColsKeyFirst(pDataCols);
  SCompBlock compBlock = {0};

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  ASSERT(blkIdx < pIdx->numOfSuperBlocks);

  SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(pCompBlock->numOfSubBlocks >= 1);

  int rowsCanMerge = tsdbGetRowsCanBeMergedWithBlock(pHelper, blkIdx, pDataCols);
  if (rowsCanMerge < 0) goto _err;

  ASSERT(rowsCanMerge > 0);

  if (pCompBlock->numOfSubBlocks <= TSDB_MAX_SUBBLOCKS &&
      ((!pCompBlock->last) || (pHelper->files.nLastF.fd < 0 &&
                               pCompBlock->numOfPoints + rowsCanMerge < pHelper->config.minRowsPerFileBlock))) {

    SFile *pFile = NULL;

    if (!pCompBlock->last) {
      pFile = &(pHelper->files.dataF);
    } else {
      pFile = &(pHelper->files.lastF);
    }

    if (tsdbWriteBlockToFile(pHelper, pFile, pDataCols, rowsCanMerge, &compBlock, pCompBlock->last) < 0) goto _err;

    // TODO: Add the sub-block
    if (pCompBlock->numOfSubBlocks == 1) {
      pCompBlock->numOfSubBlocks += 2;
      // pCompBlock->offset = ;
      // pCompBlock->len = ;
    } else {
      pCompBlock->numOfSubBlocks++;
    }
    pCompBlock->numOfPoints += rowsCanMerge;
    pCompBlock->keyFirst = MIN(pCompBlock->keyFirst, dataColsKeyFirst(pDataCols));
    pCompBlock->keyLast = MAX(pCompBlock->keyLast, dataColsKeyAt(pDataCols, rowsCanMerge - 1));

    // Update the Idx
    // pIdx->hasLast = ;
    // pIdx->len =;
    // pIdx->numOfSuperBlocks = ;

    rowsWritten = rowsCanMerge;
  } else {
    // Read-Merge-Write as a super block
    if (tsdbLoadBlockData(pHelper, blkIdx, NULL) < 0) goto _err;
    tdMergeDataCols(pHelper->pDataCols[0], pDataCols, rowsCanMerge);

    int isLast = 0;
    SFile *pFile = NULL;
    if (!pCompBlock->last || (pCompBlock->numOfPoints + rowsCanMerge >= pHelper->config.minRowsPerFileBlock)) {
      pFile = &(pHelper->files.dataF);
    } else {
      isLast = 1;
      if (pHelper->files.nLastF.fd > 0) {
        pFile = &(pHelper->files.nLastF);
      } else {
        pFile = &(pHelper->files.lastF);
      }
    }

    if (tsdbWriteBlockToFile(pHelper, pFile, pHelper->pDataCols[0], pCompBlock->numOfPoints + rowsCanMerge, &compBlock, isLast) < 0) goto _err;

    *pCompBlock = compBlock;

    pIdx->maxKey = MAX(pIdx->maxKey, compBlock.keyLast);
    // pIdx->hasLast = ;
    // pIdx->
  }

  return rowsWritten;

  _err:
  return -1;
}

static int compTSKEY(const void *key1, const void *key2) { return ((TSKEY *)key1 - (TSKEY *)key2); }

// Get the number of rows the data can be merged into the block
static int tsdbGetRowsCanBeMergedWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  int   rowsCanMerge = 0;
  TSKEY keyFirst = dataColsKeyFirst(pDataCols);

  SCompIdx *  pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  ASSERT(blkIdx < pIdx->numOfSuperBlocks);

  TSKEY keyMax = (blkIdx < pIdx->numOfSuperBlocks + 1) ? (pCompBlock + 1)->keyFirst - 1 : pHelper->files.maxKey;

  if (keyFirst > pCompBlock->keyLast) {
    void *ptr = taosbsearch((void *)(&keyMax), pDataCols->cols[0].pData, pDataCols->numOfPoints, sizeof(TSKEY),
                            compTSKEY, TD_LE);
    ASSERT(ptr != NULL);

    rowsCanMerge =
        MIN((TSKEY *)ptr - (TSKEY *)pDataCols->cols[0].pData, pHelper->config.minRowsPerFileBlock - pCompBlock->numOfPoints);

  } else {
    int32_t colId[1] = {0};
    if (tsdbLoadBlockDataCols(pHelper, NULL, &colId, 1) < 0) goto _err;

    int iter1 = 0;  // For pDataCols
    int iter2 = 0;  // For loaded data cols

    while (1) {
      if (iter1 >= pDataCols->numOfPoints || iter2 >= pHelper->pDataCols[0]->numOfPoints) break;
      if (pCompBlock->numOfPoints + rowsCanMerge >= pHelper->config.maxRowsPerFileBlock) break;

      TSKEY key1 = dataColsKeyAt(pDataCols, iter1);
      TSKEY key2 = dataColsKeyAt(pHelper->pDataCols[0], iter2);

      if (key1 > keyMax) break;

      if (key1 < key2) {
        iter1++;
      } else if (key1 == key2) {
        iter1++;
        iter2++;
      } else {
        iter2++;
        rowsCanMerge++;
      }
    }
  }

  return rowsCanMerge;

_err:
  return -1;
}