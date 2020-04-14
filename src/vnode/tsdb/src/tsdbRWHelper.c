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
// static int  tsdbInitHelperWrite(SRWHelper *pHelper);
static void tsdbClearHelperFile(SHelperFile *pHFile);
static void tsdbDestroyHelperRead(SRWHelper *pHelper);
static void tsdbDestroyHelperWrite(SRWHelper *pHelper);
static void tsdbClearHelperRead(SRWHelper *pHelper);
static void tsdbClearHelperWrite(SRWHelper *pHelper);
static bool tsdbShouldCreateNewLast(SRWHelper *pHelper);
static int tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, int rowsToWrite, SCompBlock *pCompBlock,
                                bool isLast, bool isSuperBlock);
static int compareKeyBlock(const void *arg1, const void *arg2);
static int tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols);
static int nRowsLEThan(SDataCols *pDataCols, int maxKey);
static int tsdbGetRowsCanBeMergedWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols);
static int tsdbInsertSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx);
static int tsdbAddSubBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx, int rowsAdded);
static int tsdbUpdateSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx);
static int tsdbGetRowsInRange(SDataCols *pDataCols, int minKey, int maxKey);

int tsdbInitHelper(SRWHelper *pHelper, SHelperCfg *pCfg) {
  if (pHelper == NULL || pCfg == NULL || tsdbCheckHelperCfg(pCfg) < 0) return -1;

  memset((void *)pHelper, 0, sizeof(*pHelper));

  pHelper->config = *pCfg;

  tsdbInitHelperFile(&(pHelper->files));

  if (tsdbInitHelperRead(pHelper) < 0) goto _err;

  pHelper->pDataCols[0] = tdNewDataCols(pCfg->maxRowSize, pCfg->maxCols, pCfg->maxRows);
  pHelper->pDataCols[1] = tdNewDataCols(pCfg->maxRowSize, pCfg->maxCols, pCfg->maxRows);
  
  if ((pHelper->pDataCols[0] == NULL) || (pHelper->pDataCols[1] == NULL)) goto _err;

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

int tsdbSetAndOpenHelperFile(SRWHelper *pHelper, SFileGroup *pGroup) {
  ASSERT(pHelper != NULL && pGroup != NULL);

  // Clear the helper object
  tsdbClearHelper(pHelper);

  ASSERT(pHelper->state == TSDB_HELPER_CLEAR_STATE);

  // Set the files
  pHelper->files.fid = pGroup->fileId;
  pHelper->files.headF = pGroup->files[TSDB_FILE_TYPE_HEAD];
  pHelper->files.dataF = pGroup->files[TSDB_FILE_TYPE_DATA];
  pHelper->files.lastF = pGroup->files[TSDB_FILE_TYPE_LAST];
  if (TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) {
    char *fnameDup = strdup(pHelper->files.headF.fname);
    if (fnameDup == NULL) goto _err;
    if (fnameDup == NULL) return -1;
    char *dataDir = dirname(fnameDup);

    tsdbGetFileName(dataDir, pHelper->files.fid, ".h", pHelper->files.nHeadF.fname);
    tsdbGetFileName(dataDir, pHelper->files.fid, ".l", pHelper->files.nLastF.fname);
    free((void *)fnameDup);
  }

  // Open the files
  if (tsdbOpenFile(&(pHelper->files.headF), O_RDONLY) < 0) goto _err;
  if (TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER) {
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDWR) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDWR) < 0) goto _err;

    // Create and open .h
    if (tsdbOpenFile(&(pHelper->files.nHeadF), O_WRONLY | O_CREAT) < 0) goto _err;
    size_t tsize = TSDB_FILE_HEAD_SIZE + sizeof(SCompIdx) * pHelper->config.maxTables;
    if (tsendfile(pHelper->files.nHeadF.fd, pHelper->files.headF.fd, NULL, tsize) < tsize) goto _err;

    // Create and open .l file if should
    if (tsdbShouldCreateNewLast(pHelper)) {
      if (tsdbOpenFile(&(pHelper->files.nLastF), O_WRONLY | O_CREAT) < 0) goto _err;
      if (tsendfile(pHelper->files.nLastF.fd, pHelper->files.lastF.fd, NULL, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) goto _err;
    }
  } else {
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDONLY) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDONLY) < 0) goto _err;
  }

  helperSetState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN);

  return tsdbLoadCompIdx(pHelper, NULL);

  _err:
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
  ASSERT(helperHasState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN));

  // Clear members and state used by previous table
  pHelper->blockIter = 0;
  pHelper->state &= (TSDB_HELPER_TABLE_SET - 1);

  pHelper->tableInfo = *pHelperTable;
  tdInitDataCols(pHelper->pDataCols[0], pSchema);
  tdInitDataCols(pHelper->pDataCols[1], pSchema);

  SCompIdx *pIdx = pHelper->pCompIdx + pHelperTable->tid;
  if (pIdx->offset > 0 && pIdx->hasLast) {
    pHelper->hasOldLastBlock = true;
  }

  // pHelper->compIdx = pHelper->pCompIdx[pHelper->tableInfo.tid];

  helperSetState(pHelper, TSDB_HELPER_TABLE_SET);
}

/**
 * Write part of of points from pDataCols to file
 * 
 * @return: number of points written to file successfully
 *          -1 for failure
 */
int tsdbWriteDataBlock(SRWHelper *pHelper, SDataCols *pDataCols) {
  ASSERT(TSDB_HELPER_TYPE(pHelper) == TSDB_WRITE_HELPER);
  ASSERT(pDataCols->numOfPoints > 0);

  SCompBlock compBlock;
  int        rowsToWrite = 0;
  TSKEY      keyFirst = dataColsKeyFirst(pDataCols);

  ASSERT(helperHasState(pHelper, TSDB_HELPER_IDX_LOAD));
  // SCompIdx  curIdx = pHelper->compIdx;                          // old table SCompIdx for sendfile usage
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;  // for change purpose

  // Load the SCompInfo part if neccessary
  ASSERT(helperHasState(pHelper, TSDB_HELPER_TABLE_SET));
  if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD) && (pIdx->offset > 0)) {
    if (tsdbLoadCompInfo(pHelper, NULL) < 0) goto _err;
  }

  if (pIdx->offset == 0 || (!pIdx->hasLast && keyFirst > pIdx->maxKey)) {  // Just append as a super block
    ASSERT(pHelper->hasOldLastBlock == false);
    rowsToWrite = pDataCols->numOfPoints;
    SFile *pWFile = NULL;
    bool   isLast = false;

    if (rowsToWrite > pHelper->config.minRowsPerFileBlock) {
      pWFile = &(pHelper->files.dataF);
    } else {
      isLast = true;
      pWFile = (pHelper->files.nLastF.fd > 0) ? &(pHelper->files.nLastF) : &(pHelper->files.lastF);
    }

    if (tsdbWriteBlockToFile(pHelper, pWFile, pDataCols, rowsToWrite, &compBlock, isLast, true) < 0) goto _err;

    if (tsdbInsertSuperBlock(pHelper, &compBlock, pIdx->numOfSuperBlocks) < 0) goto _err;
  } else {  // (Has old data) AND ((has last block) OR (key overlap)), need to merge the block
    SCompBlock *pCompBlock = taosbsearch((void *)(&keyFirst), (void *)(pHelper->pCompInfo->blocks),
                                         pIdx->numOfSuperBlocks, sizeof(SCompBlock), compareKeyBlock, TD_GE);

    int blkIdx = (pCompBlock == NULL) ? (pIdx->numOfSuperBlocks - 1) : (pCompBlock - pHelper->pCompInfo->blocks);

    if (pCompBlock == NULL) {  // No key overlap, must has last block, just merge with the last block
      ASSERT(pIdx->hasLast && pHelper->pCompInfo->blocks[pIdx->numOfSuperBlocks - 1].last);
      rowsToWrite = tsdbMergeDataWithBlock(pHelper, blkIdx, pDataCols);
      if (rowsToWrite < 0) goto _err;
    } else {  // Has key overlap

      if (compareKeyBlock((void *)(&keyFirst), (void *)pCompBlock) == 0) {
        // Key overlap with the block, must merge with the block

        rowsToWrite = tsdbMergeDataWithBlock(pHelper, blkIdx, pDataCols);
        if (rowsToWrite < 0) goto _err;
      } else { // Save as a super block in the middle
        rowsToWrite = tsdbGetRowsInRange(pDataCols, 0, pCompBlock->keyFirst-1);
        ASSERT(rowsToWrite > 0);
        if (tsdbWriteBlockToFile(pHelper, &(pHelper->files.dataF), pDataCols, rowsToWrite, &compBlock, false, true) < 0) goto _err;
        if (tsdbInsertSuperBlock(pHelper, pCompBlock, pCompBlock - pHelper->pCompInfo->blocks) < 0) goto _err;
      }
    }
  }

  return rowsToWrite;

_err:
  return -1;
}

int tsdbMoveLastBlockIfNeccessary(SRWHelper *pHelper) {
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  SCompBlock compBlock;
  if ((pHelper->files.nHeadF.fd > 0) && (pHelper->hasOldLastBlock)) {
    if (tsdbLoadCompInfo(pHelper, NULL) < 0) return -1;

    SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + pIdx->numOfSuperBlocks - 1;
    ASSERT(pCompBlock->last);

    if (pCompBlock->numOfSubBlocks > 1) {
      if (tsdbLoadBlockData(pHelper, pIdx->numOfSuperBlocks - 1, NULL) < 0) return -1;
      if (tsdbWriteBlockToFile(pHelper, &(pHelper->files.nLastF), pHelper->pDataCols[0],
                               pHelper->pDataCols[0]->numOfPoints, &compBlock, true, true) < 0)
        return -1;

      if (tsdbUpdateSuperBlock(pHelper, &compBlock, pIdx->numOfSuperBlocks - 1) < 0) return -1;

    } else {
      if (lseek(pHelper->files.lastF.fd, pCompBlock->offset, SEEK_SET) < 0) return -1;
      pCompBlock->offset = lseek(pHelper->files.nLastF.fd, 0, SEEK_END);
      if (pCompBlock->offset < 0) return -1;

      if (tsendfile(pHelper->files.nLastF.fd, pHelper->files.lastF.fd, NULL, pCompBlock->len) < pCompBlock->len)
        return -1;
    }

    pHelper->hasOldLastBlock = false;
  }

  return 0;
}

int tsdbWriteCompInfo(SRWHelper *pHelper) {
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) {
    if (pIdx->offset > 0) {
      pIdx->offset = lseek(pHelper->files.nHeadF.fd, 0, SEEK_END);
      if (pIdx->offset < 0) return -1;

      if (tsendfile(pHelper->files.nHeadF.fd, pHelper->files.headF.fd, NULL, pIdx->len) < pIdx->len) return -1;
    }
  } else {
    pIdx->offset = lseek(pHelper->files.nHeadF.fd, 0, SEEK_END);
    if (pIdx->offset < 0) return -1;

    if (twrite(pHelper->files.nHeadF.fd, (void *)(pHelper->pCompInfo), pIdx->len) < pIdx->len) return -1;
  }

  return 0;
}

int tsdbWriteCompIdx(SRWHelper *pHelper) {
  if (lseek(pHelper->files.nHeadF.fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) return -1;

  if (twrite(pHelper->files.nHeadF.fd, (void *)pHelper->pCompIdx, pHelper->compIdxSize) < pHelper->compIdxSize)
    return -1;
  return 0;
}

int tsdbLoadCompIdx(SRWHelper *pHelper, void *target) {
  ASSERT(pHelper->state == TSDB_HELPER_FILE_SET_AND_OPEN);

  if (!helperHasState(pHelper, TSDB_HELPER_IDX_LOAD)) {
    // If not load from file, just load it in object
    int fd = pHelper->files.headF.fd;

    if (lseek(fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) return -1;
    if (tread(fd, (void *)(pHelper->pCompIdx), pHelper->compIdxSize) < pHelper->compIdxSize) return -1;
    // TODO: check the correctness of the part
  }
  helperSetState(pHelper, TSDB_HELPER_IDX_LOAD);

  // Copy the memory for outside usage
  if (target) memcpy(target, pHelper->pCompIdx, pHelper->compIdxSize);

  return 0;
}

int tsdbLoadCompInfo(SRWHelper *pHelper, void *target) {
  ASSERT(helperHasState(pHelper, TSDB_HELPER_TABLE_SET));

  SCompIdx curCompIdx = pHelper->compIdx;

  ASSERT(curCompIdx.offset > 0 && curCompIdx.len > 0);

  int fd = pHelper->files.headF.fd;

  if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) {
    if (lseek(fd, curCompIdx.offset, SEEK_SET) < 0) return -1;

    adjustMem(pHelper->pCompInfo, pHelper->compInfoSize, curCompIdx.len);
    if (tread(fd, (void *)(pHelper->pCompInfo), pHelper->compIdx.len) < pHelper->compIdx.len) return -1;
    // TODO: check the checksum

    helperSetState(pHelper, TSDB_HELPER_INFO_LOAD);
  }

  if (target) memcpy(target, (void *)(pHelper->pCompInfo), curCompIdx.len);

  return 0;
}

int tsdbLoadCompData(SRWHelper *pHelper, SCompBlock *pCompBlock, void *target) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);
  int fd = (pCompBlock->last) ? pHelper->files.lastF.fd : pHelper->files.dataF.fd;

  if (lseek(fd, pCompBlock->offset, SEEK_SET) < 0) return -1;

  size_t tsize = sizeof(SCompData) + sizeof(SCompCol) * pCompBlock->numOfCols;
  adjustMem(pHelper->pCompData, pHelper->compDataSize, tsize);

  if (tread(fd, (void *)pHelper->pCompData, tsize) < tsize) return -1;

  ASSERT(pCompBlock->numOfCols == pHelper->pCompData->numOfCols);

  if (target) memcpy(target, pHelper->pCompData, tsize);

  return 0;
}

static int comparColIdCompCol(const void *arg1, const void *arg2) {
  return (*(int16_t *)arg1) - ((SCompCol *)arg2)->colId;
}

static int comparColIdDataCol(const void *arg1, const void *arg2) {
  return (*(int16_t *)arg1) - ((SDataCol *)arg2)->colId;
}

static int tsdbLoadSingleColumnData(int fd, SCompBlock *pCompBlock, SCompCol *pCompCol, void *buf) {
  size_t tsize = sizeof(SCompData) + sizeof(SCompCol) * pCompBlock->numOfCols;
  if (lseek(fd, pCompBlock->offset + tsize + pCompCol->offset, SEEK_SET) < 0) return -1;
  if (tread(fd, buf, pCompCol->len) < pCompCol->len) return -1;

  return 0;
}

static int tsdbLoadSingleBlockDataCols(SRWHelper *pHelper, SCompBlock *pCompBlock, int16_t *colIds, int numOfColIds,
                                       SDataCols *pDataCols) {
  if (tsdbLoadCompData(pHelper, pCompBlock, NULL) < 0) return -1;
  size_t tsize = sizeof(SCompData) + sizeof(SCompCol) * pCompBlock->numOfCols;
  int fd = (pCompBlock->last) ? pHelper->files.lastF.fd : pHelper->files.dataF.fd;

  void *ptr = NULL;
  for (int i = 0; i < numOfColIds; i++) {
    int16_t colId = colIds[i];

    ptr = bsearch((void *)&colId, (void *)pHelper->pCompData->cols, pHelper->pCompData->numOfCols, sizeof(SCompCol), comparColIdCompCol);
    if (ptr == NULL) continue;
    SCompCol *pCompCol = (SCompCol *)ptr;

    ptr = bsearch((void *)&colId, (void *)(pDataCols->cols), pDataCols->numOfCols, sizeof(SDataCol), comparColIdDataCol);
    ASSERT(ptr != NULL);
    SDataCol *pDataCol = (SDataCol *)ptr;

    pDataCol->len = pCompCol->len;
    if (tsdbLoadSingleColumnData(fd, pCompBlock, pCompCol, pDataCol->pData) < 0) return -1;
  }

  return 0;
}

// Load specific column data from file
int tsdbLoadBlockDataCols(SRWHelper *pHelper, SDataCols *pDataCols, int blkIdx, int16_t *colIds, int numOfColIds) {
  SCompIdx *  pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  ASSERT(pCompBlock->numOfSubBlocks >= 1); // Must be super block

  int numOfSubBlocks = pCompBlock->numOfSubBlocks;
  SCompBlock *pStartBlock =
      (numOfSubBlocks == 1) ? pCompBlock : (SCompBlock *)((char *)pHelper->pCompInfo->blocks + pCompBlock->offset);

  if (tsdbLoadSingleBlockDataCols(pHelper, pStartBlock, colIds, numOfColIds, pDataCols) < 0) return -1;
  for (int i = 1; i < numOfSubBlocks; i++) {
    pStartBlock++;
    if (tsdbLoadSingleBlockDataCols(pHelper, pStartBlock, colIds, numOfColIds, pHelper->pDataCols[1]) < 0) return -1;
    tdMergeDataCols(pDataCols, pHelper->pDataCols[1], pHelper->pDataCols[1]->numOfPoints);
  }

  return 0;
}

/**
 * Interface to read the data of a sub-block OR the data of a super-block of which (numOfSubBlocks == 1)
 */
static int tsdbLoadBlockDataImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);

  SCompData *pCompData = (SCompData *)malloc(pCompBlock->len);
  if (pCompData == NULL) return -1;

  int fd = (pCompBlock->last) ? pHelper->files.lastF.fd : pHelper->files.dataF.fd;
  if (tread(fd, (void *)pCompData, pCompBlock->len) < pCompBlock->len) goto _err;

  { // TODO : check the correctness of the part 
  }

  ASSERT(pCompBlock->numOfCols == pCompData->numOfCols);

  pDataCols->numOfPoints = pCompBlock->numOfPoints;

  size_t tlen = sizeof(SCompData) + sizeof(SCompCol) * pCompBlock->numOfCols;
  int    ccol = 0, dcol = 0;
  while (true) {
    if (ccol >= pDataCols->numOfCols) {
      // TODO: Fill rest NULL
      break;
    }
    if (dcol >= pCompData->numOfCols) break;

    SCompCol *pCompCol = &(pCompData->cols[ccol]);
    SDataCol *pDataCol = &(pDataCols->cols[dcol]);

    if (pCompCol->colId == pDataCol->colId) {
      // TODO: uncompress
      memcpy(pDataCol->pData, (void *)(((char *)pCompData) + tlen + pCompCol->offset), pCompCol->len);
      ccol++;
      dcol++;
    } else if (pCompCol->colId > pDataCol->colId) {
      // TODO: Fill NULL
      dcol++;
    } else {
      ccol++;
    }
  }

  tfree(pCompData);
  return 0;

_err:
  tfree(pCompData);
  return -1;
}

// Load the whole block data
int tsdbLoadBlockData(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  int numOfSubBlock = pCompBlock->numOfSubBlocks;
  if (numOfSubBlock > 1) pCompBlock = (SCompBlock *)((char *)pHelper->pCompInfo + pCompBlock->offset);

  if (tsdbLoadBlockDataImpl(pHelper, pCompBlock, pHelper->pDataCols[0]) < 0) goto _err;
  for (int i = 1; i < numOfSubBlock; i++) {
    pCompBlock++;
    if (tsdbLoadBlockDataImpl(pHelper, pCompBlock, pHelper->pDataCols[1]) < 0) goto _err;
    if (tdMergeDataCols(pHelper->pDataCols[0], pHelper->pDataCols[1], pHelper->pDataCols[1]->numOfPoints) < 0) goto _err;
  }

  return 0;

_err:
  return -1;
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

// static int tsdbInitHelperWrite(SRWHelper *pHelper) {
//   SHelperCfg *pCfg = &(pHelper->config);

//   // pHelper->wCompIdxSize = pCfg->maxTables * sizeof(SCompIdx);
//   // if ((pHelper->pWCompIdx = (SCompIdx *)malloc(pHelper->wCompIdxSize)) == NULL) return -1;

//   return 0;
// }

static void tsdbDestroyHelperWrite(SRWHelper *pHelper) {
  // tfree(pHelper->pWCompIdx);
  // pHelper->wCompIdxSize = 0;

  // tfree(pHelper->pWCompInfo);
  // pHelper->wCompInfoSize = 0;

  // tfree(pHelper->pWCompData);
  // pHelper->wCompDataSize = 0;
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
                                bool isLast, bool isSuperBlock) {
  ASSERT(rowsToWrite > 0 && rowsToWrite <= pDataCols->numOfPoints &&
         rowsToWrite <= pHelper->config.maxRowsPerFileBlock);

  SCompData *pCompData = NULL;
  int64_t offset = 0;

  offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) goto _err;

  pCompData = (SCompData *)malloc(sizeof(SCompData) + sizeof(SCompCol) * pDataCols->numOfCols);
  if (pCompData == NULL) goto _err;

  int nColsNotAllNull = 0;
  int32_t toffset = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + nColsNotAllNull;

    if (0) {
      // TODO: all data are NULL
      continue;
    }

    // Compress the data here
    {}

    pCompCol->colId = pDataCol->colId;
    pCompCol->type = pDataCol->type;
    pCompCol->len = TYPE_BYTES[pCompCol->type] * rowsToWrite; // TODO: change it
    pCompCol->offset = toffset;
    nColsNotAllNull++;

    toffset += pCompCol->len;
  }

  ASSERT(nColsNotAllNull > 0);

  pCompData->delimiter = TSDB_FILE_DELIMITER;
  pCompData->uid = pHelper->tableInfo.uid;
  pCompData->numOfCols = nColsNotAllNull;

  size_t tsize = sizeof(SCompData) + sizeof(SCompCol) * nColsNotAllNull;
  if (twrite(pFile->fd, (void *)pCompData, tsize) < tsize) goto _err;
  int nCompCol = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    ASSERT(nCompCol < nColsNotAllNull);

    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + nCompCol;

    if (pDataCol->colId == pCompCol->colId) {
      if (twrite(pFile->fd, (void *)(pDataCol->pData), pCompCol->len) < pCompCol->len) goto _err;
      tsize += pCompCol->len;
      nCompCol++;
    }
  }

  pCompBlock->last = isLast;
  pCompBlock->offset = offset;
  pCompBlock->algorithm = 2;  // TODO
  pCompBlock->numOfPoints = rowsToWrite;
  pCompBlock->sversion = pHelper->tableInfo.sversion;
  pCompBlock->len = (int32_t)tsize;
  pCompBlock->numOfSubBlocks = isSuperBlock ? 1 : 0;
  pCompBlock->numOfCols = nColsNotAllNull;
  pCompBlock->keyFirst = dataColsKeyFirst(pDataCols);
  pCompBlock->keyLast = dataColsKeyAt(pDataCols, rowsToWrite - 1);

  tfree(pCompData);
  return 0;

  _err:
  tfree(pCompData);
  return -1;
}

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

static FORCE_INLINE int compKeyFunc(const void *arg1, const void *arg2) {
  return ((*(TSKEY *)arg1) - (*(TSKEY *)arg2));
}

static int nRowsLEThan(SDataCols *pDataCols, int maxKey) {
  void *ptr = taosbsearch((void *)&maxKey, pDataCols->cols[0].pData, pDataCols->numOfPoints, sizeof(TSKEY), compKeyFunc, TD_LE);
  if (ptr == NULL) return 0;
  return ((TSKEY *)ptr - (TSKEY *)(pDataCols->cols[0].pData)) + 1;
}

// Merge the data with a block in file
static int tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  // TODO: set pHelper->hasOldBlock
  int        rowsWritten = 0;
  SCompBlock compBlock = {0};

  TSKEY keyFirst = dataColsKeyFirst(pDataCols);

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  ASSERT(blkIdx < pIdx->numOfSuperBlocks);

  SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(pCompBlock->numOfSubBlocks >= 1);
  ASSERT(keyFirst >= pCompBlock->keyFirst);
  ASSERT(compareKeyBlock((void *)&keyFirst, (void *)pCompBlock) == 0);

  if (keyFirst > pCompBlock->keyLast) { // Merge the last block by append
    ASSERT(pCompBlock->last && pCompBlock->numOfPoints < pHelper->config.minRowsPerFileBlock);
    int defaultRowsToWrite = pHelper->config.maxRowsPerFileBlock * 4 / 5;  // TODO: make a interface

    rowsWritten = MIN((defaultRowsToWrite - pCompBlock->numOfPoints), pDataCols->numOfPoints);
    if (rowsWritten + pCompBlock->numOfPoints >= pHelper->config.minRowsPerFileBlock) {
      // Need to write to .data file
      if (tsdbLoadBlockData(pHelper, blkIdx, NULL) < 0) goto _err;
      // tdMergeDataCols();
      // if (tsdbWriteBlockToFile(pHelper, &pHelper->files.dataF, NULL, rowsWritten + pCompBlock->numOfPoints, &compBlock,
      //                          false, true) < 0)
      // goto _err;
      if (tsdbUpdateSuperBlock(pHelper, &compBlock, blkIdx) < 0) goto _err;
    } else {
      // Need still write the .last or .l file
      if (pHelper->files.nLastF.fd > 0) {
        if (tsdbLoadBlockData(pHelper, blkIdx, NULL) < 0) goto _err;
        tdMergeDataCols(NULL, pDataCols, rowsWritten);
        if (tsdbWriteBlockToFile(pHelper, &pHelper->files.nLastF, NULL, rowsWritten + pCompBlock->numOfPoints,
                                 &compBlock, false, true) < 0)
          goto _err;
        if (tsdbUpdateSuperBlock(pHelper, &compBlock, blkIdx) < 0) goto _err;
      } else {
        // Write to .last file and append as a sub-block
        if (tsdbWriteBlockToFile(pHelper, &pHelper->files.lastF, pDataCols, rowsWritten, &compBlock, true, false) < 0)
          goto _err;
        if (tsdbAddSubBlock(pHelper, &compBlock, blkIdx, rowsWritten) < 0) goto _err;
      }
    }

  } else { // Must merge with the block

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
    if (tsdbLoadBlockDataCols(pHelper, NULL, blkIdx, colId, 1) < 0) goto _err;

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

static int tsdbAdjustInfoSizeIfNeeded(SRWHelper *pHelper, size_t spaceNeeded) {
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;

  size_t spaceLeft = pHelper->compInfoSize - pIdx->len;
  ASSERT(spaceLeft >= 0);
  if (spaceLeft < spaceNeeded) {
    size_t tsize = pHelper->compInfoSize + sizeof(SCompBlock) * 16;
    if (pHelper->compInfoSize == 0) tsize += sizeof(SCompInfo);

    pHelper->pCompInfo = (SCompInfo *)realloc((void *)(pHelper->pCompInfo), tsize);
    if (pHelper->pCompInfo == NULL) return -1;

    pHelper->compInfoSize = tsize;
  }

  return 0;
}

static int tsdbInsertSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx) {
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;

  ASSERT(blkIdx >=0 && blkIdx <= pIdx->numOfSuperBlocks);
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  // Adjust memory if no more room
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, sizeof(SCompBlock)) < 0)  goto _err;

  // Insert the block
  if (blkIdx < pIdx->numOfSuperBlocks) {
    SCompBlock *pTCompBlock = pHelper->pCompInfo->blocks + blkIdx;
    memmove((void *)(pTCompBlock + 1), (void *)pTCompBlock, pIdx->len - sizeof(SCompInfo) - sizeof(SCompBlock) *blkIdx);
    pTCompBlock++;
    for (int i = 0; i < pIdx->numOfSuperBlocks - blkIdx; i++) {
      if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SCompBlock);
    }
  }
  pHelper->pCompInfo->blocks[blkIdx] = *pCompBlock;

  pIdx->numOfSuperBlocks++;
  pIdx->len += sizeof(SCompBlock);
  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfSuperBlocks - 1].keyLast;
  pIdx->hasLast = pHelper->pCompInfo->blocks[pIdx->numOfSuperBlocks - 1].last;

  return 0;

  _err:
  return -1;
}

static int tsdbAddSubBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx, int rowsAdded) {
  ASSERT(pCompBlock->numOfSubBlocks == 0);

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  SCompBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(pSCompBlock->numOfSubBlocks >= 1 && pSCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS);

  size_t spaceNeeded = (pSCompBlock->numOfSubBlocks == 1) ? sizeof(SCompBlock) * 2 : sizeof(SCompBlock);
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, spaceNeeded) < 0)  goto _err;

  // Add the sub-block
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = pIdx->len - (pSCompBlock->offset + pSCompBlock->len);
    if (tsize > 0) {
      memmove((void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len + sizeof(SCompBlock)),
              (void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len), tsize);

      for (int i = blkIdx; i < pIdx->numOfSuperBlocks; i++) {
        SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SCompBlock);
      }
    }


    *(SCompBlock *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len) = *pCompBlock;

    pSCompBlock->numOfSubBlocks++;
    pSCompBlock->len += sizeof(SCompBlock);
    pIdx->len += sizeof(SCompBlock);
  } else {  // Need to create two sub-blocks
    void *ptr = NULL;
    for (int i = blkIdx - 1; i >= 0; i--) {
      SCompBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
      if (pTCompBlock->numOfSubBlocks > 1) {
        ptr = (void *)((char *)(pHelper->pCompInfo) + pTCompBlock->offset + pTCompBlock->len);
        break;
      }
    }

    if (ptr == NULL)
      ptr = (void *)((char *)(pHelper->pCompInfo) + sizeof(SCompInfo) + sizeof(SCompBlock) * pIdx->numOfSuperBlocks);

    size_t tsize = pIdx->len - ((char *)ptr - (char *)(pHelper->pCompInfo));
    if (tsize > 0) {
      memmove((void *)((char *)ptr + sizeof(SCompBlock) * 2), ptr, tsize);
      for (int i = blkIdx + 1; i < pIdx->numOfSuperBlocks; i++) {
        SCompBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += (sizeof(SCompBlock) * 2);
      }
    }

    ((SCompBlock *)ptr)[0] = *pSCompBlock;
    ((SCompBlock *)ptr)[0].numOfSubBlocks = 0;

    ((SCompBlock *)ptr)[1] = *pCompBlock;

    pSCompBlock->numOfSubBlocks = 2;
    pSCompBlock->numOfPoints += rowsAdded;
    pSCompBlock->offset = ((char *)ptr) - ((char *)pHelper->pCompInfo);
    pSCompBlock->len = sizeof(SCompBlock) * 2;
    pSCompBlock->keyFirst = MIN(((SCompBlock *)ptr)[0].keyFirst, ((SCompBlock *)ptr)[1].keyFirst);
    pSCompBlock->keyLast = MAX(((SCompBlock *)ptr)[0].keyLast, ((SCompBlock *)ptr)[1].keyLast);

    pIdx->len += (sizeof(SCompBlock) * 2);
  }

  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfSuperBlocks - 1].keyLast;
  pIdx->hasLast = pHelper->pCompInfo->blocks[pIdx->numOfSuperBlocks - 1].last;

  return 0;

_err:
  return -1;
}

static int tsdbUpdateSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx) {
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;

  ASSERT(blkIdx >= 0 && blkIdx < pIdx->numOfSuperBlocks);

  SCompBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  ASSERT(pSCompBlock->numOfSubBlocks >= 1);

  // Delete the sub blocks it has
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = pIdx->len - (pSCompBlock->offset + pSCompBlock->len);
    if (tsize > 0) {
      memmove((void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset),
              (void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len), tsize);
    }

    for (int i = blkIdx + 1; i < pIdx->numOfSuperBlocks; i++) {
      SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
      if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset -= (sizeof(SCompBlock) * pSCompBlock->numOfSubBlocks);
    }

    pIdx->len -= (sizeof(SCompBlock) * pSCompBlock->numOfSubBlocks);
  }

  *pSCompBlock = *pCompBlock;

  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfSuperBlocks - 1].keyLast;
  pIdx->hasLast = pHelper->pCompInfo->blocks[pIdx->numOfSuperBlocks - 1].last;

  return 0;
}

// Get the number of rows in range [minKey, maxKey]
static int tsdbGetRowsInRange(SDataCols *pDataCols, int minKey, int maxKey) {
  if (pDataCols->numOfPoints == 0) return 0;

  ASSERT(minKey <= maxKey);
  TSKEY keyFirst = dataColsKeyFirst(pDataCols);
  TSKEY keyLast = dataColsKeyLast(pDataCols);
  ASSERT(keyFirst <= keyLast);

  if (minKey > keyLast || maxKey < keyFirst) return 0;

  void *ptr1 = taosbsearch((void *)&minKey, (void *)pDataCols->cols[0].pData, pDataCols->numOfPoints, sizeof(TSKEY),
                           compTSKEY, TD_GE);
  ASSERT(ptr1 != NULL);

  void *ptr2 = taosbsearch((void *)&maxKey, (void *)pDataCols->cols[0].pData, pDataCols->numOfPoints, sizeof(TSKEY),
                           compTSKEY, TD_LE);
  ASSERT(ptr2 != NULL);

  if ((TSKEY *)ptr2 - (TSKEY *)ptr1 < 0) return 0;

  return (TSKEY *)ptr2 - (TSKEY *)ptr1;
}