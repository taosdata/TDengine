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

#include "os.h"
#include "talgo.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tscompression.h"
#include "tsdbMain.h"
#include "tfile.h"

#define TSDB_GET_COMPCOL_LEN(nCols) (sizeof(SCompData) + sizeof(SCompCol) * (nCols) + sizeof(TSCKSUM))
#define TSDB_KEY_COL_OFFSET 0
#define TSDB_GET_COMPBLOCK_IDX(h, b) (POINTER_DISTANCE(b, (h)->pCompInfo->blocks)/sizeof(SCompBlock))

static bool tsdbShouldCreateNewLast(SRWHelper *pHelper);
static int  tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, SCompBlock *pCompBlock,
                                 bool isLast, bool isSuperBlock);
static int  compareKeyBlock(const void *arg1, const void *arg2);
static int  tsdbAdjustInfoSizeIfNeeded(SRWHelper *pHelper, size_t esize);
static int  tsdbInsertSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx);
static int  tsdbAddSubBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx, int rowsAdded);
static int  tsdbUpdateSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx);
static void tsdbResetHelperFileImpl(SRWHelper *pHelper);
static int  tsdbInitHelperFile(SRWHelper *pHelper);
static void tsdbDestroyHelperFile(SRWHelper *pHelper);
static void tsdbResetHelperTableImpl(SRWHelper *pHelper);
static void tsdbResetHelperTable(SRWHelper *pHelper);
static void tsdbInitHelperTable(SRWHelper *pHelper);
static void tsdbDestroyHelperTable(SRWHelper *pHelper);
static void tsdbResetHelperBlockImpl(SRWHelper *pHelper);
static void tsdbResetHelperBlock(SRWHelper *pHelper);
static int  tsdbInitHelperBlock(SRWHelper *pHelper);
static int  tsdbInitHelper(SRWHelper *pHelper, STsdbRepo *pRepo, tsdb_rw_helper_t type);
static int  tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, char *content, int32_t len, int8_t comp, int numOfRows,
                                         int maxPoints, char *buffer, int bufferSize);
static int  tsdbLoadBlockDataColsImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols, int16_t *colIds,
                                      int numOfColIds);
static int  tsdbLoadBlockDataImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols);
static int  tsdbEncodeSCompIdx(void **buf, SCompIdx *pIdx);
static void *tsdbDecodeSCompIdx(void *buf, SCompIdx *pIdx);
static int   tsdbProcessAppendCommit(SRWHelper *pHelper, SCommitIter *pCommitIter, SDataCols *pDataCols, TSKEY maxKey);
static void  tsdbDestroyHelperBlock(SRWHelper *pHelper);
static int   tsdbLoadColData(SRWHelper *pHelper, SFile *pFile, SCompBlock *pCompBlock, SCompCol *pCompCol,
                             SDataCol *pDataCol);
static int   tsdbWriteBlockToProperFile(SRWHelper *pHelper, SDataCols *pDataCols, SCompBlock *pCompBlock);
static int   tsdbProcessMergeCommit(SRWHelper *pHelper, SCommitIter *pCommitIter, SDataCols *pDataCols, TSKEY maxKey,
                                    int *blkIdx);
static int   tsdbLoadAndMergeFromCache(SDataCols *pDataCols, int *iter, SCommitIter *pCommitIter, SDataCols *pTarget,
                                       TSKEY maxKey, int maxRows);

// ---------------------- INTERNAL FUNCTIONS ----------------------
int tsdbInitReadHelper(SRWHelper *pHelper, STsdbRepo *pRepo) {
  return tsdbInitHelper(pHelper, pRepo, TSDB_READ_HELPER);
}

int tsdbInitWriteHelper(SRWHelper *pHelper, STsdbRepo *pRepo) {
  return tsdbInitHelper(pHelper, pRepo, TSDB_WRITE_HELPER);
}

void tsdbDestroyHelper(SRWHelper *pHelper) {
  if (pHelper) {
    tzfree(pHelper->pBuffer);
    tzfree(pHelper->compBuffer);
    tsdbDestroyHelperFile(pHelper);
    tsdbDestroyHelperTable(pHelper);
    tsdbDestroyHelperBlock(pHelper);
    memset((void *)pHelper, 0, sizeof(*pHelper));
  }
}

void tsdbResetHelper(SRWHelper *pHelper) {
  if (pHelper) {
    // Reset the block part
    tsdbResetHelperBlockImpl(pHelper);

    // Reset the table part
    tsdbResetHelperTableImpl(pHelper);

    // Reset the file part
    tsdbCloseHelperFile(pHelper, false);
    tsdbResetHelperFileImpl(pHelper);

    pHelper->state = TSDB_HELPER_CLEAR_STATE;
  }
}

int tsdbSetAndOpenHelperFile(SRWHelper *pHelper, SFileGroup *pGroup) {
  ASSERT(pHelper != NULL && pGroup != NULL);
  SFile *pFile = NULL;

  // Clear the helper object
  tsdbResetHelper(pHelper);

  ASSERT(pHelper->state == TSDB_HELPER_CLEAR_STATE);

  // Set the files
  pHelper->files.fGroup = *pGroup;
  if (helperType(pHelper) == TSDB_WRITE_HELPER) {
#ifdef TSDB_IDX
    tsdbGetDataFileName(pHelper->pRepo, pGroup->fileId, TSDB_FILE_TYPE_NIDX, helperNewIdxF(pHelper)->fname);
#endif
    tsdbGetDataFileName(pHelper->pRepo, pGroup->fileId, TSDB_FILE_TYPE_NHEAD, helperNewHeadF(pHelper)->fname);
    tsdbGetDataFileName(pHelper->pRepo, pGroup->fileId, TSDB_FILE_TYPE_NLAST, helperNewLastF(pHelper)->fname);
  }

  // Open the files
#ifdef TSDB_IDX
  if (tsdbOpenFile(helperIdxF(pHelper), O_RDONLY) < 0) goto _err;
#endif
  if (tsdbOpenFile(helperHeadF(pHelper), O_RDONLY) < 0) goto _err;
  if (helperType(pHelper) == TSDB_WRITE_HELPER) {
    if (tsdbOpenFile(helperDataF(pHelper), O_RDWR) < 0) goto _err;
    if (tsdbOpenFile(helperLastF(pHelper), O_RDWR) < 0) goto _err;

#ifdef TSDB_IDX
    // Create and open .i file
    pFile = helperNewIdxF(pHelper);
    if (tsdbOpenFile(pFile, O_WRONLY | O_CREAT) < 0) return -1;
    pFile->info.size = TSDB_FILE_HEAD_SIZE;
    pFile->info.magic = TSDB_FILE_INIT_MAGIC;
    if (tsdbUpdateFileHeader(pFile, 0) < 0) return -1;
#endif

    // Create and open .h
    pFile = helperNewHeadF(pHelper);
    if (tsdbOpenFile(pFile, O_WRONLY | O_CREAT) < 0) return -1;
    pFile->info.size = TSDB_FILE_HEAD_SIZE;
    pFile->info.magic = TSDB_FILE_INIT_MAGIC;
    if (tsdbUpdateFileHeader(pFile, 0) < 0) return -1;

    // Create and open .l file if should
    if (tsdbShouldCreateNewLast(pHelper)) {
      pFile = helperNewLastF(pHelper);
      if (tsdbOpenFile(pFile, O_WRONLY | O_CREAT) < 0) goto _err;
      pFile->info.size = TSDB_FILE_HEAD_SIZE;
      pFile->info.magic = TSDB_FILE_INIT_MAGIC;
      pFile->info.len = 0;
      if (tsdbUpdateFileHeader(pFile, 0) < 0) return -1;
    }
  } else {
    if (tsdbOpenFile(helperDataF(pHelper), O_RDONLY) < 0) goto _err;
    if (tsdbOpenFile(helperLastF(pHelper), O_RDONLY) < 0) goto _err;
  }

  helperSetState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN);

  return tsdbLoadCompIdx(pHelper, NULL);

_err:
  return -1;
}

int tsdbCloseHelperFile(SRWHelper *pHelper, bool hasError) {
  SFile *pFile = NULL;

#ifdef TSDB_IDX
  pFile = helperIdxF(pHelper);
  if (pFile->fd > 0) {
    close(pFile->fd);
    pFile->fd = -1;
  }
#endif

  pFile = helperHeadF(pHelper);
  if (pFile->fd > 0) {
    close(pFile->fd);
    pFile->fd = -1;
  }

  pFile = helperDataF(pHelper);
  if (pFile->fd > 0) {
    if (helperType(pHelper) == TSDB_WRITE_HELPER) {
      tsdbUpdateFileHeader(pFile, 0);
      fsync(pFile->fd);
    }
    close(pFile->fd);
    pFile->fd = -1;
  }

  pFile = helperLastF(pHelper);
  if (pFile->fd > 0) {
    if (helperType(pHelper) == TSDB_WRITE_HELPER && !TSDB_NLAST_FILE_OPENED(pHelper)) {
      fsync(pFile->fd);
    }
    close(pFile->fd);
    pFile->fd = -1;
  }

  if (helperType(pHelper) == TSDB_WRITE_HELPER) {
#ifdef TSDB_IDX
    pFile = helperNewIdxF(pHelper);
    if (pFile->fd > 0) {
      if (!hasError) tsdbUpdateFileHeader(pFile, 0);
      fsync(pFile->fd);
      close(pFile->fd);
      pFile->fd = -1;
      if (hasError) {
        (void)remove(pFile->fname);
      } else {
        if (rename(pFile->fname, helperIdxF(pHelper)->fname) < 0) {
          tsdbError("failed to rename file from %s to %s since %s", pFile->fname, helperIdxF(pHelper)->fname,
                    strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          return -1;
        }
        helperIdxF(pHelper)->info = pFile->info;
      }
    }
#endif

    pFile = helperNewHeadF(pHelper);
    if (pFile->fd > 0) {
      if (!hasError) tsdbUpdateFileHeader(pFile, 0);
      fsync(pFile->fd);
      close(pFile->fd);
      pFile->fd = -1;
      if (hasError) {
        (void)remove(pFile->fname);
      } else {
        if (rename(pFile->fname, helperHeadF(pHelper)->fname) < 0) {
          tsdbError("failed to rename file from %s to %s since %s", pFile->fname, helperHeadF(pHelper)->fname,
                    strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          return -1;
        }
        helperHeadF(pHelper)->info = pFile->info;
      }
    }

    pFile = helperNewLastF(pHelper);
    if (pFile->fd > 0) {
      if (!hasError) tsdbUpdateFileHeader(pFile, 0);
      fsync(pFile->fd);
      close(pFile->fd);
      pFile->fd = -1;
      if (hasError) {
        (void)remove(pFile->fname);
      } else {
        if (rename(pFile->fname, helperLastF(pHelper)->fname) < 0) {
          tsdbError("failed to rename file from %s to %s since %s", pFile->fname, helperLastF(pHelper)->fname,
                    strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          return -1;
        }
        helperLastF(pHelper)->info = helperNewLastF(pHelper)->info;
      }
    }
  }
  return 0;
}

void tsdbSetHelperTable(SRWHelper *pHelper, STable *pTable, STsdbRepo *pRepo) {
  ASSERT(helperHasState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN | TSDB_HELPER_IDX_LOAD));

  // Clear members and state used by previous table
  tsdbResetHelperTable(pHelper);
  ASSERT(helperHasState(pHelper, (TSDB_HELPER_FILE_SET_AND_OPEN | TSDB_HELPER_IDX_LOAD)));

  pHelper->tableInfo.tid = pTable->tableId.tid;
  pHelper->tableInfo.uid = pTable->tableId.uid;
  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);

  tdInitDataCols(pHelper->pDataCols[0], pSchema);
  tdInitDataCols(pHelper->pDataCols[1], pSchema);

  if (pHelper->idxH.numOfIdx > 0) {
    while (true) {
      if (pHelper->idxH.curIdx >= pHelper->idxH.numOfIdx) {
        memset(&(pHelper->curCompIdx), 0, sizeof(SCompIdx));
        break;
      }

      SCompIdx *pIdx = &(pHelper->idxH.pIdxArray[pHelper->idxH.curIdx]);
      if (pIdx->tid == TABLE_TID(pTable)) {
        if (pIdx->uid == TABLE_UID(pTable)) {
          pHelper->curCompIdx = *pIdx;
        } else {
          memset(&(pHelper->curCompIdx), 0, sizeof(SCompIdx));
        }
        pHelper->idxH.curIdx++;
        break;
      } else if (pIdx->tid > TABLE_TID(pTable)) {
        memset(&(pHelper->curCompIdx), 0, sizeof(SCompIdx));
        break;
      } else {
        pHelper->idxH.curIdx++;
      }
    }
  } else {
    memset(&(pHelper->curCompIdx), 0, sizeof(SCompIdx));
  }

  if (helperType(pHelper) == TSDB_WRITE_HELPER && pHelper->curCompIdx.hasLast) {
    pHelper->hasOldLastBlock = true;
  }

  helperSetState(pHelper, TSDB_HELPER_TABLE_SET);
  ASSERT(pHelper->state == ((TSDB_HELPER_TABLE_SET << 1) - 1));
}

int tsdbCommitTableData(SRWHelper *pHelper, SCommitIter *pCommitIter, SDataCols *pDataCols, TSKEY maxKey) {
  ASSERT(helperType(pHelper) == TSDB_WRITE_HELPER);

  SCompIdx *pIdx = &(pHelper->curCompIdx);
  int       blkIdx = 0;

  ASSERT(pIdx->offset == 0 || pIdx->uid == TABLE_UID(pCommitIter->pTable));
  if (tsdbLoadCompInfo(pHelper, NULL) < 0) return -1;

  while (true) {
    ASSERT(blkIdx <= pIdx->numOfBlocks);
    TSKEY keyFirst = tsdbNextIterKey(pCommitIter->pIter);
    if (keyFirst < 0 || keyFirst > maxKey) break;  // iter over

    if (pIdx->len <= 0 || keyFirst > pIdx->maxKey) {
      if (tsdbProcessAppendCommit(pHelper, pCommitIter, pDataCols, maxKey) < 0) return -1;
      blkIdx = pIdx->numOfBlocks;
    } else {
      if (tsdbProcessMergeCommit(pHelper, pCommitIter, pDataCols, maxKey, &blkIdx) < 0) return -1;
    }
  }

  return 0;
}

int tsdbMoveLastBlockIfNeccessary(SRWHelper *pHelper) {
  STsdbCfg *pCfg = &pHelper->pRepo->config;

  ASSERT(helperType(pHelper) == TSDB_WRITE_HELPER);
  SCompIdx * pIdx = &(pHelper->curCompIdx);
  SCompBlock compBlock = {0};
  if (TSDB_NLAST_FILE_OPENED(pHelper) && (pHelper->hasOldLastBlock)) {
    if (tsdbLoadCompInfo(pHelper, NULL) < 0) return -1;

    SCompBlock *pCompBlock = blockAtIdx(pHelper, pIdx->numOfBlocks - 1);
    ASSERT(pCompBlock->last);
    if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
    ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows &&
           pHelper->pDataCols[0]->numOfRows < pCfg->minRowsPerFileBlock);
    if (tsdbWriteBlockToFile(pHelper, helperNewLastF(pHelper), pHelper->pDataCols[0], &compBlock, true, true) < 0)
      return -1;

    if (tsdbUpdateSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks - 1) < 0) return -1;

#if 0
    if (pCompBlock->numOfSubBlocks > 1) {
      if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
      ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows &&
             pHelper->pDataCols[0]->numOfRows < pCfg->minRowsPerFileBlock);
      if (tsdbWriteBlockToFile(pHelper, helperNewLastF(pHelper), pHelper->pDataCols[0], &compBlock, true, true) < 0)
        return -1;

      if (tsdbUpdateSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks - 1) < 0) return -1;
    } else {
      if (lseek(helperLastF(pHelper)->fd, pCompBlock->offset, SEEK_SET) < 0) {
        tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), helperLastF(pHelper)->fname,
                  strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
      pCompBlock->offset = lseek(helperNewLastF(pHelper)->fd, 0, SEEK_END);
      if (pCompBlock->offset < 0) {
        tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), helperNewLastF(pHelper)->fname,
                  strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }

      if (tsendfile(helperNewLastF(pHelper)->fd, helperLastF(pHelper)->fd, NULL, pCompBlock->len) < pCompBlock->len) {
        tsdbError("vgId:%d failed to sendfile from file %s to file %s since %s", REPO_ID(pHelper->pRepo),
                  helperLastF(pHelper)->fname, helperNewLastF(pHelper)->fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    }
#endif

    pHelper->hasOldLastBlock = false;
  }

  return 0;
}

int tsdbWriteCompInfo(SRWHelper *pHelper) {
  SCompIdx *pIdx = &(pHelper->curCompIdx);
  off_t     offset = 0;
  SFile *   pFile = helperNewHeadF(pHelper);

  if (pIdx->len > 0) {
    if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) {
      if (tsdbLoadCompInfo(pHelper, NULL) < 0) return -1;
    } else {
      pHelper->pCompInfo->delimiter = TSDB_FILE_DELIMITER;
      pHelper->pCompInfo->uid = pHelper->tableInfo.uid;
      pHelper->pCompInfo->tid = pHelper->tableInfo.tid;
      ASSERT(pIdx->len > sizeof(SCompInfo) + sizeof(TSCKSUM) &&
             (pIdx->len - sizeof(SCompInfo) - sizeof(TSCKSUM)) % sizeof(SCompBlock) == 0);
      taosCalcChecksumAppend(0, (uint8_t *)pHelper->pCompInfo, pIdx->len);
    }

    pFile->info.magic = taosCalcChecksum(
        pFile->info.magic, (uint8_t *)POINTER_SHIFT(pHelper->pCompInfo, pIdx->len - sizeof(TSCKSUM)), sizeof(TSCKSUM));
    offset = lseek(pFile->fd, 0, SEEK_END);
    if (offset < 0) {
      tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
    pIdx->offset = offset;
    pIdx->uid = pHelper->tableInfo.uid;
    pIdx->tid = pHelper->tableInfo.tid;
    ASSERT(pIdx->offset >= TSDB_FILE_HEAD_SIZE);

    if (twrite(pFile->fd, (void *)(pHelper->pCompInfo), pIdx->len) < pIdx->len) {
      tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pHelper->pRepo), pIdx->len,
                pFile->fname, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }

#ifdef TSDB_IDX
    pFile = helperNewIdxF(pHelper);
#endif

    if (tsizeof(pHelper->pWIdx) < pFile->info.len + sizeof(SCompIdx) + 12) {
      pHelper->pWIdx = trealloc(pHelper->pWIdx, tsizeof(pHelper->pWIdx) == 0 ? 1024 : tsizeof(pHelper->pWIdx) * 2);
      if (pHelper->pWIdx == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    }

    void *pBuf = POINTER_SHIFT(pHelper->pWIdx, pFile->info.len);
    pFile->info.len += tsdbEncodeSCompIdx(&pBuf, &(pHelper->curCompIdx));
  }

  return 0;
}

int tsdbWriteCompIdx(SRWHelper *pHelper) {
  ASSERT(helperType(pHelper) == TSDB_WRITE_HELPER);
  off_t offset = 0;

#ifdef TSDB_IDX
  SFile *pFile = helperNewIdxF(pHelper);
#else
  SFile *pFile = helperNewHeadF(pHelper);
#endif

  pFile->info.len += sizeof(TSCKSUM);
  if (tsizeof(pHelper->pWIdx) < pFile->info.len) {
    pHelper->pWIdx = trealloc(pHelper->pWIdx, pFile->info.len);
    if (pHelper->pWIdx == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }
  taosCalcChecksumAppend(0, (uint8_t *)pHelper->pWIdx, pFile->info.len);
  pFile->info.magic = taosCalcChecksum(
      pFile->info.magic, (uint8_t *)POINTER_SHIFT(pHelper->pWIdx, pFile->info.len - sizeof(TSCKSUM)), sizeof(TSCKSUM));

  offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pFile->info.offset = offset;

  if (twrite(pFile->fd, (void *)pHelper->pWIdx, pFile->info.len) < pFile->info.len) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pHelper->pRepo), pFile->info.len,
              pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tsdbLoadCompIdx(SRWHelper *pHelper, void *target) {
  ASSERT(pHelper->state == TSDB_HELPER_FILE_SET_AND_OPEN);
#ifdef TSDB_IDX
  SFile *pFile = helperIdxF(pHelper);
#else
  SFile *pFile = helperHeadF(pHelper);
#endif
  int fd = pFile->fd;

  if (!helperHasState(pHelper, TSDB_HELPER_IDX_LOAD)) {
    // If not load from file, just load it in object
    if (pFile->info.len > 0) {
      if ((pHelper->pBuffer = trealloc(pHelper->pBuffer, pFile->info.len)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      if (lseek(fd, pFile->info.offset, SEEK_SET) < 0) {
        tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }

      if (tread(fd, (void *)(pHelper->pBuffer), pFile->info.len) < pFile->info.len) {
        tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pFile->info.len,
                  pFile->fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }

      if (!taosCheckChecksumWhole((uint8_t *)(pHelper->pBuffer), pFile->info.len)) {
        tsdbError("vgId:%d file %s SCompIdx part is corrupted. len %u", REPO_ID(pHelper->pRepo), pFile->fname,
                  pFile->info.len);
        terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
        return -1;
      }

      // Decode it
      pHelper->idxH.numOfIdx = 0;
      void *ptr = pHelper->pBuffer;
      while (POINTER_DISTANCE(ptr, pHelper->pBuffer) < (pFile->info.len - sizeof(TSCKSUM))) {
        size_t tlen = tsizeof(pHelper->idxH.pIdxArray);
        pHelper->idxH.numOfIdx++;

        if (tlen < pHelper->idxH.numOfIdx * sizeof(SCompIdx)) {
          pHelper->idxH.pIdxArray = (SCompIdx *)trealloc(pHelper->idxH.pIdxArray, (tlen == 0) ? 1024 : tlen * 2);
          if (pHelper->idxH.pIdxArray == NULL) {
            terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
            return -1;
          }
        }

        ptr = tsdbDecodeSCompIdx(ptr, &(pHelper->idxH.pIdxArray[pHelper->idxH.numOfIdx - 1]));
        if (ptr == NULL) {
          tsdbError("vgId:%d file %s SCompIdx part is corrupted. len %u", REPO_ID(pHelper->pRepo), pFile->fname,
                    pFile->info.len);
          terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
          return -1;
        }

        ASSERT(pHelper->idxH.numOfIdx == 1 || pHelper->idxH.pIdxArray[pHelper->idxH.numOfIdx - 1].tid >
                                                  pHelper->idxH.pIdxArray[pHelper->idxH.numOfIdx - 2].tid);

        ASSERT(POINTER_DISTANCE(ptr, pHelper->pBuffer) <= pFile->info.len - sizeof(TSCKSUM));
      }
    }
  }
  helperSetState(pHelper, TSDB_HELPER_IDX_LOAD);

  // Copy the memory for outside usage
  if (target && pHelper->idxH.numOfIdx > 0)
    memcpy(target, pHelper->idxH.pIdxArray, sizeof(SCompIdx) * pHelper->idxH.numOfIdx);

  return 0;
}

int tsdbLoadCompInfo(SRWHelper *pHelper, void *target) {
  ASSERT(helperHasState(pHelper, TSDB_HELPER_TABLE_SET));

  SCompIdx *pIdx = &(pHelper->curCompIdx);

  int fd = helperHeadF(pHelper)->fd;

  if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) {
    if (pIdx->offset > 0) {
      ASSERT(pIdx->uid == pHelper->tableInfo.uid);
      if (lseek(fd, pIdx->offset, SEEK_SET) < 0) {
        tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), helperHeadF(pHelper)->fname,
                  strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }

      pHelper->pCompInfo = trealloc((void *)pHelper->pCompInfo, pIdx->len);
      if (tread(fd, (void *)(pHelper->pCompInfo), pIdx->len) < pIdx->len) {
        tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pIdx->len,
                  helperHeadF(pHelper)->fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
      if (!taosCheckChecksumWhole((uint8_t *)pHelper->pCompInfo, pIdx->len)) {
        tsdbError("vgId:%d file %s SCompInfo part is corrupted, tid %d uid %" PRIu64, REPO_ID(pHelper->pRepo),
                  helperHeadF(pHelper)->fname, pHelper->tableInfo.tid, pHelper->tableInfo.uid);
        terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
        return -1;
      }

      ASSERT(pIdx->uid == pHelper->pCompInfo->uid && pIdx->tid == pHelper->pCompInfo->tid);
    }

    helperSetState(pHelper, TSDB_HELPER_INFO_LOAD);
  }

  if (target) memcpy(target, (void *)(pHelper->pCompInfo), pIdx->len);

  return 0;
}

int tsdbLoadCompData(SRWHelper *pHelper, SCompBlock *pCompBlock, void *target) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);
  SFile *pFile = (pCompBlock->last) ? helperLastF(pHelper) : helperDataF(pHelper);

  if (lseek(pFile->fd, pCompBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  size_t tsize = TSDB_GET_COMPCOL_LEN(pCompBlock->numOfCols);
  pHelper->pCompData = trealloc((void *)pHelper->pCompData, tsize);
  if (pHelper->pCompData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tread(pFile->fd, (void *)pHelper->pCompData, tsize) < tsize) {
    tsdbError("vgId:%d failed to read %zu bytes from file %s since %s", REPO_ID(pHelper->pRepo), tsize, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)pHelper->pCompData, tsize)) {
    tsdbError("vgId:%d file %s is broken, offset %" PRId64 " size %zu", REPO_ID(pHelper->pRepo), pFile->fname,
              (int64_t)pCompBlock->offset, tsize);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  ASSERT(pCompBlock->numOfCols == pHelper->pCompData->numOfCols);

  if (target) memcpy(target, pHelper->pCompData, tsize);

  return 0;
}

void tsdbGetDataStatis(SRWHelper *pHelper, SDataStatis *pStatis, int numOfCols) {
  SCompData *pCompData = pHelper->pCompData;

  for (int i = 0, j = 0; i < numOfCols;) {
    if (j >= pCompData->numOfCols) {
      pStatis[i].numOfNull = -1;
      i++;
      continue;
    }

    if (pStatis[i].colId == pCompData->cols[j].colId) {
      pStatis[i].sum = pCompData->cols[j].sum;
      pStatis[i].max = pCompData->cols[j].max;
      pStatis[i].min = pCompData->cols[j].min;
      pStatis[i].maxIndex = pCompData->cols[j].maxIndex;
      pStatis[i].minIndex = pCompData->cols[j].minIndex;
      pStatis[i].numOfNull = pCompData->cols[j].numOfNull;
      i++;
      j++;
    } else if (pStatis[i].colId < pCompData->cols[j].colId) {
      pStatis[i].numOfNull = -1;
      i++;
    } else {
      j++;
    }
  }
}

int tsdbLoadBlockDataCols(SRWHelper *pHelper, SCompBlock *pCompBlock, SCompInfo *pCompInfo, int16_t *colIds, int numOfColIds) {
  ASSERT(pCompBlock->numOfSubBlocks >= 1);  // Must be super block
  SCompBlock *pTCompBlock = pCompBlock;

  int numOfSubBlocks = pCompBlock->numOfSubBlocks;
  if (numOfSubBlocks > 1)
    pTCompBlock = (SCompBlock *)POINTER_SHIFT((pCompInfo == NULL) ? pHelper->pCompInfo : pCompInfo, pCompBlock->offset);

  tdResetDataCols(pHelper->pDataCols[0]);
  if (tsdbLoadBlockDataColsImpl(pHelper, pTCompBlock, pHelper->pDataCols[0], colIds, numOfColIds) < 0) goto _err;
  for (int i = 1; i < numOfSubBlocks; i++) {
    tdResetDataCols(pHelper->pDataCols[1]);
    pTCompBlock++;
    if (tsdbLoadBlockDataColsImpl(pHelper, pTCompBlock, pHelper->pDataCols[1], colIds, numOfColIds) < 0) goto _err;
    if (tdMergeDataCols(pHelper->pDataCols[0], pHelper->pDataCols[1], pHelper->pDataCols[1]->numOfRows) < 0) goto _err;
  }

  ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows &&
         dataColsKeyFirst(pHelper->pDataCols[0]) == pCompBlock->keyFirst &&
         dataColsKeyLast(pHelper->pDataCols[0]) == pCompBlock->keyLast);

  return 0;

_err:
  return -1;
}

int tsdbLoadBlockData(SRWHelper *pHelper, SCompBlock *pCompBlock, SCompInfo *pCompInfo) {
  SCompBlock *pTCompBlock = pCompBlock;

  int numOfSubBlock = pCompBlock->numOfSubBlocks;
  if (numOfSubBlock > 1)
    pTCompBlock = (SCompBlock *)POINTER_SHIFT((pCompInfo == NULL) ? pHelper->pCompInfo : pCompInfo, pCompBlock->offset);

  tdResetDataCols(pHelper->pDataCols[0]);
  if (tsdbLoadBlockDataImpl(pHelper, pTCompBlock, pHelper->pDataCols[0]) < 0) goto _err;
  for (int i = 1; i < numOfSubBlock; i++) {
    tdResetDataCols(pHelper->pDataCols[1]);
    pTCompBlock++;
    if (tsdbLoadBlockDataImpl(pHelper, pTCompBlock, pHelper->pDataCols[1]) < 0) goto _err;
    if (tdMergeDataCols(pHelper->pDataCols[0], pHelper->pDataCols[1], pHelper->pDataCols[1]->numOfRows) < 0) goto _err;
  }

  ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows &&
         dataColsKeyFirst(pHelper->pDataCols[0]) == pCompBlock->keyFirst &&
         dataColsKeyLast(pHelper->pDataCols[0]) == pCompBlock->keyLast);

  return 0;

_err:
  return -1;
}

// ---------------------- INTERNAL FUNCTIONS ----------------------
static bool tsdbShouldCreateNewLast(SRWHelper *pHelper) {
  ASSERT(helperLastF(pHelper)->fd > 0);
  struct stat st;
  if (fstat(helperLastF(pHelper)->fd, &st) < 0) return true;
  if (st.st_size > 32 * 1024 + TSDB_FILE_HEAD_SIZE) return true;
  return false;
}

static int tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, SCompBlock *pCompBlock,
                                bool isLast, bool isSuperBlock) {
  STsdbCfg * pCfg = &(pHelper->pRepo->config);
  SCompData *pCompData = (SCompData *)(pHelper->pBuffer);
  int64_t    offset = 0;
  int        rowsToWrite = pDataCols->numOfRows;

  ASSERT(rowsToWrite > 0 && rowsToWrite <= pCfg->maxRowsPerFileBlock);
  ASSERT(isLast ? rowsToWrite < pCfg->minRowsPerFileBlock : true);

  offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) {
    tsdbError("vgId:%d failed to write block to file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  int nColsNotAllNull = 0;
  for (int ncol = 1; ncol < pDataCols->numOfCols; ncol++) {  // ncol from 1, we skip the timestamp column
    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + nColsNotAllNull;

    if (isNEleNull(pDataCol, rowsToWrite)) {  // all data to commit are NULL, just ignore it
      continue;
    }

    memset(pCompCol, 0, sizeof(*pCompCol));

    pCompCol->colId = pDataCol->colId;
    pCompCol->type = pDataCol->type;
    if (tDataTypeDesc[pDataCol->type].getStatisFunc) {
      (*tDataTypeDesc[pDataCol->type].getStatisFunc)(
          (TSKEY *)(pDataCols->cols[0].pData), pDataCol->pData, rowsToWrite, &(pCompCol->min), &(pCompCol->max),
          &(pCompCol->sum), &(pCompCol->minIndex), &(pCompCol->maxIndex), &(pCompCol->numOfNull));
    }
    nColsNotAllNull++;
  }

  ASSERT(nColsNotAllNull >= 0 && nColsNotAllNull <= pDataCols->numOfCols);

  // Compress the data if neccessary
  int     tcol = 0;
  int32_t toffset = 0;
  int32_t tsize = TSDB_GET_COMPCOL_LEN(nColsNotAllNull);
  int32_t lsize = tsize;
  int32_t keyLen = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    if (tcol >= nColsNotAllNull) break;

    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + tcol;

    if (ncol != 0 && (pDataCol->colId != pCompCol->colId)) continue;
    void *tptr = POINTER_SHIFT(pCompData, lsize);

    int32_t flen = 0;  // final length
    int32_t tlen = dataColGetNEleLen(pDataCol, rowsToWrite);

    if (pCfg->compression) {
      if (pCfg->compression == TWO_STAGE_COMP) {
        pHelper->compBuffer = trealloc(pHelper->compBuffer, tlen + COMP_OVERFLOW_BYTES);
        if (pHelper->compBuffer == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          goto _err;
        }
      }

      flen = (*(tDataTypeDesc[pDataCol->type].compFunc))((char *)pDataCol->pData, tlen, rowsToWrite, tptr,
                                                         tsizeof(pHelper->pBuffer) - lsize, pCfg->compression,
                                                         pHelper->compBuffer, tsizeof(pHelper->compBuffer));
    } else {
      flen = tlen;
      memcpy(tptr, pDataCol->pData, flen);
    }

    // Add checksum
    ASSERT(flen > 0);
    flen += sizeof(TSCKSUM);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, flen);
    pFile->info.magic =
        taosCalcChecksum(pFile->info.magic, (uint8_t *)POINTER_SHIFT(tptr, flen - sizeof(TSCKSUM)), sizeof(TSCKSUM));

    if (ncol != 0) {
      pCompCol->offset = toffset;
      pCompCol->len = flen;
      tcol++;
    } else {
      keyLen = flen;
    }

    toffset += flen;
    lsize += flen;
  }

  pCompData->delimiter = TSDB_FILE_DELIMITER;
  pCompData->uid = pHelper->tableInfo.uid;
  pCompData->numOfCols = nColsNotAllNull;

  taosCalcChecksumAppend(0, (uint8_t *)pCompData, tsize);
  pFile->info.magic = taosCalcChecksum(pFile->info.magic, (uint8_t *)POINTER_SHIFT(pCompData, tsize - sizeof(TSCKSUM)),
                                       sizeof(TSCKSUM));

  // Write the whole block to file
  if (twrite(pFile->fd, (void *)pCompData, lsize) < lsize) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(helperRepo(pHelper)), lsize, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // Update pCompBlock membership vairables
  pCompBlock->last = isLast;
  pCompBlock->offset = offset;
  pCompBlock->algorithm = pCfg->compression;
  pCompBlock->numOfRows = rowsToWrite;
  pCompBlock->len = lsize;
  pCompBlock->keyLen = keyLen;
  pCompBlock->numOfSubBlocks = isSuperBlock ? 1 : 0;
  pCompBlock->numOfCols = nColsNotAllNull;
  pCompBlock->keyFirst = dataColsKeyFirst(pDataCols);
  pCompBlock->keyLast = dataColsKeyAt(pDataCols, rowsToWrite - 1);

  tsdbDebug("vgId:%d tid:%d a block of data is written to file %s, offset %" PRId64
            " numOfRows %d len %d numOfCols %" PRId16 " keyFirst %" PRId64 " keyLast %" PRId64,
            REPO_ID(helperRepo(pHelper)), pHelper->tableInfo.tid, pFile->fname, (int64_t)(pCompBlock->offset),
            (int)(pCompBlock->numOfRows), pCompBlock->len, pCompBlock->numOfCols, pCompBlock->keyFirst,
            pCompBlock->keyLast);

  return 0;

_err:
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

static int tsdbAdjustInfoSizeIfNeeded(SRWHelper *pHelper, size_t esize) {
  if (tsizeof((void *)pHelper->pCompInfo) <= esize) {
    size_t tsize = esize + sizeof(SCompBlock) * 16;
    pHelper->pCompInfo = (SCompInfo *)trealloc(pHelper->pCompInfo, tsize);
    if (pHelper->pCompInfo == NULL) return -1;
  }

  return 0;
}

static int tsdbInsertSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx) {
  SCompIdx *pIdx = &(pHelper->curCompIdx);

  ASSERT(blkIdx >= 0 && blkIdx <= pIdx->numOfBlocks);
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  // Adjust memory if no more room
  if (pIdx->len == 0) pIdx->len = sizeof(SCompInfo) + sizeof(TSCKSUM);
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, pIdx->len + sizeof(SCompInfo)) < 0) goto _err;

  // Change the offset
  for (int i = 0; i < pIdx->numOfBlocks; i++) {
    SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
    if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SCompBlock);
  }

  // Memmove if needed
  int tsize = pIdx->len - (sizeof(SCompInfo) + sizeof(SCompBlock) * blkIdx);
  if (tsize > 0) {
    ASSERT(sizeof(SCompInfo) + sizeof(SCompBlock) * (blkIdx + 1) < tsizeof(pHelper->pCompInfo));
    ASSERT(sizeof(SCompInfo) + sizeof(SCompBlock) * (blkIdx + 1) + tsize <= tsizeof(pHelper->pCompInfo));
    memmove(POINTER_SHIFT(pHelper->pCompInfo, sizeof(SCompInfo) + sizeof(SCompBlock) * (blkIdx + 1)),
            POINTER_SHIFT(pHelper->pCompInfo, sizeof(SCompInfo) + sizeof(SCompBlock) * blkIdx), tsize);
  }
  pHelper->pCompInfo->blocks[blkIdx] = *pCompBlock;

  pIdx->numOfBlocks++;
  pIdx->len += sizeof(SCompBlock);
  ASSERT(pIdx->len <= tsizeof(pHelper->pCompInfo));
  pIdx->maxKey = blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->keyLast;
  pIdx->hasLast = blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->last;

  if (pIdx->numOfBlocks > 1) {
    ASSERT(pHelper->pCompInfo->blocks[0].keyLast < pHelper->pCompInfo->blocks[1].keyFirst);
  }

  tsdbDebug("vgId:%d tid:%d a super block is inserted at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
            blkIdx);

  return 0;

_err:
  return -1;
}

static int tsdbAddSubBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx, int rowsAdded) {
  ASSERT(pCompBlock->numOfSubBlocks == 0);

  SCompIdx *pIdx = &(pHelper->curCompIdx);
  ASSERT(blkIdx >= 0 && blkIdx < pIdx->numOfBlocks);

  SCompBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(pSCompBlock->numOfSubBlocks >= 1 && pSCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS);

  size_t spaceNeeded =
      (pSCompBlock->numOfSubBlocks == 1) ? pIdx->len + sizeof(SCompBlock) * 2 : pIdx->len + sizeof(SCompBlock);
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, spaceNeeded) < 0) goto _err;

  pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  // Add the sub-block
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = pIdx->len - (pSCompBlock->offset + pSCompBlock->len);
    if (tsize > 0) {
      memmove((void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len + sizeof(SCompBlock)),
              (void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len), tsize);

      for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
        SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SCompBlock);
      }
    }

    *(SCompBlock *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len) = *pCompBlock;

    pSCompBlock->numOfSubBlocks++;
    ASSERT(pSCompBlock->numOfSubBlocks <= TSDB_MAX_SUBBLOCKS);
    pSCompBlock->len += sizeof(SCompBlock);
    pSCompBlock->numOfRows += rowsAdded;
    pSCompBlock->keyFirst = MIN(pSCompBlock->keyFirst, pCompBlock->keyFirst);
    pSCompBlock->keyLast = MAX(pSCompBlock->keyLast, pCompBlock->keyLast);
    pIdx->len += sizeof(SCompBlock);
  } else {  // Need to create two sub-blocks
    void *ptr = NULL;
    for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
      SCompBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
      if (pTCompBlock->numOfSubBlocks > 1) {
        ptr = POINTER_SHIFT(pHelper->pCompInfo, pTCompBlock->offset);
        break;
      }
    }

    if (ptr == NULL) ptr = POINTER_SHIFT(pHelper->pCompInfo, pIdx->len - sizeof(TSCKSUM));

    size_t tsize = pIdx->len - ((char *)ptr - (char *)(pHelper->pCompInfo));
    if (tsize > 0) {
      memmove(POINTER_SHIFT(ptr, sizeof(SCompBlock) * 2), ptr, tsize);
      for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
        SCompBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += (sizeof(SCompBlock) * 2);
      }
    }

    ((SCompBlock *)ptr)[0] = *pSCompBlock;
    ((SCompBlock *)ptr)[0].numOfSubBlocks = 0;

    ((SCompBlock *)ptr)[1] = *pCompBlock;

    pSCompBlock->numOfSubBlocks = 2;
    pSCompBlock->numOfRows += rowsAdded;
    pSCompBlock->offset = ((char *)ptr) - ((char *)pHelper->pCompInfo);
    pSCompBlock->len = sizeof(SCompBlock) * 2;
    pSCompBlock->keyFirst = MIN(((SCompBlock *)ptr)[0].keyFirst, ((SCompBlock *)ptr)[1].keyFirst);
    pSCompBlock->keyLast = MAX(((SCompBlock *)ptr)[0].keyLast, ((SCompBlock *)ptr)[1].keyLast);

    pIdx->len += (sizeof(SCompBlock) * 2);
  }

  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].keyLast;
  pIdx->hasLast = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].last;

  tsdbDebug("vgId:%d tid:%d a subblock is added at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid, blkIdx);

  return 0;

_err:
  return -1;
}

static int tsdbUpdateSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx) {
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  SCompIdx *pIdx = &(pHelper->curCompIdx);

  ASSERT(blkIdx >= 0 && blkIdx < pIdx->numOfBlocks);

  SCompBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  ASSERT(pSCompBlock->numOfSubBlocks >= 1);

  // Delete the sub blocks it has
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = pIdx->len - (pSCompBlock->offset + pSCompBlock->len);
    if (tsize > 0) {
      memmove(POINTER_SHIFT(pHelper->pCompInfo, pSCompBlock->offset),
              POINTER_SHIFT(pHelper->pCompInfo, pSCompBlock->offset + pSCompBlock->len), tsize);
    }

    for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
      SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
      if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset -= (sizeof(SCompBlock) * pSCompBlock->numOfSubBlocks);
    }

    pIdx->len -= (sizeof(SCompBlock) * pSCompBlock->numOfSubBlocks);
  }

  *pSCompBlock = *pCompBlock;

  pIdx->maxKey = blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->keyLast;
  pIdx->hasLast = blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->last;

  tsdbDebug("vgId:%d tid:%d a super block is updated at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
            blkIdx);

  return 0;
}

static void tsdbResetHelperFileImpl(SRWHelper *pHelper) {
  pHelper->idxH.numOfIdx = 0;
  pHelper->idxH.curIdx = 0;
  memset((void *)&pHelper->files, 0, sizeof(pHelper->files));
  helperHeadF(pHelper)->fd = -1;
  helperDataF(pHelper)->fd = -1;
  helperLastF(pHelper)->fd = -1;
  helperNewHeadF(pHelper)->fd = -1;
  helperNewLastF(pHelper)->fd = -1;
#ifdef TSDB_IDX
  helperIdxF(pHelper)->fd = -1;
  helperNewIdxF(pHelper)->fd = -1;
#endif
}

static int tsdbInitHelperFile(SRWHelper *pHelper) {
  tsdbResetHelperFileImpl(pHelper);
  return 0;
}

static void tsdbDestroyHelperFile(SRWHelper *pHelper) {
  tsdbCloseHelperFile(pHelper, false);
  tsdbResetHelperFileImpl(pHelper);
  tzfree(pHelper->idxH.pIdxArray);
  tzfree(pHelper->pWIdx);
}

// ---------- Operations on Helper Table part
static void tsdbResetHelperTableImpl(SRWHelper *pHelper) {
  memset((void *)&pHelper->tableInfo, 0, sizeof(SHelperTable));
  pHelper->hasOldLastBlock = false;
}

static void tsdbResetHelperTable(SRWHelper *pHelper) {
  tsdbResetHelperBlock(pHelper);
  tsdbResetHelperTableImpl(pHelper);
  helperClearState(pHelper, (TSDB_HELPER_TABLE_SET | TSDB_HELPER_INFO_LOAD));
}

static void tsdbInitHelperTable(SRWHelper *pHelper) { tsdbResetHelperTableImpl(pHelper); }

static void tsdbDestroyHelperTable(SRWHelper *pHelper) { tzfree((void *)pHelper->pCompInfo); }

// ---------- Operations on Helper Block part
static void tsdbResetHelperBlockImpl(SRWHelper *pHelper) {
  tdResetDataCols(pHelper->pDataCols[0]);
  tdResetDataCols(pHelper->pDataCols[1]);
}

static void tsdbResetHelperBlock(SRWHelper *pHelper) {
  tsdbResetHelperBlockImpl(pHelper);
  // helperClearState(pHelper, TSDB_HELPER_)
}

static int tsdbInitHelperBlock(SRWHelper *pHelper) {
  STsdbRepo *pRepo = helperRepo(pHelper);
  STsdbMeta *pMeta = pHelper->pRepo->tsdbMeta;

  pHelper->pDataCols[0] = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pRepo->config.maxRowsPerFileBlock);
  pHelper->pDataCols[1] = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pRepo->config.maxRowsPerFileBlock);
  if (pHelper->pDataCols[0] == NULL || pHelper->pDataCols[1] == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  tsdbResetHelperBlockImpl(pHelper);

  return 0;
}

static void tsdbDestroyHelperBlock(SRWHelper *pHelper) {
  tzfree(pHelper->pCompData);
  tdFreeDataCols(pHelper->pDataCols[0]);
  tdFreeDataCols(pHelper->pDataCols[1]);
}

static int tsdbInitHelper(SRWHelper *pHelper, STsdbRepo *pRepo, tsdb_rw_helper_t type) {
  STsdbCfg *pCfg = &pRepo->config;
  memset((void *)pHelper, 0, sizeof(*pHelper));
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  helperType(pHelper) = type;
  helperRepo(pHelper) = pRepo;
  helperState(pHelper) = TSDB_HELPER_CLEAR_STATE;

  // Init file part
  if (tsdbInitHelperFile(pHelper) < 0) goto _err;

  // Init table part
  tsdbInitHelperTable(pHelper);

  // Init block part
  if (tsdbInitHelperBlock(pHelper) < 0) goto _err;

  // TODO: pMeta->maxRowBytes and pMeta->maxCols may change here causing invalid write
  pHelper->pBuffer =
      tmalloc(sizeof(SCompData) + (sizeof(SCompCol) + sizeof(TSCKSUM) + COMP_OVERFLOW_BYTES) * pMeta->maxCols +
              pMeta->maxRowBytes * pCfg->maxRowsPerFileBlock + sizeof(TSCKSUM));
  if (pHelper->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  return 0;

_err:
  tsdbDestroyHelper(pHelper);
  return -1;
}

static int tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, char *content, int32_t len, int8_t comp, int numOfRows,
                                        int maxPoints, char *buffer, int bufferSize) {
  // Verify by checksum
  if (!taosCheckChecksumWhole((uint8_t *)content, len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  // Decode the data
  if (comp) {
    // // Need to decompress
    pDataCol->len = (*(tDataTypeDesc[pDataCol->type].decompFunc))(
        content, len - sizeof(TSCKSUM), numOfRows, pDataCol->pData, pDataCol->spaceSize, comp, buffer, bufferSize);
    if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
      dataColSetOffset(pDataCol, numOfRows);
    }
  } else {
    // No need to decompress, just memcpy it
    pDataCol->len = len - sizeof(TSCKSUM);
    memcpy(pDataCol->pData, content, pDataCol->len);
    if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
      dataColSetOffset(pDataCol, numOfRows);
    }
  }
  return 0;
}

static int tsdbLoadColData(SRWHelper *pHelper, SFile *pFile, SCompBlock *pCompBlock, SCompCol *pCompCol,
                           SDataCol *pDataCol) {
  ASSERT(pDataCol->colId == pCompCol->colId);
  int tsize = pDataCol->bytes * pCompBlock->numOfRows + COMP_OVERFLOW_BYTES;
  pHelper->pBuffer = trealloc(pHelper->pBuffer, pCompCol->len);
  if (pHelper->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pHelper->compBuffer = trealloc(pHelper->compBuffer, tsize);
  if (pHelper->compBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  int64_t offset = pCompBlock->offset + TSDB_GET_COMPCOL_LEN(pCompBlock->numOfCols) + pCompCol->offset;
  if (lseek(pFile->fd, offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tread(pFile->fd, pHelper->pBuffer, pCompCol->len) < pCompCol->len) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pCompCol->len, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tsdbCheckAndDecodeColumnData(pDataCol, pHelper->pBuffer, pCompCol->len, pCompBlock->algorithm,
                                   pCompBlock->numOfRows, pHelper->pRepo->config.maxRowsPerFileBlock,
                                   pHelper->compBuffer, tsizeof(pHelper->compBuffer)) < 0) {
    tsdbError("vgId:%d file %s is broken at column %d offset %" PRId64, REPO_ID(pHelper->pRepo), pFile->fname,
              pCompCol->colId, offset);
    return -1;
  }

  return 0;
}

static int tsdbLoadBlockDataColsImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols, int16_t *colIds, int numOfColIds) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);
  ASSERT(colIds[0] == 0);

  SFile *  pFile = (pCompBlock->last) ? helperLastF(pHelper) : helperDataF(pHelper);
  SCompCol compCol = {0};

  // If only load timestamp column, no need to load SCompData part
  if (numOfColIds > 1 && tsdbLoadCompData(pHelper, pCompBlock, NULL) < 0) goto _err;

  pDataCols->numOfRows = pCompBlock->numOfRows;

  int dcol = 0;
  int ccol = 0;
  for (int i = 0; i < numOfColIds; i++) {
    int16_t   colId = colIds[i];
    SDataCol *pDataCol = NULL;
    SCompCol *pCompCol = NULL;

    while (true) {
      if (dcol >= pDataCols->numOfCols) {
        pDataCol = NULL;
        break;
      }
      pDataCol = &pDataCols->cols[dcol];
      if (pDataCol->colId > colId) {
        pDataCol = NULL;
        break;
      } else {
        dcol++;
        if (pDataCol->colId == colId) break;
      }
    }

    if (pDataCol == NULL) continue;
    ASSERT(pDataCol->colId == colId);

    if (colId == 0) {  // load the key row
      compCol.colId = colId;
      compCol.len = pCompBlock->keyLen;
      compCol.type = pDataCol->type;
      compCol.offset = TSDB_KEY_COL_OFFSET;
      pCompCol = &compCol;
    } else {  // load non-key rows
      while (true) {
        if (ccol >= pCompBlock->numOfCols) {
          pCompCol = NULL;
          break;
        }

        pCompCol = &(pHelper->pCompData->cols[ccol]);
        if (pCompCol->colId > colId) {
          pCompCol = NULL;
          break;
        } else {
          ccol++;
          if (pCompCol->colId == colId) break;
        }
      }

      if (pCompCol == NULL) {
        dataColSetNEleNull(pDataCol, pCompBlock->numOfRows, pDataCols->maxPoints);
        continue;
      }

      ASSERT(pCompCol->colId == pDataCol->colId);
    }

    if (tsdbLoadColData(pHelper, pFile, pCompBlock, pCompCol, pDataCol) < 0) goto _err;
  }

  return 0;

_err:
  return -1;
}

static int tsdbLoadBlockDataImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);

  SFile *pFile = (pCompBlock->last) ? helperLastF(pHelper) : helperDataF(pHelper);

  pHelper->pBuffer = trealloc(pHelper->pBuffer, pCompBlock->len);
  if (pHelper->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  SCompData *pCompData = (SCompData *)pHelper->pBuffer;

  int fd = pFile->fd;
  if (lseek(fd, pCompBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d tid:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
              pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (tread(fd, (void *)pCompData, pCompBlock->len) < pCompBlock->len) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pCompBlock->len,
              pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  int32_t tsize = TSDB_GET_COMPCOL_LEN(pCompBlock->numOfCols);
  if (!taosCheckChecksumWhole((uint8_t *)pCompData, tsize)) {
    tsdbError("vgId:%d file %s block data is corrupted offset %" PRId64 " len %d", REPO_ID(pHelper->pRepo),
              pFile->fname, (int64_t)(pCompBlock->offset), pCompBlock->len);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }
  ASSERT(pCompData->numOfCols == pCompBlock->numOfCols);

  pDataCols->numOfRows = pCompBlock->numOfRows;

  // Recover the data
  int ccol = 0;  // loop iter for SCompCol object
  int dcol = 0;  // loop iter for SDataCols object
  while (dcol < pDataCols->numOfCols) {
    SDataCol *pDataCol = &(pDataCols->cols[dcol]);
    if (ccol >= pCompData->numOfCols) {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pCompBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
      continue;
    }

    int16_t tcolId = 0;
    int32_t toffset = TSDB_KEY_COL_OFFSET;
    int32_t tlen = pCompBlock->keyLen;

    if (dcol != 0) {
      SCompCol *pCompCol = &(pCompData->cols[ccol]);
      tcolId = pCompCol->colId;
      toffset = pCompCol->offset;
      tlen = pCompCol->len;
    } else {
      ASSERT(pDataCol->colId == tcolId);
    }

    if (tcolId == pDataCol->colId) {
      if (pCompBlock->algorithm == TWO_STAGE_COMP) {
        int zsize = pDataCol->bytes * pCompBlock->numOfRows + COMP_OVERFLOW_BYTES;
        if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
          zsize += (sizeof(VarDataLenT) * pCompBlock->numOfRows);
        }
        pHelper->compBuffer = trealloc(pHelper->compBuffer, zsize);
        if (pHelper->compBuffer == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          goto _err;
        }
      }
      if (tsdbCheckAndDecodeColumnData(pDataCol, (char *)pCompData + tsize + toffset, tlen, pCompBlock->algorithm,
                                       pCompBlock->numOfRows, pDataCols->maxPoints, pHelper->compBuffer,
                                       tsizeof(pHelper->compBuffer)) < 0) {
        tsdbError("vgId:%d file %s is broken at column %d block offset %" PRId64 " column offset %d",
                  REPO_ID(pHelper->pRepo), pFile->fname, tcolId, (int64_t)pCompBlock->offset, toffset);
        goto _err;
      }
      if (dcol != 0) ccol++;
      dcol++;
    } else if (tcolId < pDataCol->colId) {
      ccol++;
    } else {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pCompBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
    }
  }

  return 0;

_err:
  return -1;
}

static int tsdbEncodeSCompIdx(void **buf, SCompIdx *pIdx) {
  int tlen = 0;

  tlen += taosEncodeVariantI32(buf, pIdx->tid);
  tlen += taosEncodeVariantU32(buf, pIdx->len);
  tlen += taosEncodeVariantU32(buf, pIdx->offset);
  tlen += taosEncodeFixedU8(buf, pIdx->hasLast);
  tlen += taosEncodeVariantU32(buf, pIdx->numOfBlocks);
  tlen += taosEncodeFixedU64(buf, pIdx->uid);
  tlen += taosEncodeFixedU64(buf, pIdx->maxKey);

  return tlen;
}

static void *tsdbDecodeSCompIdx(void *buf, SCompIdx *pIdx) {
  uint8_t  hasLast = 0;
  uint32_t numOfBlocks = 0;
  uint64_t value = 0;

  if ((buf = taosDecodeVariantI32(buf, &(pIdx->tid))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->len))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->offset))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU8(buf, &(hasLast))) == NULL) return NULL;
  pIdx->hasLast = hasLast;
  if ((buf = taosDecodeVariantU32(buf, &(numOfBlocks))) == NULL) return NULL;
  pIdx->numOfBlocks = numOfBlocks;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->uid = (int64_t)value;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->maxKey = (TSKEY)value;

  return buf;
}

static int tsdbProcessAppendCommit(SRWHelper *pHelper, SCommitIter *pCommitIter, SDataCols *pDataCols, TSKEY maxKey) {
  STsdbCfg * pCfg = &(pHelper->pRepo->config);
  STable *   pTable = pCommitIter->pTable;
  SCompIdx * pIdx = &(pHelper->curCompIdx);
  TSKEY      keyFirst = tsdbNextIterKey(pCommitIter->pIter);
  int        defaultRowsInBlock = pCfg->maxRowsPerFileBlock * 4 / 5;
  SCompBlock compBlock = {0};

  ASSERT(pIdx->len <= 0 || keyFirst > pIdx->maxKey);
  if (pIdx->hasLast) {  // append to with last block
    ASSERT(pIdx->len > 0);
    SCompBlock *pCompBlock = blockAtIdx(pHelper, pIdx->numOfBlocks - 1);
    ASSERT(pCompBlock->last && pCompBlock->numOfRows < pCfg->minRowsPerFileBlock);
    tdResetDataCols(pDataCols);
    int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, maxKey, defaultRowsInBlock - pCompBlock->numOfRows,
                                         pDataCols, NULL, 0);
    ASSERT(rowsRead > 0 && rowsRead == pDataCols->numOfRows);
    if (rowsRead + pCompBlock->numOfRows < pCfg->minRowsPerFileBlock &&
        pCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && !TSDB_NLAST_FILE_OPENED(pHelper)) {
      if (tsdbWriteBlockToFile(pHelper, helperLastF(pHelper), pDataCols, &compBlock, true, false) < 0) return -1;
      if (tsdbAddSubBlock(pHelper, &compBlock, pIdx->numOfBlocks - 1, rowsRead) < 0) return -1;
    } else {
      if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
      ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows);

      if (tdMergeDataCols(pHelper->pDataCols[0], pDataCols, pDataCols->numOfRows) < 0) return -1;
      ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows + pDataCols->numOfRows);

      if (tsdbWriteBlockToProperFile(pHelper, pHelper->pDataCols[0], &compBlock) < 0) return -1;
      if (tsdbUpdateSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks - 1) < 0) return -1;
    }

    if (pHelper->hasOldLastBlock) pHelper->hasOldLastBlock = false;
  } else {
    ASSERT(!pHelper->hasOldLastBlock);
    tdResetDataCols(pDataCols);
    int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, maxKey, defaultRowsInBlock, pDataCols, NULL, 0);
    ASSERT(rowsRead > 0 && rowsRead == pDataCols->numOfRows);

    if (tsdbWriteBlockToProperFile(pHelper, pDataCols, &compBlock) < 0) return -1;
    if (tsdbInsertSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks) < 0) return -1;
  }

#ifndef NDEBUG
  TSKEY keyNext = tsdbNextIterKey(pCommitIter->pIter);
  ASSERT(keyNext < 0 || keyNext > pIdx->maxKey);
#endif

  return 0;
}

static int tsdbProcessMergeCommit(SRWHelper *pHelper, SCommitIter *pCommitIter, SDataCols *pDataCols, TSKEY maxKey,
                                  int *blkIdx) {
  STsdbCfg * pCfg = &(pHelper->pRepo->config);
  STable *   pTable = pCommitIter->pTable;
  SCompIdx * pIdx = &(pHelper->curCompIdx);
  SCompBlock compBlock = {0};
  TSKEY      keyFirst = tsdbNextIterKey(pCommitIter->pIter);
  int        defaultRowsInBlock = pCfg->maxRowsPerFileBlock * 4 / 5;
  SDataCols *pDataCols0 = pHelper->pDataCols[0];

  SSkipListIterator slIter = {0};

  ASSERT(keyFirst <= pIdx->maxKey);

  SCompBlock *pCompBlock = taosbsearch((void *)(&keyFirst), (void *)blockAtIdx(pHelper, *blkIdx),
                                       pIdx->numOfBlocks - *blkIdx, sizeof(SCompBlock), compareKeyBlock, TD_GE);
  ASSERT(pCompBlock != NULL);
  int tblkIdx = TSDB_GET_COMPBLOCK_IDX(pHelper, pCompBlock);

  if (pCompBlock->last) {
    ASSERT(pCompBlock->numOfRows < pCfg->minRowsPerFileBlock && tblkIdx == pIdx->numOfBlocks - 1);
    int16_t colId = 0;
    slIter = *(pCommitIter->pIter);
    if (tsdbLoadBlockDataCols(pHelper, pCompBlock, NULL, &colId, 1) < 0) return -1;
    ASSERT(pDataCols0->numOfRows == pCompBlock->numOfRows);

    int rows1 = defaultRowsInBlock - pCompBlock->numOfRows;
    int rows2 =
        tsdbLoadDataFromCache(pTable, &slIter, maxKey, rows1, NULL, pDataCols0->cols[0].pData, pDataCols0->numOfRows);
    if (rows2 == 0) {  // all data filtered out
      *(pCommitIter->pIter) = slIter;
    } else {
      if (pCompBlock->numOfRows + rows2 < pCfg->minRowsPerFileBlock &&
          pCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && !TSDB_NLAST_FILE_OPENED(pHelper)) {
        tdResetDataCols(pDataCols);
        int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, maxKey, rows1, pDataCols,
                                             pDataCols0->cols[0].pData, pDataCols0->numOfRows);
        ASSERT(rowsRead == rows2 && rowsRead == pDataCols->numOfRows);
        if (tsdbWriteBlockToFile(pHelper, helperLastF(pHelper), pDataCols, &compBlock, true, false) < 0) return -1;
        if (tsdbAddSubBlock(pHelper, &compBlock, tblkIdx, rowsRead) < 0) return -1;
        tblkIdx++;
      } else {
        if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
        int round = 0;
        int dIter = 0;
        while (true) {
          tdResetDataCols(pDataCols);
          int rowsRead =
              tsdbLoadAndMergeFromCache(pDataCols0, &dIter, pCommitIter, pDataCols, maxKey, defaultRowsInBlock);
          if (rowsRead == 0) break;

          if (tsdbWriteBlockToProperFile(pHelper, pDataCols, &compBlock) < 0) return -1;
          if (round == 0) {
            if (tsdbUpdateSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
          } else {
            if (tsdbInsertSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
          }

          tblkIdx++;
          round++;
        }
      }
      if (pHelper->hasOldLastBlock) pHelper->hasOldLastBlock = false;
    }
  } else {
    TSKEY keyLimit = (tblkIdx == pIdx->numOfBlocks - 1) ? maxKey : (pCompBlock[1].keyFirst - 1);
    TSKEY blkKeyFirst = pCompBlock->keyFirst;
    TSKEY blkKeyLast = pCompBlock->keyLast;

    if (keyFirst < blkKeyFirst) {
      while (true) {
        tdResetDataCols(pDataCols);
        int rowsRead =
            tsdbLoadDataFromCache(pTable, pCommitIter->pIter, blkKeyFirst - 1, defaultRowsInBlock, pDataCols, NULL, 0);
        if (rowsRead == 0) break;

        ASSERT(rowsRead == pDataCols->numOfRows);
        if (tsdbWriteBlockToFile(pHelper, helperDataF(pHelper), pDataCols, &compBlock, false, true) < 0) return -1;
        if (tsdbInsertSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
        tblkIdx++;
      }
      ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                              tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
    } else {
      ASSERT(keyFirst <= blkKeyLast);
      int16_t colId = 0;
      if (tsdbLoadBlockDataCols(pHelper, pCompBlock, NULL, &colId, 1) < 0) return -1;

      slIter = *(pCommitIter->pIter);
      int rows1 = (pCfg->maxRowsPerFileBlock - pCompBlock->numOfRows);
      int rows2 = tsdbLoadDataFromCache(pTable, &slIter, blkKeyLast, INT_MAX, NULL, pDataCols0->cols[0].pData,
                                        pDataCols0->numOfRows);

      if (rows2 == 0) {  // all filtered out
        *(pCommitIter->pIter) = slIter;
        ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                                tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
      } else {
        int rows3 = tsdbLoadDataFromCache(pTable, &slIter, keyLimit, INT_MAX, NULL, NULL, 0) + rows2;

        if (pCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && rows1 >= rows2) {
          int rows = (rows1 >= rows3) ? rows3 : rows2;
          tdResetDataCols(pDataCols);
          int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, keyLimit, rows, pDataCols,
                                               pDataCols0->cols[0].pData, pDataCols0->numOfRows);
          ASSERT(rowsRead == rows && rowsRead == pDataCols->numOfRows);
          if (tsdbWriteBlockToFile(pHelper, helperDataF(pHelper), pDataCols, &compBlock, false, false) < 0)
            return -1;
          if (tsdbAddSubBlock(pHelper, &compBlock, tblkIdx, rowsRead) < 0) return -1;
          tblkIdx++;
          ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                                  tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
        } else {
          if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
          int round = 0;
          int dIter = 0;
          while (true) {
            int rowsRead =
                tsdbLoadAndMergeFromCache(pDataCols0, &dIter, pCommitIter, pDataCols, keyLimit, defaultRowsInBlock);
            if (rowsRead == 0) break;

            if (tsdbWriteBlockToFile(pHelper, helperDataF(pHelper), pDataCols, &compBlock, false, true) < 0)
              return -1;
            if (round == 0) {
              if (tsdbUpdateSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
            } else {
              if (tsdbInsertSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
            }

            round++;
            tblkIdx++;
          }
          ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                                  tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
        }
      }
    }
  }

  *blkIdx = tblkIdx;
  return 0;
}

static int tsdbLoadAndMergeFromCache(SDataCols *pDataCols, int *iter, SCommitIter *pCommitIter, SDataCols *pTarget,
                                     TSKEY maxKey, int maxRows) {
  int       numOfRows = 0;
  TSKEY     key1 = INT64_MAX;
  TSKEY     key2 = INT64_MAX;
  STSchema *pSchema = NULL;

  ASSERT(maxRows > 0 && dataColsKeyLast(pDataCols) <= maxKey);
  tdResetDataCols(pTarget);

  while (true) {
    key1 = (*iter >= pDataCols->numOfRows) ? INT64_MAX : dataColsKeyAt(pDataCols, *iter);
    SDataRow row = tsdbNextIterRow(pCommitIter->pIter);
    key2 = (row == NULL || dataRowKey(row) > maxKey) ? INT64_MAX : dataRowKey(row);

    if (key1 == INT64_MAX && key2 == INT64_MAX) break;

    if (key1 <= key2) {
      for (int i = 0; i < pDataCols->numOfCols; i++) {
        dataColAppendVal(pTarget->cols + i, tdGetColDataOfRow(pDataCols->cols + i, *iter), pTarget->numOfRows,
                         pTarget->maxPoints);
      }
      pTarget->numOfRows++;
      (*iter)++;
      if (key1 == key2) tSkipListIterNext(pCommitIter->pIter);
    } else {
      if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
        pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, dataRowVersion(row));
        ASSERT(pSchema != NULL);
      }

      tdAppendDataRowToDataCol(row, pSchema, pTarget);
      tSkipListIterNext(pCommitIter->pIter);
    }

    numOfRows++;
    if (numOfRows >= maxRows) break;
    ASSERT(numOfRows == pTarget->numOfRows && numOfRows <= pTarget->maxPoints);
  }

  return numOfRows;
}

static int tsdbWriteBlockToProperFile(SRWHelper *pHelper, SDataCols *pDataCols, SCompBlock *pCompBlock) {
  STsdbCfg *pCfg = &(pHelper->pRepo->config);
  SFile *   pFile = NULL;
  bool      isLast = false;

  ASSERT(pDataCols->numOfRows > 0);

  if (pDataCols->numOfRows >= pCfg->minRowsPerFileBlock) {
    pFile = helperDataF(pHelper);
  } else {
    isLast = true;
    pFile = TSDB_NLAST_FILE_OPENED(pHelper) ? helperNewLastF(pHelper) : helperLastF(pHelper);
  }

  ASSERT(pFile->fd > 0);

  if (tsdbWriteBlockToFile(pHelper, pFile, pDataCols, pCompBlock, isLast, true) < 0) return -1;

  return 0;
}