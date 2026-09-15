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

// Test scaffolding for the partition operator: fixtures that stage buffer
// pages, and the entry points groupoperatorTest.h exposes to the gtest
// binary. It drives the production functions declared in
// groupoperatorInt.h rather than reimplementing them, so a change to the
// operator shows up here as a compile error instead of silently leaving
// the tests exercising a copy.
//
// The whole file is compiled away without BUILD_TEST, so it costs nothing
// in a normal build even though aux_source_directory always picks it up.

#if defined(BUILD_TEST)

#include "os.h"
#include "taoserror.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"

#include "executorInt.h"
#include "groupoperatorInt.h"
#include "groupoperatorTest.h"
#include "operator.h"
#include "querytask.h"
#include "thash.h"

// One-shot failure injection. The two consume helpers are called from the
// production seams in groupoperator.c, so the tests drive the real error
// paths; both disarm on first use, so a helper only affects its own call.
static int64_t tsSpillTestFailWriteOffset = -1;
static int64_t tsSpillTestPhysicalSizeAtWriteFailure = -1;
static int32_t tsSpillTestFailRecycleAt = -1;

bool partitionSpillTestConsumeWriteFailure(TdFilePtr pFile, int64_t offset,
                                           const void* pData, int32_t length) {
  if (tsSpillTestFailWriteOffset != offset) {
    return false;
  }
  tsSpillTestFailWriteOffset = -1;
  int32_t partialLength = TMIN(length, (int32_t)sizeof(int32_t));
  int64_t written = 0;
  if (partialLength > 0 && taosLSeekFile(pFile, offset, SEEK_SET) >= 0) {
    written = taosWriteFile(pFile, pData, partialLength);
  }
  // Recorded here rather than at the seam so the physical size at the
  // moment of failure stays observable without exporting the variable.
  // Leaving the -1 sentinel when the torn write or the stat itself fails
  // keeps the caller's assertions failing rather than reading a stale size.
  if (written != partialLength ||
      taosFStatFile(pFile, &tsSpillTestPhysicalSizeAtWriteFailure, NULL) !=
          TSDB_CODE_SUCCESS) {
    tsSpillTestPhysicalSizeAtWriteFailure = -1;
  }
  return true;
}

bool partitionSpillTestConsumeRecycleFailure(int32_t pageIndex) {
  if (tsSpillTestFailRecycleAt != pageIndex) {
    return false;
  }
  tsSpillTestFailRecycleAt = -1;
  return true;
}

// Mirrors destroyPartitionOperatorInfo: numPageClasses counts the classes
// whose buffers exist, so every helper that builds classes tears them down
// through this loop rather than naming class 0.
static void partitionSpillTestDestroyPageClasses(
    SPartitionOperatorInfo* pInfo) {
  for (int32_t i = 0; i < pInfo->numPageClasses; ++i) {
    destroyDiskbasedBuf(pInfo->pageClasses[i].pBuf);
    pInfo->pageClasses[i].pBuf = NULL;
    taosMemoryFreeClear(pInfo->pageClasses[i].columnOffset);
  }
  pInfo->numPageClasses = 0;
}

static void partitionSpillTestCleanupInfo(SPartitionOperatorInfo* pInfo,
                                          SPartitionDataInfo*     pGroup) {
  tsSpillTestFailWriteOffset = -1;
  tsSpillTestPhysicalSizeAtWriteFailure = -1;
  tsSpillTestFailRecycleAt = -1;
  cleanupPartitionGroupInfo(pGroup);
  partitionFreeSpillResources(&pInfo->spill);
  partitionSpillTestDestroyPageClasses(pInfo);
  blockDataDestroy(pInfo->binfo.pRes);
}

// Builds one size class with the same state shape production gives it, and
// publishes it by raising numPageClasses only once the buffer exists, so the
// teardown loop never sees a half-built class.
static int32_t partitionSpillTestInitPageClass(SPartitionOperatorInfo* pInfo,
                                               int32_t classIndex,
                                               int32_t pageSize,
                                               int32_t rowCapacity,
                                               int32_t numPages) {
  SPartitionPageClass* pClass = &pInfo->pageClasses[classIndex];
  pClass->pageSize = pageSize;
  pClass->rowCapacity = rowCapacity;
  pClass->columnOffset = setupColumnOffset(pInfo->binfo.pRes, rowCapacity);
  if (pClass->columnOffset == NULL) {
    return terrno;
  }

  int32_t code = createDiskbasedBuf(&pClass->pBuf, pageSize,
                                    (int64_t)pageSize * (numPages + 2),
                                    "partition-spill-test", TD_TMP_DIR_PATH);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(pClass->columnOffset);
    return code;
  }
  pInfo->numPageClasses = classIndex + 1;
  return TSDB_CODE_SUCCESS;
}

// Fixed stand-in task so GET_TASKID(pInfo->pTaskInfo) in production error
// logs has something to print instead of dereferencing NULL. Read-only,
// shared across tests: single-threaded gtest never mutates it concurrently.
static SExecTaskInfo tsSpillTestTaskInfo = {.id = {.str = "partitionSpillTest"}};

static int32_t partitionSpillTestCreateInfo(SPartitionOperatorInfo* pInfo,
                                            int32_t pageSize, int32_t numPages,
                                            bool openSpillFile) {
  if (pInfo->pTaskInfo == NULL) {
    pInfo->pTaskInfo = &tsSpillTestTaskInfo;
  }
  int32_t code = createDataBlock(&pInfo->binfo.pRes);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData column =
      createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  code = blockDataAppendColInfo(pInfo->binfo.pRes, &column);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // Production always has at least one class, so the scaffolding must too:
  // page refs, budget accounting and teardown all key off it.
  code = partitionSpillTestInitPageClass(pInfo, 0, pageSize, 32, numPages);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pInfo->memBudgetBytes = (int64_t)pageSize * TMAX(1, numPages);

  if (!openSpillFile) {
    return TSDB_CODE_SUCCESS;
  }

  char path[PATH_MAX] = {0};
  taosGetTmpfilePath(TD_TMP_DIR_PATH, "partition-spill-test", path);
  pInfo->spill.pFile =
      taosOpenFile(path, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE |
                             TD_FILE_TRUNC | TD_FILE_AUTO_DEL);
  return pInfo->spill.pFile != NULL ? TSDB_CODE_SUCCESS : terrno;
}

// Stages one page in the layout doHashPartition produces. Rows whose bit
// is set in nullRowMask are written as NULL: the bitmap bit is set and the
// payload slot stays zeroed, and columnLength still covers every row -
// exactly what doHashPartition does, since a fixed column reserves its
// bytes whether or not the row is NULL.
static int32_t partitionSpillTestAddPage(SPartitionOperatorInfo* pInfo,
                                         SPartitionDataInfo*     pGroup,
                                         int32_t pageIndex, int32_t rows,
                                         uint32_t nullRowMask,
                                         int32_t* pPageId) {
  void* pPage = getNewBufPage(pInfo->pageClasses[0].pBuf, pPageId);
  if (pPage == NULL) {
    return terrno;
  }

  memset(pPage, 0, getBufPageSize(pInfo->pageClasses[0].pBuf));
  memcpy(pPage, &rows, sizeof(rows));
  memcpy((char*)pPage + sizeof(rows), &pGroup->groupId,
         sizeof(pGroup->groupId));

  char*   pBitmap = (char*)pPage + pInfo->pageClasses[0].columnOffset[0];
  int32_t bitmapSize = BitmapLen(pInfo->pageClasses[0].rowCapacity);
  int32_t columnLength = rows * sizeof(int32_t);
  memcpy(pBitmap + bitmapSize, &columnLength, sizeof(columnLength));

  char* pData = pBitmap + bitmapSize + sizeof(columnLength);
  for (int32_t rowIndex = 0; rowIndex < rows; ++rowIndex) {
    if ((nullRowMask & (1u << rowIndex)) != 0) {
      colDataSetNull_f(pBitmap, rowIndex);
      continue;
    }
    int32_t value = pageIndex * 1000 + rowIndex;
    memcpy(pData + rowIndex * sizeof(value), &value, sizeof(value));
  }

  if (taosArrayPush(pGroup->pPageList, pPageId) == NULL) {
    releaseBufPage(pInfo->pageClasses[0].pBuf, pPage);
    return terrno;
  }

  setBufPageDirty(pPage, true);
  releaseBufPage(pInfo->pageClasses[0].pBuf, pPage);
  pGroup->residentRows += rows;
  // Mirror what partitionGetNewPage records, so the byte budget the spill
  // path reads stays consistent with the pages staged here.
  pInfo->numResidentPages += 1;
  pInfo->residentBytes += pInfo->pageClasses[0].pageSize;
  pInfo->spillStat.residentRows += rows;
  return TSDB_CODE_SUCCESS;
}

// Creates the operator scaffolding, the group's page list, and one staged
// page per pRowsPerPage entry.
static int32_t partitionSpillTestSetupGroup(SPartitionOperatorInfo* pInfo,
                                            SPartitionDataInfo* pGroup,
                                            const int32_t* pRowsPerPage,
                                            int32_t numPages,
                                            uint32_t nullRowMask,
                                            bool openSpillFile) {
  int32_t code = partitionSpillTestCreateInfo(pInfo, 1024, numPages,
                                              openSpillFile);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pGroup->pPageList = taosArrayInit(numPages, sizeof(int32_t));
  if (pGroup->pPageList == NULL) {
    return terrno;
  }
  int32_t pageId = 0;
  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    code = partitionSpillTestAddPage(pInfo, pGroup, pageIndex,
                                     pRowsPerPage[pageIndex], nullRowMask,
                                     &pageId);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionSpillTestCollectDecodedBlock(
    SPartitionOperatorInfo* pInfo, int64_t* pRows, int64_t* pValueSum,
    int32_t* pFirstValue) {
  SSDataBlock* pRes = pInfo->binfo.pRes;
  if (pRes == NULL || pRes->info.rows <= 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SColumnInfoData* pColumn = taosArrayGet(pRes->pDataBlock, 0);
  if (pColumn == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (pFirstValue != NULL) {
    memcpy(pFirstValue, colDataGetData(pColumn, 0), sizeof(*pFirstValue));
  }
  for (int32_t rowIndex = 0; rowIndex < pRes->info.rows; ++rowIndex) {
    int32_t value = 0;
    memcpy(&value, colDataGetData(pColumn, rowIndex), sizeof(value));
    *pValueSum += value;
  }
  *pRows += pRes->info.rows;
  return TSDB_CODE_SUCCESS;
}

// Replays every fragment through the production reader, accumulating the
// decoded row count and value sum.
static int32_t partitionSpillTestReadAllFragments(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup,
    int64_t* pRows, int64_t* pValueSum) {
  while (pInfo->spill.fragmentIndex < taosArrayGetSize(pGroup->fragments)) {
    int32_t code = partitionReadFragmentBlock(pInfo, pGroup);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = partitionSpillTestCollectDecodedBlock(pInfo, pRows, pValueSum,
                                                 NULL);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void partitionSpillTestCaptureCursor(
    const SPartitionSpillInfo* pSpill, SPartitionSpillTestCursorState* pState) {
  pState->fragmentIndex = pSpill->fragmentIndex;
  pState->readOffset = pSpill->fragmentReadOffset;
  pState->decodedRows = pSpill->fragmentDecodedRows;
}

static int32_t partitionSpillTestSnapshotState(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup,
    SPartitionSpillTestState* pState) {
  memset(pState, 0, sizeof(*pState));
  pState->logicalFileSize = pInfo->spill.fileSize;
  if (pInfo->spill.pFile != NULL) {
    int32_t code =
        taosFStatFile(pInfo->spill.pFile, &pState->physicalFileSize, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  pState->fragmentCount =
      pGroup->fragments != NULL ? taosArrayGetSize(pGroup->fragments) : 0;
  pState->groupPages =
      pGroup->pPageList != NULL ? taosArrayGetSize(pGroup->pPageList) : 0;
  pState->groupRows = pGroup->residentRows;
  pState->residentPages = pInfo->numResidentPages;
  pState->spillCount = pInfo->spillStat.spillCount;
  pState->spillFailures = pInfo->spillStat.spillFailures;
  pState->spillPages = pInfo->spillStat.spillPages;
  pState->spillRows = pInfo->spillStat.spillRows;
  pState->spillBytes = pInfo->spillStat.spillBytes;
  pState->residentRows = pInfo->spillStat.residentRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionSpillTestResidentValueSum(SPartitionOperatorInfo* pInfo,
                                                  SPartitionDataInfo* pGroup,
                                                  int64_t* pValueSum) {
  *pValueSum = 0;
  int32_t numPages = taosArrayGetSize(pGroup->pPageList);
  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    int32_t* pPageId = taosArrayGet(pGroup->pPageList, pageIndex);
    void*    pPage = getBufPage(pInfo->pageClasses[0].pBuf, *pPageId);
    if (pPage == NULL) {
      return terrno;
    }

    int32_t rows = *(int32_t*)pPage;
    if (rows < 0 || rows > pInfo->pageClasses[0].rowCapacity) {
      releaseBufPage(pInfo->pageClasses[0].pBuf, pPage);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    char* pData =
        (char*)pPage + pInfo->pageClasses[0].columnOffset[0] +
        BitmapLen(pInfo->pageClasses[0].rowCapacity) + sizeof(int32_t);
    for (int32_t rowIndex = 0; rowIndex < rows; ++rowIndex) {
      int32_t value = 0;
      memcpy(&value, pData + rowIndex * sizeof(value), sizeof(value));
      *pValueSum += value;
    }
    releaseBufPage(pInfo->pageClasses[0].pBuf, pPage);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionSpillTestReadFragment(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup,
    SPartitionSpillTestResult* pResult) {
  if (taosArrayGetSize(pGroup->fragments) != 1) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SPartitionFragment* pFragment = taosArrayGet(pGroup->fragments, 0);
  int32_t             code = TSDB_CODE_SUCCESS;

  pResult->fragmentOffset = pFragment->offset;
  pResult->fragmentLength = pFragment->length;
  pResult->fragmentNumRows = pFragment->numRows;

  // Structural scan for the compactness assertions: walk the length
  // prefixes without decoding any payload. Decoding is deliberately left to
  // the production reader below so the test exercises the real read path.
  int64_t cursor = pFragment->offset;
  int64_t fragmentEnd = pFragment->offset + pFragment->length;
  while (cursor < fragmentEnd) {
    int32_t blockSize = 0;
    code = partitionReadAt(pInfo->spill.pFile, cursor, &blockSize,
                           sizeof(blockSize));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    cursor += sizeof(blockSize);
    if (blockSize <= 0 || cursor + blockSize > fragmentEnd ||
        pResult->blockCount >= PARTITION_SPILL_TEST_MAX_PAGES) {
      return TSDB_CODE_FILE_CORRUPTED;
    }
    pResult->blockSizes[pResult->blockCount] = blockSize;
    pResult->blockCount += 1;
    cursor += blockSize;
  }

  // Round-trip through partitionReadFragmentBlock so row reconciliation,
  // cursor advancement and reset, and load statistics run exactly as in
  // production.
  partitionSpillTestResetIoCalls();
  int32_t blockIndex = 0;
  while (pInfo->spill.fragmentIndex < taosArrayGetSize(pGroup->fragments)) {
    code = partitionReadFragmentBlock(pInfo, pGroup);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (blockIndex >= pResult->blockCount) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    SSDataBlock* pRes = pInfo->binfo.pRes;
    pResult->decodedRows[blockIndex] = pRes->info.rows;
    SColumnInfoData* pColumn = taosArrayGet(pRes->pDataBlock, 0);
    if (pColumn == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    for (int32_t rowIndex = 0; rowIndex < pRes->info.rows; ++rowIndex) {
      int32_t value = 0;
      memcpy(&value, colDataGetData(pColumn, rowIndex), sizeof(value));
      pResult->decodedValueSum += value;
    }
    blockIndex += 1;
  }

  partitionSpillTestGetIoCalls(NULL, &pResult->readCallCount);
  pResult->loadCount = pInfo->spillStat.loadCount;
  pResult->loadBytes = pInfo->spillStat.loadBytes;
  return TSDB_CODE_SUCCESS;
}

int32_t partitionSpillTestSelectSequence(const SPartitionSpillTestGroup* pGroups, int32_t numGroups,
                                         uint64_t* pSelectedGroupIds, int32_t numSelections) {
  if (pGroups == NULL || numGroups <= 0 || pSelectedGroupIds == NULL || numSelections <= 0 ||
      numSelections > numGroups) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  SPartitionOperatorInfo info = {0};
  _hash_fn_t             hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  SHashObj*              pGroupSet = taosHashInit(numGroups, hashFn, false, HASH_NO_LOCK);
  if (pGroupSet == NULL) {
    return terrno;
  }
  info.pGroupSet = pGroupSet;

  for (int32_t groupIndex = 0; groupIndex < numGroups; ++groupIndex) {
    SPartitionDataInfo group = {
        .groupId = pGroups[groupIndex].groupId,
        .numOfRows = pGroups[groupIndex].numOfRows,
        .residentRows = pGroups[groupIndex].residentRows,
    };
    group.pPageList = taosArrayInit(1, sizeof(int32_t));
    if (group.pPageList == NULL) {
      code = terrno;
      break;
    }

    if (pGroups[groupIndex].growLevel >= 0) {
      // Keep the staged ref's class in step with the bucket level below.
      int32_t pageRef =
          partitionEncodePageRef(pGroups[groupIndex].growLevel, 0);
      if (taosArrayPush(group.pPageList, &pageRef) == NULL) {
        code = terrno;
      }
    }
    if (code != TSDB_CODE_SUCCESS) {
      cleanupPartitionGroupInfo(&group);
      break;
    }

    code = taosHashPut(pGroupSet, &group.groupId, sizeof(group.groupId), &group,
                       sizeof(group));
    if (code != TSDB_CODE_SUCCESS) {
      cleanupPartitionGroupInfo(&group);
      break;
    }

    SPartitionDataInfo* pStored =
        taosHashGet(pGroupSet, &group.groupId, sizeof(group.groupId));
    if (pStored == NULL) {
      code = terrno != TSDB_CODE_SUCCESS
                 ? terrno
                 : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      break;
    }
    if (pGroups[groupIndex].growLevel >= 0) {
      code = partitionSetSpillGroupLevel(
          &info, pStored, pGroups[groupIndex].growLevel);
    }
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    for (int32_t index = 0; index < numSelections; ++index) {
      SPartitionDataInfo* pSelected = partitionSelectSpillGroup(&info);
      if (pSelected == NULL) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        break;
      }
      pSelectedGroupIds[index] = pSelected->groupId;

      // Model the metric changes committed by partitionSpillGroup without
      // needing real buffer pages. The next selection must use the adjusted
      // bucket index instead of returning the previous victim again.
      taosArrayClear(pSelected->pPageList);
      pSelected->residentRows = 0;
      partitionRemoveSpillGroup(&info, pSelected);
    }
  }

  partitionClearSpillBuckets(&info);
  partitionCleanupGroupSet(pGroupSet);
  taosHashCleanup(pGroupSet);
  return code;
}

int32_t partitionSpillTestSelectGroup(const SPartitionSpillTestGroup* pGroups, int32_t numGroups,
                                      uint64_t* pSelectedGroupId) {
  return partitionSpillTestSelectSequence(pGroups, numGroups, pSelectedGroupId, 1);
}

int64_t partitionSpillTestBatchTargetBytes(int64_t residentBytes) {
  return partitionGetSpillBatchTargetBytes(residentBytes);
}

static int32_t partitionSpillTestRunImpl(
    const int32_t* pRowsPerPage, int32_t numPages, int32_t writeBatchLimit,
    SPartitionSpillTestResult* pResult) {
  if (pRowsPerPage == NULL || numPages <= 0 ||
      numPages > PARTITION_SPILL_TEST_MAX_PAGES || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  const int32_t          pageSize = 1024;
  int32_t                code = TSDB_CODE_SUCCESS;
  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     group = {.groupId = 1001};
  memset(pResult, 0, sizeof(*pResult));

  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    if (pRowsPerPage[pageIndex] < 0 || pRowsPerPage[pageIndex] > 32) {
      return TSDB_CODE_INVALID_PARA;
    }
    pResult->inputRowCount += pRowsPerPage[pageIndex];
  }
  pResult->pageSize = pageSize;
  pResult->inputPageCount = numPages;

  code = partitionSpillTestSetupGroup(&info, &group, pRowsPerPage, numPages,
                                      0, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  partitionSpillTestResetIoCalls();
  partitionSpillTestSetWriteBatchLimit(writeBatchLimit);
  code = partitionSpillGroup(&info, &group);
  partitionSpillTestSetWriteBatchLimit(0);
  partitionSpillTestGetIoCalls(&pResult->writeCallCount, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  pResult->fragmentCount = taosArrayGetSize(group.fragments);
  pResult->logicalFileSize = info.spill.fileSize;
  code = taosFStatFile(info.spill.pFile, &pResult->physicalFileSize, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  if (pResult->fragmentCount > 0) {
    code = partitionSpillTestReadFragment(&info, &group, pResult);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }

  pResult->remainingGroupPages = taosArrayGetSize(group.pPageList);
  pResult->remainingGroupRows = group.residentRows;
  pResult->remainingResidentPages = info.numResidentPages;
  pResult->spillCount = info.spillStat.spillCount;
  pResult->spillPages = info.spillStat.spillPages;
  pResult->spillRows = info.spillStat.spillRows;
  pResult->spillBytes = info.spillStat.spillBytes;

_cleanup:
  partitionSpillTestSetWriteBatchLimit(0);
  partitionSpillTestCleanupInfo(&info, &group);
  return code;
}

int32_t partitionSpillTestRun(const int32_t* pRowsPerPage, int32_t numPages,
                              SPartitionSpillTestResult* pResult) {
  return partitionSpillTestRunImpl(pRowsPerPage, numPages, 0, pResult);
}

int32_t partitionSpillTestRunWithWriteLimit(
    const int32_t* pRowsPerPage, int32_t numPages, int32_t writeBatchLimit,
    SPartitionSpillTestResult* pResult) {
  if (writeBatchLimit <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return partitionSpillTestRunImpl(pRowsPerPage, numPages, writeBatchLimit,
                                   pResult);
}

int32_t partitionSpillTestBatchWrite(bool failWrite, SPartitionSpillTestBatchWriteResult* pResult) {
  if (pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     groups[PARTITION_SPILL_TEST_BATCH_GROUPS] = {0};
  memset(pResult, 0, sizeof(*pResult));

  code = partitionSpillTestCreateInfo(&info, 1024, PARTITION_SPILL_TEST_BATCH_GROUPS, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  for (int32_t groupIndex = 0; groupIndex < PARTITION_SPILL_TEST_BATCH_GROUPS; ++groupIndex) {
    groups[groupIndex].groupId = 1001 + groupIndex;
    groups[groupIndex].pPageList = taosArrayInit(1, sizeof(int32_t));
    if (groups[groupIndex].pPageList == NULL) {
      code = terrno;
      goto _cleanup;
    }

    int32_t pageId = 0;
    code = partitionSpillTestAddPage(&info, &groups[groupIndex], groupIndex, 1, 0, &pageId);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
    code = partitionSetSpillGroupLevel(&info, &groups[groupIndex], 0);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }

  partitionSpillTestResetIoCalls();
  if (failWrite) {
    tsSpillTestFailWriteOffset = 0;
  }
  pResult->spillCode = partitionSpillGroupBatch(&info);
  tsSpillTestFailWriteOffset = -1;
  pResult->physicalFileSizeAtFailure = tsSpillTestPhysicalSizeAtWriteFailure;
  partitionSpillTestGetIoCalls(&pResult->writeCallCount, NULL);

  pResult->spillCount = info.spillStat.spillCount;
  pResult->spillFailures = info.spillStat.spillFailures;
  pResult->remainingResidentPages = info.numResidentPages;
  pResult->logicalFileSize = info.spill.fileSize;
  code = taosFStatFile(info.spill.pFile, &pResult->physicalFileSize, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  for (int32_t groupIndex = 0; groupIndex < PARTITION_SPILL_TEST_BATCH_GROUPS; ++groupIndex) {
    SPartitionDataInfo* pGroup = &groups[groupIndex];
    pResult->groupPages[groupIndex] = taosArrayGetSize(pGroup->pPageList);
    pResult->groupRows[groupIndex] = pGroup->residentRows;
    pResult->fragmentCounts[groupIndex] = pGroup->fragments != NULL ? taosArrayGetSize(pGroup->fragments) : 0;
    if (pResult->fragmentCounts[groupIndex] > 0) {
      SPartitionFragment* pFragment = taosArrayGet(pGroup->fragments, 0);
      pResult->fragmentOffsets[groupIndex] = pFragment->offset;
      pResult->fragmentLengths[groupIndex] = pFragment->length;
      pResult->fragmentRows[groupIndex] = pFragment->numRows;
    }
  }

  if (pResult->spillCode == TSDB_CODE_SUCCESS) {
    for (int32_t groupIndex = 0; groupIndex < 2; ++groupIndex) {
      info.spill.fragmentIndex = 0;
      info.spill.fragmentReadOffset = 0;
      info.spill.fragmentDecodedRows = 0;
      code = partitionSpillTestReadAllFragments(&info, &groups[groupIndex], &pResult->decodedRows,
                                                &pResult->decodedValueSum);
      if (code != TSDB_CODE_SUCCESS) {
        goto _cleanup;
      }
    }
  }

_cleanup:
  tsSpillTestFailWriteOffset = -1;
  tsSpillTestPhysicalSizeAtWriteFailure = -1;
  partitionClearSpillBuckets(&info);
  for (int32_t groupIndex = 0; groupIndex < PARTITION_SPILL_TEST_BATCH_GROUPS; ++groupIndex) {
    cleanupPartitionGroupInfo(&groups[groupIndex]);
  }
  partitionFreeSpillResources(&info.spill);
  partitionSpillTestDestroyPageClasses(&info);
  blockDataDestroy(info.binfo.pRes);
  return code;
}

int32_t partitionSpillTestMultipleFragments(
    const int32_t* pRowsPerPage, int32_t firstNumPages, int32_t secondNumPages,
    bool                                    corruptSecondFragment,
    SPartitionSpillTestMultiFragmentResult* pResult) {
  if (pRowsPerPage == NULL || firstNumPages <= 0 || secondNumPages <= 0 ||
      firstNumPages > PARTITION_SPILL_TEST_MAX_PAGES - secondNumPages ||
      pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t numPages = firstNumPages + secondNumPages;
  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    if (pRowsPerPage[pageIndex] <= 0 || pRowsPerPage[pageIndex] > 32) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                pageId = 0;
  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     group = {.groupId = 1001};
  memset(pResult, 0, sizeof(*pResult));

  code = partitionSpillTestCreateInfo(&info, 1024, numPages, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  group.pPageList = taosArrayInit(numPages, sizeof(int32_t));
  if (group.pPageList == NULL) {
    code = terrno;
    goto _cleanup;
  }

  for (int32_t pageIndex = 0; pageIndex < firstNumPages; ++pageIndex) {
    code = partitionSpillTestAddPage(&info, &group, pageIndex,
                                     pRowsPerPage[pageIndex], 0, &pageId);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }
  code = partitionSpillGroup(&info, &group);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  for (int32_t pageIndex = firstNumPages; pageIndex < numPages; ++pageIndex) {
    code = partitionSpillTestAddPage(&info, &group, pageIndex,
                                     pRowsPerPage[pageIndex], 0, &pageId);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }
  code = partitionSpillGroup(&info, &group);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  pResult->fragmentCount = taosArrayGetSize(group.fragments);
  if (pResult->fragmentCount != PARTITION_SPILL_TEST_FRAGMENT_BATCHES) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  for (int32_t fragmentIndex = 0;
       fragmentIndex < PARTITION_SPILL_TEST_FRAGMENT_BATCHES; ++fragmentIndex) {
    SPartitionFragment* pStored = taosArrayGet(group.fragments, fragmentIndex);
    if (pStored == NULL) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _cleanup;
    }
    pResult->fragmentOffsets[fragmentIndex] = pStored->offset;
    pResult->fragmentLengths[fragmentIndex] = pStored->length;
    pResult->fragmentNumRows[fragmentIndex] = pStored->numRows;
  }
  pResult->logicalFileSize = info.spill.fileSize;
  code = taosFStatFile(info.spill.pFile, &pResult->physicalFileSize, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  if (corruptSecondFragment) {
    // The recorded row total drives the reader's last-block reconciliation:
    // bumping it must make fragment 1 unreadable while fragment 0, whose
    // record is untouched, still replays cleanly.
    SPartitionFragment* pStored = taosArrayGet(group.fragments, 1);
    if (pStored == NULL) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _cleanup;
    }
    pStored->numRows += 1;
  }

  pResult->readCode = TSDB_CODE_SUCCESS;
  while (info.spill.fragmentIndex < pResult->fragmentCount) {
    int32_t previousFragment = info.spill.fragmentIndex;
    int32_t readCode = partitionReadFragmentBlock(&info, &group);
    if (readCode != TSDB_CODE_SUCCESS) {
      pResult->readCode = readCode;
      break;
    }

    if (pResult->decodedBlockCount >= PARTITION_SPILL_TEST_MAX_PAGES) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _cleanup;
    }
    int32_t blockIndex = pResult->decodedBlockCount;
    pResult->decodedBlockRows[blockIndex] = info.binfo.pRes->info.rows;
    code = partitionSpillTestCollectDecodedBlock(
        &info, &pResult->decodedRows, &pResult->decodedValueSum,
        &pResult->decodedFirstValues[blockIndex]);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
    pResult->decodedBlockCount += 1;

    if (previousFragment == 0 && info.spill.fragmentIndex == 1) {
      partitionSpillTestCaptureCursor(&info.spill,
                                      &pResult->afterFirstFragment);
    }
  }
  partitionSpillTestCaptureCursor(&info.spill, &pResult->afterRead);
  pResult->loadCount = info.spillStat.loadCount;
  pResult->loadBytes = info.spillStat.loadBytes;

_cleanup:
  partitionSpillTestCleanupInfo(&info, &group);
  return code;
}

int32_t partitionSpillTestEncryption(
    int32_t rows, SPartitionSpillTestEncryptionResult* pResult) {
  if (rows <= 0 || rows > 32 || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                pageId = 0;
  EEncryptScope          savedScope = tsiEncryptScope;
  char                   savedTempDir[PATH_MAX] = {0};
  char                   testTempDir[PATH_MAX] = {0};
  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     group = {.groupId = 1001};
  memcpy(savedTempDir, tsTempDir, sizeof(savedTempDir));
  memset(pResult, 0, sizeof(*pResult));

  // Keep the generic disk buffer unencrypted so this test isolates the
  // partition-owned file created by partitionEnsureSpillFile.
  tsiEncryptScope = (EEncryptScope)(savedScope & ~DND_CS_QUERY_SPILL);
  code = partitionSpillTestCreateInfo(&info, 1024, 1, false);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  group.pPageList = taosArrayInit(1, sizeof(int32_t));
  if (group.pPageList == NULL) {
    code = terrno;
    goto _cleanup;
  }
  code = partitionSpillTestAddPage(&info, &group, 0, rows, 0, &pageId);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  int32_t pathLength = snprintf(
      testTempDir, sizeof(testTempDir), "%s/partition_spill_gtest_%d_%" PRId64,
      TD_TMP_DIR_PATH, taosGetPId(), taosGetTimestampUs());
  if (pathLength < 0 || pathLength >= (int32_t)sizeof(testTempDir)) {
    code = TSDB_CODE_INVALID_PARA;
    goto _cleanup;
  }
  code = taosMulMkDir(testTempDir);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  tstrncpy(tsTempDir, testTempDir, PATH_MAX);
  tsiEncryptScope = (EEncryptScope)(savedScope | DND_CS_QUERY_SPILL);
  code = partitionSpillGroup(&info, &group);

  // The context is already attached to the file, so restore process-global
  // configuration before inspecting or reading the fragment.
  tsiEncryptScope = savedScope;
  memcpy(tsTempDir, savedTempDir, sizeof(savedTempDir));
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  pResult->cryptContextCreated = info.spill.pCryptCtx != NULL;
  pResult->fragmentCount = taosArrayGetSize(group.fragments);
  SPartitionFragment* pFragment = taosArrayGet(group.fragments, 0);
  if (pFragment == NULL) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  pResult->fragmentNumRows = pFragment->numRows;
  pResult->logicalFileSize = info.spill.fileSize;
  code = taosFStatFile(info.spill.pFile, &pResult->physicalFileSize, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  code = partitionReadAt(info.spill.pFile, pFragment->offset,
                         &pResult->encryptedBlockSize,
                         sizeof(pResult->encryptedBlockSize));
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  size_t plainBlockSize = blockDataGetBufSize(info.spill.pBlock);
  if (plainBlockSize > INT32_MAX) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  pResult->plainBlockSize = (int32_t)plainBlockSize;
  pResult->encryptedSpillCount = info.spillStat.encryptedSpillCount;

  code = partitionSpillTestReadAllFragments(&info, &group,
                                            &pResult->readBackRows,
                                            &pResult->decodedValueSum);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  pResult->loadCount = info.spillStat.loadCount;
  pResult->loadBytes = info.spillStat.loadBytes;

_cleanup:
  tsiEncryptScope = savedScope;
  memcpy(tsTempDir, savedTempDir, sizeof(savedTempDir));
  pResult->scopeRestored = tsiEncryptScope == savedScope;
  partitionSpillTestCleanupInfo(&info, &group);
  if (testTempDir[0] != 0) {
    taosRemoveDir(testTempDir);
  }
  return code;
}

int32_t partitionSpillTestRollback(int32_t committedRows, int32_t pendingRows,
                                   SPartitionSpillTestRollbackResult* pResult) {
  if (committedRows <= 0 || committedRows > 32 || pendingRows <= 0 ||
      pendingRows > 32 || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                pageId = 0;
  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     group = {.groupId = 1001};
  SPartitionFragment     fragmentBefore = {0};
  SPartitionFragment     fragmentAfter = {0};
  memset(pResult, 0, sizeof(*pResult));

  code = partitionSpillTestCreateInfo(&info, 1024, 2, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  group.pPageList = taosArrayInit(2, sizeof(int32_t));
  if (group.pPageList == NULL) {
    code = terrno;
    goto _cleanup;
  }
  code = partitionSpillTestAddPage(&info, &group, 0, committedRows, 0, &pageId);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  code = partitionSpillGroup(&info, &group);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  code = partitionSpillTestAddPage(&info, &group, 1, pendingRows, 0, &pageId);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  SPartitionFragment* pFragment = taosArrayGet(group.fragments, 0);
  if (pFragment == NULL) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  fragmentBefore = *pFragment;
  pResult->fragmentOffsetBefore = fragmentBefore.offset;
  pResult->fragmentLengthBefore = fragmentBefore.length;
  code = partitionSpillTestSnapshotState(&info, &group, &pResult->before);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  code = partitionSpillTestResidentValueSum(&info, &group,
                                            &pResult->residentValueSumBefore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  // The hook writes a partial combined record before reporting failure, which
  // proves rollback truncates real appended bytes rather than an untouched
  // gap.
  tsSpillTestFailWriteOffset = info.spill.fileSize;
  pResult->spillCode = partitionSpillGroup(&info, &group);
  tsSpillTestFailWriteOffset = -1;
  pResult->physicalFileSizeAtFailure = tsSpillTestPhysicalSizeAtWriteFailure;

  code = partitionSpillTestSnapshotState(&info, &group, &pResult->after);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  pFragment = taosArrayGet(group.fragments, 0);
  if (pFragment == NULL) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  fragmentAfter = *pFragment;
  pResult->fragmentOffsetAfter = fragmentAfter.offset;
  pResult->fragmentLengthAfter = fragmentAfter.length;
  code = partitionSpillTestResidentValueSum(&info, &group,
                                            &pResult->residentValueSumAfter);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  code = partitionSpillTestReadAllFragments(&info, &group,
                                            &pResult->readBackRows,
                                            &pResult->decodedValueSum);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

_cleanup:
  tsSpillTestFailWriteOffset = -1;
  tsSpillTestPhysicalSizeAtWriteFailure = -1;
  partitionSpillTestCleanupInfo(&info, &group);
  return code;
}

int32_t partitionSpillTestCorruptRead(const int32_t* pRowsPerPage,
                                      int32_t numPages, int32_t corruption) {
  if (pRowsPerPage == NULL || numPages <= 0 ||
      numPages > PARTITION_SPILL_TEST_MAX_PAGES) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    if (pRowsPerPage[pageIndex] <= 0 || pRowsPerPage[pageIndex] > 32) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     group = {.groupId = 1001};
  int32_t code = partitionSpillTestSetupGroup(&info, &group, pRowsPerPage,
                                              numPages, 0, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  code = partitionSpillGroup(&info, &group);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  {
    SPartitionFragment* pFragment = taosArrayGet(group.fragments, 0);
    if (pFragment == NULL) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _cleanup;
    }
    int64_t blockStart = pFragment->offset;
    if (corruption == PARTITION_SPILL_TEST_CORRUPT_FRAGMENT_ROWS) {
      // Tampers the in-memory record, not the file: the reader's row
      // reconciliation must reject the mismatch at the last block.
      pFragment->numRows += 1;
    } else if (corruption == PARTITION_SPILL_TEST_CORRUPT_BLOCK_ROWS ||
               corruption == PARTITION_SPILL_TEST_CORRUPT_COL_LENGTH ||
               corruption == PARTITION_SPILL_TEST_CORRUPT_COL_LENGTH_ZERO) {
      // First block layout:
      // [blockSize][rows][precision+scale][bitmap][colLength][data].
      int64_t fieldOffset = blockStart + sizeof(int32_t);
      if (corruption != PARTITION_SPILL_TEST_CORRUPT_BLOCK_ROWS) {
        fieldOffset += sizeof(uint32_t) + 2 * sizeof(uint8_t) +
                       BitmapLen(pRowsPerPage[0]);
      }
      int32_t field = 0;
      code = partitionReadAt(info.spill.pFile, fieldOffset, &field,
                             sizeof(field));
      if (code == TSDB_CODE_SUCCESS) {
        field = corruption == PARTITION_SPILL_TEST_CORRUPT_COL_LENGTH_ZERO
                    ? 0
                    : field + 1;
        code = partitionWriteAt(info.spill.pFile, fieldOffset, &field,
                                sizeof(field));
      }
    } else {
      code = TSDB_CODE_INVALID_PARA;
    }
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }

  // The corrupted fragment must be rejected by the production reader; a
  // successful full read leaves code as TSDB_CODE_SUCCESS, failing the
  // caller's corruption expectation.
  while (info.spill.fragmentIndex < taosArrayGetSize(group.fragments)) {
    code = partitionReadFragmentBlock(&info, &group);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

_cleanup:
  partitionSpillTestCleanupInfo(&info, &group);
  return code;
}

int32_t partitionSpillTestRecycleFailure(
    const int32_t* pRowsPerPage, int32_t numPages, int32_t failIndex,
    SPartitionSpillTestRecycleResult* pResult) {
  if (pRowsPerPage == NULL || numPages <= 0 ||
      numPages > PARTITION_SPILL_TEST_MAX_PAGES || failIndex < 0 ||
      failIndex >= numPages || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    if (pRowsPerPage[pageIndex] <= 0 || pRowsPerPage[pageIndex] > 32) {
      return TSDB_CODE_INVALID_PARA;
    }
  }
  memset(pResult, 0, sizeof(*pResult));

  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     group = {.groupId = 1001};
  int32_t code = partitionSpillTestSetupGroup(&info, &group, pRowsPerPage,
                                              numPages, 0, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  tsSpillTestFailRecycleAt = failIndex;
  pResult->spillCode = partitionSpillGroup(&info, &group);
  pResult->remainingGroupPages = taosArrayGetSize(group.pPageList);
  pResult->remainingGroupRows = group.residentRows;
  pResult->remainingResidentPages = info.numResidentPages;
  pResult->fragmentCount = taosArrayGetSize(group.fragments);

  // The committed fragment must stay fully readable despite the failure.
  while (info.spill.fragmentIndex < taosArrayGetSize(group.fragments)) {
    code = partitionReadFragmentBlock(&info, &group);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
    pResult->readBackRows += info.binfo.pRes->info.rows;
  }

_cleanup:
  tsSpillTestFailRecycleAt = -1;
  partitionSpillTestCleanupInfo(&info, &group);
  return code;
}

// Accumulates NULL/value statistics from whatever currently sits in the
// operator's result block. hasNull is AND-ed so a single block that lost
// the flag is enough to fail the caller's expectation.
static int32_t partitionSpillTestCollectNulls(SPartitionOperatorInfo* pInfo,
                                              bool* pHasNull, int64_t* pRows,
                                              int64_t* pNullRows,
                                              int64_t* pValueSum) {
  SSDataBlock*     pRes = pInfo->binfo.pRes;
  SColumnInfoData* pColumn = taosArrayGet(pRes->pDataBlock, 0);
  if (pColumn == NULL || pRes->info.rows <= 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  *pHasNull = *pHasNull && pColumn->hasNull;
  for (int32_t rowIndex = 0; rowIndex < pRes->info.rows; ++rowIndex) {
    if (colDataIsNull_s(pColumn, rowIndex)) {
      *pNullRows += 1;
      continue;
    }
    int32_t value = 0;
    memcpy(&value, colDataGetData(pColumn, rowIndex), sizeof(value));
    *pValueSum += value;
  }
  *pRows += pRes->info.rows;
  return TSDB_CODE_SUCCESS;
}

/*
 * Stages pages holding NULL rows, reads them back twice - once from the
 * resident pages, then once from the spill fragment - and reports what
 * each path saw. The two decoders are different (blockDataFromPagedBuf vs
 * blockDataFromBufBounded), so a NULL that survives one but not the other
 * means results silently depend on whether memory pressure happened to
 * spill the group.
 */
int32_t partitionSpillTestNullRoundTrip(
    const int32_t* pRowsPerPage, int32_t numPages, uint32_t nullRowMask,
    SPartitionSpillTestNullResult* pResult) {
  if (pRowsPerPage == NULL || numPages <= 0 ||
      numPages > PARTITION_SPILL_TEST_MAX_PAGES || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    if (pRowsPerPage[pageIndex] <= 0 || pRowsPerPage[pageIndex] > 32) {
      return TSDB_CODE_INVALID_PARA;
    }
  }
  memset(pResult, 0, sizeof(*pResult));
  pResult->pagedHasNull = true;
  pResult->spilledHasNull = true;

  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     group = {.groupId = 1001};
  int32_t code = partitionSpillTestSetupGroup(&info, &group, pRowsPerPage,
                                              numPages, nullRowMask, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  // Resident-page path, as partitionLoadNextBlock walks it: the result
  // block is cleaned before each page, which clears hasNull.
  for (int32_t pageIndex = 0; pageIndex < numPages; ++pageIndex) {
    int32_t* pPageId = taosArrayGet(group.pPageList, pageIndex);
    void* pPage = pPageId != NULL
                      ? getBufPage(info.pageClasses[0].pBuf, *pPageId)
                      : NULL;
    if (pPage == NULL) {
      code = terrno != TSDB_CODE_SUCCESS
                 ? terrno
                 : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _cleanup;
    }
    blockDataCleanup(info.binfo.pRes);
    code = blockDataEnsureCapacity(
        info.binfo.pRes, info.pageClasses[0].rowCapacity);
    if (code == TSDB_CODE_SUCCESS) {
      code = blockDataFromPagedBuf(
          info.binfo.pRes, pPage, info.pageClasses[0].rowCapacity);
    }
    releaseBufPage(info.pageClasses[0].pBuf, pPage);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
    code = partitionSpillTestCollectNulls(&info, &pResult->pagedHasNull,
                                          &pResult->pagedRows,
                                          &pResult->pagedNullRows,
                                          &pResult->pagedValueSum);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }

  code = partitionSpillGroup(&info, &group);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  // Spill path: same rows, read back through the compact decoder.
  while (info.spill.fragmentIndex < taosArrayGetSize(group.fragments)) {
    blockDataCleanup(info.binfo.pRes);
    code = partitionReadFragmentBlock(&info, &group);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
    code = partitionSpillTestCollectNulls(&info, &pResult->spilledHasNull,
                                          &pResult->spilledRows,
                                          &pResult->spilledNullRows,
                                          &pResult->spilledValueSum);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }

_cleanup:
  partitionSpillTestCleanupInfo(&info, &group);
  return code;
}

int32_t partitionSpillTestErrnoPropagation(int32_t* pReportedCode) {
  if (pReportedCode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pReportedCode = TSDB_CODE_SUCCESS;

  int32_t                pageId = 0;
  void*                  pPage = NULL;
  SPartitionDataInfo*    pNewGroup = NULL;
  SPartitionDataInfo     dummy = {0};
  SPartitionOperatorInfo info = {0};
  SPartitionDataInfo     victim = {.groupId = 2001};
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  int32_t code = partitionSpillTestCreateInfo(&info, 1024, 1, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  info.pGroupSet = taosHashInit(4, hashFn, false, HASH_NO_LOCK);
  if (info.pGroupSet == NULL) {
    code = terrno;
    goto _cleanup;
  }
  info.keyBuf = taosMemoryCalloc(1, 32);
  if (info.keyBuf == NULL) {
    code = terrno;
    goto _cleanup;
  }
  victim.pPageList = taosArrayInit(1, sizeof(int32_t));
  if (victim.pPageList == NULL) {
    code = terrno;
    goto _cleanup;
  }
  code = partitionSpillTestAddPage(&info, &victim, 0, 4, 0, &pageId);
  if (code != TSDB_CODE_SUCCESS) {
    cleanupPartitionGroupInfo(&victim);
    goto _cleanup;
  }
  code = taosHashPut(info.pGroupSet, &victim.groupId, sizeof(victim.groupId),
                     &victim, sizeof(victim));
  if (code != TSDB_CODE_SUCCESS) {
    cleanupPartitionGroupInfo(&victim);
    goto _cleanup;
  }
  // Ownership of victim's arrays moved to the hash copy above.
  SPartitionDataInfo* pVictim =
      taosHashGet(info.pGroupSet, &victim.groupId, sizeof(victim.groupId));
  if (pVictim == NULL) {
    code = terrno != TSDB_CODE_SUCCESS
               ? terrno
               : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  code = partitionSetSpillGroupLevel(&info, pVictim, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  // The helper's one-page budget is now full. Arm a write failure whose code
  // never touches terrno; the next group allocation must spill the victim.
  // The victim spill's first write is the leading block's size prefix at
  // the start of the fresh file.
  tsSpillTestFailWriteOffset = 0;
  memcpy(info.keyBuf, "k2", 2);
  terrno = TSDB_CODE_SUCCESS;
  code = partitionGetOrCreateGroupInfo(&info, &pNewGroup, 2);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  pPage = partitionGetWritablePage(&info, pNewGroup);
  if (pPage != NULL) {
    releaseBufPage(info.pageClasses[0].pBuf, pPage);
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  *pReportedCode = terrno;
  code = TSDB_CODE_SUCCESS;

_cleanup:
  tsSpillTestFailWriteOffset = -1;
  partitionClearSpillBuckets(&info);
  partitionCleanupGroupSet(info.pGroupSet);
  taosHashCleanup(info.pGroupSet);
  info.pGroupSet = NULL;
  taosMemoryFreeClear(info.keyBuf);
  partitionSpillTestCleanupInfo(&info, &dummy);
  return code;
}

int32_t partitionTestNotLoadedBlock(SPartitionNotLoadedTestResult* pResult) {
  if (pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  memset(pResult, 0, sizeof(*pResult));

  int32_t                code = TSDB_CODE_SUCCESS;
  SSDataBlock*            pInput = NULL;
  SPartitionDataInfo*    pGroup = NULL;
  SPartitionDataInfo     dummy = {0};
  SPartitionOperatorInfo info = {0};
  SExecTaskInfo          task = {0};
  SOperatorInfo          op = {.info = &info, .pTaskInfo = &task};
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  info.pTaskInfo = &task;
  code = partitionSpillTestCreateInfo(&info, 1024, 1, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  info.pGroupSet = taosHashInit(4, hashFn, false, HASH_NO_LOCK);
  if (info.pGroupSet == NULL) {
    code = terrno;
    goto _cleanup;
  }

  info.pGroupCols = taosArrayInit(1, sizeof(SColumn));
  if (info.pGroupCols == NULL) {
    code = terrno;
    goto _cleanup;
  }
  SColumn groupCol = {
      .slotId = 0, .type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  if (taosArrayPush(info.pGroupCols, &groupCol) == NULL) {
    code = terrno;
    goto _cleanup;
  }
  code = initGroupOptrInfo(&info.pGroupColVals, &info.groupKeyLen, &info.keyBuf,
                           info.pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  code = createDataBlock(&pInput);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  SColumnInfoData inputCol =
      createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  code = blockDataAppendColInfo(pInput, &inputCol);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  code = blockDataEnsureCapacity(pInput, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  SColumnInfoData* pInputCol = taosArrayGet(pInput->pDataBlock, 0);
  if (pInputCol == NULL) {
    code = terrno != TSDB_CODE_SUCCESS ? terrno
                                       : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  int32_t          groupValue = 42;
  code = colDataSetVal(pInputCol, 0, (const char*)&groupValue, false);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  pInput->info.rows = 1;
  pInput->info.dataLoad = false;

  int32_t jmpCode = setjmp(task.env);
  if (jmpCode != TSDB_CODE_SUCCESS) {
    code = jmpCode;
    goto _cleanup;
  }
  doHashPartition(&op, pInput);

  pGroup = taosHashIterate(info.pGroupSet, NULL);
  if (pGroup == NULL) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  pResult->pageListInitialized = pGroup->pPageList != NULL;
  pResult->groupPages = taosArrayGetSize(pGroup->pPageList);
  pResult->residentPages = info.numResidentPages;
  pResult->peakResidentPages = info.spillStat.peakResidentPages;
  pResult->spillCount = info.spillStat.spillCount;
  pResult->notLoadedBlocks = taosArrayGetSize(pGroup->blockForNotLoaded);
  pResult->groupId = pGroup->groupId;

  SSDataBlock* pOutput = buildPartitionResultForNotLoadBlock(pGroup);
  if (pOutput == NULL) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  pResult->outputRows = pOutput->info.rows;
  pResult->outputDataLoad = pOutput->info.dataLoad;
  pResult->outputGroupId = pOutput->info.id.groupId;

_cleanup:
  partitionClearSpillBuckets(&info);
  partitionCleanupGroupSet(info.pGroupSet);
  taosHashCleanup(info.pGroupSet);
  info.pGroupSet = NULL;
  taosArrayDestroy(info.pGroupCols);
  info.pGroupCols = NULL;
  taosArrayDestroyEx(info.pGroupColVals, freeGroupKey);
  info.pGroupColVals = NULL;
  taosMemoryFreeClear(info.keyBuf);
  blockDataDestroy(pInput);
  partitionSpillTestCleanupInfo(&info, &dummy);
  return code;
}

int32_t partitionTestRetainMaxPages(
    int32_t numPages, SPartitionMaxPageTestResult* pResult) {
  if (numPages <= 1 || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  memset(pResult, 0, sizeof(*pResult));

  int32_t                code = TSDB_CODE_SUCCESS;
  SPartitionDataInfo     group = {.groupId = 1001};
  SPartitionOperatorInfo info = {0};
  code = partitionSpillTestCreateInfo(&info, 1024, numPages, true);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  code = partitionSpillTestInitPageClass(&info, 1, 2048, 64, numPages);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  SPartitionPageClass* pMaxClass = &info.pageClasses[1];
  info.memBudgetBytes =
      (int64_t)pMaxClass->pageSize * numPages +
      info.pageClasses[0].pageSize;

  group.pPageList = taosArrayInit(1, sizeof(int32_t));
  if (group.pPageList == NULL) {
    code = terrno;
    goto _cleanup;
  }

  void* pPage = partitionGetWritablePage(&info, &group);
  if (pPage == NULL) {
    code = terrno;
    goto _cleanup;
  }
  *(int32_t*)pPage = info.pageClasses[0].rowCapacity;
  setBufPageDirty(pPage, true);
  releaseBufPage(info.pageClasses[0].pBuf, pPage);
  group.residentRows += info.pageClasses[0].rowCapacity;
  info.spillStat.residentRows += info.pageClasses[0].rowCapacity;

  for (int32_t i = 0; i < numPages; ++i) {
    pPage = partitionGetWritablePage(&info, &group);
    if (pPage == NULL) {
      code = terrno;
      goto _cleanup;
    }
    int32_t addedRows = pMaxClass->rowCapacity - *(int32_t*)pPage;
    *(int32_t*)pPage = pMaxClass->rowCapacity;
    setBufPageDirty(pPage, true);
    releaseBufPage(pMaxClass->pBuf, pPage);
    group.residentRows += addedRows;
    info.spillStat.residentRows += addedRows;
  }

  code = partitionSpillTestSnapshotState(&info, &group,
                                         &pResult->beforeSpill);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  {
    SPartitionDataInfo* pGroups[] = {&group};
    pResult->residentBytesInvariantOk =
        partitionCheckResidentBytesInvariant(&info, pGroups, 1);
    pResult->residentPagesInvariantOk =
        partitionCheckResidentPagesInvariant(&info, pGroups, 1);
    pResult->spillBucketLevelInvariantOk =
        partitionCheckSpillBucketLevelInvariant(&info, &group);
  }

  code = partitionSpillGroup(&info, &group);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  code = partitionSpillTestSnapshotState(&info, &group,
                                         &pResult->afterSpill);

_cleanup:
  partitionClearSpillBuckets(&info);
  partitionSpillTestCleanupInfo(&info, &group);
  return code;
}

// A bufferless operator carrying only the class row capacities, which is all
// the pure growth-policy predicates read.
static void partitionTestMakeClassInfo(const int32_t* pRowCapacities,
                                       int32_t numClasses,
                                       SPartitionOperatorInfo* pInfo) {
  *pInfo = (SPartitionOperatorInfo){.numPageClasses = numClasses};
  for (int32_t i = 0; i < numClasses; ++i) {
    pInfo->pageClasses[i].rowCapacity = pRowCapacities[i];
  }
}

int32_t partitionTestSelectGrowthClass(const int32_t* pRowCapacities,
                                       int32_t numClasses,
                                       int32_t currentClass,
                                       int32_t requiredRows) {
  if (pRowCapacities == NULL || numClasses < 2 ||
      numClasses > PARTITION_MAX_PAGE_CLASSES || currentClass < 0 ||
      currentClass + 1 >= numClasses || requiredRows <= 0) {
    return -1;
  }

  SPartitionOperatorInfo info = {0};
  partitionTestMakeClassInfo(pRowCapacities, numClasses, &info);
  return partitionSelectGrowthClass(&info, currentClass, requiredRows);
}

bool partitionTestNeedsRunSize(const int32_t* pRowCapacities,
                               int32_t numClasses, int32_t currentClass,
                               int64_t residentRows, bool hasPage) {
  if (pRowCapacities == NULL || numClasses <= 0 ||
      numClasses > PARTITION_MAX_PAGE_CLASSES || currentClass < 0 ||
      currentClass >= numClasses || residentRows < 0) {
    return false;
  }

  SPartitionOperatorInfo info = {0};
  partitionTestMakeClassInfo(pRowCapacities, numClasses, &info);

  SPartitionDataInfo group = {.residentRows = residentRows};
  group.pPageList = taosArrayInit(1, sizeof(int32_t));
  if (group.pPageList == NULL) {
    return false;
  }
  if (hasPage) {
    // The predicate reads the current class off the last page ref, so the
    // staged page must carry it.
    int32_t pageRef = partitionEncodePageRef(currentClass, 0);
    if (taosArrayPush(group.pPageList, &pageRef) == NULL) {
      taosArrayDestroy(group.pPageList);
      return false;
    }
  }

  bool needsRunSize = partitionNeedsRunSize(&info, &group);
  taosArrayDestroy(group.pPageList);
  return needsRunSize;
}

typedef struct SPartitionPhaseTestSource {
  SSDataBlock**              pBlocks;
  int32_t                    numBlocks;
  int32_t                    nextBlock;
  SPartitionOperatorInfo*    pPartitionInfo;
  SPartitionPhaseTestResult* pResult;
} SPartitionPhaseTestSource;

typedef struct SPartitionPhaseTestContext {
  SExecTaskInfo             task;
  SPartitionOperatorInfo*   pInfo;
  SPartitionPhysiNode       phyNode;
  SOperatorInfo             partition;
  SOperatorInfo             source;
  SOperatorInfo*            downstream[1];
  SPartitionPhaseTestSource sourceInfo;
  SExprInfo                 exprs[2];
  SFunctParam               params[2];
  SColumn                   exprCols[2];
} SPartitionPhaseTestContext;

static int32_t partitionPhaseTestSourceNext(SOperatorInfo* pOperator,
                                            SSDataBlock** ppRes) {
  SPartitionPhaseTestSource* pSource = pOperator->info;
  if (ppRes == NULL || pSource == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Sample the spill state exactly at the side-batch boundary, before the
  // first block of the second batch is handed out.
  if (pSource->nextBlock == PARTITION_SIDE_BATCH_BLOCKS &&
      pSource->pPartitionInfo != NULL && pSource->pResult != NULL) {
    SPartitionSpillInfo* pSpill = &pSource->pPartitionInfo->spill;
    pSource->pResult->spillCountBeforeSecondBatch =
        pSource->pPartitionInfo->spillStat.spillCount;
    pSource->pResult->encryptedSpillCountBeforeSecondBatch =
        pSource->pPartitionInfo->spillStat.encryptedSpillCount;
    pSource->pResult->spillScratchRetainedBeforeSecondBatch =
        pSpill->pBuf != NULL && pSpill->pBatchBuf != NULL &&
        pSpill->pBatchGroups != NULL && pSpill->pBlock != NULL;
    pSource->pResult->spillCryptReleasedBeforeSecondBatch =
        pSpill->pCryptCtx == NULL;
  }

  if (pSource->nextBlock >= pSource->numBlocks) {
    *ppRes = NULL;
  } else {
    *ppRes = pSource->pBlocks[pSource->nextBlock++];
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionPhaseTestCreateBlock(
    const SPartitionPhaseTestBlock* pInput, SSDataBlock** ppBlock) {
  if (pInput == NULL || ppBlock == NULL || pInput->rows <= 0 ||
      pInput->phase < SCAN_PHASE_MAIN ||
      pInput->phase > SCAN_PHASE_PREV) {
    return TSDB_CODE_INVALID_PARA;
  }

  SSDataBlock* pBlock = NULL;
  int32_t code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData tsCol = createColumnInfoData(
      TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
  SColumnInfoData groupCol = createColumnInfoData(
      TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  code = blockDataAppendColInfo(pBlock, &tsCol);
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataAppendColInfo(pBlock, &groupCol);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataEnsureCapacity(pBlock, pInput->rows);
  }
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, 1);
  if (pTsCol == NULL || pGroupCol == NULL) {
    blockDataDestroy(pBlock);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  for (int32_t i = 0; i < pInput->rows; ++i) {
    int64_t ts = pInput->firstTs + pInput->tsStep * i;
    code = colDataSetVal(pTsCol, i, (const char*)&ts, false);
    if (code == TSDB_CODE_SUCCESS) {
      bool isNull = i < sizeof(pInput->nullGroupRowMask) * CHAR_BIT &&
                    (pInput->nullGroupRowMask & (UINT64_C(1) << i)) != 0;
      code = colDataSetVal(pGroupCol, i,
                           (const char*)&pInput->groupValue, isNull);
    }
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return code;
    }
  }

  pBlock->info.rows = pInput->rows;
  pBlock->info.dataLoad = pInput->metadataOnly ? 0 : 1;
  pBlock->info.scanPhase = pInput->phase;
  pBlock->info.id.groupId = pInput->outerEpoch;
  code = blockDataUpdateTsWindow(pBlock, 0);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }
  *ppBlock = pBlock;
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionPhaseTestInit(SPartitionPhaseTestContext* pCtx,
                                      int32_t numBlocks,
                                      SPartitionPhaseTestResult* pResult,
                                      bool reserveColumn) {
  pCtx->pInfo = taosMemoryCalloc(1, sizeof(*pCtx->pInfo));
  if (pCtx->pInfo == NULL) {
    return terrno;
  }

  SPartitionOperatorInfo* pInfo = pCtx->pInfo;
  SOperatorInfo* pPartition = &pCtx->partition;
  pInfo->pTaskInfo = &pCtx->task;
  pInfo->phasedMode = true;
  pInfo->curPhase = SCAN_PHASE_MAIN;
  pInfo->phaseState = PARTITION_PHASE_READING;

  int32_t code = createDataBlock(&pInfo->binfo.pRes);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SColumnInfoData tsCol = createColumnInfoData(
      TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
  SColumnInfoData groupCol = createColumnInfoData(
      TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  code = blockDataAppendColInfo(pInfo->binfo.pRes, &tsCol);
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataAppendColInfo(pInfo->binfo.pRes, &groupCol);
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (reserveColumn) {
    SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_DOUBLE,
                                                   sizeof(double), 3);
    column.info.noData = true;
    code = blockDataAppendColInfo(pInfo->binfo.pRes, &column);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  int32_t numPages = TMAX(4, numBlocks + 2);
  code = partitionSpillTestInitPageClass(
      pInfo, 0, PARTITION_PHASE_TEST_PAGE_SIZE_SMALL, 64, numPages);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = partitionSpillTestInitPageClass(
      pInfo, 1, PARTITION_PHASE_TEST_PAGE_SIZE_LARGE, 128, numPages);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pInfo->memBudgetBytes =
      (int64_t)PARTITION_PHASE_TEST_PAGE_BUDGET * numPages;

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pGroupSet = taosHashInit(8, hashFn, false, HASH_NO_LOCK);
  if (pInfo->pGroupSet == NULL) {
    return terrno;
  }

  pInfo->pGroupCols = taosArrayInit(1, sizeof(SColumn));
  if (pInfo->pGroupCols == NULL) {
    return terrno;
  }
  SColumn partitionCol = {
      .slotId = 1,
      .type = TSDB_DATA_TYPE_INT,
      .bytes = sizeof(int32_t),
  };
  if (taosArrayPush(pInfo->pGroupCols, &partitionCol) == NULL) {
    return terrno;
  }
  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen,
                           &pInfo->keyBuf, pInfo->pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->pOrderInfoArr = taosArrayInit(1, sizeof(SBlockOrderInfo));
  if (pInfo->pOrderInfoArr == NULL) {
    return terrno;
  }
  SBlockOrderInfo order = {
      .order = ORDER_ASC,
      .nullFirst = false,
      .slotId = 0,
  };
  if (taosArrayPush(pInfo->pOrderInfoArr, &order) == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < 2; ++i) {
    pCtx->exprCols[i] = (SColumn){
        .slotId = i,
        .type = i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_INT,
        .bytes = i == 0 ? sizeof(TSKEY) : sizeof(int32_t),
    };
    pCtx->params[i].type = FUNC_PARAM_TYPE_COLUMN;
    pCtx->params[i].pCol = &pCtx->exprCols[i];
    pCtx->exprs[i].base.numOfParams = 1;
    pCtx->exprs[i].base.pParam = &pCtx->params[i];
  }
  pPartition->exprSupp.numOfExprs = 2;
  pPartition->exprSupp.pExprInfo = pCtx->exprs;
  setOperatorInfo(pPartition, "PartitionPhaseTest",
                  QUERY_NODE_PHYSICAL_PLAN_PARTITION, false, OP_OPENED,
                  pInfo, &pCtx->task);
  pPartition->pPhyNode = (SPhysiNode*)&pCtx->phyNode;

  pCtx->sourceInfo.numBlocks = numBlocks;
  pCtx->sourceInfo.pPartitionInfo = pInfo;
  pCtx->sourceInfo.pResult = pResult;
  pCtx->sourceInfo.pBlocks = taosMemoryCalloc(
      TMAX(1, numBlocks), sizeof(SSDataBlock*));
  if (pCtx->sourceInfo.pBlocks == NULL) {
    return terrno;
  }
  setOperatorInfo(&pCtx->source, "PartitionPhaseSource", 0, false,
                  OP_OPENED, &pCtx->sourceInfo, &pCtx->task);
  pCtx->source.fpSet.getNextFn = partitionPhaseTestSourceNext;
  pCtx->downstream[0] = &pCtx->source;
  pPartition->pDownstream = pCtx->downstream;
  pPartition->numOfDownstream = 1;
  pPartition->numOfRealDownstream = 1;
  return TSDB_CODE_SUCCESS;
}

static void partitionPhaseTestCleanup(SPartitionPhaseTestContext* pCtx) {
  SPartitionOperatorInfo* pInfo = pCtx->pInfo;
  if (pInfo != NULL) {
    blockDataDestroy(pInfo->pHeldBlock);
    pInfo->pHeldBlock = NULL;

    clearPartitionOperator(pInfo);
    taosArrayDestroy(pInfo->sortedGroupArray);
    pInfo->sortedGroupArray = NULL;
    partitionClearSpillBuckets(pInfo);
    if (pInfo->pGroupSet != NULL) {
      partitionCleanupGroupSet(pInfo->pGroupSet);
      taosHashCleanup(pInfo->pGroupSet);
      pInfo->pGroupSet = NULL;
    }

    taosArrayDestroy(pInfo->pGroupCols);
    taosArrayDestroyEx(pInfo->pGroupColVals, freeGroupKey);
    taosMemoryFreeClear(pInfo->keyBuf);
    taosArrayDestroy(pInfo->pOrderInfoArr);
    partitionFreeSpillResources(&pInfo->spill);
    partitionSpillTestDestroyPageClasses(pInfo);
    blockDataDestroy(pInfo->binfo.pRes);
    taosMemoryFreeClear(pCtx->pInfo);
  }

  if (pCtx->sourceInfo.pBlocks != NULL) {
    for (int32_t i = 0; i < pCtx->sourceInfo.numBlocks; ++i) {
      blockDataDestroy(pCtx->sourceInfo.pBlocks[i]);
    }
  }
  taosMemoryFreeClear(pCtx->sourceInfo.pBlocks);
}

static int32_t partitionPhaseTestCaptureOutput(
    SPartitionPhaseTestContext* pCtx, const SSDataBlock* pBlock,
    SPartitionPhaseTestResult* pResult) {
  if (pBlock == NULL || pBlock->info.rows <= 0 ||
      pBlock->info.rows > INT32_MAX ||
      pResult->outputCount >= PARTITION_PHASE_TEST_MAX_OUTPUTS) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, 1);
  if (pTsCol == NULL || pGroupCol == NULL ||
      colDataIsNull_s(pTsCol, 0) ||
      colDataIsNull_s(pTsCol, pBlock->info.rows - 1)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (pResult->rowCount >
      PARTITION_PHASE_TEST_MAX_ROWS - pBlock->info.rows) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (colDataIsNull_s(pTsCol, i) || colDataIsNull_s(pGroupCol, i)) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }

  pResult->spillCountDuringOutput = TMAX(
      pResult->spillCountDuringOutput, pCtx->pInfo->spillStat.spillCount);
  pResult->encryptedSpillCountDuringOutput = TMAX(
      pResult->encryptedSpillCountDuringOutput,
      pCtx->pInfo->spillStat.encryptedSpillCount);

  SPartitionPhaseTestOutput* pOutput =
      &pResult->outputs[pResult->outputCount++];
  pOutput->phase = pBlock->info.scanPhase;
  pOutput->rows = (int32_t)pBlock->info.rows;
  pOutput->pulledBlocks = pCtx->sourceInfo.nextBlock;
  pOutput->groupId = pBlock->info.id.groupId;
  pOutput->baseGId = pBlock->info.id.baseGId;
  memcpy(&pOutput->firstTs, colDataGetData(pTsCol, 0), sizeof(int64_t));
  memcpy(&pOutput->lastTs,
         colDataGetData(pTsCol, pBlock->info.rows - 1), sizeof(int64_t));

  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    memcpy(&pResult->timestamps[pResult->rowCount++],
           colDataGetData(pTsCol, i), sizeof(int64_t));
    memcpy(&pResult->groupValues[pResult->rowCount - 1],
           colDataGetData(pGroupCol, i), sizeof(int32_t));
  }
  return TSDB_CODE_SUCCESS;
}

int32_t partitionTestRunPhasedWithConfig(
    const SPartitionPhaseTestBlock* pBlocks, int32_t numBlocks,
    const SPartitionPhaseTestConfig* pConfig,
    SPartitionPhaseTestResult* pResult) {
  if (numBlocks < 0 || (numBlocks > 0 && pBlocks == NULL) ||
      pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  memset(pResult, 0, sizeof(*pResult));

  // The context outlives longjmp state restoration, so its mutated fields
  // remain defined when the recovery path inspects and releases them.
  SPartitionPhaseTestContext* pCtx =
      taosMemoryCalloc(1, sizeof(*pCtx));
  if (pCtx == NULL) {
    return terrno;
  }

  char savedTempDir[PATH_MAX];
  EEncryptScope savedScope = tsiEncryptScope;
  tstrncpy(savedTempDir, tsTempDir, sizeof(savedTempDir));
  tstrncpy(tsTempDir, TD_TMP_DIR_PATH, PATH_MAX);

  pCtx->task.id.str = "partitionPhaseTest";
  bool reserveColumn = pConfig != NULL && pConfig->reserveColumn;
  int32_t code = partitionPhaseTestInit(pCtx, numBlocks, pResult,
                                        reserveColumn);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  if (pConfig != NULL) {
    if (pConfig->memBudgetBytes > 0) {
      pCtx->pInfo->memBudgetBytes = pConfig->memBudgetBytes;
    }
    if (pConfig->encryptSpill) {
      tsiEncryptScope = (EEncryptScope)(savedScope | DND_CS_QUERY_SPILL);
    }
    partitionTestSetPrepareOutputFailure(pConfig->prepareOutputFailureCode);
    partitionTestSetPageAllocationFailure(
        pConfig->pageAllocationFailureCode);
    partitionTestSetSpillReadFailure(pConfig->spillReadFailureCode);
    partitionTestSetSpillCryptFailure(pConfig->spillCryptFailureCode);
  }

  for (int32_t i = 0; i < numBlocks; ++i) {
    code = partitionPhaseTestCreateBlock(
        &pBlocks[i], &pCtx->sourceInfo.pBlocks[i]);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }

  int32_t jmpCode = setjmp(pCtx->task.env);
  if (jmpCode != TSDB_CODE_SUCCESS) {
    code = jmpCode;
    goto _cleanup;
  }

  while (true) {
    SSDataBlock* pOutput = NULL;
    code = phasedHashPartitionNext(&pCtx->partition, &pOutput);
    if (code != TSDB_CODE_SUCCESS || pOutput == NULL) {
      break;
    }
    code = partitionPhaseTestCaptureOutput(pCtx, pOutput, pResult);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    if (reserveColumn) {
      // colDataAssign in a parent expression clears the reserved flag.
      SColumnInfoData* pCol = taosArrayGet(pOutput->pDataBlock, 2);
      pCol->info.noData = false;
    }
  }

_cleanup:
  pResult->totalPulledBlocks = pCtx->sourceInfo.nextBlock;
  pResult->hashGroupCountOnExit =
      pCtx->pInfo != NULL && pCtx->pInfo->pGroupSet != NULL
          ? taosHashGetSize(pCtx->pInfo->pGroupSet)
          : 0;
  pResult->sortedGroupCountOnExit = pCtx->pInfo != NULL
                                        ? taosArrayGetSize(
                                              pCtx->pInfo->sortedGroupArray)
                                        : 0;
  pResult->residentPageCountOnExit =
      pCtx->pInfo != NULL ? pCtx->pInfo->numResidentPages : 0;
  pResult->spillFileOpenOnExit =
      pCtx->pInfo != NULL && pCtx->pInfo->spill.pFile != NULL;
  partitionTestSetPrepareOutputFailure(TSDB_CODE_SUCCESS);
  partitionTestSetPageAllocationFailure(TSDB_CODE_SUCCESS);
  partitionTestSetSpillReadFailure(TSDB_CODE_SUCCESS);
  partitionTestSetSpillCryptFailure(TSDB_CODE_SUCCESS);
  tsiEncryptScope = savedScope;
  partitionPhaseTestCleanup(pCtx);
  taosMemoryFree(pCtx);
  tstrncpy(tsTempDir, savedTempDir, PATH_MAX);
  return code;
}

int32_t partitionTestRunPhased(
    const SPartitionPhaseTestBlock* pBlocks, int32_t numBlocks,
    SPartitionPhaseTestResult* pResult) {
  return partitionTestRunPhasedWithConfig(pBlocks, numBlocks, NULL, pResult);
}

int32_t partitionTestHeldBlockLifecycle(
    EPartitionPhaseTestLifecycleAction action,
    SPartitionPhaseTestLifecycleResult* pResult) {
  if ((action != PARTITION_PHASE_TEST_RESET &&
       action != PARTITION_PHASE_TEST_DESTROY) ||
      pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  memset(pResult, 0, sizeof(*pResult));

  const SPartitionPhaseTestBlock blocks[] = {
      {SCAN_PHASE_MAIN, 1, 1, 10, 0, false},
      {SCAN_PHASE_NEXT, 1, 1, 11, 0, false},
  };
  SPartitionPhaseTestContext* pCtx =
      taosMemoryCalloc(1, sizeof(*pCtx));
  if (pCtx == NULL) {
    return terrno;
  }
  pCtx->task.id.str = "partitionPhaseLifecycleTest";
  SPartitionPhaseTestResult* pPhaseResult =
      taosMemoryCalloc(1, sizeof(*pPhaseResult));
  if (pPhaseResult == NULL) {
    taosMemoryFree(pCtx);
    return terrno;
  }

  char savedTempDir[PATH_MAX];
  tstrncpy(savedTempDir, tsTempDir, sizeof(savedTempDir));
  tstrncpy(tsTempDir, TD_TMP_DIR_PATH, PATH_MAX);

  int32_t code = partitionPhaseTestInit(pCtx, 2, pPhaseResult, false);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }
  for (int32_t i = 0; i < 2; ++i) {
    code = partitionPhaseTestCreateBlock(
        &blocks[i], &pCtx->sourceInfo.pBlocks[i]);
    if (code != TSDB_CODE_SUCCESS) {
      goto _cleanup;
    }
  }

  int32_t jmpCode = setjmp(pCtx->task.env);
  if (jmpCode != TSDB_CODE_SUCCESS) {
    code = jmpCode;
    goto _cleanup;
  }

  SSDataBlock* pOutput = NULL;
  code = phasedHashPartitionNext(&pCtx->partition, &pOutput);
  if (code != TSDB_CODE_SUCCESS || pOutput == NULL ||
      pCtx->pInfo->pHeldBlock == NULL) {
    code = code != TSDB_CODE_SUCCESS
               ? code
               : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _cleanup;
  }
  pResult->heldBeforeAction = true;

  if (action == PARTITION_PHASE_TEST_RESET) {
    code = resetPartitionOperState(&pCtx->partition);
    if (code == TSDB_CODE_SUCCESS) {
      pResult->heldAfterAction = pCtx->pInfo->pHeldBlock != NULL;
      pResult->phaseAfterAction = pCtx->pInfo->curPhase;
      pResult->readingAfterAction =
          pCtx->pInfo->phaseState == PARTITION_PHASE_READING;
      pResult->inputDoneAfterAction = pCtx->pInfo->inputDone;
    }
  } else {
    destroyPartitionOperatorInfo(pCtx->pInfo);
    pCtx->pInfo = NULL;
    pCtx->partition.info = NULL;
    pResult->destroyed = true;
  }

_cleanup:
  partitionPhaseTestCleanup(pCtx);
  taosMemoryFree(pCtx);
  taosMemoryFree(pPhaseResult);
  tstrncpy(tsTempDir, savedTempDir, PATH_MAX);
  return code;
}

#endif  // BUILD_TEST
