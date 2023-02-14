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
#include "ttszip.h"
#include "taoserror.h"
#include "tcompression.h"
#include "tlog.h"

static int32_t getDataStartOffset();
static void    TSBufUpdateGroupInfo(STSBuf* pTSBuf, int32_t index, STSGroupBlockInfo* pBlockInfo);
static STSBuf* allocResForTSBuf(STSBuf* pTSBuf);
static int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader);

/**
 * todo error handling
 * support auto closeable tmp file
 * @param path
 * @return
 */
STSBuf* tsBufCreate(bool autoDelete, int32_t order) {
  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    // tscError("tmp file created failed since %s", terrstr());
    return NULL;
  }

  STSBuf* pTSBuf = taosMemoryCalloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }

  pTSBuf->autoDelete = autoDelete;

  taosGetTmpfilePath(tsTempDir, "join", pTSBuf->path);
  // pTSBuf->pFile = fopen(pTSBuf->path, "wb+");
  pTSBuf->pFile = taosOpenFile(pTSBuf->path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC);
  if (pTSBuf->pFile == NULL) {
    taosMemoryFree(pTSBuf);
    return NULL;
  }

  if (!autoDelete) {
    if (taosRemoveFile(pTSBuf->path) != 0) {
      taosMemoryFree(pTSBuf);
      return NULL;
    }
  }

  if (allocResForTSBuf(pTSBuf) == NULL) {
    return NULL;
  }

  // update the header info
  STSBufFileHeader header = {.magic = TS_COMP_FILE_MAGIC, .numOfGroup = pTSBuf->numOfGroups, .tsOrder = TSDB_ORDER_ASC};
  STSBufUpdateHeader(pTSBuf, &header);

  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSDB_ORDER_ASC;

  pTSBuf->tsOrder = order;

  return pTSBuf;
}

STSBuf* tsBufCreateFromFile(const char* path, bool autoDelete) {
  STSBuf* pTSBuf = taosMemoryCalloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }

  pTSBuf->autoDelete = autoDelete;

  tstrncpy(pTSBuf->path, path, sizeof(pTSBuf->path));

  // pTSBuf->pFile = fopen(pTSBuf->path, "rb+");
  pTSBuf->pFile = taosOpenFile(pTSBuf->path, TD_FILE_WRITE | TD_FILE_READ);
  if (pTSBuf->pFile == NULL) {
    taosMemoryFree(pTSBuf);
    return NULL;
  }

  if (allocResForTSBuf(pTSBuf) == NULL) {
    return NULL;
  }

  // validate the file magic number
  STSBufFileHeader header = {0};
  int32_t          ret = taosLSeekFile(pTSBuf->pFile, 0, SEEK_SET);
  UNUSED(ret);
  size_t sz = taosReadFile(pTSBuf->pFile, &header, sizeof(STSBufFileHeader));
  UNUSED(sz);

  // invalid file
  if (header.magic != TS_COMP_FILE_MAGIC) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  if (header.numOfGroup > pTSBuf->numOfAlloc) {
    pTSBuf->numOfAlloc = header.numOfGroup;
    STSGroupBlockInfoEx* tmp = taosMemoryRealloc(pTSBuf->pData, sizeof(STSGroupBlockInfoEx) * pTSBuf->numOfAlloc);
    if (tmp == NULL) {
      tsBufDestroy(pTSBuf);
      return NULL;
    }

    pTSBuf->pData = tmp;
  }

  pTSBuf->numOfGroups = header.numOfGroup;

  // check the ts order
  pTSBuf->tsOrder = header.tsOrder;
  if (pTSBuf->tsOrder != TSDB_ORDER_ASC && pTSBuf->tsOrder != TSDB_ORDER_DESC) {
    //    tscError("invalid order info in buf:%d", pTSBuf->tsOrder);
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  size_t infoSize = sizeof(STSGroupBlockInfo) * pTSBuf->numOfGroups;

  STSGroupBlockInfo* buf = (STSGroupBlockInfo*)taosMemoryCalloc(1, infoSize);
  if (buf == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  // int64_t pos = ftell(pTSBuf->pFile); //pos not used
  sz = taosReadFile(pTSBuf->pFile, buf, infoSize);
  UNUSED(sz);

  // the length value for each vnode is not kept in file, so does not set the length value
  for (int32_t i = 0; i < pTSBuf->numOfGroups; ++i) {
    STSGroupBlockInfoEx* pBlockList = &pTSBuf->pData[i];
    memcpy(&pBlockList->info, &buf[i], sizeof(STSGroupBlockInfo));
  }
  taosMemoryFree(buf);

  ret = taosLSeekFile(pTSBuf->pFile, 0, SEEK_END);
  UNUSED(ret);

  int64_t file_size;
  if (taosFStatFile(pTSBuf->pFile, &file_size, NULL) != 0) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  pTSBuf->fileSize = (uint32_t)file_size;
  tsBufResetPos(pTSBuf);

  // ascending by default
  pTSBuf->cur.order = TSDB_ORDER_ASC;

  //  tscDebug("create tsBuf from file:%s, fd:%d, size:%d, numOfGroups:%d, autoDelete:%d", pTSBuf->path,
  //  fileno(pTSBuf->pFile),
  //           pTSBuf->fileSize, pTSBuf->numOfGroups, pTSBuf->autoDelete);

  return pTSBuf;
}

void* tsBufDestroy(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return NULL;
  }

  taosMemoryFreeClear(pTSBuf->assistBuf);
  taosMemoryFreeClear(pTSBuf->tsData.rawBuf);

  taosMemoryFreeClear(pTSBuf->pData);
  taosMemoryFreeClear(pTSBuf->block.payload);

  if (!pTSBuf->remainOpen) {
    taosCloseFile(&pTSBuf->pFile);
  }

  if (pTSBuf->autoDelete) {
    //    ("tsBuf %p destroyed, delete tmp file:%s", pTSBuf, pTSBuf->path);
    if (taosRemoveFile(pTSBuf->path) != 0) {
      // tscError("tsBuf %p destroyed, failed to remove tmp file:%s", pTSBuf, pTSBuf->path);
    }
  } else {
    //    tscDebug("tsBuf %p destroyed, tmp file:%s, remains", pTSBuf, pTSBuf->path);
  }

  taosVariantDestroy(&pTSBuf->block.tag);
  taosMemoryFree(pTSBuf);
  return NULL;
}

static STSGroupBlockInfoEx* tsBufGetLastGroupInfo(STSBuf* pTSBuf) {
  int32_t last = pTSBuf->numOfGroups - 1;

  ASSERT(last >= 0);
  return &pTSBuf->pData[last];
}

static STSGroupBlockInfoEx* addOneGroupInfo(STSBuf* pTSBuf, int32_t id) {
  if (pTSBuf->numOfAlloc <= pTSBuf->numOfGroups) {
    uint32_t newSize = (uint32_t)(pTSBuf->numOfAlloc * 1.5);
    ASSERT((int32_t)newSize > pTSBuf->numOfAlloc);

    STSGroupBlockInfoEx* tmp =
        (STSGroupBlockInfoEx*)taosMemoryRealloc(pTSBuf->pData, sizeof(STSGroupBlockInfoEx) * newSize);
    if (tmp == NULL) {
      return NULL;
    }

    pTSBuf->pData = tmp;
    pTSBuf->numOfAlloc = newSize;
    memset(&pTSBuf->pData[pTSBuf->numOfGroups], 0, sizeof(STSGroupBlockInfoEx) * (newSize - pTSBuf->numOfGroups));
  }

  if (pTSBuf->numOfGroups > 0) {
    STSGroupBlockInfoEx* pPrevBlockInfoEx = tsBufGetLastGroupInfo(pTSBuf);

    // update prev vnode length info in file
    TSBufUpdateGroupInfo(pTSBuf, pTSBuf->numOfGroups - 1, &pPrevBlockInfoEx->info);
  }

  // set initial value for vnode block
  STSGroupBlockInfo* pBlockInfo = &pTSBuf->pData[pTSBuf->numOfGroups].info;
  pBlockInfo->id = id;
  pBlockInfo->offset = pTSBuf->fileSize;
  ASSERT(pBlockInfo->offset >= getDataStartOffset());

  // update vnode info in file
  TSBufUpdateGroupInfo(pTSBuf, pTSBuf->numOfGroups, pBlockInfo);

  // add one vnode info
  pTSBuf->numOfGroups += 1;

  // update the header info
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfGroup = pTSBuf->numOfGroups, .tsOrder = pTSBuf->tsOrder};

  STSBufUpdateHeader(pTSBuf, &header);
  return tsBufGetLastGroupInfo(pTSBuf);
}

static void shrinkBuffer(STSList* ptsData) {
  // shrink tmp buffer size if it consumes too many memory compared to the pre-defined size
  if (ptsData->allocSize >= ptsData->threshold * 2) {
    char* rawBuf = taosMemoryRealloc(ptsData->rawBuf, MEM_BUF_SIZE);
    if (rawBuf) {
      ptsData->rawBuf = rawBuf;
      ptsData->allocSize = MEM_BUF_SIZE;
    }
  }
}

static int32_t getTagAreaLength(SVariant* pa) {
  int32_t t = sizeof(pa->nLen) * 2 + sizeof(pa->nType);
  if (pa->nType != TSDB_DATA_TYPE_NULL) {
    t += pa->nLen;
  }

  return t;
}

static void writeDataToDisk(STSBuf* pTSBuf) {
  if (pTSBuf->tsData.len == 0) {
    return;
  }

  STSBlock* pBlock = &pTSBuf->block;
  STSList*  pTsData = &pTSBuf->tsData;

  pBlock->numOfElem = pTsData->len / TSDB_KEYSIZE;
  pBlock->compLen = tsCompressTimestamp(pTsData->rawBuf, pTsData->len, pTsData->len / TSDB_KEYSIZE, pBlock->payload,
                                        pTsData->allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);

  int64_t r = taosLSeekFile(pTSBuf->pFile, pTSBuf->fileSize, SEEK_SET);
  ASSERT(r == 0);

  /*
   * format for output data:
   * 1. tags, number of ts, size after compressed, payload, size after compressed
   * 2. tags, number of ts, size after compressed, payload, size after compressed
   *
   * both side has the compressed length is used to support load data forwards/backwords.
   */
  int32_t metaLen = 0;
  metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &pBlock->tag.nType, sizeof(pBlock->tag.nType));

  int32_t trueLen = pBlock->tag.nLen;
  if (pBlock->tag.nType == TSDB_DATA_TYPE_BINARY || pBlock->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &pBlock->tag.nLen, sizeof(pBlock->tag.nLen));
    metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, pBlock->tag.pz, (size_t)pBlock->tag.nLen);
  } else if (pBlock->tag.nType == TSDB_DATA_TYPE_FLOAT) {
    metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &pBlock->tag.nLen, sizeof(pBlock->tag.nLen));
    float tfloat = (float)pBlock->tag.d;
    metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &tfloat, (size_t)pBlock->tag.nLen);
  } else if (pBlock->tag.nType != TSDB_DATA_TYPE_NULL) {
    metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &pBlock->tag.nLen, sizeof(pBlock->tag.nLen));
    metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &pBlock->tag.i, (size_t)pBlock->tag.nLen);
  } else {
    trueLen = 0;
    metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &trueLen, sizeof(pBlock->tag.nLen));
  }

  taosWriteFile(pTSBuf->pFile, &pBlock->numOfElem, sizeof(pBlock->numOfElem));
  taosWriteFile(pTSBuf->pFile, &pBlock->compLen, sizeof(pBlock->compLen));
  taosWriteFile(pTSBuf->pFile, pBlock->payload, (size_t)pBlock->compLen);
  taosWriteFile(pTSBuf->pFile, &pBlock->compLen, sizeof(pBlock->compLen));

  metaLen += (int32_t)taosWriteFile(pTSBuf->pFile, &trueLen, sizeof(pBlock->tag.nLen));
  ASSERT(metaLen == getTagAreaLength(&pBlock->tag));

  int32_t blockSize = metaLen + sizeof(pBlock->numOfElem) + sizeof(pBlock->compLen) * 2 + pBlock->compLen;
  pTSBuf->fileSize += blockSize;

  pTSBuf->tsData.len = 0;

  STSGroupBlockInfoEx* pGroupBlockInfoEx = tsBufGetLastGroupInfo(pTSBuf);

  pGroupBlockInfoEx->info.compLen += blockSize;
  pGroupBlockInfoEx->info.numOfBlocks += 1;

  shrinkBuffer(&pTSBuf->tsData);
}

static void expandBuffer(STSList* ptsData, int32_t inputSize) {
  if (ptsData->allocSize - ptsData->len < inputSize) {
    int32_t newSize = inputSize + ptsData->len;
    char*   tmp = taosMemoryRealloc(ptsData->rawBuf, (size_t)newSize);
    if (tmp == NULL) {
      // todo
    }

    ptsData->rawBuf = tmp;
    ptsData->allocSize = newSize;
  }
}

STSBlock* readDataFromDisk(STSBuf* pTSBuf, int32_t order, bool decomp) {
  STSBlock* pBlock = &pTSBuf->block;

  // clear the memory buffer
  pBlock->compLen = 0;
  pBlock->padding = 0;
  pBlock->numOfElem = 0;

  int32_t offset = -1;

  if (order == TSDB_ORDER_DESC) {
    /*
     * set the right position for the reversed traverse, the reversed traverse is started from
     * the end of each comp data block
     */
    int32_t prev = -(int32_t)(sizeof(pBlock->padding) + sizeof(pBlock->tag.nLen));
    int32_t ret = taosLSeekFile(pTSBuf->pFile, prev, SEEK_CUR);
    size_t  sz = taosReadFile(pTSBuf->pFile, &pBlock->padding, sizeof(pBlock->padding));
    sz = taosReadFile(pTSBuf->pFile, &pBlock->tag.nLen, sizeof(pBlock->tag.nLen));
    UNUSED(sz);

    pBlock->compLen = pBlock->padding;

    offset = pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + getTagAreaLength(&pBlock->tag);
    ret = taosLSeekFile(pTSBuf->pFile, -offset, SEEK_CUR);
    UNUSED(ret);
  }

  int32_t ret = taosReadFile(pTSBuf->pFile, &pBlock->tag.nType, sizeof(pBlock->tag.nType));
  ret = taosReadFile(pTSBuf->pFile, &pBlock->tag.nLen, sizeof(pBlock->tag.nLen));

  // NOTE: mix types tags are not supported
  size_t sz = 0;
  if (pBlock->tag.nType == TSDB_DATA_TYPE_BINARY || pBlock->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    char* tp = taosMemoryRealloc(pBlock->tag.pz, pBlock->tag.nLen + 1);
    ASSERT(tp != NULL);

    memset(tp, 0, pBlock->tag.nLen + 1);
    pBlock->tag.pz = tp;

    sz = taosReadFile(pTSBuf->pFile, pBlock->tag.pz, (size_t)pBlock->tag.nLen);
    UNUSED(sz);
  } else if (pBlock->tag.nType == TSDB_DATA_TYPE_FLOAT) {
    float tfloat = 0;
    sz = taosReadFile(pTSBuf->pFile, &tfloat, (size_t)pBlock->tag.nLen);
    pBlock->tag.d = (double)tfloat;
    UNUSED(sz);
  } else if (pBlock->tag.nType != TSDB_DATA_TYPE_NULL) {  // TODO check the return value
    sz = taosReadFile(pTSBuf->pFile, &pBlock->tag.i, (size_t)pBlock->tag.nLen);
    UNUSED(sz);
  }

  sz = taosReadFile(pTSBuf->pFile, &pBlock->numOfElem, sizeof(pBlock->numOfElem));
  UNUSED(sz);
  sz = taosReadFile(pTSBuf->pFile, &pBlock->compLen, sizeof(pBlock->compLen));
  UNUSED(sz);
  sz = taosReadFile(pTSBuf->pFile, pBlock->payload, (size_t)pBlock->compLen);

  if (decomp) {
    pTSBuf->tsData.len =
        tsDecompressTimestamp(pBlock->payload, pBlock->compLen, pBlock->numOfElem, pTSBuf->tsData.rawBuf,
                              pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);
  }

  // read the comp length at the length of comp block
  sz = taosReadFile(pTSBuf->pFile, &pBlock->padding, sizeof(pBlock->padding));
  ASSERT(pBlock->padding == pBlock->compLen);

  int32_t n = 0;
  sz = taosReadFile(pTSBuf->pFile, &n, sizeof(pBlock->tag.nLen));
  if (pBlock->tag.nType == TSDB_DATA_TYPE_NULL) {
    ASSERT(n == 0);
  } else {
    ASSERT(n == pBlock->tag.nLen);
  }

  UNUSED(sz);

  // for backwards traverse, set the start position at the end of previous block
  if (order == TSDB_ORDER_DESC) {
    int32_t r = taosLSeekFile(pTSBuf->pFile, -offset, SEEK_CUR);
    UNUSED(r);
  }

  return pBlock;
}

// set the order of ts buffer if the ts order has not been set yet
static int32_t setCheckTSOrder(STSBuf* pTSBuf, const char* pData, int32_t len) {
  STSList* ptsData = &pTSBuf->tsData;

  if (pTSBuf->tsOrder == -1) {
    if (ptsData->len > 0) {
      TSKEY lastKey = *(TSKEY*)(ptsData->rawBuf + ptsData->len - TSDB_KEYSIZE);

      if (lastKey > *(TSKEY*)pData) {
        pTSBuf->tsOrder = TSDB_ORDER_DESC;
      } else {
        pTSBuf->tsOrder = TSDB_ORDER_ASC;
      }
    } else if (len > TSDB_KEYSIZE) {
      // no data in current vnode, more than one ts is added, check the orders
      TSKEY k1 = *(TSKEY*)(pData);
      TSKEY k2 = *(TSKEY*)(pData + TSDB_KEYSIZE);

      if (k1 < k2) {
        pTSBuf->tsOrder = TSDB_ORDER_ASC;
      } else if (k1 > k2) {
        pTSBuf->tsOrder = TSDB_ORDER_DESC;
      } else {
        // todo handle error
      }
    }
  } else {
    // todo the timestamp order is set, check the asc/desc order of appended data
  }

  return TSDB_CODE_SUCCESS;
}

void tsBufAppend(STSBuf* pTSBuf, int32_t id, SVariant* tag, const char* pData, int32_t len) {
  STSGroupBlockInfoEx* pBlockInfo = NULL;
  STSList*             ptsData = &pTSBuf->tsData;

  if (pTSBuf->numOfGroups == 0 || tsBufGetLastGroupInfo(pTSBuf)->info.id != id) {
    writeDataToDisk(pTSBuf);
    shrinkBuffer(ptsData);

    pBlockInfo = addOneGroupInfo(pTSBuf, id);
  } else {
    pBlockInfo = tsBufGetLastGroupInfo(pTSBuf);
  }

  ASSERT(pBlockInfo->info.id == id);

  if ((taosVariantCompare(&pTSBuf->block.tag, tag) != 0) && ptsData->len > 0) {
    // new arrived data with different tags value, save current value into disk first
    writeDataToDisk(pTSBuf);
  } else {
    expandBuffer(ptsData, len);
  }

  taosVariantAssign(&pTSBuf->block.tag, tag);
  memcpy(ptsData->rawBuf + ptsData->len, pData, (size_t)len);

  // todo check return value
  setCheckTSOrder(pTSBuf, pData, len);

  ptsData->len += len;
  pBlockInfo->len += len;

  pTSBuf->numOfTotal += len / TSDB_KEYSIZE;

  // the size of raw data exceeds the size of the default prepared buffer, so
  // during getBufBlock, the output buffer needs to be large enough.
  if (ptsData->len >= ptsData->threshold) {
    writeDataToDisk(pTSBuf);
    shrinkBuffer(ptsData);
  }

  tsBufResetPos(pTSBuf);
}

void tsBufFlush(STSBuf* pTSBuf) {
  if (pTSBuf->tsData.len <= 0) {
    return;
  }

  writeDataToDisk(pTSBuf);
  shrinkBuffer(&pTSBuf->tsData);

  STSGroupBlockInfoEx* pBlockInfoEx = tsBufGetLastGroupInfo(pTSBuf);

  // update prev vnode length info in file
  TSBufUpdateGroupInfo(pTSBuf, pTSBuf->numOfGroups - 1, &pBlockInfoEx->info);

  // save the ts order into header
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfGroup = pTSBuf->numOfGroups, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);
}

static int32_t tsBufFindGroupById(STSGroupBlockInfoEx* pGroupInfoEx, int32_t numOfGroups, int32_t id) {
  int32_t j = -1;
  for (int32_t i = 0; i < numOfGroups; ++i) {
    if (pGroupInfoEx[i].info.id == id) {
      j = i;
      break;
    }
  }

  return j;
}

// todo opt performance by cache blocks info
static int32_t tsBufFindBlock(STSBuf* pTSBuf, STSGroupBlockInfo* pBlockInfo, int32_t blockIndex) {
  if (taosLSeekFile(pTSBuf->pFile, pBlockInfo->offset, SEEK_SET) != 0) {
    return -1;
  }

  // sequentially read the compressed data blocks, start from the beginning of the comp data block of this vnode
  int32_t i = 0;
  bool    decomp = false;

  while ((i++) <= blockIndex) {
    if (readDataFromDisk(pTSBuf, TSDB_ORDER_ASC, decomp) == NULL) {
      return -1;
    }
  }

  // set the file position to be the end of previous comp block
  if (pTSBuf->cur.order == TSDB_ORDER_DESC) {
    STSBlock* pBlock = &pTSBuf->block;
    int32_t   compBlockSize =
        pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + getTagAreaLength(&pBlock->tag);
    int32_t ret = taosLSeekFile(pTSBuf->pFile, -compBlockSize, SEEK_CUR);
    UNUSED(ret);
  }

  return 0;
}

static int32_t tsBufFindBlockByTag(STSBuf* pTSBuf, STSGroupBlockInfo* pBlockInfo, SVariant* tag) {
  bool decomp = false;

  int64_t offset = 0;
  if (pTSBuf->cur.order == TSDB_ORDER_ASC) {
    offset = pBlockInfo->offset;
  } else {  // reversed traverse starts from the end of block
    offset = pBlockInfo->offset + pBlockInfo->compLen;
  }

  if (taosLSeekFile(pTSBuf->pFile, (int32_t)offset, SEEK_SET) != 0) {
    return -1;
  }

  for (int32_t i = 0; i < pBlockInfo->numOfBlocks; ++i) {
    if (readDataFromDisk(pTSBuf, pTSBuf->cur.order, decomp) == NULL) {
      return -1;
    }

    if (taosVariantCompare(&pTSBuf->block.tag, tag) == 0) {
      return (pTSBuf->cur.order == TSDB_ORDER_ASC) ? i : (pBlockInfo->numOfBlocks - (i + 1));
    }
  }

  return -1;
}

static void tsBufGetBlock(STSBuf* pTSBuf, int32_t groupIndex, int32_t blockIndex) {
  STSGroupBlockInfo* pBlockInfo = &pTSBuf->pData[groupIndex].info;
  if (pBlockInfo->numOfBlocks <= blockIndex) {
    ASSERT(false);
  }

  STSCursor* pCur = &pTSBuf->cur;
  if (pCur->vgroupIndex == groupIndex && ((pCur->blockIndex <= blockIndex && pCur->order == TSDB_ORDER_ASC) ||
                                          (pCur->blockIndex >= blockIndex && pCur->order == TSDB_ORDER_DESC))) {
    int32_t i = 0;
    bool    decomp = false;
    int32_t step = abs(blockIndex - pCur->blockIndex);

    while ((++i) <= step) {
      if (readDataFromDisk(pTSBuf, pCur->order, decomp) == NULL) {
        return;
      }
    }
  } else {
    if (tsBufFindBlock(pTSBuf, pBlockInfo, blockIndex) == -1) {
      ASSERT(false);
    }
  }

  STSBlock* pBlock = &pTSBuf->block;

  size_t s = pBlock->numOfElem * TSDB_KEYSIZE;

  /*
   * In order to accommodate all the qualified data, the actual buffer size for one block with identical tags value
   * may exceed the maximum allowed size during *tsBufAppend* function by invoking expandBuffer function
   */
  if (s > pTSBuf->tsData.allocSize) {
    expandBuffer(&pTSBuf->tsData, (int32_t)s);
  }

  pTSBuf->tsData.len =
      tsDecompressTimestamp(pBlock->payload, pBlock->compLen, pBlock->numOfElem, pTSBuf->tsData.rawBuf,
                            pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);

  ASSERT((pTSBuf->tsData.len / TSDB_KEYSIZE == pBlock->numOfElem) && (pTSBuf->tsData.allocSize >= pTSBuf->tsData.len));

  pCur->vgroupIndex = groupIndex;
  pCur->blockIndex = blockIndex;

  pCur->tsIndex = (pCur->order == TSDB_ORDER_ASC) ? 0 : pBlock->numOfElem - 1;
}

static int32_t doUpdateGroupInfo(STSBuf* pTSBuf, int64_t offset, STSGroupBlockInfo* pVInfo) {
  if (offset < 0 || offset >= getDataStartOffset()) {
    return -1;
  }

  if (taosLSeekFile(pTSBuf->pFile, (int32_t)offset, SEEK_SET) != 0) {
    return -1;
  }

  taosWriteFile(pTSBuf->pFile, pVInfo, sizeof(STSGroupBlockInfo));
  return 0;
}

STSGroupBlockInfo* tsBufGetGroupBlockInfo(STSBuf* pTSBuf, int32_t id) {
  int32_t j = tsBufFindGroupById(pTSBuf->pData, pTSBuf->numOfGroups, id);
  if (j == -1) {
    return NULL;
  }

  return &pTSBuf->pData[j].info;
}

int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader) {
  if ((pTSBuf->pFile == NULL) || pHeader == NULL || pHeader->numOfGroup == 0 || pHeader->magic != TS_COMP_FILE_MAGIC) {
    return -1;
  }

  if (pHeader->tsOrder != TSDB_ORDER_ASC && pHeader->tsOrder != TSDB_ORDER_DESC) {
    return -1;
  }

  int32_t r = taosLSeekFile(pTSBuf->pFile, 0, SEEK_SET);
  if (r != 0) {
    //    qError("fseek failed, errno:%d", errno);
    return -1;
  }

  size_t ws = taosWriteFile(pTSBuf->pFile, pHeader, sizeof(STSBufFileHeader));
  if (ws != 1) {
    //    qError("ts update header fwrite failed, size:%d, expected size:%d", (int32_t)ws,
    //    (int32_t)sizeof(STSBufFileHeader));
    return -1;
  }
  return 0;
}

bool tsBufNextPos(STSBuf* pTSBuf) {
  if (pTSBuf == NULL || pTSBuf->numOfGroups == 0) {
    return false;
  }

  STSCursor* pCur = &pTSBuf->cur;

  // get the first/last position according to traverse order
  if (pCur->vgroupIndex == -1) {
    if (pCur->order == TSDB_ORDER_ASC) {
      tsBufGetBlock(pTSBuf, 0, 0);

      if (pTSBuf->block.numOfElem == 0) {  // the whole list is empty, return
        tsBufResetPos(pTSBuf);
        return false;
      } else {
        return true;
      }

    } else {  // get the last timestamp record in the last block of the last vnode
      ASSERT(pTSBuf->numOfGroups > 0);

      int32_t groupIndex = pTSBuf->numOfGroups - 1;
      pCur->vgroupIndex = groupIndex;

      int32_t            id = pTSBuf->pData[pCur->vgroupIndex].info.id;
      STSGroupBlockInfo* pBlockInfo = tsBufGetGroupBlockInfo(pTSBuf, id);
      int32_t            blockIndex = pBlockInfo->numOfBlocks - 1;

      tsBufGetBlock(pTSBuf, groupIndex, blockIndex);

      pCur->tsIndex = pTSBuf->block.numOfElem - 1;
      if (pTSBuf->block.numOfElem == 0) {
        tsBufResetPos(pTSBuf);
        return false;
      } else {
        return true;
      }
    }
  }

  int32_t step = pCur->order == TSDB_ORDER_ASC ? 1 : -1;

  while (1) {
    ASSERT(pTSBuf->tsData.len == pTSBuf->block.numOfElem * TSDB_KEYSIZE);

    if ((pCur->order == TSDB_ORDER_ASC && pCur->tsIndex >= pTSBuf->block.numOfElem - 1) ||
        (pCur->order == TSDB_ORDER_DESC && pCur->tsIndex <= 0)) {
      int32_t id = pTSBuf->pData[pCur->vgroupIndex].info.id;

      STSGroupBlockInfo* pBlockInfo = tsBufGetGroupBlockInfo(pTSBuf, id);
      if (pBlockInfo == NULL || (pCur->blockIndex >= pBlockInfo->numOfBlocks - 1 && pCur->order == TSDB_ORDER_ASC) ||
          (pCur->blockIndex <= 0 && pCur->order == TSDB_ORDER_DESC)) {
        if ((pCur->vgroupIndex >= pTSBuf->numOfGroups - 1 && pCur->order == TSDB_ORDER_ASC) ||
            (pCur->vgroupIndex <= 0 && pCur->order == TSDB_ORDER_DESC)) {
          pCur->vgroupIndex = -1;
          return false;
        }

        if (pBlockInfo == NULL) {
          return false;
        }

        int32_t blockIndex = (pCur->order == TSDB_ORDER_ASC) ? 0 : (pBlockInfo->numOfBlocks - 1);
        tsBufGetBlock(pTSBuf, pCur->vgroupIndex + step, blockIndex);
        break;

      } else {
        tsBufGetBlock(pTSBuf, pCur->vgroupIndex, pCur->blockIndex + step);
        break;
      }
    } else {
      pCur->tsIndex += step;
      break;
    }
  }

  return true;
}

void tsBufResetPos(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return;
  }

  pTSBuf->cur = (STSCursor){.tsIndex = -1, .blockIndex = -1, .vgroupIndex = -1, .order = pTSBuf->cur.order};
}

STSElem tsBufGetElem(STSBuf* pTSBuf) {
  STSElem elem1 = {.id = -1};
  if (pTSBuf == NULL) {
    return elem1;
  }

  STSCursor* pCur = &pTSBuf->cur;
  if (pCur != NULL && pCur->vgroupIndex < 0) {
    return elem1;
  }

  STSBlock* pBlock = &pTSBuf->block;

  elem1.id = pTSBuf->pData[pCur->vgroupIndex].info.id;
  elem1.ts = *(TSKEY*)(pTSBuf->tsData.rawBuf + pCur->tsIndex * TSDB_KEYSIZE);
  elem1.tag = &pBlock->tag;

  return elem1;
}

/**
 * current only support ts comp data from two vnode merge
 * @param pDestBuf
 * @param pSrcBuf
 * @param id
 * @return
 */
int32_t tsBufMerge(STSBuf* pDestBuf, const STSBuf* pSrcBuf) {
  if (pDestBuf == NULL || pSrcBuf == NULL || pSrcBuf->numOfGroups <= 0) {
    return 0;
  }

  if (pDestBuf->numOfGroups + pSrcBuf->numOfGroups > TS_COMP_FILE_GROUP_MAX) {
    return -1;
  }

  // src can only have one vnode index
  ASSERT(pSrcBuf->numOfGroups == 1);

  // there are data in buffer, flush to disk first
  tsBufFlush(pDestBuf);

  // compared with the last vnode id
  int32_t id = tsBufGetLastGroupInfo((STSBuf*)pSrcBuf)->info.id;
  if (id != tsBufGetLastGroupInfo(pDestBuf)->info.id) {
    int32_t oldSize = pDestBuf->numOfGroups;
    int32_t newSize = oldSize + pSrcBuf->numOfGroups;

    if (pDestBuf->numOfAlloc < newSize) {
      pDestBuf->numOfAlloc = newSize;

      STSGroupBlockInfoEx* tmp = taosMemoryRealloc(pDestBuf->pData, sizeof(STSGroupBlockInfoEx) * newSize);
      if (tmp == NULL) {
        return -1;
      }

      pDestBuf->pData = tmp;
    }

    // directly copy the vnode index information
    memcpy(&pDestBuf->pData[oldSize], pSrcBuf->pData, (size_t)pSrcBuf->numOfGroups * sizeof(STSGroupBlockInfoEx));

    // set the new offset value
    for (int32_t i = 0; i < pSrcBuf->numOfGroups; ++i) {
      STSGroupBlockInfoEx* pBlockInfoEx = &pDestBuf->pData[i + oldSize];
      pBlockInfoEx->info.offset = (pSrcBuf->pData[i].info.offset - getDataStartOffset()) + pDestBuf->fileSize;
      pBlockInfoEx->info.id = id;
    }

    pDestBuf->numOfGroups = newSize;
  } else {
    STSGroupBlockInfoEx* pBlockInfoEx = tsBufGetLastGroupInfo(pDestBuf);

    pBlockInfoEx->len += pSrcBuf->pData[0].len;
    pBlockInfoEx->info.numOfBlocks += pSrcBuf->pData[0].info.numOfBlocks;
    pBlockInfoEx->info.compLen += pSrcBuf->pData[0].info.compLen;
    pBlockInfoEx->info.id = id;
  }

  int32_t r = taosLSeekFile(pDestBuf->pFile, 0, SEEK_END);
  ASSERT(r == 0);

  int64_t offset = getDataStartOffset();
  int32_t size = (int32_t)pSrcBuf->fileSize - (int32_t)offset;
  int64_t written = taosFSendFile(pDestBuf->pFile, pSrcBuf->pFile, &offset, size);

  if (written == -1 || written != size) {
    return -1;
  }

  pDestBuf->numOfTotal += pSrcBuf->numOfTotal;

  int32_t oldSize = pDestBuf->fileSize;

  // file meta data may be cached, close and reopen the file for accurate file size.
  taosCloseFile(&pDestBuf->pFile);
  // pDestBuf->pFile = fopen(pDestBuf->path, "rb+");
  pDestBuf->pFile = taosOpenFile(pDestBuf->path, TD_FILE_WRITE | TD_FILE_READ);
  if (pDestBuf->pFile == NULL) {
    return -1;
  }

  int64_t file_size;
  if (taosFStatFile(pDestBuf->pFile, &file_size, NULL) != 0) {
    return -1;
  }
  pDestBuf->fileSize = (uint32_t)file_size;

  ASSERT(pDestBuf->fileSize == oldSize + size);

  return 0;
}

STSBuf* tsBufCreateFromCompBlocks(const char* pData, int32_t numOfBlocks, int32_t len, int32_t order, int32_t id) {
  STSBuf* pTSBuf = tsBufCreate(true, order);

  STSGroupBlockInfo* pBlockInfo = &(addOneGroupInfo(pTSBuf, 0)->info);
  pBlockInfo->numOfBlocks = numOfBlocks;
  pBlockInfo->compLen = len;
  pBlockInfo->offset = getDataStartOffset();
  pBlockInfo->id = id;

  // update prev vnode length info in file
  TSBufUpdateGroupInfo(pTSBuf, pTSBuf->numOfGroups - 1, pBlockInfo);

  int32_t ret = taosLSeekFile(pTSBuf->pFile, pBlockInfo->offset, SEEK_SET);
  if (ret == -1) {
    //    qError("fseek failed, errno:%d", errno);
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  size_t sz = taosWriteFile(pTSBuf->pFile, (void*)pData, len);
  if (sz != len) {
    //    qError("ts data fwrite failed, write size:%d, expected size:%d", (int32_t)sz, len);
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  pTSBuf->fileSize += len;

  pTSBuf->tsOrder = order;
  if (order != TSDB_ORDER_ASC && order != TSDB_ORDER_DESC) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfGroup = pTSBuf->numOfGroups, .tsOrder = pTSBuf->tsOrder};
  if (STSBufUpdateHeader(pTSBuf, &header) < 0) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  // TODO taosFsync??
  //  if (taosFsync(fileno(pTSBuf->pFile)) == -1) {
  ////    qError("fsync failed, errno:%d", errno);
  //    tsBufDestroy(pTSBuf);
  //    return NULL;
  //  }

  return pTSBuf;
}

STSElem tsBufGetElemStartPos(STSBuf* pTSBuf, int32_t id, SVariant* tag) {
  STSElem elem = {.id = -1};

  if (pTSBuf == NULL) {
    return elem;
  }

  int32_t j = tsBufFindGroupById(pTSBuf->pData, pTSBuf->numOfGroups, id);
  if (j == -1) {
    return elem;
  }

  // for debug purpose
  //  tsBufDisplay(pTSBuf);

  STSCursor*         pCur = &pTSBuf->cur;
  STSGroupBlockInfo* pBlockInfo = &pTSBuf->pData[j].info;

  int32_t blockIndex = tsBufFindBlockByTag(pTSBuf, pBlockInfo, tag);
  if (blockIndex < 0) {
    return elem;
  }

  pCur->vgroupIndex = j;
  pCur->blockIndex = blockIndex;
  tsBufGetBlock(pTSBuf, j, blockIndex);

  return tsBufGetElem(pTSBuf);
}

STSCursor tsBufGetCursor(STSBuf* pTSBuf) {
  STSCursor c = {.vgroupIndex = -1};
  if (pTSBuf == NULL) {
    return c;
  }

  return pTSBuf->cur;
}

void tsBufSetCursor(STSBuf* pTSBuf, STSCursor* pCur) {
  if (pTSBuf == NULL || pCur == NULL) {
    return;
  }

  //  assert(pCur->vgroupIndex != -1 && pCur->tsIndex >= 0 && pCur->blockIndex >= 0);
  if (pCur->vgroupIndex != -1) {
    tsBufGetBlock(pTSBuf, pCur->vgroupIndex, pCur->blockIndex);
  }

  pTSBuf->cur = *pCur;
}

void tsBufSetTraverseOrder(STSBuf* pTSBuf, int32_t order) {
  if (pTSBuf == NULL) {
    return;
  }

  pTSBuf->cur.order = order;
}

STSBuf* tsBufClone(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return NULL;
  }

  tsBufFlush(pTSBuf);

  return tsBufCreateFromFile(pTSBuf->path, false);
}

void tsBufDisplay(STSBuf* pTSBuf) {
  printf("-------start of ts comp file-------\n");
  printf("number of vnode:%d\n", pTSBuf->numOfGroups);

  int32_t old = pTSBuf->cur.order;
  pTSBuf->cur.order = TSDB_ORDER_ASC;

  tsBufResetPos(pTSBuf);

  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    if (elem.tag->nType == TSDB_DATA_TYPE_BIGINT) {
      printf("%d-%" PRId64 "-%" PRId64 "\n", elem.id, elem.tag->i, elem.ts);
    }
  }

  pTSBuf->cur.order = old;
  printf("-------end of ts comp file-------\n");
}

static int32_t getDataStartOffset() {
  return sizeof(STSBufFileHeader) + TS_COMP_FILE_GROUP_MAX * sizeof(STSGroupBlockInfo);
}

// update prev vnode length info in file
static void TSBufUpdateGroupInfo(STSBuf* pTSBuf, int32_t index, STSGroupBlockInfo* pBlockInfo) {
  int32_t offset = sizeof(STSBufFileHeader) + index * sizeof(STSGroupBlockInfo);
  doUpdateGroupInfo(pTSBuf, offset, pBlockInfo);
}

static STSBuf* allocResForTSBuf(STSBuf* pTSBuf) {
  const int32_t INITIAL_GROUPINFO_SIZE = 4;

  pTSBuf->numOfAlloc = INITIAL_GROUPINFO_SIZE;
  pTSBuf->pData = taosMemoryCalloc(pTSBuf->numOfAlloc, sizeof(STSGroupBlockInfoEx));
  if (pTSBuf->pData == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  pTSBuf->tsData.rawBuf = taosMemoryMalloc(MEM_BUF_SIZE);
  if (pTSBuf->tsData.rawBuf == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  pTSBuf->bufSize = MEM_BUF_SIZE;
  pTSBuf->tsData.threshold = MEM_BUF_SIZE;
  pTSBuf->tsData.allocSize = MEM_BUF_SIZE;

  pTSBuf->assistBuf = taosMemoryMalloc(MEM_BUF_SIZE);
  if (pTSBuf->assistBuf == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  pTSBuf->block.payload = taosMemoryMalloc(MEM_BUF_SIZE);
  if (pTSBuf->block.payload == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }

  pTSBuf->fileSize += getDataStartOffset();
  return pTSBuf;
}

int32_t tsBufGetNumOfGroup(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return 0;
  }

  return pTSBuf->numOfGroups;
}

void tsBufGetGroupIdList(STSBuf* pTSBuf, int32_t* num, int32_t** id) {
  int32_t size = tsBufGetNumOfGroup(pTSBuf);
  if (num != NULL) {
    *num = size;
  }

  *id = NULL;
  if (size == 0) {
    return;
  }

  (*id) = taosMemoryMalloc(tsBufGetNumOfGroup(pTSBuf) * sizeof(int32_t));

  for (int32_t i = 0; i < size; ++i) {
    (*id)[i] = pTSBuf->pData[i].info.id;
  }
}

int32_t dumpFileBlockByGroupId(STSBuf* pTSBuf, int32_t groupIndex, void* buf, int32_t* len, int32_t* numOfBlocks) {
  ASSERT(groupIndex >= 0 && groupIndex < pTSBuf->numOfGroups);
  STSGroupBlockInfo* pBlockInfo = &pTSBuf->pData[groupIndex].info;

  *len = 0;
  *numOfBlocks = 0;

  if (taosLSeekFile(pTSBuf->pFile, pBlockInfo->offset, SEEK_SET) != 0) {
    int32_t code = TAOS_SYSTEM_ERROR(taosGetErrorFile(pTSBuf->pFile));
    //    qError("%p: fseek failed: %s", pSql, tstrerror(code));
    return code;
  }

  size_t s = taosReadFile(pTSBuf->pFile, buf, pBlockInfo->compLen);
  if (s != pBlockInfo->compLen) {
    int32_t code = TAOS_SYSTEM_ERROR(taosGetErrorFile(pTSBuf->pFile));
    //    tscError("%p: fread didn't return expected data: %s", pSql, tstrerror(code));
    return code;
  }

  *len = pBlockInfo->compLen;
  *numOfBlocks = pBlockInfo->numOfBlocks;

  return TSDB_CODE_SUCCESS;
}

STSElem tsBufFindElemStartPosByTag(STSBuf* pTSBuf, SVariant* pTag) {
  STSElem el = {.id = -1};

  for (int32_t i = 0; i < pTSBuf->numOfGroups; ++i) {
    el = tsBufGetElemStartPos(pTSBuf, pTSBuf->pData[i].info.id, pTag);
    if (el.id == pTSBuf->pData[i].info.id) {
      return el;
    }
  }

  return el;
}

bool tsBufIsValidElem(STSElem* pElem) { return pElem->id >= 0; }
