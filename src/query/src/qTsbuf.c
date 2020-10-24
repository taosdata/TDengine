#include "qTsbuf.h"
#include "taoserror.h"
#include "tscompression.h"
#include "tutil.h"

static int32_t getDataStartOffset();
static void TSBufUpdateVnodeInfo(STSBuf* pTSBuf, int32_t index, STSVnodeBlockInfo* pBlockInfo);
static STSBuf* allocResForTSBuf(STSBuf* pTSBuf);
static int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader);

/**
 * todo error handling
 * support auto closeable tmp file
 * @param path
 * @return
 */
STSBuf* tsBufCreate(bool autoDelete, int32_t order) {
  STSBuf* pTSBuf = calloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }
  
  taosGetTmpfilePath("join", pTSBuf->path);
  pTSBuf->f = fopen(pTSBuf->path, "w+");
  if (pTSBuf->f == NULL) {
    free(pTSBuf);
    return NULL;
  }
  
  if (NULL == allocResForTSBuf(pTSBuf)) {
    return NULL;
  }
  
  // update the header info
  STSBufFileHeader header = {.magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = TSDB_ORDER_ASC};
  STSBufUpdateHeader(pTSBuf, &header);
  
  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSDB_ORDER_ASC;
  
  pTSBuf->autoDelete = autoDelete;
  pTSBuf->tsOrder = order;
  
  return pTSBuf;
}

STSBuf* tsBufCreateFromFile(const char* path, bool autoDelete) {
  STSBuf* pTSBuf = calloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }
  
  tstrncpy(pTSBuf->path, path, sizeof(pTSBuf->path));
  
  pTSBuf->f = fopen(pTSBuf->path, "r+");
  if (pTSBuf->f == NULL) {
    free(pTSBuf);
    return NULL;
  }
  
  if (allocResForTSBuf(pTSBuf) == NULL) {
    return NULL;
  }
  
  // validate the file magic number
  STSBufFileHeader header = {0};
  int32_t ret = fseek(pTSBuf->f, 0, SEEK_SET);
  UNUSED(ret);
  size_t sz = fread(&header, 1, sizeof(STSBufFileHeader), pTSBuf->f);
  UNUSED(sz);

  // invalid file
  if (header.magic != TS_COMP_FILE_MAGIC) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  
  if (header.numOfVnode > pTSBuf->numOfAlloc) {
    pTSBuf->numOfAlloc = header.numOfVnode;
    STSVnodeBlockInfoEx* tmp = realloc(pTSBuf->pData, sizeof(STSVnodeBlockInfoEx) * pTSBuf->numOfAlloc);
    if (tmp == NULL) {
      tsBufDestroy(pTSBuf);
      return NULL;
    }
    
    pTSBuf->pData = tmp;
  }
  
  pTSBuf->numOfVnodes = header.numOfVnode;
  
  // check the ts order
  pTSBuf->tsOrder = header.tsOrder;
  if (pTSBuf->tsOrder != TSDB_ORDER_ASC && pTSBuf->tsOrder != TSDB_ORDER_DESC) {
//    tscError("invalid order info in buf:%d", pTSBuf->tsOrder);
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  
  size_t infoSize = sizeof(STSVnodeBlockInfo) * pTSBuf->numOfVnodes;
  
  STSVnodeBlockInfo* buf = (STSVnodeBlockInfo*)calloc(1, infoSize);
  if (buf == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL; 
  } 
  
  //int64_t pos = ftell(pTSBuf->f); //pos not used
  sz = fread(buf, infoSize, 1, pTSBuf->f);
  UNUSED(sz);
  
  // the length value for each vnode is not kept in file, so does not set the length value
  for (int32_t i = 0; i < pTSBuf->numOfVnodes; ++i) {
    STSVnodeBlockInfoEx* pBlockList = &pTSBuf->pData[i];
    memcpy(&pBlockList->info, &buf[i], sizeof(STSVnodeBlockInfo));
  }
  free(buf);
  
  ret = fseek(pTSBuf->f, 0, SEEK_END);
  UNUSED(ret);
  
  struct stat fileStat;
  if (fstat(fileno(pTSBuf->f), &fileStat) != 0) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  
  pTSBuf->fileSize = (uint32_t)fileStat.st_size;
  tsBufResetPos(pTSBuf);
  
  // ascending by default
  pTSBuf->cur.order = TSDB_ORDER_ASC;
  pTSBuf->autoDelete = autoDelete;
  
//  tscDebug("create tsBuf from file:%s, fd:%d, size:%d, numOfVnode:%d, autoDelete:%d", pTSBuf->path, fileno(pTSBuf->f),
//           pTSBuf->fileSize, pTSBuf->numOfVnodes, pTSBuf->autoDelete);
  
  return pTSBuf;
}

void* tsBufDestroy(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return NULL;
  }
  
  taosTFree(pTSBuf->assistBuf);
  taosTFree(pTSBuf->tsData.rawBuf);
  
  taosTFree(pTSBuf->pData);
  taosTFree(pTSBuf->block.payload);
  
  fclose(pTSBuf->f);
  
  if (pTSBuf->autoDelete) {
//    ("tsBuf %p destroyed, delete tmp file:%s", pTSBuf, pTSBuf->path);
    unlink(pTSBuf->path);
  } else {
//    tscDebug("tsBuf %p destroyed, tmp file:%s, remains", pTSBuf, pTSBuf->path);
  }
  
  free(pTSBuf);
  return NULL;
}

static STSVnodeBlockInfoEx* tsBufGetLastVnodeInfo(STSBuf* pTSBuf) {
  int32_t last = pTSBuf->numOfVnodes - 1;
  
  assert(last >= 0);
  return &pTSBuf->pData[last];
}

static STSVnodeBlockInfoEx* addOneVnodeInfo(STSBuf* pTSBuf, int32_t vnodeId) {
  if (pTSBuf->numOfAlloc <= pTSBuf->numOfVnodes) {
    uint32_t newSize = (uint32_t)(pTSBuf->numOfAlloc * 1.5);
    assert((int32_t)newSize > pTSBuf->numOfAlloc);
    
    STSVnodeBlockInfoEx* tmp = (STSVnodeBlockInfoEx*)realloc(pTSBuf->pData, sizeof(STSVnodeBlockInfoEx) * newSize);
    if (tmp == NULL) {
      return NULL;
    }
    
    pTSBuf->pData = tmp;
    pTSBuf->numOfAlloc = newSize;
    memset(&pTSBuf->pData[pTSBuf->numOfVnodes], 0, sizeof(STSVnodeBlockInfoEx) * (newSize - pTSBuf->numOfVnodes));
  }
  
  if (pTSBuf->numOfVnodes > 0) {
    STSVnodeBlockInfoEx* pPrevBlockInfoEx = tsBufGetLastVnodeInfo(pTSBuf);
    
    // update prev vnode length info in file
    TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, &pPrevBlockInfoEx->info);
  }
  
  // set initial value for vnode block
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[pTSBuf->numOfVnodes].info;
  pBlockInfo->vnode = vnodeId;
  pBlockInfo->offset = pTSBuf->fileSize;
  assert(pBlockInfo->offset >= getDataStartOffset());
  
  // update vnode info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes, pBlockInfo);
  
  // add one vnode info
  pTSBuf->numOfVnodes += 1;
  
  // update the header info
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  
  STSBufUpdateHeader(pTSBuf, &header);
  return tsBufGetLastVnodeInfo(pTSBuf);
}

static void shrinkBuffer(STSList* ptsData) {
  // shrink tmp buffer size if it consumes too many memory compared to the pre-defined size
  if (ptsData->allocSize >= ptsData->threshold * 2) {
    ptsData->rawBuf = realloc(ptsData->rawBuf, MEM_BUF_SIZE);
    ptsData->allocSize = MEM_BUF_SIZE;
  }
}

static void writeDataToDisk(STSBuf* pTSBuf) {
  if (pTSBuf->tsData.len == 0) {
    return;
  }
  
  STSBlock* pBlock = &pTSBuf->block;
  STSList*  pTsData = &pTSBuf->tsData;

  pBlock->numOfElem = pTsData->len / TSDB_KEYSIZE;
  pBlock->compLen =
      tsCompressTimestamp(pTsData->rawBuf, pTsData->len, pTsData->len/TSDB_KEYSIZE, pBlock->payload, pTsData->allocSize,
          TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);
  
  int64_t r = fseek(pTSBuf->f, pTSBuf->fileSize, SEEK_SET);
  assert(r == 0);
  
  /*
   * format for output data:
   * 1. tags, number of ts, size after compressed, payload, size after compressed
   * 2. tags, number of ts, size after compressed, payload, size after compressed
   *
   * both side has the compressed length is used to support load data forwards/backwords.
   */
  int32_t metaLen = 0;
  metaLen += (int32_t)fwrite(&pBlock->tag.nType, 1, sizeof(pBlock->tag.nType), pTSBuf->f);
  metaLen += (int32_t)fwrite(&pBlock->tag.nLen, 1, sizeof(pBlock->tag.nLen), pTSBuf->f);

  if (pBlock->tag.nType == TSDB_DATA_TYPE_BINARY || pBlock->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    metaLen += (int32_t)fwrite(pBlock->tag.pz, 1, (size_t)pBlock->tag.nLen, pTSBuf->f);
  } else if (pBlock->tag.nType != TSDB_DATA_TYPE_NULL) {
    metaLen += (int32_t)fwrite(&pBlock->tag.i64Key, 1, sizeof(int64_t), pTSBuf->f);
  }

  fwrite(&pBlock->numOfElem, sizeof(pBlock->numOfElem), 1, pTSBuf->f);
  fwrite(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);
  fwrite(pBlock->payload, (size_t)pBlock->compLen, 1, pTSBuf->f);
  fwrite(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);
  
  int32_t blockSize = metaLen + sizeof(pBlock->numOfElem) + sizeof(pBlock->compLen) * 2 + pBlock->compLen;
  pTSBuf->fileSize += blockSize;
  
  pTSBuf->tsData.len = 0;
  
  STSVnodeBlockInfoEx* pVnodeBlockInfoEx = tsBufGetLastVnodeInfo(pTSBuf);
  
  pVnodeBlockInfoEx->info.compLen += blockSize;
  pVnodeBlockInfoEx->info.numOfBlocks += 1;
  
  shrinkBuffer(&pTSBuf->tsData);
}

static void expandBuffer(STSList* ptsData, int32_t inputSize) {
  if (ptsData->allocSize - ptsData->len < inputSize) {
    int32_t newSize = inputSize + ptsData->len;
    char*   tmp = realloc(ptsData->rawBuf, (size_t)newSize);
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
  void* tmp = pBlock->payload;
  memset(pBlock, 0, sizeof(STSBlock));
  pBlock->payload = tmp;
  
  if (order == TSDB_ORDER_DESC) {
    /*
     * set the right position for the reversed traverse, the reversed traverse is started from
     * the end of each comp data block
     */
    int32_t ret = fseek(pTSBuf->f, -(int32_t)(sizeof(pBlock->padding)), SEEK_CUR);
    size_t sz = fread(&pBlock->padding, sizeof(pBlock->padding), 1, pTSBuf->f);
    UNUSED(sz); 
    
    pBlock->compLen = pBlock->padding;
    int32_t offset = pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    ret = fseek(pTSBuf->f, -offset, SEEK_CUR);
    UNUSED(ret);
  }

  fread(&pBlock->tag.nType, sizeof(pBlock->tag.nType), 1, pTSBuf->f);
  fread(&pBlock->tag.nLen, sizeof(pBlock->tag.nLen), 1, pTSBuf->f);

  // NOTE: mix types tags are not supported
  size_t sz = 0;
  if (pBlock->tag.nType == TSDB_DATA_TYPE_BINARY || pBlock->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    char* tp = realloc(pBlock->tag.pz, pBlock->tag.nLen + 1);
    assert(tp != NULL);

    memset(tp, 0, pBlock->tag.nLen + 1);
    pBlock->tag.pz = tp;

    sz = fread(pBlock->tag.pz, (size_t)pBlock->tag.nLen, 1, pTSBuf->f);
  } else if (pBlock->tag.nType != TSDB_DATA_TYPE_NULL) {
    sz = fread(&pBlock->tag.i64Key, sizeof(int64_t), 1, pTSBuf->f);
  }

  sz = fread(&pBlock->numOfElem, sizeof(pBlock->numOfElem), 1, pTSBuf->f);
  UNUSED(sz);
  sz = fread(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);
  UNUSED(sz);
  sz = fread(pBlock->payload, (size_t)pBlock->compLen, 1, pTSBuf->f);
  UNUSED(sz);
  
  if (decomp) {
    pTSBuf->tsData.len =
        tsDecompressTimestamp(pBlock->payload, pBlock->compLen, pBlock->numOfElem, pTSBuf->tsData.rawBuf,
                              pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);
  }
  
  // read the comp length at the length of comp block
  sz = fread(&pBlock->padding, sizeof(pBlock->padding), 1, pTSBuf->f);
  UNUSED(sz);
  
  // for backwards traverse, set the start position at the end of previous block
  if (order == TSDB_ORDER_DESC) {
    int32_t offset = pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    int32_t r = fseek(pTSBuf->f, -offset, SEEK_CUR);
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

void tsBufAppend(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag, const char* pData, int32_t len) {
  STSVnodeBlockInfoEx* pBlockInfo = NULL;
  STSList*             ptsData = &pTSBuf->tsData;
  
  if (pTSBuf->numOfVnodes == 0 || tsBufGetLastVnodeInfo(pTSBuf)->info.vnode != vnodeId) {
    writeDataToDisk(pTSBuf);
    shrinkBuffer(ptsData);
    
    pBlockInfo = addOneVnodeInfo(pTSBuf, vnodeId);
  } else {
    pBlockInfo = tsBufGetLastVnodeInfo(pTSBuf);
  }
  
  assert(pBlockInfo->info.vnode == vnodeId);

  if ((tVariantCompare(&pTSBuf->block.tag, tag) != 0) && ptsData->len > 0) {
    // new arrived data with different tags value, save current value into disk first
    writeDataToDisk(pTSBuf);
  } else {
    expandBuffer(ptsData, len);
  }

  tVariantAssign(&pTSBuf->block.tag, tag);
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
  
  STSVnodeBlockInfoEx* pBlockInfoEx = tsBufGetLastVnodeInfo(pTSBuf);
  
  // update prev vnode length info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, &pBlockInfoEx->info);
  
  // save the ts order into header
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);
  
  fsync(fileno(pTSBuf->f));
}

static int32_t tsBufFindVnodeIndexFromId(STSVnodeBlockInfoEx* pVnodeInfoEx, int32_t numOfVnodes, int32_t vnodeId) {
  int32_t j = -1;
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (pVnodeInfoEx[i].info.vnode == vnodeId) {
      j = i;
      break;
    }
  }
  
  return j;
}

// todo opt performance by cache blocks info
static int32_t tsBufFindBlock(STSBuf* pTSBuf, STSVnodeBlockInfo* pBlockInfo, int32_t blockIndex) {
  if (fseek(pTSBuf->f, pBlockInfo->offset, SEEK_SET) != 0) {
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
        pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    int32_t ret = fseek(pTSBuf->f, -compBlockSize, SEEK_CUR);
    UNUSED(ret);
  }
  
  return 0;
}

static int32_t tsBufFindBlockByTag(STSBuf* pTSBuf, STSVnodeBlockInfo* pBlockInfo, tVariant* tag) {
  bool decomp = false;
  
  int64_t offset = 0;
  if (pTSBuf->cur.order == TSDB_ORDER_ASC) {
    offset = pBlockInfo->offset;
  } else {  // reversed traverse starts from the end of block
    offset = pBlockInfo->offset + pBlockInfo->compLen;
  }
  
  if (fseek(pTSBuf->f, (int32_t)offset, SEEK_SET) != 0) {
    return -1;
  }
  
  for (int32_t i = 0; i < pBlockInfo->numOfBlocks; ++i) {
    if (readDataFromDisk(pTSBuf, pTSBuf->cur.order, decomp) == NULL) {
      return -1;
    }
    
    if (tVariantCompare(&pTSBuf->block.tag, tag) == 0) {
      return i;
    }
  }
  
  return -1;
}

static void tsBufGetBlock(STSBuf* pTSBuf, int32_t vnodeIndex, int32_t blockIndex) {
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[vnodeIndex].info;
  if (pBlockInfo->numOfBlocks <= blockIndex) {
    assert(false);
  }
  
  STSCursor* pCur = &pTSBuf->cur;
  if (pCur->vgroupIndex == vnodeIndex && ((pCur->blockIndex <= blockIndex && pCur->order == TSDB_ORDER_ASC) ||
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
      assert(false);
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
  
  assert((pTSBuf->tsData.len / TSDB_KEYSIZE == pBlock->numOfElem) && (pTSBuf->tsData.allocSize >= pTSBuf->tsData.len));
  
  pCur->vgroupIndex = vnodeIndex;
  pCur->blockIndex = blockIndex;
  
  pCur->tsIndex = (pCur->order == TSDB_ORDER_ASC) ? 0 : pBlock->numOfElem - 1;
}

static int32_t doUpdateVnodeInfo(STSBuf* pTSBuf, int64_t offset, STSVnodeBlockInfo* pVInfo) {
  if (offset < 0 || offset >= getDataStartOffset()) {
    return -1;
  }

  if (fseek(pTSBuf->f, (int32_t)offset, SEEK_SET) != 0) {
    return -1;
  }

  fwrite(pVInfo, sizeof(STSVnodeBlockInfo), 1, pTSBuf->f);
  return 0;
}

STSVnodeBlockInfo* tsBufGetVnodeBlockInfo(STSBuf* pTSBuf, int32_t vnodeId) {
  int32_t j = tsBufFindVnodeIndexFromId(pTSBuf->pData, pTSBuf->numOfVnodes, vnodeId);
  if (j == -1) {
    return NULL;
  }
  
  return &pTSBuf->pData[j].info;
}

int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader) {
  if ((pTSBuf->f == NULL) || pHeader == NULL || pHeader->numOfVnode == 0 || pHeader->magic != TS_COMP_FILE_MAGIC) {
    return -1;
  }

  assert(pHeader->tsOrder == TSDB_ORDER_ASC || pHeader->tsOrder == TSDB_ORDER_DESC);

  int32_t r = fseek(pTSBuf->f, 0, SEEK_SET);
  if (r != 0) {
    return -1;
  }
  
  fwrite(pHeader, sizeof(STSBufFileHeader), 1, pTSBuf->f);
  return 0;
}

bool tsBufNextPos(STSBuf* pTSBuf) {
  if (pTSBuf == NULL || pTSBuf->numOfVnodes == 0) {
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
      assert(pTSBuf->numOfVnodes > 0);
      
      int32_t vnodeIndex = pTSBuf->numOfVnodes - 1;
      pCur->vgroupIndex = vnodeIndex;
      
      int32_t            vnodeId = pTSBuf->pData[pCur->vgroupIndex].info.vnode;
      STSVnodeBlockInfo* pBlockInfo = tsBufGetVnodeBlockInfo(pTSBuf, vnodeId);
      int32_t            blockIndex = pBlockInfo->numOfBlocks - 1;
      
      tsBufGetBlock(pTSBuf, vnodeIndex, blockIndex);
      
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
    assert(pTSBuf->tsData.len == pTSBuf->block.numOfElem * TSDB_KEYSIZE);
    
    if ((pCur->order == TSDB_ORDER_ASC && pCur->tsIndex >= pTSBuf->block.numOfElem - 1) ||
        (pCur->order == TSDB_ORDER_DESC && pCur->tsIndex <= 0)) {
      int32_t vnodeId = pTSBuf->pData[pCur->vgroupIndex].info.vnode;
      
      STSVnodeBlockInfo* pBlockInfo = tsBufGetVnodeBlockInfo(pTSBuf, vnodeId);
      if (pBlockInfo == NULL || (pCur->blockIndex >= pBlockInfo->numOfBlocks - 1 && pCur->order == TSDB_ORDER_ASC) ||
          (pCur->blockIndex <= 0 && pCur->order == TSDB_ORDER_DESC)) {
        if ((pCur->vgroupIndex >= pTSBuf->numOfVnodes - 1 && pCur->order == TSDB_ORDER_ASC) ||
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
  STSElem elem1 = {.vnode = -1};
  if (pTSBuf == NULL) {
    return elem1;
  }
  
  STSCursor* pCur = &pTSBuf->cur;
  if (pCur != NULL && pCur->vgroupIndex < 0) {
    return elem1;
  }

  STSBlock* pBlock = &pTSBuf->block;
  
  elem1.vnode = pTSBuf->pData[pCur->vgroupIndex].info.vnode;
  elem1.ts = *(TSKEY*)(pTSBuf->tsData.rawBuf + pCur->tsIndex * TSDB_KEYSIZE);
  elem1.tag = &pBlock->tag;

  return elem1;
}

/**
 * current only support ts comp data from two vnode merge
 * @param pDestBuf
 * @param pSrcBuf
 * @param vnodeId
 * @return
 */
int32_t tsBufMerge(STSBuf* pDestBuf, const STSBuf* pSrcBuf) {
  if (pDestBuf == NULL || pSrcBuf == NULL || pSrcBuf->numOfVnodes <= 0) {
    return 0;
  }
  
  if (pDestBuf->numOfVnodes + pSrcBuf->numOfVnodes > TS_COMP_FILE_VNODE_MAX) {
    return -1;
  }
  
  // src can only have one vnode index
  assert(pSrcBuf->numOfVnodes == 1);

  // there are data in buffer, flush to disk first
  tsBufFlush(pDestBuf);
  
  // compared with the last vnode id
  int32_t vnodeId = tsBufGetLastVnodeInfo((STSBuf*) pSrcBuf)->info.vnode;
  if (vnodeId != tsBufGetLastVnodeInfo(pDestBuf)->info.vnode) {
    int32_t oldSize = pDestBuf->numOfVnodes;
    int32_t newSize = oldSize + pSrcBuf->numOfVnodes;
    
    if (pDestBuf->numOfAlloc < newSize) {
      pDestBuf->numOfAlloc = newSize;
      
      STSVnodeBlockInfoEx* tmp = realloc(pDestBuf->pData, sizeof(STSVnodeBlockInfoEx) * newSize);
      if (tmp == NULL) {
        return -1;
      }
      
      pDestBuf->pData = tmp;
    }
    
    // directly copy the vnode index information
    memcpy(&pDestBuf->pData[oldSize], pSrcBuf->pData, (size_t)pSrcBuf->numOfVnodes * sizeof(STSVnodeBlockInfoEx));
    
    // set the new offset value
    for (int32_t i = 0; i < pSrcBuf->numOfVnodes; ++i) {
      STSVnodeBlockInfoEx* pBlockInfoEx = &pDestBuf->pData[i + oldSize];
      pBlockInfoEx->info.offset = (pSrcBuf->pData[i].info.offset - getDataStartOffset()) + pDestBuf->fileSize;
      pBlockInfoEx->info.vnode = vnodeId;
    }
    
    pDestBuf->numOfVnodes = newSize;
  } else {
    STSVnodeBlockInfoEx* pBlockInfoEx = tsBufGetLastVnodeInfo(pDestBuf);
    
    pBlockInfoEx->len += pSrcBuf->pData[0].len;
    pBlockInfoEx->info.numOfBlocks += pSrcBuf->pData[0].info.numOfBlocks;
    pBlockInfoEx->info.compLen += pSrcBuf->pData[0].info.compLen;
    pBlockInfoEx->info.vnode = vnodeId;
  }
  
  int32_t r = fseek(pDestBuf->f, 0, SEEK_END);
  assert(r == 0);
  
  int64_t offset = getDataStartOffset();
  int32_t size = (int32_t)pSrcBuf->fileSize - (int32_t)offset;

  ssize_t rc = taosFSendFile(pDestBuf->f, pSrcBuf->f, &offset, size);
  
  if (rc == -1) {
//    tscError("failed to merge tsBuf from:%s to %s, reason:%s\n", pSrcBuf->path, pDestBuf->path, strerror(errno));
    return -1;
  }
  
  if (rc != size) {
//    tscError("failed to merge tsBuf from:%s to %s, reason:%s\n", pSrcBuf->path, pDestBuf->path, strerror(errno));
    return -1;
  }
  
  pDestBuf->numOfTotal += pSrcBuf->numOfTotal;
  
  int32_t oldSize = pDestBuf->fileSize;
  
  struct stat fileStat;
  if (fstat(fileno(pDestBuf->f), &fileStat) != 0) {
    return -1;  
  }
  pDestBuf->fileSize = (uint32_t)fileStat.st_size;
  
  assert(pDestBuf->fileSize == oldSize + size);
  
//  tscDebug("tsBuf merge success, %p, path:%s, fd:%d, file size:%d, numOfVnode:%d, autoDelete:%d", pDestBuf,
//           pDestBuf->path, fileno(pDestBuf->f), pDestBuf->fileSize, pDestBuf->numOfVnodes, pDestBuf->autoDelete);
  
  return 0;
}

STSBuf* tsBufCreateFromCompBlocks(const char* pData, int32_t numOfBlocks, int32_t len, int32_t order, int32_t vnodeId) {
  STSBuf* pTSBuf = tsBufCreate(true, order);
  
  STSVnodeBlockInfo* pBlockInfo = &(addOneVnodeInfo(pTSBuf, 0)->info);
  pBlockInfo->numOfBlocks = numOfBlocks;
  pBlockInfo->compLen = len;
  pBlockInfo->offset = getDataStartOffset();
  pBlockInfo->vnode = vnodeId;
  
  // update prev vnode length info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, pBlockInfo);
  
  int32_t ret = fseek(pTSBuf->f, pBlockInfo->offset, SEEK_SET);
  UNUSED(ret);
  size_t sz = fwrite((void*)pData, 1, len, pTSBuf->f);
  UNUSED(sz);
  pTSBuf->fileSize += len;
  
  pTSBuf->tsOrder = order;
  assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);
  
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);
  
  fsync(fileno(pTSBuf->f));
  
  return pTSBuf;
}

STSElem tsBufGetElemStartPos(STSBuf* pTSBuf, int32_t vnodeId, tVariant* tag) {
  STSElem elem = {.vnode = -1};
  
  if (pTSBuf == NULL) {
    return elem;
  }
  
  int32_t j = tsBufFindVnodeIndexFromId(pTSBuf->pData, pTSBuf->numOfVnodes, vnodeId);
  if (j == -1) {
    return elem;
  }
  
  // for debug purpose
  //  tsBufDisplay(pTSBuf);
  
  STSCursor*         pCur = &pTSBuf->cur;
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[j].info;
  
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
  printf("number of vnode:%d\n", pTSBuf->numOfVnodes);
  
  int32_t old = pTSBuf->cur.order;
  pTSBuf->cur.order = TSDB_ORDER_ASC;
  
  tsBufResetPos(pTSBuf);
  
  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    if (elem.tag->nType == TSDB_DATA_TYPE_BIGINT) {
      printf("%d-%" PRId64 "-%" PRId64 "\n", elem.vnode, elem.tag->i64Key, elem.ts);
    }
  }
  
  pTSBuf->cur.order = old;
  printf("-------end of ts comp file-------\n");
}

static int32_t getDataStartOffset() {
  return sizeof(STSBufFileHeader) + TS_COMP_FILE_VNODE_MAX * sizeof(STSVnodeBlockInfo);
}

// update prev vnode length info in file
static void TSBufUpdateVnodeInfo(STSBuf* pTSBuf, int32_t index, STSVnodeBlockInfo* pBlockInfo) {
  int32_t offset = sizeof(STSBufFileHeader) + index * sizeof(STSVnodeBlockInfo);
  doUpdateVnodeInfo(pTSBuf, offset, pBlockInfo);
}

static STSBuf* allocResForTSBuf(STSBuf* pTSBuf) {
  const int32_t INITIAL_VNODEINFO_SIZE = 4;
  
  pTSBuf->numOfAlloc = INITIAL_VNODEINFO_SIZE;
  pTSBuf->pData = calloc(pTSBuf->numOfAlloc, sizeof(STSVnodeBlockInfoEx));
  if (pTSBuf->pData == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  
  pTSBuf->tsData.rawBuf = malloc(MEM_BUF_SIZE);
  if (pTSBuf->tsData.rawBuf == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  
  pTSBuf->bufSize = MEM_BUF_SIZE;
  pTSBuf->tsData.threshold = MEM_BUF_SIZE;
  pTSBuf->tsData.allocSize = MEM_BUF_SIZE;
  
  pTSBuf->assistBuf = malloc(MEM_BUF_SIZE);
  if (pTSBuf->assistBuf == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  
  pTSBuf->block.payload = malloc(MEM_BUF_SIZE);
  if (pTSBuf->block.payload == NULL) {
    tsBufDestroy(pTSBuf);
    return NULL;
  }
  
  pTSBuf->fileSize += getDataStartOffset();
  return pTSBuf;
}

int32_t tsBufGetNumOfVnodes(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return 0;
  }

  return pTSBuf->numOfVnodes;
}

void tsBufGetVnodeIdList(STSBuf* pTSBuf, int32_t* num, int32_t** vnodeId) {
  int32_t size = tsBufGetNumOfVnodes(pTSBuf);
  if (num != NULL) {
    *num = size;
  }

  *vnodeId = NULL;
  if (size == 0) {
    return;
  }

  (*vnodeId) = malloc(tsBufGetNumOfVnodes(pTSBuf) * sizeof(int32_t));

  for(int32_t i = 0; i < size; ++i) {
    (*vnodeId)[i] = pTSBuf->pData[i].info.vnode;
  }
}