#include "tep.h"
#include "common.h"
#include "tglobal.h"
#include "tlockfree.h"

int taosGetFqdnPortFromEp(const char *ep, SEp* pEp) {
  pEp->port = 0;
  strcpy(pEp->fqdn, ep);

  char *temp = strchr(pEp->fqdn, ':');
  if (temp) {
    *temp = 0;
    pEp->port = atoi(temp+1);
  }

  if (pEp->port == 0) {
    pEp->port = tsServerPort;
    return -1;
  }

  return 0;
}

void addEpIntoEpSet(SEpSet *pEpSet, const char* fqdn, uint16_t port) {
  if (pEpSet == NULL || fqdn == NULL || strlen(fqdn) == 0) {
    return;
  }

  int32_t index = pEpSet->numOfEps;
  tstrncpy(pEpSet->eps[index].fqdn, fqdn, tListLen(pEpSet->eps[index].fqdn));
  pEpSet->eps[index].port = port;
  pEpSet->numOfEps += 1;
}

bool isEpsetEqual(const SEpSet *s1, const SEpSet *s2) {
  if (s1->numOfEps != s2->numOfEps || s1->inUse != s2->inUse) {
    return false;
  }

  for (int32_t i = 0; i < s1->numOfEps; i++) {
    if (s1->eps[i].port != s2->eps[i].port
        || strncmp(s1->eps[i].fqdn, s2->eps[i].fqdn, TSDB_FQDN_LEN) != 0)
      return false;
  }
  return true;
}

void updateEpSet_s(SCorEpSet *pEpSet, SEpSet *pNewEpSet) {
  taosCorBeginWrite(&pEpSet->version);
  pEpSet->epSet = *pNewEpSet;
  taosCorEndWrite(&pEpSet->version);
}

SEpSet getEpSet_s(SCorEpSet *pEpSet) {
  SEpSet ep = {0};
  taosCorBeginRead(&pEpSet->version);
  ep = pEpSet->epSet;
  taosCorEndRead(&pEpSet->version);

  return ep;
}

bool colDataIsNull(const SColumnInfoData* pColumnInfoData, uint32_t totalRows, uint32_t row, SColumnDataAgg* pColAgg) {
  if (pColAgg != NULL) {
    if (pColAgg->numOfNull == totalRows) {
      ASSERT(pColumnInfoData->nullbitmap == NULL);
      return true;
    } else if (pColAgg->numOfNull == 0) {
      ASSERT(pColumnInfoData->nullbitmap == NULL);
      return false;
    }
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return pColumnInfoData->varmeta.offset[row] == -1;
  } else {
    if (pColumnInfoData->nullbitmap == NULL) {
      return false;
    }

    return colDataIsNull_f(pColumnInfoData->nullbitmap, row);
  }
}

#define NBIT (3u)
#define BitmapLen(_n)     (((_n) + ((1<<NBIT)-1)) >> NBIT)
#define BitPos(_n)        ((_n) & ((1<<NBIT) - 1))

bool colDataIsNull_f(const char* bitmap, uint32_t row) {
  return (bitmap[row>>3u] & (1u<<(7u - BitPos(row)))) == (1u<<(7u - BitPos(row)));
}

void colDataSetNull_f(char* bitmap, uint32_t row) {
  bitmap[row>>3u] |= (1u << (7u - BitPos(row)));
}

char* colDataGet(SColumnInfoData* pColumnInfoData, uint32_t row) {
  char* p = pColumnInfoData->pData;
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return p + pColumnInfoData->varmeta.offset[row];
  } else {
    return p + (row * pColumnInfoData->info.bytes);
  }
}

static int32_t ensureBitmapSize(SColumnInfoData* pColumnInfoData, uint32_t size) {
#if 0
  ASSERT(pColumnInfoData != NULL);
  if (pColumnInfoData->bitmapLen * 8 < size) {
    int32_t inc = pColumnInfoData->bitmapLen * 1.25;
    if (inc < 8) {
      inc = 8;
    }

    char* tmp = realloc(pColumnInfoData->nullbitmap, inc + pColumnInfoData->bitmapLen);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pColumnInfoData->nullbitmap = tmp;
    memset(pColumnInfoData->nullbitmap + pColumnInfoData->bitmapLen, 0, inc);
  }
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t colDataGetSize(const SColumnInfoData* pColumnInfoData, int32_t numOfRows) {
  ASSERT(pColumnInfoData != NULL);
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return pColumnInfoData->varmeta.length;
  } else {
    return pColumnInfoData->info.bytes * numOfRows;
  }
}

void colDataTrim(SColumnInfoData* pColumnInfoData) {
  // TODO
}

int32_t colDataAppend(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, bool isNull) {
  ASSERT(pColumnInfoData != NULL);

  if (isNull) {
    // There is a placehold for each NULL value of binary or nchar type.
    if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
      pColumnInfoData->varmeta.offset[currentRow] = -1; // it is a null value of VAR type.
    } else {
      colDataSetNull_f(pColumnInfoData->nullbitmap, currentRow);
    }

    return 0;
  }

  int32_t type = pColumnInfoData->info.type;
  if (IS_VAR_DATA_TYPE(type)) {
    SVarColAttr* pAttr = &pColumnInfoData->varmeta;
    if (pAttr->allocLen < pAttr->length + varDataTLen(pData)) {
      uint32_t newSize = pAttr->allocLen;
      if (newSize == 0) {
        newSize = 8;
      }

      while(newSize < pAttr->length + varDataTLen(pData)) {
        newSize = newSize * 1.5;
      }

      char* buf = realloc(pColumnInfoData->pData, newSize);
      if (buf == NULL) {
        // TODO handle the malloc failure.
      }

      pColumnInfoData->pData = buf;
      pAttr->allocLen = newSize;
    }

    uint32_t len = pColumnInfoData->varmeta.length;
    pColumnInfoData->varmeta.offset[currentRow] = len;

    memcpy(pColumnInfoData->pData + len, pData, varDataTLen(pData));
    pColumnInfoData->varmeta.length += varDataTLen(pData);
  } else {
    char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
    switch(type) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT: {*(int8_t*) p = *(int8_t*) pData;break;}
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT: {*(int16_t*) p = *(int16_t*) pData;break;}
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT: {*(int32_t*) p = *(int32_t*) pData;break;}
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT: {*(int64_t*) p = *(int64_t*) pData;break;}
      default:
        assert(0);
    }
  }

  return 0;
}

static void doBitmapMerge(SColumnInfoData* pColumnInfoData, int32_t numOfRow1, const SColumnInfoData* pSource, int32_t numOfRow2) {
  uint32_t total = numOfRow1 + numOfRow2;

  if (BitmapLen(numOfRow1) < BitmapLen(total)) {
    char*    tmp = realloc(pColumnInfoData->nullbitmap, BitmapLen(total));
    uint32_t extend = BitmapLen(total) - BitmapLen(numOfRow1);
    memset(tmp + BitmapLen(numOfRow1), 0, extend);
    pColumnInfoData->nullbitmap = tmp;
  }

  uint32_t remindBits = BitPos(numOfRow1);
  uint32_t shiftBits = 8 - remindBits;

  if (remindBits == 0) {  // no need to shift bits of bitmap
    memcpy(pColumnInfoData->nullbitmap + BitmapLen(numOfRow1), pSource->nullbitmap, BitmapLen(numOfRow2));
  } else {
    int32_t len = BitmapLen(numOfRow2);
    int32_t i = 0;

    uint8_t* p = (uint8_t*)pSource->nullbitmap;
    pColumnInfoData->nullbitmap[BitmapLen(numOfRow1) - 1] |= (p[0] >> remindBits);

    uint8_t* start = (uint8_t*)&pColumnInfoData->nullbitmap[BitmapLen(numOfRow1)];
    while (i < len) {
      start[i] |= (p[i] << shiftBits);
      i += 1;

      if (i > 1) {
        start[i - 1] |= (p[i] >> remindBits);
      }
    }
  }
}

int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, uint32_t numOfRow1, const SColumnInfoData* pSource, uint32_t numOfRow2) {
  ASSERT(pColumnInfoData != NULL && pSource != NULL && pColumnInfoData->info.type == pSource->info.type);

  if (numOfRow2 == 0) {
    return numOfRow1;
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    // Handle the bitmap
    char* p = realloc(pColumnInfoData->varmeta.offset, sizeof(int32_t) * (numOfRow1 + numOfRow2));
    if (p == NULL) {
      // TODO
    }

    pColumnInfoData->varmeta.offset = (int32_t*) p;
    memcpy(pColumnInfoData->varmeta.offset + sizeof(int32_t) * numOfRow1, pSource->varmeta.offset, sizeof(int32_t) * numOfRow2);

    // copy the
    uint32_t len = pSource->varmeta.length;
    uint32_t oldLen = pColumnInfoData->varmeta.length;
    if (pColumnInfoData->varmeta.allocLen < len + oldLen) {
      char* tmp = realloc(pColumnInfoData->pData, len + oldLen);
      if (tmp == NULL) {
        return TSDB_CODE_VND_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = tmp;
      pColumnInfoData->varmeta.allocLen = len + oldLen;
    }

    memcpy(pColumnInfoData->pData + oldLen, pSource->pData + sizeof(int32_t), len);
  } else {
    doBitmapMerge(pColumnInfoData, numOfRow1, pSource, numOfRow2);

    int32_t newSize = (numOfRow1 + numOfRow2) * pColumnInfoData->info.bytes;
    char*   tmp = realloc(pColumnInfoData->pData, newSize);
    if (tmp == NULL) {
      return TSDB_CODE_VND_OUT_OF_MEMORY;
    }

    pColumnInfoData->pData = tmp;
    int32_t offset = pColumnInfoData->info.bytes * numOfRow1;
    memcpy(pColumnInfoData->pData + offset, pSource->pData, pSource->info.bytes * numOfRow2);
  }

  return numOfRow1 + numOfRow2;
}

size_t colDataGetNumOfCols(const SSDataBlock* pBlock) {
  ASSERT(pBlock);

  size_t constantCols = (pBlock->pConstantList != NULL)? taosArrayGetSize(pBlock->pConstantList):0;
  ASSERT( pBlock->info.numOfCols == taosArrayGetSize(pBlock->pDataBlock) + constantCols);
  return pBlock->info.numOfCols;
}

size_t colDataGetNumOfRows(const SSDataBlock* pBlock) {
  return pBlock->info.rows;
}

int32_t colDataUpdateTsWindow(SSDataBlock* pDataBlock) {
  if (pDataBlock == NULL || pDataBlock->info.rows <= 0) {
    return 0;
  }

  if (pDataBlock->info.numOfCols <= 0) {
    return -1;
  }

  SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, 0);
  if (pColInfoData->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return 0;
  }

  ASSERT(pColInfoData->nullbitmap == NULL);
  pDataBlock->info.window.skey = *(TSKEY*) colDataGet(pColInfoData, 0);
  pDataBlock->info.window.ekey = *(TSKEY*) colDataGet(pColInfoData, (pDataBlock->info.rows - 1));
  return 0;
}

int32_t blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc) {
  assert(pSrc != NULL && pDest != NULL && pDest->info.numOfCols == pSrc->info.numOfCols);

  int32_t numOfCols = pSrc->info.numOfCols;
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol2 = taosArrayGet(pDest->pDataBlock, i);
    SColumnInfoData* pCol1 = taosArrayGet(pSrc->pDataBlock, i);

    uint32_t oldLen = colDataGetSize(pCol2, pDest->info.rows);
    uint32_t newLen = colDataGetSize(pCol1, pSrc->info.rows);

    int32_t newSize = oldLen + newLen;
    char* tmp = realloc(pCol2->pData, newSize);
    if (tmp != NULL) {
      pCol2->pData = tmp;
      colDataMergeCol(pCol2, pDest->info.rows, pCol1, pSrc->info.rows);
    } else {
      return TSDB_CODE_VND_OUT_OF_MEMORY;
    }
  }

  pDest->info.rows += pSrc->info.rows;
  return TSDB_CODE_SUCCESS;
}

size_t blockDataGetSize(const SSDataBlock* pBlock) {
  assert(pBlock != NULL);

  size_t total = 0;
  int32_t numOfCols = pBlock->info.numOfCols;
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    total += colDataGetSize(pColInfoData, pBlock->info.rows);
  }

  // bitmap for each column
  total += BitmapLen(pBlock->info.rows) * numOfCols;
  return total;
}

// the number of tuples can be fit in one page.
// Actual data rows pluses the corresponding meta data must fit in one memory buffer of the given page size.
int32_t blockDataSplitRows(SSDataBlock* pBlock, bool hasVarCol, int32_t startIndex, int32_t* stopIndex, int32_t pageSize) {
  ASSERT(pBlock != NULL && stopIndex != NULL);

  int32_t size = 0;
  int32_t numOfCols = pBlock->info.numOfCols;
  int32_t numOfRows = pBlock->info.rows;

  size_t headerSize = sizeof(int32_t);

  // TODO speedup by checking if the whole page can fit in firstly.

  if (!hasVarCol) {
    size_t rowSize = blockDataGetRowSize(pBlock);
    int32_t capacity = ((pageSize - headerSize) / (rowSize * 8 + 1)) * 8;
    *stopIndex = startIndex + capacity;

    if (*stopIndex >= numOfRows) {
      *stopIndex = numOfRows - 1;
    }

    return TSDB_CODE_SUCCESS;
  } else {
    // iterate the rows that can be fit in this buffer page
    size += headerSize;

    for(int32_t j = startIndex; j < numOfRows; ++j) {
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
        if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
          bool isNull = colDataIsNull(pColInfoData, numOfRows, j, NULL);
          if (isNull) {
            // do nothing
          } else {
            char* p = colDataGet(pColInfoData, j);
            size += varDataTLen(p);
          }

          size += sizeof(pColInfoData->varmeta.offset[0]);
        } else {
          size += pColInfoData->info.bytes;

          if (((j - startIndex) % 8) == 0) {
            size += 1; // the space for null bitmap
          }
        }
      }

      if (size > pageSize) {
        *stopIndex = j - 1;
        ASSERT(*stopIndex > startIndex);

        return TSDB_CODE_SUCCESS;
      }
    }

    // all fit in
    *stopIndex = numOfRows - 1;
    return TSDB_CODE_SUCCESS;
  }
}

/**
 *
 * +---------------------------+---------------------+
 * |the number of rows(4 bytes)|      column #1      |
 *                             |---------------------+
 * |                           | null bitmap| values |
 * +---------------------------+---------------------+
 * @param buf
 * @param pBlock
 * @return
 */
int32_t blockDataToBuf(char* buf, const SSDataBlock* pBlock) {
  ASSERT(pBlock != NULL);

  // write the number of rows
  *(uint32_t*) buf = pBlock->info.rows;

  int32_t numOfCols = pBlock->info.numOfCols;
  int32_t numOfRows = pBlock->info.rows;

  char* pStart = buf + sizeof(uint32_t);

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      memcpy(pStart, pCol->varmeta.offset, numOfRows * sizeof(int32_t));
      pStart += numOfRows * sizeof(int32_t);
    } else {
      memcpy(pStart, pCol->nullbitmap, BitmapLen(numOfRows));
      pStart += BitmapLen(pBlock->info.rows);
    }

    uint32_t dataSize = colDataGetSize(pCol, numOfRows);
    memcpy(pStart, pCol->pData, dataSize);
    pStart += dataSize;
  }

  return 0;
}

size_t blockDataGetRowSize(const SSDataBlock* pBlock) {
  ASSERT(pBlock != NULL);
  size_t rowSize = 0;

  size_t numOfCols = pBlock->info.numOfCols;
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    rowSize += pColInfo->info.bytes;
  }

  return rowSize;
}

typedef struct SSDataBlockSortHelper {
  SArray      *orderInfo;   // SArray<SBlockOrderInfo>
  SSDataBlock *pDataBlock;
  bool         nullFirst;
} SSDataBlockSortHelper;

int32_t dataBlockCompar(const void* p1, const void* p2, const void* param) {
  const SSDataBlockSortHelper* pHelper = (const SSDataBlockSortHelper*) param;

  SSDataBlock* pDataBlock = pHelper->pDataBlock;

  int32_t* left  = (int32_t*) p1;
  int32_t* right = (int32_t*) p2;

  SArray* pInfo = pHelper->orderInfo;
  size_t num = taosArrayGetSize(pInfo);
  for(int32_t i = 0; i < num; ++i) {
    SBlockOrderInfo* pOrder = taosArrayGet(pInfo, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, pOrder->colIndex);

    bool leftNull  = colDataIsNull(pColInfoData, pDataBlock->info.rows, *left, pDataBlock->pBlockAgg);
    bool rightNull = colDataIsNull(pColInfoData, pDataBlock->info.rows, *right, pDataBlock->pBlockAgg);
    if (leftNull && rightNull) {
      continue; // continue to next slot
    }

    if (rightNull) {
      return pHelper->nullFirst? 1:-1;
    }

    if (leftNull) {
      return pHelper->nullFirst? -1:1;
    }

    void* left1 = colDataGet(pColInfoData, *left);
    void* right1 = colDataGet(pColInfoData, *right);

    switch(pColInfoData->info.type) {
      case TSDB_DATA_TYPE_INT: {
        if (*(int32_t*) left1 == *(int32_t*) right1) {
          continue;// TODO continue
        } else {
          if (pOrder->order == TSDB_ORDER_ASC) {
            return (*(int32_t*) left1 < *(int32_t*) right1)? -1:1;
          } else {
            return (*(int32_t*) left1 < *(int32_t*) right1)? 1:-1;
          }
        }
      }
      default:
        assert(0);
    }
  }

  return 0;
}

static void doAssignOneTuple(SColumnInfoData* pDstCols, int32_t numOfRows, const SSDataBlock* pSrcBlock, int32_t tupleIndex) {
  int32_t numOfCols = pSrcBlock->info.numOfCols;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = &pDstCols[i];
    SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, i);

    bool isNull = colDataIsNull(pSrc, pSrcBlock->info.rows, tupleIndex, NULL);
    if (isNull) {
      colDataAppend(pDst, numOfRows, NULL, true);
    } else {
      char* p = colDataGet((SColumnInfoData*)pSrc, tupleIndex);
      colDataAppend(pDst, numOfRows, p, false);
    }
  }
}

static void blockDataAssign(SColumnInfoData* pCols, const SSDataBlock* pDataBlock, int32_t* index) {
  for (int32_t i = 0; i < pDataBlock->info.rows; ++i) {
    doAssignOneTuple(pCols, i, pDataBlock, index[i]);
  }
}

static SColumnInfoData* createHelpColInfoData(const SSDataBlock* pDataBlock) {
  int32_t rows = pDataBlock->info.rows;
  int32_t numOfCols = pDataBlock->info.numOfCols;

  SColumnInfoData* pCols = calloc(numOfCols, sizeof(SColumnInfoData));
  if (pCols == NULL) {
    return NULL;
  }

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, i);
    pCols[i].info = pColInfoData->info;

    if (IS_VAR_DATA_TYPE(pCols[i].info.type)) {
      pCols[i].varmeta.offset = calloc(rows, sizeof(int32_t));
    } else {
      pCols[i].nullbitmap = calloc(1, BitmapLen(rows));
      pCols[i].pData = calloc(rows, pCols[i].info.bytes);
    }
  }

  return pCols;
}

static int32_t copyBackToBlock(SSDataBlock* pDataBlock, SColumnInfoData* pCols) {
  int32_t numOfCols = pDataBlock->info.numOfCols;

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, i);
    pColInfoData->info = pCols[i].info;

    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      tfree(pColInfoData->varmeta.offset);
      pColInfoData->varmeta = pCols[i].varmeta;
    } else {
      tfree(pColInfoData->nullbitmap);
      pColInfoData->nullbitmap = pCols[i].nullbitmap;
    }

    tfree(pColInfoData->pData);
    pColInfoData->pData = pCols[i].pData;
  }

  tfree(pCols);
}

static int32_t* createTupleIndex(size_t rows) {
  int32_t* index = calloc(rows, sizeof(int32_t));
  if (index == NULL) {
    return NULL;
  }

  for(int32_t i = 0; i < rows; ++i) {
    index[i] = i;
  }

  return index;
}

static void destroyTupleIndex(int32_t* index) {
  tfree(index);
}

int32_t blockDataSort(SSDataBlock* pDataBlock, SArray* pOrderInfo, bool nullFirst) {
  ASSERT(pDataBlock != NULL && pOrderInfo != NULL);
  if (pDataBlock->info.rows <= 1) {
    return TSDB_CODE_SUCCESS;
  }

  // Allocate the additional buffer.
  uint32_t rows = pDataBlock->info.rows;
  int32_t* index = createTupleIndex(rows);
  if (index == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  SSDataBlockSortHelper helper = {.nullFirst = nullFirst, .pDataBlock = pDataBlock, .orderInfo = pOrderInfo};
  taosqsort(index, rows, sizeof(int32_t), &helper, dataBlockCompar);

  int32_t numOfCols = pDataBlock->info.numOfCols;
  SColumnInfoData* pCols = createHelpColInfoData(pDataBlock);
  if (pCols == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

#if 0
  SColumnInfoData* px = taosArrayGet(pDataBlock->pDataBlock, 0);
  for(int32_t i = 0; i < pDataBlock->info.rows; ++i) {
    printf("%d, %d, %d\n", index[i], ((int32_t*)px->pData)[i], ((int32_t*)px->pData)[index[i]]);
  }
#endif
  blockDataAssign(pCols, pDataBlock, index);

#if 0
  for(int32_t i = 0; i < pDataBlock->info.rows; ++i) {
    if (colDataIsNull(&pCols[0], rows, i, NULL)) {
      printf("0\t");
    } else {
      printf("%d\t", ((int32_t*)pCols[0].pData)[i]);
    }
  }

  printf("end\n");
#endif

  copyBackToBlock(pDataBlock, pCols);
  destroyTupleIndex(index);
}
