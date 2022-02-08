#ifndef TDENGINE_TEP_H
#define TDENGINE_TEP_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tmsg.h"
#include "common.h"

typedef struct SCorEpSet {
  int32_t version;
  SEpSet  epSet;
} SCorEpSet;

typedef struct SBlockOrderInfo {
  int32_t order;
  int32_t colIndex;
} SBlockOrderInfo;

int  taosGetFqdnPortFromEp(const char *ep, SEp *pEp);
void addEpIntoEpSet(SEpSet *pEpSet, const char *fqdn, uint16_t port);

bool isEpsetEqual(const SEpSet *s1, const SEpSet *s2);

void   updateEpSet_s(SCorEpSet *pEpSet, SEpSet *pNewEpSet);
SEpSet getEpSet_s(SCorEpSet *pEpSet);

bool colDataIsNull_f(const char* bitmap, uint32_t row);
void colDataSetNull_f(char* bitmap, uint32_t row);

bool colDataIsNull(const SColumnInfoData* pColumnInfoData, uint32_t totalRows, uint32_t row, SColumnDataAgg* pColAgg);

char* colDataGet(SColumnInfoData* pColumnInfoData, uint32_t row);
int32_t colDataAppend(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, bool isNull);
int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, uint32_t numOfRow1, const SColumnInfoData* pSource, uint32_t numOfRow2);
int32_t colDataUpdateTsWindow(SSDataBlock* pDataBlock);

int32_t colDataGetSize(const SColumnInfoData* pColumnInfoData, int32_t numOfRows);
void colDataTrim(SColumnInfoData* pColumnInfoData);

size_t colDataGetNumOfCols(const SSDataBlock* pBlock);
size_t colDataGetNumOfRows(const SSDataBlock* pBlock);

int32_t blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc);
int32_t blockDataSplitRows(SSDataBlock* pBlock, bool hasVarCol, int32_t startIndex, int32_t* stopIndex, int32_t pageSize);
SSDataBlock* blockDataExtractBlock(SSDataBlock* pBlock, int32_t startIndex, int32_t rowCount);

int32_t blockDataToBuf(char* buf, const SSDataBlock* pBlock);
int32_t blockDataFromBuf(SSDataBlock* pBlock, const char* buf);

size_t blockDataGetSize(const SSDataBlock* pBlock);
size_t blockDataGetRowSize(const SSDataBlock* pBlock);
int32_t blockDataSort(SSDataBlock* pDataBlock, SArray* pOrderInfo, bool nullFirst);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TEP_H
