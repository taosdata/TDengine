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

  if (pColumnInfoData->nullbitmap == NULL) {
    return false;
  }

  uint8_t v = (pColumnInfoData->nullbitmap[row>>3] & (1<<(8 - (row&0x07))));
  return (v == 1);
}

bool colDataIsNull_f(const char* bitmap, uint32_t row) {
  return (bitmap[row>>3] & (1<<(8 - (row&0x07))));
}

void colDataSetNull_f(char* bitmap, uint32_t row) { // TODO
  return;
}

void* colDataGet(const SColumnInfoData* pColumnInfoData, uint32_t row) {
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    uint32_t offset = ((uint32_t*)pColumnInfoData->pData)[row];
    return (char*)(pColumnInfoData->pData) + offset; // the first part is the pointer to the true binary data
  } else {
    return (char*)(pColumnInfoData->pData) + (row * pColumnInfoData->info.bytes);
  }
}

int32_t colDataAppend(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, bool isNull) {
  ASSERT(pColumnInfoData != NULL);

  if (isNull) {
    // TODO set null value in the nullbitmap
    return 0;
  }

  int32_t type = pColumnInfoData->info.type;
  if (IS_VAR_DATA_TYPE(type)) {
    // TODO continue append var_type
  } else {
    char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
    switch(type) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT: {*(int8_t*) p = *(int8_t*) pData;break;}
      default:
        assert(0);
    }

  }

  return 0;
}

size_t colDataGetCols(const SSDataBlock* pBlock) {
  ASSERT(pBlock);

  size_t constantCols = (pBlock->pConstantList != NULL)? taosArrayGetSize(pBlock->pConstantList):0;
  ASSERT( pBlock->info.numOfCols == taosArrayGetSize(pBlock->pDataBlock) + constantCols);
  return pBlock->info.numOfCols;
}

size_t colDataGetRows(const SSDataBlock* pBlock) {
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




