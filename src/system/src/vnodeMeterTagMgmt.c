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

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>

#include "tsdb.h"

#include "tlog.h"
#include "tutil.h"

#include "taosmsg.h"
#include "textbuffer.h"

#include "tast.h"
#include "vnodeTagMgmt.h"

#define GET_TAG_VAL_POINTER(s, col, sc, t) ((t *)(&((s)->tags[(sc)->colOffset[(col)]])))
#define GET_TAG_VAL(s, col, sc, t) (*GET_TAG_VAL_POINTER(s, col, sc, t))

static void tTagsPrints(SMeterSidExtInfo *pMeterInfo, tTagSchema *pSchema, tOrderIdx *pOrder);

static void tSidSetDisplay(tSidSet *pSets);

// todo merge with losertree_compar/ext_comp
int32_t doCompare(char *f1, char *f2, int32_t type, int32_t size) {
  switch (type) {
    case TSDB_DATA_TYPE_INT:
      DEFAULT_COMP(GET_INT32_VAL(f1), GET_INT32_VAL(f2));
    case TSDB_DATA_TYPE_DOUBLE:
      DEFAULT_COMP(GET_DOUBLE_VAL(f1), GET_DOUBLE_VAL(f2));
    case TSDB_DATA_TYPE_FLOAT:
      DEFAULT_COMP(GET_FLOAT_VAL(f1), GET_FLOAT_VAL(f2));
    case TSDB_DATA_TYPE_BIGINT:
      DEFAULT_COMP(GET_INT64_VAL(f1), GET_INT64_VAL(f2));
    case TSDB_DATA_TYPE_SMALLINT:
      DEFAULT_COMP(GET_INT16_VAL(f1), GET_INT16_VAL(f2));
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:
      DEFAULT_COMP(GET_INT8_VAL(f1), GET_INT8_VAL(f2));
    case TSDB_DATA_TYPE_NCHAR: {
      int32_t ret = wcsncmp((wchar_t *)f1, (wchar_t *)f2, size / TSDB_NCHAR_SIZE);
      if (ret == 0) {
        return ret;
      }

      return (ret < 0) ? -1 : 1;
    }
    default: {
      int32_t ret = strncmp(f1, f2, (size_t)size);
      if (ret == 0) {
        return ret;
      }

      return (ret < 0) ? -1 : 1;
    }
  }
}

int32_t meterSidComparator(const void *p1, const void *p2, void *param) {
  tOrderDescriptor *pOrderDesc = (tOrderDescriptor *)param;

  SMeterSidExtInfo *s1 = (SMeterSidExtInfo *)p1;
  SMeterSidExtInfo *s2 = (SMeterSidExtInfo *)p2;

  for (int32_t i = 0; i < pOrderDesc->orderIdx.numOfOrderedCols; ++i) {
    int32_t colIdx = pOrderDesc->orderIdx.pData[i];

    char *  f1 = NULL;
    char *  f2 = NULL;
    int32_t type = 0;
    int32_t bytes = 0;

    if (colIdx == -1) {
      f1 = s1->tags;
      f2 = s2->tags;
      type = TSDB_DATA_TYPE_BINARY;
      bytes = TSDB_METER_NAME_LEN;
    } else {
      f1 = GET_TAG_VAL_POINTER(s1, colIdx, pOrderDesc->pTagSchema, char);
      f2 = GET_TAG_VAL_POINTER(s2, colIdx, pOrderDesc->pTagSchema, char);
      SSchema *pSchema = &pOrderDesc->pTagSchema->pSchema[colIdx];
      type = pSchema->type;
      bytes = pSchema->bytes;
    }

    int32_t ret = doCompare(f1, f2, type, bytes);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}

static void median(void **pMeterSids, size_t size, int32_t s1, int32_t s2, tOrderDescriptor *pOrderDesc,
                   __ext_compar_fn_t compareFn) {
  int32_t midIdx = ((s2 - s1) >> 1) + s1;

  if (compareFn(pMeterSids[midIdx], pMeterSids[s1], pOrderDesc) == 1) {
    tsDataSwap(&pMeterSids[midIdx], &pMeterSids[s1], TSDB_DATA_TYPE_BINARY, size);
  }

  if (compareFn(pMeterSids[midIdx], pMeterSids[s2], pOrderDesc) == 1) {
    tsDataSwap(&pMeterSids[midIdx], &pMeterSids[s1], TSDB_DATA_TYPE_BINARY, size);
    tsDataSwap(&pMeterSids[midIdx], &pMeterSids[s2], TSDB_DATA_TYPE_BINARY, size);
  } else if (compareFn(pMeterSids[s1], pMeterSids[s2], pOrderDesc) == 1) {
    tsDataSwap(&pMeterSids[s1], &pMeterSids[s2], TSDB_DATA_TYPE_BINARY, size);
  }

  assert(compareFn(pMeterSids[midIdx], pMeterSids[s1], pOrderDesc) <= 0 &&
         compareFn(pMeterSids[s1], pMeterSids[s2], pOrderDesc) <= 0);

#ifdef _DEBUG_VIEW
  tTagsPrints(pMeterSids[s1], pOrderDesc->pTagSchema, &pOrderDesc->orderIdx);
  tTagsPrints(pMeterSids[midIdx], pOrderDesc->pTagSchema, &pOrderDesc->orderIdx);
  tTagsPrints(pMeterSids[s2], pOrderDesc->pTagSchema, &pOrderDesc->orderIdx);
#endif
}

static void tInsertSort(void **pMeterSids, size_t size, int32_t startPos, int32_t endPos, void *param,
                        __ext_compar_fn_t compareFn) {
  for (int32_t i = startPos + 1; i <= endPos; ++i) {
    for (int32_t j = i; j > startPos; --j) {
      if (compareFn(pMeterSids[j], pMeterSids[j - 1], param) == -1) {
        tsDataSwap(&pMeterSids[j], &pMeterSids[j - 1], TSDB_DATA_TYPE_BINARY, size);
      } else {
        break;
      }
    }
  }
}

void tQSortEx(void **pMeterSids, size_t size, int32_t start, int32_t end, void *param, __ext_compar_fn_t compareFn) {
  tOrderDescriptor *pOrderDesc = (tOrderDescriptor *)param;

  // short array sort, incur another sort procedure instead of quick sort process
  if (end - start + 1 <= 8) {
    tInsertSort(pMeterSids, size, start, end, pOrderDesc, compareFn);
    return;
  }

  median(pMeterSids, size, start, end, pOrderDesc, compareFn);

  int32_t s = start, e = end;
  int32_t endRightS = end, startLeftS = start;

  while (s < e) {
    while (e > s) {
      int32_t ret = compareFn(pMeterSids[e], pMeterSids[s], pOrderDesc);
      if (ret < 0) {
        break;
      }

      /*
       * move the data that equals to pivotal value to the right end of the list
       */
      if (ret == 0 && e != endRightS) {
        tsDataSwap(&pMeterSids[e], &pMeterSids[endRightS--], TSDB_DATA_TYPE_BINARY, size);
      }

      e--;
    }

    if (e != s) {
      tsDataSwap(&pMeterSids[e], &pMeterSids[s], TSDB_DATA_TYPE_BINARY, size);
    }

    while (s < e) {
      int32_t ret = compareFn(pMeterSids[s], pMeterSids[e], pOrderDesc);
      if (ret > 0) {
        break;
      }

      if (ret == 0 && s != startLeftS) {
        tsDataSwap(&pMeterSids[s], &pMeterSids[startLeftS++], TSDB_DATA_TYPE_BINARY, size);
      }
      s++;
    }

    if (e != s) {
      tsDataSwap(&pMeterSids[s], &pMeterSids[e], TSDB_DATA_TYPE_BINARY, size);
    }
  }

  int32_t rightPartStart = e + 1;
  if (endRightS != end && e < end) {
    int32_t left = rightPartStart;
    int32_t right = end;

    while (right > endRightS && left <= endRightS) {
      tsDataSwap(&pMeterSids[left++], &pMeterSids[right--], TSDB_DATA_TYPE_BINARY, size);
    }

    rightPartStart += (end - endRightS);
  }

  int32_t leftPartEnd = e - 1;
  if (startLeftS != end && s > start) {
    int32_t left = start;
    int32_t right = leftPartEnd;

    while (left < startLeftS && right >= startLeftS) {
      tsDataSwap(&pMeterSids[left++], &pMeterSids[right--], TSDB_DATA_TYPE_BINARY, size);
    }

    leftPartEnd -= (startLeftS - start);
  }

  if (leftPartEnd > start) {
    tQSortEx(pMeterSids, size, start, leftPartEnd, pOrderDesc, compareFn);
  }

  if (rightPartStart < end) {
    tQSortEx(pMeterSids, size, rightPartStart, end, pOrderDesc, compareFn);
  }
}

int32_t *calculateSubGroup(void **pSids, int32_t numOfMeters, int32_t *numOfSubset, tOrderDescriptor *pOrderDesc,
                           __ext_compar_fn_t compareFn) {
  int32_t *starterPos = (int32_t *)malloc((numOfMeters + 1) * sizeof(int32_t));  // add additional buffer
  starterPos[0] = 0;

  *numOfSubset = 1;

  for (int32_t i = 1; i < numOfMeters; ++i) {
    int32_t ret = compareFn(pSids[i - 1], pSids[i], pOrderDesc);
    if (ret != 0) {
      assert(ret == -1);
      starterPos[(*numOfSubset)++] = i;
    }
  }

  starterPos[*numOfSubset] = numOfMeters;
  assert(*numOfSubset <= numOfMeters);

  return starterPos;
}

tTagSchema *tCreateTagSchema(SSchema *pSchema, int32_t numOfTagCols) {
  if (numOfTagCols == 0 || pSchema == NULL) {
    return NULL;
  }

  tTagSchema *pTagSchema =
      (tTagSchema *)calloc(1, sizeof(tTagSchema) + numOfTagCols * sizeof(int32_t) + sizeof(SSchema) * numOfTagCols);

  pTagSchema->colOffset[0] = 0;
  pTagSchema->numOfCols = numOfTagCols;
  for (int32_t i = 1; i < numOfTagCols; ++i) {
    pTagSchema->colOffset[i] = (pTagSchema->colOffset[i - 1] + pSchema[i - 1].bytes);
  }

  pTagSchema->pSchema = (SSchema *)&(pTagSchema->colOffset[numOfTagCols]);
  memcpy(pTagSchema->pSchema, pSchema, sizeof(SSchema) * numOfTagCols);
  return pTagSchema;
}

tSidSet *tSidSetCreate(struct SMeterSidExtInfo **pMeterSidExtInfo, int32_t numOfMeters, SSchema *pSchema,
                       int32_t numOfTags, int16_t *orderList, int32_t numOfOrderCols) {
  tSidSet *pSidSet = (tSidSet *)calloc(1, sizeof(tSidSet) + numOfOrderCols * sizeof(int16_t));
  if (pSidSet == NULL) {
    return NULL;
  }

  pSidSet->numOfSids = numOfMeters;
  pSidSet->pSids = pMeterSidExtInfo;
  pSidSet->pTagSchema = tCreateTagSchema(pSchema, numOfTags);
  pSidSet->orderIdx.numOfOrderedCols = numOfOrderCols;

  memcpy(pSidSet->orderIdx.pData, orderList, numOfOrderCols * sizeof(int16_t));

  pSidSet->starterPos = NULL;
  return pSidSet;
}

void tSidSetDestroy(tSidSet **pSets) {
  if ((*pSets) != NULL) {
    tfree((*pSets)->starterPos);
    tfree((*pSets)->pTagSchema)(*pSets)->pSids = NULL;
    tfree(*pSets);
  }
}

void tTagsPrints(SMeterSidExtInfo *pMeterInfo, tTagSchema *pSchema, tOrderIdx *pOrder) {
  printf("sid: %-5d tags(", pMeterInfo->sid);

  for (int32_t i = 0; i < pOrder->numOfOrderedCols; ++i) {
    int32_t tagIdx = pOrder->pData[i];

    if (tagIdx == -1) {
      /* it is the tbname column */
      printf("%s, ", pMeterInfo->tags);
      continue;
    }

    switch (pSchema->pSchema[tagIdx].type) {
      case TSDB_DATA_TYPE_INT:
        printf("%d, ", GET_TAG_VAL(pMeterInfo, tagIdx, pSchema, int32_t));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%lf, ", GET_TAG_VAL(pMeterInfo, tagIdx, pSchema, double));
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%f, ", GET_TAG_VAL(pMeterInfo, tagIdx, pSchema, float));
        break;
      case TSDB_DATA_TYPE_BIGINT:
        printf("%ld, ", GET_TAG_VAL(pMeterInfo, tagIdx, pSchema, int64_t));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%d, ", GET_TAG_VAL(pMeterInfo, tagIdx, pSchema, int16_t));
        break;
      case TSDB_DATA_TYPE_TINYINT:
        printf("%d, ", GET_TAG_VAL(pMeterInfo, tagIdx, pSchema, int8_t));
        break;
      case TSDB_DATA_TYPE_BINARY:
        printf("%s, ", GET_TAG_VAL_POINTER(pMeterInfo, tagIdx, pSchema, char));
        break;
      case TSDB_DATA_TYPE_NCHAR: {
        char *data = GET_TAG_VAL_POINTER(pMeterInfo, tagIdx, pSchema, char);
        char  buffer[512] = {0};

        taosUcs4ToMbs(data, pSchema->pSchema[tagIdx].bytes, buffer);
        printf("%s, ", buffer);
        break;
      }
      case TSDB_DATA_TYPE_BOOL:
        printf("%d, ", GET_TAG_VAL(pMeterInfo, tagIdx, pSchema, int8_t));
        break;

      default:
        assert(false);
    }
  }
  printf(")\n");
}

/*
 * display all the subset groups for debug purpose only
 */
static void UNUSED_FUNC tSidSetDisplay(tSidSet *pSets) {
  printf("%d meters.\n", pSets->numOfSids);
  for (int32_t i = 0; i < pSets->numOfSids; ++i) {
    printf("%d\t", pSets->pSids[i]->sid);
  }
  printf("\n");

  printf("total number of subset group is: %d\n", pSets->numOfSubSet);
  for (int32_t i = 0; i < pSets->numOfSubSet; ++i) {
    int32_t s = pSets->starterPos[i];
    int32_t e = pSets->starterPos[i + 1];
    printf("the %d-th subgroup: \n", i + 1);
    for (int32_t j = s; j < e; ++j) {
      tTagsPrints(pSets->pSids[j], pSets->pTagSchema, &pSets->orderIdx);
    }
  }
}

void tSidSetSort(tSidSet *pSets) {
  pTrace("number of meters in sort: %d", pSets->numOfSids);
  tOrderIdx *pOrderIdx = &pSets->orderIdx;

  if (pOrderIdx->numOfOrderedCols == 0 || pSets->numOfSids <= 1) {
    // no group by clause
    pSets->numOfSubSet = 1;
    pSets->starterPos = (int32_t *)malloc(sizeof(int32_t) * (pSets->numOfSubSet + 1));
    pSets->starterPos[0] = 0;
    pSets->starterPos[1] = pSets->numOfSids;
    pTrace("all meters belong to one subgroup, no need to subgrouping ops.");
#ifdef _DEBUG_VIEW
    tSidSetDisplay(pSets);
#endif
  } else {
    tOrderDescriptor *descriptor =
        (tOrderDescriptor *)calloc(1, sizeof(tOrderDescriptor) + sizeof(int16_t) * pSets->orderIdx.numOfOrderedCols);
    descriptor->pTagSchema = pSets->pTagSchema;
    descriptor->orderIdx = pSets->orderIdx;

    memcpy(descriptor->orderIdx.pData, pOrderIdx->pData, sizeof(int16_t) * pSets->orderIdx.numOfOrderedCols);

    tQSortEx((void **)pSets->pSids, POINTER_BYTES, 0, pSets->numOfSids - 1, descriptor, meterSidComparator);
    pSets->starterPos =
        calculateSubGroup((void **)pSets->pSids, pSets->numOfSids, &pSets->numOfSubSet, descriptor, meterSidComparator);

#ifdef _DEBUG_VIEW
    tSidSetDisplay(pSets);
#endif
    tfree(descriptor);
  }
}
