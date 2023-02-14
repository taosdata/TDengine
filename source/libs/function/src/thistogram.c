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

#include "taosdef.h"
#include "thistogram.h"
#include "tlosertree.h"
#include "tmsg.h"

/**
 *
 * implement the histogram and percentile_approx based on the paper:
 * Yael Ben-Haim, Elad Tom-Tov. A Streaming Parallel Decision Tree Algorithm,
 * The Journal of Machine Learning Research.Volume 11, 3/1/2010 pp.849-872
 * https://dl.acm.org/citation.cfm?id=1756034
 *
 * @data 2018-12-14
 * @version 0.1
 *
 */
static int32_t histogramCreateBin(SHistogramInfo* pHisto, int32_t index, double val);

SHistogramInfo* tHistogramCreate(int32_t numOfEntries) {
  /* need one redundant slot */
  SHistogramInfo* pHisto = taosMemoryMalloc(sizeof(SHistogramInfo) + sizeof(SHistBin) * (numOfEntries + 1));

#if !defined(USE_ARRAYLIST)
  pHisto->pList = SSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
  SInsertSupporter* pss = taosMemoryMalloc(sizeof(SInsertSupporter));
  pss->numOfEntries = pHisto->maxEntries;
  pss->pSkipList = pHisto->pList;

  int32_t ret = tLoserTreeCreate1(&pHisto->pLoserTree, numOfEntries, pss, compare);
  pss->pTree = pHisto->pLoserTree;
#endif

  return tHistogramCreateFrom(pHisto, numOfEntries);
}

SHistogramInfo* tHistogramCreateFrom(void* pBuf, int32_t numOfBins) {
  memset(pBuf, 0, sizeof(SHistogramInfo) + sizeof(SHistBin) * (numOfBins + 1));

  SHistogramInfo* pHisto = (SHistogramInfo*)pBuf;
  pHisto->elems = (SHistBin*)((char*)pBuf + sizeof(SHistogramInfo));
  for (int32_t i = 0; i < numOfBins; ++i) {
    pHisto->elems[i].val = -DBL_MAX;
  }

  pHisto->maxEntries = numOfBins;

  pHisto->min = DBL_MAX;
  pHisto->max = -DBL_MAX;

  return pBuf;
}

int32_t tHistogramAdd(SHistogramInfo** pHisto, double val) {
  if (*pHisto == NULL) {
    *pHisto = tHistogramCreate(MAX_HISTOGRAM_BIN);
  }

#if defined(USE_ARRAYLIST)
  int32_t idx = histoBinarySearch((*pHisto)->elems, (*pHisto)->numOfEntries, val);
  if (ASSERTS(idx >= 0 && idx <= (*pHisto)->maxEntries && (*pHisto)->elems != NULL, "tHistogramAdd Error, idx:%d, maxEntries:%d, elems:%p",
              idx, (*pHisto)->maxEntries, (*pHisto)->elems)) {
    return -1;
  }

  if ((*pHisto)->elems[idx].val == val && idx >= 0) {
    (*pHisto)->elems[idx].num += 1;

    if ((*pHisto)->numOfEntries == 0) {
      (*pHisto)->numOfEntries += 1;
    }
  } else { /* insert a new slot */
    if ((*pHisto)->numOfElems >= 1 && idx < (*pHisto)->numOfEntries) {
      if (idx > 0) {
        if (ASSERTS((*pHisto)->elems[idx - 1].val <= val, "tHistogramAdd Error, elems[%d].val:%lf, val:%lf",
                    idx - 1, (*pHisto)->elems[idx - 1].val, val)) {
          return -1;
        }
      } else {
        if (ASSERTS((*pHisto)->elems[idx].val > val, "tHistogramAdd Error, elems[%d].val:%lf, val:%lf",
                    idx, (*pHisto)->elems[idx].val, val)) {
          return -1;
        }
      }
    } else if ((*pHisto)->numOfElems > 0) {
      if (ASSERTS((*pHisto)->elems[(*pHisto)->numOfEntries].val <= val, "tHistogramAdd Error, elems[%d].val:%lf, val:%lf",
                  (*pHisto)->numOfEntries, (*pHisto)->elems[idx].val, val)) {
        return -1;
      }
    }

    int32_t code = histogramCreateBin(*pHisto, idx, val);
    if (code != 0) {
      return code;
    }
  }
#else
  tSkipListKey key = tSkipListCreateKey(TSDB_DATA_TYPE_DOUBLE, &val, tDataTypes[TSDB_DATA_TYPE_DOUBLE].nSize);
  SHistBin*    entry = taosMemoryCalloc(1, sizeof(SHistBin));
  entry->val = val;

  tSkipListNode* pResNode = SSkipListPut((*pHisto)->pList, entry, &key, 0);

  SHistBin* pEntry1 = (SHistBin*)pResNode->pData;
  pEntry1->index = -1;

  tSkipListNode* pLast = NULL;

  if (pEntry1->num == 0) { /* it is a new node */
    (*pHisto)->numOfEntries += 1;
    pEntry1->num += 1;

    /* number of entries reaches the upper limitation */
    if (pResNode->pForward[0] != NULL) {
      /* we need to update the last updated slot in loser tree*/
      pEntry1->delta = ((SHistBin*)pResNode->pForward[0]->pData)->val - val;

      if ((*pHisto)->ordered) {
        int32_t                 lastIndex = (*pHisto)->maxIndex;
        SMultiwayMergeTreeInfo* pTree = (*pHisto)->pLoserTree;

        (*pHisto)->pLoserTree->pNode[lastIndex + pTree->numOfEntries].pData = pResNode;
        pEntry1->index = (*pHisto)->pLoserTree->pNode[lastIndex + pTree->numOfEntries].index;

        // update the loser tree
        if ((*pHisto)->ordered) {
          tMergeTreeAdjust(pTree, pEntry1->index + pTree->numOfEntries);
        }

        tSkipListKey kx =
            tSkipListCreateKey(TSDB_DATA_TYPE_DOUBLE, &(*pHisto)->max, tDataTypes[TSDB_DATA_TYPE_DOUBLE].nSize);
        pLast = tSkipListGetOne((*pHisto)->pList, &kx);
      }
    } else {
      /* this node located at the last position of the skiplist, we do not
       * update the loser-tree */
      pEntry1->delta = DBL_MAX;
      pLast = pResNode;
    }

    if (pResNode->pBackward[0] != &(*pHisto)->pList->pHead) {
      SHistBin* pPrevEntry = (SHistBin*)pResNode->pBackward[0]->pData;
      pPrevEntry->delta = val - pPrevEntry->val;

      SMultiwayMergeTreeInfo* pTree = (*pHisto)->pLoserTree;
      if ((*pHisto)->ordered) {
        tMergeTreeAdjust(pTree, pPrevEntry->index + pTree->numOfEntries);
        tMergeTreePrint(pTree);
      }
    }

    if ((*pHisto)->numOfEntries >= (*pHisto)->maxEntries + 1) {
      // set the right value for loser-tree
      if (!(*pHisto)->ordered) {
        SSkipListPrint((*pHisto)->pList, 1);

        SMultiwayMergeTreeInfo* pTree = (*pHisto)->pLoserTree;
        tSkipListNode*          pHead = (*pHisto)->pList->pHead.pForward[0];

        tSkipListNode* p1 = pHead;

        printf("\n");
        while (p1 != NULL) {
          printf("%f\t", ((SHistBin*)(p1->pData))->delta);
          p1 = p1->pForward[0];
        }
        printf("\n");

        /* last one in skiplist is ignored */
        for (int32_t i = pTree->numOfEntries; i < pTree->totalEntries; ++i) {
          pTree->pNode[i].pData = pHead;
          pTree->pNode[i].index = i - pTree->numOfEntries;
          SHistBin* pBin = (SHistBin*)pHead->pData;
          pBin->index = pTree->pNode[i].index;

          pHead = pHead->pForward[0];
        }

        pLast = pHead;

        for (int32_t i = 0; i < pTree->numOfEntries; ++i) {
          pTree->pNode[i].index = -1;
        }

        tMergeTreePrint(pTree);

        for (int32_t i = pTree->totalEntries - 1; i >= pTree->numOfEntries; i--) {
          tMergeTreeAdjust(pTree, i);
        }

        tMergeTreePrint(pTree);
        (*pHisto)->ordered = true;
      }

      printf("delta is:%lf\n", pEntry1->delta);

      SSkipListPrint((*pHisto)->pList, 1);

      /* the chosen node */
      tSkipListNode* pNode = (*pHisto)->pLoserTree->pNode[0].pData;
      SHistBin*      pEntry = (SHistBin*)pNode->pData;

      tSkipListNode* pNext = pNode->pForward[0];
      SHistBin*      pNextEntry = (SHistBin*)pNext->pData;
      if (ASSERTS(pNextEntry->val - pEntry->val == pEntry->delta, "tHistogramAdd Error, pNextEntry->val:%lf, pEntry->val:%lf, pEntry->delta:%lf",
                  pNextEntry->val, pEntry->val, pEntry->delta)) {
        return -1;
      }

      double newVal = (pEntry->val * pEntry->num + pNextEntry->val * pNextEntry->num) / (pEntry->num + pNextEntry->num);
      pEntry->val = newVal;
      pNode->key.dKey = newVal;
      pEntry->num = pEntry->num + pNextEntry->num;

      // update delta value in current node
      pEntry->delta = (pNextEntry->delta + pNextEntry->val) - pEntry->val;

      // reset delta value in the previous node
      SHistBin* pPrevEntry = (SHistBin*)pNode->pBackward[0]->pData;
      if (pPrevEntry) {
        pPrevEntry->delta = pEntry->val - pPrevEntry->val;
      }

      SMultiwayMergeTreeInfo* pTree = (*pHisto)->pLoserTree;
      if (pNextEntry->index != -1) {
        (*pHisto)->maxIndex = pNextEntry->index;

        // set the last element in skiplist, of which delta is FLT_MAX;
        pTree->pNode[pNextEntry->index + pTree->numOfEntries].pData = pLast;
        ((SHistBin*)pLast->pData)->index = pNextEntry->index;
        int32_t f = pTree->pNode[pNextEntry->index + pTree->numOfEntries].index;
        printf("disappear index is:%d\n", f);
      }

      tMergeTreeAdjust(pTree, pEntry->index + pTree->numOfEntries);
      // remove the next node in skiplist
      tSkipListRemoveNode((*pHisto)->pList, pNext);
      SSkipListPrint((*pHisto)->pList, 1);

      tMergeTreePrint((*pHisto)->pLoserTree);
    } else {  // add to heap
      if (pResNode->pForward[0] != NULL) {
        pEntry1->delta = ((SHistBin*)pResNode->pForward[0]->pData)->val - val;
      } else {
        pEntry1->delta = DBL_MAX;
      }

      if (pResNode->pBackward[0] != &(*pHisto)->pList->pHead) {
        SHistBin* pPrevEntry = (SHistBin*)pResNode->pBackward[0]->pData;
        pEntry1->delta = val - pPrevEntry->val;
      }

      printf("delta is:%9lf\n", pEntry1->delta);
    }

  } else {
    SHistBin* pEntry = (SHistBin*)pResNode->pData;
    if (ASSERTS(pEntry->val == val, "tHistogramAdd Error, pEntry->val:%lf, val:%lf")) {
      return -1;
    }
    pEntry->num += 1;
  }

#endif
  if (val > (*pHisto)->max) {
    (*pHisto)->max = val;
  }

  if (val < (*pHisto)->min) {
    (*pHisto)->min = val;
  }

  (*pHisto)->numOfElems += 1;
  return 0;
}

int32_t histoBinarySearch(SHistBin* pEntry, int32_t len, double val) {
  int32_t end = len - 1;
  int32_t start = 0;

  while (start <= end) {
    int32_t mid = (end - start) / 2 + start;
    if (pEntry[mid].val == val) {
      return mid;
    }

    if (pEntry[mid].val < val) {
      start = mid + 1;
    } else {
      end = mid - 1;
    }
  }

  int32_t ret = start > end ? start : end;
  if (ret < 0) {
    return 0;
  } else {
    return ret;
  }
}

static void histogramMergeImpl(SHistBin* pHistBin, int32_t* size) {
#if defined(USE_ARRAYLIST)
  int32_t oldSize = *size;

  double  delta = DBL_MAX;
  int32_t index = -1;
  for (int32_t i = 1; i < oldSize; ++i) {
    double d = pHistBin[i].val - pHistBin[i - 1].val;
    if (d < delta) {
      delta = d;
      index = i - 1;
    }
  }

  SHistBin* s1 = &pHistBin[index];
  SHistBin* s2 = &pHistBin[index + 1];

  double newVal = (s1->val * s1->num + s2->val * s2->num) / (s1->num + s2->num);
  s1->val = newVal;
  s1->num = s1->num + s2->num;

  memmove(&pHistBin[index + 1], &pHistBin[index + 2], (oldSize - index - 2) * sizeof(SHistBin));
  (*size) -= 1;
#endif
}

/* optimize this procedure */
int32_t histogramCreateBin(SHistogramInfo* pHisto, int32_t index, double val) {
#if defined(USE_ARRAYLIST)
  int32_t remain = pHisto->numOfEntries - index;
  if (remain > 0) {
    memmove(&pHisto->elems[index + 1], &pHisto->elems[index], sizeof(SHistBin) * remain);
  }

  if (ASSERTS(index >= 0 && index <= pHisto->maxEntries, "histogramCreateBin Error, index:%d, maxEntries:%d",
              index, pHisto->maxEntries)) {
    return -1;
  }

  pHisto->elems[index].num = 1;
  pHisto->elems[index].val = val;
  pHisto->numOfEntries += 1;

  /* we need to merge the slot */
  if (pHisto->numOfEntries == pHisto->maxEntries + 1) {
    histogramMergeImpl(pHisto->elems, &pHisto->numOfEntries);

    pHisto->elems[pHisto->maxEntries].val = 0;
    pHisto->elems[pHisto->maxEntries].num = 0;
  }
#endif
  if (ASSERTS(pHisto->numOfEntries <= pHisto->maxEntries, "histogramCreateBin Error, numOfEntries:%d, maxEntries:%d",
              pHisto->numOfEntries, pHisto->maxEntries)) {
    return -1;
  }

  return 0;
}

void tHistogramDestroy(SHistogramInfo** pHisto) {
  if (*pHisto == NULL) {
    return;
  }

  taosMemoryFree(*pHisto);
  *pHisto = NULL;
}

void tHistogramPrint(SHistogramInfo* pHisto) {
  printf("total entries: %d, elements: %" PRId64 "\n", pHisto->numOfEntries, pHisto->numOfElems);
#if defined(USE_ARRAYLIST)
  for (int32_t i = 0; i < pHisto->numOfEntries; ++i) {
    printf("%d: (%f, %" PRId64 ")\n", i + 1, pHisto->elems[i].val, pHisto->elems[i].num);
  }
#else
  tSkipListNode* pNode = pHisto->pList->pHead.pForward[0];

  for (int32_t i = 0; i < pHisto->numOfEntries; ++i) {
    SHistBin* pEntry = (SHistBin*)pNode->pData;
    printf("%d: (%f, %" PRId64 ")\n", i + 1, pEntry->val, pEntry->num);
    pNode = pNode->pForward[0];
  }
#endif
}

/**
 * Estimated number of points in the interval (−inf,b].
 * @param pHisto
 * @param v
 */
int64_t tHistogramSum(SHistogramInfo* pHisto, double v) {
#if defined(USE_ARRAYLIST)
  int32_t slotIdx = histoBinarySearch(pHisto->elems, pHisto->numOfEntries, v);
  if (pHisto->elems[slotIdx].val != v) {
    slotIdx -= 1;

    if (slotIdx < 0) {
      slotIdx = 0;
      ASSERTS(v <= pHisto->elems[slotIdx].val, "tHistogramSum Error, elems[%d].val:%lf, v:%lf",
              slotIdx, pHisto->elems[slotIdx].val, v);
    } else {
      ASSERTS(v >= pHisto->elems[slotIdx].val, "tHistogramSum Error, elems[%d].val:%lf, v:%lf",
              slotIdx, pHisto->elems[slotIdx].val, v);
      if (slotIdx + 1 < pHisto->numOfEntries) {
        ASSERTS(v < pHisto->elems[slotIdx + 1].val, "tHistogramSum Error, elems[%d].val:%lf, v:%lf",
                slotIdx + 1, pHisto->elems[slotIdx + 1].val, v);
      }
    }
  }

  double m1 = (double)pHisto->elems[slotIdx].num;
  double v1 = pHisto->elems[slotIdx].val;

  double m2 = (double)pHisto->elems[slotIdx + 1].num;
  double v2 = pHisto->elems[slotIdx + 1].val;

  double estNum = m1 + (m2 - m1) * (v - v1) / (v2 - v1);
  double s1 = (m1 + estNum) * (v - v1) / (2 * (v2 - v1));

  for (int32_t i = 0; i < slotIdx; ++i) {
    s1 += pHisto->elems[i].num;
  }

  s1 = s1 + m1 / 2;

  return (int64_t)s1;
#endif
}

double* tHistogramUniform(SHistogramInfo* pHisto, double* ratio, int32_t num) {
#if defined(USE_ARRAYLIST)
  double* pVal = taosMemoryMalloc(num * sizeof(double));

  for (int32_t i = 0; i < num; ++i) {
    double numOfElem = (ratio[i] / 100) * pHisto->numOfElems;

    if (numOfElem == 0) {
      pVal[i] = pHisto->min;
      continue;
    } else if (numOfElem <= pHisto->elems[0].num) {
      pVal[i] = pHisto->elems[0].val;
      continue;
    } else if (numOfElem == pHisto->numOfElems) {
      pVal[i] = pHisto->max;
      continue;
    }

    int32_t j = 0;
    int64_t total = 0;

    while (j < pHisto->numOfEntries) {
      total += pHisto->elems[j].num;
      if (total <= numOfElem && total + pHisto->elems[j + 1].num > numOfElem) {
        break;
      }

      j += 1;
    }

    ASSERTS(total <= numOfElem && total + pHisto->elems[j + 1].num > numOfElem,
            "tHistogramUniform Error, total:%d, numOfElem:%d, elems[%d].num:%d",
            total, numOfElem, j + 1, pHisto->elems[j + 1].num);

    double delta = numOfElem - total;
    if (fabs(delta) < FLT_EPSILON) {
      pVal[i] = pHisto->elems[j].val;
    }

    double start = (double)pHisto->elems[j].num;
    double range = pHisto->elems[j + 1].num - start;

    if (range == 0) {
      pVal[i] = (pHisto->elems[j + 1].val - pHisto->elems[j].val) * delta / start + pHisto->elems[j].val;
    } else {
      double factor = (-2 * start + sqrt(4 * start * start - 4 * range * (-2 * delta))) / (2 * range);
      pVal[i] = pHisto->elems[j].val + (pHisto->elems[j + 1].val - pHisto->elems[j].val) * factor;
    }
  }
#else
  double* pVal = taosMemoryMalloc(num * sizeof(double));

  for (int32_t i = 0; i < num; ++i) {
    double numOfElem = ratio[i] * pHisto->numOfElems;

    tSkipListNode* pFirst = pHisto->pList->pHead.pForward[0];
    SHistBin*      pEntry = (SHistBin*)pFirst->pData;
    if (numOfElem == 0) {
      pVal[i] = pHisto->min;
      printf("i/numofSlot: %f, v:%f, %f\n", ratio[i], numOfElem, pVal[i]);
      continue;
    } else if (numOfElem <= pEntry->num) {
      pVal[i] = pEntry->val;
      printf("i/numofSlot: %f, v:%f, %f\n", ratio[i], numOfElem, pVal[i]);
      continue;
    } else if (numOfElem == pHisto->numOfElems) {
      pVal[i] = pHisto->max;
      printf("i/numofSlot: %f, v:%f, %f\n", ratio[i], numOfElem, pVal[i]);
      continue;
    }

    int32_t   j = 0;
    int64_t   total = 0;
    SHistBin* pPrev = pEntry;

    while (j < pHisto->numOfEntries) {
      if (total <= numOfElem && total + pEntry->num > numOfElem) {
        break;
      }

      total += pEntry->num;
      pPrev = pEntry;

      pFirst = pFirst->pForward[0];
      pEntry = (SHistBin*)pFirst->pData;

      j += 1;
    }

    ASSERTS(total <= numOfElem && total + pEntry->num > numOfElem,
            "tHistogramUniform Error, total:%d, numOfElem:%d, pEntry->num:%d",
            total, numOfElem, pEntry->num);

    double delta = numOfElem - total;
    if (fabs(delta) < FLT_EPSILON) {
      //                printf("i/numofSlot: %f, v:%f, %f\n",
      //                (double)i/numOfSlots, numOfElem, pHisto->elems[j].val);
      pVal[i] = pPrev->val;
    }

    double start = pPrev->num;
    double range = pEntry->num - start;

    if (range == 0) {
      pVal[i] = (pEntry->val - pPrev->val) * delta / start + pPrev->val;
    } else {
      double factor = (-2 * start + sqrt(4 * start * start - 4 * range * (-2 * delta))) / (2 * range);
      pVal[i] = pPrev->val + (pEntry->val - pPrev->val) * factor;
    }
    //            printf("i/numofSlot: %f, v:%f, %f\n", (double)i/numOfSlots,
    //            numOfElem, val);
  }
#endif
  return pVal;
}

SHistogramInfo* tHistogramMerge(SHistogramInfo* pHisto1, SHistogramInfo* pHisto2, int32_t numOfEntries) {
  SHistogramInfo* pResHistogram = tHistogramCreate(numOfEntries);

  // error in histogram info
  if (pHisto1->numOfEntries > MAX_HISTOGRAM_BIN || pHisto2->numOfEntries > MAX_HISTOGRAM_BIN) {
    return pResHistogram;
  }

  SHistBin* pHistoBins = taosMemoryCalloc(1, sizeof(SHistBin) * (pHisto1->numOfEntries + pHisto2->numOfEntries));
  int32_t   i = 0, j = 0, k = 0;

  while (i < pHisto1->numOfEntries && j < pHisto2->numOfEntries) {
    if (pHisto1->elems[i].val < pHisto2->elems[j].val) {
      pHistoBins[k++] = pHisto1->elems[i++];
    } else if (pHisto1->elems[i].val > pHisto2->elems[j].val) {
      pHistoBins[k++] = pHisto2->elems[j++];
    } else {
      pHistoBins[k] = pHisto1->elems[i++];
      pHistoBins[k++].num += pHisto2->elems[j++].num;
    }
  }

  if (i < pHisto1->numOfEntries) {
    int32_t remain = pHisto1->numOfEntries - i;
    memcpy(&pHistoBins[k], &pHisto1->elems[i], sizeof(SHistBin) * remain);
    k += remain;
  }

  if (j < pHisto2->numOfEntries) {
    int32_t remain = pHisto2->numOfEntries - j;
    memcpy(&pHistoBins[k], &pHisto2->elems[j], sizeof(SHistBin) * remain);
    k += remain;
  }

  /* update other information */
  pResHistogram->numOfElems = pHisto1->numOfElems + pHisto2->numOfElems;
  pResHistogram->min = (pHisto1->min < pHisto2->min) ? pHisto1->min : pHisto2->min;
  pResHistogram->max = (pHisto1->max > pHisto2->max) ? pHisto1->max : pHisto2->max;

  while (k > numOfEntries) {
    histogramMergeImpl(pHistoBins, &k);
  }

  pResHistogram->numOfEntries = k;
  memcpy(pResHistogram->elems, pHistoBins, sizeof(SHistBin) * k);

  taosMemoryFree(pHistoBins);
  return pResHistogram;
}
