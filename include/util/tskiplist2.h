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
#ifndef _TD_UTIL_SKIPLIST2_H_
#define _TD_UTIL_SKIPLIST2_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SL_MAX_LEVEL 15

typedef struct SSkipList2 SSkipList2;
typedef struct SSLCursor  SSLCursor;
typedef struct SSLCfg     SSLCfg;

typedef int32_t (*tslCmprFn)(const void *pKey1, int32_t nKey1, const void *pKey2, int32_t nKey2);

// SSkipList2
int32_t slOpen(const SSLCfg *pCfg, SSkipList2 **ppSl);
int32_t slClose(SSkipList2 *pSl);

// SSLCursor
int32_t slcOpen(SSkipList2 *pSl, SSLCursor *pSlc);
int32_t slcClose(SSLCursor *pSlc);
int32_t slcMoveTo(SSLCursor *pSlc, const void *pKey, int32_t nKey);
int32_t slcMoveToNext(SSLCursor *pSlc);
int32_t slcMoveToPrev(SSLCursor *pSlc);
int32_t slcMoveToFirst(SSLCursor *pSlc);
int32_t slcMoveToLast(SSLCursor *pSlc);
int32_t slcPut(SSLCursor *pSlc, const void *pKey, int32_t nKey, const void *pData, int32_t nData);
int32_t slcGet(SSLCursor *pSlc, const void **ppKey, int32_t *nKey, const void **ppData, int32_t *nData);
int32_t slcDrop(SSLCursor *pSlc);

// struct
struct SSLCfg {
  int8_t    maxLevel;
  int32_t   nKey;
  int32_t   nData;
  tslCmprFn cmprFn;
  void     *pPool;
  void *(*xMalloc)(void *, int32_t size);
  void (*xFree)(void *, void *);
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SKIPLIST2_H_*/