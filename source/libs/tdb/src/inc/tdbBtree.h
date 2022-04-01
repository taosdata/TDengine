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

#ifndef _TD_BTREE_H_
#define _TD_BTREE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBTree SBTree;
typedef struct SBTC   SBTC;
typedef struct SBtInfo {
  SPgno root;
  int   nLevel;
  int   nData;
} SBtInfo;

struct SBTC {
  SBTree *pBt;
  i8      iPage;
  SPage  *pPage;
  int     idx;
  int     idxStack[BTREE_MAX_DEPTH + 1];
  SPage  *pgStack[BTREE_MAX_DEPTH + 1];
};

// SBTree
int tdbBtreeOpen(int keyLen, int valLen, SPager *pFile, FKeyComparator kcmpr, SBTree **ppBt);
int tdbBtreeClose(SBTree *pBt);
int tdbBtreeInsert(SBTree *pBt, const void *pKey, int kLen, const void *pVal, int vLen);
int tdbBtreeGet(SBTree *pBt, const void *pKey, int kLen, void **ppVal, int *vLen);
int tdbBtreePGet(SBTree *pBt, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen);

// SBTC
int tdbBtcOpen(SBTC *pCur, SBTree *pBt);
int tdbBtcMoveToFirst(SBTC *pBtc);
int tdbBtcMoveToLast(SBTC *pBtc);
int tdbBtreeNext(SBTC *pBtc, void **ppKey, int *kLen, void **ppVal, int *vLen);
int tdbBtcClose(SBTC *pBtc);

#ifdef __cplusplus
}
#endif

#endif /*_TD_BTREE_H_*/