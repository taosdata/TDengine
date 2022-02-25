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

#include "tdbInt.h"

#define BTREE_MAX_DEPTH 20

typedef int (*FKeyComparator)(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

struct SBTree {
  SPgno          root;
  int            keyLen;
  int            valLen;
  SPFile *       pFile;
  FKeyComparator kcmpr;
};

typedef struct SPgHdr {
  u8  flags;
  u8  nFree;
  u16 firstFree;
  u16 nCells;
  u16 pCell;
  i32 kLen;
  i32 vLen;
} SPgHdr;

typedef struct SBtPage {
  SPgHdr *pHdr;
  u16 *   aCellIdx;
} SBtPage;

struct SBtCursor {
  SBTree * pBt;
  i8       iPage;
  SBtPage *pPage;
};

static int tdbBtCursorMoveTo(SBtCursor *pCur, const void *pKey, int kLen);
static int tdbEncodeLength(u8 *pBuf, uint len);
static int tdbBtCursorMoveToRoot(SBtCursor *pCur);
static int tdbInitBtPage(SPage *pPage, SBtPage **ppBtPage);

int tdbBtreeOpen(SPgno root, SBTree **ppBt) {
  *ppBt = NULL;
  /* TODO */
  return 0;
}

int tdbBtreeClose(SBTree *pBt) {
  // TODO
  return 0;
}

int tdbBtreeCursor(SBTree *pBt, SBtCursor *pCur) {
  pCur->pBt = pBt;
  pCur->iPage = -1;
  /* TODO */
  return 0;
}

int tdbBtCursorInsert(SBtCursor *pCur, const void *pKey, int kLen, const void *pVal, int vLen) {
  int     ret;
  SPFile *pFile;

  ret = tdbBtCursorMoveTo(pCur, pKey, kLen);
  if (ret < 0) {
    // TODO: handle error
    return -1;
  }

  pFile = pCur->pBt->pFile;
  // ret = tdbPFileWrite(pFile, pCur->pPage);
  // if (ret < 0) {
  //   // TODO: handle error
  //   return -1;
  // }

  return 0;
}

static int tdbBtCursorMoveTo(SBtCursor *pCur, const void *pKey, int kLen) {
  int ret;

  ret = tdbBtCursorMoveToRoot(pCur);
  // TODO
  return 0;
}

static int tdbEncodeKeyValue(const void *pKey, int kLen, int kLenG, const void *pVal, int vLen, int vLenG, void *pBuf,
                             int *bLen) {
  u8 *pPtr;

  ASSERT(kLen > 0 && vLen > 0);
  ASSERT(kLenG == TDB_VARIANT_LEN || kLenG == kLen);
  ASSERT(vLenG == TDB_VARIANT_LEN || vLenG == vLen);

  pPtr = (u8 *)pBuf;
  if (kLenG == TDB_VARIANT_LEN) {
    pPtr += tdbEncodeLength(pPtr, kLen);
  }

  if (vLenG == TDB_VARIANT_LEN) {
    pPtr += tdbEncodeLength(pPtr, vLen);
  }

  memcpy(pPtr, pKey, kLen);
  pPtr += kLen;

  memcpy(pPtr, pVal, vLen);
  pPtr += vLen;

  *bLen = pPtr - (u8 *)pBuf;
  return 0;
}

static int tdbDecodeKeyValue(const void *pBuf, void *pKey, int *kLen, void *pVal, int *vLen) {
  if (*kLen == TDB_VARIANT_LEN) {
    // Decode the key length
  }

  if (*vLen == TDB_VARIANT_LEN) {
    // Decode the value length
  }

  // TODO: decode the key and value

  return 0;
}

static int tdbEncodeLength(u8 *pBuf, uint len) {
  int iCount = 0;

  while (len > 127) {
    pBuf[iCount++] = (u8)((len & 0xff) | 128);
    len >>= 7;
  }

  pBuf[iCount++] = (u8)len;

  return iCount;
}

static int tdbBtCursorMoveToRoot(SBtCursor *pCur) {
  SBTree * pBt;
  SPFile * pFile;
  SPage *  pPage;
  SBtPage *pBtPage;
  int      ret;

  pBt = pCur->pBt;
  pFile = pBt->pFile;

  pPage = tdbPFileGet(pFile, pBt->root);
  if (pPage == NULL) {
    // TODO: handle error
  }

  ret = tdbInitBtPage(pPage, &pBtPage);
  if (ret < 0) {
    // TODO
    return 0;
  }

  pCur->pPage = pBtPage;
  pCur->iPage = 0;

  return 0;
}

static int tdbInitBtPage(SPage *pPage, SBtPage **ppBtPage) {
  // TODO
  return 0;
}