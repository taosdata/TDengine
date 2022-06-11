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

#ifndef _TD_TDB_H_
#define _TD_TDB_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*tdb_cmpr_fn_t)(const void *pKey1, int32_t kLen1, const void *pKey2, int32_t kLen2);

// exposed types
typedef struct STDB TDB;
typedef struct STTB TTB;
typedef struct STBC TBC;
typedef struct STxn TXN;

// TDB
int32_t tdbOpen(const char *dbname, int szPage, int pages, TDB **ppDb);
int32_t tdbClose(TDB *pDb);
int32_t tdbBegin(TDB *pDb, TXN *pTxn);
int32_t tdbCommit(TDB *pDb, TXN *pTxn);

// TTB
int32_t tdbTbOpen(const char *tbname, int keyLen, int valLen, tdb_cmpr_fn_t keyCmprFn, TDB *pEnv, TTB **ppTb);
int32_t tdbTbClose(TTB *pTb);
int32_t tdbTbDrop(TTB *pTb);
int32_t tdbTbInsert(TTB *pTb, const void *pKey, int keyLen, const void *pVal, int valLen, TXN *pTxn);
int32_t tdbTbDelete(TTB *pTb, const void *pKey, int kLen, TXN *pTxn);
int32_t tdbTbUpsert(TTB *pTb, const void *pKey, int kLen, const void *pVal, int vLen, TXN *pTxn);
int32_t tdbTbGet(TTB *pTb, const void *pKey, int kLen, void **ppVal, int *vLen);
int32_t tdbTbPGet(TTB *pTb, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen);

// TBC
int32_t tdbTbcOpen(TTB *pTb, TBC **ppTbc, TXN *pTxn);
int32_t tdbTbcClose(TBC *pTbc);
int32_t tdbTbcIsValid(TBC *pTbc);
int32_t tdbTbcMoveTo(TBC *pTbc, const void *pKey, int kLen, int *c);
int32_t tdbTbcMoveToFirst(TBC *pTbc);
int32_t tdbTbcMoveToLast(TBC *pTbc);
int32_t tdbTbcMoveToNext(TBC *pTbc);
int32_t tdbTbcMoveToPrev(TBC *pTbc);
int32_t tdbTbcGet(TBC *pTbc, const void **ppKey, int *pkLen, const void **ppVal, int *pvLen);
int32_t tdbTbcDelete(TBC *pTbc);
int32_t tdbTbcNext(TBC *pTbc, void **ppKey, int *kLen, void **ppVal, int *vLen);
int32_t tdbTbcUpsert(TBC *pTbc, const void *pKey, int nKey, const void *pData, int nData, int insert);

// TXN
#define TDB_TXN_WRITE            0x1
#define TDB_TXN_READ_UNCOMMITTED 0x2

int32_t tdbTxnOpen(TXN *pTxn, int64_t txnid, void *(*xMalloc)(void *, size_t), void (*xFree)(void *, void *),
                   void *xArg, int flags);
int32_t tdbTxnClose(TXN *pTxn);

// other
void tdbFree(void *);

struct STxn {
  int     flags;
  int64_t txnId;
  void *(*xMalloc)(void *, size_t);
  void (*xFree)(void *, void *);
  void *xArg;
};

// error code
enum { TDB_CODE_SUCCESS = 0, TDB_CODE_MAX };

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_H_*/