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

int tdbTxnOpen(TXN *pTxn, int64_t txnid, void *(*xMalloc)(void *, size_t), void (*xFree)(void *, void *), void *xArg,
               int flags) {
  // not support read-committed version at the moment
  if (flags != 0 && flags != (TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED)) {
    tdbError("tdb/txn: invalid txn flags: %" PRId32, flags);
    return -1;
  }

  pTxn->flags = flags;
  pTxn->txnId = txnid;
  pTxn->xMalloc = xMalloc;
  pTxn->xFree = xFree;
  pTxn->xArg = xArg;
  return 0;
}

int tdbTxnCloseImpl(TXN *pTxn) {
  if (pTxn) {
    if (pTxn->jPageSet) {
      hashset_destroy(pTxn->jPageSet);
      pTxn->jPageSet = NULL;
    }

    if (pTxn->jfd) {
      tdbOsClose(pTxn->jfd);
      ASSERT(pTxn->jfd == NULL);
    }

    tdbOsFree(pTxn);
  }

  return 0;
}
