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

#ifndef TSDB_ROW_MERGE_BUF_H
#define TSDB_ROW_MERGE_BUF_H 

#ifdef __cplusplus
extern "C" {
#endif

#include "tsdb.h"

typedef void* SMergeBuf;

#define SMERGE_BUF_LEN(x) (*(int*)(x))
#define SMERGE_BUF_PTR(x) POINTER_SHIFT(x, sizeof(int))

int tsdbMergeBufMakeSureRoom(SMergeBuf *pBuf, STSchema* pSchema1, STSchema* pSchema2);

SDataRow tsdbMergeTwoRows(SMergeBuf *pBuf, SMemRow row1, SMemRow row2, STSchema *pSchema1, STSchema *pSchema2);

FORCE_INLINE SMergeBuf tsdbMakeBuf(int size) {
  return malloc(size);
}

FORCE_INLINE void tsdbFreeMergeBuf(SMergeBuf buf) {
  if(buf) free(buf);
}

#ifdef __cplusplus
}
#endif

#endif /* ifndef TSDB_ROW_MERGE_BUF_H */
