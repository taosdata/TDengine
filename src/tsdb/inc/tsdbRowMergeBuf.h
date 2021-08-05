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
#include "tchecksum.h"
#include "tsdbReadImpl.h"

typedef void* SMergeBuf;

SDataRow tsdbMergeTwoRows(SMergeBuf *pBuf, SMemRow row1, SMemRow row2, STSchema *pSchema1, STSchema *pSchema2);

static FORCE_INLINE int tsdbMergeBufMakeSureRoom(SMergeBuf *pBuf, STSchema* pSchema1, STSchema* pSchema2) {
  size_t len1 = dataRowMaxBytesFromSchema(pSchema1);
  size_t len2 = dataRowMaxBytesFromSchema(pSchema2);
  return tsdbMakeRoom(pBuf, MAX(len1, len2));
}

static FORCE_INLINE void tsdbFreeMergeBuf(SMergeBuf buf) {
  taosTZfree(buf);
}

#ifdef __cplusplus
}
#endif

#endif /* ifndef TSDB_ROW_MERGE_BUF_H */
