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

#include "metaDef.h"
#include "tcoding.h"

int metaValidateTbOptions(SMeta *pMeta, const STbOptions *pTbOptions) {
  // TODO
  return 0;
}

size_t metaEncodeTbObjFromTbOptions(const STbOptions *pTbOptions, void *pBuf, size_t bsize) {
  void **ppBuf = &pBuf;
  int    tlen = 0;

  tlen += taosEncodeFixedU8(ppBuf, pTbOptions->type);
  tlen += taosEncodeString(ppBuf, pTbOptions->name);
  tlen += taosEncodeFixedU32(ppBuf, pTbOptions->ttl);

  switch (pTbOptions->type) {
    case META_SUPER_TABLE:
      tlen += taosEncodeFixedU64(ppBuf, pTbOptions->stbOptions.uid);
      tlen += tdEncodeSchema(ppBuf, pTbOptions->stbOptions.pTagSchema);
      // TODO: encode schema version array
      break;
    case META_CHILD_TABLE:
      tlen += taosEncodeFixedU64(ppBuf, pTbOptions->ctbOptions.suid);
      break;
    case META_NORMAL_TABLE:
      // TODO: encode schema version array
      break;
    default:
      break;
  }

  return tlen;
}