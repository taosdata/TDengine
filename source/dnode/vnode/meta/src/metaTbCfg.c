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

int metaValidateTbCfg(SMeta *pMeta, const STbCfg *pTbOptions) {
  // TODO
  return 0;
}

size_t metaEncodeTbObjFromTbOptions(const STbCfg *pTbOptions, void *pBuf, size_t bsize) {
  void **ppBuf = &pBuf;
  int    tlen = 0;

  tlen += taosEncodeFixedU8(ppBuf, pTbOptions->type);
  tlen += taosEncodeString(ppBuf, pTbOptions->name);
  tlen += taosEncodeFixedU32(ppBuf, pTbOptions->ttl);

  switch (pTbOptions->type) {
    case META_SUPER_TABLE:
      tlen += taosEncodeFixedU64(ppBuf, pTbOptions->stbCfg.suid);
      tlen += tdEncodeSchema(ppBuf, pTbOptions->stbCfg.pTagSchema);
      // TODO: encode schema version array
      break;
    case META_CHILD_TABLE:
      tlen += taosEncodeFixedU64(ppBuf, pTbOptions->ctbCfg.suid);
      break;
    case META_NORMAL_TABLE:
      // TODO: encode schema version array
      break;
    default:
      break;
  }

  return tlen;
}

int metaEncodeTbCfg(void **pBuf, STbCfg *pTbCfg) {
  int tsize = 0;

  tsize += taosEncodeString(pBuf, pTbCfg->name);
  tsize += taosEncodeFixedU32(pBuf, pTbCfg->ttl);
  tsize += taosEncodeFixedU32(pBuf, pTbCfg->keep);
  tsize += taosEncodeFixedU8(pBuf, pTbCfg->type);

  switch (pTbCfg->type) {
    case META_SUPER_TABLE:
      tsize += taosEncodeFixedU64(pBuf, pTbCfg->stbCfg.suid);
      tsize += tdEncodeSchema(pBuf, pTbCfg->stbCfg.pSchema);
      tsize += tdEncodeSchema(pBuf, pTbCfg->stbCfg.pTagSchema);
      break;
    case META_CHILD_TABLE:
      tsize += taosEncodeFixedU64(pBuf, pTbCfg->ctbCfg.suid);
      tsize += tdEncodeKVRow(pBuf, pTbCfg->ctbCfg.pTag);
      break;
    case META_NORMAL_TABLE:
      tsize += tdEncodeSchema(pBuf, pTbCfg->ntbCfg.pSchema);
      break;
    default:
      break;
  }

  return tsize;
}

void *metaDecodeTbCfg(void *pBuf, STbCfg *pTbCfg) {
  pBuf = taosDecodeString(pBuf, &(pTbCfg->name));
  pBuf = taosDecodeFixedU32(pBuf, &(pTbCfg->ttl));
  pBuf = taosDecodeFixedU32(pBuf, &(pTbCfg->keep));
  pBuf = taosDecodeFixedU8(pBuf, &(pTbCfg->type));

  switch (pTbCfg->type) {
    case META_SUPER_TABLE:
      pBuf = taosDecodeFixedU64(pBuf, &(pTbCfg->stbCfg.suid));
      pBuf = tdDecodeSchema(pBuf, &(pTbCfg->stbCfg.pSchema));
      pBuf = tdDecodeSchema(pBuf, &(pTbCfg->stbCfg.pTagSchema));
      break;
    case META_CHILD_TABLE:
      pBuf = taosDecodeFixedU64(pBuf, &(pTbCfg->ctbCfg.suid));
      pBuf = tdDecodeKVRow(pBuf, &(pTbCfg->ctbCfg.pTag));
      break;
    case META_NORMAL_TABLE:
      pBuf = tdDecodeSchema(pBuf, &(pTbCfg->ntbCfg.pSchema));
      break;
    default:
      break;
  }

  return pBuf;
}