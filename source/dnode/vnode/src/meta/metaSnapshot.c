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

#include "meta.h"

struct SMetaSnapshotReader {
  SMeta*  pMeta;
  TBC*    pTbc;
  int64_t sver;
  int64_t ever;
};

int32_t metaSnapshotReaderOpen(SMeta* pMeta, SMetaSnapshotReader** ppReader, int64_t sver, int64_t ever) {
  int32_t              code = 0;
  int32_t              c = 0;
  SMetaSnapshotReader* pMetaReader = NULL;

  pMetaReader = (SMetaSnapshotReader*)taosMemoryCalloc(1, sizeof(*pMetaReader));
  if (pMetaReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pMetaReader->pMeta = pMeta;
  pMetaReader->sver = sver;
  pMetaReader->ever = ever;
  code = tdbTbcOpen(pMeta->pTbDb, &pMetaReader->pTbc, NULL);
  if (code) {
    goto _err;
  }

  code = tdbTbcMoveTo(pMetaReader->pTbc, &(STbDbKey){.version = sver, .uid = INT64_MIN}, sizeof(STbDbKey), &c);
  if (code) {
    goto _err;
  }

  *ppReader = pMetaReader;
  return code;

_err:
  *ppReader = NULL;
  return code;
}

int32_t metaSnapshotReaderClose(SMetaSnapshotReader* pReader) {
  if (pReader) {
    tdbTbcClose(pReader->pTbc);
    taosMemoryFree(pReader);
  }
  return 0;
}

int32_t metaSnapshotRead(SMetaSnapshotReader* pReader, void** ppData, uint32_t* nDatap) {
  const void* pKey = NULL;
  const void* pData = NULL;
  int32_t     nKey = 0;
  int32_t     nData = 0;
  int32_t     code = 0;

  for (;;) {
    code = tdbTbcGet(pReader->pTbc, &pKey, &nKey, &pData, &nData);
    if (code || ((STbDbKey*)pData)->version > pReader->ever) {
      return TSDB_CODE_VND_READ_END;
    }

    if (((STbDbKey*)pData)->version < pReader->sver) {
      continue;
    }

    break;
  }

  // copy the data
  if (vnodeRealloc(ppData, nData) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  memcpy(*ppData, pData, nData);
  *nDatap = nData;
  return code;
}