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

// SMetaSnapReader ========================================
struct SMetaSnapReader {
  SMeta*  pMeta;
  int64_t sver;
  int64_t ever;
  TBC*    pTbc;
};

int32_t metaSnapReaderOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapReader** ppReader) {
  int32_t          code = 0;
  int32_t          c = 0;
  SMetaSnapReader* pMetaSnapReader = NULL;

  // alloc
  pMetaSnapReader = (SMetaSnapReader*)taosMemoryCalloc(1, sizeof(*pMetaSnapReader));
  if (pMetaSnapReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pMetaSnapReader->pMeta = pMeta;
  pMetaSnapReader->sver = sver;
  pMetaSnapReader->ever = ever;

  // impl
  code = tdbTbcOpen(pMeta->pTbDb, &pMetaSnapReader->pTbc, NULL);
  if (code) {
    goto _err;
  }

  code = tdbTbcMoveTo(pMetaSnapReader->pTbc, &(STbDbKey){.version = sver, .uid = INT64_MIN}, sizeof(STbDbKey), &c);
  if (code) {
    goto _err;
  }

  *ppReader = pMetaSnapReader;
  return code;

_err:
  metaError("vgId:%d meta snap reader open failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t metaSnapReaderClose(SMetaSnapReader** ppReader) {
  tdbTbcClose((*ppReader)->pTbc);
  taosMemoryFree(*ppReader);
  *ppReader = NULL;
  return 0;
}

int32_t metaSnapRead(SMetaSnapReader* pReader, uint8_t** ppData) {
  const void* pKey = NULL;
  const void* pData = NULL;
  int32_t     nKey = 0;
  int32_t     nData = 0;
  int32_t     code = 0;

  for (;;) {
    code = tdbTbcGet(pReader->pTbc, &pKey, &nKey, &pData, &nData);
    if (code || ((STbDbKey*)pData)->version > pReader->ever) {
      code = TSDB_CODE_VND_READ_END;
      goto _exit;
    }

    if (((STbDbKey*)pData)->version < pReader->sver) {
      tdbTbcMoveToNext(pReader->pTbc);
      continue;
    }

    tdbTbcMoveToNext(pReader->pTbc);
    break;
  }

  // copy the data
  if (tRealloc(ppData, sizeof(SSnapDataHdr) + nData) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }
  ((SSnapDataHdr*)(*ppData))->type = 0;  // TODO: use macro
  ((SSnapDataHdr*)(*ppData))->size = nData;
  memcpy(((SSnapDataHdr*)(*ppData))->data, pData, nData);

_exit:
  return code;
}

// SMetaSnapWriter ========================================