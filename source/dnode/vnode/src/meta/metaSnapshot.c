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
  SMetaSnapReader* pReader = NULL;

  // alloc
  pReader = (SMetaSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pMeta = pMeta;
  pReader->sver = sver;
  pReader->ever = ever;

  // impl
  code = tdbTbcOpen(pMeta->pTbDb, &pReader->pTbc, NULL);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  code = tdbTbcMoveTo(pReader->pTbc, &(STbDbKey){.version = sver, .uid = INT64_MIN}, sizeof(STbDbKey), &c);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  metaInfo("vgId:%d, vnode snapshot meta reader opened", TD_VID(pMeta->pVnode));

  *ppReader = pReader;
  return code;

_err:
  metaError("vgId:%d, vnode snapshot meta reader open failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t metaSnapReaderClose(SMetaSnapReader** ppReader) {
  int32_t code = 0;

  tdbTbcClose((*ppReader)->pTbc);
  taosMemoryFree(*ppReader);
  *ppReader = NULL;

  return code;
}

int32_t metaSnapRead(SMetaSnapReader* pReader, uint8_t** ppData) {
  int32_t     code = 0;
  const void* pKey = NULL;
  const void* pData = NULL;
  int32_t     nKey = 0;
  int32_t     nData = 0;
  STbDbKey    key;

  *ppData = NULL;
  for (;;) {
    if (tdbTbcGet(pReader->pTbc, &pKey, &nKey, &pData, &nData)) {
      goto _exit;
    }

    key = ((STbDbKey*)pKey)[0];
    if (key.version > pReader->ever) {
      goto _exit;
    }

    if (key.version < pReader->sver) {
      tdbTbcMoveToNext(pReader->pTbc);
      continue;
    }

    tdbTbcMoveToNext(pReader->pTbc);
    break;
  }

  ASSERT(pData && nData);

  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + nData);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
  pHdr->type = SNAP_DATA_META;
  pHdr->size = nData;
  memcpy(pHdr->data, pData, nData);

  metaInfo("vgId:%d, vnode snapshot meta read data, version:%" PRId64 " uid:%" PRId64 " nData:%d",
           TD_VID(pReader->pMeta->pVnode), key.version, key.uid, nData);

_exit:
  return code;

_err:
  metaError("vgId:%d, vnode snapshot meta read data failed since %s", TD_VID(pReader->pMeta->pVnode), tstrerror(code));
  return code;
}

// SMetaSnapWriter ========================================
struct SMetaSnapWriter {
  SMeta*  pMeta;
  int64_t sver;
  int64_t ever;
};

int32_t metaSnapWriterOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapWriter** ppWriter) {
  int32_t          code = 0;
  SMetaSnapWriter* pWriter;

  // alloc
  pWriter = (SMetaSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pMeta = pMeta;
  pWriter->sver = sver;
  pWriter->ever = ever;

  metaBegin(pMeta, 1);

  *ppWriter = pWriter;
  return code;

_err:
  metaError("vgId:%d, meta snapshot writer open failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t metaSnapWriterClose(SMetaSnapWriter** ppWriter, int8_t rollback) {
  int32_t          code = 0;
  SMetaSnapWriter* pWriter = *ppWriter;

  if (rollback) {
    ASSERT(0);
  } else {
    code = metaCommit(pWriter->pMeta);
    if (code) goto _err;
  }
  taosMemoryFree(pWriter);
  *ppWriter = NULL;

  return code;

_err:
  metaError("vgId:%d, meta snapshot writer close failed since %s", TD_VID(pWriter->pMeta->pVnode), tstrerror(code));
  return code;
}

int32_t metaSnapWrite(SMetaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t    code = 0;
  SMeta*     pMeta = pWriter->pMeta;
  SMetaEntry metaEntry = {0};
  SDecoder*  pDecoder = &(SDecoder){0};

  tDecoderInit(pDecoder, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  metaDecodeEntry(pDecoder, &metaEntry);

  code = metaHandleEntry(pMeta, &metaEntry);
  if (code) goto _err;

  tDecoderClear(pDecoder);
  return code;

_err:
  metaError("vgId:%d, vnode snapshot meta write failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  return code;
}
