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
#include "tdbInt.h"
#include "tq.h"

// STqSnapReader ========================================
struct STqSnapReader {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
  TBC*    pCur;
};

int32_t tqSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, STqSnapReader** ppReader) {
  int32_t        code = 0;
  STqSnapReader* pReader = NULL;

  // alloc
  pReader = (STqSnapReader*)taosMemoryCalloc(1, sizeof(STqSnapReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pTq = pTq;
  pReader->sver = sver;
  pReader->ever = ever;

  // impl
  code = tdbTbcOpen(pTq->pExecStore, &pReader->pCur, NULL);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  code = tdbTbcMoveToFirst(pReader->pCur);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  tqInfo("vgId:%d, vnode snapshot tq reader opened", TD_VID(pTq->pVnode));

  *ppReader = pReader;
  return code;

_err:
  tqError("vgId:%d, vnode snapshot tq reader open failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t tqSnapReaderClose(STqSnapReader** ppReader) {
  int32_t code = 0;

  tdbTbcClose((*ppReader)->pCur);
  taosMemoryFree(*ppReader);
  *ppReader = NULL;

  return code;
}

int32_t tqSnapRead(STqSnapReader* pReader, uint8_t** ppData) {
  int32_t     code = 0;
  const void* pKey = NULL;
  const void* pVal = NULL;
  int32_t     kLen = 0;
  int32_t     vLen = 0;
  SDecoder    decoder;
  STqHandle   handle;

  *ppData = NULL;
  for (;;) {
    if (tdbTbcGet(pReader->pCur, &pKey, &kLen, &pVal, &vLen)) {
      goto _exit;
    }

    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    tDecodeSTqHandle(&decoder, &handle);
    tDecoderClear(&decoder);

    if (handle.snapshotVer <= pReader->sver && handle.snapshotVer >= pReader->ever) {
      tdbTbcMoveToNext(pReader->pCur);
      break;
    } else {
      tdbTbcMoveToNext(pReader->pCur);
    }
  }

  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + vLen);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
  pHdr->type = SNAP_DATA_TQ_HANDLE;
  pHdr->size = vLen;
  memcpy(pHdr->data, pVal, vLen);

  tqInfo("vgId:%d, vnode snapshot tq read data, version:%" PRId64 " subKey: %s vLen:%d", TD_VID(pReader->pTq->pVnode),
         handle.snapshotVer, handle.subKey, vLen);

_exit:
  return code;

_err:
  tqError("vgId:%d, vnode snapshot tq read data failed since %s", TD_VID(pReader->pTq->pVnode), tstrerror(code));
  return code;
}

// STqSnapWriter ========================================
struct STqSnapWriter {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
  TXN*    txn;
};

int32_t tqSnapWriterOpen(STQ* pTq, int64_t sver, int64_t ever, STqSnapWriter** ppWriter) {
  int32_t        code = 0;
  STqSnapWriter* pWriter;

  // alloc
  pWriter = (STqSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pTq = pTq;
  pWriter->sver = sver;
  pWriter->ever = ever;

  if (tdbBegin(pTq->pMetaDB, &pWriter->txn, tdbDefaultMalloc, tdbDefaultFree, NULL, 0) < 0) {
    code = -1;
    taosMemoryFree(pWriter);
    goto _err;
  }

  *ppWriter = pWriter;
  return code;

_err:
  tqError("vgId:%d, tq snapshot writer open failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tqSnapWriterClose(STqSnapWriter** ppWriter, int8_t rollback) {
  int32_t        code = 0;
  STqSnapWriter* pWriter = *ppWriter;
  STQ*           pTq = pWriter->pTq;

  if (rollback) {
    tdbAbort(pWriter->pTq->pMetaDB, pWriter->txn);
  } else {
    code = tdbCommit(pWriter->pTq->pMetaDB, pWriter->txn);
    if (code) goto _err;
    code = tdbPostCommit(pWriter->pTq->pMetaDB, pWriter->txn);
    if (code) goto _err;
  }

  int vgId = TD_VID(pWriter->pTq->pVnode);

  taosMemoryFree(pWriter);
  *ppWriter = NULL;

  // restore from metastore
  if (tqMetaRestoreHandle(pTq) < 0) {
    goto _err;
  }

  return code;

_err:
  tqError("vgId:%d, tq snapshot writer close failed since %s", vgId, tstrerror(code));
  return code;
}

int32_t tqSnapWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t   code = 0;
  STQ*      pTq = pWriter->pTq;
  SDecoder  decoder = {0};
  SDecoder* pDecoder = &decoder;
  STqHandle handle;

  tDecoderInit(pDecoder, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  code = tDecodeSTqHandle(pDecoder, &handle);
  if (code) goto _err;
  code = tqMetaSaveHandle(pTq, handle.subKey, &handle);
  if (code < 0) goto _err;
  tDecoderClear(pDecoder);

  return code;

_err:
  tDecoderClear(pDecoder);
  tqError("vgId:%d, vnode snapshot tq write failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  return code;
}
