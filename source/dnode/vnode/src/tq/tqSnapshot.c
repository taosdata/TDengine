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
  int8_t  type;
};

int32_t tqSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, int8_t type, STqSnapReader** ppReader) {
  int32_t        code = 0;
  STqSnapReader* pReader = NULL;

  // alloc
  pReader = (STqSnapReader*)taosMemoryCalloc(1, sizeof(STqSnapReader));
  if (pReader == NULL) {
    code = terrno;
    goto _err;
  }
  pReader->pTq = pTq;
  pReader->sver = sver;
  pReader->ever = ever;
  pReader->type = type;

  // impl
  TTB* pTb = NULL;
  if (type == SNAP_DATA_TQ_CHECKINFO) {
    pTb = pTq->pCheckStore;
  } else if (type == SNAP_DATA_TQ_HANDLE) {
    pTb = pTq->pExecStore;
  } else if (type == SNAP_DATA_TQ_OFFSET) {
    pTb = pTq->pOffsetStore;
  } else {
    code = TSDB_CODE_INVALID_MSG;
    goto _err;
  }
  code = tdbTbcOpen(pTb, &pReader->pCur, NULL);
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

void tqSnapReaderClose(STqSnapReader** ppReader) {
  tdbTbcClose((*ppReader)->pCur);
  taosMemoryFree(*ppReader);
  *ppReader = NULL;
}

int32_t tqSnapRead(STqSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  void*   pKey = NULL;
  void*   pVal = NULL;
  int32_t kLen = 0;
  int32_t vLen = 0;

  if (tdbTbcNext(pReader->pCur, &pKey, &kLen, &pVal, &vLen)) {
    goto _exit;
  }

  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + vLen);
  if (*ppData == NULL) {
    code = TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
    goto _err;
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
  pHdr->type = pReader->type;
  pHdr->size = vLen;
  (void)memcpy(pHdr->data, pVal, vLen);

_exit:
  tdbFree(pKey);
  tdbFree(pVal);
  tqInfo("vgId:%d, vnode snapshot tq read data, vLen:%d", TD_VID(pReader->pTq->pVnode), vLen);
  return code;

_err:
  tdbFree(pKey);
  tdbFree(pVal);
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
  STqSnapWriter* pWriter = NULL;

  // alloc
  pWriter = (STqSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
    ;
    goto _err;
  }
  pWriter->pTq = pTq;
  pWriter->sver = sver;
  pWriter->ever = ever;

  code = tdbBegin(pTq->pMetaDB, &pWriter->txn, tdbDefaultMalloc, tdbDefaultFree, NULL, 0);
  if (code < 0) {
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

  taosMemoryFree(pWriter);
  *ppWriter = NULL;

  return code;

_err:
  tqError("vgId:%d, tq snapshot writer close failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  return code;
}

int32_t tqSnapHandleWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t   code = 0;
  STQ*      pTq = pWriter->pTq;
  SDecoder  decoder = {0};
  SDecoder* pDecoder = &decoder;
  STqHandle handle = {0};

  tDecoderInit(pDecoder, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  code = tDecodeSTqHandle(pDecoder, &handle);
  if (code) goto end;
  taosWLockLatch(&pTq->lock);
  code = tqMetaSaveInfo(pTq, pTq->pExecStore, handle.subKey, (int)strlen(handle.subKey), pData + sizeof(SSnapDataHdr),
                        nData - sizeof(SSnapDataHdr));
  taosWUnLockLatch(&pTq->lock);

end:
  tDecoderClear(pDecoder);
  tqDestroyTqHandle(&handle);
  tqInfo("vgId:%d, vnode snapshot tq write result:%d", TD_VID(pTq->pVnode), code);
  return code;
}

int32_t tqSnapCheckInfoWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t      code = 0;
  STQ*         pTq = pWriter->pTq;
  STqCheckInfo info = {0};
  code = tqMetaDecodeCheckInfo(&info, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  if (code != 0) {
    goto _err;
  }

  code = tqMetaSaveInfo(pTq, pTq->pCheckStore, &info.topic, strlen(info.topic), pData + sizeof(SSnapDataHdr),
                        nData - sizeof(SSnapDataHdr));
  tDeleteSTqCheckInfo(&info);
  if (code) goto _err;

  return code;

_err:
  tqError("vgId:%d, vnode check info tq write failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  return code;
}

int32_t tqSnapOffsetWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;
  STQ*    pTq = pWriter->pTq;

  STqOffset info = {0};
  code = tqMetaDecodeOffsetInfo(&info, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  if (code != 0) {
    goto _err;
  }

  code = tqMetaSaveInfo(pTq, pTq->pOffsetStore, info.subKey, strlen(info.subKey), pData + sizeof(SSnapDataHdr),
                        nData - sizeof(SSnapDataHdr));
  tDeleteSTqOffset(&info);
  if (code) goto _err;

  return code;

_err:
  tqError("vgId:%d, vnode check info tq write failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  return code;
}
