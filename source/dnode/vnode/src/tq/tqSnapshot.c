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
  int      code  = TSDB_CODE_SUCCESS;
  int32_t  lino  = 0;
  STqSnapReader* pReader = NULL;
  TSDB_CHECK_NULL(pTq, code, lino, end, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(ppReader, code, lino, end, TSDB_CODE_INVALID_MSG);

  // alloc
  pReader = (STqSnapReader*)taosMemoryCalloc(1, sizeof(STqSnapReader));
  TSDB_CHECK_NULL(pReader, code, lino, end, terrno);

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
    goto end;
  }
  code = tdbTbcOpen(pTb, &pReader->pCur, NULL);
  TSDB_CHECK_CODE(code, lino, end);
  code = tdbTbcMoveToFirst(pReader->pCur);
  TSDB_CHECK_CODE(code, lino, end);
  tqInfo("vgId:%d, vnode tq snapshot reader open success", TD_VID(pTq->pVnode));
  *ppReader = pReader;

end:
  if (code != 0){
    tqError("%s failed at %d, vnode tq snapshot reader open failed since %s", __func__, lino, tstrerror(code));
    taosMemoryFreeClear(pReader);
  }

  return code;
}

void tqSnapReaderClose(STqSnapReader** ppReader) {
  if (ppReader == NULL || *ppReader == NULL) {
    return;
  }
  tdbTbcClose((*ppReader)->pCur);
  taosMemoryFreeClear(*ppReader);
}

int32_t tqSnapRead(STqSnapReader* pReader, uint8_t** ppData) {
  int     code  = TSDB_CODE_SUCCESS;
  int32_t lino  = 0;
  void*   pKey  = NULL;
  void*   pVal  = NULL;
  int32_t kLen  = 0;
  int32_t vLen  = 0;
  TSDB_CHECK_NULL(pReader, code, lino, end, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(ppData, code, lino, end, TSDB_CODE_INVALID_MSG);

  code = tdbTbcNext(pReader->pCur, &pKey, &kLen, &pVal, &vLen);
  TSDB_CHECK_CONDITION(code == 0, code, lino, end, TDB_CODE_SUCCESS);

  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + vLen);
  TSDB_CHECK_NULL(*ppData, code, lino, end, terrno);

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
  pHdr->type = pReader->type;
  pHdr->size = vLen;
  (void)memcpy(pHdr->data, pVal, vLen);
  tqInfo("vgId:%d, vnode tq snapshot read data, vLen:%d", TD_VID(pReader->pTq->pVnode), vLen);

end:
  if (code != 0) {
    tqError("%s failed at %d, vnode tq snapshot read data failed since %s", __func__, lino, tstrerror(code));
  }
  tdbFree(pKey);
  tdbFree(pVal);
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
  int     code  = TSDB_CODE_SUCCESS;
  int32_t lino  = 0;
  STqSnapWriter* pWriter = NULL;

  TSDB_CHECK_NULL(pTq, code, lino, end, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(ppWriter, code, lino, end, TSDB_CODE_INVALID_MSG);

  // alloc
  pWriter = (STqSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  TSDB_CHECK_NULL(pWriter, code, lino, end, terrno);
  pWriter->pTq = pTq;
  pWriter->sver = sver;
  pWriter->ever = ever;

  code = tdbBegin(pTq->pMetaDB, &pWriter->txn, tdbDefaultMalloc, tdbDefaultFree, NULL, 0);
  TSDB_CHECK_CODE(code, lino, end);
  tqInfo("vgId:%d, tq snapshot writer opene success", TD_VID(pTq->pVnode));
  *ppWriter = pWriter;

end:
  if (code != 0){
    tqError("%s failed at %d tq snapshot writer open failed since %s", __func__, lino, tstrerror(code));
    taosMemoryFreeClear(pWriter);
  }
  return code;
}

int32_t tqSnapWriterClose(STqSnapWriter** ppWriter, int8_t rollback) {
  int     code  = TSDB_CODE_SUCCESS;
  int32_t lino  = 0;
  STqSnapWriter* pWriter = NULL;

  TSDB_CHECK_NULL(ppWriter, code, lino, end, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(*ppWriter, code, lino, end, TSDB_CODE_INVALID_MSG);
  pWriter = *ppWriter;

  if (rollback) {
    tdbAbort(pWriter->pTq->pMetaDB, pWriter->txn);
  } else {
    code = tdbCommit(pWriter->pTq->pMetaDB, pWriter->txn);
    TSDB_CHECK_CODE(code, lino, end);

    code = tdbPostCommit(pWriter->pTq->pMetaDB, pWriter->txn);
    TSDB_CHECK_CODE(code, lino, end);
  }
  tqInfo("vgId:%d, tq snapshot writer close success", TD_VID(pWriter->pTq->pVnode));
  taosMemoryFreeClear(*ppWriter);

end:
  if (code != 0){
    tqError("%s failed at %d, tq snapshot writer close failed since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tqWriteCheck(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData){
  int       code  = TSDB_CODE_SUCCESS;
  int32_t   lino  = 0;
  TSDB_CHECK_NULL(pWriter, code, lino, end, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_NULL(pData, code, lino, end, TSDB_CODE_INVALID_MSG);
  TSDB_CHECK_CONDITION(nData >= sizeof(SSnapDataHdr), code, lino, end, TSDB_CODE_INVALID_MSG);
end:
  if (code != 0){
    tqError("%s failed at %d failed since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
int32_t tqSnapHandleWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int       code  = TSDB_CODE_SUCCESS;
  int32_t   lino  = 0;
  SDecoder  decoder = {0};
  SDecoder* pDecoder = &decoder;
  STqHandle handle = {0};
  code = tqWriteCheck(pWriter, pData, nData);
  TSDB_CHECK_CODE(code, lino, end);

  STQ*      pTq = pWriter->pTq;
  tDecoderInit(pDecoder, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  code = tDecodeSTqHandle(pDecoder, &handle);
  TSDB_CHECK_CODE(code, lino, end);

  taosWLockLatch(&pTq->lock);
  code = tqMetaSaveInfo(pTq, pTq->pExecStore, handle.subKey, strlen(handle.subKey), pData + sizeof(SSnapDataHdr),
                        nData - sizeof(SSnapDataHdr));
  taosWUnLockLatch(&pTq->lock);
  TSDB_CHECK_CODE(code, lino, end);
  tqInfo("vgId:%d, vnode tq snapshot write success", TD_VID(pTq->pVnode));

end:
  tDecoderClear(pDecoder);
  tqDestroyTqHandle(&handle);
  if (code != 0){
    tqError("%s failed at %d, vnode tq snapshot write failed since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tqSnapCheckInfoWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int       code  = TSDB_CODE_SUCCESS;
  int32_t   lino  = 0;

  code = tqWriteCheck(pWriter, pData, nData);
  TSDB_CHECK_CODE(code, lino, end);

  STQ*         pTq = pWriter->pTq;
  STqCheckInfo info = {0};
  code = tqMetaDecodeCheckInfo(&info, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  TSDB_CHECK_CODE(code, lino, end);

  code = tqMetaSaveInfo(pTq, pTq->pCheckStore, &info.topic, strlen(info.topic), pData + sizeof(SSnapDataHdr),
                        nData - sizeof(SSnapDataHdr));
  tDeleteSTqCheckInfo(&info);
  TSDB_CHECK_CODE(code, lino, end);
  tqInfo("vgId:%d, vnode tq check info  write success", TD_VID(pTq->pVnode));

end:
  if (code != 0){
    tqError("%s failed at %d, vnode tq check info write failed since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tqSnapOffsetWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int       code  = TSDB_CODE_SUCCESS;
  int32_t   lino  = 0;
  code = tqWriteCheck(pWriter, pData, nData);
  TSDB_CHECK_CODE(code, lino, end);

  STQ*    pTq = pWriter->pTq;
  STqOffset info = {0};
  code = tqMetaDecodeOffsetInfo(&info, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  TSDB_CHECK_CODE(code, lino, end);

  code = tqMetaSaveInfo(pTq, pTq->pOffsetStore, info.subKey, strlen(info.subKey), pData + sizeof(SSnapDataHdr),
                        nData - sizeof(SSnapDataHdr));
  tDeleteSTqOffset(&info);
  TSDB_CHECK_CODE(code, lino, end);
  tqInfo("vgId:%d, vnode tq offset write success", TD_VID(pTq->pVnode));

end:
  if (code != 0){
    tqError("%s failed at %d, vnode tq offset write failed since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
