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

typedef struct {
  int8_t type;
  TTB*   tbl;
} STablePair;
struct SStreamTaskReader {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
  TBC*    pCur;
  SArray* tdbTbList;
  int8_t  pos;
};

int32_t streamTaskSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamTaskReader** ppReader) {
  int32_t            code = 0;
  SStreamTaskReader* pReader = NULL;

  // alloc
  pReader = (SStreamTaskReader*)taosMemoryCalloc(1, sizeof(SStreamTaskReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pTq = pTq;
  pReader->sver = sver;
  pReader->ever = ever;
  pReader->tdbTbList = taosArrayInit(4, sizeof(STablePair));

  STablePair pair1 = {.tbl = pTq->pStreamMeta->pTaskDb, .type = SNAP_DATA_STREAM_TASK};
  taosArrayPush(pReader->tdbTbList, &pair1);

  STablePair pair2 = {.tbl = pTq->pStreamMeta->pCheckpointDb, .type = SNAP_DATA_STREAM_TASK_CHECKPOINT};
  taosArrayPush(pReader->tdbTbList, &pair2);

  pReader->pos = 0;

  STablePair* pPair = taosArrayGet(pReader->tdbTbList, pReader->pos);
  code = tdbTbcOpen(pPair->tbl, &pReader->pCur, NULL);
  if (code) {
    tqInfo("vgId:%d, vnode stream-task snapshot reader failed to open, reason: %s", TD_VID(pTq->pVnode),
           tstrerror(code));
    taosMemoryFree(pReader);
    goto _err;
  }

  code = tdbTbcMoveToFirst(pReader->pCur);
  if (code) {
    tqInfo("vgId:%d, vnode stream-task snapshot reader failed to iterate, reason: %s", TD_VID(pTq->pVnode),
           tstrerror(code));
    taosMemoryFree(pReader);
    goto _err;
  }

  tqDebug("vgId:%d, vnode stream-task snapshot reader opened", TD_VID(pTq->pVnode));

  *ppReader = pReader;
  return code;

_err:
  tqError("vgId:%d, vnode stream-task snapshot reader open failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t streamTaskSnapReaderClose(SStreamTaskReader* pReader) {
  int32_t code = 0;
  tqInfo("vgId:%d, vnode stream-task snapshot reader closed", TD_VID(pReader->pTq->pVnode));
  taosArrayDestroy(pReader->tdbTbList);
  tdbTbcClose(pReader->pCur);
  taosMemoryFree(pReader);
  return code;
}

int32_t streamTaskSnapRead(SStreamTaskReader* pReader, uint8_t** ppData) {
  int32_t     code = 0;
  const void* pKey = NULL;
  void*       pVal = NULL;
  int32_t     kLen = 0;
  int32_t     vLen = 0;
  SDecoder    decoder;
  STqHandle   handle;

  *ppData = NULL;
  int8_t except = 0;
  tqDebug("vgId:%d, vnode stream-task snapshot start read data", TD_VID(pReader->pTq->pVnode));

  STablePair* pPair = taosArrayGet(pReader->tdbTbList, pReader->pos);
NextTbl:
  except = 0;
  for (;;) {
    const void* tVal = NULL;
    int32_t     tLen = 0;
    if (tdbTbcGet(pReader->pCur, &pKey, &kLen, &tVal, &tLen)) {
      except = 1;
      break;
    } else {
      pVal = taosMemoryCalloc(1, tLen);
      memcpy(pVal, tVal, tLen);
      vLen = tLen;
    }
    tdbTbcMoveToNext(pReader->pCur);
    break;
  }
  if (except == 1) {
    if (pReader->pos + 1 < taosArrayGetSize(pReader->tdbTbList)) {
      tdbTbcClose(pReader->pCur);

      pReader->pos += 1;
      pPair = taosArrayGet(pReader->tdbTbList, pReader->pos);
      code = tdbTbcOpen(pPair->tbl, &pReader->pCur, NULL);
      tdbTbcMoveToFirst(pReader->pCur);

      goto NextTbl;
    }
  }
  if (pVal == NULL || vLen == 0) {
    *ppData = NULL;
    tqDebug("vgId:%d, vnode stream-task snapshot finished read data", TD_VID(pReader->pTq->pVnode));
    return code;
  }
  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + vLen);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
  pHdr->type = pPair->type;
  pHdr->size = vLen;
  memcpy(pHdr->data, pVal, vLen);
  taosMemoryFree(pVal);

  tqDebug("vgId:%d, vnode stream-task snapshot read data vLen:%d", TD_VID(pReader->pTq->pVnode), vLen);

  return code;
_err:
  tqError("vgId:%d, vnode stream-task snapshot read data failed since %s", TD_VID(pReader->pTq->pVnode),
          tstrerror(code));
  return code;
}

// STqSnapWriter ========================================
struct SStreamTaskWriter {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
};

int32_t streamTaskSnapWriterOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamTaskWriter** ppWriter) {
  int32_t            code = 0;
  SStreamTaskWriter* pWriter;

  // alloc
  pWriter = (SStreamTaskWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pTq = pTq;
  pWriter->sver = sver;
  pWriter->ever = ever;

  *ppWriter = pWriter;
  tqDebug("vgId:%d, vnode stream-task snapshot writer opened", TD_VID(pTq->pVnode));
  return code;

_err:
  tqError("vgId:%d, vnode stream-task snapshot writer failed to write since %s", TD_VID(pTq->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
  return 0;
}

int32_t streamTaskSnapWriterClose(SStreamTaskWriter* pWriter, int8_t rollback) {
  int32_t code = 0;
  STQ*    pTq = pWriter->pTq;

  streamMetaWLock(pTq->pStreamMeta);
  tqDebug("vgId:%d, vnode stream-task snapshot writer closed", TD_VID(pTq->pVnode));
  if (rollback) {
    tdbAbort(pTq->pStreamMeta->db, pTq->pStreamMeta->txn);
  } else {
    code = tdbCommit(pTq->pStreamMeta->db, pTq->pStreamMeta->txn);
    if (code) goto _err;
    code = tdbPostCommit(pTq->pStreamMeta->db, pTq->pStreamMeta->txn);
    if (code) goto _err;
  }

  if (tdbBegin(pTq->pStreamMeta->db, &pTq->pStreamMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL, 0) < 0) {
    code = -1;
    taosMemoryFree(pWriter);
    goto _err;
  }
  streamMetaWUnLock(pTq->pStreamMeta);
  taosMemoryFree(pWriter);
  return code;

_err:
  tqError("vgId:%d, vnode stream-task snapshot writer failed to close since %s", TD_VID(pWriter->pTq->pVnode),
          tstrerror(code));
  streamMetaWUnLock(pTq->pStreamMeta);
  return code;
}

int32_t streamTaskSnapWrite(SStreamTaskWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t       code = 0;
  STQ*          pTq = pWriter->pTq;
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;
  if (pHdr->type == SNAP_DATA_STREAM_TASK) {
    STaskId taskId = {0};

    SDecoder decoder;
    tDecoderInit(&decoder, (uint8_t*)pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
    code = tDecodeStreamTaskId(&decoder, &taskId);
    if (code < 0) {
      tDecoderClear(&decoder);
      goto _err;
    }
    tDecoderClear(&decoder);

    int64_t key[2] = {taskId.streamId, taskId.taskId};
    streamMetaWLock(pTq->pStreamMeta);
    if (tdbTbUpsert(pTq->pStreamMeta->pTaskDb, key, sizeof(int64_t) << 1, (uint8_t*)pData + sizeof(SSnapDataHdr),
                    nData - sizeof(SSnapDataHdr), pTq->pStreamMeta->txn) < 0) {
      streamMetaWUnLock(pTq->pStreamMeta);
      return -1;
    }
    streamMetaWUnLock(pTq->pStreamMeta);
  } else if (pHdr->type == SNAP_DATA_STREAM_TASK_CHECKPOINT) {
    // do nothing
  }
  tqDebug("vgId:%d, vnode stream-task snapshot write", TD_VID(pTq->pVnode));

  return code;

_err:
  tqError("vgId:%d, vnode stream-task snapshot failed to write since %s", TD_VID(pTq->pVnode), tstrerror(code));
  return code;
}
