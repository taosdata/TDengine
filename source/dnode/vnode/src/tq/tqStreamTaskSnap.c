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
  pReader->tdbTbList = taosArrayInit(4, sizeof(void*));

  STablePair pair1 = {.tbl = pTq->pStreamMeta->pTaskDb, .type = SNAP_DATA_STREAM_TASK};
  taosArrayPush(pReader->tdbTbList, &pair1);

  STablePair pair2 = {.tbl = pTq->pStreamMeta->pCheckpointDb, .type = SNAP_DATA_STREAM_TASK_CHECKPOINT};
  taosArrayPush(pReader->tdbTbList, &pair2);

  pReader->pos = 0;

  STablePair* pPair = taosArrayGet(pReader->tdbTbList, pReader->pos);
  code = tdbTbcOpen(pPair->tbl, &pReader->pCur, NULL);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  code = tdbTbcMoveToFirst(pReader->pCur);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  tqInfo("vgId:%d, vnode stream-task snapshot reader opened", TD_VID(pTq->pVnode));

  *ppReader = pReader;

_err:
  tqError("vgId:%d, vnode stream-task snapshot reader open failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
  return 0;
}

int32_t streamTaskSnapReaderClose(SStreamTaskReader* pReader) {
  int32_t code = 0;
  taosArrayDestroy(pReader->tdbTbList);
  tdbTbcClose(pReader->pCur);
  taosMemoryFree(pReader);

  return code;
}

int32_t streamTaskSnapRead(SStreamTaskReader* pReader, uint8_t** ppData) {
  int32_t     code = 0;
  const void* pKey = NULL;
  const void* pVal = NULL;
  int32_t     kLen = 0;
  int32_t     vLen = 0;
  SDecoder    decoder;
  STqHandle   handle;

  *ppData = NULL;
  int8_t except = 0;

  STablePair* pPair = taosArrayGet(pReader->tdbTbList, pReader->pos);
NextTbl:
  except = 0;
  for (;;) {
    if (tdbTbcGet(pReader->pCur, &pKey, &kLen, &pVal, &vLen)) {
      except = 1;
      break;
    }
    tdbTbcMoveToNext(pReader->pCur);
    break;
  }
  if (except == 1) {
    if (pReader->pos + 1 >= taosArrayGetSize(pReader->tdbTbList)) {
    } else {
      tdbTbcClose(pReader->pCur);

      pReader->pos += 1;
      code = tdbTbcOpen(taosArrayGetP(pReader->tdbTbList, pReader->pos), &pReader->pCur, NULL);
      tdbTbcMoveToFirst(pReader->pCur);

      pPair = taosArrayGet(pReader->tdbTbList, pReader->pos);
      goto NextTbl;
    }
  }
  if (pVal == NULL || vLen == 0) {
    *ppData = NULL;
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

  tqInfo("vgId:%d, vnode stream-task snapshot read data vLen:%d", TD_VID(pReader->pTq->pVnode), vLen);

  return code;
_exit:
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
  TXN*    txn;
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

  if (tdbBegin(pTq->pStreamMeta->db, &pWriter->txn, tdbDefaultMalloc, tdbDefaultFree, NULL, 0) < 0) {
    code = -1;
    taosMemoryFree(pWriter);
    goto _err;
  }

  *ppWriter = pWriter;
  return code;

_err:
  tqError("vgId:%d, stream-task snapshot writer open failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
  return 0;
}

int32_t streamTaskSnapWriterClose(SStreamTaskWriter* pWriter, int8_t rollback) {
  int32_t code = 0;
  STQ*    pTq = pWriter->pTq;

  if (rollback) {
    tdbAbort(pWriter->pTq->pStreamMeta->db, pWriter->txn);
  } else {
    code = tdbCommit(pWriter->pTq->pStreamMeta->db, pWriter->txn);
    if (code) goto _err;
    code = tdbPostCommit(pWriter->pTq->pStreamMeta->db, pWriter->txn);
    if (code) goto _err;
  }

  taosMemoryFree(pWriter);

  // restore from metastore
  // if (tqMetaRestoreHandle(pTq) < 0) {
  //   goto _err;
  // }

  return code;

_err:
  tqError("vgId:%d, tq snapshot writer close failed since %s", TD_VID(pWriter->pTq->pVnode), tstrerror(code));
  return code;
  return 0;
}

int32_t streamTaskSnapWrite(SStreamTaskWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t       code = 0;
  STQ*          pTq = pWriter->pTq;
  STqHandle     handle;
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;
  if (pHdr->type == SNAP_DATA_STREAM_TASK) {
    SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
    if (pTask == NULL) {
      return -1;
    }

    SDecoder decoder;
    tDecoderInit(&decoder, (uint8_t*)pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
    code = tDecodeStreamTask(&decoder, pTask);
    if (code < 0) {
      tDecoderClear(&decoder);
      taosMemoryFree(pTask);
      goto _err;
    }
    tDecoderClear(&decoder);
    // tdbTbInsert(TTB *pTb, const void *pKey, int keyLen, const void *pVal, int valLen, TXN *pTxn)
    if (tdbTbUpsert(pTq->pStreamMeta->pTaskDb, &pTask->id.taskId, sizeof(int32_t),
                    (uint8_t*)pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr), pWriter->txn) < 0) {
      taosMemoryFree(pTask);
      return -1;
    }
    taosMemoryFree(pTask);
  } else if (pHdr->type == SNAP_DATA_STREAM_TASK_CHECKPOINT) {
    // do nothing
  }

  return code;

_err:
  tqError("vgId:%d, stream-task snapshot tq write failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  return code;
}
