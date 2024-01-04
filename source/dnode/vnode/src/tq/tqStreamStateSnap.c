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
#include "streamSnapshot.h"
#include "tdbInt.h"
#include "tq.h"

// STqSnapReader ========================================
struct SStreamStateReader {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
  TBC*    pCur;

  SStreamSnapReader* pReaderImpl;
  int32_t            complete;  // open reader or not
};

int32_t streamStateSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamStateReader** ppReader) {
  int32_t             code = 0;
  SStreamStateReader* pReader = NULL;

  char tdir[TSDB_FILENAME_LEN * 2] = {0};

  // alloc
  pReader = (SStreamStateReader*)taosMemoryCalloc(1, sizeof(SStreamStateReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SStreamMeta* meta = pTq->pStreamMeta;
  pReader->pTq = pTq;
  pReader->sver = sver;
  pReader->ever = ever;

  int64_t chkpId = meta ? meta->chkpId : 0;

  SStreamSnapReader* pSnapReader = NULL;

  if (streamSnapReaderOpen(meta, sver, chkpId, pTq->path, &pSnapReader) == 0) {
    pReader->complete = 1;
  } else {
    code = -1;
    taosMemoryFree(pReader);
    goto _err;
  }
  pReader->pReaderImpl = pSnapReader;

  tqDebug("vgId:%d, vnode %s snapshot reader opened", TD_VID(pTq->pVnode), STREAM_STATE_TRANSFER);

  *ppReader = pReader;
  return code;

_err:
  tqError("vgId:%d, vnode %s snapshot reader failed to open since %s", TD_VID(pTq->pVnode), STREAM_STATE_TRANSFER,
          tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t streamStateSnapReaderClose(SStreamStateReader* pReader) {
  int32_t code = 0;
  tqDebug("vgId:%d, vnode %s snapshot reader closed", TD_VID(pReader->pTq->pVnode), STREAM_STATE_TRANSFER);
  streamSnapReaderClose(pReader->pReaderImpl);
  taosMemoryFree(pReader);
  return code;
}

int32_t streamStateSnapRead(SStreamStateReader* pReader, uint8_t** ppData) {
  tqDebug("vgId:%d, vnode %s snapshot read data", TD_VID(pReader->pTq->pVnode), STREAM_STATE_TRANSFER);

  int32_t code = 0;
  if (pReader->complete == 0) {
    return 0;
  }

  uint8_t* rowData = NULL;
  int64_t  len;
  code = streamSnapRead(pReader->pReaderImpl, &rowData, &len);
  if (code != 0 || rowData == NULL || len == 0) {
    return code;
  }
  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + len);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  // refactor later, avoid mem/free freq
  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
  pHdr->type = SNAP_DATA_STREAM_STATE_BACKEND;
  pHdr->size = len;
  memcpy(pHdr->data, rowData, len);
  taosMemoryFree(rowData);
  tqDebug("vgId:%d, vnode stream-state snapshot read data success", TD_VID(pReader->pTq->pVnode));
  return code;

_err:
  tqError("vgId:%d, vnode stream-state snapshot failed to read since %s", TD_VID(pReader->pTq->pVnode),
          tstrerror(code));
  return code;
}

// STqSnapWriter ========================================
struct SStreamStateWriter {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
  TXN*    txn;

  SStreamSnapWriter* pWriterImpl;
};

int32_t streamStateSnapWriterOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamStateWriter** ppWriter) {
  int32_t             code = 0;
  SStreamStateWriter* pWriter;

  char tdir[TSDB_FILENAME_LEN * 2] = {0};
  // alloc
  pWriter = (SStreamStateWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pTq = pTq;
  pWriter->sver = sver;
  pWriter->ever = ever;

  sprintf(tdir, "%s%s%s", pTq->path, TD_DIRSEP, VNODE_TQ_STREAM);
  taosMkDir(tdir);

  SStreamSnapWriter* pSnapWriter = NULL;
  if (streamSnapWriterOpen(pTq, sver, ever, tdir, &pSnapWriter) < 0) {
    goto _err;
  }

  tqDebug("vgId:%d, vnode %s snapshot writer opened, path:%s", TD_VID(pTq->pVnode), STREAM_STATE_TRANSFER, tdir);
  pWriter->pWriterImpl = pSnapWriter;

  *ppWriter = pWriter;
  return code;
_err:
  tqError("vgId:%d, vnode %s snapshot writer failed to open since %s", TD_VID(pTq->pVnode), STREAM_STATE_TRANSFER,
          tstrerror(code));
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
  return -1;
}

int32_t streamStateSnapWriterClose(SStreamStateWriter* pWriter, int8_t rollback) {
  int32_t code = 0;
  tqDebug("vgId:%d, vnode %s snapshot writer closed", TD_VID(pWriter->pTq->pVnode), STREAM_STATE_TRANSFER);
  code = streamSnapWriterClose(pWriter->pWriterImpl, rollback);

  return code;
}

int32_t streamStateSnapWrite(SStreamStateWriter* pWriter, uint8_t* pData, uint32_t nData) {
  tqDebug("vgId:%d, vnode %s snapshot write data", TD_VID(pWriter->pTq->pVnode), STREAM_STATE_TRANSFER);
  return streamSnapWrite(pWriter->pWriterImpl, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
}
int32_t streamStateRebuildFromSnap(SStreamStateWriter* pWriter, int64_t chkpId) {
  tqDebug("vgId:%d, vnode %s  start to rebuild stream-state", TD_VID(pWriter->pTq->pVnode), STREAM_STATE_TRANSFER);
  int32_t code = streamStateLoadTasks(pWriter);
  tqDebug("vgId:%d, vnode %s  succ to rebuild stream-state", TD_VID(pWriter->pTq->pVnode), STREAM_STATE_TRANSFER);
  taosMemoryFree(pWriter);
  return code;
}

int32_t streamStateLoadTasks(SStreamStateWriter* pWriter) {
  return streamMetaLoadAllTasks(pWriter->pTq->pStreamMeta);
}
