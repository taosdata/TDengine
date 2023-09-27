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

// STqOffsetReader ========================================
struct STqOffsetReader {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
  int8_t  readEnd;
};

int32_t tqOffsetReaderOpen(STQ* pTq, int64_t sver, int64_t ever, STqOffsetReader** ppReader) {
  STqOffsetReader* pReader = NULL;

  pReader = taosMemoryCalloc(1, sizeof(STqOffsetReader));
  if (pReader == NULL) {
    *ppReader = NULL;
    return -1;
  }
  pReader->pTq = pTq;
  pReader->sver = sver;
  pReader->ever = ever;

  tqInfo("vgId:%d, vnode snapshot tq offset reader opened", TD_VID(pTq->pVnode));

  *ppReader = pReader;
  return 0;
}

int32_t tqOffsetReaderClose(STqOffsetReader** ppReader) {
  taosMemoryFree(*ppReader);
  *ppReader = NULL;
  return 0;
}

int32_t tqOffsetSnapRead(STqOffsetReader* pReader, uint8_t** ppData) {
  if (pReader->readEnd != 0) return 0;

  char*     fname = tqOffsetBuildFName(pReader->pTq->path, 0);
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ);
  if (pFile == NULL) {
    taosMemoryFree(fname);
    return 0;
  }

  int64_t sz = 0;
  if (taosStatFile(fname, &sz, NULL, NULL) < 0) {
    taosCloseFile(&pFile);
    taosMemoryFree(fname);
    return -1;
  }
  taosMemoryFree(fname);

  SSnapDataHdr* buf = taosMemoryCalloc(1, sz + sizeof(SSnapDataHdr));
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosCloseFile(&pFile);
    return terrno;
  }
  void*   abuf = POINTER_SHIFT(buf, sizeof(SSnapDataHdr));
  int64_t contLen = taosReadFile(pFile, abuf, sz);
  if (contLen != sz) {
    taosCloseFile(&pFile);
    taosMemoryFree(buf);
    return -1;
  }
  buf->size = sz;
  buf->type = SNAP_DATA_TQ_OFFSET;
  *ppData = (uint8_t*)buf;

  pReader->readEnd = 1;
  taosCloseFile(&pFile);
  return 0;
}

// STqOffseWriter ========================================
struct STqOffsetWriter {
  STQ*    pTq;
  int64_t sver;
  int64_t ever;
  int32_t tmpFileVer;
  char*   fname;
};

int32_t tqOffsetWriterOpen(STQ* pTq, int64_t sver, int64_t ever, STqOffsetWriter** ppWriter) {
  int32_t          code = 0;
  STqOffsetWriter* pWriter;

  pWriter = (STqOffsetWriter*)taosMemoryCalloc(1, sizeof(STqOffsetWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pTq = pTq;
  pWriter->sver = sver;
  pWriter->ever = ever;

  *ppWriter = pWriter;
  return code;

_err:
  tqError("vgId:%d, tq snapshot writer open failed since %s", TD_VID(pTq->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tqOffsetWriterClose(STqOffsetWriter** ppWriter, int8_t rollback) {
  STqOffsetWriter* pWriter = *ppWriter;
  STQ*             pTq = pWriter->pTq;
  char*            fname = tqOffsetBuildFName(pTq->path, 0);

  if (rollback) {
    if (taosRemoveFile(pWriter->fname) < 0) {
      taosMemoryFree(fname);
      return -1;
    }
  } else {
    if (taosRenameFile(pWriter->fname, fname) < 0) {
      taosMemoryFree(fname);
      return -1;
    }
    if (tqOffsetRestoreFromFile(pTq->pOffsetStore, fname) < 0) {
      taosMemoryFree(fname);
      return -1;
    }
  }
  taosMemoryFree(fname);
  taosMemoryFree(pWriter->fname);
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
  return 0;
}

int32_t tqOffsetSnapWrite(STqOffsetWriter* pWriter, uint8_t* pData, uint32_t nData) {
  STQ* pTq = pWriter->pTq;
  pWriter->tmpFileVer = 1;
  pWriter->fname = tqOffsetBuildFName(pTq->path, pWriter->tmpFileVer);
  TdFilePtr     pFile = taosOpenFile(pWriter->fname, TD_FILE_CREATE | TD_FILE_WRITE);
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;
  int64_t       size = pHdr->size;
  if (pFile) {
    int64_t contLen = taosWriteFile(pFile, pHdr->data, size);
    if (contLen != size) {
      taosCloseFile(&pFile);
      return -1;
    }
    taosCloseFile(&pFile);
  } else {
    return -1;
  }
  return 0;
}
