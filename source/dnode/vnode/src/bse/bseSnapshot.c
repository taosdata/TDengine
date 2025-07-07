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

#include "bseSnapshot.h"
#include "bseInc.h"
#include "bseTable.h"
#include "bseTableMgt.h"
#include "bseUtil.h"
#include "vnodeInt.h"

static int32_t bseRawFileWriterOpen(SBse *pBse, int64_t sver, int64_t ever, SBseSnapMeta *pMeta,
                                    SBseRawFileWriter **pWriter);
static int32_t bseRawFileWriterDoWrite(SBseRawFileWriter *p, uint8_t *data, int32_t len);
static void    bseRawFileWriterClose(SBseRawFileWriter *p, int8_t rollback);
static void    bseRawFileGenLiveInfo(SBseRawFileWriter *p, SBseLiveFileInfo *pInfo);

static int32_t bseRawFileWriterOpen(SBse *pBse, int64_t sver, int64_t ever, SBseSnapMeta *pMeta,
                                    SBseRawFileWriter **pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  SBseRawFileWriter *p = taosMemoryCalloc(1, sizeof(SBseRawFileWriter));
  if (p == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  char name[TSDB_FILENAME_LEN] = {0};
  char path[TSDB_FILENAME_LEN] = {0};

  SSeqRange *range = &pMeta->range;
  if (pMeta->fileType == BSE_TABLE_SNAP) {
    bseBuildDataName(pMeta->keepDays, name);
    bseBuildFullName(pBse, name, path);
  } else if (pMeta->fileType == BSE_TABLE_META_TYPE) {
    bseBuildMetaName(pMeta->keepDays, name);
    bseBuildFullName(pBse, name, path);
  } else if (pMeta->fileType == BSE_CURRENT_SNAP) {
    bseBuildCurrentName(pBse, path);
  } else {
    return TSDB_CODE_INVALID_MSG;
  }

  p->pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND | TD_FILE_TRUNC);
  if (p->pFile == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  p->fileType = pMeta->fileType;
  p->range = *range;

  p->pBse = pBse;

  *pWriter = p;
_error:
  if (code) {
    if (p != NULL) {
      bseError("vgId:%d failed to open table pWriter at line %d since %s", BSE_GET_VGID((SBse *)pBse), lino,
               tstrerror(code));
      bseRawFileWriterClose(p, 0);
    }
    *pWriter = NULL;
  }

  return code;
}

static int32_t bseRawFileWriterDoWrite(SBseRawFileWriter *p, uint8_t *data, int32_t len) {
  int32_t code = 0;
  int32_t nwrite = taosWriteFile(p->pFile, data, len);
  if (nwrite != len) {
    return terrno;
  }
  p->offset += len;

  return code;
}
static void bseRawFileWriterClose(SBseRawFileWriter *p, int8_t rollback) {
  if (p == NULL) return;

  int32_t code = 0;
  taosCloseFile(&p->pFile);
  if (rollback) {
    bseError("vgId:%d failed to close table pWriter since %s", BSE_GET_VGID((SBse *)p->pBse), tstrerror(code));
  }
  taosMemoryFree(p);

  return;
}

void bseRawFileGenLiveInfo(SBseRawFileWriter *p, SBseLiveFileInfo *pInfo) {
  pInfo->range = p->range;
  pInfo->size = p->offset;
  memcpy(pInfo->name, p->name, sizeof(p->name));
}

static int32_t bseSnapMayOpenNewFile(SBseSnapWriter *pWriter, SBseSnapMeta *pMeta) {
  int32_t code = 0;

  SBse *pBse = pWriter->pBse;

  SBseRawFileWriter *pOld = pWriter->pWriter;

  if (pOld == NULL || (pOld->fileType != pMeta->fileType)) {
    if (pOld != NULL) {
      SBseLiveFileInfo info;
      bseRawFileGenLiveInfo(pOld, &info);

      if (taosArrayPush(pWriter->pFileSet, &info) == NULL) {
        code = terrno;
        bseError("vgId:%d failed to push file info since %s", BSE_GET_VGID((SBse *)pBse), tstrerror(code));
        return code;
      }
      bseRawFileWriterClose(pOld, 0);
      pWriter->pWriter = NULL;
    }

    SBseRawFileWriter *pNew = NULL;
    code = bseRawFileWriterOpen(pBse, 0, 0, pMeta, &pNew);
    if (code) {
      bseError("vgId:%d failed to open table pWriter since %s", BSE_GET_VGID((SBse *)pBse), tstrerror(code));
      return code;
    }
    pWriter->pWriter = pNew;
  }
  return code;
}

int32_t bseSnapWriterOpen(SBse *pBse, int64_t sver, int64_t ever, SBseSnapWriter **pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  SBseSnapWriter *p = taosMemoryCalloc(1, sizeof(SBseSnapWriter));
  if (p == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  p->pFileSet = taosArrayInit(128, sizeof(SBseLiveFileInfo));
  if (p->pFileSet == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  p->pBse = pBse;

  *pWriter = p;
_error:
  if (code) {
    if (p != NULL) {
      bseError("vgId:%d failed to open table pWriter at line %d since %s", BSE_GET_VGID((SBse *)pBse), lino,
               tstrerror(code));
      bseSnapWriterClose(&p, 0);
    }
    *pWriter = NULL;
  }

  return code;
}
int32_t bseSnapWriterWrite(SBseSnapWriter *p, uint8_t *data, int32_t len) {
  int32_t       code;
  int32_t       lino = 0;
  SSnapDataHdr *pHdr = (SSnapDataHdr *)data;
  if (pHdr->size + sizeof(SSnapDataHdr) != len) {
    return TSDB_CODE_INVALID_MSG;
  }
  SBseSnapMeta *pMeta = (SBseSnapMeta *)pHdr->data;

  uint8_t *pBuf = pHdr->data + sizeof(SBseSnapMeta);
  int64_t  tlen = len - sizeof(SSnapDataHdr) - sizeof(SBseSnapMeta);

  code = bseSnapMayOpenNewFile(p, pMeta);
  TSDB_CHECK_CODE(code, lino, _error);

  code = bseRawFileWriterDoWrite(p->pWriter, pBuf, tlen);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code) {
    if (p->pWriter != NULL) {
      bseError("vgId:%d failed to write snapshot data since %s", BSE_GET_VGID((SBse *)p->pBse), tstrerror(code));
      bseRawFileWriterClose(p->pWriter, 0);
    }
    return code;
  }
  return code;
}
int32_t bseSnapWriterClose(SBseSnapWriter **pp, int8_t rollback) {
  int32_t code = 0;

  SBseSnapWriter *p = *pp;
  if (p == NULL) {
    return code;
  }

  taosArrayDestroy(p->pFileSet);
  bseRawFileWriterClose(p->pWriter, 0);
  taosMemoryFree(p);

  return code;
}

int32_t bseSnapReaderOpen(SBse *pBse, int64_t sver, int64_t ever, SBseSnapReader **ppReader) {
  int32_t code = 0;
  int32_t lino = 0;

  SBseSnapReader *p = taosMemoryCalloc(1, sizeof(SBseSnapReader));
  if (p == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  code = bseOpenIter(pBse, (SBseIter **)&p->pIter);
  TSDB_CHECK_CODE(code, lino, _error);

  p->pBse = pBse;
  *ppReader = p;
_error:
  if (code) {
    if (p != NULL) {
      bseError("vgId:%d failed to open table pReader at line %d since %s", BSE_GET_VGID((SBse *)pBse), lino,
               tstrerror(code));
      bseSnapReaderClose(&p);
    }
    *ppReader = NULL;
  }
  return code;
}

int32_t bseSnapReaderRead(SBseSnapReader *p, uint8_t **data) {
  int32_t code = 0;
  int32_t line = 0;
  int32_t size = 0;

  uint8_t *pBuf = NULL;
  int32_t  bufLen = 0;

  if (bseIterIsOver(p->pIter)) {
    *data = NULL;
    return code;
  }

  code = bseIterNext(p->pIter, &pBuf, &bufLen);
  TSDB_CHECK_CODE(code, line, _error);

  *data = taosMemoryCalloc(1, sizeof(SSnapDataHdr) + bufLen);
  if (*data == NULL) {
    TSDB_CHECK_CODE(code = terrno, line, _error);
  }

  SSnapDataHdr *pHdr = (SSnapDataHdr *)(*data);
  pHdr->type = SNAP_DATA_BSE;
  pHdr->size = bufLen;
  uint8_t *tdata = pHdr->data;
  memcpy(tdata, pBuf, bufLen);

_error:
  if (code) {
    if (*data != NULL) {
      taosMemoryFree(*data);
      *data = NULL;
    }
    bseError("vgId:%d failed to read snapshot data at line %d since %s", BSE_GET_VGID((SBse *)p->pBse), line,
             tstrerror(code));
  }
  return code;
}
// test func
int32_t bseSnapReaderRead2(SBseSnapReader *p, uint8_t **data, int32_t *len) {
  int32_t code = 0;
  int32_t line = 0;
  int32_t size = 0;

  uint8_t *pBuf = NULL;
  int32_t  bufLen = 0;

  if (bseIterIsOver(p->pIter)) {
    *data = NULL;
    return code;
  }

  code = bseIterNext(p->pIter, &pBuf, &bufLen);
  TSDB_CHECK_CODE(code, line, _error);

  *data = taosMemoryCalloc(1, sizeof(SSnapDataHdr) + bufLen);
  if (*data == NULL) {
    TSDB_CHECK_CODE(code = terrno, line, _error);
  }

  SSnapDataHdr *pHdr = (SSnapDataHdr *)(*data);
  pHdr->type = SNAP_DATA_BSE;
  pHdr->size = bufLen;
  uint8_t *tdata = pHdr->data;
  memcpy(tdata, pBuf, bufLen);

  *len = sizeof(SSnapDataHdr) + bufLen;

_error:
  if (code) {
    if (*data != NULL) {
      taosMemoryFree(*data);
      *data = NULL;
    }
    bseError("vgId:%d failed to read snapshot data at line %d since %s", BSE_GET_VGID((SBse *)p->pBse), line,
             tstrerror(code));
  }
  return code;
}

int32_t bseSnapReaderClose(SBseSnapReader **p) {
  int32_t code = 0;
  if (p == NULL || *p == NULL) {
    return code;
  }

  SBseSnapReader *pReader = *p;
  bseIterDestroy(pReader->pIter);
  taosMemoryFree(pReader);

  *p = NULL;
  return code;
}

int32_t bseOpenIter(SBse *pBse, SBseIter **ppIter) {
  int32_t code = 0;
  int32_t line = 0;

  SBseIter *pIter = taosMemoryCalloc(1, sizeof(SBseIter));
  if (pIter == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pIter->pBse = pBse;

  SArray *pAliveFile = NULL;
  code = bseGetAliveFileList(pBse, &pAliveFile);
  TSDB_CHECK_CODE(code, line, _error);

  pIter->index = 0;
  pIter->pFileSet = pAliveFile;
  pIter->fileType = BSE_TABLE_SNAP;

  *ppIter = pIter;

_error:
  if (code != 0) {
    bseError("vgId:%d failed to open iter since %s", BSE_GET_VGID(pBse), tstrerror(code));
    taosMemoryFree(pIter);
    taosArrayDestroy(pAliveFile);
    return code;
  }
  return code;
}

int32_t bseIterNext(SBseIter *pIter, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  STableReaderIter *pTableIter = NULL;

  if (pIter->fileType == BSE_TABLE_SNAP) {
    pTableIter = pIter->pTableIter;
    if (pTableIter != NULL && tableReaderIterValid(pTableIter)) {
      code = tableReaderIterNext(pTableIter, pValue, len);
      TSDB_CHECK_CODE(code, lino, _error);

      pTableIter->fileType = pIter->fileType;
      if (!tableReaderIterValid(pTableIter)) {
        // current file is over
        tableReaderIterDestroy(pTableIter);
        pIter->pTableIter = NULL;
      } else {
        return code;
      }
    }

    if (pIter->index >= taosArrayGetSize(pIter->pFileSet)) {
      pIter->fileType = BSE_TABLE_META_SNAP;
      pTableIter->fileType = pIter->fileType;
      pIter->index = 0;
    } else {
      if (pIter->pTableIter != NULL) {
        tableReaderIterDestroy(pIter->pTableIter);
        pIter->pTableIter = NULL;
      }

      SBseLiveFileInfo *pInfo = taosArrayGet(pIter->pFileSet, pIter->index);
      code = tableReaderIterInit(pInfo->retentionTs, BSE_TABLE_DATA_TYPE, &pTableIter, pIter->pBse);
      TSDB_CHECK_CODE(code, lino, _error);

      pTableIter->fileType = BSE_TABLE_SNAP;
      code = tableReaderIterNext(pTableIter, pValue, len);
      TSDB_CHECK_CODE(code, lino, _error);

      pIter->pTableIter = pTableIter;

      pIter->index++;
      return code;
    }
  }

  if (pIter->fileType == BSE_TABLE_META_SNAP) {
    pTableIter = pIter->pTableIter;
    if (pTableIter != NULL && tableReaderIterValid(pTableIter)) {
      code = tableReaderIterNext(pTableIter, pValue, len);
      TSDB_CHECK_CODE(code, lino, _error);

      pTableIter->fileType = pIter->fileType;

      if (!tableReaderIterValid(pTableIter)) {
        tableReaderIterDestroy(pTableIter);
        pIter->pTableIter = NULL;
      } else {
        return code;
      }
    }
    if (pIter->index >= taosArrayGetSize(pIter->pFileSet)) {
      pIter->fileType = BSE_CURRENT_SNAP;
      pTableIter->fileType = pIter->fileType;
    } else {
      if (pIter->pTableIter != NULL) {
        tableReaderIterDestroy(pIter->pTableIter);
        pIter->pTableIter = NULL;
      }

      SBseLiveFileInfo *pInfo = taosArrayGet(pIter->pFileSet, pIter->index);
      code = tableReaderIterInit(pInfo->retentionTs, BSE_TABLE_META_TYPE, &pTableIter, pIter->pBse);
      TSDB_CHECK_CODE(code, lino, _error);

      pTableIter->fileType = BSE_TABLE_META_SNAP;
      code = tableReaderIterNext(pTableIter, pValue, len);
      TSDB_CHECK_CODE(code, lino, _error);

      pIter->pTableIter = pTableIter;
      pIter->index++;
      return code;
    }
  }

  if (pIter->fileType == BSE_CURRENT_SNAP) {
    code = bseReadCurrentSnap(pIter->pBse, pValue, len);
    // do read current
    pIter->fileType = BSE_MAX_SNAP;
    pIter->isOver = 1;
    pTableIter->fileType = pIter->fileType;
  } else if (pIter->fileType == BSE_MAX_SNAP) {
    pIter->isOver = 1;
  }

_error:
  if (code != 0) {
    bseError("vgId:%d failed to get next iter since %s", BSE_GET_VGID(pIter->pBse), tstrerror(code));
  }
  return code;
}

void bseIterDestroy(SBseIter *pIter) {
  if (pIter == NULL) {
    return;
  }

  if (pIter->pTableIter != NULL) {
    tableReaderIterDestroy(pIter->pTableIter);
  }

  taosArrayDestroy(pIter->pFileSet);
  return;
}
int8_t bseIterValid(SBseIter *pIter) {
  if (pIter == NULL) {
    return 0;
  }
  return pIter->isOver == 0;
}
int8_t bseIterIsOver(SBseIter *pIter) {
  if (pIter == NULL) {
    return 1;
  }
  return pIter->isOver == 1;
}