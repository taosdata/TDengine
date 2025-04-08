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

#include "bseInc.h"
#include "bseTable.h"
#include "bseTableMgt.h"
#include "bseUtil.h"
#include "vnodeInt.h"

int32_t bseSnapWriterOpen(SBse *pBse, int64_t sver, int64_t ever, SBseSnapWriter **pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  SBseSnapWriter *p = taosMemoryCalloc(1, sizeof(SBseSnapWriter));
  if (p == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  p->pBse = pBse;

  *pWriter = p;
_error:
  if (code) {
    if (p != NULL) {
      bseError("vgId:%d failed to open table pWriter at line %d since %s at line %d", BSE_GET_VGID((SBse *)pBse), lino,
               tstrerror(code));
      bseSnapWriterClose(&p, 0);
    }
    *pWriter = NULL;
  }

  return code;
}
int32_t bseSnapWriterWrite(SBseSnapWriter *p, uint8_t *data, int32_t len) {
  int32_t code;
  return code;
}
int32_t bseSnapWriterClose(SBseSnapWriter **pp, int8_t rollback) {
  int32_t code = 0;

  SBseSnapWriter *p = *pp;
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
      bseError("vgId:%d failed to open table pReader at line %d since %s at line %d", BSE_GET_VGID((SBse *)pBse), lino,
               tstrerror(code));
      bseSnapReaderClose(&p);
    }
    *ppReader = NULL;
  }
  return code;
}
typedef struct {
  SBlockWrapper buf[1];
} SBseSnapReadBuf;
int32_t bseSnapReaderRead(SBseSnapReader *p, uint8_t **data, int32_t *len) {
  int32_t code = 0;
  int32_t line = 0;
  int32_t size = 0;
  *data = taosMemoryCalloc(sizeof(SSnapDataHdr), size);
  if (*data == NULL) {
    TSDB_CHECK_CODE(code = terrno, line, _error);
  }

  p->pBuf = NULL;

  SSnapDataHdr *pHdr = (SSnapDataHdr *)(*data);
  pHdr->type = SNAP_DATA_BSE;
  pHdr->size = size;
  uint8_t *pBuf = pHdr->data;

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
  // blockWrapperCleanup(&pReader->pBlockWrapper);
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
  code = bseTableMgtGetLiveFileSet(pBse->pTableMgt, &pAliveFile);
  TSDB_CHECK_CODE(code, line, _error);

  pIter->index = -1;
  pIter->pFileSet = pAliveFile;

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

static int32_t bseIterMoveToNextFile(SBseIter *pIter) {
  int32_t code = 0;
  if (pIter == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  if (pIter->index == -1) {
    pIter->index = 0;
    SBseLiveFileInfo *pInfo = taosArrayGet(pIter->pFileSet, pIter->index);
    if (pInfo == NULL) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
  }
  if (pIter->index >= taosArrayGetSize(pIter->pFileSet)) {
    pIter->isOver = 1;
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return code;
}

int32_t bseIterNext(SBseIter *pIter, SBseBatch **ppBatch) {
  if (pIter->isOver) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t code = 0;
  int32_t lino = 0;
  if (pIter == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  code = bseIterMoveToNextFile(pIter);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("vgId:%d failed to get next iter since %s", BSE_GET_VGID(pIter->pBse), tstrerror(code));
  }
  return code;
}

void bseIterDestroy(SBseIter *pIter) { return; }