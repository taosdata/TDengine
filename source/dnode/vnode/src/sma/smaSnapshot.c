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

#include "sma.h"

static int32_t rsmaSnapReadQTaskInfo(SRSmaSnapReader* pReader, uint8_t** ppData);
static int32_t rsmaSnapWriteQTaskInfo(SRSmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
static int32_t rsmaQTaskInfSnapReaderOpen(SRSmaSnapReader* pReader, int64_t version);
static int32_t rsmaQTaskInfSnapReaderClose(SQTaskFReader** ppReader);

// SRSmaSnapReader ========================================
struct SRSmaSnapReader {
  SSma*   pSma;
  int64_t sver;
  int64_t ever;

  // for data file
  int8_t           rsmaDataDone[TSDB_RETENTION_L2];
  STsdbSnapReader* pDataReader[TSDB_RETENTION_L2];

  // for qtaskinfo file
  int8_t         qTaskDone;
  SQTaskFReader* pQTaskFReader;
};

int32_t rsmaSnapReaderOpen(SSma* pSma, int64_t sver, int64_t ever, SRSmaSnapReader** ppReader) {
  int32_t          code = 0;
  SVnode*          pVnode = pSma->pVnode;
  SRSmaSnapReader* pReader = NULL;

  // alloc
  pReader = (SRSmaSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pSma = pSma;
  pReader->sver = sver;
  pReader->ever = ever;

  // open rsma1/rsma2
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbSnapReaderOpen(pSma->pRSmaTsdb[i], sver, ever, i == 0 ? SNAP_DATA_RSMA1 : SNAP_DATA_RSMA2,
                                &pReader->pDataReader[i]);
      if (code < 0) {
        goto _err;
      }
    }
  }

  // open qtaskinfo
  if ((code = rsmaQTaskInfSnapReaderOpen(pReader, ever)) < 0) {
    goto _err;
  }

  *ppReader = pReader;

  return TSDB_CODE_SUCCESS;
_err:
  if (pReader) rsmaSnapReaderClose(&pReader);
  *ppReader = NULL;
  smaError("vgId:%d, vnode snapshot rsma reader open failed since %s", TD_VID(pVnode), tstrerror(code));
  return TSDB_CODE_FAILED;
}

static int32_t rsmaQTaskInfSnapReaderOpen(SRSmaSnapReader* pReader, int64_t version) {
  int32_t    code = 0;
  SSma*      pSma = pReader->pSma;
  SVnode*    pVnode = pSma->pVnode;
  SSmaEnv*   pEnv = NULL;
  SRSmaStat* pStat = NULL;

  if (!(pEnv = SMA_RSMA_ENV(pSma))) {
    smaInfo("vgId:%d, vnode snapshot rsma reader for qtaskinfo version %" PRIi64 " not need as env is NULL",
            TD_VID(pVnode), version);
    return TSDB_CODE_SUCCESS;
  }

  pStat = (SRSmaStat*)SMA_ENV_STAT(pEnv);

  int32_t ref = tdRSmaFSRef(pReader->pSma, pStat, version);
  if (ref < 1) {
    smaInfo("vgId:%d, vnode snapshot rsma reader for qtaskinfo version %" PRIi64 " not need as ref is %d",
            TD_VID(pVnode), version, ref);
    return TSDB_CODE_SUCCESS;
  }

  char qTaskInfoFullName[TSDB_FILENAME_LEN];
  tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), version, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFullName);

  if (!taosCheckExistFile(qTaskInfoFullName)) {
    tdRSmaFSUnRef(pSma, pStat, version);
    smaInfo("vgId:%d, vnode snapshot rsma reader for qtaskinfo version %" PRIi64 " not need as %s not exist",
            TD_VID(pVnode), version, qTaskInfoFullName);
    return TSDB_CODE_SUCCESS;
  }

  pReader->pQTaskFReader = taosMemoryCalloc(1, sizeof(SQTaskFReader));
  if (!pReader->pQTaskFReader) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  TdFilePtr fp = taosOpenFile(qTaskInfoFullName, TD_FILE_READ);
  if (!fp) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosMemoryFreeClear(pReader->pQTaskFReader);
    goto _end;
  }

  pReader->pQTaskFReader->pReadH = fp;
  pReader->pQTaskFReader->pSma = pSma;
  pReader->pQTaskFReader->version = pReader->ever;

_end:
  if (code < 0) {
    tdRSmaFSUnRef(pSma, pStat, version);
    smaError("vgId:%d, vnode snapshot rsma reader open %s succeed", TD_VID(pVnode), qTaskInfoFullName);
    return TSDB_CODE_FAILED;
  }

  smaInfo("vgId:%d, vnode snapshot rsma reader open %s succeed", TD_VID(pVnode), qTaskInfoFullName);
  return TSDB_CODE_SUCCESS;
}

static int32_t rsmaQTaskInfSnapReaderClose(SQTaskFReader** ppReader) {
  if (!(*ppReader)) {
    return TSDB_CODE_SUCCESS;
  }

  SSma*      pSma = (*ppReader)->pSma;
  SRSmaStat* pStat = SMA_RSMA_STAT(pSma);
  int64_t    version = (*ppReader)->version;

  taosCloseFile(&(*ppReader)->pReadH);
  tdRSmaFSUnRef(pSma, pStat, version);
  taosMemoryFreeClear(*ppReader);
  smaInfo("vgId:%d, vnode snapshot rsma reader closed for qTaskInfo version %" PRIi64, SMA_VID(pSma), version);

  return TSDB_CODE_SUCCESS;
}

static int32_t rsmaSnapReadQTaskInfo(SRSmaSnapReader* pReader, uint8_t** ppBuf) {
  int32_t        code = 0;
  SSma*          pSma = pReader->pSma;
  int64_t        n = 0;
  uint8_t*       pBuf = NULL;
  SQTaskFReader* qReader = pReader->pQTaskFReader;

  if (!qReader) {
    *ppBuf = NULL;
    smaInfo("vgId:%d, vnode snapshot rsma reader qtaskinfo, qTaskReader is NULL", SMA_VID(pSma));
    return 0;
  }

  if (!qReader->pReadH) {
    *ppBuf = NULL;
    smaInfo("vgId:%d, vnode snapshot rsma reader qtaskinfo, readh is NULL", SMA_VID(pSma));
    return 0;
  }

  int64_t size = 0;
  if (taosFStatFile(qReader->pReadH, &size, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // seek
  if (taosLSeekFile(qReader->pReadH, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  ASSERT(!(*ppBuf));
  // alloc
  *ppBuf = taosMemoryCalloc(1, sizeof(SSnapDataHdr) + size);
  if (!(*ppBuf)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // read
  n = taosReadFile(qReader->pReadH, POINTER_SHIFT(*ppBuf, sizeof(SSnapDataHdr)), size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  } else if (n != size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _err;
  }

  smaInfo("vgId:%d, vnode snapshot rsma read qtaskinfo, size:%" PRIi64, SMA_VID(pSma), size);

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppBuf);
  pHdr->type = SNAP_DATA_QTASK;
  pHdr->size = size;

_exit:
  smaInfo("vgId:%d, vnode snapshot rsma read qtaskinfo succeed", SMA_VID(pSma));
  return code;

_err:
  *ppBuf = NULL;
  smaError("vgId:%d, vnode snapshot rsma read qtaskinfo failed since %s", SMA_VID(pSma), tstrerror(code));
  return code;
}

int32_t rsmaSnapRead(SRSmaSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;

  *ppData = NULL;

  smaInfo("vgId:%d, vnode snapshot rsma read entry", SMA_VID(pReader->pSma));
  // read rsma1/rsma2 file
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    STsdbSnapReader* pTsdbSnapReader = pReader->pDataReader[i];
    if (!pTsdbSnapReader) {
      continue;
    }
    if (!pReader->rsmaDataDone[i]) {
      smaInfo("vgId:%d, vnode snapshot rsma read level %d not done", SMA_VID(pReader->pSma), i);
      code = tsdbSnapRead(pTsdbSnapReader, ppData);
      if (code) {
        goto _err;
      } else {
        if (*ppData) {
          goto _exit;
        } else {
          pReader->rsmaDataDone[i] = 1;
        }
      }
    } else {
      smaInfo("vgId:%d, vnode snapshot rsma read level %d is done", SMA_VID(pReader->pSma), i);
    }
  }

  // read qtaskinfo file
  if (!pReader->qTaskDone) {
    smaInfo("vgId:%d, vnode snapshot rsma qtaskinfo not done", SMA_VID(pReader->pSma));
    code = rsmaSnapReadQTaskInfo(pReader, ppData);
    if (code) {
      goto _err;
    } else {
      pReader->qTaskDone = 1;
      if (*ppData) {
        goto _exit;
      }
    }
  }

_exit:
  smaInfo("vgId:%d, vnode snapshot rsma read succeed", SMA_VID(pReader->pSma));
  return code;

_err:
  smaError("vgId:%d, vnode snapshot rsma read failed since %s", SMA_VID(pReader->pSma), tstrerror(code));
  return code;
}

int32_t rsmaSnapReaderClose(SRSmaSnapReader** ppReader) {
  int32_t          code = 0;
  SRSmaSnapReader* pReader = *ppReader;

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pReader->pDataReader[i]) {
      tsdbSnapReaderClose(&pReader->pDataReader[i]);
    }
  }

  rsmaQTaskInfSnapReaderClose(&pReader->pQTaskFReader);

  smaInfo("vgId:%d, vnode snapshot rsma reader closed", SMA_VID(pReader->pSma));

  taosMemoryFreeClear(*ppReader);
  return code;
}

// SRSmaSnapWriter ========================================
struct SRSmaSnapWriter {
  SSma*   pSma;
  int64_t sver;
  int64_t ever;

  // config
  int64_t commitID;

  // for data file
  STsdbSnapWriter* pDataWriter[TSDB_RETENTION_L2];

  // for qtaskinfo file
  SQTaskFReader* pQTaskFReader;
  SQTaskFWriter* pQTaskFWriter;
};

int32_t rsmaSnapWriterOpen(SSma* pSma, int64_t sver, int64_t ever, SRSmaSnapWriter** ppWriter) {
  int32_t          code = 0;
  SRSmaSnapWriter* pWriter = NULL;
  SVnode*          pVnode = pSma->pVnode;

  // alloc
  pWriter = (SRSmaSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pSma = pSma;
  pWriter->sver = sver;
  pWriter->ever = ever;

  // rsma1/rsma2
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbSnapWriterOpen(pSma->pRSmaTsdb[i], sver, ever, &pWriter->pDataWriter[i]);
      if (code < 0) {
        goto _err;
      }
    }
  }

  // qtaskinfo
  SQTaskFWriter* qWriter = (SQTaskFWriter*)taosMemoryCalloc(1, sizeof(SQTaskFWriter));
  qWriter->pSma = pSma;

  char qTaskInfoFullName[TSDB_FILENAME_LEN];
  tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), 0, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFullName);
  TdFilePtr qTaskF = taosCreateFile(qTaskInfoFullName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (!qTaskF) {
    taosMemoryFree(qWriter);
    code = TAOS_SYSTEM_ERROR(errno);
    smaError("vgId:%d, rsma snapshot writer open %s failed since %s", TD_VID(pSma->pVnode), qTaskInfoFullName,
             tstrerror(code));
    goto _err;
  }
  qWriter->pWriteH = qTaskF;
  int32_t fnameLen = strlen(qTaskInfoFullName) + 1;
  qWriter->fname = taosMemoryCalloc(1, fnameLen);
  strncpy(qWriter->fname, qTaskInfoFullName, fnameLen);
  pWriter->pQTaskFWriter = qWriter;
  smaDebug("vgId:%d, rsma snapshot writer open succeed for %s", TD_VID(pSma->pVnode), qTaskInfoFullName);

  // snapWriter
  *ppWriter = pWriter;

  smaInfo("vgId:%d, rsma snapshot writer open succeed", TD_VID(pSma->pVnode));
  return code;

_err:
  smaError("vgId:%d, rsma snapshot writer open failed since %s", TD_VID(pSma->pVnode), tstrerror(code));
  if (pWriter) rsmaSnapWriterClose(&pWriter, 0);
  *ppWriter = NULL;
  return code;
}

int32_t rsmaSnapWriterClose(SRSmaSnapWriter** ppWriter, int8_t rollback) {
  int32_t          code = 0;
  SRSmaSnapWriter* pWriter = *ppWriter;
  SVnode*          pVnode = pWriter->pSma->pVnode;

  if (rollback) {
    // TODO: rsma1/rsma2
    // qtaskinfo
    if (pWriter->pQTaskFWriter) {
      if (taosRemoveFile(pWriter->pQTaskFWriter->fname) != 0) {
        smaWarn("vgId:%d, vnode snapshot rsma writer failed to remove %s since %s", SMA_VID(pWriter->pSma),
                pWriter->pQTaskFWriter->fname ? pWriter->pQTaskFWriter->fname : "NULL",
                tstrerror(TAOS_SYSTEM_ERROR(errno)));
      }
    }
  } else {
    // rsma1/rsma2
    for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
      if (pWriter->pDataWriter[i]) {
        code = tsdbSnapWriterClose(&pWriter->pDataWriter[i], rollback);
        if (code) goto _err;
      }
    }
    // qtaskinfo
    if (pWriter->pQTaskFWriter) {
      char qTaskInfoFullName[TSDB_FILENAME_LEN];
      tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pWriter->ever, tfsGetPrimaryPath(pVnode->pTfs), qTaskInfoFullName);
      if (taosRenameFile(pWriter->pQTaskFWriter->fname, qTaskInfoFullName) < 0) {
        code = TAOS_SYSTEM_ERROR(errno);
        goto _err;
      }
      smaInfo("vgId:%d, vnode snapshot rsma writer rename %s to %s", SMA_VID(pWriter->pSma),
              pWriter->pQTaskFWriter->fname, qTaskInfoFullName);

      // rsma restore
      if ((code = tdRSmaRestore(pWriter->pSma, RSMA_RESTORE_SYNC, pWriter->ever)) < 0) {
        goto _err;
      }
      smaInfo("vgId:%d, vnode snapshot rsma writer restore from %s succeed", SMA_VID(pWriter->pSma), qTaskInfoFullName);
    }
  }

  smaInfo("vgId:%d, vnode snapshot rsma writer close succeed", SMA_VID(pWriter->pSma));
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
  return code;

_err:
  smaError("vgId:%d, vnode snapshot rsma writer close failed since %s", SMA_VID(pWriter->pSma), tstrerror(code));
  return code;
}

int32_t rsmaSnapWrite(SRSmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t       code = 0;
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;

  // rsma1/rsma2
  if (pHdr->type == SNAP_DATA_RSMA1) {
    pHdr->type = SNAP_DATA_TSDB;
    code = tsdbSnapWrite(pWriter->pDataWriter[0], pHdr);
  } else if (pHdr->type == SNAP_DATA_RSMA2) {
    pHdr->type = SNAP_DATA_TSDB;
    code = tsdbSnapWrite(pWriter->pDataWriter[1], pHdr);
  } else if (pHdr->type == SNAP_DATA_QTASK) {
    code = rsmaSnapWriteQTaskInfo(pWriter, pData, nData);
  } else {
    ASSERT(0);
  }
  if (code < 0) goto _err;

_exit:
  smaInfo("vgId:%d, rsma snapshot write for data type %" PRIi8 " succeed", SMA_VID(pWriter->pSma), pHdr->type);
  return code;

_err:
  smaError("vgId:%d, rsma snapshot write for data type %" PRIi8 " failed since %s", SMA_VID(pWriter->pSma), pHdr->type,
           tstrerror(code));
  return code;
}

static int32_t rsmaSnapWriteQTaskInfo(SRSmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t        code = 0;
  SQTaskFWriter* qWriter = pWriter->pQTaskFWriter;

  if (qWriter && qWriter->pWriteH) {
    SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;
    int64_t       size = pHdr->size;
    ASSERT(size == (nData - sizeof(SSnapDataHdr)));
    int64_t contLen = taosWriteFile(qWriter->pWriteH, pHdr->data, size);
    if (contLen != size) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    smaInfo("vgId:%d, vnode snapshot rsma write qtaskinfo %s succeed", SMA_VID(pWriter->pSma), qWriter->fname);
  } else {
    smaInfo("vgId:%d, vnode snapshot rsma write qtaskinfo is not needed", SMA_VID(pWriter->pSma));
  }

_exit:
  return code;

_err:
  smaError("vgId:%d, vnode snapshot rsma write qtaskinfo failed since %s", SMA_VID(pWriter->pSma), tstrerror(code));
  return code;
}
