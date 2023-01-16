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

// SRSmaSnapReader ========================================
struct SRSmaSnapReader {
  SSma*   pSma;
  int64_t sver;
  int64_t ever;
  SRSmaFS fs;

  // for data file
  int8_t           rsmaDataDone[TSDB_RETENTION_L2];
  STsdbSnapReader* pDataReader[TSDB_RETENTION_L2];

  // for qtaskinfo file
  int8_t         qTaskDone;
  int32_t        fsIter;
  SQTaskFReader* pQTaskFReader;
};

int32_t rsmaSnapReaderOpen(SSma* pSma, int64_t sver, int64_t ever, SRSmaSnapReader** ppReader) {
  int32_t          code = 0;
  int32_t          lino = 0;
  SVnode*          pVnode = pSma->pVnode;
  SRSmaSnapReader* pReader = NULL;
  SSmaEnv*         pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat*       pStat = (SRSmaStat*)SMA_ENV_STAT(pEnv);

  // alloc
  pReader = (SRSmaSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pReader->pSma = pSma;
  pReader->sver = sver;
  pReader->ever = ever;

  // open rsma1/rsma2
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbSnapReaderOpen(pSma->pRSmaTsdb[i], sver, ever, i == 0 ? SNAP_DATA_RSMA1 : SNAP_DATA_RSMA2,
                                &pReader->pDataReader[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // open qtaskinfo
  taosRLockLatch(RSMA_FS_LOCK(pStat));
  code = tdRSmaFSRef(pSma, &pReader->fs);
  taosRUnLockLatch(RSMA_FS_LOCK(pStat));
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosArrayGetSize(pReader->fs.aQTaskInf) > 0) {
    pReader->pQTaskFReader = taosMemoryCalloc(1, sizeof(SQTaskFReader));
    if (!pReader->pQTaskFReader) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pReader->pQTaskFReader->pSma = pSma;
    pReader->pQTaskFReader->version = pReader->ever;
  }

  *ppReader = pReader;
_exit:
  if (code) {
    if (pReader) rsmaSnapReaderClose(&pReader);
    *ppReader = NULL;
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t rsmaSnapReadQTaskInfo(SRSmaSnapReader* pReader, uint8_t** ppBuf) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SVnode*        pVnode = pReader->pSma->pVnode;
  SQTaskFReader* qReader = pReader->pQTaskFReader;
  SRSmaFS*       pFS = &pReader->fs;
  int64_t        n = 0;
  uint8_t*       pBuf = NULL;
  int64_t        version = pReader->ever;
  char           fname[TSDB_FILENAME_LEN];

  if (!qReader) {
    *ppBuf = NULL;
    smaInfo("vgId:%d, vnode snapshot rsma reader qtaskinfo, not needed since qTaskReader is NULL", TD_VID(pVnode));
    goto _exit;
  }

  if (pReader->fsIter >= taosArrayGetSize(pFS->aQTaskInf)) {
    *ppBuf = NULL;
    smaInfo("vgId:%d, vnode snapshot rsma reader qtaskinfo, fsIter reach end", TD_VID(pVnode));
    goto _exit;
  }

  while (pReader->fsIter < taosArrayGetSize(pFS->aQTaskInf)) {
    SQTaskFile* qTaskF = taosArrayGet(pFS->aQTaskInf, pReader->fsIter++);
    if (qTaskF->version != version) {
      continue;
    }

    tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), qTaskF->suid, qTaskF->level, version, tfsGetPrimaryPath(pVnode->pTfs),
                               fname);
    if (!taosCheckExistFile(fname)) {
      smaError("vgId:%d, vnode snapshot rsma reader for qtaskinfo, table %" PRIi64 ", level %" PRIi8
               ", version %" PRIi64 " failed since %s not exist",
               TD_VID(pVnode), qTaskF->suid, qTaskF->level, version, fname);
      code = TSDB_CODE_RSMA_FS_SYNC;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    TdFilePtr fp = taosOpenFile(fname, TD_FILE_READ);
    if (!fp) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    qReader->pReadH = fp;
    qReader->level = qTaskF->level;
    qReader->suid = qTaskF->suid;
  }

  if (!qReader->pReadH) {
    *ppBuf = NULL;
    smaInfo("vgId:%d, vnode snapshot rsma reader qtaskinfo, not needed since readh is NULL", TD_VID(pVnode));
    goto _exit;
  }

  int64_t size = 0;
  if (taosFStatFile(qReader->pReadH, &size, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // seek
  if (taosLSeekFile(qReader->pReadH, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (*ppBuf) {
    *ppBuf = taosMemoryRealloc(*ppBuf, sizeof(SSnapDataHdr) + size);
  } else {
    *ppBuf = taosMemoryMalloc(sizeof(SSnapDataHdr) + size);
  }
  if (!(*ppBuf)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // read
  n = taosReadFile(qReader->pReadH, POINTER_SHIFT(*ppBuf, sizeof(SSnapDataHdr)), size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (n != size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  smaInfo("vgId:%d, vnode snapshot rsma read qtaskinfo, version:%" PRIi64 ", size:%" PRIi64, TD_VID(pVnode), version,
          size);

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppBuf);
  pHdr->type = SNAP_DATA_QTASK;
  pHdr->flag = qReader->level;
  pHdr->index = qReader->suid;
  pHdr->size = size;

_exit:
  if (qReader) taosCloseFile(&qReader->pReadH);

  if (code) {
    *ppBuf = NULL;
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    smaInfo("vgId:%d, vnode snapshot rsma read qtaskinfo succeed", TD_VID(pVnode));
  }
  return code;
}

int32_t rsmaSnapRead(SRSmaSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

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
      TSDB_CHECK_CODE(code, lino, _exit);
      if (*ppData) {
        goto _exit;
      } else {
        pReader->rsmaDataDone[i] = 1;
      }
    } else {
      smaInfo("vgId:%d, vnode snapshot rsma read level %d is done", SMA_VID(pReader->pSma), i);
    }
  }

  // read qtaskinfo file
  if (!pReader->qTaskDone) {
    smaInfo("vgId:%d, vnode snapshot rsma qtaskinfo not done", SMA_VID(pReader->pSma));
    code = rsmaSnapReadQTaskInfo(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->qTaskDone = 1;
    }
  }

_exit:
  if (code) {
    smaError("vgId:%d, vnode snapshot rsma read failed since %s", SMA_VID(pReader->pSma), tstrerror(code));
    rsmaSnapReaderClose(&pReader);
  } else {
    smaInfo("vgId:%d, vnode snapshot rsma read succeed", SMA_VID(pReader->pSma));
  }
  return code;
}

int32_t rsmaSnapReaderClose(SRSmaSnapReader** ppReader) {
  int32_t          code = 0;
  SRSmaSnapReader* pReader = *ppReader;

  tdRSmaFSUnRef(pReader->pSma, &pReader->fs);
  taosMemoryFreeClear(pReader->pQTaskFReader);

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pReader->pDataReader[i]) {
      tsdbSnapReaderClose(&pReader->pDataReader[i]);
    }
  }

  smaInfo("vgId:%d, vnode snapshot rsma reader closed", SMA_VID(pReader->pSma));

  taosMemoryFreeClear(*ppReader);
  return code;
}

// SRSmaSnapWriter ========================================
struct SRSmaSnapWriter {
  SSma*   pSma;
  int64_t sver;
  int64_t ever;
  SRSmaFS fs;

  // for data file
  STsdbSnapWriter* pDataWriter[TSDB_RETENTION_L2];
};

int32_t rsmaSnapWriterOpen(SSma* pSma, int64_t sver, int64_t ever, SRSmaSnapWriter** ppWriter) {
  int32_t          code = 0;
  int32_t          lino = 0;
  SVnode*          pVnode = pSma->pVnode;
  SRSmaSnapWriter* pWriter = NULL;

  // alloc
  pWriter = (SRSmaSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (!pWriter) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pWriter->pSma = pSma;
  pWriter->sver = sver;
  pWriter->ever = ever;

  // rsma1/rsma2
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbSnapWriterOpen(pSma->pRSmaTsdb[i], sver, ever, &pWriter->pDataWriter[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // qtaskinfo
  code = tdRSmaFSCopy(pSma, &pWriter->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // snapWriter
  *ppWriter = pWriter;
_exit:
  if (code) {
    if (pWriter) rsmaSnapWriterClose(&pWriter, 0);
    *ppWriter = NULL;
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    smaInfo("vgId:%d, rsma snapshot writer open succeed", TD_VID(pSma->pVnode));
  }
  return code;
}

int32_t rsmaSnapWriterPrepareClose(SRSmaSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pWriter) {
    code = tdRSmaFSPrepareCommit(pWriter->pSma, &pWriter->fs);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", SMA_VID(pWriter->pSma), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t rsmaSnapWriterClose(SRSmaSnapWriter** ppWriter, int8_t rollback) {
  int32_t          code = 0;
  int32_t          lino = 0;
  SSma*            pSma = NULL;
  SVnode*          pVnode = NULL;
  SSmaEnv*         pEnv = NULL;
  SRSmaStat*       pStat = NULL;
  SRSmaSnapWriter* pWriter = *ppWriter;
  const char*      primaryPath = NULL;
  char             fname[TSDB_FILENAME_LEN] = {0};
  char             fnameVer[TSDB_FILENAME_LEN] = {0};
  TdFilePtr        pOutFD = NULL;
  TdFilePtr        pInFD = NULL;

  if (!pWriter) {
    goto _exit;
  }

  pSma = pWriter->pSma;
  pVnode = pSma->pVnode;
  pEnv = SMA_RSMA_ENV(pSma);
  pStat = (SRSmaStat*)SMA_ENV_STAT(pEnv);
  primaryPath = tfsGetPrimaryPath(pVnode->pTfs);

  // rsma1/rsma2
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pWriter->pDataWriter[i]) {
      code = tsdbSnapWriterClose(&pWriter->pDataWriter[i], rollback);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // qtaskinfo
  if (rollback) {
    tdRSmaFSRollback(pSma);
    // remove qTaskFiles
  } else {
    // sendFile from fname.Ver to fname
    SRSmaFS* pFS = &pWriter->fs;
    int32_t  size = taosArrayGetSize(pFS->aQTaskInf);
    for (int32_t i = 0; i < size; ++i) {
      SQTaskFile* pTaskF = TARRAY_GET_ELEM(pFS->aQTaskInf, i);
      if (pTaskF->version == pWriter->ever) {
        tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->suid, pTaskF->level, pTaskF->version, primaryPath, fnameVer);
        tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->suid, pTaskF->level, -1, primaryPath, fname);

        pInFD = taosOpenFile(fnameVer, TD_FILE_READ);
        if (pInFD == NULL) {
          code = TAOS_SYSTEM_ERROR(errno);
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        pOutFD = taosCreateFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
        if (pOutFD == NULL) {
          code = TAOS_SYSTEM_ERROR(errno);
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        int64_t size = 0;
        if (taosFStatFile(pInFD, &size, NULL) < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        int64_t offset = 0;
        if (taosFSendFile(pOutFD, pInFD, &offset, size) < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          smaError("vgId:%d, vnode snapshot rsma writer, send qtaskinfo file %s to %s failed since %s", TD_VID(pVnode),
                   fnameVer, fname, tstrerror(code));
          TSDB_CHECK_CODE(code, lino, _exit);
        }
        taosCloseFile(&pOutFD);
        taosCloseFile(&pInFD);
      }
    }

    // lock
    taosWLockLatch(RSMA_FS_LOCK(pStat));
    code = tdRSmaFSCommit(pSma);
    if (code) {
      taosWUnLockLatch(RSMA_FS_LOCK(pStat));
      goto _exit;
    }
    // unlock
    taosWUnLockLatch(RSMA_FS_LOCK(pStat));
  }

  // rsma restore
  code = tdRSmaRestore(pWriter->pSma, RSMA_RESTORE_SYNC, pWriter->ever, rollback);
  TSDB_CHECK_CODE(code, lino, _exit);
  smaInfo("vgId:%d, vnode snapshot rsma writer restore from sync succeed", SMA_VID(pSma));

_exit:
  if (pWriter) taosMemoryFree(pWriter);
  *ppWriter = NULL;
  if (code) {
    if (pOutFD) taosCloseFile(&pOutFD);
    if (pInFD) taosCloseFile(&pInFD);
    smaError("vgId:%d, vnode snapshot rsma writer close failed since %s", SMA_VID(pSma), tstrerror(code));
  } else {
    smaInfo("vgId:%d, vnode snapshot rsma writer close succeed", pSma ? SMA_VID(pSma) : 0);
  }

  return code;
}

int32_t rsmaSnapWrite(SRSmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;

  // rsma1/rsma2
  if (pHdr->type == SNAP_DATA_RSMA1) {
    pHdr->type = SNAP_DATA_TSDB;
    code = tsdbSnapWrite(pWriter->pDataWriter[0], pData, nData);
  } else if (pHdr->type == SNAP_DATA_RSMA2) {
    pHdr->type = SNAP_DATA_TSDB;
    code = tsdbSnapWrite(pWriter->pDataWriter[1], pData, nData);
  } else if (pHdr->type == SNAP_DATA_QTASK) {
    code = rsmaSnapWriteQTaskInfo(pWriter, pData, nData);
  } else {
    code = TSDB_CODE_RSMA_FS_SYNC;
  }
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s, data type %" PRIi8, SMA_VID(pWriter->pSma), __func__, lino,
             tstrerror(code), pHdr->type);
  } else {
    smaInfo("vgId:%d, rsma snapshot write for data type %" PRIi8 " succeed", SMA_VID(pWriter->pSma), pHdr->type);
  }
  return code;
}

static int32_t rsmaSnapWriteQTaskInfo(SRSmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SSma*         pSma = pWriter->pSma;
  SVnode*       pVnode = pSma->pVnode;
  char          fname[TSDB_FILENAME_LEN];
  TdFilePtr     fp = NULL;
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;

  fname[0] = '\0';

  if (pHdr->size != (nData - sizeof(SSnapDataHdr))) {
    code = TSDB_CODE_RSMA_FS_SYNC;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SQTaskFile qTaskFile = {
      .nRef = 1, .level = pHdr->flag, .suid = pHdr->index, .version = pWriter->ever, .size = pHdr->size};

  tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pHdr->index, pHdr->flag, qTaskFile.version,
                             tfsGetPrimaryPath(pVnode->pTfs), fname);

  fp = taosCreateFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (!fp) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t contLen = taosWriteFile(fp, pHdr->data, pHdr->size);
  if (contLen != pHdr->size) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  uint32_t mtime = 0;
  if (taosFStatFile(fp, NULL, &mtime) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    qTaskFile.mtime = mtime;
  }

  if (taosFsyncFile(fp) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosCloseFile(&fp);

  code = tdRSmaFSUpsertQTaskFile(pSma, &pWriter->fs, &qTaskFile, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    if (fp) {
      (void)taosRemoveFile(fname);
    }
    smaError("vgId:%d, %s failed at line %d since %s, file:%s", TD_VID(pVnode), __func__, lino, tstrerror(code), fname);
  } else {
    smaInfo("vgId:%d, vnode snapshot rsma write qtaskinfo %s succeed", TD_VID(pVnode), fname);
  }

  return code;
}
