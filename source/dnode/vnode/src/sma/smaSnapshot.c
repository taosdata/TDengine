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

// SRSmaSnapReader ========================================
struct SRSmaSnapReader {
  SSma*   pSma;
  int64_t sver;
  int64_t ever;
  SRSmaFS fs;

  // for data file
  int8_t           rsmaDataDone[TSDB_RETENTION_L2];
  STsdbSnapReader* pDataReader[TSDB_RETENTION_L2];
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
      code = tsdbSnapReaderOpen(pSma->pRSmaTsdb[i], sver, ever, (i == 0 ? SNAP_DATA_RSMA1 : SNAP_DATA_RSMA2), NULL,
                                &pReader->pDataReader[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
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

int32_t rsmaSnapWriterOpen(SSma* pSma, int64_t sver, int64_t ever, void** ppRanges, SRSmaSnapWriter** ppWriter) {
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
      code = tsdbSnapWriterOpen(pSma->pRSmaTsdb[i], sver, ever, ((void**)ppRanges)[i], &pWriter->pDataWriter[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

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
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pWriter->pDataWriter[i]) {
      code = tsdbSnapWriterPrepareClose(pWriter->pDataWriter[i]);
      if (code) {
        smaError("vgId:%d, failed to prepare close tsdbSnapWriter since %s. i: %d", SMA_VID(pWriter->pSma), terrstr(),
                 i);
        return -1;
      }
    }
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

  // rsma1/rsma2
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pWriter->pDataWriter[i]) {
      code = tsdbSnapWriterClose(&pWriter->pDataWriter[i], rollback);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
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
    code = tsdbSnapWrite(pWriter->pDataWriter[0], pHdr);
  } else if (pHdr->type == SNAP_DATA_RSMA2) {
    pHdr->type = SNAP_DATA_TSDB;
    code = tsdbSnapWrite(pWriter->pDataWriter[1], pHdr);
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
