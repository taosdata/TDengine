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

static int32_t rsmaSnapReadQTaskInfo(SRsmaSnapReader* pReader, uint8_t** ppData);
static int32_t rsmaSnapWriteQTaskInfo(SRsmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData);

// SRsmaSnapReader ========================================
struct SRsmaSnapReader {
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

int32_t rsmaSnapReaderOpen(SSma* pSma, int64_t sver, int64_t ever, SRsmaSnapReader** ppReader) {
  int32_t          code = 0;
  SRsmaSnapReader* pReader = NULL;

  // alloc
  pReader = (SRsmaSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pSma = pSma;
  pReader->sver = sver;
  pReader->ever = ever;

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbSnapReaderOpen(pSma->pRSmaTsdb[i], sver, ever, i == 0 ? SNAP_DATA_RSMA1 : SNAP_DATA_RSMA2,
                                &pReader->pDataReader[i]);
      if (code < 0) {
        goto _err;
      }
    }
  }
  *ppReader = pReader;
  smaInfo("vgId:%d vnode snapshot rsma reader opened succeed", SMA_VID(pSma));
  return TSDB_CODE_SUCCESS;
_err:
  smaError("vgId:%d vnode snapshot rsma reader opened failed since %s", SMA_VID(pSma), tstrerror(code)); 
  return TSDB_CODE_FAILED;
}

static int32_t rsmaSnapReadQTaskInfo(SRsmaSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  SSma*   pSma = pReader->pSma;

_exit:
  smaInfo("vgId:%d vnode snapshot rsma read qtaskinfo succeed", SMA_VID(pSma));
  return code;

_err:
  smaError("vgId:%d vnode snapshot rsma read qtaskinfo failed since %s", SMA_VID(pSma), tstrerror(code));
  return code;
}

int32_t rsmaSnapRead(SRsmaSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;

  *ppData = NULL;

  smaInfo("vgId:%d vnode snapshot rsma read entry", SMA_VID(pReader->pSma));
  // read rsma1/rsma2 file
  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    STsdbSnapReader* pTsdbSnapReader = pReader->pDataReader[i];
    if (!pTsdbSnapReader) {
      continue;
    }
    if (!pReader->rsmaDataDone[i]) {
      smaInfo("vgId:%d vnode snapshot rsma read level %d not done", SMA_VID(pReader->pSma), i);
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
      smaInfo("vgId:%d vnode snapshot rsma read level %d is done", SMA_VID(pReader->pSma), i);
    }
  }

  // read qtaskinfo file
  if (!pReader->qTaskDone) {
    code = rsmaSnapReadQTaskInfo(pReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->qTaskDone = 1;
      }
    }
  }

_exit:
  smaInfo("vgId:%d vnode snapshot rsma read succeed", SMA_VID(pReader->pSma));
  return code;

_err:
  smaError("vgId:%d vnode snapshot rsma read failed since %s", SMA_VID(pReader->pSma), tstrerror(code));
  return code;
}

int32_t rsmaSnapReaderClose(SRsmaSnapReader** ppReader) {
  int32_t          code = 0;
  SRsmaSnapReader* pReader = *ppReader;

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pReader->pDataReader[i]) {
      tsdbSnapReaderClose(&pReader->pDataReader[i]);
    }
  }

  if (pReader->pQTaskFReader) {
    // TODO: close for qtaskinfo
    smaInfo("vgId:%d vnode snapshot rsma reader closed for qTaskInfo", SMA_VID(pReader->pSma));
  }


  smaInfo("vgId:%d vnode snapshot rsma reader closed", SMA_VID(pReader->pSma));

  taosMemoryFreeClear(*ppReader);
  return code;
}

// SRsmaSnapWriter ========================================
struct SRsmaSnapWriter {
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

int32_t rsmaSnapWriterOpen(SSma* pSma, int64_t sver, int64_t ever, SRsmaSnapWriter** ppWriter) {
  int32_t          code = 0;
  SRsmaSnapWriter* pWriter = NULL;

  // alloc
  pWriter = (SRsmaSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pSma = pSma;
  pWriter->sver = sver;
  pWriter->ever = ever;

  for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
    if (pSma->pRSmaTsdb[i]) {
      code = tsdbSnapWriterOpen(pSma->pRSmaTsdb[i], sver, ever, &pWriter->pDataWriter[i]);
      if (code < 0) {
        goto _err;
      }
    }
  }

  // qtaskinfo
  // TODO

  *ppWriter = pWriter;

  smaInfo("vgId:%d rsma snapshot writer open succeed", TD_VID(pSma->pVnode));
  return code;

_err:
  smaError("vgId:%d rsma snapshot writer open failed since %s", TD_VID(pSma->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t rsmaSnapWriterClose(SRsmaSnapWriter** ppWriter, int8_t rollback) {
  int32_t          code = 0;
  SRsmaSnapWriter* pWriter = *ppWriter;

  if (rollback) {
    ASSERT(0);
    // code = tsdbFSRollback(pWriter->pTsdb->pFS);
    // if (code) goto _err;
  } else {
    for (int32_t i = 0; i < TSDB_RETENTION_L2; ++i) {
      if (pWriter->pDataWriter[i]) {
        code = tsdbSnapWriterClose(&pWriter->pDataWriter[i], rollback);
        if (code) goto _err;
      }
    }
  }

  smaInfo("vgId:%d vnode snapshot rsma writer close succeed", SMA_VID(pWriter->pSma));
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
  return code;

_err:
  smaError("vgId:%d vnode snapshot rsma writer close failed since %s", SMA_VID(pWriter->pSma), tstrerror(code));
  return code;
}

int32_t rsmaSnapWrite(SRsmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t       code = 0;
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
    ASSERT(0);
  }
  if (code < 0) goto _err;

_exit:
  smaInfo("vgId:%d rsma snapshot write for data type %" PRIi8 " succeed", SMA_VID(pWriter->pSma), pHdr->type);
  return code;

_err:
  smaError("vgId:%d rsma snapshot write for data type %" PRIi8 " failed since %s", SMA_VID(pWriter->pSma), pHdr->type,
           tstrerror(code));
  return code;
}

static int32_t rsmaSnapWriteQTaskInfo(SRsmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;

  if (pWriter->pQTaskFWriter == NULL) {
    // SDelFile* pDelFile = pWriter->fs.pDelFile;

    // // reader
    // if (pDelFile) {
    //   code = tsdbDelFReaderOpen(&pWriter->pDelFReader, pDelFile, pTsdb, NULL);
    //   if (code) goto _err;

    //   code = tsdbReadDelIdx(pWriter->pDelFReader, pWriter->aDelIdxR, NULL);
    //   if (code) goto _err;
    // }

    // // writer
    // SDelFile delFile = {.commitID = pWriter->commitID, .offset = 0, .size = 0};
    // code = tsdbDelFWriterOpen(&pWriter->pDelFWriter, &delFile, pTsdb);
    // if (code) goto _err;
  }
  smaInfo("vgId:%d vnode snapshot rsma write qtaskinfo succeed", SMA_VID(pWriter->pSma));
_exit:
  return code;

_err:
  smaError("vgId:%d vnode snapshot rsma write qtaskinfo failed since %s", SMA_VID(pWriter->pSma), tstrerror(code));
  return code;
}
