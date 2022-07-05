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

#include "tsdb.h"

// STsdbSnapReader ========================================
struct STsdbSnapReader {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  // for data file
  int8_t        dataDone;
  int32_t       fid;
  SDataFReader* pDataFReader;
  int32_t       iBlockIdx;
  SArray*       aBlockIdx;  // SArray<SBlockIdx>
  SMapData      mBlock;     // SMapData<SBlock>
  SBlockData    blkData;
  // for del file
  int8_t       delDone;
  SDelFReader* pDelFReader;
  int32_t      iDelIdx;
  SArray*      aDelIdx;   // SArray<SDelIdx>
  SArray*      aDelData;  // SArray<SDelData>
};

static int32_t tsdbSnapReadData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;

  if (pReader->pDataFReader == NULL) {
    code = tsdbDataFReaderOpen(&pReader->pDataFReader, pReader->pTsdb, NULL);
    if (code) goto _err;

    code = tsdbReadBlockIdx(pReader->pDataFReader, pReader->aBlockIdx, NULL);
    if (code) goto _err;

    pReader->iBlockIdx = 0;
  }

  return code;

_err:
  tsdbError("vgId:%d snap read data failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbSnapReadDel(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t   code = 0;
  STsdb*    pTsdb = pReader->pTsdb;
  SDelFile* pDelFile = pTsdb->fs->cState->pDelFile;

  if (pReader->pDelFReader == NULL) {
    if (pDelFile == NULL) {
      code = TSDB_CODE_VND_READ_END;
      goto _exit;
    }

    // open
    code = tsdbDelFReaderOpen(&pReader->pDelFReader, pDelFile, pTsdb, NULL);
    if (code) goto _err;

    // read index
    code = tsdbReadDelIdx(pReader->pDelFReader, pReader->aDelIdx, NULL);
    if (code) goto _err;

    pReader->iDelIdx = 0;
  }

  while (pReader->iDelIdx < taosArrayGetSize(pReader->aDelIdx)) {
    SDelIdx* pDelIdx = (SDelIdx*)taosArrayGet(pReader->aDelIdx, pReader->iDelIdx);
    int8_t   overlap = 0;

    code = tsdbReadDelData(pReader->pDelFReader, pDelIdx, pReader->aDelData, NULL);
    if (code) goto _err;

    for (int32_t iDelData = 0; iDelData < taosArrayGetSize(pReader->aDelData); iDelData++) {
      SDelData* pDelData = (SDelData*)taosArrayGet(pReader->aDelData, iDelData);

      if (pDelData->version >= pReader->sver && pDelData->version <= pReader->ever) {
        // encode the data to sync (todo)
        overlap = 1;
      }
    }

    if (overlap) {
      // prepare the data
      goto _exit;
    }
  }

  code = TSDB_CODE_VND_READ_END;

_exit:
  return code;

_err:
  tsdbError("vgId:%d snap read del failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbSnapReaderOpen(STsdb* pTsdb, int64_t sver, int64_t ever, STsdbSnapReader** ppReader) {
  int32_t          code = 0;
  STsdbSnapReader* pReader = NULL;

  // alloc
  pReader = (STsdbSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pTsdb = pTsdb;
  pReader->sver = sver;
  pReader->ever = ever;

  pReader->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pReader->aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pReader->mBlock = tMapDataInit();

  code = tBlockDataInit(&pReader->blkData);
  if (code) goto _err;

  pReader->aDelIdx = taosArrayInit(0, sizeof(SDelIdx));
  if (pReader->aDelIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pReader->aDelData = taosArrayInit(0, sizeof(SDelData));
  if (pReader->aDelData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  *ppReader = pReader;
  return code;

_err:
  tsdbError("vgId:%d snapshot reader open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t tsdbSnapReaderClose(STsdbSnapReader** ppReader) {
  int32_t          code = 0;
  STsdbSnapReader* pReader = *ppReader;

  taosArrayDestroy(pReader->aDelData);
  taosArrayDestroy(pReader->aDelIdx);
  if (pReader->pDelFReader) {
    tsdbDelFReaderClose(&pReader->pDelFReader);
  }
  tBlockDataClear(&pReader->blkData);
  tMapDataClear(&pReader->mBlock);
  taosArrayDestroy(&pReader->aBlockIdx);
  if (pReader->pDataFReader) {
    tsdbDataFReaderClose(&pReader->pDataFReader);
  }
  taosMemoryFree(pReader);
  *ppReader = NULL;

  return code;
}

int32_t tsdbSnapRead(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;

  // read data file
  if (!pReader->dataDone) {
    code = tsdbSnapReadData(pReader, ppData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->dataDone = 1;
      } else {
        goto _err;
      }
    } else {
      goto _exit;
    }
  }

  // read del file
  if (!pReader->delDone) {
    code = tsdbSnapReadDel(pReader, ppData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->delDone = 1;
      } else {
        goto _err;
      }
    } else {
      goto _exit;
    }
  }

  code = TSDB_CODE_VND_READ_END;

_exit:
  return code;

_err:
  tsdbError("vgId:%d snapshot read failed since %s", TD_VID(pReader->pTsdb->pVnode), tstrerror(code));
  return code;
}

// STsdbSnapReader ========================================
struct STsdbSnapWriter {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  // for data file
  int32_t       iDFileSet;
  SDataFWriter* pDataFWriter;
  // for del file
  SDelFWriter* pDelFWriter;
};