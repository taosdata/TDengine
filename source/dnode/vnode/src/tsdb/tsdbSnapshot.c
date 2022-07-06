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
  SArray*       aBlockIdx;  // SArray<SBlockIdx>
  int32_t       iBlockIdx;
  SBlockIdx*    pBlockIdx;
  SMapData      mBlock;  // SMapData<SBlock>
  int32_t       iBlock;
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

  while (true) {
    if (pReader->pDataFReader == NULL) {
      SDFileSet* pSet = NULL;

      // search the next data file set to read (todo)
      if (0 /* TODO */) {
        code = TSDB_CODE_VND_READ_END;
        goto _exit;
      }

      // open
      code = tsdbDataFReaderOpen(&pReader->pDataFReader, pReader->pTsdb, pSet);
      if (code) goto _err;

      // SBlockIdx
      code = tsdbReadBlockIdx(pReader->pDataFReader, pReader->aBlockIdx, NULL);
      if (code) goto _err;

      pReader->iBlockIdx = 0;
      pReader->pBlockIdx = NULL;
    }

    while (true) {
      if (pReader->pBlockIdx == NULL) {
        if (pReader->iBlockIdx >= taosArrayGetSize(pReader->aBlockIdx)) {
          tsdbDataFReaderClose(&pReader->pDataFReader);
          break;
        }

        pReader->pBlockIdx = (SBlockIdx*)taosArrayGet(pReader->aBlockIdx, pReader->iBlockIdx);
        pReader->iBlockIdx++;

        // SBlock
        code = tsdbReadBlock(pReader->pDataFReader, pReader->pBlockIdx, &pReader->mBlock, NULL);
        if (code) goto _err;

        pReader->iBlock = 0;
      }

      while (true) {
        SBlock  block;
        SBlock* pBlock = &block;

        if (pReader->iBlock >= pReader->mBlock.nItem) {
          pReader->pBlockIdx = NULL;
          break;
        }

        tMapDataGetItemByIdx(&pReader->mBlock, pReader->iBlock, pBlock, tGetBlock);
        pReader->iBlock++;

        if ((pBlock->minVersion >= pReader->sver && pBlock->minVersion <= pReader->ever) ||
            (pBlock->maxVersion >= pReader->sver && pBlock->maxVersion <= pReader->ever)) {
          // overlap (todo)

          code = tsdbReadBlockData(pReader->pDataFReader, pReader->pBlockIdx, pBlock, &pReader->blkData, NULL, NULL);
          if (code) goto _err;

          goto _exit;
        }
      }
    }
  }

_exit:
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
    int32_t  size = 0;

    pReader->iDelIdx++;

    code = tsdbReadDelData(pReader->pDelFReader, pDelIdx, pReader->aDelData, NULL);
    if (code) goto _err;

    for (int32_t iDelData = 0; iDelData < taosArrayGetSize(pReader->aDelData); iDelData++) {
      SDelData* pDelData = (SDelData*)taosArrayGet(pReader->aDelData, iDelData);

      if (pDelData->version >= pReader->sver && pDelData->version <= pReader->ever) {
        size += tPutDelData(NULL, pDelData);
      }
    }

    if (size > 0) {
      int64_t n = 0;

      size = size + sizeof(SSnapDataHdr) + sizeof(TABLEID);
      code = tRealloc(ppData, size);
      if (code) goto _err;

      // SSnapDataHdr
      SSnapDataHdr* pSnapDataHdr = (SSnapDataHdr*)(*ppData + n);
      pSnapDataHdr->type = 1;
      pSnapDataHdr->size = size;  // TODO: size here may incorrect
      n += sizeof(SSnapDataHdr);

      // TABLEID
      TABLEID* pId = (TABLEID*)(*ppData + n);
      pId->suid = pDelIdx->suid;
      pId->uid = pDelIdx->uid;
      n += sizeof(*pId);

      // DATA
      for (int32_t iDelData = 0; iDelData < taosArrayGetSize(pReader->aDelData); iDelData++) {
        SDelData* pDelData = (SDelData*)taosArrayGet(pReader->aDelData, iDelData);

        if (pDelData->version >= pReader->sver && pDelData->version <= pReader->ever) {
          n += tPutDelData(*ppData + n, pDelData);
        }
      }

      goto _exit;
    }
  }

  code = TSDB_CODE_VND_READ_END;
  tsdbDelFReaderClose(&pReader->pDelFReader);

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
  taosArrayDestroy(pReader->aBlockIdx);
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

// STsdbSnapWriter ========================================
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

static int32_t tsdbSnapRollback(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbSnapCommit(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSnapWriterOpen(STsdb* pTsdb, int64_t sver, int64_t ever, STsdbSnapWriter** ppWriter) {
  int32_t          code = 0;
  STsdbSnapWriter* pWriter = NULL;

  // alloc
  pWriter = (STsdbSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pTsdb = pTsdb;
  pWriter->sver = sver;
  pWriter->ever = ever;

  *ppWriter = pWriter;
  return code;

_err:
  tsdbError("vgId:%d tsdb snapshot writer open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t tsdbSnapWriterClose(STsdbSnapWriter** ppWriter, int8_t rollback) {
  int32_t          code = 0;
  STsdbSnapWriter* pWriter = *ppWriter;

  if (rollback) {
    code = tsdbSnapRollback(pWriter);
    if (code) goto _err;
  } else {
    code = tsdbSnapCommit(pWriter);
    if (code) goto _err;
  }

  taosMemoryFree(pWriter);
  *ppWriter = NULL;

  return code;

_err:
  tsdbError("vgId:%d tsdb snapshot writer close failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbSnapWrite(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;
  // TODO
  return code;
}
