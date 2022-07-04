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

#include "vnodeInt.h"

struct SVSnapReader {
  SVnode *pVnode;
  int64_t sver;
  int64_t ever;
  // meta
  int8_t           metaDone;
  SMetaSnapReader *pMetaReader;
  // tsdb
  int8_t           tsdbDone;
  STsdbSnapReader *pTsdbReader;
  void            *pData;
  int32_t          nData;
};

struct SVSnapWriter {
  SVnode *pVnode;
  int64_t sver;
  int64_t ever;
};

// SVSnapReader ========================================================
int32_t vnodeSnapReaderOpen(SVnode *pVnode, int64_t sver, int64_t ever, SVSnapReader **ppReader) {
  int32_t       code = 0;
  SVSnapReader *pReader = NULL;

  pReader = (SVSnapReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pVnode = pVnode;
  pReader->sver = sver;
  pReader->ever = ever;

  if (metaSnapReaderOpen(pVnode->pMeta, &pReader->pMetaReader, sver, ever) < 0) {
    taosMemoryFree(pReader);
    goto _err;
  }

  if (tsdbSnapReaderOpen(pVnode->pTsdb, &pReader->pTsdbReader, sver, ever) < 0) {
    metaSnapReaderClose(pReader->pMetaReader);
    taosMemoryFree(pReader);
    goto _err;
  }

  *ppReader = pReader;
  return code;

_err:
  *ppReader = NULL;
  return code;
}

int32_t vnodeSnapReaderClose(SVSnapReader *pReader) {
  vnodeFree(pReader->pData);
  tsdbSnapReaderClose(pReader->pTsdbReader);
  metaSnapReaderClose(pReader->pMetaReader);
  taosMemoryFree(pReader);

  return 0;
}

int32_t vnodeSnapRead(SVSnapReader *pReader, const void **ppData, uint32_t *nData) {
  int32_t code = 0;

  if (!pReader->metaDone) {
    code = metaSnapRead(pReader->pMetaReader, &pReader->pData, &pReader->nData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->metaDone = 1;
      } else {
        return code;
      }
    } else {
      *ppData = pReader->pData;
      *nData = pReader->nData;
      return code;
    }
  }

  if (!pReader->tsdbDone) {
    code = tsdbSnapRead(pReader->pTsdbReader, &pReader->pData, &pReader->nData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->tsdbDone = 1;
      } else {
        return code;
      }
    } else {
      *ppData = pReader->pData;
      *nData = pReader->nData;
      return code;
    }
  }

  code = TSDB_CODE_VND_READ_END;
  return code;
}

// SVSnapWriter ========================================================
int32_t vnodeSnapshotWriterOpen(SVnode *pVnode, int64_t sver, int64_t ever, SVSnapWriter **ppWriter) {
  int32_t       code = 0;
  SVSnapWriter *pWriter = NULL;

  // alloc
  pWriter = (SVSnapWriter *)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pVnode = pVnode;
  pWriter->sver = sver;
  pWriter->ever = ever;

  return code;

_err:
  return code;
}

int32_t vnodeSnapshotWriterClose(SVSnapWriter *pWriter, int8_t rollback) {
  int32_t code = 0;

  if (!rollback) {
    // apply the change
  } else {
    // rollback the change
  }

  taosMemoryFree(pWriter);
  return code;
}

int32_t vnodeSnapshotWrite(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;
  // TODO
  return code;
}