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

#include "vnd.h"

// SVSnapReader ========================================================
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
  uint8_t         *pData;
};

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

  code = metaSnapReaderOpen(pVnode->pMeta, sver, ever, &pReader->pMetaReader);
  if (code) goto _err;

  code = tsdbSnapReaderOpen(pVnode->pTsdb, sver, ever, &pReader->pTsdbReader);
  if (code) goto _err;

  *ppReader = pReader;
  return code;

_err:
  vError("vgId:%d vnode snapshot reader open failed since %s", TD_VID(pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t vnodeSnapReaderClose(SVSnapReader *pReader) {
  int32_t code = 0;

  tFree(pReader->pData);
  if (pReader->pTsdbReader) tsdbSnapReaderClose(&pReader->pTsdbReader);
  if (pReader->pMetaReader) metaSnapReaderClose(&pReader->pMetaReader);
  taosMemoryFree(pReader);

  return code;
}

int32_t vnodeSnapRead(SVSnapReader *pReader, uint8_t **ppData, uint32_t *nData) {
  int32_t code = 0;

  if (!pReader->metaDone) {
    code = metaSnapRead(pReader->pMetaReader, &pReader->pData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->metaDone = 1;
      } else {
        goto _err;
      }
    } else {
      *ppData = pReader->pData;
      *nData = sizeof(SSnapDataHdr) + ((SSnapDataHdr *)pReader->pData)->size;
      goto _exit;
    }
  }

  if (!pReader->tsdbDone) {
    code = tsdbSnapRead(pReader->pTsdbReader, &pReader->pData);
    if (code) {
      if (code == TSDB_CODE_VND_READ_END) {
        pReader->tsdbDone = 1;
      } else {
        goto _err;
      }
    } else {
      *ppData = pReader->pData;
      *nData = sizeof(SSnapDataHdr) + ((SSnapDataHdr *)pReader->pData)->size;
      goto _exit;
    }
  }

  code = TSDB_CODE_VND_READ_END;

_exit:
  return code;

_err:
  vError("vgId:% snapshot read failed since %s", TD_VID(pReader->pVnode), tstrerror(code));
  return code;
}

// SVSnapWriter ========================================================
struct SVSnapWriter {
  SVnode *pVnode;
  int64_t sver;
  int64_t ever;
  // meta
  SMetaSnapWriter *pMetaSnapWriter;
  // tsdb
  STsdbSnapWriter *pTsdbSnapWriter;
};

static int32_t vnodeSnapRollback(SVSnapWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t vnodeSnapCommit(SVSnapWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t vnodeSnapWriterOpen(SVnode *pVnode, int64_t sver, int64_t ever, SVSnapWriter **ppWriter) {
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
  vError("vgId:%d vnode snapshot writer open failed since %s", TD_VID(pVnode), tstrerror(code));
  return code;
}

int32_t vnodeSnapWriterClose(SVSnapWriter *pWriter, int8_t rollback) {
  int32_t code = 0;

  if (rollback) {
    code = vnodeSnapRollback(pWriter);
    if (code) goto _err;
  } else {
    code = vnodeSnapCommit(pWriter);
    if (code) goto _err;
  }

  taosMemoryFree(pWriter);
  return code;

_err:
  vError("vgId:%d vnode snapshow writer close failed since %s", TD_VID(pWriter->pVnode), tstrerror(code));
  return code;
}

int32_t vnodeSnapWrite(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t       code = 0;
  SSnapDataHdr *pSnapDataHdr = (SSnapDataHdr *)pData;
  SVnode       *pVnode = pWriter->pVnode;

  ASSERT(pSnapDataHdr->size + sizeof(SSnapDataHdr) == nData);

  if (pSnapDataHdr->type == 0) {
    // meta
    if (pWriter->pMetaSnapWriter == NULL) {
      code = metaSnapWriterOpen(pVnode->pMeta, pWriter->sver, pWriter->ever, &pWriter->pMetaSnapWriter);
      if (code) goto _err;
    }

    code = metaSnapWrite(pWriter->pMetaSnapWriter, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
    if (code) goto _err;
  } else {
    // tsdb
    if (pWriter->pTsdbSnapWriter == NULL) {
      code = tsdbSnapWriterOpen(pVnode->pTsdb, pWriter->sver, pWriter->ever, &pWriter->pTsdbSnapWriter);
      if (code) goto _err;
    }

    code = tsdbSnapWrite(pWriter->pTsdbSnapWriter, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
    if (code) goto _err;
  }

  return code;

_err:
  vError("vgId:%d vnode snapshot write failed since %s", TD_VID(pVnode), tstrerror(code));
  return code;
}