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
  int64_t index;
  // meta
  int8_t           metaDone;
  SMetaSnapReader *pMetaReader;
  // tsdb
  int8_t           tsdbDone;
  STsdbSnapReader *pTsdbReader;
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

  vInfo("vgId:%d vnode snapshot reader opened, sver:%" PRId64 " ever:%" PRId64, TD_VID(pVnode), sver, ever);
  *ppReader = pReader;
  return code;

_err:
  vError("vgId:%d vnode snapshot reader open failed since %s", TD_VID(pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t vnodeSnapReaderClose(SVSnapReader *pReader) {
  int32_t code = 0;

  // tFree(pReader->pData);
  // if (pReader->pTsdbReader) tsdbSnapReaderClose(&pReader->pTsdbReader);
  // if (pReader->pMetaReader) metaSnapReaderClose(&pReader->pMetaReader);
  // taosMemoryFree(pReader);

  vInfo("vgId:%d vnode snapshot reader closed", TD_VID(pReader->pVnode));
  return code;
}

int32_t vnodeSnapRead(SVSnapReader *pReader, uint8_t **ppData, uint32_t *nData) {
  int32_t code = 0;

  // META ==============
  if (!pReader->metaDone) {
    // open reader if not
    if (pReader->pMetaReader == NULL) {
      code = metaSnapReaderOpen(pReader->pVnode->pMeta, pReader->sver, pReader->ever, &pReader->pMetaReader);
      if (code) goto _err;
    }

    code = metaSnapRead(pReader->pMetaReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->metaDone = 1;
        code = metaSnapReaderClose(&pReader->pMetaReader);
        if (code) goto _err;

        vInfo("vgId:%d vnode snapshot meta data read end, index:%" PRId64, TD_VID(pReader->pVnode), pReader->index);
      }
    }
  }

  // TSDB ==============
  if (!pReader->tsdbDone) {
    // open if not
    // if (pReader->pTsdbReader == NULL) {
    //   code = tsdbSnapReaderOpen(pReader->pVnode->pTsdb, pReader->sver, pReader->ever, &pReader->pTsdbReader);
    //   if (code) goto _err;
    // }

    // code = tsdbSnapRead(pReader->pTsdbReader, &pReader->pData);
    // if (code) {
    //   if (code == TSDB_CODE_VND_READ_END) {
    //     pReader->tsdbDone = 1;
    //   } else {
    //     goto _err;
    //   }
    // } else {
    //   *ppData = pReader->pData;
    //   *nData = sizeof(SSnapDataHdr) + ((SSnapDataHdr *)pReader->pData)->size;
    //   goto _exit;
    // }
  }

  *ppData = NULL;
  *nData = 0;

_exit:
  if (*ppData) {
    pReader->index++;
    ((SSnapDataHdr *)(*ppData))->index = pReader->index;
    vInfo("vgId:%d vnode snapshot read data, index:%" PRId64, TD_VID(pReader->pVnode), pReader->index);
  } else {
    vInfo("vgId:%d vnode snapshot read data end, index:%" PRId64, TD_VID(pReader->pVnode), pReader->index);
  }
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
  int64_t index;
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

  vInfo("vgId:%d vnode snapshot writer opened", TD_VID(pVnode));

  *ppWriter = pWriter;
  return code;

_err:
  vError("vgId:%d vnode snapshot writer open failed since %s", TD_VID(pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t vnodeSnapWriterClose(SVSnapWriter *pWriter, int8_t rollback) {
  int32_t code = 0;

  goto _exit;

  if (rollback) {
    code = vnodeSnapRollback(pWriter);
    if (code) goto _err;
  } else {
    code = vnodeSnapCommit(pWriter);
    if (code) goto _err;
  }

_exit:
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

  goto _exit;

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

_exit:
  return code;

_err:
  vError("vgId:%d vnode snapshot write failed since %s", TD_VID(pVnode), tstrerror(code));
  return code;
}