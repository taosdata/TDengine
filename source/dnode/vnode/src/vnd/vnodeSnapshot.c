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

  if (pReader->pTsdbReader) {
    tsdbSnapReaderClose(&pReader->pTsdbReader);
  }

  if (pReader->pMetaReader) {
    metaSnapReaderClose(&pReader->pMetaReader);
  }

  vInfo("vgId:%d vnode snapshot reader closed", TD_VID(pReader->pVnode));
  taosMemoryFree(pReader);
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
      }
    }
  }

  // TSDB ==============
  if (!pReader->tsdbDone) {
    // open if not
    if (pReader->pTsdbReader == NULL) {
      code = tsdbSnapReaderOpen(pReader->pVnode->pTsdb, pReader->sver, pReader->ever, &pReader->pTsdbReader);
      if (code) goto _err;
    }

    code = tsdbSnapRead(pReader->pTsdbReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->tsdbDone = 1;
        code = tsdbSnapReaderClose(&pReader->pTsdbReader);
        if (code) goto _err;
      }
    }
  }

  *ppData = NULL;
  *nData = 0;

_exit:
  if (*ppData) {
    SSnapDataHdr *pHdr = (SSnapDataHdr *)(*ppData);

    pReader->index++;
    *nData = sizeof(SSnapDataHdr) + pHdr->size;
    pHdr->index = pReader->index;
    vInfo("vgId:%d vnode snapshot read data,index:%" PRId64 " type:%d nData:%d ", TD_VID(pReader->pVnode),
          pReader->index, pHdr->type, *nData);
  } else {
    vInfo("vgId:%d vnode snapshot read data end, index:%" PRId64, TD_VID(pReader->pVnode), pReader->index);
  }
  return code;

_err:
  vError("vgId:% vnode snapshot read failed since %s", TD_VID(pReader->pVnode), tstrerror(code));
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
  SVnode *pVnode = pWriter->pVnode;

  if (pWriter->pMetaSnapWriter) {
    code = metaSnapWriterClose(&pWriter->pMetaSnapWriter, rollback);
    if (code) goto _err;
  }

  if (pWriter->pTsdbSnapWriter) {
    code = tsdbSnapWriterClose(&pWriter->pTsdbSnapWriter, rollback);
    if (code) goto _err;
  }

  if (!rollback) {
    SVnodeInfo info = {0};
    char       dir[TSDB_FILENAME_LEN];

    pVnode->state.committed = pWriter->ever;
    pVnode->state.applied = pWriter->ever;
    // pVnode->state.applyTerm = ;
    // pVnode->state.commitTerm = ;

    info.config = pVnode->config;
    info.state.committed = pVnode->state.applied;
    info.state.commitTerm = pVnode->state.applyTerm;
    info.state.commitID = pVnode->state.commitID;
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
    code = vnodeSaveInfo(dir, &info);
    if (code) goto _err;

    code = vnodeCommitInfo(dir, &info);
    if (code) goto _err;
  } else {
    ASSERT(0);
  }

_exit:
  vInfo("vgId:%d vnode snapshot writer closed, rollback:%d", TD_VID(pVnode), rollback);
  taosMemoryFree(pWriter);
  return code;

_err:
  vError("vgId:%d vnode snapshot writer close failed since %s", TD_VID(pWriter->pVnode), tstrerror(code));
  return code;
}

int32_t vnodeSnapWrite(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t       code = 0;
  SSnapDataHdr *pHdr = (SSnapDataHdr *)pData;
  SVnode       *pVnode = pWriter->pVnode;

  ASSERT(pHdr->size + sizeof(SSnapDataHdr) == nData);
  ASSERT(pHdr->index == pWriter->index + 1);
  pWriter->index = pHdr->index;

  vInfo("vgId:%d vnode snapshot write data, index:%" PRId64 " type:%d nData:%d", TD_VID(pVnode), pHdr->index,
        pHdr->type, nData);

  if (pHdr->type == 0) {
    // meta

    if (pWriter->pMetaSnapWriter == NULL) {
      code = metaSnapWriterOpen(pVnode->pMeta, pWriter->sver, pWriter->ever, &pWriter->pMetaSnapWriter);
      if (code) goto _err;
    }

    code = metaSnapWrite(pWriter->pMetaSnapWriter, pData, nData);
    if (code) goto _err;
  } else {
    // tsdb

    if (pWriter->pTsdbSnapWriter == NULL) {
      code = tsdbSnapWriterOpen(pVnode->pTsdb, pWriter->sver, pWriter->ever, &pWriter->pTsdbSnapWriter);
      if (code) goto _err;
    }

    code = tsdbSnapWrite(pWriter->pTsdbSnapWriter, pData, nData);
    if (code) goto _err;
  }

_exit:
  return code;

_err:
  vError("vgId:%d vnode snapshot write failed since %s, index:%" PRId64 " type:%d nData:%d", TD_VID(pVnode),
         tstrerror(code), pHdr->index, pHdr->type, nData);
  return code;
}