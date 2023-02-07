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
  // config
  int8_t cfgDone;
  // meta
  int8_t           metaDone;
  SMetaSnapReader *pMetaReader;
  // tsdb
  int8_t           tsdbDone;
  STsdbSnapReader *pTsdbReader;
  // tq
  int8_t           tqHandleDone;
  STqSnapReader   *pTqSnapReader;
  int8_t           tqOffsetDone;
  STqOffsetReader *pTqOffsetReader;
  // stream
  int8_t              streamTaskDone;
  SStreamTaskReader  *pStreamTaskReader;
  int8_t              streamStateDone;
  SStreamStateReader *pStreamStateReader;
  // rsma
  int8_t           rsmaDone;
  SRSmaSnapReader *pRsmaReader;
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

  vInfo("vgId:%d, vnode snapshot reader opened, sver:%" PRId64 " ever:%" PRId64, TD_VID(pVnode), sver, ever);
  *ppReader = pReader;
  return code;

_err:
  vError("vgId:%d, vnode snapshot reader open failed since %s", TD_VID(pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

void vnodeSnapReaderClose(SVSnapReader *pReader) {
  vInfo("vgId:%d, close vnode snapshot reader", TD_VID(pReader->pVnode));
  if (pReader->pRsmaReader) {
    rsmaSnapReaderClose(&pReader->pRsmaReader);
  }

  if (pReader->pTsdbReader) {
    tsdbSnapReaderClose(&pReader->pTsdbReader);
  }

  if (pReader->pMetaReader) {
    metaSnapReaderClose(&pReader->pMetaReader);
  }

  taosMemoryFree(pReader);
}

int32_t vnodeSnapRead(SVSnapReader *pReader, uint8_t **ppData, uint32_t *nData) {
  int32_t code = 0;

  // CONFIG ==============
  // FIXME: if commit multiple times and the config changed?
  if (!pReader->cfgDone) {
    char fName[TSDB_FILENAME_LEN];
    if (pReader->pVnode->pTfs) {
      snprintf(fName, TSDB_FILENAME_LEN, "%s%s%s%s%s", tfsGetPrimaryPath(pReader->pVnode->pTfs), TD_DIRSEP,
               pReader->pVnode->path, TD_DIRSEP, VND_INFO_FNAME);
    } else {
      snprintf(fName, TSDB_FILENAME_LEN, "%s%s%s", pReader->pVnode->path, TD_DIRSEP, VND_INFO_FNAME);
    }

    TdFilePtr pFile = taosOpenFile(fName, TD_FILE_READ);
    if (NULL == pFile) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    int64_t size;
    if (taosFStatFile(pFile, &size, NULL) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      taosCloseFile(&pFile);
      goto _err;
    }

    *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + size + 1);
    if (*ppData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      taosCloseFile(&pFile);
      goto _err;
    }
    ((SSnapDataHdr *)(*ppData))->type = SNAP_DATA_CFG;
    ((SSnapDataHdr *)(*ppData))->size = size + 1;
    ((SSnapDataHdr *)(*ppData))->data[size] = '\0';

    if (taosReadFile(pFile, ((SSnapDataHdr *)(*ppData))->data, size) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      taosMemoryFree(*ppData);
      taosCloseFile(&pFile);
      goto _err;
    }

    taosCloseFile(&pFile);

    pReader->cfgDone = 1;
    goto _exit;
  }

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
      code = tsdbSnapReaderOpen(pReader->pVnode->pTsdb, pReader->sver, pReader->ever, SNAP_DATA_TSDB,
                                &pReader->pTsdbReader);
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

  // TQ ================
  if (!pReader->tqHandleDone) {
    if (pReader->pTqSnapReader == NULL) {
      code = tqSnapReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->ever, &pReader->pTqSnapReader);
      if (code < 0) goto _err;
    }

    code = tqSnapRead(pReader->pTqSnapReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->tqHandleDone = 1;
        code = tqSnapReaderClose(&pReader->pTqSnapReader);
        if (code) goto _err;
      }
    }
  }
  if (!pReader->tqOffsetDone) {
    if (pReader->pTqOffsetReader == NULL) {
      code = tqOffsetReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->ever, &pReader->pTqOffsetReader);
      if (code < 0) goto _err;
    }

    code = tqOffsetSnapRead(pReader->pTqOffsetReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->tqOffsetDone = 1;
        code = tqOffsetReaderClose(&pReader->pTqOffsetReader);
        if (code) goto _err;
      }
    }
  }

  // STREAM ============
  if (!pReader->streamTaskDone) {
  }
  if (!pReader->streamStateDone) {
  }

  // RSMA ==============
  if (VND_IS_RSMA(pReader->pVnode) && !pReader->rsmaDone) {
    // open if not
    if (pReader->pRsmaReader == NULL) {
      code = rsmaSnapReaderOpen(pReader->pVnode->pSma, pReader->sver, pReader->ever, &pReader->pRsmaReader);
      if (code) goto _err;
    }

    code = rsmaSnapRead(pReader->pRsmaReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->rsmaDone = 1;
        code = rsmaSnapReaderClose(&pReader->pRsmaReader);
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
    vDebug("vgId:%d, vnode snapshot read data, index:%" PRId64 " type:%d blockLen:%d ", TD_VID(pReader->pVnode),
           pReader->index, pHdr->type, *nData);
  } else {
    vInfo("vgId:%d, vnode snapshot read data end, index:%" PRId64, TD_VID(pReader->pVnode), pReader->index);
  }
  return code;

_err:
  vError("vgId:%d, vnode snapshot read failed since %s", TD_VID(pReader->pVnode), tstrerror(code));
  return code;
}

// SVSnapWriter ========================================================
struct SVSnapWriter {
  SVnode *pVnode;
  int64_t sver;
  int64_t ever;
  int64_t commitID;
  int64_t index;
  // config
  SVnodeInfo info;
  // meta
  SMetaSnapWriter *pMetaSnapWriter;
  // tsdb
  STsdbSnapWriter *pTsdbSnapWriter;
  // tq
  STqSnapWriter   *pTqSnapWriter;
  STqOffsetWriter *pTqOffsetWriter;
  // stream
  SStreamTaskWriter  *pStreamTaskWriter;
  SStreamStateWriter *pStreamStateWriter;
  // rsma
  SRSmaSnapWriter *pRsmaSnapWriter;
};

int32_t vnodeSnapWriterOpen(SVnode *pVnode, int64_t sver, int64_t ever, SVSnapWriter **ppWriter) {
  int32_t       code = 0;
  SVSnapWriter *pWriter = NULL;

  // commit memory data
  vnodeAsyncCommit(pVnode);
  tsem_wait(&pVnode->canCommit);

  // alloc
  pWriter = (SVSnapWriter *)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pVnode = pVnode;
  pWriter->sver = sver;
  pWriter->ever = ever;

  // inc commit ID
  pWriter->commitID = ++pVnode->state.commitID;

  vInfo("vgId:%d, vnode snapshot writer opened, sver:%" PRId64 " ever:%" PRId64 " commit id:%" PRId64, TD_VID(pVnode),
        sver, ever, pWriter->commitID);
  *ppWriter = pWriter;
  return code;

_err:
  vError("vgId:%d, vnode snapshot writer open failed since %s", TD_VID(pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

int32_t vnodeSnapWriterClose(SVSnapWriter *pWriter, int8_t rollback, SSnapshot *pSnapshot) {
  int32_t code = 0;
  SVnode *pVnode = pWriter->pVnode;

  // prepare
  if (pWriter->pTsdbSnapWriter) {
    tsdbSnapWriterPrepareClose(pWriter->pTsdbSnapWriter);
  }

  // commit json
  if (!rollback) {
    pVnode->config = pWriter->info.config;
    pVnode->state = (SVState){.committed = pWriter->info.state.committed,
                              .applied = pWriter->info.state.committed,
                              .commitID = pWriter->commitID,
                              .commitTerm = pWriter->info.state.commitTerm,
                              .applyTerm = pWriter->info.state.commitTerm};
    pVnode->statis = pWriter->info.statis;
    char dir[TSDB_FILENAME_LEN] = {0};
    if (pWriter->pVnode->pTfs) {
      snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
    } else {
      snprintf(dir, TSDB_FILENAME_LEN, "%s", pWriter->pVnode->path);
    }

    vnodeCommitInfo(dir, &pWriter->info);
  } else {
    vnodeRollback(pWriter->pVnode);
  }

  // commit/rollback sub-system
  if (pWriter->pMetaSnapWriter) {
    code = metaSnapWriterClose(&pWriter->pMetaSnapWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTsdbSnapWriter) {
    code = tsdbSnapWriterClose(&pWriter->pTsdbSnapWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pRsmaSnapWriter) {
    code = rsmaSnapWriterClose(&pWriter->pRsmaSnapWriter, rollback);
    if (code) goto _exit;
  }

  vnodeBegin(pVnode);

_exit:
  if (code) {
    vError("vgId:%d, vnode snapshot writer close failed since %s", TD_VID(pWriter->pVnode), tstrerror(code));
  } else {
    vInfo("vgId:%d, vnode snapshot writer closed, rollback:%d", TD_VID(pVnode), rollback);
    taosMemoryFree(pWriter);
  }
  tsem_post(&pVnode->canCommit);
  return code;
}

static int32_t vnodeSnapWriteInfo(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t code = 0;

  SSnapDataHdr *pHdr = (SSnapDataHdr *)pData;

  // decode info
  if (vnodeDecodeInfo(pHdr->data, &pWriter->info) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  // change some value
  pWriter->info.state.commitID = pWriter->commitID;

  // modify info as needed
  char dir[TSDB_FILENAME_LEN] = {0};
  if (pWriter->pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pWriter->pVnode->pTfs), TD_DIRSEP,
             pWriter->pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pWriter->pVnode->path);
  }

  SVnodeStats vndStats = pWriter->info.config.vndStats;
  SVnode     *pVnode = pWriter->pVnode;
  pWriter->info.config = pVnode->config;
  pWriter->info.config.vndStats = vndStats;
  vDebug("vgId:%d, save config while write snapshot", pWriter->pVnode->config.vgId);
  if (vnodeSaveInfo(dir, &pWriter->info) < 0) {
    code = terrno;
    goto _exit;
  }

_exit:
  return code;
}

int32_t vnodeSnapWrite(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t       code = 0;
  SSnapDataHdr *pHdr = (SSnapDataHdr *)pData;
  SVnode       *pVnode = pWriter->pVnode;

  ASSERT(pHdr->size + sizeof(SSnapDataHdr) == nData);

  if (pHdr->index != pWriter->index + 1) {
    vError("vgId:%d, unexpected vnode snapshot msg. index:%" PRId64 ", expected index:%" PRId64, TD_VID(pVnode),
           pHdr->index, pWriter->index + 1);
    return -1;
  }

  pWriter->index = pHdr->index;

  vDebug("vgId:%d, vnode snapshot write data, index:%" PRId64 " type:%d blockLen:%d", TD_VID(pVnode), pHdr->index,
         pHdr->type, nData);

  switch (pHdr->type) {
    case SNAP_DATA_CFG: {
      code = vnodeSnapWriteInfo(pWriter, pData, nData);
      if (code) goto _err;
    } break;
    case SNAP_DATA_META: {
      // meta
      if (pWriter->pMetaSnapWriter == NULL) {
        code = metaSnapWriterOpen(pVnode->pMeta, pWriter->sver, pWriter->ever, &pWriter->pMetaSnapWriter);
        if (code) goto _err;
      }

      code = metaSnapWrite(pWriter->pMetaSnapWriter, pData, nData);
      if (code) goto _err;
    } break;
    case SNAP_DATA_TSDB:
    case SNAP_DATA_DEL: {
      // tsdb
      if (pWriter->pTsdbSnapWriter == NULL) {
        code = tsdbSnapWriterOpen(pVnode->pTsdb, pWriter->sver, pWriter->ever, &pWriter->pTsdbSnapWriter);
        if (code) goto _err;
      }

      code = tsdbSnapWrite(pWriter->pTsdbSnapWriter, pHdr);
      if (code) goto _err;
    } break;
    case SNAP_DATA_TQ_HANDLE: {
    } break;
    case SNAP_DATA_TQ_OFFSET: {
    } break;
    case SNAP_DATA_STREAM_TASK: {
    } break;
    case SNAP_DATA_STREAM_STATE: {
    } break;
    case SNAP_DATA_RSMA1:
    case SNAP_DATA_RSMA2:
    case SNAP_DATA_QTASK: {
      // rsma1/rsma2/qtask for rsma
      if (pWriter->pRsmaSnapWriter == NULL) {
        code = rsmaSnapWriterOpen(pVnode->pSma, pWriter->sver, pWriter->ever, &pWriter->pRsmaSnapWriter);
        if (code) goto _err;
      }

      code = rsmaSnapWrite(pWriter->pRsmaSnapWriter, pData, nData);
      if (code) goto _err;
    } break;
    default:
      break;
  }
_exit:
  return code;

_err:
  vError("vgId:%d, vnode snapshot write failed since %s, index:%" PRId64 " type:%d nData:%d", TD_VID(pVnode),
         tstrerror(code), pHdr->index, pHdr->type, nData);
  return code;
}
