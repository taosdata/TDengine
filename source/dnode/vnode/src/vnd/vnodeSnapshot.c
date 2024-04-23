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
#include "vnd.h"

static int32_t vnodeExtractSnapInfoDiff(void *buf, int32_t bufLen, TFileSetRangeArray **ppRanges) {
  int32_t            code = -1;
  STsdbFSetPartList *pList = tsdbFSetPartListCreate();
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _out;
  }
  if (tDeserializeTsdbFSetPartList(buf, bufLen, pList) < 0) {
    terrno = TSDB_CODE_INVALID_DATA_FMT;
    goto _out;
  }
  if (tsdbFSetPartListToRangeDiff(pList, ppRanges) < 0) {
    goto _out;
  }
  code = 0;
_out:
  tsdbFSetPartListDestroy(&pList);
  return code;
}

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
  TFileSetRangeArray *pRanges;
  STsdbSnapReader *pTsdbReader;
  // tsdb raw
  int8_t              tsdbRAWDone;
  STsdbSnapRAWReader *pTsdbRAWReader;

  // tq
  int8_t              tqHandleDone;
  STqSnapReader      *pTqSnapReader;
  int8_t              tqOffsetDone;
  STqOffsetReader    *pTqOffsetReader;
  int8_t              tqCheckInfoDone;
  STqCheckInfoReader *pTqCheckInfoReader;
  // stream
  int8_t              streamTaskDone;
  SStreamTaskReader  *pStreamTaskReader;
  int8_t              streamStateDone;
  SStreamStateReader *pStreamStateReader;
  // rsma
  int8_t           rsmaDone;
  TFileSetRangeArray *pRsmaRanges[TSDB_RETENTION_L2];
  SRSmaSnapReader *pRsmaReader;
};

static TFileSetRangeArray **vnodeSnapReaderGetTsdbRanges(SVSnapReader *pReader, int32_t tsdbTyp) {
  ASSERTS(sizeof(pReader->pRsmaRanges) / sizeof(pReader->pRsmaRanges[0]) == 2, "Unexpected array size");
  switch (tsdbTyp) {
    case SNAP_DATA_TSDB:
      return &pReader->pRanges;
    case SNAP_DATA_RSMA1:
      return &pReader->pRsmaRanges[0];
    case SNAP_DATA_RSMA2:
      return &pReader->pRsmaRanges[1];
    default:
      return NULL;
  }
}

static int32_t vnodeSnapReaderDealWithSnapInfo(SVSnapReader *pReader, SSnapshotParam *pParam) {
  SVnode *pVnode = pReader->pVnode;
  int32_t code = -1;

  if (pParam->data) {
    // decode
    SSyncTLV *datHead = (void *)pParam->data;
    if (datHead->typ != TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
      terrno = TSDB_CODE_INVALID_DATA_FMT;
      goto _out;
    }

    STsdbRepOpts         tsdbOpts = {0};
    TFileSetRangeArray **ppRanges = NULL;
    int32_t              offset = 0;

    while (offset + sizeof(SSyncTLV) < datHead->len) {
      SSyncTLV *subField = (void *)(datHead->val + offset);
      offset += sizeof(SSyncTLV) + subField->len;
      void   *buf = subField->val;
      int32_t bufLen = subField->len;

      switch (subField->typ) {
        case SNAP_DATA_TSDB:
        case SNAP_DATA_RSMA1:
        case SNAP_DATA_RSMA2: {
          ppRanges = vnodeSnapReaderGetTsdbRanges(pReader, subField->typ);
          if (ppRanges == NULL) {
            vError("vgId:%d, unexpected subfield type in snapshot param. subtyp:%d", TD_VID(pVnode), subField->typ);
            goto _out;
          }
          if (vnodeExtractSnapInfoDiff(buf, bufLen, ppRanges) < 0) {
            vError("vgId:%d, failed to get range diff since %s", TD_VID(pVnode), terrstr());
            goto _out;
          }
        } break;
        case SNAP_DATA_RAW: {
          if (tDeserializeTsdbRepOpts(buf, bufLen, &tsdbOpts) < 0) {
            vError("vgId:%d, failed to deserialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
            goto _out;
          }
        } break;
        default:
          vError("vgId:%d, unexpected subfield type of snap info. typ:%d", TD_VID(pVnode), subField->typ);
          goto _out;
      }
    }

    // toggle snap replication mode
    vInfo("vgId:%d, vnode snap reader supported tsdb rep of format:%d", TD_VID(pVnode), tsdbOpts.format);
    if (pReader->sver == 0 && tsdbOpts.format == TSDB_SNAP_REP_FMT_RAW) {
      pReader->tsdbDone = true;
    } else {
      pReader->tsdbRAWDone = true;
    }

    ASSERT(pReader->tsdbDone != pReader->tsdbRAWDone);
    vInfo("vgId:%d, vnode snap writer enabled replication mode: %s", TD_VID(pVnode),
          (pReader->tsdbDone ? "raw" : "normal"));
  }
  code = 0;
_out:
  return code;
}

int32_t vnodeSnapReaderOpen(SVnode *pVnode, SSnapshotParam *pParam, SVSnapReader **ppReader) {
  int32_t       code = 0;
  int64_t       sver = pParam->start;
  int64_t       ever = pParam->end;
  SVSnapReader *pReader = NULL;

  pReader = (SVSnapReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pVnode = pVnode;
  pReader->sver = sver;
  pReader->ever = ever;

  // snapshot info
  if (vnodeSnapReaderDealWithSnapInfo(pReader, pParam) < 0) {
    goto _err;
  }

  // open tsdb snapshot raw reader
  if (!pReader->tsdbRAWDone) {
    ASSERT(pReader->sver == 0);
    code = tsdbSnapRAWReaderOpen(pVnode->pTsdb, ever, SNAP_DATA_RAW, &pReader->pTsdbRAWReader);
    if (code) goto _err;
  }

  // check snapshot ever
  SSnapshot snapshot = {0};
  vnodeGetSnapshot(pVnode, &snapshot);
  if (ever != snapshot.lastApplyIndex) {
    vError("vgId:%d, abort reader open due to vnode snapshot changed. ever:%" PRId64 ", commit ver:%" PRId64,
           TD_VID(pVnode), ever, snapshot.lastApplyIndex);
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _err;
  }

  vInfo("vgId:%d, vnode snapshot reader opened, sver:%" PRId64 " ever:%" PRId64, TD_VID(pVnode), sver, ever);
  *ppReader = pReader;
  return code;

_err:
  vError("vgId:%d, vnode snapshot reader open failed since %s", TD_VID(pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

static void vnodeSnapReaderDestroyTsdbRanges(SVSnapReader *pReader) {
  int32_t tsdbTyps[TSDB_RETENTION_MAX] = {SNAP_DATA_TSDB, SNAP_DATA_RSMA1, SNAP_DATA_RSMA2};
  for (int32_t j = 0; j < TSDB_RETENTION_MAX; ++j) {
    TFileSetRangeArray **ppRanges = vnodeSnapReaderGetTsdbRanges(pReader, tsdbTyps[j]);
    if (ppRanges == NULL) continue;
    tsdbTFileSetRangeArrayDestroy(ppRanges);
  }
}

void vnodeSnapReaderClose(SVSnapReader *pReader) {
  vInfo("vgId:%d, close vnode snapshot reader", TD_VID(pReader->pVnode));
  vnodeSnapReaderDestroyTsdbRanges(pReader);

  if (pReader->pRsmaReader) {
    rsmaSnapReaderClose(&pReader->pRsmaReader);
  }

  if (pReader->pTsdbReader) {
    tsdbSnapReaderClose(&pReader->pTsdbReader);
  }

  if (pReader->pTsdbRAWReader) {
    tsdbSnapRAWReaderClose(&pReader->pTsdbRAWReader);
  }

  if (pReader->pMetaReader) {
    metaSnapReaderClose(&pReader->pMetaReader);
  }

  if (pReader->pTqSnapReader) {
    tqSnapReaderClose(&pReader->pTqSnapReader);
  }

  if (pReader->pTqOffsetReader) {
    tqOffsetReaderClose(&pReader->pTqOffsetReader);
  }

  if (pReader->pTqCheckInfoReader) {
    tqCheckInfoReaderClose(&pReader->pTqCheckInfoReader);
  }

  taosMemoryFree(pReader);
}

int32_t vnodeSnapRead(SVSnapReader *pReader, uint8_t **ppData, uint32_t *nData) {
  int32_t code = 0;
  SVnode *pVnode = pReader->pVnode;
  int32_t vgId = TD_VID(pReader->pVnode);

  // CONFIG ==============
  // FIXME: if commit multiple times and the config changed?
  if (!pReader->cfgDone) {
    char    fName[TSDB_FILENAME_LEN];
    int32_t offset = 0;

    vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, fName, TSDB_FILENAME_LEN);
    offset = strlen(fName);
    snprintf(fName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VND_INFO_FNAME);

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
      code = tsdbSnapReaderOpen(pReader->pVnode->pTsdb, pReader->sver, pReader->ever, SNAP_DATA_TSDB, pReader->pRanges,
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

  if (!pReader->tsdbRAWDone) {
    // open if not
    if (pReader->pTsdbRAWReader == NULL) {
      ASSERT(pReader->sver == 0);
      code = tsdbSnapRAWReaderOpen(pReader->pVnode->pTsdb, pReader->ever, SNAP_DATA_RAW, &pReader->pTsdbRAWReader);
      if (code) goto _err;
    }

    code = tsdbSnapRAWRead(pReader->pTsdbRAWReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->tsdbRAWDone = 1;
        code = tsdbSnapRAWReaderClose(&pReader->pTsdbRAWReader);
        if (code) goto _err;
      }
    }
  }

  // TQ ================
  vInfo("vgId:%d tq transform start", vgId);
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
  if (!pReader->tqCheckInfoDone) {
    if (pReader->pTqCheckInfoReader == NULL) {
      code = tqCheckInfoReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->ever, &pReader->pTqCheckInfoReader);
      if (code < 0) goto _err;
    }

    code = tqCheckInfoRead(pReader->pTqCheckInfoReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->tqCheckInfoDone = 1;
        code = tqCheckInfoReaderClose(&pReader->pTqCheckInfoReader);
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
  vInfo("vgId:%d stream task start", vgId);
  if (!pReader->streamTaskDone) {
    if (pReader->pStreamTaskReader == NULL) {
      code = streamTaskSnapReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->sver, &pReader->pStreamTaskReader);
      if (code) {
        vError("vgId:%d open streamtask snapshot reader failed, code:%s", vgId, tstrerror(code));
        goto _err;
      }
    }

    code = streamTaskSnapRead(pReader->pStreamTaskReader, ppData);
    if (code) {
      vError("vgId:%d error happens during read data from streatask snapshot, code:%s", vgId, tstrerror(code));
      goto _err;
    } else {
      if (*ppData) {
        vInfo("vgId:%d no streamTask snapshot", vgId);
        goto _exit;
      } else {
        pReader->streamTaskDone = 1;
        code = streamTaskSnapReaderClose(pReader->pStreamTaskReader);
        if (code) {
          goto _err;
        }
        pReader->pStreamTaskReader = NULL;
      }
    }
  }
  if (!pReader->streamStateDone) {
    if (pReader->pStreamStateReader == NULL) {
      code =
          streamStateSnapReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->sver, &pReader->pStreamStateReader);
      if (code) {
        pReader->streamStateDone = 1;
        pReader->pStreamStateReader = NULL;
        goto _err;
      }
    }
    code = streamStateSnapRead(pReader->pStreamStateReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->streamStateDone = 1;
        code = streamStateSnapReaderClose(pReader->pStreamStateReader);
        if (code) goto _err;
        pReader->pStreamStateReader = NULL;
      }
    }
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
    vDebug("vgId:%d, vnode snapshot read data, index:%" PRId64 " type:%d blockLen:%d ", vgId, pReader->index,
           pHdr->type, *nData);
  } else {
    vInfo("vgId:%d, vnode snapshot read data end, index:%" PRId64, vgId, pReader->index);
  }
  return code;

_err:
  vError("vgId:%d, vnode snapshot read failed since %s", vgId, tstrerror(code));
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
  TFileSetRangeArray *pRanges;
  STsdbSnapWriter *pTsdbSnapWriter;
  // tsdb raw
  STsdbSnapRAWWriter *pTsdbSnapRAWWriter;
  // tq
  STqSnapWriter      *pTqSnapWriter;
  STqOffsetWriter    *pTqOffsetWriter;
  STqCheckInfoWriter *pTqCheckInfoWriter;
  // stream
  SStreamTaskWriter  *pStreamTaskWriter;
  SStreamStateWriter *pStreamStateWriter;
  // rsma
  TFileSetRangeArray *pRsmaRanges[TSDB_RETENTION_L2];
  SRSmaSnapWriter *pRsmaSnapWriter;
};

TFileSetRangeArray **vnodeSnapWriterGetTsdbRanges(SVSnapWriter *pWriter, int32_t tsdbTyp) {
  ASSERTS(sizeof(pWriter->pRsmaRanges) / sizeof(pWriter->pRsmaRanges[0]) == 2, "Unexpected array size");
  switch (tsdbTyp) {
    case SNAP_DATA_TSDB:
      return &pWriter->pRanges;
    case SNAP_DATA_RSMA1:
      return &pWriter->pRsmaRanges[0];
    case SNAP_DATA_RSMA2:
      return &pWriter->pRsmaRanges[1];
    default:
      return NULL;
  }
}

static int32_t vnodeSnapWriterDealWithSnapInfo(SVSnapWriter *pWriter, SSnapshotParam *pParam) {
  SVnode *pVnode = pWriter->pVnode;
  int32_t code = -1;

  if (pParam->data) {
    SSyncTLV *datHead = (void *)pParam->data;
    if (datHead->typ != TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
      terrno = TSDB_CODE_INVALID_DATA_FMT;
      goto _out;
    }

    STsdbRepOpts         tsdbOpts = {0};
    TFileSetRangeArray **ppRanges = NULL;
    int32_t           offset = 0;

    while (offset + sizeof(SSyncTLV) < datHead->len) {
      SSyncTLV *subField = (void *)(datHead->val + offset);
      offset += sizeof(SSyncTLV) + subField->len;
      void   *buf = subField->val;
      int32_t bufLen = subField->len;

      switch (subField->typ) {
        case SNAP_DATA_TSDB:
        case SNAP_DATA_RSMA1:
        case SNAP_DATA_RSMA2: {
          ppRanges = vnodeSnapWriterGetTsdbRanges(pWriter, subField->typ);
          if (ppRanges == NULL) {
            vError("vgId:%d, unexpected subfield type in snapshot param. subtyp:%d", TD_VID(pVnode), subField->typ);
            goto _out;
          }
          if (vnodeExtractSnapInfoDiff(buf, bufLen, ppRanges) < 0) {
            vError("vgId:%d, failed to get range diff since %s", TD_VID(pVnode), terrstr());
            goto _out;
          }
        } break;
        case SNAP_DATA_RAW: {
          if (tDeserializeTsdbRepOpts(buf, bufLen, &tsdbOpts) < 0) {
            vError("vgId:%d, failed to deserialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
            goto _out;
          }
        } break;
        default:
          vError("vgId:%d, unexpected subfield type of snap info. typ:%d", TD_VID(pVnode), subField->typ);
          goto _out;
      }
    }

    vInfo("vgId:%d, vnode snap writer supported tsdb rep of format:%d", TD_VID(pVnode), tsdbOpts.format);
  }

  code = 0;
_out:
  return code;
}

extern int32_t tsdbDisableAndCancelAllBgTask(STsdb *pTsdb);
extern int32_t tsdbEnableBgTask(STsdb *pTsdb);

static int32_t vnodeCancelAndDisableAllBgTask(SVnode *pVnode) {
  tsdbDisableAndCancelAllBgTask(pVnode->pTsdb);
  vnodeSyncCommit(pVnode);
  vnodeAChannelDestroy(vnodeAsyncHandle[0], pVnode->commitChannel, true);
  return 0;
}

static int32_t vnodeEnableBgTask(SVnode *pVnode) {
  tsdbEnableBgTask(pVnode->pTsdb);
  vnodeAChannelInit(vnodeAsyncHandle[0], &pVnode->commitChannel);
  return 0;
}

int32_t vnodeSnapWriterOpen(SVnode *pVnode, SSnapshotParam *pParam, SVSnapWriter **ppWriter) {
  int32_t       code = 0;
  SVSnapWriter *pWriter = NULL;
  int64_t       sver = pParam->start;
  int64_t       ever = pParam->end;

  // cancel and disable all bg task
  vnodeCancelAndDisableAllBgTask(pVnode);

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

  // snapshot info
  if (vnodeSnapWriterDealWithSnapInfo(pWriter, pParam) < 0) {
    goto _err;
  }

  vInfo("vgId:%d, vnode snapshot writer opened, sver:%" PRId64 " ever:%" PRId64 " commit id:%" PRId64, TD_VID(pVnode),
        sver, ever, pWriter->commitID);
  *ppWriter = pWriter;
  return code;

_err:
  vError("vgId:%d, vnode snapshot writer open failed since %s", TD_VID(pVnode), tstrerror(code));
  *ppWriter = NULL;
  return code;
}

static void vnodeSnapWriterDestroyTsdbRanges(SVSnapWriter *pWriter) {
  int32_t tsdbTyps[TSDB_RETENTION_MAX] = {SNAP_DATA_TSDB, SNAP_DATA_RSMA1, SNAP_DATA_RSMA2};
  for (int32_t j = 0; j < TSDB_RETENTION_MAX; ++j) {
    TFileSetRangeArray **ppRanges = vnodeSnapWriterGetTsdbRanges(pWriter, tsdbTyps[j]);
    if (ppRanges == NULL) continue;
    tsdbTFileSetRangeArrayDestroy(ppRanges);
  }
}

int32_t vnodeSnapWriterClose(SVSnapWriter *pWriter, int8_t rollback, SSnapshot *pSnapshot) {
  int32_t code = 0;
  SVnode *pVnode = pWriter->pVnode;

  vnodeSnapWriterDestroyTsdbRanges(pWriter);

  // prepare
  if (pWriter->pTsdbSnapWriter) {
    tsdbSnapWriterPrepareClose(pWriter->pTsdbSnapWriter);
  }

  if (pWriter->pTsdbSnapRAWWriter) {
    tsdbSnapRAWWriterPrepareClose(pWriter->pTsdbSnapRAWWriter);
  }

  if (pWriter->pRsmaSnapWriter) {
    rsmaSnapWriterPrepareClose(pWriter->pRsmaSnapWriter);
  }

  // commit json
  if (!rollback) {
    ASSERT(pVnode->config.vgId == pWriter->info.config.vgId);
    pWriter->info.state.committed = pWriter->ever;
    pVnode->config = pWriter->info.config;
    pVnode->state = (SVState){.committed = pWriter->info.state.committed,
                              .applied = pWriter->info.state.committed,
                              .commitID = pWriter->commitID,
                              .commitTerm = pWriter->info.state.commitTerm,
                              .applyTerm = pWriter->info.state.commitTerm};
    pVnode->statis = pWriter->info.statis;
    char dir[TSDB_FILENAME_LEN] = {0};
    vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, dir, TSDB_FILENAME_LEN);

    code = vnodeCommitInfo(dir);
    if (code) goto _exit;

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

  if (pWriter->pTsdbSnapRAWWriter) {
    code = tsdbSnapRAWWriterClose(&pWriter->pTsdbSnapRAWWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTqSnapWriter) {
    code = tqSnapWriterClose(&pWriter->pTqSnapWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTqCheckInfoWriter) {
    code = tqCheckInfoWriterClose(&pWriter->pTqCheckInfoWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTqOffsetWriter) {
    code = tqOffsetWriterClose(&pWriter->pTqOffsetWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pStreamTaskWriter) {
    code = streamTaskSnapWriterClose(pWriter->pStreamTaskWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pStreamStateWriter) {
    code = streamStateSnapWriterClose(pWriter->pStreamStateWriter, rollback);
    if (code) goto _exit;

    code = streamStateRebuildFromSnap(pWriter->pStreamStateWriter, 0);
    pWriter->pStreamStateWriter = NULL;
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
  vnodeEnableBgTask(pVnode);
  return code;
}

static int32_t vnodeSnapWriteInfo(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t       code = 0;
  SVnode       *pVnode = pWriter->pVnode;
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
  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, dir, TSDB_FILENAME_LEN);

  SVnodeStats vndStats = pWriter->info.config.vndStats;
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
        code = tsdbSnapWriterOpen(pVnode->pTsdb, pWriter->sver, pWriter->ever, pWriter->pRanges,
                                  &pWriter->pTsdbSnapWriter);
        if (code) goto _err;
      }

      code = tsdbSnapWrite(pWriter->pTsdbSnapWriter, pHdr);
      if (code) goto _err;
    } break;
    case SNAP_DATA_RAW: {
      // tsdb
      if (pWriter->pTsdbSnapRAWWriter == NULL) {
        ASSERT(pWriter->sver == 0);
        code = tsdbSnapRAWWriterOpen(pVnode->pTsdb, pWriter->ever, &pWriter->pTsdbSnapRAWWriter);
        if (code) goto _err;
      }

      code = tsdbSnapRAWWrite(pWriter->pTsdbSnapRAWWriter, pHdr);
      if (code) goto _err;
    } break;
    case SNAP_DATA_TQ_HANDLE: {
      // tq handle
      if (pWriter->pTqSnapWriter == NULL) {
        code = tqSnapWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pTqSnapWriter);
        if (code) goto _err;
      }

      code = tqSnapWrite(pWriter->pTqSnapWriter, pData, nData);
      if (code) goto _err;
    } break;
    case SNAP_DATA_TQ_CHECKINFO: {
      // tq checkinfo
      if (pWriter->pTqCheckInfoWriter == NULL) {
        code = tqCheckInfoWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pTqCheckInfoWriter);
        if (code) goto _err;
      }

      code = tqCheckInfoWrite(pWriter->pTqCheckInfoWriter, pData, nData);
      if (code) goto _err;
    } break;
    case SNAP_DATA_TQ_OFFSET: {
      // tq offset
      if (pWriter->pTqOffsetWriter == NULL) {
        code = tqOffsetWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pTqOffsetWriter);
        if (code) goto _err;
      }

      code = tqOffsetSnapWrite(pWriter->pTqOffsetWriter, pData, nData);
      if (code) goto _err;
    } break;
    case SNAP_DATA_STREAM_TASK:
    case SNAP_DATA_STREAM_TASK_CHECKPOINT: {
      if (pWriter->pStreamTaskWriter == NULL) {
        code = streamTaskSnapWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pStreamTaskWriter);
        if (code) goto _err;
      }
      code = streamTaskSnapWrite(pWriter->pStreamTaskWriter, pData, nData);
      if (code) goto _err;
    } break;
    case SNAP_DATA_STREAM_STATE_BACKEND: {
      if (pWriter->pStreamStateWriter == NULL) {
        code = streamStateSnapWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pStreamStateWriter);
        if (code) goto _err;
      }
      code = streamStateSnapWrite(pWriter->pStreamStateWriter, pData, nData);
      if (code) goto _err;

    } break;
    case SNAP_DATA_RSMA1:
    case SNAP_DATA_RSMA2:
    case SNAP_DATA_QTASK: {
      // rsma1/rsma2/qtask for rsma
      if (pWriter->pRsmaSnapWriter == NULL) {
        code = rsmaSnapWriterOpen(pVnode->pSma, pWriter->sver, pWriter->ever, (void **)pWriter->pRsmaRanges,
                                  &pWriter->pRsmaSnapWriter);
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
