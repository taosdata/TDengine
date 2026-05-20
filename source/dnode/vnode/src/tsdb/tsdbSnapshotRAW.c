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
#include "tsdbDataFileRAW.h"
#include "tsdbFS2.h"
#include "tsdbFSetRAW.h"

static void tsdbSnapRAWReadFileSetCloseReader(STsdbSnapRAWReader* reader);

// reader
typedef struct SDataFileRAWReaderIter {
  int32_t count;
  int32_t idx;
} SDataFileRAWReaderIter;

typedef struct STsdbSnapRAWReader {
  STsdb*  tsdb;
  int64_t ever;
  int8_t  type;

  TFileSetArray* fsetArr;

  // context
  struct {
    int32_t    fsetArrIdx;
    STFileSet* fset;
    bool       isDataDone;
  } ctx[1];

  // reader
  SDataFileRAWReaderArray dataReaderArr[1];

  // iter
  SDataFileRAWReaderIter dataIter[1];

  // missing file filter
  SHashObj* missingFileHash;  // key=(fid,ftype,level,minVer,maxVer) — per-file filtering (not owned, do not free)
  SHashObj* fidModeHash;      // key=fid, val=uint8_t mode (not owned, do not free)
  SHashObj* missingSttHash;   // key=(fid,cid) — per-STT filtering (not owned, do not free)
  int32_t*  missingFids;      // FID set for FID-level pre-filtering (owned, copy)
  int32_t   missingFidCount;
} STsdbSnapRAWReader;

static bool tsdbFidInMissingSet(int32_t fid, const int32_t* missingFids, int32_t missingFidCount) {
  int32_t lo = 0, hi = missingFidCount - 1;
  while (lo <= hi) {
    int32_t mid = lo + (hi - lo) / 2;
    if (missingFids[mid] == fid) return true;
    if (missingFids[mid] < fid)
      lo = mid + 1;
    else
      hi = mid - 1;
  }
  return false;
}

int32_t tsdbSnapRAWReaderOpen(STsdb* tsdb, int64_t ever, int8_t type, void* pRanges, SHashObj* missingFileHash,
                              SHashObj* fidModeHash, SHashObj* missingSttHash, const int32_t* missingFids,
                              int32_t missingFidCount, STsdbSnapRAWReader** reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = taosMemoryCalloc(1, sizeof(STsdbSnapRAWReader));
  if (reader[0] == NULL) return terrno;

  reader[0]->tsdb = tsdb;
  reader[0]->ever = ever;
  reader[0]->type = type;

  // set missing file filter (hash is borrowed, not owned)
  reader[0]->missingFileHash = missingFileHash;
  reader[0]->fidModeHash = fidModeHash;
  reader[0]->missingSttHash = missingSttHash;

  // copy missing fid filter
  if (missingFids != NULL && missingFidCount > 0) {
    reader[0]->missingFids = taosMemoryMalloc(missingFidCount * sizeof(int32_t));
    if (reader[0]->missingFids == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    memcpy(reader[0]->missingFids, missingFids, missingFidCount * sizeof(int32_t));
    reader[0]->missingFidCount = missingFidCount;
    tsdbInfo("vgId:%d, RAW reader opened with %d missing-fid filter", TD_VID(tsdb->pVnode), missingFidCount);
  }

  TFileSetRangeArray* pTypedRanges = (TFileSetRangeArray*)pRanges;
  if (pTypedRanges != NULL && TARRAY2_SIZE(pTypedRanges) > 0) {
    code = tsdbFSCreateRefSnapshotWithRanges(tsdb->pFS, pTypedRanges, &reader[0]->fsetArr);
  } else {
    code = tsdbFSCreateRefSnapshot(tsdb->pFS, &reader[0]->fsetArr);
  }
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode), __func__, lino,
              tstrerror(code), ever, type);
    tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
    taosMemoryFree(reader[0]->missingFids);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  } else {
    tsdbInfo("vgId:%d, tsdb snapshot raw reader opened. ever:%" PRId64 " type:%d ranged:%d", TD_VID(tsdb->pVnode), ever,
             type, (pTypedRanges != NULL && TARRAY2_SIZE(pTypedRanges) > 0));
  }
  return code;
}

void tsdbSnapRAWReaderClose(STsdbSnapRAWReader** reader) {
  if (reader[0] == NULL) return;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb* tsdb = reader[0]->tsdb;

  TARRAY2_DESTROY(reader[0]->dataReaderArr, tsdbDataFileRAWReaderClose);
  tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
  // missingFileHash is borrowed, not freed here
  taosMemoryFree(reader[0]->missingFids);
  taosMemoryFree(reader[0]);
  reader[0] = NULL;
  return;
}

static int32_t tsdbSnapRAWReadFileSetOpenReader(STsdbSnapRAWReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  // determine sync mode for this fid
  int32_t curFid = reader->ctx->fset->fid;
  bool    fsetLevel = false;
  if (reader->fidModeHash != NULL) {
    uint8_t* pMode = taosHashGet(reader->fidModeHash, &curFid, sizeof(curFid));
    if (pMode != NULL && *pMode == TSDB_SNAP_SYNC_FSET_LEVEL) {
      fsetLevel = true;
      tsdbInfo("vgId:%d, RAW fid:%d using FSET_LEVEL sync (send all files)", TD_VID(reader->tsdb->pVnode), curFid);
    } else {
      tsdbInfo("vgId:%d, RAW fid:%d using FILE_LEVEL sync (send only missing files)", TD_VID(reader->tsdb->pVnode),
               curFid);
    }
  }

  // data
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
    if (reader->ctx->fset->farr[ftype] == NULL) {
      continue;
    }
    STFileObj*               fobj = reader->ctx->fset->farr[ftype];
    // per-file filter: skip files not in missing set (only when FILE_LEVEL mode)
    if (!fsetLevel && reader->missingFileHash != NULL) {
      char key[TSDB_SNAP_FILE_KEY_LEN];
      tsdbSnapFileKeyMake(reader->ctx->fset->fid, ftype, 0, fobj->f->minVer, fobj->f->maxVer, key);
      if (taosHashGet(reader->missingFileHash, key, sizeof(key)) == NULL) {
        tsdbDebug("vgId:%d, RAW skip file fid:%d ftype:%d not in missing set", TD_VID(reader->tsdb->pVnode),
                  reader->ctx->fset->fid, ftype);
        continue;
      }
    }
    SDataFileRAWReader*      dataReader;
    SDataFileRAWReaderConfig config = {
        .tsdb = reader->tsdb,
        .szPage = reader->tsdb->pVnode->config.tsdbPageSize,
        .file = fobj->f[0],
    };
    code = tsdbDataFileRAWReaderOpen(NULL, &config, &dataReader);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(reader->dataReaderArr, dataReader);
    tsdbInfo("vgId:%d, RAW include file non-stt fid:%d ftype:%d in missing set", TD_VID(reader->tsdb->pVnode),
             reader->ctx->fset->fid, ftype);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt
  SSttLvl* lvl;
  TARRAY2_FOREACH(reader->ctx->fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      // per-file filter: skip stt files not in missing set (only when FILE_LEVEL mode)
      if (!fsetLevel && reader->missingSttHash != NULL) {
        char sttKey[TSDB_SNAP_FILE_KEY_LEN];
        tsdbSnapFileKeyMake(reader->ctx->fset->fid, TSDB_FTYPE_STT, lvl->level, fobj->f->minVer, fobj->f->maxVer,
                            sttKey);
        if (taosHashGet(reader->missingSttHash, sttKey, sizeof(sttKey)) == NULL) {
          tsdbDebug("vgId:%d, RAW skip stt file fid:%d cid:%" PRId64 " not in missing set",
                    TD_VID(reader->tsdb->pVnode), reader->ctx->fset->fid, fobj->f->cid);
          continue;
        }
      }
      SDataFileRAWReader*      dataReader;
      SDataFileRAWReaderConfig config = {
          .tsdb = reader->tsdb,
          .szPage = reader->tsdb->pVnode->config.tsdbPageSize,
          .file = fobj->f[0],
      };
      code = tsdbDataFileRAWReaderOpen(NULL, &config, &dataReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(reader->dataReaderArr, dataReader);
      tsdbInfo("vgId:%d, RAW include file stt fid:%d in missing set", TD_VID(reader->tsdb->pVnode),
               reader->ctx->fset->fid);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbSnapRAWReadFileSetCloseReader(reader);
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static void tsdbSnapRAWReadFileSetCloseReader(STsdbSnapRAWReader* reader) {
  TARRAY2_CLEAR(reader->dataReaderArr, tsdbDataFileRAWReaderClose);
}

static int32_t tsdbSnapRAWReadFileSetOpenIter(STsdbSnapRAWReader* reader) {
  reader->dataIter->count = TARRAY2_SIZE(reader->dataReaderArr);
  reader->dataIter->idx = 0;
  return 0;
}

static void tsdbSnapRAWReadFileSetCloseIter(STsdbSnapRAWReader* reader) {
  reader->dataIter->count = 0;
  reader->dataIter->idx = 0;
}

static int64_t tsdbSnapRAWReadPeek(SDataFileRAWReader* reader) {
  int64_t size = TMIN(reader->config->file.size - reader->ctx->offset, TSDB_SNAP_DATA_PAYLOAD_SIZE);
  return size;
}

static SDataFileRAWReader* tsdbSnapRAWReaderIterNext(STsdbSnapRAWReader* reader) {
  while (reader->dataIter->idx < reader->dataIter->count) {
    SDataFileRAWReader* dataReader = TARRAY2_GET(reader->dataReaderArr, reader->dataIter->idx);
    if (dataReader->ctx->offset < dataReader->config->file.size) {
      return dataReader;
    }
    reader->dataIter->idx++;
  }
  return NULL;
}

static int32_t tsdbSnapRAWReadNext(STsdbSnapRAWReader* reader, SSnapDataHdr** ppData) {
  int32_t code = 0;
  int32_t lino = 0;
  int8_t  type = reader->type;
  ppData[0] = NULL;

  SDataFileRAWReader* dataReader = tsdbSnapRAWReaderIterNext(reader);
  if (dataReader == NULL) {
    return 0;
  }

  // prepare
  int64_t dataLength = tsdbSnapRAWReadPeek(dataReader);

  void* pBuf = taosMemoryCalloc(1, sizeof(SSnapDataHdr) + sizeof(STsdbDataRAWBlockHeader) + dataLength);
  if (pBuf == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  SSnapDataHdr* pHdr = pBuf;
  pHdr->type = type;
  pHdr->size = sizeof(STsdbDataRAWBlockHeader) + dataLength;

  // read
  STsdbDataRAWBlockHeader* pBlock = (void*)pHdr->data;
  pBlock->offset = dataReader->ctx->offset;
  pBlock->dataLength = dataLength;

  code = tsdbDataFileRAWReadBlockData(dataReader, pBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  // finish
  dataReader->ctx->offset += pBlock->dataLength;
  ppData[0] = pBuf;

_exit:
  if (code) {
    taosMemoryFree(pBuf);
    pBuf = NULL;
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapRAWReadData(STsdbSnapRAWReader* reader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbSnapRAWReadNext(reader, (SSnapDataHdr**)ppData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapRAWReadBegin(STsdbSnapRAWReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  while (reader->ctx->fsetArrIdx < TARRAY2_SIZE(reader->fsetArr)) {
    reader->ctx->fset = TARRAY2_GET(reader->fsetArr, reader->ctx->fsetArrIdx++);

    // skip fids not in missing-fid filter
    if (reader->missingFids != NULL &&
        !tsdbFidInMissingSet(reader->ctx->fset->fid, reader->missingFids, reader->missingFidCount)) {
      tsdbDebug("vgId:%d, skip fid:%d not in missing-fid set", TD_VID(reader->tsdb->pVnode), reader->ctx->fset->fid);
      reader->ctx->fset = NULL;
      continue;
    }
    tsdbInfo("vgId:%d, RAW include fset fid:%d in missing-fid set", TD_VID(reader->tsdb->pVnode),
             reader->ctx->fset->fid);

    reader->ctx->isDataDone = false;

    code = tsdbSnapRAWReadFileSetOpenReader(reader);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbSnapRAWReadFileSetOpenIter(reader);
    TSDB_CHECK_CODE(code, lino, _exit);

    return code;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapRAWReadEnd(STsdbSnapRAWReader* reader) {
  tsdbSnapRAWReadFileSetCloseIter(reader);
  tsdbSnapRAWReadFileSetCloseReader(reader);
  reader->ctx->fset = NULL;
  return 0;
}

int32_t tsdbSnapRAWRead(STsdbSnapRAWReader* reader, uint8_t** data) {
  int32_t code = 0;
  int32_t lino = 0;

  data[0] = NULL;

  for (;;) {
    if (reader->ctx->fset == NULL) {
      code = tsdbSnapRAWReadBegin(reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (reader->ctx->fset == NULL) {
        break;
      }
    }

    if (!reader->ctx->isDataDone) {
      code = tsdbSnapRAWReadData(reader, data);
      TSDB_CHECK_CODE(code, lino, _exit);
      if (data[0]) {
        goto _exit;
      } else {
        reader->ctx->isDataDone = true;
      }
    }

    code = tsdbSnapRAWReadEnd(reader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(reader->tsdb->pVnode), __func__);
  }
  return code;
}

// writer
struct STsdbSnapRAWWriter {
  STsdb*  tsdb;
  int64_t sver;
  int64_t ever;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int64_t commitID;
  int32_t szPage;
  int64_t compactVersion;
  int64_t now;

  TFileSetArray* fsetArr;
  TFileOpArray   fopArr[1];

  struct {
    bool       fsetWriteBegin;
    int32_t    fid;
    STFileSet* fset;
    int64_t    cid;
    int64_t    level;

    // writer
    SFSetRAWWriter* fsetWriter;
  } ctx[1];
};

int32_t tsdbSnapRAWWriterOpen(STsdb* pTsdb, int64_t ever, STsdbSnapRAWWriter** writer) {
  int32_t code = 0;
  int32_t lino = 0;

  // start to write
  writer[0] = taosMemoryCalloc(1, sizeof(*writer[0]));
  if (writer[0] == NULL) return terrno;

  writer[0]->tsdb = pTsdb;
  writer[0]->ever = ever;
  writer[0]->minutes = pTsdb->keepCfg.days;
  writer[0]->precision = pTsdb->keepCfg.precision;
  writer[0]->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  writer[0]->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;
  writer[0]->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  writer[0]->commitID = tsdbFSAllocEid(pTsdb->pFS);
  writer[0]->szPage = pTsdb->pVnode->config.tsdbPageSize;
  writer[0]->compactVersion = INT64_MAX;
  writer[0]->now = taosGetTimestampMs();

  code = tsdbFSCreateCopySnapshot(pTsdb->pFS, &writer[0]->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, sver:0, ever:%" PRId64, TD_VID(pTsdb->pVnode), __func__, ever);
  }
  return code;
}

static int32_t tsdbSnapRAWWriteFileSetCloseIter(STsdbSnapRAWWriter* writer) { return 0; }

static int32_t tsdbSnapRAWWriteFileSetOpenWriter(STsdbSnapRAWWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

  SFSetRAWWriterConfig config = {
      .tsdb = writer->tsdb,
      .szPage = writer->szPage,
      .fid = writer->ctx->fid,
      .cid = writer->commitID,
      .expLevel = writer->ctx->level,
      .level = writer->ctx->level,
  };

  code = tsdbFSetRAWWriterOpen(&config, &writer->ctx->fsetWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapRAWWriteFileSetCloseWriter(STsdbSnapRAWWriter* writer) {
  return tsdbFSetRAWWriterClose(&writer->ctx->fsetWriter, 0, writer->fopArr);
}

static int32_t tsdbSnapRAWWriteFileSetBegin(STsdbSnapRAWWriter* writer, int32_t fid) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileSet* fset = &(STFileSet){.fid = fid};

  writer->ctx->fid = fid;
  STFileSet** fsetPtr = TARRAY2_SEARCH(writer->fsetArr, &fset, tsdbTFileSetCmprFn, TD_EQ);
  writer->ctx->fset = (fsetPtr == NULL) ? NULL : *fsetPtr;

  int32_t level = tsdbFidLevel(fid, &writer->tsdb->keepCfg, taosGetTimestampSec());

  code = tsdbSnapRAWWriteFileSetOpenWriter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->fsetWriteBegin = true;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapRAWWriteFileSetEnd(STsdbSnapRAWWriter* writer) {
  if (!writer->ctx->fsetWriteBegin) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  // close write
  code = tsdbSnapRAWWriteFileSetCloseWriter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->fsetWriteBegin = false;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSnapRAWWriterPrepareClose(STsdbSnapRAWWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbSnapRAWWriteFileSetEnd(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSEditBegin(writer->tsdb->pFS, writer->fopArr, TSDB_FEDIT_COMMIT);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(writer->tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbSnapRAWWriterClose(STsdbSnapRAWWriter** writer, int8_t rollback) {
  if (writer[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb* tsdb = writer[0]->tsdb;

  if (rollback) {
    code = tsdbFSEditAbort(writer[0]->tsdb->pFS);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    (void)taosThreadMutexLock(&writer[0]->tsdb->mutex);

    code = tsdbFSEditCommit(writer[0]->tsdb->pFS);
    if (code) {
      (void)taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    writer[0]->tsdb->pFS->fsstate = TSDB_FS_STATE_NORMAL;

    (void)taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
  }

  TARRAY2_DESTROY(writer[0]->fopArr, NULL);
  tsdbFSDestroyCopySnapshot(&writer[0]->fsetArr);

  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapRAWWriteTimeSeriesData(STsdbSnapRAWWriter* writer, STsdbDataRAWBlockHeader* bHdr) {
  int32_t code = 0;
  int32_t lino = 0;

  SEncryptData *pEncryptData = &(writer->tsdb->pVnode->config.tsdbCfg.encryptData);

  code = tsdbFSetRAWWriteBlockData(writer->ctx->fsetWriter, bHdr, pEncryptData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapRAWWriteData(STsdbSnapRAWWriter* writer, SSnapDataHdr* hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbDataRAWBlockHeader* bHdr = (void*)hdr->data;
  int32_t                  fid = bHdr->file.fid;
  if (!writer->ctx->fsetWriteBegin || fid != writer->ctx->fid) {
    code = tsdbSnapRAWWriteFileSetEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbSnapRAWWriteFileSetBegin(writer, fid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbSnapRAWWriteTimeSeriesData(writer, bHdr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSnapRAWWrite(STsdbSnapRAWWriter* writer, SSnapDataHdr* hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbSnapRAWWriteData(writer, hdr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, type:%d index:%" PRId64 " size:%" PRId64,
              TD_VID(writer->tsdb->pVnode), __func__, lino, tstrerror(code), hdr->type, hdr->index, hdr->size);
  } else {
    tsdbDebug("vgId:%d %s done, type:%d index:%" PRId64 " size:%" PRId64, TD_VID(writer->tsdb->pVnode), __func__,
              hdr->type, hdr->index, hdr->size);
  }
  return code;
}
