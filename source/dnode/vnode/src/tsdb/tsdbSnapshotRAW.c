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

static int32_t tsdbSnapRAWReadFileSetCloseReader(STsdbSnapRAWReader* reader);

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
} STsdbSnapRAWReader;

int32_t tsdbSnapRAWReaderOpen(STsdb* tsdb, int64_t ever, int8_t type, STsdbSnapRAWReader** reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = taosMemoryCalloc(1, sizeof(STsdbSnapRAWReader));
  if (reader[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  reader[0]->tsdb = tsdb;
  reader[0]->ever = ever;
  reader[0]->type = type;

  code = tsdbFSCreateRefSnapshot(tsdb->pFS, &reader[0]->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, sver:0, ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode), __func__,
              lino, tstrerror(code), ever, type);
    tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  } else {
    tsdbInfo("vgId:%d, tsdb snapshot raw reader opened. sver:0, ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode), ever,
             type);
  }
  return code;
}

int32_t tsdbSnapRAWReaderClose(STsdbSnapRAWReader** reader) {
  if (reader[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb* tsdb = reader[0]->tsdb;

  TARRAY2_DESTROY(reader[0]->dataReaderArr, tsdbDataFileRAWReaderClose);
  tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
  taosMemoryFree(reader[0]);
  reader[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapRAWReadFileSetOpenReader(STsdbSnapRAWReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  // data
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
    if (reader->ctx->fset->farr[ftype] == NULL) {
      continue;
    }
    STFileObj*               fobj = reader->ctx->fset->farr[ftype];
    SDataFileRAWReader*      dataReader;
    SDataFileRAWReaderConfig config = {
        .tsdb = reader->tsdb,
        .szPage = reader->tsdb->pVnode->config.tsdbPageSize,
        .file = fobj->f[0],
    };
    code = tsdbDataFileRAWReaderOpen(NULL, &config, &dataReader);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(reader->dataReaderArr, dataReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt
  SSttLvl* lvl;
  TARRAY2_FOREACH(reader->ctx->fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      SDataFileRAWReader*      dataReader;
      SDataFileRAWReaderConfig config = {
          .tsdb = reader->tsdb,
          .szPage = reader->tsdb->pVnode->config.tsdbPageSize,
          .file = fobj->f[0],
      };
      code = tsdbDataFileRAWReaderOpen(NULL, &config, &dataReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(reader->dataReaderArr, dataReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapRAWReadFileSetCloseReader(STsdbSnapRAWReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  TARRAY2_CLEAR(reader->dataReaderArr, tsdbDataFileRAWReaderClose);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapRAWReadFileSetOpenIter(STsdbSnapRAWReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader->dataIter->count = TARRAY2_SIZE(reader->dataReaderArr);
  reader->dataIter->idx = 0;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapRAWReadFileSetCloseIter(STsdbSnapRAWReader* reader) {
  reader->dataIter->count = 0;
  reader->dataIter->idx = 0;
  return 0;
}

static int64_t tsdbSnapRAWReadPeek(SDataFileRAWReader* reader) {
  int64_t size = TMIN(reader->config->file.size - reader->ctx->offset, TSDB_SNAP_DATA_PAYLOAD_SIZE);
  return size;
}

static SDataFileRAWReader* tsdbSnapRAWReaderIterNext(STsdbSnapRAWReader* reader) {
  ASSERT(reader->dataIter->idx <= reader->dataIter->count);

  while (reader->dataIter->idx < reader->dataIter->count) {
    SDataFileRAWReader* dataReader = TARRAY2_GET(reader->dataReaderArr, reader->dataIter->idx);
    ASSERT(dataReader);
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
  ASSERT(dataLength > 0);

  void* pBuf = taosMemoryCalloc(1, sizeof(SSnapDataHdr) + sizeof(STsdbDataRAWBlockHeader) + dataLength);
  if (pBuf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
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
  ASSERT(dataReader->ctx->offset <= dataReader->config->file.size);
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

  ASSERT(reader->ctx->fset == NULL);

  if (reader->ctx->fsetArrIdx < TARRAY2_SIZE(reader->fsetArr)) {
    reader->ctx->fset = TARRAY2_GET(reader->fsetArr, reader->ctx->fsetArrIdx++);
    reader->ctx->isDataDone = false;

    code = tsdbSnapRAWReadFileSetOpenReader(reader);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbSnapRAWReadFileSetOpenIter(reader);
    TSDB_CHECK_CODE(code, lino, _exit);
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
    SDiskID    did;
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
  if (writer[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

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

static int32_t tsdbSnapRAWWriteFileSetOpenIter(STsdbSnapRAWWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
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
      .did = writer->ctx->did,
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

  ASSERT(writer->ctx->fsetWriteBegin == false);

  STFileSet* fset = &(STFileSet){.fid = fid};

  writer->ctx->fid = fid;
  STFileSet** fsetPtr = TARRAY2_SEARCH(writer->fsetArr, &fset, tsdbTFileSetCmprFn, TD_EQ);
  writer->ctx->fset = (fsetPtr == NULL) ? NULL : *fsetPtr;

  int32_t level = tsdbFidLevel(fid, &writer->tsdb->keepCfg, taosGetTimestampSec());
  if (tfsAllocDisk(writer->tsdb->pVnode->pTfs, level, &writer->ctx->did)) {
    code = TSDB_CODE_NO_AVAIL_DISK;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tfsMkdirRecurAt(writer->tsdb->pVnode->pTfs, writer->tsdb->path, writer->ctx->did);

  code = tsdbSnapRAWWriteFileSetOpenWriter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->level = level;
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
    taosThreadMutexLock(&writer[0]->tsdb->mutex);

    code = tsdbFSEditCommit(writer[0]->tsdb->pFS);
    if (code) {
      taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    writer[0]->tsdb->pFS->fsstate = TSDB_FS_STATE_NORMAL;

    taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
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

  code = tsdbFSetRAWWriteBlockData(writer->ctx->fsetWriter, bHdr);
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
  ASSERT(hdr->type == SNAP_DATA_RAW);

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
