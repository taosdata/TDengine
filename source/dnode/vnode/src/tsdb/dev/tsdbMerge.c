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

#include "inc/tsdbMerge.h"

typedef struct {
  STsdb         *tsdb;
  TFileSetArray *fsetArr;
  int32_t        sttTrigger;
  int32_t        maxRow;
  int32_t        minRow;
  int32_t        szPage;
  int8_t         cmprAlg;
  int64_t        compactVersion;
  int64_t        cid;
  SSkmInfo       skmTb[1];

  // context
  struct {
    bool       opened;
    STFileSet *fset;
    bool       toData;
    int32_t    level;
    SSttLvl   *lvl;
    STFileObj *fobj;
    SRowInfo  *row;
    SBlockData bData[1];
  } ctx[1];

  TFileOpArray fopArr[1];

  // reader
  TSttFileReaderArray sttReaderArr[1];
  // iter
  TTsdbIterArray iterArr[1];
  SIterMerger   *iterMerger;
  // writer
  SSttFileWriter  *sttWriter;
  SDataFileWriter *dataWriter;
} SMerger;

static int32_t tsdbMergerOpen(SMerger *merger) {
  merger->maxRow = merger->tsdb->pVnode->config.tsdbCfg.maxRows;
  merger->minRow = merger->tsdb->pVnode->config.tsdbCfg.minRows;
  merger->szPage = merger->tsdb->pVnode->config.tsdbPageSize;
  merger->cmprAlg = merger->tsdb->pVnode->config.tsdbCfg.compression;
  merger->compactVersion = INT64_MAX;
  tsdbFSAllocEid(merger->tsdb->pFS, &merger->cid);
  merger->ctx->opened = true;
  return 0;
}

static int32_t tsdbMergerClose(SMerger *merger) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SVnode       *pVnode = merger->tsdb->pVnode;
  int32_t       vid = TD_VID(pVnode);
  STFileSystem *fs = merger->tsdb->pFS;

  // edit file system
  code = tsdbFSEditBegin(fs, merger->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSEditCommit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // clear the merge
  TARRAY2_FREE(merger->iterArr);
  TARRAY2_FREE(merger->sttReaderArr);
  TARRAY2_FREE(merger->fopArr);
  tBlockDataDestroy(merger->ctx->bData);
  tDestroyTSchema(merger->skmTb->pTSchema);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return 0;
}

static int32_t tsdbMergeToDataTableEnd(SMerger *merger) {
  if (merger->ctx->bData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  if (merger->ctx->bData->nRow < merger->minRow) {
    code = tsdbSttFileWriteTSDataBlock(merger->sttWriter, merger->ctx->bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbDataFileWriteTSDataBlock(merger->dataWriter, merger->ctx->bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tBlockDataClear(merger->ctx->bData);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeToDataTableBegin(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  code = tsdbUpdateSkmTb(merger->tsdb, (const TABLEID *)merger->ctx->row, merger->skmTb);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tBlockDataInit(merger->ctx->bData, (TABLEID *)merger->ctx->row, merger->skmTb->pTSchema, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeToData(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  while ((merger->ctx->row = tsdbIterMergerGet(merger->iterMerger))) {
    if (merger->ctx->row->uid != merger->ctx->bData->uid) {
      code = tsdbMergeToDataTableEnd(merger);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbMergeToDataTableBegin(merger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataAppendRow(merger->ctx->bData, &merger->ctx->row->row, NULL, merger->ctx->row->uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (merger->ctx->bData->nRow >= merger->maxRow) {
      code = tsdbDataFileWriteTSDataBlock(merger->dataWriter, merger->ctx->bData);
      TSDB_CHECK_CODE(code, lino, _exit);

      tBlockDataReset(merger->ctx->bData);
    }

    code = tsdbIterMergerNext(merger->iterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbMergeToDataTableEnd(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeToUpperLevel(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  SRowInfo *row;
  while ((row = tsdbIterMergerGet(merger->iterMerger))) {
    code = tsdbSttFileWriteTSData(merger->sttWriter, row);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbIterMergerNext(merger->iterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vid:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeFileSetBeginOpenReader(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  merger->ctx->toData = true;
  merger->ctx->level = 0;
  TARRAY2_FOREACH(merger->ctx->fset->lvlArr, merger->ctx->lvl) {
    if (merger->ctx->lvl->level != merger->ctx->level || TARRAY2_SIZE(merger->ctx->lvl->fobjArr) == 0) {
      merger->ctx->toData = false;
      merger->ctx->lvl = NULL;
      break;
    }

    ASSERT(merger->ctx->lvl->level == 0 || TARRAY2_SIZE(merger->ctx->lvl->fobjArr) == 1);

    merger->ctx->fobj = TARRAY2_FIRST(merger->ctx->lvl->fobjArr);
    if (merger->ctx->fobj->f->stt->nseg < merger->sttTrigger) {
      merger->ctx->toData = false;
      break;
    } else {
      merger->ctx->level++;

      // add the operation
      STFileOp op = {
          .optype = TSDB_FOP_REMOVE,
          .fid = merger->ctx->fset->fid,
          .of = merger->ctx->fobj->f[0],
      };
      code = TARRAY2_APPEND(merger->fopArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);

      // open the reader
      SSttFileReader      *reader;
      SSttFileReaderConfig config[1] = {{
          .tsdb = merger->tsdb,
          .szPage = merger->szPage,
          .file[0] = merger->ctx->fobj->f[0],
      }};
      code = tsdbSttFileReaderOpen(merger->ctx->fobj->fname, config, &reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(merger->sttReaderArr, reader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetBeginOpenIter(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  SSttFileReader *sttReader;
  TARRAY2_FOREACH(merger->sttReaderArr, sttReader) {
    const TSttSegReaderArray *segReaderArr;

    code = tsdbSttFileReaderGetSegReader(sttReader, &segReaderArr);
    TSDB_CHECK_CODE(code, lino, _exit);

    SSttSegReader *segReader;
    TARRAY2_FOREACH(segReaderArr, segReader) {
      STsdbIter *iter;

      STsdbIterConfig config[1] = {{
          .type = TSDB_ITER_TYPE_STT,
          .sttReader = segReader,
      }};

      code = tsdbIterOpen(config, &iter);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(merger->iterArr, iter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tsdbIterMergerInit(merger->iterArr, &merger->iterMerger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetBeginOpenWriter(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  SDiskID did = {
      .level = 0,
      .id = 0,
  };  // TODO

  if (merger->ctx->lvl) {  // to existing level
    SSttFileWriterConfig config[1] = {{
        .tsdb = merger->tsdb,
        .maxRow = merger->maxRow,
        .szPage = merger->szPage,
        .cmprAlg = merger->cmprAlg,
        .compactVersion = merger->compactVersion,
        .file = merger->ctx->fobj->f[0],
    }};
    code = tsdbSttFileWriterOpen(config, &merger->sttWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {  // to new level
    SSttFileWriterConfig config[1] = {{
        .tsdb = merger->tsdb,
        .maxRow = merger->maxRow,
        .szPage = merger->szPage,
        .cmprAlg = merger->cmprAlg,
        .compactVersion = merger->compactVersion,
        .file =
            {
                .type = TSDB_FTYPE_STT,
                .did = did,
                .fid = merger->ctx->fset->fid,
                .cid = merger->cid,
                .size = 0,
                .stt = {{
                    .level = merger->ctx->level,
                    .nseg = 0,
                }},
            },
    }};
    code = tsdbSttFileWriterOpen(config, &merger->sttWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (merger->ctx->toData) {  // TODO
    tBlockDataReset(merger->ctx->bData);

    SDataFileWriterConfig config = {
        .tsdb = merger->tsdb,
        .maxRow = merger->maxRow,
        .f =
            {
                [0] =
                    {
                        .type = TSDB_FTYPE_HEAD,
                        .did = did,
                        .fid = merger->ctx->fset->fid,
                        .cid = merger->cid,
                        .size = 0,
                    },
                [1] =
                    {
                        .type = TSDB_FTYPE_DATA,
                        .did = did,
                        .fid = merger->ctx->fset->fid,
                        .cid = merger->cid,
                        .size = 0,
                    },
                [2] =
                    {
                        .type = TSDB_FTYPE_SMA,
                        .did = did,
                        .fid = merger->ctx->fset->fid,
                        .cid = merger->cid,
                        .size = 0,
                    },
                [3] =
                    {
                        .type = TSDB_FTYPE_TOMB,
                        .did = did,
                        .fid = merger->ctx->fset->fid,
                        .cid = merger->cid,
                        .size = 0,
                    },
            },
    };
    code = tsdbDataFileWriterOpen(&config, &merger->dataWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetBegin(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  ASSERT(TARRAY2_SIZE(merger->sttReaderArr) == 0);
  ASSERT(TARRAY2_SIZE(merger->iterArr) == 0);
  ASSERT(merger->iterMerger == NULL);
  ASSERT(merger->sttWriter == NULL);
  ASSERT(merger->dataWriter == NULL);

  // open reader
  code = tsdbMergeFileSetBeginOpenReader(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open iterator
  code = tsdbMergeFileSetBeginOpenIter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open writer
  code = tsdbMergeFileSetBeginOpenWriter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetEndCloseWriter(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  STFileOp op[1];

  if (merger->ctx->toData) {
    code = tsdbDataFileWriterClose(&merger->dataWriter, 0, op);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (op->optype != TSDB_FOP_NONE) {
      code = TARRAY2_APPEND_PTR(merger->fopArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tsdbSttFileWriterClose(&merger->sttWriter, 0, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (op->optype != TSDB_FOP_NONE) {
    code = TARRAY2_APPEND_PTR(merger->fopArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSetEndCloseIter(SMerger *merger) {
  tsdbIterMergerClear(&merger->iterMerger);
  TARRAY2_CLEAR(merger->iterArr, tsdbIterClose);
  return 0;
}

static int32_t tsdbMergeFileSetEndCloseReader(SMerger *merger) {
  TARRAY2_CLEAR(merger->sttReaderArr, tsdbSttFileReaderClose);
  return 0;
}

static int32_t tsdbMergeFileSetEnd(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  code = tsdbMergeFileSetEndCloseWriter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbMergeFileSetEndCloseIter(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbMergeFileSetEndCloseReader(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbMergeFileSet(SMerger *merger, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  merger->ctx->fset = fset;
  code = tsdbMergeFileSetBegin(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // do merge
  if (merger->ctx->toData) {
    code = tsdbMergeToData(merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbMergeToUpperLevel(merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbMergeFileSetEnd(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(merger->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(merger->tsdb->pVnode), __func__, fset->fid);
  }
  return 0;
}

static int32_t tsdbDoMerge(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  STFileSet *fset;
  SSttLvl   *lvl;
  STFileObj *fobj;
  TARRAY2_FOREACH(merger->fsetArr, fset) {
    lvl = TARRAY2_SIZE(fset->lvlArr) > 0 ? TARRAY2_FIRST(fset->lvlArr) : NULL;
    if (!lvl || lvl->level != 0 || TARRAY2_SIZE(lvl->fobjArr) == 0) continue;

    fobj = TARRAY2_FIRST(lvl->fobjArr);
    if (fobj->f->stt->nseg < merger->sttTrigger) continue;

    if (!merger->ctx->opened) {
      code = tsdbMergerOpen(merger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbMergeFileSet(merger, fset);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (merger->ctx->opened) {
    code = tsdbMergerClose(merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", vid, __func__);
  }
  return code;
}

int32_t tsdbMerge(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(tsdb->pVnode);

  SMerger merger[1] = {{
      .tsdb = tsdb,
      .sttTrigger = tsdb->pVnode->config.sttTrigger,
  }};

  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &merger->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDoMerge(merger);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbFSDestroyCopySnapshot(&merger->fsetArr);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else if (merger->ctx->opened) {
    tsdbDebug("vgId:%d %s done", vid, __func__);
  }
  return 0;
}
