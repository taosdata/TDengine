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
  STsdb *tsdb;
  // context
  struct {
    bool       opened;
    bool       toData;
    int32_t    level;
    STFileSet *fset;
    SRowInfo  *row;
    SBlockData bData;
  } ctx[1];
  // config
  int32_t  maxRow;
  int32_t  minRow;
  int32_t  szPage;
  int8_t   cmprAlg;
  int64_t  cid;
  SSkmInfo skmTb;
  SSkmInfo skmRow;
  uint8_t *aBuf[5];
  // reader
  TARRAY2(SSttFileReader *) sttReaderArr;
  SDataFileReader *dataReader;
  // writer
  SSttFileWriter  *sttWriter;
  SDataFileWriter *dataWriter;
  // operations
  TFileOpArray fopArr;
} SMerger;

static int32_t tsdbMergerOpen(SMerger *merger) {
  merger->ctx->opened = true;
  TARRAY2_INIT(&merger->fopArr);
  return 0;
}

static int32_t tsdbMergerClose(SMerger *merger) {
  // TODO
  int32_t       code = 0;
  int32_t       lino = 0;
  SVnode       *pVnode = merger->tsdb->pVnode;
  int32_t       vid = TD_VID(pVnode);
  STFileSystem *fs = merger->tsdb->pFS;

  // edit file system
  code = tsdbFSEditBegin(fs, &merger->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSEditCommit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // clear the merge
  TARRAY2_FREE(&merger->fopArr);

_exit:
  if (code) {
  } else {
  }
  return 0;
}

static int32_t tsdbMergeNextRow(SMerger *merger) {
  // TODO
  return 0;
}

static int32_t tsdbMergeToDataWriteTSDataBlock(SMerger *merger) {
  if (merger->ctx->bData.nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);
  if (merger->ctx->bData.nRow >= merger->minRow) {
    // code = tsdbDataFWriteTSDataBlock(merger->dataWriter, &merger->ctx->bData);
    // TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbSttFileWriteTSDataBlock(merger->sttWriter, &merger->ctx->bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tBlockDataReset(&merger->ctx->bData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}
static int32_t tsdbMergeToData(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  for (;;) {
    code = tsdbMergeNextRow(merger);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (!merger->ctx->row) {
      code = tsdbMergeToDataWriteTSDataBlock(merger);
      TSDB_CHECK_CODE(code, lino, _exit);
      break;
    }

    if (!TABLE_SAME_SCHEMA(merger->ctx->bData.suid, merger->ctx->bData.suid, merger->ctx->row->suid,
                           merger->ctx->row->uid)) {
      code = tsdbMergeToDataWriteTSDataBlock(merger);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbUpdateSkmTb(merger->tsdb, (TABLEID *)merger->ctx->row, &merger->skmTb);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tBlockDataInit(&merger->ctx->bData, (TABLEID *)merger->ctx->row, merger->skmTb.pTSchema, NULL, 0);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataAppendRow(&merger->ctx->bData, &merger->ctx->row->row, NULL, merger->ctx->row->uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (merger->ctx->bData.nRow >= merger->maxRow) {
      code = tsdbMergeToDataWriteTSDataBlock(merger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vid:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeToUpperLevel(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  for (;;) {
    code = tsdbMergeNextRow(merger);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (!merger->ctx->row) break;

    code = tsdbSttFileWriteTSData(merger->sttWriter, merger->ctx->row);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vid:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeFileSetBegin(SMerger *merger) {
  int32_t    code = 0;
  int32_t    lino = 0;
  int32_t    vid = TD_VID(merger->tsdb->pVnode);
  STFileSet *fset = merger->ctx->fset;

  // prepare the merger file set
  SSttLvl   *lvl;
  STFileObj *fobj;
  merger->ctx->toData = true;
  merger->ctx->level = 0;

  TARRAY2_FOREACH(&fset->lvlArr, lvl) {
    if (lvl->level != merger->ctx->level) {
      lvl = NULL;
      break;
    }

    fobj = TARRAY2_GET(&lvl->farr, 0);
    if (fobj->f->stt->nseg < merger->tsdb->pVnode->config.sttTrigger) {
      merger->ctx->toData = false;
      break;
    } else {
      ASSERT(lvl->level == 0 || TARRAY2_SIZE(&lvl->farr) == 1);
      merger->ctx->level++;

      // open the reader
      SSttFileReader      *reader;
      SSttFileReaderConfig config = {
          .tsdb = merger->tsdb,
          // TODO
      };
      code = tsdbSttFReaderOpen(fobj->fname, &config, &reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(&merger->sttReaderArr, reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      // add the operation
      STFileOp op = {
          .fid = fobj->f->fid,
          .optype = TSDB_FOP_REMOVE,
          .of = fobj->f[0],
      };
      code = TARRAY2_APPEND(&merger->fopArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // open stt file writer
  if (lvl) {
    SSttFileWriterConfig config = {
        .tsdb = merger->tsdb,
        .maxRow = merger->maxRow,
        .szPage = merger->szPage,
        .cmprAlg = merger->cmprAlg,
        .skmTb = &merger->skmTb,
        .skmRow = &merger->skmRow,
        .aBuf = merger->aBuf,
        .file = fobj->f[0],
    };
    code = tsdbSttFileWriterOpen(&config, &merger->sttWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    SSttFileWriterConfig config = {
        .tsdb = merger->tsdb,
        .maxRow = merger->maxRow,
        .szPage = merger->szPage,
        .cmprAlg = merger->cmprAlg,
        .skmTb = &merger->skmTb,
        .skmRow = &merger->skmRow,
        .aBuf = merger->aBuf,
        .file =
            (STFile){
                .type = TSDB_FTYPE_STT,
                .did = {.level = 0, .id = 0},
                .fid = fset->fid,
                .cid = merger->cid,
                .size = 0,
                .stt = {{.level = merger->ctx->level, .nseg = 0}},
            },
    };
    code = tsdbSttFileWriterOpen(&config, &merger->sttWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // open data file writer
  if (merger->ctx->toData) {
    SDataFileWriterConfig config = {
        .tsdb = merger->tsdb,
        // TODO
    };
    code = tsdbDataFileWriterOpen(&config, &merger->dataWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}
static int32_t tsdbMergeFileSetEnd(SMerger *merger) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(merger->tsdb->pVnode);

  STFileOp op;
  code = tsdbSttFileWriterClose(&merger->sttWriter, 0, &op);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (op.optype != TSDB_FOP_NONE) {
    code = TARRAY2_APPEND(&merger->fopArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (merger->ctx->toData) {
    // code = tsdbDataFWriterClose();
    // TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}
static int32_t tsdbMergeFileSet(SMerger *merger, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  if (merger->ctx->opened == false) {
    code = tsdbMergerOpen(merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

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

int32_t tsdbMerge(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino;

  SVnode       *vnode = tsdb->pVnode;
  int32_t       vid = TD_VID(vnode);
  STFileSystem *fs = tsdb->pFS;
  STFileSet    *fset;
  STFileObj    *fobj;
  int32_t       sttTrigger = vnode->config.sttTrigger;

  SMerger merger[1];
  merger->tsdb = tsdb;
  merger->ctx->opened = false;

  // loop to merge each file set
  TARRAY2_FOREACH(&fs->cstate, fset) {
    SSttLvl *lvl0 = tsdbTFileSetGetLvl(fset, 0);
    if (lvl0 == NULL) {
      continue;
    }

    ASSERT(TARRAY2_SIZE(&lvl0->farr) > 0);

    fobj = TARRAY2_GET(&lvl0->farr, 0);

    if (fobj->f->stt->nseg >= sttTrigger) {
      code = tsdbMergeFileSet(merger, fset);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // end the merge
  if (merger->ctx->opened) {
    code = tsdbMergerClose(merger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  } else if (merger->ctx->opened) {
    tsdbDebug("vgId:%d %s done", vid, __func__);
  }
  return 0;
}
