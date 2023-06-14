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

#include "inc/tsdbCommit2.h"

// extern dependencies
typedef struct {
  STsdb         *tsdb;
  TFileSetArray *fsetArr;
  TFileOpArray   fopArray[1];

  SSkmInfo skmTb[1];
  SSkmInfo skmRow[1];

  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int32_t sttTrigger;
  int32_t szPage;
  int64_t compactVersion;

  struct {
    int64_t    cid;
    int64_t    now;
    TSKEY      nextKey;
    int32_t    fid;
    int32_t    expLevel;
    SDiskID    did;
    TSKEY      minKey;
    TSKEY      maxKey;
    STFileSet *fset;
    TABLEID    tbid[1];
  } ctx[1];

  SSttFileReader *sttReader;
  TTsdbIterArray  iterArray[1];
  SIterMerger    *iterMerger;

  // writer
  SBlockData       blockData[2];
  int32_t          blockDataIdx;
  SDataFileWriter *dataWriter;
  SSttFileWriter  *sttWriter;
} SCommitter2;

static int32_t tsdbCommitOpenNewSttWriter(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  SSttFileWriterConfig config[1] = {{
      .tsdb = committer->tsdb,
      .maxRow = committer->maxRow,
      .szPage = committer->szPage,
      .cmprAlg = committer->cmprAlg,
      .compactVersion = committer->compactVersion,
      .file =
          {
              .type = TSDB_FTYPE_STT,
              .did = committer->ctx->did,
              .fid = committer->ctx->fid,
              .cid = committer->ctx->cid,
          },
  }};

  code = tsdbSttFileWriterOpen(config, &committer->sttWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s success", TD_VID(committer->tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCommitOpenExistSttWriter(SCommitter2 *committer, const STFile *f) {
  int32_t code = 0;
  int32_t lino = 0;

  SSttFileWriterConfig config[1] = {{
      .tsdb = committer->tsdb,
      .maxRow = committer->maxRow,
      .szPage = committer->szPage,
      .cmprAlg = committer->cmprAlg,
      .compactVersion = committer->compactVersion,
      .file = f[0],
  }};

  code = tsdbSttFileWriterOpen(config, &committer->sttWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s success", TD_VID(committer->tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCommitOpenWriter(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  // stt writer
  if (committer->ctx->fset == NULL) {
    code = tsdbCommitOpenNewSttWriter(committer);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    const SSttLvl *lvl0 = tsdbTFileSetGetSttLvl(committer->ctx->fset, 0);
    if (lvl0 == NULL || TARRAY2_SIZE(lvl0->fobjArr) == 0) {
      code = tsdbCommitOpenNewSttWriter(committer);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      STFileObj *fobj = TARRAY2_LAST(lvl0->fobjArr);
      if (fobj->f->stt->nseg >= committer->sttTrigger) {
        code = tsdbCommitOpenNewSttWriter(committer);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (committer->sttTrigger == 1) {
          SSttFileReaderConfig sttFileReaderConfig = {
              .tsdb = committer->tsdb,
              .szPage = committer->szPage,
              .file = fobj->f[0],
          };
          code = tsdbSttFileReaderOpen(NULL, &sttFileReaderConfig, &committer->sttReader);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      } else {
        code = tsdbCommitOpenExistSttWriter(committer, fobj->f);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

  // data writer
  if (committer->sttTrigger == 1) {
    // data writer
    SDataFileWriterConfig config = {
        .tsdb = committer->tsdb,
        .cmprAlg = committer->cmprAlg,
        .maxRow = committer->maxRow,
        .szPage = committer->szPage,
        .fid = committer->ctx->fid,
        .cid = committer->ctx->cid,
        .did = committer->ctx->did,
        .compactVersion = committer->compactVersion,
    };

    if (committer->ctx->fset) {
      for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ftype++) {
        if (committer->ctx->fset->farr[ftype] != NULL) {
          config.files[ftype].exist = true;
          config.files[ftype].file = committer->ctx->fset->farr[ftype]->f[0];
        }
      }
    }

    code = tsdbDataFileWriterOpen(&config, &committer->dataWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTSDataOpenIterMerger(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(TARRAY2_SIZE(committer->iterArray) == 0);
  ASSERT(committer->iterMerger == NULL);

  STsdbIter      *iter;
  STsdbIterConfig config[1];

  // memtable iter
  config->type = TSDB_ITER_TYPE_MEMT;
  config->memt = committer->tsdb->imem;
  config->from->ts = committer->ctx->minKey;
  config->from->version = VERSION_MIN;

  code = tsdbIterOpen(config, &iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = TARRAY2_APPEND(committer->iterArray, iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // stt file iter
  if (committer->sttReader) {
    const TSttSegReaderArray *readerArray;

    tsdbSttFileReaderGetSegReader(committer->sttReader, &readerArray);

    SSttSegReader *segReader;
    TARRAY2_FOREACH(readerArray, segReader) {
      config->type = TSDB_ITER_TYPE_STT;
      config->sttReader = segReader;
    }

    code = tsdbIterOpen(config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(committer->iterArray, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // open iter merger
  code = tsdbIterMergerOpen(committer->iterArray, &committer->iterMerger, false);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTSDataCloseIterMerger(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  tsdbIterMergerClose(&committer->iterMerger);
  TARRAY2_CLEAR(committer->iterArray, tsdbIterClose);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTSDataToDataTableBegin(SCommitter2 *committer, const TABLEID *tbid) {
  int32_t code = 0;
  int32_t lino = 0;

  committer->ctx->tbid->suid = tbid->suid;
  committer->ctx->tbid->uid = tbid->uid;

  code = tsdbUpdateSkmTb(committer->tsdb, committer->ctx->tbid, committer->skmTb);
  TSDB_CHECK_CODE(code, lino, _exit);

  committer->blockDataIdx = 0;
  for (int32_t i = 0; i < ARRAY_SIZE(committer->blockData); i++) {
    code = tBlockDataInit(&committer->blockData[i], committer->ctx->tbid, committer->skmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTSDataToDataTableEnd(SCommitter2 *committer) {
  if (committer->ctx->tbid->uid == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  int32_t cidx = committer->blockDataIdx;
  int32_t pidx = ((cidx + 1) & 1);
  int32_t numRow = (committer->blockData[cidx].nRow + committer->blockData[pidx].nRow) / 2;

  if (committer->blockData[pidx].nRow > 0 && numRow >= committer->minRow) {
    ASSERT(committer->blockData[pidx].nRow == committer->maxRow);

    SRowInfo row[1] = {{
        .suid = committer->ctx->tbid->suid,
        .uid = committer->ctx->tbid->uid,
        .row = tsdbRowFromBlockData(committer->blockData + pidx, 0),
    }};

    for (int32_t i = 0; i < numRow; i++) {
      row->row.iRow = i;

      code = tsdbDataFileWriteRow(committer->dataWriter, row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbDataFileFlush(committer->dataWriter);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (int32_t i = numRow; i < committer->blockData[pidx].nRow; i++) {
      row->row.iRow = i;
      code = tsdbDataFileWriteRow(committer->dataWriter, row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    row->row = tsdbRowFromBlockData(committer->blockData + cidx, 0);
    for (int32_t i = 0; i < committer->blockData[cidx].nRow; i++) {
      row->row.iRow = i;
      code = tsdbDataFileWriteRow(committer->dataWriter, row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    if (committer->blockData[pidx].nRow > 0) {
      code = tsdbDataFileWriteBlockData(committer->dataWriter, committer->blockData + cidx);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (committer->blockData[cidx].nRow < committer->minRow) {
      code = tsdbSttFileWriteBlockData(committer->sttWriter, committer->blockData + cidx);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDataFileWriteBlockData(committer->dataWriter, committer->blockData + cidx);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  for (int32_t i = 0; i < ARRAY_SIZE(committer->blockData); i++) {
    tBlockDataReset(&committer->blockData[i]);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTSDataToData(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaInfo info;
  for (SRowInfo *row; (row = tsdbIterMergerGetData(committer->iterMerger)) != NULL;) {
    if (row->uid != committer->ctx->tbid->uid) {
      // end last table write
      code = tsdbCommitTSDataToDataTableEnd(committer);
      TSDB_CHECK_CODE(code, lino, _exit);

      // Ignore table of obsolescence
      if (metaGetInfo(committer->tsdb->pVnode->pMeta, row->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(committer->iterMerger, (TABLEID *)row);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }

      code = tsdbCommitTSDataToDataTableBegin(committer, (TABLEID *)row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (row->row.type == TSDBROW_ROW_FMT) {
      code = tsdbUpdateSkmRow(committer->tsdb, committer->ctx->tbid, TSDBROW_SVERSION(&row->row), committer->skmRow);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    TSDBKEY key = TSDBROW_KEY(&row->row);
    if (key.version <= committer->compactVersion                   //
        && committer->blockData[committer->blockDataIdx].nRow > 0  //
        && key.ts == committer->blockData[committer->blockDataIdx]
                         .aTSKEY[committer->blockData[committer->blockDataIdx].nRow - 1]) {
      code =
          tBlockDataUpdateRow(committer->blockData + committer->blockDataIdx, &row->row, committer->skmRow->pTSchema);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      if (committer->blockData[committer->blockDataIdx].nRow >= committer->maxRow) {
        int32_t idx = ((committer->blockDataIdx + 1) & 1);
        if (committer->blockData[idx].nRow >= committer->maxRow) {
          code = tsdbDataFileWriteBlockData(committer->dataWriter, committer->blockData + idx);
          TSDB_CHECK_CODE(code, lino, _exit);

          tBlockDataClear(committer->blockData + idx);
        }
        committer->blockDataIdx = idx;
      }

      code = tBlockDataAppendRow(&committer->blockData[committer->blockDataIdx], &row->row, committer->skmRow->pTSchema,
                                 row->uid);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbIterMergerNext(committer->iterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbCommitTSDataToDataTableEnd(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTSDataToStt(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(committer->sttReader == NULL);

  SMetaInfo info;
  for (SRowInfo *row; (row = tsdbIterMergerGetData(committer->iterMerger)) != NULL;) {
    if (row->uid != committer->ctx->tbid->uid) {
      committer->ctx->tbid->suid = row->suid;
      committer->ctx->tbid->uid = row->uid;

      // Ignore table of obsolescence
      if (metaGetInfo(committer->tsdb->pVnode->pMeta, row->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(committer->iterMerger, committer->ctx->tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }

    TSKEY ts = TSDBROW_TS(&row->row);
    if (ts > committer->ctx->maxKey) {
      committer->ctx->nextKey = TMIN(committer->ctx->nextKey, ts);
      code = tsdbIterMergerSkipTableData(committer->iterMerger, committer->ctx->tbid);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbSttFileWriteRow(committer->sttWriter, row);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbIterMergerNext(committer->iterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTSData(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  if (committer->tsdb->imem->nRow == 0) goto _exit;

  // open iter and iter merger
  code = tsdbCommitTSDataOpenIterMerger(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // loop iter
  if (committer->sttTrigger == 1) {
    code = tsdbCommitTSDataToData(committer);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbCommitTSDataToStt(committer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // close iter and iter merger
  code = tsdbCommitTSDataCloseIterMerger(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTombDataOpenIter(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbIter      *iter;
  STsdbIterConfig config[1];

  if (committer->sttReader) {
    const TSttSegReaderArray *readerArray;

    tsdbSttFileReaderGetSegReader(committer->sttReader, &readerArray);

    SSttSegReader *segReader;
    TARRAY2_FOREACH(readerArray, segReader) {
      config->type = TSDB_ITER_TYPE_STT_TOMB;
      config->sttReader = segReader;

      code = tsdbIterOpen(config, &iter);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(committer->iterArray, iter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  config->type = TSDB_ITER_TYPE_MEMT_TOMB;
  config->memt = committer->tsdb->imem;

  code = tsdbIterOpen(config, &iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = TARRAY2_APPEND(committer->iterArray, iter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open iter
  code = tsdbIterMergerOpen(committer->iterArray, &committer->iterMerger, true);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitTombDataCloseIter(SCommitter2 *committer) {
  tsdbIterMergerClose(&committer->iterMerger);
  TARRAY2_CLEAR(committer->iterArray, tsdbIterClose);
  return 0;
}

static int32_t tsdbCommitTombData(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbCommitTombDataOpenIter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (committer->dataWriter == NULL || tsdbSttFileWriterIsOpened(committer->sttWriter)) {
    for (STombRecord *record; (record = tsdbIterMergerGetTombRecord(committer->iterMerger));) {
      code = tsdbSttFileWriteTombRecord(committer->sttWriter, record);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbIterMergerNext(committer->iterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    for (STombRecord *record; (record = tsdbIterMergerGetTombRecord(committer->iterMerger));) {
      code = tsdbDataFileWriteTombRecord(committer->dataWriter, record);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbIterMergerNext(committer->iterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tsdbCommitTombDataCloseIter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCommitFileSetBegin(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *tsdb = committer->tsdb;

  committer->ctx->fid = tsdbKeyFid(committer->ctx->nextKey, committer->minutes, committer->precision);
  committer->ctx->expLevel = tsdbFidLevel(committer->ctx->fid, &tsdb->keepCfg, committer->ctx->now);
  tsdbFidKeyRange(committer->ctx->fid, committer->minutes, committer->precision, &committer->ctx->minKey,
                  &committer->ctx->maxKey);
  code = tfsAllocDisk(committer->tsdb->pVnode->pTfs, committer->ctx->expLevel, &committer->ctx->did);
  TSDB_CHECK_CODE(code, lino, _exit);
  STFileSet fset = {.fid = committer->ctx->fid};
  committer->ctx->fset = &fset;
  committer->ctx->fset = TARRAY2_SEARCH_EX(committer->fsetArr, &committer->ctx->fset, tsdbTFileSetCmprFn, TD_EQ);
  committer->ctx->tbid->suid = 0;
  committer->ctx->tbid->uid = 0;

  ASSERT(TARRAY2_SIZE(committer->iterArray) == 0);
  ASSERT(committer->iterMerger == NULL);
  ASSERT(committer->sttWriter == NULL);
  ASSERT(committer->dataWriter == NULL);

  code = tsdbCommitOpenWriter(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // reset nextKey
  committer->ctx->nextKey = TSKEY_MAX;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d minKey:%" PRId64 " maxKey:%" PRId64 " expLevel:%d", TD_VID(tsdb->pVnode),
              __func__, committer->ctx->fid, committer->ctx->minKey, committer->ctx->maxKey, committer->ctx->expLevel);
  }
  return 0;
}

static int32_t tsdbCommitFileSetEnd(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  if (committer->sttReader) {
    code = tsdbSttFileReaderClose(&committer->sttReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (committer->dataWriter) {
    code = tsdbDataFileWriterClose(&committer->dataWriter, 0, committer->fopArray);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbSttFileWriterClose(&committer->sttWriter, 0, committer->fopArray);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbIterMergerClose(&committer->iterMerger);
  TARRAY2_CLEAR(committer->iterArray, tsdbIterClose);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->fid);
  }
  return code;
}

static int32_t tsdbCommitFileSet(SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  // fset commit start
  code = tsdbCommitFileSetBegin(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit fset
  code = tsdbCommitTSData(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitTombData(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // fset commit end
  code = tsdbCommitFileSetEnd(committer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(committer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->fid);
  }
  return code;
}

static int32_t tsdbOpenCommitter(STsdb *tsdb, SCommitInfo *info, SCommitter2 *committer) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(committer, 0, sizeof(committer[0]));

  committer->tsdb = tsdb;
  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &committer->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);
  committer->minutes = tsdb->keepCfg.days;
  committer->precision = tsdb->keepCfg.precision;
  committer->minRow = info->info.config.tsdbCfg.minRows;
  committer->maxRow = info->info.config.tsdbCfg.maxRows;
  committer->cmprAlg = info->info.config.tsdbCfg.compression;
  committer->sttTrigger = info->info.config.sttTrigger;
  committer->szPage = info->info.config.tsdbPageSize;
  committer->compactVersion = INT64_MAX;
  committer->ctx->cid = tsdbFSAllocEid(tsdb->pFS);
  committer->ctx->now = taosGetTimestampSec();

  committer->ctx->nextKey = tsdb->imem->minKey;
  if (tsdb->imem->nDel > 0) {
    SRBTreeIter iter[1] = {tRBTreeIterCreate(tsdb->imem->tbDataTree, 1)};

    for (SRBTreeNode *node = tRBTreeIterNext(iter); node; node = tRBTreeIterNext(iter)) {
      STbData *tbData = TCONTAINER_OF(node, STbData, rbtn);

      for (SDelData *delData = tbData->pHead; delData; delData = delData->pNext) {
        if (delData->sKey < committer->ctx->nextKey) {
          committer->ctx->nextKey = delData->sKey;
        }
      }
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbCloseCommitter(SCommitter2 *committer, int32_t eno) {
  int32_t code = 0;
  int32_t lino = 0;

  if (eno == 0) {
    code = tsdbFSEditBegin(committer->tsdb->pFS, committer->fopArray, TSDB_FEDIT_COMMIT);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    // TODO
    ASSERT(0);
  }

  ASSERT(committer->dataWriter == NULL);
  ASSERT(committer->sttWriter == NULL);
  ASSERT(committer->iterMerger == NULL);
  TARRAY2_DESTROY(committer->iterArray, NULL);
  TARRAY2_DESTROY(committer->fopArray, NULL);
  tsdbFSDestroyCopySnapshot(&committer->fsetArr);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, eid:%" PRId64, TD_VID(committer->tsdb->pVnode), __func__, lino,
              tstrerror(code), committer->ctx->cid);
  } else {
    tsdbDebug("vgId:%d %s done, eid:%" PRId64, TD_VID(committer->tsdb->pVnode), __func__, committer->ctx->cid);
  }
  return code;
}

int32_t tsdbPreCommit(STsdb *tsdb) {
  taosThreadRwlockWrlock(&tsdb->rwLock);
  ASSERT(tsdb->imem == NULL);
  tsdb->imem = tsdb->mem;
  tsdb->mem = NULL;
  taosThreadRwlockUnlock(&tsdb->rwLock);
  return 0;
}

int32_t tsdbCommitBegin(STsdb *tsdb, SCommitInfo *info) {
  if (!tsdb) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  SMemTable *imem = tsdb->imem;
  int64_t    nRow = imem->nRow;
  int64_t    nDel = imem->nDel;

  if (nRow == 0 && nDel == 0) {
    taosThreadRwlockWrlock(&tsdb->rwLock);
    tsdb->imem = NULL;
    taosThreadRwlockUnlock(&tsdb->rwLock);
    tsdbUnrefMemTable(imem, NULL, true);
  } else {
    SCommitter2 committer[1];

    code = tsdbOpenCommitter(tsdb, info, committer);
    TSDB_CHECK_CODE(code, lino, _exit);

    while (committer->ctx->nextKey != TSKEY_MAX) {
      code = tsdbCommitFileSet(committer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbCloseCommitter(committer, code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d %s done, nRow:%" PRId64 " nDel:%" PRId64, TD_VID(tsdb->pVnode), __func__, nRow, nDel);
  }
  return code;
}

int32_t tsdbCommitCommit(STsdb *tsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tsdb->imem == NULL) goto _exit;

  SMemTable *pMemTable = tsdb->imem;
  taosThreadRwlockWrlock(&tsdb->rwLock);
  code = tsdbFSEditCommit(tsdb->pFS);
  if (code) {
    taosThreadRwlockUnlock(&tsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tsdb->imem = NULL;
  taosThreadRwlockUnlock(&tsdb->rwLock);
  tsdbUnrefMemTable(pMemTable, NULL, true);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbCommitAbort(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pTsdb->imem == NULL) goto _exit;

  code = tsdbFSEditAbort(pTsdb->pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}