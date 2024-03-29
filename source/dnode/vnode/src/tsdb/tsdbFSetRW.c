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

#include "tsdbFSetRW.h"

// SFSetWriter ==================================================
struct SFSetWriter {
  SFSetWriterConfig config[1];

  SSkmInfo skmTb[1];
  SSkmInfo skmRow[1];
  uint8_t *bufArr[10];

  struct {
    TABLEID tbid[1];
  } ctx[1];

  // writer
  SBlockData       blockData[2];
  int32_t          blockDataIdx;
  SDataFileWriter *dataWriter;
  SSttFileWriter  *sttWriter;
};

static int32_t tsdbFSetWriteTableDataBegin(SFSetWriter *writer, const TABLEID *tbid) {
  int32_t code = 0;
  int32_t lino = 0;

  writer->ctx->tbid->suid = tbid->suid;
  writer->ctx->tbid->uid = tbid->uid;

  code = tsdbUpdateSkmTb(writer->config->tsdb, writer->ctx->tbid, writer->skmTb);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->blockDataIdx = 0;
  for (int32_t i = 0; i < ARRAY_SIZE(writer->blockData); i++) {
    code = tBlockDataInit(&writer->blockData[i], writer->ctx->tbid, writer->skmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbFSetWriteTableDataEnd(SFSetWriter *writer) {
  if (writer->ctx->tbid->uid == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  int32_t cidx = writer->blockDataIdx;
  int32_t pidx = ((cidx + 1) & 1);
  int32_t numRow = ((writer->blockData[pidx].nRow + writer->blockData[cidx].nRow) >> 1);

  if (writer->blockData[pidx].nRow > 0 && numRow >= writer->config->minRow) {
    ASSERT(writer->blockData[pidx].nRow == writer->config->maxRow);

    SRowInfo row = {
        .suid = writer->ctx->tbid->suid,
        .uid = writer->ctx->tbid->uid,
        .row = tsdbRowFromBlockData(writer->blockData + pidx, 0),
    };

    for (int32_t i = 0; i < numRow; i++) {
      row.row.iRow = i;

      code = tsdbDataFileWriteRow(writer->dataWriter, &row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbDataFileFlush(writer->dataWriter);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (int32_t i = numRow; i < writer->blockData[pidx].nRow; i++) {
      row.row.iRow = i;
      code = tsdbDataFileWriteRow(writer->dataWriter, &row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    row.row = tsdbRowFromBlockData(writer->blockData + cidx, 0);
    for (int32_t i = 0; i < writer->blockData[cidx].nRow; i++) {
      row.row.iRow = i;
      code = tsdbDataFileWriteRow(writer->dataWriter, &row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    // pidx
    if (writer->blockData[pidx].nRow > 0) {
      code = tsdbDataFileWriteBlockData(writer->dataWriter, &writer->blockData[pidx]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // cidx
    if (writer->blockData[cidx].nRow < writer->config->minRow) {
      code = tsdbSttFileWriteBlockData(writer->sttWriter, &writer->blockData[cidx]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDataFileWriteBlockData(writer->dataWriter, &writer->blockData[cidx]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  for (int32_t i = 0; i < ARRAY_SIZE(writer->blockData); i++) {
    tBlockDataReset(&writer->blockData[i]);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFSetWriterOpen(SFSetWriterConfig *config, SFSetWriter **writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer[0] = taosMemoryCalloc(1, sizeof(*writer[0]));
  if (writer[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->config[0] = config[0];

  // data writer
  if (!config->toSttOnly) {
    SDataFileWriterConfig dataWriterConfig = {
        .tsdb = config->tsdb,
        .cmprAlg = config->cmprAlg,
        .maxRow = config->maxRow,
        .szPage = config->szPage,
        .fid = config->fid,
        .cid = config->cid,
        .did = config->did,
        .compactVersion = config->compactVersion,
        .skmTb = writer[0]->skmTb,
        .skmRow = writer[0]->skmRow,
        .bufArr = writer[0]->bufArr,
    };
    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ++ftype) {
      dataWriterConfig.files[ftype].exist = config->files[ftype].exist;
      dataWriterConfig.files[ftype].file = config->files[ftype].file;
    }

    code = tsdbDataFileWriterOpen(&dataWriterConfig, &writer[0]->dataWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt writer
  SSttFileWriterConfig sttWriterConfig = {
      .tsdb = config->tsdb,
      .maxRow = config->maxRow,
      .szPage = config->szPage,
      .cmprAlg = config->cmprAlg,
      .compactVersion = config->compactVersion,
      .did = config->did,
      .fid = config->fid,
      .cid = config->cid,
      .level = config->level,
      .skmTb = writer[0]->skmTb,
      .skmRow = writer[0]->skmRow,
      .bufArr = writer[0]->bufArr,
  };
  code = tsdbSttFileWriterOpen(&sttWriterConfig, &writer[0]->sttWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFSetWriterClose(SFSetWriter **writer, bool abort, TFileOpArray *fopArr) {
  if (writer[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = writer[0]->config->tsdb;

  // end
  if (!writer[0]->config->toSttOnly) {
    code = tsdbFSetWriteTableDataEnd(writer[0]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriterClose(&writer[0]->dataWriter, abort, fopArr);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbSttFileWriterClose(&writer[0]->sttWriter, abort, fopArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  // free
  for (int32_t i = 0; i < ARRAY_SIZE(writer[0]->blockData); i++) {
    tBlockDataDestroy(&writer[0]->blockData[i]);
  }
  for (int32_t i = 0; i < ARRAY_SIZE(writer[0]->bufArr); i++) {
    tFree(writer[0]->bufArr[i]);
  }
  tDestroyTSchema(writer[0]->skmRow->pTSchema);
  tDestroyTSchema(writer[0]->skmTb->pTSchema);
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFSetWriteRow(SFSetWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;

  if (writer->config->toSttOnly) {
    code = tsdbSttFileWriteRow(writer->sttWriter, row);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    if (writer->ctx->tbid->uid != row->uid) {
      code = tsdbFSetWriteTableDataEnd(writer);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbFSetWriteTableDataBegin(writer, (TABLEID *)row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (row->row.type == TSDBROW_ROW_FMT) {
      code = tsdbUpdateSkmRow(writer->config->tsdb, writer->ctx->tbid, TSDBROW_SVERSION(&row->row), writer->skmRow);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    TSDBKEY key = TSDBROW_KEY(&row->row);
    if (key.version <= writer->config->compactVersion        //
        && writer->blockData[writer->blockDataIdx].nRow > 0  //
        && key.ts == writer->blockData[writer->blockDataIdx].aTSKEY[writer->blockData[writer->blockDataIdx].nRow - 1]) {
      code = tBlockDataUpdateRow(&writer->blockData[writer->blockDataIdx], &row->row, writer->skmRow->pTSchema);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      if (writer->blockData[writer->blockDataIdx].nRow >= writer->config->maxRow) {
        int32_t idx = ((writer->blockDataIdx + 1) & 1);
        if (writer->blockData[idx].nRow >= writer->config->maxRow) {
          code = tsdbDataFileWriteBlockData(writer->dataWriter, &writer->blockData[idx]);
          TSDB_CHECK_CODE(code, lino, _exit);

          tBlockDataClear(&writer->blockData[idx]);
        }
        writer->blockDataIdx = idx;
      }

      code =
          tBlockDataAppendRow(&writer->blockData[writer->blockDataIdx], &row->row, writer->skmRow->pTSchema, row->uid);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFSetWriteTombRecord(SFSetWriter *writer, const STombRecord *tombRecord) {
  int32_t code = 0;
  int32_t lino = 0;

  if (writer->config->toSttOnly || tsdbSttFileWriterIsOpened(writer->sttWriter)) {
    code = tsdbSttFileWriteTombRecord(writer->sttWriter, tombRecord);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbDataFileWriteTombRecord(writer->dataWriter, tombRecord);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}
