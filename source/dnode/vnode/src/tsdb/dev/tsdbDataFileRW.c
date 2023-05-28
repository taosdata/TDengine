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

#include "inc/tsdbDataFileRW.h"

// SDataFileReader =============================================
struct SDataFileReader {
  // TODO
};

// SDataFileWriter =============================================
struct SDataFileWriter {
  SDataFileWriterConfig config[1];
  struct {
    bool opened;
  } ctx[1];
  // TODO
};

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **writer) {
  writer[0] = taosMemoryCalloc(1, sizeof(SDataFileWriter));
  if (!writer[0]) return TSDB_CODE_OUT_OF_MEMORY;
  writer[0]->ctx->opened = false;
  return 0;
}

static int32_t tsdbDataFileWriterCloseCommit(SDataFileWriter *writer) {
  // TODO
  return 0;
}
static int32_t tsdbDataFileWriterCloseAbort(SDataFileWriter *writer) {
  // TODO
  return 0;
}
static int32_t tsdbDataFileWriterDoClose(SDataFileWriter *writer) {
  // TODO
  return 0;
}
static int32_t tsdbDataFileWriterCloseImpl(SDataFileWriter *writer, bool abort, STFileOp op[/*TSDB_FTYPE_MAX*/]) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  if (!writer->ctx->opened) {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) op[i].optype = TSDB_FOP_NONE;
  } else {
    if (abort) {
      code = tsdbDataFileWriterCloseAbort(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDataFileWriterCloseCommit(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbDataFileWriterDoClose(writer);
  }
  taosMemoryFree(writer);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}
int32_t tsdbDataFileWriterClose(SDataFileWriter **writer, bool abort, STFileOp op[/*TSDB_FTYPE_MAX*/]) {
  int32_t code = tsdbDataFileWriterCloseImpl(writer[0], abort, op);
  if (code) {
    return code;
  } else {
    writer[0] = NULL;
    return 0;
  }
}

static int32_t tsdbDataFileWriterDoOpen(SDataFileWriter *writer) {
  // TODO
  return 0;
}
int32_t tsdbDataFileWriteTSData(SDataFileWriter *writer, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  if (!writer->ctx->opened) {
    code = tsdbDataFileWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}
