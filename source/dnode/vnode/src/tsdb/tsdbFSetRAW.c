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

#include "tsdbFSetRAW.h"
#include "tsdbFS2.h"

// SFSetRAWWriter ==================================================
typedef struct SFSetRAWWriter {
  SFSetRAWWriterConfig config[1];

  struct {
    TFileOpArray fopArr[1];
    STFile       file;
    int64_t      offset;
  } ctx[1];

  // writer
  SDataFileRAWWriter *dataWriter;
} SFSetRAWWriter;

int32_t tsdbFSetRAWWriterOpen(SFSetRAWWriterConfig *config, SFSetRAWWriter **writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer[0] = taosMemoryCalloc(1, sizeof(SFSetRAWWriter));
  if (writer[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->config[0] = config[0];

  TARRAY2_INIT(writer[0]->ctx->fopArr);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbFSetRAWWriterFinish(SFSetRAWWriter *writer, TFileOpArray *fopArr) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = writer->config->tsdb;

  STFileOp op;
  TARRAY2_FOREACH(writer->ctx->fopArr, op) {
    code = TARRAY2_APPEND(fopArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TARRAY2_CLEAR(writer->ctx->fopArr, NULL);
_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbFSetRAWWriteFileDataBegin(SFSetRAWWriter *writer, STsdbDataRAWBlockHeader *bHdr) {
  int32_t code = 0;
  int32_t lino = 0;

  SDataFileRAWWriterConfig config = {
      .tsdb = writer->config->tsdb,
      .szPage = writer->config->szPage,
      .fid = bHdr->file.fid,
      .did = writer->config->did,
      .cid = bHdr->file.cid,
      .level = writer->config->level,

      .file =
          {
              .type = bHdr->file.type,
              .fid = bHdr->file.fid,
              .did = writer->config->did,
              .cid = bHdr->file.cid,
              .size = bHdr->file.size,
              .minVer = bHdr->file.minVer,
              .maxVer = bHdr->file.maxVer,
              .stt = {{
                  .level = bHdr->file.stt->level,
              }},
          },
  };

  tsdbFSUpdateEid(config.tsdb->pFS, config.cid);
  writer->ctx->offset = 0;
  writer->ctx->file = config.file;

  code = tsdbDataFileRAWWriterOpen(&config, &writer->dataWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbFSetRAWWriteFileDataEnd(SFSetRAWWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbDataFileRAWWriterClose(&writer->dataWriter, false, writer->ctx->fopArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFSetRAWWriterClose(SFSetRAWWriter **writer, bool abort, TFileOpArray *fopArr) {
  if (writer[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = writer[0]->config->tsdb;

  // end
  code = tsdbFSetRAWWriteFileDataEnd(writer[0]);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileRAWWriterClose(&writer[0]->dataWriter, abort, writer[0]->ctx->fopArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSetRAWWriterFinish(writer[0], fopArr);
  TSDB_CHECK_CODE(code, lino, _exit);
  // free
  TARRAY2_DESTROY(writer[0]->ctx->fopArr, NULL);
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFSetRAWWriteBlockData(SFSetRAWWriter *writer, STsdbDataRAWBlockHeader *bHdr) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(writer->ctx->offset >= 0 && writer->ctx->offset <= writer->ctx->file.size);

  if (writer->ctx->offset == writer->ctx->file.size) {
    code = tsdbFSetRAWWriteFileDataEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbFSetRAWWriteFileDataBegin(writer, bHdr);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbDataFileRAWWriteBlockData(writer->dataWriter, bHdr);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->offset += bHdr->dataLength;
  ASSERT(writer->ctx->offset == writer->dataWriter->ctx->offset);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}
