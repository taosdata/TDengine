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

#include "tsdbDataFileRAW.h"

// SDataFileRAWReader =============================================
int32_t tsdbDataFileRAWReaderOpen(const char *fname, const SDataFileRAWReaderConfig *config,
                                  SDataFileRAWReader **reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = taosMemoryCalloc(1, sizeof(SDataFileRAWReader));
  if (reader[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  reader[0]->config[0] = config[0];

  if (fname) {
    if (fname) {
      code = tsdbOpenFile(fname, config->tsdb, TD_FILE_READ, &reader[0]->fd);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    char fname1[TSDB_FILENAME_LEN];
    tsdbTFileName(config->tsdb, &config->file, fname1);
    code = tsdbOpenFile(fname1, config->tsdb, TD_FILE_READ, &reader[0]->fd);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileRAWReaderClose(SDataFileRAWReader **reader) {
  if (reader[0] == NULL) return 0;

  if (reader[0]->fd) {
    tsdbCloseFile(&reader[0]->fd);
  }

  taosMemoryFree(reader[0]);
  reader[0] = NULL;
  return 0;
}

int32_t tsdbDataFileRAWReadBlockData(SDataFileRAWReader *reader, STsdbDataRAWBlockHeader *pBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  pBlock->file.type = reader->config->file.type;
  pBlock->file.fid = reader->config->file.fid;
  pBlock->file.cid = reader->config->file.cid;
  pBlock->file.size = reader->config->file.size;
  pBlock->file.minVer = reader->config->file.minVer;
  pBlock->file.maxVer = reader->config->file.maxVer;
  pBlock->file.stt->level = reader->config->file.stt->level;

  code = tsdbReadFile(reader->fd, pBlock->offset, pBlock->data, pBlock->dataLength, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

// SDataFileRAWWriter =============================================
int32_t tsdbDataFileRAWWriterOpen(const SDataFileRAWWriterConfig *config, SDataFileRAWWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  SDataFileRAWWriter *writer = taosMemoryCalloc(1, sizeof(SDataFileRAWWriter));
  if (!writer) return TSDB_CODE_OUT_OF_MEMORY;

  writer->config[0] = config[0];

  code = tsdbDataFileRAWWriterDoOpen(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    taosMemoryFree(writer);
    writer = NULL;
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  ppWriter[0] = writer;
  return code;
}

static int32_t tsdbDataFileRAWWriterCloseAbort(SDataFileRAWWriter *writer) {
  ASSERT(0);
  return 0;
}

static int32_t tsdbDataFileRAWWriterDoClose(SDataFileRAWWriter *writer) { return 0; }

static int32_t tsdbDataFileRAWWriterCloseCommit(SDataFileRAWWriter *writer, TFileOpArray *opArr) {
  int32_t  code = 0;
  int32_t  lino = 0;
  ASSERT(writer->ctx->offset <= writer->file.size);
  ASSERT(writer->config->fid == writer->file.fid);

  STFileOp op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = writer->config->fid,
      .nf = writer->file,
  };
  code = TARRAY2_APPEND(opArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (writer->fd) {
    code = tsdbFsyncFile(writer->fd);
    TSDB_CHECK_CODE(code, lino, _exit);
    tsdbCloseFile(&writer->fd);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileRAWWriterOpenDataFD(SDataFileRAWWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  char    fname[TSDB_FILENAME_LEN];
  int32_t flag = TD_FILE_READ | TD_FILE_WRITE;

  if (writer->ctx->offset == 0) {
    flag |= (TD_FILE_CREATE | TD_FILE_TRUNC);
  }

  tsdbTFileName(writer->config->tsdb, &writer->file, fname);
  code = tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileRAWWriterDoOpen(SDataFileRAWWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer->file = writer->config->file;
  writer->ctx->offset = 0;

  code = tsdbDataFileRAWWriterOpenDataFD(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->opened = true;
_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileRAWWriterClose(SDataFileRAWWriter **writer, bool abort, TFileOpArray *opArr) {
  if (writer[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  if (writer[0]->ctx->opened) {
    if (abort) {
      code = tsdbDataFileRAWWriterCloseAbort(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDataFileRAWWriterCloseCommit(writer[0], opArr);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbDataFileRAWWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer[0]->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileRAWWriteBlockData(SDataFileRAWWriter *writer, const STsdbDataRAWBlockHeader *pDataBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbWriteFile(writer->fd, writer->ctx->offset, (const uint8_t *)pDataBlock->data, pDataBlock->dataLength);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->offset += pDataBlock->dataLength;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}
