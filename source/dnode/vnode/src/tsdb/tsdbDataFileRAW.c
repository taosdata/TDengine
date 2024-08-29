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
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  reader[0]->config[0] = config[0];

  int32_t lcn = config->file.lcn;
  if (fname) {
    if (fname) {
      TAOS_CHECK_GOTO(tsdbOpenFile(fname, config->tsdb, TD_FILE_READ, &reader[0]->fd, lcn), &lino, _exit);
    }
  } else {
    char fname1[TSDB_FILENAME_LEN];
    (void)tsdbTFileName(config->tsdb, &config->file, fname1);
    TAOS_CHECK_GOTO(tsdbOpenFile(fname1, config->tsdb, TD_FILE_READ, &reader[0]->fd, lcn), &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbDataFileRAWReaderClose(SDataFileRAWReader **reader) {
  if (reader[0] == NULL) {
    return 0;
  }

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

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  TAOS_CHECK_GOTO(
      tsdbReadFile(reader->fd, pBlock->offset, pBlock->data, pBlock->dataLength, 0, encryptAlgorithm, encryptKey),
      &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(reader->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

// SDataFileRAWWriter =============================================
int32_t tsdbDataFileRAWWriterOpen(const SDataFileRAWWriterConfig *config, SDataFileRAWWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  SDataFileRAWWriter *writer = taosMemoryCalloc(1, sizeof(SDataFileRAWWriter));
  if (!writer) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  writer->config[0] = config[0];

  TAOS_CHECK_GOTO(tsdbDataFileRAWWriterDoOpen(writer), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
    taosMemoryFree(writer);
    writer = NULL;
  }
  ppWriter[0] = writer;
  return code;
}

static int32_t tsdbDataFileRAWWriterCloseAbort(SDataFileRAWWriter *writer) {
  tsdbError("vgId:%d %s failed since not implemented", TD_VID(writer->config->tsdb->pVnode), __func__);
  return 0;
}

static int32_t tsdbDataFileRAWWriterDoClose(SDataFileRAWWriter *writer) { return 0; }

static int32_t tsdbDataFileRAWWriterCloseCommit(SDataFileRAWWriter *writer, TFileOpArray *opArr) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileOp op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = writer->config->fid,
      .nf = writer->file,
  };
  TAOS_CHECK_GOTO(TARRAY2_APPEND(opArr, op), &lino, _exit);

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  if (writer->fd) {
    TAOS_CHECK_GOTO(tsdbFsyncFile(writer->fd, encryptAlgorithm, encryptKey), &lino, _exit);
    tsdbCloseFile(&writer->fd);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
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

  (void)tsdbTFileName(writer->config->tsdb, &writer->file, fname);
  TAOS_CHECK_GOTO(tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd, writer->file.lcn), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbDataFileRAWWriterDoOpen(SDataFileRAWWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer->file = writer->config->file;
  writer->ctx->offset = 0;

  TAOS_CHECK_GOTO(tsdbDataFileRAWWriterOpenDataFD(writer), &lino, _exit);

  writer->ctx->opened = true;
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbDataFileRAWWriterClose(SDataFileRAWWriter **writer, bool abort, TFileOpArray *opArr) {
  if (writer[0] == NULL) {
    return 0;
  }

  int32_t code = 0;
  int32_t lino = 0;

  if (writer[0]->ctx->opened) {
    if (abort) {
      TAOS_CHECK_GOTO(tsdbDataFileRAWWriterCloseAbort(writer[0]), &lino, _exit);
    } else {
      TAOS_CHECK_GOTO(tsdbDataFileRAWWriterCloseCommit(writer[0], opArr), &lino, _exit);
    }
    (void)tsdbDataFileRAWWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer[0]->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbDataFileRAWWriteBlockData(SDataFileRAWWriter *writer, const STsdbDataRAWBlockHeader *pDataBlock,
                                      int32_t encryptAlgorithm, char *encryptKey) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_GOTO(tsdbWriteFile(writer->fd, writer->ctx->offset, (const uint8_t *)pDataBlock->data,
                                pDataBlock->dataLength, encryptAlgorithm, encryptKey),
                  &lino, _exit);

  writer->ctx->offset += pDataBlock->dataLength;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}
