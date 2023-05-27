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
  // TODO
};

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(config->pTsdb->pVnode);
// TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbDataFileWriterClose(SDataFileWriter *pWriter) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = 0;  // TODO: TD_VID(config->pTsdb->pVnode);
// TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbDataFileWriteTSData(SDataFileWriter *pWriter, SBlockData *pBlockData) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = 0;  // TODO: TD_VID(config->pTsdb->pVnode);
// TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbDataFileWriteTSDataBlock(SDataFileWriter *pWriter, SBlockData *pBlockData) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = 0;  // TODO: TD_VID(config->pTsdb->pVnode);
// TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}