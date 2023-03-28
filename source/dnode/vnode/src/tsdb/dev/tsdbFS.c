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

#include "dev.h"

static int32_t create_file_system(STsdb *pTsdb, struct STFileSystem **ppFS) {
  ppFS[0] = taosMemoryCalloc(1, sizeof(*ppFS[0]));
  if (ppFS[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  ppFS[0]->pTsdb = pTsdb;
  return 0;
}

static int32_t destroy_file_system(struct STFileSystem **ppFS) {
  if (ppFS[0]) {
    taosMemoryFree(ppFS[0]->aFileSet);
    taosMemoryFree(ppFS[0]);
    ppFS[0] = NULL;
  }
  return 0;
}

static int32_t open_file_system(struct STFileSystem *pFS, int8_t rollback) {
  // TODO
  return 0;
}

static int32_t close_file_system(struct STFileSystem *pFS) {
  // TODO
  return 0;
}

int32_t tsdbOpenFileSystem(STsdb *pTsdb, struct STFileSystem **ppFS, int8_t rollback) {
  int32_t code;
  int32_t lino;

  code = create_file_system(pTsdb, ppFS);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = open_file_system(ppFS[0], rollback);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    if (ppFS[0]) destroy_file_system(ppFS);
  } else {
    tsdbInfo("vgId:%d %s success", TD_VID(pTsdb->pVnode), __func__);
  }
  return 0;
}

int32_t tsdbCloseFileSystem(struct STFileSystem **ppFS) {
  if (ppFS[0] == NULL) return 0;

  close_file_system(ppFS[0]);
  destroy_file_system(ppFS);
  return 0;
}
