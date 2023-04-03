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

const char *tsdb_ftype_suffix[] = {
    ".head",  // TSDB_FTYPE_HEAD
    ".data",  // TSDB_FTYPE_DATA
    ".sma",   // TSDB_FTYPE_SMA
    ".tomb",  // TSDB_FTYPE_TOMB
    NULL,     // TSDB_FTYPE_MAX
    ".stt",
};

int32_t tsdbTFileInit(STsdb *pTsdb, struct STFile *pFile) {
  SVnode *pVnode = pTsdb->pVnode;
  STfs   *pTfs = pVnode->pTfs;

  if (pTfs) {
    snprintf(pFile->fname, TSDB_FILENAME_LEN, "%s%s%s%sv%df%dver%" PRId64 "%s", tfsGetDiskPath(pTfs, pFile->diskId),
             TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pVnode), pFile->fid, pFile->cid, tsdb_ftype_suffix[pFile->type]);
  } else {
    snprintf(pFile->fname, TSDB_FILENAME_LEN, "%s%sv%df%dver%" PRId64 "%s", pTsdb->path, TD_DIRSEP, TD_VID(pVnode),
             pFile->fid, pFile->cid, tsdb_ftype_suffix[pFile->type]);
  }
  pFile->ref = 1;
  return 0;
}

int32_t tsdbTFileClear(struct STFile *pFile) {
  int32_t code = 0;
  // TODO
  return code;
}