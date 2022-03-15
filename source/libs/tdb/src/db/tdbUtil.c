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

#include "tdbInt.h"

int tdbGnrtFileID(const char *fname, uint8_t *fileid, bool unique) {
  int64_t stDev = 0, stIno = 0;

  if (taosDevInoFile(fname, &stDev, &stIno) < 0) {
    return -1;
  }

  memset(fileid, 0, TDB_FILE_ID_LEN);

  ((uint64_t *)fileid)[0] = stDev;
  ((uint64_t *)fileid)[1] = stIno;
  if (unique) {
    ((uint64_t *)fileid)[2] = taosRand();
  }

  return 0;
}

// int tdbCheckFileAccess(const char *pathname, int mode) {
//   int flags = 0;

//   if (mode & TDB_F_OK) {
//     flags |= F_OK;
//   }

//   if (mode & TDB_R_OK) {
//     flags |= R_OK;
//   }

//   if (mode & TDB_W_OK) {
//     flags |= W_OK;
//   }

//   return access(pathname, flags);
// }

int tdbGetFileSize(const char *fname, pgsz_t pgSize, pgno_t *pSize) {
  int         ret;
  int64_t file_size = 0;
  ret = taosStatFile(fname, &file_size, NULL);
  if (ret != 0) {
    return -1;
  }

  ASSERT(file_size % pgSize == 0);

  *pSize = file_size / pgSize;
  return 0;
}