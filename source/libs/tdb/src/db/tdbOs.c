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

// tdbOsRead
i64 tdbOsRead(tdb_fd_t fd, void *pBuf, i64 nBytes) {
  // TODO
  ASSERT(0);
  return 0;
}

// tdbOsPRead
i64 tdbOsPRead(tdb_fd_t fd, void *pBuf, i64 nBytes, i64 offset) {
  // TODO
  ASSERT(0);
  return 0;
}

// tdbOsWrite
i64 taosWriteFile(tdb_fd_t fd, const void *pBuf, i64 nBytes) {
  // TODO
  ASSERT(0);
  return 0;
}

#if 0
int tdbPRead(int fd, void *pData, int count, i64 offset) {
  void *pBuf;
  int   nbytes;
  i64   ioffset;
  int   iread;

  pBuf = pData;
  nbytes = count;
  ioffset = offset;
  while (nbytes > 0) {
    iread = pread(fd, pBuf, nbytes, ioffset);
    if (iread < 0) {
      /* TODO */
    } else if (iread == 0) {
      return (count - iread);
    }

    nbytes = nbytes - iread;
    pBuf = (void *)((u8 *)pBuf + iread);
    ioffset += iread;
  }

  return count;
}

int tdbWrite(int fd, void *pData, int count) {
  // TODO
  return write(fd, pData, count);
}
#endif