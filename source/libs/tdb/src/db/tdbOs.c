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

#include "tdbOs.h"

#ifndef TDB_FOR_TDENGINE

// tdbOsRead
i64 tdbOsRead(tdb_fd_t fd, void *pData, i64 nBytes) {
  i64 nRead = 0;
  i64 iRead = 0;
  u8 *pBuf = (u8 *)pData;

  while (nBytes > 0) {
    iRead = read(fd, pBuf, nBytes);
    if (iRead < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (iRead == 0) {
      break;
    }

    nRead += iRead;
    pBuf += iRead;
    nBytes -= iRead;
  }

  return nRead;
}

// tdbOsPRead
i64 tdbOsPRead(tdb_fd_t fd, void *pData, i64 nBytes, i64 offset) {
  i64 nRead = 0;
  i64 iRead = 0;
  i64 iOffset = offset;
  u8 *pBuf = (u8 *)pData;

  while (nBytes > 0) {
    iRead = pread(fd, pBuf, nBytes, iOffset);
    if (iRead < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (iRead == 0) {
      break;
    }

    nRead += iRead;
    pBuf += iRead;
    iOffset += iRead;
    nBytes -= iRead;
  }

  return nRead;
}

// tdbOsWrite
i64 tdbOsWrite(tdb_fd_t fd, const void *pData, i64 nBytes) {
  i64 nWrite = 0;
  i64 iWrite = 0;
  u8 *pBuf = (u8 *)pData;

  while (nBytes > 0) {
    iWrite = write(fd, pBuf, nBytes);
    if (iWrite < 0) {
      if (errno == EINTR) {
        continue;
      }

      return -1;
    }

    nWrite += iWrite;
    pBuf += iWrite;
    nBytes -= iWrite;
  }

  return nWrite;
}

#endif