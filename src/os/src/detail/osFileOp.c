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

#define _DEFAULT_SOURCE
#include "os.h"

ssize_t taosTReadImp(int fd, void *buf, size_t count) {
  size_t  leftbytes = count;
  ssize_t readbytes;
  char *  tbuf = (char *)buf;

  while (leftbytes > 0) {
    readbytes = read(fd, (void *)tbuf, leftbytes);
    if (readbytes < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (readbytes == 0) {
      return (ssize_t)(count - leftbytes);
    }

    leftbytes -= readbytes;
    tbuf += readbytes;
  }

  return (ssize_t)count;
}

ssize_t taosTWriteImp(int fd, void *buf, size_t n) {
  size_t nleft = n; 
  ssize_t nwritten = 0;
  char *tbuf = (char *)buf;

  while (nleft > 0) {
    nwritten = write(fd, (void *)tbuf, nleft);
    if (nwritten < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    nleft -= nwritten;
    tbuf += nwritten;
  }

  return n;
}

#ifndef TAOS_OS_FUNC_FILEOP

ssize_t taosTSendFileImp(int dfd, int sfd, off_t *offset, size_t size) {
  size_t  leftbytes = size;
  ssize_t sentbytes;

  while (leftbytes > 0) {
    /*
     * TODO : Think to check if file is larger than 1GB
     */
    //if (leftbytes > 1000000000) leftbytes = 1000000000;
    sentbytes = sendfile(dfd, sfd, offset, leftbytes);
    if (sentbytes == -1) {
      if (errno == EINTR) {
        continue;
      }
      else {
        return -1;
      }
    } else if (sentbytes == 0) {
      return (ssize_t)(size - leftbytes);
    }

    leftbytes -= sentbytes;
  }

  return size;
}

#endif