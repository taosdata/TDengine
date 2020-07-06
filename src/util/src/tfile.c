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

#include <stdio.h>
#include <stdlib.h>
#include <error.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "os.h"

#define RANDOM_FILE_FAIL_FACTOR 5

ssize_t taos_tread(int fd, void *buf, size_t count)
{
#ifdef TAOS_RANDOM_FILE_FAIL
  if (rand() % RANDOM_FILE_FAIL_FACTOR == 0) {
    errno = EIO;
    return -1;
  }
#endif

  return tread(fd, buf, count);
}

ssize_t taos_twrite(int fd, void *buf, size_t count)
{
#ifdef TAOS_RANDOM_FILE_FAIL
  if (rand() % RANDOM_FILE_FAIL_FACTOR == 0) {
    errno = EIO;
    return -1;
  }
#endif

  return twrite(fd, buf, count);
}

off_t taos_lseek(int fd, off_t offset, int whence)
{
#ifdef TAOS_RANDOM_FILE_FAIL
  if (rand() % RANDOM_FILE_FAIL_FACTOR == 0) {
    errno = EIO;
    return -1;
  }
#endif

  return lseek(fd, offset, whence);
}
