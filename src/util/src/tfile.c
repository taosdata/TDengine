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

#include "os.h"
#include "taoserror.h"
#include "tulog.h"
#include "tutil.h"
#include "tref.h"

static int32_t tsFileRsetId = -1;

static void taosCloseFile(void *p) {
  close((int32_t)(uintptr_t)p);
}

int32_t tfinit() {
  tsFileRsetId = taosOpenRef(2000, taosCloseFile);
  return tsFileRsetId;
}

void tfcleanup() {
  if (tsFileRsetId >= 0) taosCloseRef(tsFileRsetId);
  tsFileRsetId = -1;
}

int64_t tfopen(const char *pathname, int32_t flags) {
  int32_t fd = open(pathname, flags);

  if (fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  } 

  void *p = (void *)(int64_t)fd;
  int64_t rid = taosAddRef(tsFileRsetId, p);
  if (rid < 0) close(fd);

  return rid;
}

int64_t tfclose(int64_t tfd) {
  return taosRemoveRef(tsFileRsetId, tfd);
}

int64_t tfwrite(int64_t tfd, void *buf, int64_t count) {
  void *p = taosAcquireRef(tsFileRsetId, tfd);
  if (p == NULL) return -1;

  int32_t fd = (int32_t)(uintptr_t)p;

  int64_t ret = taosWrite(fd, buf, count);
  if (ret < 0) terrno = TAOS_SYSTEM_ERROR(errno);

  taosReleaseRef(tsFileRsetId, tfd);
  return ret;
}

int64_t tfread(int64_t tfd, void *buf, int64_t count) {
  void *p = taosAcquireRef(tsFileRsetId, tfd);
  if (p == NULL) return -1;

  int32_t fd = (int32_t)(uintptr_t)p;

  int64_t ret = taosRead(fd, buf, count);
  if (ret < 0) terrno = TAOS_SYSTEM_ERROR(errno);

  taosReleaseRef(tsFileRsetId, tfd);
  return ret;
}
