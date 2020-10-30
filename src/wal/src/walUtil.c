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
#include "walInt.h"

int32_t walGetNextFile(SWal *pWal, int64_t *nextFileId) {
  int64_t curFileId = *nextFileId;
  int64_t nearFileId = INT64_MAX;

  DIR *dir = opendir(pWal->path);
  if (dir == NULL) {
    wError("vgId:%d, path:%s, failed to open since %s", pWal->vgId, pWal->path, strerror(errno));
    return -1;
  }

  struct dirent *ent;
  while ((ent = readdir(dir)) != NULL) {
    char *name = ent->d_name;

    if (strncmp(name, WAL_PREFIX, WAL_PREFIX_LEN) == 0) {
      int64_t id = atoll(name + WAL_PREFIX_LEN);
      if (id <= curFileId) continue;

      if (id < nearFileId) {
        nearFileId = id;
      }
    }
  }
  closedir(dir);

  if (nearFileId == INT64_MAX) return -1;

  *nextFileId = nearFileId;
  wTrace("vgId:%d, path:%s, curFileId:%" PRId64 " nextFileId:%" PRId64, pWal->vgId, pWal->path, curFileId, *nextFileId);

  return 0;
}

int32_t walGetOldFile(SWal *pWal, int64_t curFileId, int32_t minDiff, int64_t *oldFileId) {
  int64_t nearFileId = INT64_MAX;

  DIR *dir = opendir(pWal->path);
  if (dir == NULL) {
    wError("vgId:%d, path:%s, failed to open since %s", pWal->vgId, pWal->path, strerror(errno));
    return -1;
  }

  struct dirent *ent;
  while ((ent = readdir(dir)) != NULL) {
    char *name = ent->d_name;

    if (strncmp(name, WAL_PREFIX, WAL_PREFIX_LEN) == 0) {
      int64_t id = atoll(name + WAL_PREFIX_LEN);
      if (id >= curFileId) continue;

      minDiff--;
      if (id < nearFileId) {
        nearFileId = id;
      }
    }
  }
  closedir(dir);

  if (nearFileId == INT64_MAX) return -1;
  if (minDiff > 0) return -1;

  *oldFileId = nearFileId;
  wTrace("vgId:%d, path:%s, curFileId:%" PRId64 " oldFildId:%" PRId64, pWal->vgId, pWal->path, curFileId, *oldFileId);

  return 0;
}