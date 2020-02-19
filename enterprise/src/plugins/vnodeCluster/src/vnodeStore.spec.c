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
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "ttier.h"
#include "vnode.h"

int vnodeInitInfo() {
  struct dirent *de = NULL;
  struct dirent *tDe = NULL;
  char           path[TSDB_FILENAME_LEN] = "\0";
  char           hPath[TSDB_FILENAME_LEN] = "\0";
  DIR *          dir = opendir(tsDirectory);
  if (dir == NULL) return -1;

  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
    if (de->d_type & DT_DIR) {
      sprintf(path, "%s/%s/db", tsDirectory, de->d_name);
      DIR *tDir = opendir(path);
      if (tDir == NULL) continue;

      while ((tDe = readdir(tDir)) != NULL) {
        if (strcmp(tDe->d_name, ".") == 0 || strcmp(tDe->d_name, "..") == 0) continue;
        if (strcmp(tDe->d_name + strlen(tDe->d_name) - strlen(".head"), ".head") == 0 && (tDe->d_type & DT_LNK)) {
          sprintf(hPath, "%s/%s", path, tDe->d_name);
          SDisk *disk = taosGetDiskFromHeadFile(hPath);
          if (disk != NULL) __sync_fetch_and_add(&(disk->numOfFiles), 1);
        }
      }
      closedir(tDir);
    }
  }

  closedir(dir);
  return 0;
}

bool vnodeRemoveDataFileFromLinkFile(char* linkFile, char* de_name) {
  SDisk *disk = taosGetDiskFromHeadFile(linkFile);
  if (disk == NULL) {
    remove(linkFile);
    dTrace("linkFile:%s link to a non-existent file", linkFile);
    return false;
  }
  if (strcmp(de_name + strlen(de_name) - strlen(".head"), ".head") == 0 && disk != NULL) {
    __sync_fetch_and_sub(&(disk->numOfFiles), 1);
  }

  return true;
}