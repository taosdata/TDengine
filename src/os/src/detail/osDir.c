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
#include "tglobal.h"
#include "tulog.h"

#ifndef TAOS_OS_FUNC_DIR

void taosRemoveDir(char *rootDir) {
  DIR *dir = opendir(rootDir);
  if (dir == NULL) return;

  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
     
    char filename[1024];
    snprintf(filename, 1023, "%s/%s", rootDir, de->d_name);
    if (de->d_type & DT_DIR) {
      taosRemoveDir(filename);
    } else {
      (void)remove(filename);
      uInfo("file:%s is removed", filename);
    }
  }

  closedir(dir);
  rmdir(rootDir);

  uInfo("dir:%s is removed", rootDir);
}

int taosMkDir(const char *path, mode_t mode) {
  int code = mkdir(path, 0755);
  if (code < 0 && errno == EEXIST) code = 0;
  return code;
}

void taosRename(char* oldName, char *newName) {
  // if newName in not empty, rename return fail. 
  // the newName must be empty or does not exist
  if (rename(oldName, newName)) {
    uError("%s is modify to %s fail, reason:%s", oldName, newName, strerror(errno));
  } else {
    uInfo("%s is modify to %s success!", oldName, newName);
  }
}

void taosRemoveOldLogFiles(char *rootDir, int32_t keepDays) {
  DIR *dir = opendir(rootDir);
  if (dir == NULL) return;

  int64_t ms = taosGetTimestampMs();
  struct dirent *de = NULL;

  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, 1023, "%s/%s", rootDir, de->d_name);
    if (de->d_type & DT_DIR) {
      continue;
    } else {
      // struct stat fState;
      // if (stat(fname, &fState) < 0) {
      //   continue;
      // }
      int32_t len = strlen(filename);
      int64_t fileMs = 0;
      for (int i = len - 1; i >= 0; ++i) {
        if (filename[i] == '.') {
          fileMs = atoll(filename + i + 1);
          break;
        }
      }

      if (fileMs <= 0) continue;
      int32_t days = (fileMs - ms) / 86400 + 1;
      if (days > keepDays) {
        (void)remove(filename);
        uInfo("file:%s is removed, days:%d keepDays:%d", filename, days, keepDays);

      } else {
        uTrace("file:%s won't be removed, days:%d keepDays:%d", filename, days, keepDays);
      }
    }
  }

  closedir(dir);
  rmdir(rootDir);
}

#endif
