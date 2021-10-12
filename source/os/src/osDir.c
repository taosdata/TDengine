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
#include "osString.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
/*
 * windows implementation
 */

// todo

#else
/*
 * linux implementation
 */

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wordexp.h>

void taosRemoveDir(char *dirname) {
  DIR *dir = opendir(dirname);
  if (dir == NULL) return;

  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, sizeof(filename), "%s/%s", dirname, de->d_name);
    if (de->d_type & DT_DIR) {
      taosRemoveDir(filename);
    } else {
      (void)remove(filename);
      printf("file:%s is removed\n", filename);
    }
  }

  closedir(dir);
  rmdir(dirname);

  printf("dir:%s is removed", dirname);
}

bool taosDirExist(char *dirname) { return access(dirname, F_OK) == 0; }

bool taosMkDir(char *dirname) {
  int32_t code = mkdir(dirname, 0755);
  if (code < 0 && errno == EEXIST) {
    return true;
  }

  return code == 0;
}

void taosRemoveOldFiles(char *dirname, int32_t keepDays) {
  DIR *dir = opendir(dirname);
  if (dir == NULL) return;

  int64_t        sec = taosGetTimestampSec();
  struct dirent *de = NULL;

  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, sizeof(filename), "%s/%s", dirname, de->d_name);
    if (de->d_type & DT_DIR) {
      continue;
    } else {
      int32_t len = (int32_t)strlen(filename);
      if (len > 3 && strcmp(filename + len - 3, ".gz") == 0) {
        len -= 3;
      }

      int64_t fileSec = 0;
      for (int32_t i = len - 1; i >= 0; i--) {
        if (filename[i] == '.') {
          fileSec = atoll(filename + i + 1);
          break;
        }
      }

      if (fileSec <= 100) continue;
      int32_t days = (int32_t)(ABS(sec - fileSec) / 86400 + 1);
      if (days > keepDays) {
        (void)remove(filename);
        printf("file:%s is removed, days:%d keepDays:%d", filename, days, keepDays);
      } else {
        printf("file:%s won't be removed, days:%d keepDays:%d", filename, days, keepDays);
      }
    }
  }

  closedir(dir);
  rmdir(dirname);
}

bool taosExpandDir(char *dirname, char *outname, int32_t maxlen) {
  wordexp_t full_path;
  if (0 != wordexp(dirname, &full_path, 0)) {
    printf("failed to expand path:%s since %s", dirname, strerror(errno));
    wordfree(&full_path);
    return false;
  }

  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strncpy(outname, full_path.we_wordv[0], maxlen);
  }

  wordfree(&full_path);

  return true;
}

bool taosRealPath(char *dirname, int32_t maxlen) {
  char tmp[PATH_MAX] = {0};
  if (realpath(dirname, tmp) != NULL) {
    strncpy(dirname, tmp, maxlen);
  }

  return true;
}

#endif