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
#include "zlib.h"

#define COMPRESS_STEP_SIZE 163840

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
#ifdef WINDOWS
  remove(newName);
#endif
  if (rename(oldName, newName)) {
    uError("failed to rename file %s to %s, reason:%s", oldName, newName, strerror(errno));
  } else {
    uInfo("successfully to rename file %s to %s", oldName, newName);
  }
}

void taosRemoveOldLogFiles(char *rootDir, int32_t keepDays) {
  DIR *dir = opendir(rootDir);
  if (dir == NULL) return;

  int64_t sec = taosGetTimestampSec();
  struct dirent *de = NULL;

  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, 1023, "%s/%s", rootDir, de->d_name);
    if (de->d_type & DT_DIR) {
      continue;
    } else {
      int32_t len = (int32_t)strlen(filename);
      if (len > 3 && strcmp(filename + len - 3, ".gz") == 0) {
        len -= 3;
      }

      int64_t fileSec = 0;
      for (int i = len - 1; i >= 0; i--) {
        if (filename[i] == '.') {
          fileSec = atoll(filename + i + 1);
          break;
        }
      }

      if (fileSec <= 100) continue;
      int32_t days = (int32_t)(ABS(sec - fileSec) / 86400 + 1);
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

int32_t taosCompressFile(char *srcFileName, char *destFileName) {
  int32_t ret = 0;
  int32_t len = 0;
  char *  data = malloc(COMPRESS_STEP_SIZE);
  FILE *  srcFp = NULL;
  gzFile  dstFp = NULL;

  srcFp = fopen(srcFileName, "r");
  if (srcFp == NULL) {
    ret = -1;
    goto cmp_end;
  }

  int32_t fd = open(destFileName, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    ret = -2;
    goto cmp_end;
  }

  dstFp = gzdopen(fd, "wb6f");
  if (dstFp == NULL) {
    ret = -3;
    close(fd);
    goto cmp_end;
  }

  while (!feof(srcFp)) {
    len = (int32_t)fread(data, 1, COMPRESS_STEP_SIZE, srcFp);
    (void)gzwrite(dstFp, data, len);
  }

cmp_end:
  if (srcFp) {
    fclose(srcFp);
  }
  if (dstFp) {
    gzclose(dstFp);
  }
  free(data);

  return ret;
}
