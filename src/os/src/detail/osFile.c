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
#include "ttime.h"

#ifndef TAOS_OS_FUNC_FILE

void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath) {
  const char* tdengineTmpFileNamePrefix = "tdengine-";
  
  char tmpPath[PATH_MAX];
  char *tmpDir = "/tmp/";
  
  strcpy(tmpPath, tmpDir);
  strcat(tmpPath, tdengineTmpFileNamePrefix);
  if (strlen(tmpPath) + strlen(fileNamePrefix) + strlen("-%d-%s") < PATH_MAX) {
    strcat(tmpPath, fileNamePrefix);
    strcat(tmpPath, "-%d-%s");
  }
  
  char rand[8] = {0};
  taosRandStr(rand, tListLen(rand) - 1);
  snprintf(dstPath, PATH_MAX, tmpPath, getpid(), rand);
}

#endif

// rename file name
int32_t taosFileRename(char *fullPath, char *suffix, char delimiter, char **dstPath) {
  int32_t ts = taosGetTimestampSec();

  char fname[PATH_MAX] = {0};  // max file name length must be less than 255

  char *delimiterPos = strrchr(fullPath, delimiter);
  if (delimiterPos == NULL) return -1;

  int32_t fileNameLen = 0;
  if (suffix)
    fileNameLen = snprintf(fname, PATH_MAX, "%s.%d.%s", delimiterPos + 1, ts, suffix);
  else
    fileNameLen = snprintf(fname, PATH_MAX, "%s.%d", delimiterPos + 1, ts);

  size_t len = (size_t)((delimiterPos - fullPath) + fileNameLen + 1);
  if (*dstPath == NULL) {
    *dstPath = calloc(1, len + 1);
    if (*dstPath == NULL) return -1;
  }

  strncpy(*dstPath, fullPath, (size_t)(delimiterPos - fullPath + 1));
  strncat(*dstPath, fname, (size_t)fileNameLen);
  (*dstPath)[len] = 0;

  return rename(fullPath, *dstPath);
}
