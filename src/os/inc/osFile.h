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

#ifndef TDENGINE_OS_FILE_H
#define TDENGINE_OS_FILE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "osSocket.h"

int64_t taosReadImp(int32_t fd, void *buf, int64_t count);
int64_t taosWriteImp(int32_t fd, void *buf, int64_t count);
int64_t taosLSeekImp(int32_t fd, int64_t offset, int32_t whence);
int32_t taosRenameFile(char *fullPath, char *suffix, char delimiter, char **dstPath);

#define taosRead(fd, buf, count) taosReadImp(fd, buf, count)
#define taosWrite(fd, buf, count) taosWriteImp(fd, buf, count)
#define taosLSeek(fd, offset, whence) taosLSeekImp(fd, offset, whence)
#define taosClose(fd)      \
  {                        \
    if (FD_VALID(fd)) {    \
      close(fd);           \
      fd = FD_INITIALIZER; \
    }                      \
  }

// TAOS_OS_FUNC_FILE_SENDIFLE
int64_t taosSendFile(int32_t dfd, int32_t sfd, int64_t *offset, int64_t size);
int64_t taosFSendFile(FILE *outfile, FILE *infile, int64_t *offset, int64_t size);

#ifdef TAOS_RANDOM_FILE_FAIL
  void taosSetRandomFileFailFactor(int32_t factor);
  void taosSetRandomFileFailOutput(const char *path);
  #ifdef TAOS_RANDOM_FILE_FAIL_TEST
    int64_t taosReadFileRandomFail(int32_t fd, void *buf, int32_t count, const char *file, uint32_t line);
    int64_t taosWriteFileRandomFail(int32_t fd, void *buf, int32_t count, const char *file, uint32_t line);
    int64_t taosLSeekRandomFail(int32_t fd, int64_t offset, int32_t whence, const char *file, uint32_t line);
    #undef taosRead
    #undef taosWrite
    #undef taosLSeek
    #define taosRead(fd, buf, count) taosReadFileRandomFail(fd, buf, count, __FILE__, __LINE__)
    #define taosWrite(fd, buf, count) taosWriteFileRandomFail(fd, buf, count, __FILE__, __LINE__)
    #define taosLSeek(fd, offset, whence) taosLSeekRandomFail(fd, offset, whence, __FILE__, __LINE__)
  #endif  
#endif 

// TAOS_OS_FUNC_FILE_GETTMPFILEPATH
void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath);

// TAOS_OS_FUNC_FILE_FTRUNCATE
int32_t taosFtruncate(int32_t fd, int64_t length);
#ifdef __cplusplus
}
#endif

#endif
