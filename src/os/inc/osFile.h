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

#define FD_VALID(x) ((x) > STDERR_FILENO)
#define FD_INITIALIZER  ((int32_t)-1)

#ifndef PATH_MAX
  #define PATH_MAX 256
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
typedef int32_t FileFd;
typedef SOCKET  SocketFd;
#else
typedef int32_t FileFd;
typedef int32_t SocketFd;
#endif

int64_t taosRead(FileFd fd, void *buf, int64_t count);
int64_t taosWrite(FileFd fd, void *buf, int64_t count);

int64_t taosLSeek(FileFd fd, int64_t offset, int32_t whence);
int32_t taosFtruncate(FileFd fd, int64_t length);
int32_t taosFsync(FileFd fd);

int32_t taosRename(char* oldName, char *newName);
int64_t taosCopy(char *from, char *to);

int64_t taosSendFile(SocketFd dfd, FileFd sfd, int64_t *offset, int64_t size);
int64_t taosFSendFile(FILE *outfile, FILE *infile, int64_t *offset, int64_t size);

void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath);
void taosClose(FileFd fd);

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

#ifdef __cplusplus
}
#endif

#endif
