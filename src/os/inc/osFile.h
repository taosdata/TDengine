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

#define tread(fd, buf, count) read(fd, buf, count)
#define twrite(fd, buf, count) write(fd, buf, count)
#define tlseek(fd, offset, whence) lseek(fd, offset, whence)
#define tclose(fd)    \
  {                       \
    if (FD_VALID(fd)) {    \
      close(fd);           \
      fd = FD_INITIALIZER; \
    }                     \
  }

int64_t taosRead(int32_t fd, void *buf, int64_t count);
int64_t taosWrite(int32_t fd, void *buf, int64_t count);
int64_t taosLSeek(int32_t fd, int64_t offset, int32_t whence);
int32_t taosRenameFile(char *fullPath, char *suffix, char delimiter, char **dstPath);
#define taosClose(x) tclose(x)

// TAOS_OS_FUNC_FILE_SENDIFLE
int64_t taosSendFile(int32_t dfd, int32_t sfd, int64_t *offset, int64_t size);
int64_t taosFSendFile(FILE *outfile, FILE *infile, int64_t *offset, int64_t size);

#ifdef TAOS_RANDOM_FILE_FAIL
  void taosSetRandomFileFailFactor(int factor);
  void taosSetRandomFileFailOutput(const char *path);
  #ifdef TAOS_RANDOM_FILE_FAIL_TEST
    ssize_t taosReadFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
    ssize_t taosWriteFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
    off_t taosLSeekRandomFail(int fd, off_t offset, int whence, const char *file, uint32_t line);
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
