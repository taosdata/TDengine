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

#define treadfile(fd, buf, count) read(fd, buf, count)
#define twritefile(fd, buf, count) write(fd, buf, count)
#define tlseekfile(fd, offset, whence) lseek(fd, offset, whence)
#define tclosefile(fd)    \
  {                       \
    if (FD_VALID(x)) {    \
      close(x);           \
      x = FD_INITIALIZER; \
    }                     \
  }

int64_t taosRead(int32_t fd, void *buf, int64_t count);
int64_t taosWrite(int32_t fd, void *buf, int64_t count);
int64_t taosLSeek(int32_t fd, int64_t offset, int32_t whence);
int32_t taosRenameFile(char *fullPath, char *suffix, char delimiter, char **dstPath);


// TAOS_OS_FUNC_FILE_SENDIFLE
int64_t taosSendFile(int32_t dfd, int32_t sfd, int64_t *offset, int64_t size);
int64_t taosFSendFile(FILE *outfile, FILE *infile, int64_t *offset, int64_t size);

// TAOS_OS_FUNC_FILE_GETTMPFILEPATH
void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath);

// TAOS_OS_FUNC_FILE_FTRUNCATE
int32_t taosFtruncate(int32_t fd, int64_t length);
#ifdef __cplusplus
}
#endif

#endif
