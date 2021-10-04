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

#ifndef _TD_OS_FILE_H_
#define _TD_OS_FILE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "osSocket.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
typedef int32_t FileFd;
#else
typedef int32_t FileFd;
#endif

#define FD_INITIALIZER ((int32_t)-1)

#ifndef PATH_MAX
#define PATH_MAX 256
#endif

int32_t taosLockFile(FileFd fd);
int32_t taosUnLockFile(FileFd fd);

int32_t taosUmaskFile(FileFd fd);

int32_t taosStatFile(const char *path, int64_t *size, int32_t *mtime);
int32_t taosFStatFile(FileFd fd, int64_t *size, int32_t *mtime);

FileFd taosOpenFileWrite(const char *path);
FileFd taosOpenFileCreateWrite(const char *path);
FileFd taosOpenFileCreateWriteTrunc(const char *path);
FileFd taosOpenFileCreateWriteAppend(const char *path);
FileFd taosOpenFileRead(const char *path);
FileFd taosOpenFileReadWrite(const char *path);

int64_t taosLSeekFile(FileFd fd, int64_t offset, int32_t whence);
int32_t taosFtruncateFile(FileFd fd, int64_t length);
int32_t taosFsyncFile(FileFd fd);

int64_t taosReadFile(FileFd fd, void *buf, int64_t count);
int64_t taosWriteFile(FileFd fd, void *buf, int64_t count);

void taosCloseFile(FileFd fd);

int32_t taosRenameFile(char *oldName, char *newName);
int64_t taosCopyFile(char *from, char *to);

void taosGetTmpfilePath(const char *inputTmpDir, const char *fileNamePrefix, char *dstPath);

int64_t taosSendFile(SocketFd dfd, FileFd sfd, int64_t *offset, int64_t size);
int64_t taosFSendFile(FILE *outfile, FILE *infile, int64_t *offset, int64_t size);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_FILE_H_*/
