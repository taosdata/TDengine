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

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following sectio
#ifndef ALLOW_FORBID_FUNC
#define open    OPEN_FUNC_TAOS_FORBID
#define fopen   FOPEN_FUNC_TAOS_FORBID
#define access  ACCESS_FUNC_TAOS_FORBID
#define stat    STAT_FUNC_TAOS_FORBID
#define lstat   LSTAT_FUNC_TAOS_FORBID
#define fstat   FSTAT_FUNC_TAOS_FORBID
#define close   CLOSE_FUNC_TAOS_FORBID
#define fclose  FCLOSE_FUNC_TAOS_FORBID
#define fsync   FSYNC_FUNC_TAOS_FORBID
#define getline GETLINE_FUNC_TAOS_FORBID
// #define fflush FFLUSH_FUNC_TAOS_FORBID
#endif

#ifndef PATH_MAX
#define PATH_MAX 256
#endif

#ifdef WINDOWS
#define TD_PATH_MAX _MAX_PATH
#elif defined(PATH_MAX)
#define TD_PATH_MAX PATH_MAX
#elif defined(_XOPEN_PATH_MAX)
#define TD_PATH_MAX _XOPEN_PATH_MAX
#else
#define TD_PATH_MAX _POSIX_PATH_MAX
#endif

typedef struct TdFile *TdFilePtr;

#define TD_FILE_CREATE        0x0001
#define TD_FILE_WRITE         0x0002
#define TD_FILE_READ          0x0004
#define TD_FILE_TRUNC         0x0008
#define TD_FILE_APPEND        0x0010
#define TD_FILE_TEXT          0x0020
#define TD_FILE_AUTO_DEL      0x0040
#define TD_FILE_EXCL          0x0080
#define TD_FILE_STREAM        0x0100  // Only support taosFprintfFile, taosGetLineFile, taosEOFFile
#define TD_FILE_WRITE_THROUGH 0x0200
#define TD_FILE_CLOEXEC       0x0400

TdFilePtr taosOpenFile(const char *path, int32_t tdFileOptions);
TdFilePtr taosCreateFile(const char *path, int32_t tdFileOptions);

#define TD_FILE_ACCESS_EXIST_OK 0x1
#define TD_FILE_ACCESS_READ_OK  0x2
#define TD_FILE_ACCESS_WRITE_OK 0x4
bool taosCheckAccessFile(const char *pathname, int mode);

int32_t taosLockFile(TdFilePtr pFile);
int32_t taosUnLockFile(TdFilePtr pFile);

int32_t taosUmaskFile(int32_t maskVal);

int32_t taosStatFile(const char *path, int64_t *size, int32_t *mtime, int32_t *atime);
int32_t taosDevInoFile(TdFilePtr pFile, int64_t *stDev, int64_t *stIno);
int32_t taosFStatFile(TdFilePtr pFile, int64_t *size, int32_t *mtime);
bool    taosCheckExistFile(const char *pathname);

int64_t taosLSeekFile(TdFilePtr pFile, int64_t offset, int32_t whence);
int32_t taosFtruncateFile(TdFilePtr pFile, int64_t length);
int32_t taosFsyncFile(TdFilePtr pFile);

int64_t taosReadFile(TdFilePtr pFile, void *buf, int64_t count);
int64_t taosPReadFile(TdFilePtr pFile, void *buf, int64_t count, int64_t offset);
int64_t taosWriteFile(TdFilePtr pFile, const void *buf, int64_t count);
int64_t taosPWriteFile(TdFilePtr pFile, const void *buf, int64_t count, int64_t offset);
void    taosFprintfFile(TdFilePtr pFile, const char *format, ...);

int64_t taosGetLineFile(TdFilePtr pFile, char **__restrict ptrBuf);
int64_t taosGetsFile(TdFilePtr pFile, int32_t maxSize, char *__restrict buf);

int32_t taosEOFFile(TdFilePtr pFile);

int32_t taosCloseFile(TdFilePtr *ppFile);

int32_t taosRenameFile(const char *oldName, const char *newName);
int64_t taosCopyFile(const char *from, const char *to);
int32_t taosRemoveFile(const char *path);

void taosGetTmpfilePath(const char *inputTmpDir, const char *fileNamePrefix, char *dstPath);

int64_t taosFSendFile(TdFilePtr pFileOut, TdFilePtr pFileIn, int64_t *offset, int64_t size);

bool taosValidFile(TdFilePtr pFile);

int32_t taosGetErrorFile(TdFilePtr pFile);

int32_t taosCompressFile(char *srcFileName, char *destFileName);

int32_t taosSetFileHandlesLimit();

int32_t taosLinkFile(char *src, char *dst);

bool lastErrorIsFileNotExist();

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_FILE_H_*/
