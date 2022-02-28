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

#ifndef ALLOW_FORBID_FUNC
    #define open OPEN_FUNC_TAOS_FORBID
    #define fopen FOPEN_FUNC_TAOS_FORBID
    // #define close CLOSE_FUNC_TAOS_FORBID
    // #define fclose FCLOSE_FUNC_TAOS_FORBID
#endif

#ifndef PATH_MAX
#define PATH_MAX 256
#endif

typedef struct TdFile *TdFilePtr;
 
#define TD_FILE_CTEATE    0x0001
#define TD_FILE_WRITE     0x0002
#define TD_FILE_READ      0x0004
#define TD_FILE_TRUNC     0x0008
#define TD_FILE_APPEND    0x0010
#define TD_FILE_TEXT      0x0020
#define TD_FILE_AUTO_DEL  0x0040
#define TD_FILE_EXCL      0x0080
#define TD_FILE_STREAM    0x0100   // Only support taosFprintfFile, taosGetLineFile, taosGetLineFile, taosEOFFile
 
int32_t taosLockFile(TdFilePtr pFile);
int32_t taosUnLockFile(TdFilePtr pFile);
 
int32_t taosUmaskFile(int32_t maskVal);
 
int32_t taosStatFile(const char *path, int64_t *size, int32_t *mtime);
int32_t taosFStatFile(TdFilePtr pFile, int64_t *size, int32_t *mtime);
 
TdFilePtr taosOpenFile(const char *path,int32_t tdFileOptions);
 
int64_t taosLSeekFile(TdFilePtr pFile, int64_t offset, int32_t whence);
int32_t taosFtruncateFile(TdFilePtr pFile, int64_t length);
int32_t taosFsyncFile(TdFilePtr pFile);
 
int64_t taosReadFile(TdFilePtr pFile, void *buf, int64_t count);
int64_t taosPReadFile(TdFilePtr pFile, void *buf, int64_t count, int64_t offset);
int64_t taosWriteFile(TdFilePtr pFile, const void *buf, int64_t count);
void taosFprintfFile(TdFilePtr pFile, const char *format, ...);
int64_t taosGetLineFile(TdFilePtr pFile, char ** __restrict__ ptrBuf);
int32_t taosEOFFile(TdFilePtr pFile);
 
int64_t taosCloseFile(TdFilePtr *ppFile);
 
int32_t taosRenameFile(const char *oldName, const char *newName);
int64_t taosCopyFile(const char *from, const char *to);
 
void taosGetTmpfilePath(const char *inputTmpDir, const char *fileNamePrefix, char *dstPath);
 
int64_t taosSendFile(SocketFd fdDst, TdFilePtr pFileSrc, int64_t *offset, int64_t size);
int64_t taosFSendFile(TdFilePtr pFileOut, TdFilePtr pFileIn, int64_t *offset, int64_t size);

void *taosMmapReadOnlyFile(TdFilePtr pFile, int64_t length);
bool taosValidFile(TdFilePtr pFile);

int taosGetErrorFile(TdFilePtr pFile);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_FILE_H_*/
