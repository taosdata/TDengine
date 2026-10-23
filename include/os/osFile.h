/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifdef WINDOWS
typedef struct TaosIOVec {
  void  *iov_base;
  size_t iov_len;
} TaosIOVec;
#else
#include <sys/uio.h>
typedef struct iovec TaosIOVec;
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
#define TD_FILE_NOFOLLOW      0x0800  // Do not follow a symbolic link at the path (Linux/macOS only)
#define TD_FILE_SHARE_READ    0x1000  // Let other users read the created file instead of owner-only (Linux/macOS only)

// Create with TD_FILE_CREATE | TD_FILE_EXCL and TD_FILE_READ and/or TD_FILE_WRITE,
// or open an existing file with flags == TD_FILE_READ; all other flags are rejected.
// The target must be a regular file. POSIX requires effective-UID ownership and
// creates files with mode 0600. Windows accepts the current user, SYSTEM,
// Administrators, or an Authz-verified administrator as owner. New Windows files
// have a protected DACL granting full control only to the current user, SYSTEM,
// and Administrators, with Administrators as owner when elevated, otherwise the
// current user. Opening existing files does not reset their DACL.
// Only the final component is checked for links; callers are responsible for the parent path.
TdFilePtr taosOpenPrivateFile(const char *path, int32_t flags);
// Reserve 37 bytes for '-' and the UUID in a 255-byte component.
enum { TD_TMP_FILE_PREFIX_MAX_LEN = 218 };

// Create an owner-only, read/write file and remove it on close or earlier.
// dir may be a symbolic link; resolve it once when opening the directory.
// Directory handles anchor creation; no persistent subdirectory is retained.
// Return NULL and set terrno on failure. prefix must be non-empty, contain
// at most TD_TMP_FILE_PREFIX_MAX_LEN bytes, and exclude separators/colons.
TdFilePtr taosOpenTempFile(const char *dir, const char *prefix);
// Succeed only if path is a unix socket (not a link) owned by the effective UID; always succeeds on Windows.
int32_t   taosCheckPrivateSocket(const char *path);
TdFilePtr taosOpenFile(const char *path, int32_t tdFileOptions);
TdFilePtr taosCreateFile(const char *path, int32_t tdFileOptions);

#define TD_FILE_ACCESS_EXIST_OK 0x1
#define TD_FILE_ACCESS_READ_OK  0x2
#define TD_FILE_ACCESS_WRITE_OK 0x4
#define TD_FILE_ACCESS_EXEC_OK  0x8

#define TD_TMP_FILE_PREFIX "tdengine-"

bool taosCheckAccessFile(const char *pathname, int mode);

int32_t taosLockFile(TdFilePtr pFile);
int32_t taosUnLockFile(TdFilePtr pFile);

int32_t taosUmaskFile(int32_t maskVal);

#ifndef WINDOWS
// Set permissions through an open file descriptor without changing umask.
int32_t taosFchmodFile(TdFilePtr pFile, int32_t mode);
#endif

int32_t taosStatFile(const char *path, int64_t *size, int64_t *mtime, int64_t *atime);
int32_t taosGetFileDiskID(const char *path, int64_t *diskid);
bool    taosCheckFileDiskID(const char *path, int64_t *actDiskID, int64_t expDiskID);
int32_t taosDevInoFile(TdFilePtr pFile, int64_t *stDev, int64_t *stIno);
int32_t taosFStatFile(TdFilePtr pFile, int64_t *size, int64_t *mtime);
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

int32_t taosCompressFile(TdFilePtr pSrcFile, char *destFileName);

#ifndef WINDOWS
// Like taosCompressFile, but explicitly set destination mode regardless of umask.
int32_t taosCompressFileWithMode(TdFilePtr pSrcFile, char *destFileName,
                               int32_t mode);
#endif

int32_t taosSetFileHandlesLimit();

int32_t taosLinkFile(char *src, char *dst);
int32_t taosSymLink(const char *target, const char *linkpath);

FILE  *taosOpenCFile(const char *filename, const char *mode);
int    taosSeekCFile(FILE *file, int64_t offset, int whence);
size_t taosReadFromCFile(void *buffer, size_t size, size_t count, FILE *stream);
size_t taosWriteToCFile(const void *ptr, size_t size, size_t nitems, FILE *stream);
int    taosCloseCFile(FILE *);
int    taosSetAutoDelFile(char *path);

FILE   *taosOpenFileForStream(const char *path, int32_t tdFileOptions);
bool    errorIsFileNotExist(int32_t code);  // the code should be a TSDB_CODE_XXX code
bool    lastErrorIsFileNotExist();

int64_t taosWritevFile(TdFilePtr pFile, const TaosIOVec *iov, int iovcnt);

#ifdef WINDOWS
// Wide-char path variants: interpret the path as UTF-8 on Windows (via wide
// Win32 APIs) so non-ASCII (e.g. CJK) names round-trip correctly.  On POSIX
// the paths are already UTF-8 byte strings, so these delegate to the plain
// APIs.
TdFilePtr taosOpenFileW(const char *path, int32_t tdFileOptions);
int32_t   taosRemoveFileW(const char *path);
int32_t   taosRenameFileW(const char *oldName, const char *newName);
int32_t   taosStatFileW(const char *path, int64_t *size, int64_t *mtime, int64_t *atime);
bool      taosCheckAccessFileW(const char *pathname, int32_t mode);
bool      taosCheckExistFileW(const char *pathname);
#endif

#ifdef BUILD_WITH_RAND_ERR
#define STUB_RAND_NETWORK_ERR(ret)                                        \
  do {                                                                    \
    if (tsEnableRandErr && (tsRandErrScope & RAND_ERR_NETWORK)) {         \
      uint32_t r = taosRand() % tsRandErrDivisor;                         \
      if ((r + 1) <= tsRandErrChance) {                                   \
        ret = TSDB_CODE_RPC_NETWORK_UNAVAIL;                              \
        uError("random network error: %s, %s", tstrerror(ret), __func__); \
      }                                                                   \
    }                                                                     \
    while (0)
#else
#define STUB_RAND_NETWORK_ERR(status)
#endif

#if defined(TAOS_OS_WIDE_PATH) && defined(WINDOWS)
#define taosOpenFile        taosOpenFileW
#define taosRemoveFile      taosRemoveFileW
#define taosRenameFile      taosRenameFileW
#define taosStatFile        taosStatFileW
#define taosCheckAccessFile taosCheckAccessFileW
#define taosCheckExistFile  taosCheckExistFileW
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_FILE_H_*/
