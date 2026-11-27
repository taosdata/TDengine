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

#ifndef _TD_OS_DIR_H_
#define _TD_OS_DIR_H_

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define opendir  OPENDIR_FUNC_TAOS_FORBID
#define readdir  READDIR_FUNC_TAOS_FORBID
#define closedir CLOSEDIR_FUNC_TAOS_FORBID
#define dirname  DIRNAME_FUNC_TAOS_FORBID
#undef basename
#define basename BASENAME_FUNC_TAOS_FORBID
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include "cus_name.h"

#ifdef WINDOWS

#define TD_TMP_DIR_PATH "C:\\Windows\\Temp\\"
#ifdef CUS_NAME
#define TD_CFG_DIR_PATH  "C:\\" CUS_NAME "\\cfg\\"
#define TD_DATA_DIR_PATH "C:\\" CUS_NAME "\\data\\"
#define TD_LOG_DIR_PATH  "C:\\" CUS_NAME "\\log\\"
#else
#define TD_CFG_DIR_PATH  "C:\\TDengine\\cfg\\"
#define TD_DATA_DIR_PATH "C:\\TDengine\\data\\"
#define TD_LOG_DIR_PATH  "C:\\TDengine\\log\\"
#endif  // CUS_NAME

#elif defined(_TD_DARWIN_64)

#ifdef CUS_PROMPT
#define TD_TMP_DIR_PATH  "/tmp/" CUS_PROMPT "d/"
#define TD_CFG_DIR_PATH  "/etc/" CUS_PROMPT "/"
#define TD_DATA_DIR_PATH "/var/lib/" CUS_PROMPT "/"
#define TD_LOG_DIR_PATH  "/var/log/" CUS_PROMPT "/"
#else
#define TD_TMP_DIR_PATH  "/tmp/taosd/"
#define TD_CFG_DIR_PATH  "/etc/taos/"
#define TD_DATA_DIR_PATH "/var/lib/taos/"
#define TD_LOG_DIR_PATH  "/var/log/taos/"
#endif  // CUS_PROMPT

#elif defined(TD_ASTRA)

#ifdef CUS_NAME
#define TD_TMP_DIR_PATH  "C:\\" CUS_NAME "\\Temp\\"
#define TD_CFG_DIR_PATH  "C:\\" CUS_NAME "\\cfg\\"
#define TD_DATA_DIR_PATH "C:\\" CUS_NAME "\\data\\"
#define TD_LOG_DIR_PATH  "C:\\" CUS_NAME "\\log\\"
#else
#define TD_TMP_DIR_PATH  "C:\\TDengine\\Temp\\"
#define TD_CFG_DIR_PATH  "C:\\TDengine\\cfg\\"
#define TD_DATA_DIR_PATH "C:\\TDengine\\data\\"
#define TD_LOG_DIR_PATH  "C:\\TDengine\\log\\"
#endif  // CUS_NAME

#else

#define TD_TMP_DIR_PATH "/tmp/"
#ifdef CUS_PROMPT
#define TD_CFG_DIR_PATH  "/etc/" CUS_PROMPT "/"
#define TD_DATA_DIR_PATH "/var/lib/" CUS_PROMPT "/"
#define TD_LOG_DIR_PATH  "/var/log/" CUS_PROMPT "/"
#else
#define TD_CFG_DIR_PATH  "/etc/taos/"
#define TD_DATA_DIR_PATH "/var/lib/taos/"
#define TD_LOG_DIR_PATH  "/var/log/taos/"
#endif  // CUS_PROMPT
#endif

typedef struct TdDir      *TdDirPtr;
typedef struct TdDirEntry *TdDirEntryPtr;

#define TAOS_DIRNAME(name) ((void)taosDirName(name))

void    taosRemoveDir(const char *dirname);
void    taosClearDir(const char *dirname);
bool    taosDirExist(const char *dirname);
// Create or reuse a dedicated directory, resetting existing permissions to private.
// POSIX requires effective-UID ownership and sets mode 0700. Windows accepts the
// current user, SYSTEM, Administrators, or an Authz-verified administrator as owner;
// existing directories may also belong to the token's default owner. The protected
// DACL grants full control only to the current user, SYSTEM, and Administrators.
// New Windows directories use Administrators as owner when elevated, otherwise the
// current user. Only the final component is checked for links;
// callers are responsible for the parent path.
int32_t taosEnsurePrivateDir(const char *path);
// stage points to a static description of the last attempted operation.
int32_t taosEnsurePrivateDirWithStage(const char *path, const char **stage);
// Recursively remove a directory created by taosEnsurePrivateDir. The top-level path must be a
// real directory (not a link) owned by the effective UID; nested links are removed, never followed.
int32_t taosRemovePrivateDir(const char *path);
int32_t taosMkDir(const char *dirname);
int32_t taosMulMkDir(const char *dirname);
int32_t taosMulModeMkDir(const char *dirname, int mode, bool checkAccess);
void    taosRemoveOldFiles(const char *dirname, int32_t keepDays);
/* Expands dirname into outname. Invalid pointers or maxlen <= 0 return
 * TSDB_CODE_INVALID_PARA. A path whose bytes plus terminating NUL exceed maxlen
 * returns TSDB_CODE_INVALID_CFG_VALUE and leaves outname untouched. */
int32_t taosExpandDir(const char *dirname, char *outname, int32_t maxlen);
int32_t taosRealPath(char *dirname, char *realPath, int32_t maxlen);
bool    taosIsDir(const char *dirname);
char   *taosDirName(char *dirname);
char   *taosDirEntryBaseName(char *dirname);
void    taosGetCwd(char *buf, int32_t len);

TdDirPtr      taosOpenDir(const char *dirname);
TdDirEntryPtr taosReadDir(TdDirPtr pDir);
bool          taosDirEntryIsDir(TdDirEntryPtr pDirEntry);
bool          taosDirEntryIsRegular(TdDirEntryPtr pDirEntry);
char         *taosGetDirEntryName(TdDirEntryPtr pDirEntry);
int32_t       taosCloseDir(TdDirPtr *ppDir);

#ifdef WINDOWS
// Wide-char path variants: on Windows these use the wide Win32 directory APIs
// (FindFirstFileW / FindNextFileW) and convert names back to UTF-8, so
// non-ASCII (e.g. CJK) names round-trip correctly.
typedef struct TdDirW      *TdDirWPtr;
typedef struct TdDirWEntry *TdDirWEntryPtr;

TdDirWPtr      taosOpenDirW(const char *dirname);
TdDirWEntryPtr taosReadDirW(TdDirWPtr pDir);
bool           taosDirEntryIsDirW(TdDirWEntryPtr pDirEntry);
bool           taosDirEntryIsRegularW(TdDirWEntryPtr pDirEntry);
char          *taosGetDirEntryNameW(TdDirWEntryPtr pDirEntry);
int32_t        taosCloseDirW(TdDirWPtr *ppDir);
int32_t        taosMkDirW(const char *dirname);
int32_t        taosMulMkDirW(const char *dirname);
void           taosClearDirW(const char *dirname);
void           taosRemoveDirW(const char *dirname);
bool           taosDirExistW(const char *dirname);
#endif

int32_t taosAppPath(char *path, int32_t maxLen);
// Resolve a daemon shipped next to taosd (name without extension; ".exe" is appended on Windows).
// Only the running executable's directory and fixed install directories are searched, never the
// working directory or PATH. The match must be a regular file; directories and links are skipped.
int32_t taosLocateCompanionExec(const char *name, char *path, int32_t size);
int32_t taosGetDirSize(const char *path, int64_t *size);

#if defined(TAOS_OS_WIDE_PATH) && defined(WINDOWS)
#define TdDirPtr            TdDirWPtr
#define TdDirEntryPtr       TdDirWEntryPtr
#define taosOpenDir         taosOpenDirW
#define taosReadDir         taosReadDirW
#define taosDirEntryIsDir   taosDirEntryIsDirW
#define taosDirEntryIsRegular taosDirEntryIsRegularW
#define taosGetDirEntryName taosGetDirEntryNameW
#define taosCloseDir        taosCloseDirW
#define taosMkDir           taosMkDirW
#define taosMulMkDir        taosMulMkDirW
#define taosClearDir        taosClearDirW
#define taosRemoveDir       taosRemoveDirW
#define taosDirExist        taosDirExistW
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_DIR_H_*/
