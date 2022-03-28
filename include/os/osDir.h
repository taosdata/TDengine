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

#ifndef _TD_OS_DIR_H_
#define _TD_OS_DIR_H_

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
    #define opendir OPENDIR_FUNC_TAOS_FORBID
    #define readdir READDIR_FUNC_TAOS_FORBID
    #define closedir CLOSEDIR_FUNC_TAOS_FORBID
    #define dirname DIRNAME_FUNC_TAOS_FORBID
    #undef basename
    #define basename BASENAME_FUNC_TAOS_FORBID
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TdDir *TdDirPtr;
typedef struct TdDirEntry *TdDirEntryPtr;


void    taosRemoveDir(const char *dirname);
bool    taosDirExist(char *dirname);
int32_t taosMkDir(const char *dirname);
void    taosRemoveOldFiles(const char *dirname, int32_t keepDays);
int32_t taosExpandDir(const char *dirname, char *outname, int32_t maxlen);
int32_t taosRealPath(char *dirname, int32_t maxlen);
bool    taosIsDir(const char *dirname);
char*   taosDirName(char *dirname);
char*   taosDirEntryBaseName(char *dirname);

TdDirPtr      taosOpenDir(const char *dirname);
TdDirEntryPtr taosReadDir(TdDirPtr pDir);
bool          taosDirEntryIsDir(TdDirEntryPtr pDirEntry);
char*         taosGetDirEntryName(TdDirEntryPtr pDirEntry);
int32_t       taosCloseDir(TdDirPtr pDir);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_DIR_H_*/
