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

#ifdef __cplusplus
extern "C" {
#endif

void    taosRemoveDir(char *rootDir);
bool    taosDirExist(const char* dirname);
int32_t taosMkDir(const char *pathname, mode_t mode);
void    taosRemoveOldLogFiles(char *rootDir, int32_t keepDays);
int32_t taosRename(char *oldName, char *newName);
int32_t taosCompressFile(char *srcFileName, char *destFileName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_DIR_H_*/
