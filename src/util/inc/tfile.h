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

#ifndef TDENGINE_TFILE_H
#define TDENGINE_TFILE_H

#ifdef __cplusplus
extern "C" {
#endif

// init taos file module
int32_t tfInit();

// clean up taos file module
void tfCleanup();

// the same syntax as UNIX standard open/close/read/write
// but FD is int64_t and will never be reused
int64_t tfOpen(const char *pathname, int32_t flags);
int64_t tfOpenM(const char *pathname, int32_t flags, mode_t mode);
int64_t tfClose(int64_t tfd);
int64_t tfWrite(int64_t tfd, void *buf, int64_t count);
int64_t tfRead(int64_t tfd, void *buf, int64_t count);
int32_t tfFsync(int64_t tfd);
bool    tfValid(int64_t tfd);
int32_t tfLseek(int64_t tfd, int64_t offset, int32_t whence);
int32_t tfFtruncate(int64_t tfd, int64_t length);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TFILE_H
