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

#include <unistd.h>

// init taos file module
int  tfinit();

// clean up taos file module
void tfcleanup();

// the same syntax as UNIX standard open/close/read/write
// but FD is int64_t and will never be reused
int64_t tfopen(const char *pathname, int flags);
int64_t tfclose(int64_t tfd);
ssize_t tfwrite(int64_t tfd, const void *buf, size_t count);
ssize_t tfread(int64_t tfd, void *buf, size_t count);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TREF_H
