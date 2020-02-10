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

#ifndef TDENGINE_THASH_H
#define TDENGINE_THASH_H

#ifdef __cplusplus
extern "C" {
#endif

void *taosOpenHash(int maxSessions, int (*fp)(void *, uint64_t));

void taosCloseHash(void *handle);

int taosAddHash(void *handle, uint64_t, uint32_t id);

void taosDeleteHash(void *handle, uint64_t);

int32_t taosGetIdFromHash(void *handle, uint64_t);

int taosHashLong(void *, uint64_t ip);

uint64_t taosHashUInt64(uint64_t handle);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_THASH_H
