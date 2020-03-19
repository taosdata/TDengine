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

#ifndef _sdb_int_hash_header_
#define _sdb_int_hash_header_

void *sdbOpenIntHash(int maxSessions, int dataSize);
void sdbCloseIntHash(void *handle);
void *sdbAddIntHash(void *handle, void *key, void *pData);
void sdbDeleteIntHash(void *handle, void *key);
void *sdbGetIntHashData(void *handle, void *key);
void *sdbFetchIntHashData(void *handle, void *ptr, void **ppMeta);

#endif
