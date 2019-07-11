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

#ifndef _sdb_str_hash_header_
#define _sdb_str_hash_header_

void *sdbOpenStrHash(int maxSessions, int dataSize);
void sdbCloseStrHash(void *handle);
void *sdbAddStrHash(void *handle, void *key, void *pData);
void sdbDeleteStrHash(void *handle, void *key);
void *sdbGetStrHashData(void *handle, void *key);
void *sdbFetchStrHashData(void *handle, void *ptr, void **ppMeta);

#endif
