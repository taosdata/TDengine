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

#ifndef _taos_hash_id_header_
#define _taos_hash_id_header_

#ifdef __cplusplus
extern "C" {
#endif

void *taosOpenIdHash(int maxSessions);
void  taosCloseIdHash(void *handle);
void *taosAddIdHash(void *handle, void *pData, int32_t id);
void  taosDeleteIdHash(void *handle, int32_t id);
void *taosGetIdHash(void *handle, int32_t);

#ifdef __cplusplus
}
#endif

#endif
