/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef TDENGINE_CACHE_DEFINE_H
#define TDENGINE_CACHE_DEFINE_H

#ifdef __cplusplus
extern "C" {
#endif

#define HASH_POWER_INIT             1024
#define CHUNK_SIZE                  48

#define MAX_NUMBER_OF_SLAB_CLASSES  64

#define POWER_LARGEST               MAX_NUMBER_OF_SLAB_CLASSES*3

#define CHUNK_ALIGN_BYTES 8

#define SLAB_PAGE_SIZE 1024 * 1024

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHE_DEFINE_H */