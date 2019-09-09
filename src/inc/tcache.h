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

#ifndef TDENGINE_TCACHE_H
#define TDENGINE_TCACHE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>

/**
 *
 * @param maxSessions       maximum slots available for hash elements
 * @param tmrCtrl           timer ctrl
 * @param refreshTime       refresh operation interval time, the maximum survival time when one element is expired and
 *                          not referenced by other objects
 * @return
 */
void *taosInitDataCache(int maxSessions, void *tmrCtrl, int64_t refreshTimeInSeconds);

/**
 * add data into cache
 *
 * @param handle        cache object
 * @param key           key
 * @param pData         cached data
 * @param dataSize      data size
 * @param keepTime      survival time in second
 * @return              cached element
 */
void *taosAddDataIntoCache(void *handle, char *key, char *pData, int dataSize, int keepTimeInSeconds);

/**
 * remove data in cache, the data will not be removed immediately.
 * if it is referenced by other object, it will be remain in cache
 * @param handle    cache object
 * @param data      not the key, actually referenced data
 * @param remove   force model, reduce the ref count and move the data into
 * pTrash
 */
void taosRemoveDataFromCache(void *handle, void **data, bool remove);

/**
 * update data in cache
 * @param handle hash object handle(pointer)
 * @param key    key for hash
 * @param pData  actually data
 * @param size   length of data
 * @param duration  survival time of this object in cache
 * @return       new referenced data
 */
void *taosUpdateDataFromCache(void *handle, char *key, char *pData, int size, int duration);

/**
 * get data from cache
 * @param handle        cache object
 * @param key           key
 * @return              cached data or NULL
 */
void *taosGetDataFromCache(void *handle, char *key);

/**
 * release all allocated memory and destroy the cache object
 *
 * @param handle
 */
void taosCleanUpDataCache(void *handle);

/**
 *  move all data node into trash,clear node in trash can if it is not referenced by client
 * @param handle
 */
void taosClearDataCache(void *handle);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCACHE_H
