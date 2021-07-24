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

#include "cacheLru.h"
#include "cacheItem.h"

void cacheLruUnlinkItem(cache_t* pCache, cacheItem* pItem, bool lock) {  
  cacheSlabLruClass* pLru = &(pCache->lruArray[pItem->slabLruId]);
  if (lock) cacheMutexLock(&(pLru->mutex));

  cacheItem* tail = pLru->tail;

  if (tail == pItem) {
    pLru->tail = tail->prev;
  }

  if (pItem->next) pItem->next->prev = pItem->prev;
  if (pItem->prev) pItem->prev->next = pItem->next;

  pLru->num -= 1;
  pLru->bytes -= cacheItemTotalBytes(pItem->nkey, pItem->nbytes);

  if (lock) cacheMutexUnlock(&(pLru->mutex));
}

void cacheLruLinkItem(cache_t* pCache, cacheItem* pItem, bool lock) {
  cacheSlabLruClass* pLru = &(pCache->lruArray[item_lru_id(pItem)]);

  if (lock) cacheMutexLock(&(pLru->mutex));
  
  cacheItem* tail = pLru->tail;
  cacheItem* head = NULL;

  if (tail) {
    head = tail->next;
    tail->next = pItem;
  }

  if (head) head->prev = pItem;
  pItem->next = head;
  pItem->prev = NULL;

  pLru->num += 1;
  pLru->bytes += cacheItemTotalBytes(pItem->nkey, pItem->nbytes);

  if (lock) cacheMutexUnlock(&(pLru->mutex));
}

void cacheLruMoveToHead(cache_t* cache, cacheItem* pItem, bool lock) {
  cacheSlabLruClass* pLru = &(cache->lruArray[item_slablru_id(pItem)]);
  if (lock) cacheMutexLock(&(pLru->mutex));

  cacheLruUnlinkItem(cache, pItem, false);
  cacheLruLinkItem(cache, pItem, false);

  if (lock) cacheMutexUnlock(&(pLru->mutex));
}