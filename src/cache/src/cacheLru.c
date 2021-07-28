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

int cacheLruInit(cacheSlabLruClass* pLru, int i) {
  pLru->tail = pLru->head = NULL;
  pLru->bytes = pLru->num = 0;
  pLru->id = i;
  return cacheMutexInit(&(pLru->mutex));
}

int cacheLruDestroy(cacheSlabLruClass* pLru) {
  return (cacheMutexDestroy(&(pLru->mutex)));
}

void cacheLruUnlinkItem(cache_t* pCache, cacheItem* pItem, bool lock) {  
  cacheSlabLruClass* pLru = &(pCache->lruArray[item_slablru_id(pItem)]);
  assert(item_slablru_id(pItem) == pLru->id);

  if (lock) cacheMutexLock(&(pLru->mutex));

  cacheItem* head = pLru->head;
  cacheItem* tail = pLru->tail;
  
  assert((head == NULL && tail == NULL) || (head && tail));
  assert(head == NULL || pLru->num > 0);

  if (head == pItem) {
    assert(pItem->prev == NULL);
    pLru->head = pItem->next;
  }
  if (tail == pItem) {
    assert(pItem->next == NULL);
    pLru->tail = pItem->prev;
  }
  assert(pItem->next != pItem);
  assert(pItem->prev != pItem);

  if (pItem->next) pItem->next->prev = pItem->prev;
  if (pItem->prev) pItem->prev->next = pItem->next;
  pItem->next = pItem->prev = NULL;
  pLru->num -= 1;
  pLru->bytes -= cacheItemTotalBytes(pItem->nkey, pItem->nbytes);

  if (lock) cacheMutexUnlock(&(pLru->mutex));
}

void cacheLruLinkItem(cache_t* pCache, cacheItem* pItem, bool lock) {
  cacheSlabLruClass* pLru = &(pCache->lruArray[item_slablru_id(pItem)]);
  assert(item_slablru_id(pItem) == pLru->id);

  if (lock) cacheMutexLock(&(pLru->mutex));
  
  cacheItem* head = pLru->head;
  cacheItem* tail = pLru->tail;

  assert((head == NULL && tail == NULL) || (head && tail));
  assert(head == NULL || pLru->num > 0);

  pItem->next = head;
  pItem->prev = NULL;

  if (pItem->next) pItem->next->prev = pItem;

  pLru->head = pItem;
  if (tail == NULL) pLru->tail = pItem;

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