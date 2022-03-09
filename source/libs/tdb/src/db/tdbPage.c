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

#include "tdbInt.h"

int tdbPageCreate(int pageSize, SPage **ppPage, void *(*xMalloc)(void *, size_t), void *arg) {
  // TODO
  return 0;
}

int tdbPageDestroy(SPage *pPage, void (*xFree)(void *)) {
  // TODO
  return 0;
}

int tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell) {
  // TODO
  return 0;
}

int tdbPageDropCell(SPage *pPage, int idx) {
  // TODO
  return 0;
}

static int tdbPageAllocate(SPage *pPage, int size, SCell **ppCell) {
  // TODO
  return 0;
}

static int tdbPageFree(SPage *pPage, int idx, SCell *pCell) {
  // TODO
  return 0;
}