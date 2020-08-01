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

#define _DEFAULT_SOURCE
#include "os.h"

int tsem_init(dispatch_semaphore_t *sem, int pshared, unsigned int value) {
  *sem = dispatch_semaphore_create(value);
  if (*sem == NULL) {
    return -1;
  } else {
    return 0;
  }
}

int tsem_wait(dispatch_semaphore_t *sem) {
  dispatch_semaphore_wait(*sem, DISPATCH_TIME_FOREVER);
  return 0;
}

int tsem_post(dispatch_semaphore_t *sem) {
  dispatch_semaphore_signal(*sem);
  return 0;
}

int tsem_destroy(dispatch_semaphore_t *sem) {
  return 0;
}
