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

#ifndef TAOS_MAP_T_H
#define TAOS_MAP_T_H

#include <pthread.h>

// Public
#include "taos_map.h"

// Private
#include "taos_linked_list_t.h"

typedef void (*taos_map_node_free_value_fn)(void *);

struct taos_map_node {
  const char *key;
  void *value;
  taos_map_node_free_value_fn free_value_fn;
};

struct taos_map {
  size_t size;                /**< contains the size of the map */
  size_t max_size;            /**< stores the current max_size */
  taos_linked_list_t *keys;   /**< linked list containing containing all keys present */
  taos_linked_list_t **addrs; /**< Sequence of linked lists. Each list contains nodes with the same index */
  pthread_rwlock_t *rwlock;
  taos_map_node_free_value_fn free_value_fn;
};

#endif  // TAOS_MAP_T_H
