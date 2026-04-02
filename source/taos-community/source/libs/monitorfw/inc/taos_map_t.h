/**
 * Copyright 2019-2020 DigitalOcean Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
