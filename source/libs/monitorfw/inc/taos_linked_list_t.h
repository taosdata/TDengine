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

#ifndef TAOS_LIST_T_H
#define TAOS_LIST_T_H

#include "taos_linked_list.h"

typedef enum { TAOS_LESS = -1, TAOS_EQUAL = 0, TAOS_GREATER = 1 } taos_linked_list_compare_t;

/**
 * @brief API PRIVATE Frees an item in a taos_linked_list_node
 */
typedef void (*taos_linked_list_free_item_fn)(void *);

/**
 * @brief API PRIVATE Compares two items within a taos_linked_list
 */
typedef taos_linked_list_compare_t (*taos_linked_list_compare_item_fn)(void *item_a, void *item_b);

/**
 * @brief API PRIVATE A struct containing a generic item, represented as a void pointer, and next, a pointer to the
 * next taos_linked_list_node*
 */
typedef struct taos_linked_list_node {
  struct taos_linked_list_node *next;
  void *item;
} taos_linked_list_node_t;

/**
 * @brief API PRIVATE A linked list comprised of taos_linked_list_node* instances
 */
struct taos_linked_list {
  taos_linked_list_node_t *head;
  taos_linked_list_node_t *tail;
  size_t size;
  taos_linked_list_free_item_fn free_fn;
  taos_linked_list_compare_item_fn compare_fn;
};

#endif  // TAOS_LIST_T_H
