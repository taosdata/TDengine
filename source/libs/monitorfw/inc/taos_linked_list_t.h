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
