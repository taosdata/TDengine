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

#ifndef TAOS_LIST_I_INCLUDED
#define TAOS_LIST_I_INCLUDED

// Private
#include "taos_linked_list_t.h"

/**
 * @brief API PRIVATE Returns a pointer to a taos_linked_list
 */
taos_linked_list_t *taos_linked_list_new(void);

/**
 * @brief API PRIVATE removes all nodes from the given taos_linked_list *
 */
int taos_linked_list_purge(taos_linked_list_t *self);

/**
 * @brief API PRIVATE Destroys a taos_linked_list
 */
int taos_linked_list_destroy(taos_linked_list_t *self);

/**
 * @brief API PRIVATE Append an item to the back of the list
 */
int taos_linked_list_append(taos_linked_list_t *self, void *item);

/**
 * @brief API PRIVATE Push an item onto the front of the list
 */
int taos_linked_list_push(taos_linked_list_t *self, void *item);

/**
 * @brief API PRIVATE Pop the first item off of the list
 */
void *taos_linked_list_pop(taos_linked_list_t *self);

/**
 * @brief API PRIVATE Returns the item at the head of the list or NULL if not present
 */
void *taos_linked_list_first(taos_linked_list_t *self);

/**
 * @brief API PRIVATE Returns the item at the tail of the list or NULL if not present
 */
void *taos_linked_list_last(taos_linked_list_t *self);

/**
 * @brief API PRIVATE Removes an item from the linked list
 */
int taos_linked_list_remove(taos_linked_list_t *self, void *item);

/**
 * @brief API PRIVATE Compares two items within a linked list
 */
taos_linked_list_compare_t taos_linked_list_compare(taos_linked_list_t *self, void *item_a, void *node_b);

/**
 * @brief API PRIVATE Get the size
 */
size_t taos_linked_list_size(taos_linked_list_t *self);

/**
 * @brief API PRIVATE Set the free_fn member on taos_linked_list
 */
int taos_linked_list_set_free_fn(taos_linked_list_t *self, taos_linked_list_free_item_fn free_fn);

/**
 * @brief API PRIVATE Set the compare_fn member on the taos_linked_list
 */
int taos_linked_list_set_compare_fn(taos_linked_list_t *self, taos_linked_list_compare_item_fn compare_fn);

/**
 * API PRIVATE
 * @brief does nothing
 */
void taos_linked_list_no_op_free(void *item);

#endif  // TAOS_LIST_I_INCLUDED
