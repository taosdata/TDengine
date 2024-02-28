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

#include <pthread.h>
#include <stdbool.h>

// Public
#include "taos_alloc.h"

// Private
#include "taos_assert.h"
#include "taos_errors.h"
#include "taos_linked_list_i.h"
#include "taos_linked_list_t.h"
#include "taos_log.h"
#include "taos_map_i.h"
#include "taos_map_t.h"

#define TAOS_MAP_INITIAL_SIZE 32

static void destroy_map_node_value_no_op(void *value) {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// taos_map_node
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

taos_map_node_t *taos_map_node_new(const char *key, void *value, taos_map_node_free_value_fn free_value_fn) {
  taos_map_node_t *self = taos_malloc(sizeof(taos_map_node_t));
  self->key = taos_strdup(key);
  self->value = value;
  self->free_value_fn = free_value_fn;
  return self;
}

int taos_map_node_destroy(taos_map_node_t *self) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 0;
  taos_free((void *)self->key);
  self->key = NULL;
  if (self->value != NULL) (*self->free_value_fn)(self->value);
  self->value = NULL;
  taos_free(self);
  self = NULL;
  return 0;
}

void taos_map_node_free(void *item) {
  taos_map_node_t *map_node = (taos_map_node_t *)item;
  taos_map_node_destroy(map_node);
}

taos_linked_list_compare_t taos_map_node_compare(void *item_a, void *item_b) {
  taos_map_node_t *map_node_a = (taos_map_node_t *)item_a;
  taos_map_node_t *map_node_b = (taos_map_node_t *)item_b;

  return strcmp(map_node_a->key, map_node_b->key);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// taos_map
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

taos_map_t *taos_map_new() {
  int r = 0;

  taos_map_t *self = (taos_map_t *)taos_malloc(sizeof(taos_map_t));
  self->size = 0;
  self->max_size = TAOS_MAP_INITIAL_SIZE;

  self->keys = taos_linked_list_new();
  if (self->keys == NULL) return NULL;

  // These each key will be allocated once by taos_map_node_new and used here as well to save memory. With that said
  // we will only have to deallocate each key once. That will happen on taos_map_node_destroy.
  r = taos_linked_list_set_free_fn(self->keys, taos_linked_list_no_op_free);
  if (r) {
    taos_map_destroy(self);
    return NULL;
  }

  self->addrs = taos_malloc(sizeof(taos_linked_list_t) * self->max_size);
  self->free_value_fn = destroy_map_node_value_no_op;

  for (int i = 0; i < self->max_size; i++) {
    self->addrs[i] = taos_linked_list_new();
    r = taos_linked_list_set_free_fn(self->addrs[i], taos_map_node_free);
    if (r) {
      taos_map_destroy(self);
      return NULL;
    }
    r = taos_linked_list_set_compare_fn(self->addrs[i], taos_map_node_compare);
    if (r) {
      taos_map_destroy(self);
      return NULL;
    }
  }

  self->rwlock = (pthread_rwlock_t *)taos_malloc(sizeof(pthread_rwlock_t));
  r = pthread_rwlock_init(self->rwlock, NULL);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_INIT_ERROR);
    taos_map_destroy(self);
    return NULL;
  }

  return self;
}

int taos_map_destroy(taos_map_t *self) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  int ret = 0;

  r = taos_linked_list_destroy(self->keys);
  if (r) ret = r;
  self->keys = NULL;

  for (size_t i = 0; i < self->max_size; i++) {
    r = taos_linked_list_destroy(self->addrs[i]);
    if (r) ret = r;
    self->addrs[i] = NULL;
  }
  taos_free(self->addrs);
  self->addrs = NULL;

  r = pthread_rwlock_destroy(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_DESTROY_ERROR)
    ret = r;
  }

  taos_free(self->rwlock);
  self->rwlock = NULL;
  taos_free(self);
  self = NULL;

  return ret;
}

static size_t taos_map_get_index_internal(const char *key, size_t *size, size_t *max_size) {
  size_t index;
  size_t a = 31415, b = 27183;
  for (index = 0; *key != '\0'; key++, a = a * b % (*max_size - 1)) {
    index = (a * index + *key) % *max_size;
  }
  return index;
}

/**
 * @brief API PRIVATE hash function that returns an array index from the given key and taos_map.
 *
 * The algorithm is based off of Horner's method. In a simpler version, you set the return value to 0. Next, for each
 * character in the string, you add the integer value of the current character to the product of the prime number and
 * the current return value, set the result to the return value, then finally return the return value.
 *
 * In this version of the algorithm, we attempt to achieve a probabily of key to index conversion collisions to
 * 1/M (with M being the max_size of the map). This optimizes dispersion and consequently, evens out the performance
 * for gets and sets for each item. Instead of using a fixed prime number, we generate a coefficient for each iteration
 * through the loop.
 *
 * Reference:
 *   * Algorithms in C: Third Edition by Robert Sedgewick, p579
 */
size_t taos_map_get_index(taos_map_t *self, const char *key) {
  return taos_map_get_index_internal(key, &self->size, &self->max_size);
}

static void *taos_map_get_internal(const char *key, size_t *size, size_t *max_size, taos_linked_list_t *keys,
                                   taos_linked_list_t **addrs, taos_map_node_free_value_fn free_value_fn) {
  size_t index = taos_map_get_index_internal(key, size, max_size);
  taos_linked_list_t *list = addrs[index];
  taos_map_node_t *temp_map_node = taos_map_node_new(key, NULL, free_value_fn);

  for (taos_linked_list_node_t *current_node = list->head; current_node != NULL; current_node = current_node->next) {
    taos_map_node_t *current_map_node = (taos_map_node_t *)current_node->item;
    taos_linked_list_compare_t result = taos_linked_list_compare(list, current_map_node, temp_map_node);
    if (result == TAOS_EQUAL) {
      taos_map_node_destroy(temp_map_node);
      temp_map_node = NULL;
      return current_map_node->value;
    }
  }
  taos_map_node_destroy(temp_map_node);
  temp_map_node = NULL;
  return NULL;
}

void *taos_map_get(taos_map_t *self, const char *key) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  r = pthread_rwlock_wrlock(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_LOCK_ERROR);
    NULL;
  }
  void *payload =
      taos_map_get_internal(key, &self->size, &self->max_size, self->keys, self->addrs, self->free_value_fn);
  r = pthread_rwlock_unlock(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_UNLOCK_ERROR);
    return NULL;
  }
  return payload;
}

void *taos_map_get_withoutlock(taos_map_t *self, const char *key) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  void *payload =
    taos_map_get_internal(key, &self->size, &self->max_size, self->keys, self->addrs, self->free_value_fn);
  return payload;
}

static int taos_map_set_internal(const char *key, void *value, size_t *size, size_t *max_size, taos_linked_list_t *keys,
                                 taos_linked_list_t **addrs, taos_map_node_free_value_fn free_value_fn,
                                 bool destroy_current_value) {
  taos_map_node_t *map_node = taos_map_node_new(key, value, free_value_fn);
  if (map_node == NULL) return 1;

  size_t index = taos_map_get_index_internal(key, size, max_size);
  taos_linked_list_t *list = addrs[index];
  for (taos_linked_list_node_t *current_node = list->head; current_node != NULL; current_node = current_node->next) {
    taos_map_node_t *current_map_node = (taos_map_node_t *)current_node->item;
    taos_linked_list_compare_t result = taos_linked_list_compare(list, current_map_node, map_node);
    if (result == TAOS_EQUAL) {
      if (destroy_current_value) {
        free_value_fn(current_map_node->value);
        current_map_node->value = NULL;
      }
      taos_free((char *)current_map_node->key);
      current_map_node->key = NULL;
      taos_free(current_map_node);
      current_map_node = NULL;
      current_node->item = map_node;
      return 0;
    }
  }
  taos_linked_list_append(list, map_node);
  taos_linked_list_append(keys, (char *)map_node->key);
  (*size)++;
  return 0;
}

int taos_map_ensure_space(taos_map_t *self) {
  TAOS_ASSERT(self != NULL);
  int r = 0;

  if (self->size <= self->max_size / 2) {
    return 0;
  }

  // Increase the max size
  size_t new_max = self->max_size * 2;
  size_t new_size = 0;

  // Create a new list of keys
  taos_linked_list_t *new_keys = taos_linked_list_new();
  if (new_keys == NULL) return 1;

  r = taos_linked_list_set_free_fn(new_keys, taos_linked_list_no_op_free);
  if (r) return r;

  // Create a new array of addrs
  taos_linked_list_t **new_addrs = taos_malloc(sizeof(taos_linked_list_t) * new_max);

  // Initialize the new array
  for (int i = 0; i < new_max; i++) {
    new_addrs[i] = taos_linked_list_new();
    r = taos_linked_list_set_free_fn(new_addrs[i], taos_map_node_free);
    if (r) return r;
    r = taos_linked_list_set_compare_fn(new_addrs[i], taos_map_node_compare);
    if (r) return r;
  }

  // Iterate through each linked-list at each memory region in the map's backbone
  for (int i = 0; i < self->max_size; i++) {
    // Create a new map node for each node in the linked list and insert it into the new map. Afterwards, deallocate
    // the old map node
    taos_linked_list_t *list = self->addrs[i];
    taos_linked_list_node_t *current_node = list->head;
    while (current_node != NULL) {
      taos_map_node_t *map_node = (taos_map_node_t *)current_node->item;
      r = taos_map_set_internal(map_node->key, map_node->value, &new_size, &new_max, new_keys, new_addrs,
                                self->free_value_fn, false);
      if (r) return r;

      taos_linked_list_node_t *next = current_node->next;
      taos_free(current_node);
      current_node = NULL;
      taos_free((void *)map_node->key);
      map_node->key = NULL;
      taos_free(map_node);
      map_node = NULL;
      current_node = next;
    }
    // We're done deallocating each map node in the linked list, so deallocate the linked-list object
    taos_free(self->addrs[i]);
    self->addrs[i] = NULL;
  }
  // Destroy the collection of keys in the map
  taos_linked_list_destroy(self->keys);
  self->keys = NULL;

  // Deallocate the backbone of the map
  taos_free(self->addrs);
  self->addrs = NULL;

  // Update the members of the current map
  self->size = new_size;
  self->max_size = new_max;
  self->keys = new_keys;
  self->addrs = new_addrs;

  return 0;
}

int taos_map_set(taos_map_t *self, const char *key, void *value) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  r = pthread_rwlock_wrlock(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_LOCK_ERROR);
    return r;
  }

  r = taos_map_ensure_space(self);
  if (r) {
    int rr = 0;
    rr = pthread_rwlock_unlock(self->rwlock);
    if (rr) {
      TAOS_LOG(TAOS_PTHREAD_RWLOCK_UNLOCK_ERROR);
      return rr;
    } else {
      return r;
    }
  }
  r = taos_map_set_internal(key, value, &self->size, &self->max_size, self->keys, self->addrs, self->free_value_fn,
                            true);
  if (r) {
    int rr = 0;
    rr = pthread_rwlock_unlock(self->rwlock);
    if (rr) {
      TAOS_LOG(TAOS_PTHREAD_RWLOCK_UNLOCK_ERROR);
      return rr;
    } else {
      return r;
    }
  }
  r = pthread_rwlock_unlock(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_UNLOCK_ERROR);
  }
  return r;
}

static int taos_map_delete_internal(const char *key, size_t *size, size_t *max_size, taos_linked_list_t *keys,
                                    taos_linked_list_t **addrs, taos_map_node_free_value_fn free_value_fn) {
  int r = 0;
  size_t index = taos_map_get_index_internal(key, size, max_size);
  taos_linked_list_t *list = addrs[index];
  taos_map_node_t *temp_map_node = taos_map_node_new(key, NULL, free_value_fn);

  for (taos_linked_list_node_t *current_node = list->head; current_node != NULL; current_node = current_node->next) {
    taos_map_node_t *current_map_node = (taos_map_node_t *)current_node->item;
    taos_linked_list_compare_t result = taos_linked_list_compare(list, current_map_node, temp_map_node);
    if (result == TAOS_EQUAL) {
      r = taos_linked_list_remove(keys, (char*)current_map_node->key);
      if (r) return r;

      r = taos_linked_list_remove(list, current_node->item);
      if (r) return r;

      (*size)--;
      break;
    }
  }
  r = taos_map_node_destroy(temp_map_node);
  temp_map_node = NULL;
  return r;
}

int taos_map_delete(taos_map_t *self, const char *key) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  int ret = 0;
  r = pthread_rwlock_wrlock(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_LOCK_ERROR);
    ret = r;
  }
  r = taos_map_delete_internal(key, &self->size, &self->max_size, self->keys, self->addrs, self->free_value_fn);
  if (r) ret = r;
  r = pthread_rwlock_unlock(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_UNLOCK_ERROR);
    ret = r;
  }
  return ret;
}

int taos_map_set_free_value_fn(taos_map_t *self, taos_map_node_free_value_fn free_value_fn) {
  TAOS_ASSERT(self != NULL);
  self->free_value_fn = free_value_fn;
  return 0;
}

size_t taos_map_size(taos_map_t *self) {
  TAOS_ASSERT(self != NULL);
  return self->size;
}
