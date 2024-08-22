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

// Public
#include "taos_alloc.h"

// Private
#include "taos_assert.h"
#include "taos_linked_list_i.h"
#include "taos_linked_list_t.h"
#include "taos_log.h"

taos_linked_list_t *taos_linked_list_new(void) {
  taos_linked_list_t *self = (taos_linked_list_t *)taos_malloc(sizeof(taos_linked_list_t));
  self->head = NULL;
  self->tail = NULL;
  self->free_fn = NULL;
  self->compare_fn = NULL;
  self->size = 0;
  return self;
}

int taos_linked_list_purge(taos_linked_list_t *self) {
  TAOS_ASSERT(self != NULL);
  taos_linked_list_node_t *node = self->head;
  while (node != NULL) {
    taos_linked_list_node_t *next = node->next;
    if (node->item != NULL) {
      if (self->free_fn) {
        (*self->free_fn)(node->item);
      } else {
        taos_free(node->item);
      }
    }
    taos_free(node);
    node = NULL;
    node = next;
  }
  self->head = NULL;
  self->tail = NULL;
  self->size = 0;
  return 0;
}

int taos_linked_list_destroy(taos_linked_list_t *self) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  int ret = 0;

  r = taos_linked_list_purge(self);
  if (r) ret = r;
  taos_free(self);
  self = NULL;
  return ret;
}

void *taos_linked_list_first(taos_linked_list_t *self) {
  TAOS_ASSERT_NULL(self != NULL);
  if (self->head) {
    return self->head->item;
  } else {
    return NULL;
  }
}

void *taos_linked_list_last(taos_linked_list_t *self) {
  TAOS_ASSERT_NULL(self != NULL);
  if (self->tail) {
    return self->tail->item;
  } else {
    return NULL;
  }
}

int taos_linked_list_append(taos_linked_list_t *self, void *item) {
  TAOS_ASSERT(self != NULL);
  taos_linked_list_node_t *node = (taos_linked_list_node_t *)taos_malloc(sizeof(taos_linked_list_node_t));

  node->item = item;
  if (self->tail) {
    self->tail->next = node;
  } else {
    self->head = node;
  }
  self->tail = node;
  node->next = NULL;
  self->size++;
  return 0;
}

int taos_linked_list_push(taos_linked_list_t *self, void *item) {
  TAOS_ASSERT(self != NULL);
  taos_linked_list_node_t *node = (taos_linked_list_node_t *)taos_malloc(sizeof(taos_linked_list_node_t));

  node->item = item;
  node->next = self->head;
  self->head = node;
  if (self->tail == NULL) {
    self->tail = node;
  }
  self->size++;
  return 0;
}

/*
void *taos_linked_list_pop(taos_linked_list_t *self) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return NULL;
  taos_linked_list_node_t *node = self->head;
  void *item = NULL;
  if (node != NULL) {
    item = node->item;
    self->head = node->next;
    if (self->tail == node) {
      self->tail = NULL;
    }
    if (node->item != NULL) {
      if (self->free_fn) {
        (*self->free_fn)(node->item);
      } else {
        taos_free(node->item);
      }
    }
    node->item = NULL;
    node = NULL;
    self->size--;
  }
  return item;
}
*/

int taos_linked_list_remove(taos_linked_list_t *self, void *item) {
  TAOS_ASSERT(self != NULL);
  taos_linked_list_node_t *node;
  taos_linked_list_node_t *prev_node = NULL;
#ifdef TAOS_LOG_ENABLE
  int32_t count = 0;
  char tmp[200] = {0};

  count = 0;
  for (node = self->head; node != NULL; node = node->next) {
    count++;
  }
  sprintf(tmp, "list count:%d", count);
  TAOS_LOG(tmp);
#endif

  // Locate the node
#ifdef TAOS_LOG_ENABLE
  count = 0;
#endif
  for (node = self->head; node != NULL; node = node->next) {
#ifdef TAOS_LOG_ENABLE
    count++;
#endif
    if (self->compare_fn) {
      if ((*self->compare_fn)(node->item, item) == TAOS_EQUAL) {
        break;
      }
    } else {
      if (node->item == item) {
        break;
      }
    }
    prev_node = node;
  }

#ifdef TAOS_LOG_ENABLE
  sprintf(tmp, "remove item:%d", count);
  TAOS_LOG(tmp);
#endif

  if (node == NULL) return 0;

  if (prev_node) {
    prev_node->next = node->next;
  } else {
    self->head = node->next;
  }
  if (node->next == NULL) {
    self->tail = prev_node;
  }

  if (node->item != NULL) {
    if (self->free_fn) {
      (*self->free_fn)(node->item);
    } else {
      taos_free(node->item);
    }
  }

  node->item = NULL;
  taos_free(node);
  node = NULL;
  self->size--;

#ifdef TAOS_LOG_ENABLE
  count = 0;
  for (node = self->head; node != NULL; node = node->next) {
    count++;
  }

  sprintf(tmp, "list count:%d", count);
  TAOS_LOG(tmp);
#endif

  return 0;
}

taos_linked_list_compare_t taos_linked_list_compare(taos_linked_list_t *self, void *item_a, void *item_b) {
  TAOS_ASSERT(self != NULL);
  if (self->compare_fn) {
    return (*self->compare_fn)(item_a, item_b);
  } else {
    return strcmp(item_a, item_b);
  }
}

size_t taos_linked_list_size(taos_linked_list_t *self) {
  TAOS_ASSERT(self != NULL);
  return self->size;
}

int taos_linked_list_set_free_fn(taos_linked_list_t *self, taos_linked_list_free_item_fn free_fn) {
  TAOS_ASSERT(self != NULL);
  self->free_fn = free_fn;
  return 0;
}

int taos_linked_list_set_compare_fn(taos_linked_list_t *self, taos_linked_list_compare_item_fn compare_fn) {
  TAOS_ASSERT(self != NULL);
  self->compare_fn = compare_fn;
  return 0;
}

void taos_linked_list_no_op_free(void *item) {}
