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
#include "taos_counter.h"

#include "taos_alloc.h"

// Private
#include "taos_errors.h"
#include "taos_log.h"
#include "taos_metric_formatter_i.h"
#include "taos_metric_i.h"
#include "taos_metric_sample_i.h"
#include "taos_metric_sample_t.h"
#include "taos_metric_t.h"
#include "taos_test.h"

taos_counter_t *taos_counter_new(const char *name, const char *help, size_t label_key_count, const char **label_keys) {
  return (taos_counter_t *)taos_metric_new(TAOS_COUNTER, name, help, label_key_count, label_keys);
}

int taos_counter_destroy(taos_counter_t *self) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 0;
  int r = 0;
  r = taos_metric_destroy(self);
  self = NULL;
  return r;
}

int taos_counter_inc(taos_counter_t *self, const char **label_values) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  taos_metric_sample_t *sample = taos_metric_sample_from_labels(self, label_values);
  if (sample == NULL) return 1;
  return taos_metric_sample_add(sample, 1.0);
}

int taos_counter_add(taos_counter_t *self, double r_value, const char **label_values) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  taos_metric_sample_t *sample = taos_metric_sample_from_labels(self, label_values);
  if (sample == NULL) return 1;
  return taos_metric_sample_add(sample, r_value);
}

int taos_counter_get_keys_size(taos_counter_t *self) { return self->samples->keys->size; }

int taos_counter_get_vgroup_ids(taos_counter_t *self, char ***keys, int32_t **vgroup_ids, int *list_size) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  if (self->samples == NULL) return 1;
  (void)pthread_rwlock_rdlock(self->rwlock);
  taos_linked_list_t *key_list = self->samples->keys;
  *list_size = key_list->size;
  int r = 0;
  *vgroup_ids = (int32_t *)taos_malloc(*list_size * sizeof(int32_t));
  // [TD-SEC-2026-ITEM84] Test the allocated pointer (*vgroup_ids), not the out-param itself
  // (which is the caller's &var and therefore always non-NULL). The original checks were dead code:
  // had the malloc actually failed, the loop below would have dereferenced NULL and crashed.
  // Nothing has been allocated yet on this path, so just NULL the sibling out-param and return;
  // both callers only free non-NULL out-params, so they safely skip.
  if (*vgroup_ids == NULL) {
    *keys = NULL;
    (void)pthread_rwlock_unlock(self->rwlock);
    return 1;
  }
  *keys = (char **)taos_malloc(*list_size * sizeof(char *));
  // [TD-SEC-2026-ITEM84] Same fix: test *keys, not the out-param. The first array (*vgroup_ids) was
  // already allocated, so free it and NULL both out-params; otherwise vmHandle.c (which does NOT free
  // on r != 0) would leak it, while metrics.c would safely skip the NULLs.
  if (*keys == NULL) {
    taos_free(*vgroup_ids);
    *vgroup_ids = NULL;
    *keys = NULL;
    (void)pthread_rwlock_unlock(self->rwlock);
    return 1;
  }
  int index = 0;
  for (taos_linked_list_node_t *current_key = key_list->head; current_key != NULL; current_key = current_key->next) {
    char   *key = (char *)current_key->item;
    int32_t vgroup_id = taos_metric_formatter_get_vgroup_id(key);
    (*vgroup_ids)[index] = vgroup_id;
    // [TD-SEC-2026-ITEM84] Copy the key string instead of returning the raw pointer that lives
    // inside self->samples->keys. The caller (vmCleanExpriedSamples) uses the keys AFTER this
    // function drops self->rwlock; a concurrent rehash (taos_map_set -> taos_map_ensure_space)
    // would otherwise free the original node and leave the returned pointers dangling
    // (use-after-free / CWE-416) when the caller later passes them to taos_counter_delete.
    // The caller is responsible for freeing each copied string (see vmHandle.c).
    char *key_copy = (char *)taos_malloc(strlen(key) + 1);
    if (key_copy == NULL) {
      for (int j = 0; j < index; j++) {
        taos_free((*keys)[j]);
      }
      taos_free(*keys);
      taos_free(*vgroup_ids);
      // [TD-SEC-2026-ITEM84] Reset the out-params so callers (which free them again on r != 0)
      // do not double-free the arrays we just freed.
      *keys = NULL;
      *vgroup_ids = NULL;
      (void)pthread_rwlock_unlock(self->rwlock);
      return 1;
    }
    memcpy(key_copy, key, strlen(key) + 1);
    (*keys)[index] = key_copy;
    index++;
  }
  (void)pthread_rwlock_unlock(self->rwlock);
  return r;
}

int taos_counter_delete(taos_counter_t *self, char *key) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  return taos_map_delete(self->samples, key);
}