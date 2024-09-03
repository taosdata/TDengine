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
#include "taos_test.h"
#include "taos_errors.h"
#include "taos_log.h"
#include "taos_metric_i.h"
#include "taos_metric_sample_i.h"
#include "taos_metric_sample_t.h"
#include "taos_metric_t.h"
#include "taos_metric_formatter_i.h"

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

int taos_counter_get_keys_size(taos_counter_t *self) {
  return self->samples->keys->size;
}

int taos_counter_get_vgroup_ids(taos_counter_t *self, char ***keys,int32_t **vgroup_ids) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  if (self->samples == NULL) return 1;
  taos_linked_list_t *key_list = self->samples->keys;
  int r = 0;
  *vgroup_ids = (int32_t *)taos_malloc(key_list->size*sizeof(int32_t));
  *keys = (char **)taos_malloc(key_list->size*sizeof(char *));
  int index = 0;
  for (taos_linked_list_node_t *current_key = key_list->head; current_key != NULL;current_key = current_key->next) {
    char *key = (char *)current_key->item;
    int32_t vgroup_id = taos_metric_formatter_get_vgroup_id(key);
    (*vgroup_ids)[index] = vgroup_id;
    (*keys)[index] = key;
    index++;
  }
  return r;
}

int taos_counter_delete(taos_counter_t *self, char *key) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  return  taos_map_delete(self->samples, key);
}