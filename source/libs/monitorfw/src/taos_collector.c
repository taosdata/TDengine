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

#include <stdio.h>

// Public
#include "taos_alloc.h"
#include "taos_collector.h"
#include "taos_collector_registry.h"

// Private
#include "taos_test.h"
#include "taos_collector_t.h"
#include "taos_log.h"
#include "taos_map_i.h"
#include "taos_metric_i.h"
#include "taos_string_builder_i.h"

taos_map_t *taos_collector_default_collect(taos_collector_t *self) { return self->metrics; }

taos_collector_t *taos_collector_new(const char *name) {
  int r = 0;
  taos_collector_t *self = (taos_collector_t *)taos_malloc(sizeof(taos_collector_t));
  if (self == NULL) return NULL;
  memset(self, 0, sizeof(taos_collector_t));
  self->name = taos_strdup(name);
  self->metrics = taos_map_new();
  if (self->metrics == NULL) {
    if (taos_collector_destroy(self) != 0) {
      TAOS_LOG("taos_collector_destroy failed");
    }
    return NULL;
  }
  r = taos_map_set_free_value_fn(self->metrics, &taos_metric_free_generic);
  if (r) {
    if (taos_collector_destroy(self) != 0) {
      TAOS_LOG("taos_collector_destroy failed");
    }
    return NULL;
  }
  self->collect_fn = &taos_collector_default_collect;
  self->string_builder = taos_string_builder_new();
  if (self->string_builder == NULL) {
    if (taos_collector_destroy(self) != 0) {
      TAOS_LOG("taos_collector_destroy failed");
    }
    return NULL;
  }
  self->proc_limits_file_path = NULL;
  self->proc_stat_file_path = NULL;
  return self;
}

int taos_collector_destroy(taos_collector_t *self) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 0;

  int r = 0;
  int ret = 0;

  r = taos_map_destroy(self->metrics);
  if (r) ret = r;
  self->metrics = NULL;

  if(self->string_builder != NULL){
    taos_string_builder_destroy(self->string_builder);
    self->string_builder = NULL;
  }

  taos_free((char *)self->name);
  self->name = NULL;
  taos_free(self);
  self = NULL;

  return ret;
}

int taos_collector_destroy_generic(void *gen) {
  int r = 0;
  taos_collector_t *self = (taos_collector_t *)gen;
  r = taos_collector_destroy(self);
  self = NULL;
  return r;
}

void taos_collector_free_generic(void *gen) {
  taos_collector_t *self = (taos_collector_t *)gen;
  if (taos_collector_destroy(self) != 0) {
    TAOS_LOG("taos_collector_destroy failed");
  }
}

int taos_collector_set_collect_fn(taos_collector_t *self, taos_collect_fn *fn) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  self->collect_fn = fn;
  return 0;
}

int taos_collector_add_metric(taos_collector_t *self, taos_metric_t *metric) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  if (taos_map_get(self->metrics, metric->name) != NULL) {
    TAOS_LOG("metric already found in collector");
    return 1;
  }
  return taos_map_set(self->metrics, metric->name, metric);
}

int taos_collector_remove_metric(taos_collector_t *self, const char* key){
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;
  return taos_map_delete(self->metrics, key);
}

taos_metric_t* taos_collector_get_metric(taos_collector_t *self, char *metric_name){
  TAOS_TEST_PARA_NULL(self != NULL);
  if (self == NULL) return NULL;
  return taos_map_get(self->metrics, metric_name);
}