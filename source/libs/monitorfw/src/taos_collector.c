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

#include <stdio.h>

// Public
#include "taos_alloc.h"
#include "taos_collector.h"
#include "taos_collector_registry.h"

// Private
#include "taos_assert.h"
#include "taos_collector_t.h"
#include "taos_log.h"
#include "taos_map_i.h"
#include "taos_metric_i.h"
#include "taos_string_builder_i.h"

taos_map_t *taos_collector_default_collect(taos_collector_t *self) { return self->metrics; }

taos_collector_t *taos_collector_new(const char *name) {
  int r = 0;
  taos_collector_t *self = (taos_collector_t *)taos_malloc(sizeof(taos_collector_t));
  self->name = taos_strdup(name);
  self->metrics = taos_map_new();
  if (self->metrics == NULL) {
    taos_collector_destroy(self);
    return NULL;
  }
  r = taos_map_set_free_value_fn(self->metrics, &taos_metric_free_generic);
  if (r) {
    taos_collector_destroy(self);
    return NULL;
  }
  self->collect_fn = &taos_collector_default_collect;
  self->string_builder = taos_string_builder_new();
  if (self->string_builder == NULL) {
    taos_collector_destroy(self);
    return NULL;
  }
  self->proc_limits_file_path = NULL;
  self->proc_stat_file_path = NULL;
  return self;
}

int taos_collector_destroy(taos_collector_t *self) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 0;

  int r = 0;
  int ret = 0;

  r = taos_map_destroy(self->metrics);
  if (r) ret = r;
  self->metrics = NULL;

  r = taos_string_builder_destroy(self->string_builder);
  if (r) ret = r;
  self->string_builder = NULL;

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
  taos_collector_destroy(self);
}

int taos_collector_set_collect_fn(taos_collector_t *self, taos_collect_fn *fn) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  self->collect_fn = fn;
  return 0;
}

int taos_collector_add_metric(taos_collector_t *self, taos_metric_t *metric) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (taos_map_get(self->metrics, metric->name) != NULL) {
    TAOS_LOG("metric already found in collector");
    return 1;
  }
  return taos_map_set(self->metrics, metric->name, metric);
}

int taos_collector_remove_metric(taos_collector_t *self, const char* key){
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  return taos_map_delete(self->metrics, key);
}

taos_metric_t* taos_collector_get_metric(taos_collector_t *self, char *metric_name){
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return NULL;
  return taos_map_get(self->metrics, metric_name);
}