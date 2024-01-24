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

// Private
#include "taos_assert.h"
#include "taos_collector_t.h"
#include "taos_linked_list_t.h"
#include "taos_map_i.h"
#include "taos_metric_formatter_i.h"
#include "taos_metric_sample_t.h"
#include "taos_metric_t.h"
#include "taos_string_builder_i.h"


taos_metric_formatter_t *taos_metric_formatter_new() {
  taos_metric_formatter_t *self = (taos_metric_formatter_t *)taos_malloc(sizeof(taos_metric_formatter_t));
  self->string_builder = taos_string_builder_new();
  if (self->string_builder == NULL) {
    taos_metric_formatter_destroy(self);
    return NULL;
  }
  self->err_builder = taos_string_builder_new();
  if (self->err_builder == NULL) {
    taos_metric_formatter_destroy(self);
    return NULL;
  }
  return self;
}

int taos_metric_formatter_destroy(taos_metric_formatter_t *self) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 0;

  int r = 0;
  int ret = 0;

  r = taos_string_builder_destroy(self->string_builder);
  self->string_builder = NULL;
  if (r) ret = r;

  r = taos_string_builder_destroy(self->err_builder);
  self->err_builder = NULL;
  if (r) ret = r;

  taos_free(self);
  self = NULL;
  return ret;
}

int taos_metric_formatter_load_help(taos_metric_formatter_t *self, const char *name, const char *help) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  r = taos_string_builder_add_str(self->string_builder, "# HELP ");
  if (r) return r;

  r = taos_string_builder_add_str(self->string_builder, name);
  if (r) return r;

  r = taos_string_builder_add_char(self->string_builder, ' ');
  if (r) return r;

  r = taos_string_builder_add_str(self->string_builder, help);
  if (r) return r;

  return taos_string_builder_add_char(self->string_builder, '\n');
}

int taos_metric_formatter_load_type(taos_metric_formatter_t *self, const char *name, taos_metric_type_t metric_type) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  r = taos_string_builder_add_str(self->string_builder, "# TYPE ");
  if (r) return r;

  r = taos_string_builder_add_str(self->string_builder, name);
  if (r) return r;

  r = taos_string_builder_add_char(self->string_builder, ' ');
  if (r) return r;

  r = taos_string_builder_add_str(self->string_builder, taos_metric_type_map[metric_type]);
  if (r) return r;

  return taos_string_builder_add_char(self->string_builder, '\n');
}

int taos_metric_formatter_load_l_value(taos_metric_formatter_t *self, const char *name, const char *suffix,
                                       size_t label_count, const char **label_keys, const char **label_values) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  r = taos_string_builder_add_str(self->string_builder, name);
  if (r) return r;

  if (suffix != NULL) {
    r = taos_string_builder_add_char(self->string_builder, '_');
    if (r) return r;

    r = taos_string_builder_add_str(self->string_builder, suffix);
    if (r) return r;
  }

  if (label_count == 0) return 0;

  for (int i = 0; i < label_count; i++) {
    if (i == 0) {
      r = taos_string_builder_add_char(self->string_builder, '{');
      if (r) return r;
    }
    r = taos_string_builder_add_str(self->string_builder, (const char *)label_keys[i]);
    if (r) return r;

    r = taos_string_builder_add_char(self->string_builder, '=');
    if (r) return r;

    r = taos_string_builder_add_char(self->string_builder, '"');
    if (r) return r;

    r = taos_string_builder_add_str(self->string_builder, (const char *)label_values[i]);
    if (r) return r;

    r = taos_string_builder_add_char(self->string_builder, '"');
    if (r) return r;

    if (i == label_count - 1) {
      r = taos_string_builder_add_char(self->string_builder, '}');
      if (r) return r;
    } else {
      r = taos_string_builder_add_char(self->string_builder, ',');
      if (r) return r;
    }
  }
  return 0;
}

int taos_metric_formatter_load_sample(taos_metric_formatter_t *self, taos_metric_sample_t *sample, 
                                      char *ts, char *format) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  r = taos_string_builder_add_str(self->string_builder, sample->l_value);
  if (r) return r;

  r = taos_string_builder_add_char(self->string_builder, ' ');
  if (r) return r;

  char buffer[50];
  sprintf(buffer, format, sample->r_value);
  r = taos_string_builder_add_str(self->string_builder, buffer);
  if (r) return r;

  r = taos_string_builder_add_char(self->string_builder, ' ');
  if (r) return r;

  r = taos_string_builder_add_str(self->string_builder, ts);
  if (r) return r;

  //taos_metric_sample_set(sample, 0);

  return taos_string_builder_add_char(self->string_builder, '\n');
}

int taos_metric_formatter_clear(taos_metric_formatter_t *self) {
  TAOS_ASSERT(self != NULL);
  return taos_string_builder_clear(self->string_builder);
}

char *taos_metric_formatter_dump(taos_metric_formatter_t *self) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  if (self == NULL) return NULL;
  char *data = taos_string_builder_dump(self->string_builder);
  if (data == NULL) return NULL;
  r = taos_string_builder_clear(self->string_builder);
  if (r) {
    taos_free(data);
    return NULL;
  }
  return data;
}

int taos_metric_formatter_load_metric(taos_metric_formatter_t *self, taos_metric_t *metric, char *ts, char *format) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  r = taos_metric_formatter_load_help(self, metric->name, metric->help);
  if (r) return r;

  r = taos_metric_formatter_load_type(self, metric->name, metric->type);
  if (r) return r;

  for (taos_linked_list_node_t *current_node = metric->samples->keys->head; current_node != NULL;
       current_node = current_node->next) {
    const char *key = (const char *)current_node->item;
    if (metric->type == TAOS_HISTOGRAM) {

    } else {
      taos_metric_sample_t *sample = (taos_metric_sample_t *)taos_map_get(metric->samples, key);
      if (sample == NULL) return 1;
      r = taos_metric_formatter_load_sample(self, sample, ts, format);
      if (r) return r;
    }
  }
  return taos_string_builder_add_char(self->string_builder, '\n');
}

int taos_metric_formatter_load_metrics(taos_metric_formatter_t *self, taos_map_t *collectors, char *ts, char *format) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  for (taos_linked_list_node_t *current_node = collectors->keys->head; current_node != NULL;
       current_node = current_node->next) {
    const char *collector_name = (const char *)current_node->item;
    taos_collector_t *collector = (taos_collector_t *)taos_map_get(collectors, collector_name);
    if (collector == NULL) return 1;

    taos_map_t *metrics = collector->collect_fn(collector);
    if (metrics == NULL) return 1;

    for (taos_linked_list_node_t *current_node = metrics->keys->head; current_node != NULL;
         current_node = current_node->next) {
      const char *metric_name = (const char *)current_node->item;
      taos_metric_t *metric = (taos_metric_t *)taos_map_get(metrics, metric_name);
      if (metric == NULL) return 1;
      r = taos_metric_formatter_load_metric(self, metric, ts, format);
      if (r) return r;
    }
  }
  return r;
}
