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

// Public
#include "taos_gauge.h"

#include "taos_alloc.h"

// Private
#include "taos_assert.h"
#include "taos_errors.h"
#include "taos_log.h"
#include "taos_metric_i.h"
#include "taos_metric_sample_i.h"
#include "taos_metric_sample_t.h"
#include "taos_metric_t.h"

taos_gauge_t *taos_gauge_new(const char *name, const char *help, size_t label_key_count, const char **label_keys) {
  return (taos_gauge_t *)taos_metric_new(TAOS_GAUGE, name, help, label_key_count, label_keys);
}

int taos_gauge_destroy(taos_gauge_t *self) {
  TAOS_ASSERT(self != NULL);
  int r = 0;
  r = taos_metric_destroy(self);
  self = NULL;
  return r;
}

int taos_gauge_inc(taos_gauge_t *self, const char **label_values) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_GAUGE) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  taos_metric_sample_t *sample = taos_metric_sample_from_labels(self, label_values);
  if (sample == NULL) return 1;
  return taos_metric_sample_add(sample, 1.0);
}

int taos_gauge_dec(taos_gauge_t *self, const char **label_values) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_GAUGE) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  taos_metric_sample_t *sample = taos_metric_sample_from_labels(self, label_values);
  if (sample == NULL) return 1;
  return taos_metric_sample_sub(sample, 1.0);
}

int taos_gauge_add(taos_gauge_t *self, double r_value, const char **label_values) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_GAUGE) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  taos_metric_sample_t *sample = taos_metric_sample_from_labels(self, label_values);
  if (sample == NULL) return 1;
  return taos_metric_sample_add(sample, r_value);
}

int taos_gauge_sub(taos_gauge_t *self, double r_value, const char **label_values) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_GAUGE) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  taos_metric_sample_t *sample = taos_metric_sample_from_labels(self, label_values);
  if (sample == NULL) return 1;
  return taos_metric_sample_sub(sample, r_value);
}

int taos_gauge_set(taos_gauge_t *self, double r_value, const char **label_values) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (self->type != TAOS_GAUGE) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }
  taos_metric_sample_t *sample = taos_metric_sample_from_labels(self, label_values);
  if (sample == NULL) return 1;
  return taos_metric_sample_set(sample, r_value);
}
