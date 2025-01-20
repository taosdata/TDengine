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

#include <pthread.h>

// Public
#include "taos_alloc.h"

// Private
#include "taos_errors.h"
#include "taos_log.h"
#include "taos_map_i.h"
#include "taos_metric_formatter_i.h"
#include "taos_metric_i.h"
#include "taos_metric_sample_i.h"
#include "taos_test.h"

char *taos_metric_type_map[4] = {"counter", "gauge", "histogram", "summary"};

taos_metric_t *taos_metric_new(taos_metric_type_t metric_type, const char *name, const char *help,
                               size_t label_key_count, const char **label_keys) {
  int r = 0;
  taos_metric_t *self = (taos_metric_t *)taos_malloc(sizeof(taos_metric_t));
  if (self == NULL) return NULL;
  memset(self, 0, sizeof(taos_metric_t));
  self->type = metric_type;
  int len = strlen(name) + 1;
  self->name = taos_malloc(len);
  memset(self->name, 0, len);
  strcpy(self->name, name);
  //self->name = name;
  self->help = help;

  const char **k = (const char **)taos_malloc(sizeof(const char *) * label_key_count);

  for (int i = 0; i < label_key_count; i++) {
    if (strcmp(label_keys[i], "le") == 0) {
      TAOS_LOG(TAOS_METRIC_INVALID_LABEL_NAME);
      if (taos_metric_destroy(self) != 0) return NULL;
      return NULL;
    }
    if (strcmp(label_keys[i], "quantile") == 0) {
      TAOS_LOG(TAOS_METRIC_INVALID_LABEL_NAME);
      if (taos_metric_destroy(self) != 0) return NULL;
      return NULL;
    }
    k[i] = taos_strdup(label_keys[i]);
  }
  self->label_keys = k;
  self->label_key_count = label_key_count;
  self->samples = taos_map_new();

  if (metric_type == TAOS_HISTOGRAM) {

  } else {
    r = taos_map_set_free_value_fn(self->samples, &taos_metric_sample_free_generic);
    if (r) {
      if (taos_metric_destroy(self) != 0) return NULL;
      return NULL;
    }
  }

  self->formatter = taos_metric_formatter_new();
  if (self->formatter == NULL) {
    if (taos_metric_destroy(self) != 0) return NULL;
    return NULL;
  }
  self->rwlock = (pthread_rwlock_t *)taos_malloc(sizeof(pthread_rwlock_t));
  r = pthread_rwlock_init(self->rwlock, NULL);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_INIT_ERROR);
    taos_free(self);
    return NULL;
  }
  return self;
}

int taos_metric_destroy(taos_metric_t *self) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 0;

  int r = 0;
  int ret = 0;

  if(self->samples != NULL){
    r = taos_map_destroy(self->samples);
    self->samples = NULL;
    if (r) ret = r;
  }

  taos_metric_formatter_destroy(self->formatter);
  self->formatter = NULL;

  r = pthread_rwlock_destroy(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_DESTROY_ERROR);
    ret = r;
  }

  taos_free(self->rwlock);
  self->rwlock = NULL;

  for (int i = 0; i < self->label_key_count; i++) {
    taos_free((void *)self->label_keys[i]);
    self->label_keys[i] = NULL;
  }
  taos_free(self->label_keys);
  self->label_keys = NULL;

  taos_free(self->name);
  self->name = NULL;

  taos_free(self);
  self = NULL;

  return ret;
}

int taos_metric_destroy_generic(void *item) {
  int r = 0;
  taos_metric_t *self = (taos_metric_t *)item;
  r = taos_metric_destroy(self);
  self = NULL;
  return r;
}

void taos_metric_free_generic(void *item) {
  taos_metric_t *self = (taos_metric_t *)item;
  if (taos_metric_destroy(self) != 0) {
    TAOS_LOG("taos_metric_destroy failed");
  }
}

taos_metric_sample_t *taos_metric_sample_from_labels(taos_metric_t *self, const char **label_values) {
  TAOS_TEST_PARA_NULL(self != NULL);
  int r = 0;
  r = pthread_rwlock_wrlock(self->rwlock);
  if (r) {
    TAOS_LOG(TAOS_PTHREAD_RWLOCK_LOCK_ERROR);
    return NULL;
  }

#define TAOS_METRIC_SAMPLE_FROM_LABELS_HANDLE_UNLOCK() \
  r = pthread_rwlock_unlock(self->rwlock);             \
  if (r) TAOS_LOG(TAOS_PTHREAD_RWLOCK_UNLOCK_ERROR);   \
  return NULL;

  // Get l_value
  r = taos_metric_formatter_load_l_value(self->formatter, self->name, NULL, self->label_key_count, self->label_keys, label_values);
  if (r) {
    TAOS_METRIC_SAMPLE_FROM_LABELS_HANDLE_UNLOCK();
  }

  // This must be freed before returning
  const char *l_value = taos_metric_formatter_dump(self->formatter);
  if (l_value == NULL) {
    TAOS_METRIC_SAMPLE_FROM_LABELS_HANDLE_UNLOCK();
  }

  // Get sample
  taos_metric_sample_t *sample = (taos_metric_sample_t *)taos_map_get(self->samples, l_value);
  if (sample == NULL) {
    sample = taos_metric_sample_new(self->type, l_value, 0.0);
    r = taos_map_set(self->samples, l_value, sample);
    if (r) {
      taos_free((void *)l_value);
      TAOS_METRIC_SAMPLE_FROM_LABELS_HANDLE_UNLOCK();
    }
  }
  r = pthread_rwlock_unlock(self->rwlock);
  if (r) TAOS_LOG(TAOS_PTHREAD_RWLOCK_UNLOCK_ERROR); 
  taos_free((void *)l_value);
  return sample;
}

