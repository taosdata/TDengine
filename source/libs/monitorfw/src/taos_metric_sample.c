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
#include "taos_errors.h"
#include "taos_log.h"
#include "taos_metric_sample_i.h"
#include "taos_metric_sample_t.h"
#include "taos_test.h"

#ifdef C11_ATOMIC
#include <stdatomic.h>
#else
#define ALLOW_FORBID_FUNC
#include "tdef.h"
#include "osAtomic.h"
#endif

taos_metric_sample_t *taos_metric_sample_new(taos_metric_type_t type, const char *l_value, double r_value) {
  taos_metric_sample_t *self = (taos_metric_sample_t *)taos_malloc(sizeof(taos_metric_sample_t));
  self->type = type;
  self->l_value = taos_strdup(l_value);
  self->r_value = 0;
  return self;
}

int taos_metric_sample_destroy(taos_metric_sample_t *self) {
  TAOS_TEST_PARA(self != NULL);
  taos_free((void *)self->l_value);
  self->l_value = NULL;
  taos_free((void *)self);
  self = NULL;
  return 0;
}

int taos_metric_sample_destroy_generic(void *gen) {
  int r = 0;

  taos_metric_sample_t *self = (taos_metric_sample_t *)gen;
  r = taos_metric_sample_destroy(self);
  self = NULL;
  return r;
}

void taos_metric_sample_free_generic(void *gen) {
  taos_metric_sample_t *self = (taos_metric_sample_t *)gen;
  if(taos_metric_sample_destroy(self) != 0) {
    TAOS_LOG(TAOS_METRIC_SAMPLE_DESTROY_ERROR);
  }
}

int taos_metric_sample_add(taos_metric_sample_t *self, double r_value) {
  TAOS_TEST_PARA(self != NULL);
  if (r_value < 0) {
    return 1;
  }
  
#ifdef C11_ATOMIC
  /*_Atomic*/ double old = atomic_load(&self->r_value);

  for (;;) {
    _Atomic double new = ATOMIC_VAR_INIT(old + r_value);
    if (atomic_compare_exchange_weak(&self->r_value, &old, new)) {
      return 0;
    }
  }
#else
#ifdef DOUBLE_ATOMIC
  atomic_fetch_add_double(&self->r_value, r_value);
#else
  atomic_fetch_add_64(&self->r_value, r_value);
#endif
#endif

  return 0;
}

/*
int taos_metric_sample_sub(taos_metric_sample_t *self, double r_value) {
  TAOS_TEST_PARA(self != NULL);
  if (self->type != TAOS_GAUGE) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }

#ifdef C11_ATOMIC
  ///_Atomic/
  double old = atomic_load(&self->r_value);
  for (;;) {
    _Atomic double new = ATOMIC_VAR_INIT(old - r_value);
    if (atomic_compare_exchange_weak(&self->r_value, &old, new)) {
      return 0;
    }
  }
#else
#ifdef DOUBLE_ATOMIC
  atomic_fetch_sub_double(&self->r_value, r_value);
#else
  atomic_fetch_sub_64(&self->r_value, r_value);
#endif
#endif

  return 0;
}
*/

int taos_metric_sample_set(taos_metric_sample_t *self, double r_value) {
  if (self->type != TAOS_GAUGE && self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }

#ifdef C11_ATOMIC
  atomic_store(&self->r_value, r_value);
#else
#ifdef DOUBLE_ATOMIC
  atomic_store_double(&self->r_value, r_value);
#else
  atomic_store_64(&self->r_value, r_value);
#endif
#endif  
  
  return 0;
}

int taos_metric_sample_exchange(taos_metric_sample_t *self, double r_value, double* old_value) {
  if (self->type != TAOS_GAUGE && self->type != TAOS_COUNTER) {
    TAOS_LOG(TAOS_METRIC_INCORRECT_TYPE);
    return 1;
  }

#ifdef C11_ATOMIC
  _Atomic double new = ATOMIC_VAR_INIT(r_value);
  for (;;) {
    /*_Atomic*/ double old = atomic_load(&self->r_value);
    *old_value = old;
    if (atomic_compare_exchange_weak(&self->r_value, &old, new)) {
      return 0;
    }
  }
#else
#ifdef DOUBLE_ATOMIC
  *old_value = atomic_exchange_double(&self->r_value, r_value);
#else
  *old_value = atomic_exchange_64(&self->r_value, r_value);
#endif
#endif   
  
  return 0;
}