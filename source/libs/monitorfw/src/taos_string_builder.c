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

#include <stddef.h>

// Public
#include "taos_alloc.h"

// Private
#include "taos_assert.h"
#include "taos_string_builder_i.h"
#include "taos_string_builder_t.h"

// The initial size of a string created via taos_string_builder
#define TAOS_STRING_BUILDER_INIT_SIZE 32

// taos_string_builder_init prototype declaration
int taos_string_builder_init(taos_string_builder_t *self);

struct taos_string_builder {
  char *str;        /**< the target string  */
  size_t allocated; /**< the size allocated to the string in bytes */
  size_t len;       /**< the length of str */
  size_t init_size; /**< the initialize size of space to allocate */
};

taos_string_builder_t *taos_string_builder_new(void) {
  int r = 0;

  taos_string_builder_t *self = (taos_string_builder_t *)taos_malloc(sizeof(taos_string_builder_t));
  self->init_size = TAOS_STRING_BUILDER_INIT_SIZE;
  r = taos_string_builder_init(self);
  if (r) {
    taos_string_builder_destroy(self);
    return NULL;
  }

  return self;
}

int taos_string_builder_init(taos_string_builder_t *self) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  self->str = (char *)taos_malloc(self->init_size);
  *self->str = '\0';
  self->allocated = self->init_size;
  self->len = 0;
  return 0;
}

int taos_string_builder_destroy(taos_string_builder_t *self) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 0;
  taos_free(self->str);
  self->str = NULL;
  taos_free(self);
  self = NULL;
  return 0;
}

/**
 * @brief API PRIVATE Grows the size of the string given the value we want to add
 *
 * The method continuously shifts left until the new size is large enough to accommodate add_len. This private method
 * is called in methods that need to add one or more characters to the underlying string.
 */
static int taos_string_builder_ensure_space(taos_string_builder_t *self, size_t add_len) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (add_len == 0 || self->allocated >= self->len + add_len + 1) return 0;
  while (self->allocated < self->len + add_len + 1) self->allocated <<= 1;
  self->str = (char *)taos_realloc(self->str, self->allocated);
  return 0;
}

int taos_string_builder_add_str(taos_string_builder_t *self, const char *str) {
  TAOS_ASSERT(self != NULL);
  int r = 0;

  if (self == NULL) return 1;
  if (str == NULL || *str == '\0') return 0;

  size_t len = strlen(str);
  r = taos_string_builder_ensure_space(self, len);
  if (r) return r;

  memcpy(self->str + self->len, str, len);
  self->len += len;
  self->str[self->len] = '\0';
  return 0;
}

int taos_string_builder_add_char(taos_string_builder_t *self, char c) {
  TAOS_ASSERT(self != NULL);
  int r = 0;

  if (self == NULL) return 1;
  r = taos_string_builder_ensure_space(self, 1);
  if (r) return r;

  self->str[self->len] = c;
  self->len++;
  self->str[self->len] = '\0';
  return 0;
}

int taos_string_builder_truncate(taos_string_builder_t *self, size_t len) {
  TAOS_ASSERT(self != NULL);
  if (self == NULL) return 1;
  if (len >= self->len) return 0;

  self->len = len;
  self->str[self->len] = '\0';
  return 0;
}

int taos_string_builder_clear(taos_string_builder_t *self) {
  TAOS_ASSERT(self != NULL);
  taos_free(self->str);
  self->str = NULL;
  return taos_string_builder_init(self);
}

size_t taos_string_builder_len(taos_string_builder_t *self) {
  TAOS_ASSERT(self != NULL);
  return self->len;
}

char *taos_string_builder_dump(taos_string_builder_t *self) {
  TAOS_ASSERT(self != NULL);
  // +1 to accommodate \0
  char *out = (char *)taos_malloc((self->len + 1) * sizeof(char));
  memcpy(out, self->str, self->len + 1);
  return out;
}

char *taos_string_builder_str(taos_string_builder_t *self) {
  TAOS_ASSERT(self != NULL);
  return self->str;
}
