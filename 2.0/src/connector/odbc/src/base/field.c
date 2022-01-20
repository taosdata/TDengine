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

#include "field.h"

static void fieldset_clr_fields(fieldset_t *fieldset) {
  if (!fieldset) return;

  fieldset->fields       = NULL;
  fieldset->n_fields     = 0;
}

static void fieldset_clr_bindings(fieldset_t *fieldset) {
  if (!fieldset) return;

  fieldset->bindings        = NULL;
  fieldset->n_bindings      = 0;
}

void fieldset_init_fields(fieldset_t *fieldset) {
  if (!fieldset) return;
  if (fieldset->fields_cache) return;
  fieldset->fields_cache = todbc_buf_create();
}

void fieldset_init_bindings(fieldset_t *fieldset) {
  if (!fieldset) return;
  if (fieldset->bindings_cache) return;
  fieldset->bindings_cache = todbc_buf_create();
}

void fieldset_reclaim_fields(fieldset_t *fieldset) {
  if (!fieldset) return;

  if (!fieldset->fields_cache) return;

  todbc_buf_reclaim(fieldset->fields_cache);
  fieldset_clr_fields(fieldset);
}

void fieldset_reclaim_bindings(fieldset_t *fieldset) {
  if (!fieldset) return;

  if (!fieldset->bindings_cache) return;

  todbc_buf_reclaim(fieldset->bindings_cache);
  fieldset_clr_bindings(fieldset);
}

void fieldset_release(fieldset_t *fieldset) {
  if (!fieldset) return;

  fieldset_reclaim_fields(fieldset);
  fieldset_reclaim_bindings(fieldset);

  if (fieldset->fields_cache) {
    todbc_buf_free(fieldset->fields_cache);
    fieldset->fields_cache = NULL;
  }

  if (fieldset->bindings_cache) {
    todbc_buf_free(fieldset->bindings_cache);
    fieldset->bindings_cache = NULL;
  }
}

