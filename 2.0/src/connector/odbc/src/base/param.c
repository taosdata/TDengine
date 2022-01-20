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

#include "param.h"

static void paramset_clr_params(paramset_t *paramset) {
  if (!paramset) return;

  paramset->params       = NULL;
  paramset->n_params     = 0;

  paramset->i_row        = 0;
  paramset->i_col        = 0;
}

static void paramset_clr_bindings(paramset_t *paramset) {
  if (!paramset) return;

  paramset->bindings        = NULL;
  paramset->n_bindings      = 0;
}

void paramset_reclaim_params(paramset_t *paramset) {
  if (!paramset) return;

  if (!paramset->params_cache) return;

  todbc_buf_reclaim(paramset->params_cache);
  paramset_clr_params(paramset);
}

void paramset_reclaim_bindings(paramset_t *paramset) {
  if (!paramset) return;

  if (!paramset->bindings_cache) return;

  todbc_buf_reclaim(paramset->bindings_cache);
  paramset_clr_bindings(paramset);
}

void paramset_init_params_cache(paramset_t *paramset) {
  if (!paramset) return;
  if (paramset->params_cache) return;

  OILE(paramset->params==NULL, "");
  OILE(paramset->n_params==0, "");

  paramset->params_cache = todbc_buf_create();
}

void paramset_init_bindings_cache(paramset_t *paramset) {
  if (!paramset) return;
  if (paramset->bindings_cache) return;

  OILE(paramset->bindings==NULL, "");
  OILE(paramset->n_bindings==0, "");

  paramset->bindings_cache = todbc_buf_create();
}

void paramset_release(paramset_t *paramset) {
  if (!paramset) return;

  paramset_reclaim_params(paramset);
  paramset_reclaim_bindings(paramset);

  if (paramset->params_cache) {
    todbc_buf_free(paramset->params_cache);
    paramset->params_cache = NULL;
  }

  if (paramset->bindings_cache) {
    todbc_buf_free(paramset->bindings_cache);
    paramset->bindings_cache = NULL;
  }
}

