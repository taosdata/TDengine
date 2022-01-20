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

#ifndef _todbc_list_h_
#define _todbc_list_h_

#include <stdint.h>
#include <stddef.h>

// non-thread-safe

typedef struct todbc_list_s            todbc_list_t;
typedef struct todbc_list_node_s       todbc_list_node_t;

typedef struct todbc_list_conf_s       todbc_list_conf_t;

typedef void (*list_val_free_f)(todbc_list_t *list, void *val, void *arg);

struct todbc_list_conf_s {
  void               *arg;
  list_val_free_f    val_free;
};

todbc_list_t* todbc_list_create(todbc_list_conf_t conf);
void          todbc_list_free(todbc_list_t *list);

size_t todbc_list_count(todbc_list_t *list);

int todbc_list_pushback(todbc_list_t *list, void *val);
int todbc_list_pushfront(todbc_list_t *list, void *val);
int todbc_list_popback(todbc_list_t *list, void **val, int *found);
int todbc_list_popfront(todbc_list_t *list, void **val, int *found);
int todbc_list_pop(todbc_list_t *list, void *val, int *found);
typedef int (*list_iterate_f)(todbc_list_t *list, void *val, void *arg);
int todbc_list_traverse(todbc_list_t *list, list_iterate_f iterate, void *arg);
typedef int (*list_comp_f)(todbc_list_t *list, void *old, void *val, void *arg);
int todbc_list_find(todbc_list_t *list, void *val, int *found, list_comp_f comp, void *arg);


#endif // _todbc_list_h_

