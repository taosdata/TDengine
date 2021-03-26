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

#include "todbc_list.h"

#include "todbc_log.h"

#include <stdlib.h>

struct todbc_list_s {
  todbc_list_conf_t               conf;

  todbc_list_node_t              *head;
  todbc_list_node_t              *tail;
  size_t                          count;
};

struct todbc_list_node_s {
  void                           *val;

  todbc_list_t                   *list;
  todbc_list_node_t              *next;
  todbc_list_node_t              *prev;
};

static void do_remove(todbc_list_t *list, todbc_list_node_t *node);

static void do_pushback(todbc_list_t *list, todbc_list_node_t *node);
static void do_pushfront(todbc_list_t *list, todbc_list_node_t *node);

todbc_list_t* todbc_list_create(todbc_list_conf_t conf) {
  todbc_list_t *list = (todbc_list_t*)calloc(1, sizeof(*list));
  if (!list) return NULL;

  list->conf    = conf;

  return list;
}

void todbc_list_free(todbc_list_t *list) {
  if (!list) return;

  while (list->head) {
    void *val = list->head->val;
    do_remove(list, list->head);
    if (!list->conf.val_free) continue;
    list->conf.val_free(list, val, list->conf.arg);
  }

  DASSERT(list->count == 0);
}

size_t todbc_list_count(todbc_list_t *list) {
  if (!list) return 0;
  return list->count;
}

int todbc_list_pushback(todbc_list_t *list, void *val) {
  if (!list) return -1;

  todbc_list_node_t *node = (todbc_list_node_t*)calloc(1, sizeof(*node));
  if (!node) return -1;
  node->val          = val;

  do_pushback(list, node);

  return 0;
}

int todbc_list_pushfront(todbc_list_t *list, void *val) {
  if (!list) return -1;

  todbc_list_node_t *node = (todbc_list_node_t*)calloc(1, sizeof(*node));
  if (!node) return -1;
  node->val          = val;

  do_pushfront(list, node);

  return 0;
}

int todbc_list_popback(todbc_list_t *list, void **val, int *found) {
  if (!list) return -1;
  if (!found) return -1;

  *found = 0;

  todbc_list_node_t *node = list->tail;
  if (!node) return 0;

  if (val) *val = node->val;
  do_remove(list, node);

  *found        = 1;

  return 0;
}

int todbc_list_popfront(todbc_list_t *list, void **val, int *found) {
  if (!list) return -1;
  if (!found) return -1;

  *found = 0;

  todbc_list_node_t *node = list->head;
  if (!node) return 0;

  if (val) *val = node->val;
  do_remove(list, node);

  *found        = 1;

  return 0;
}

int todbc_list_pop(todbc_list_t *list, void *val, int *found) {
  if (!list) return -1;
  if (!found) return -1;

  *found = 0;

  todbc_list_node_t *node = list->head;
  while (node) {
    if (node->val == val) break;
    node = node->next;
  }

  if (!node) return 0;

  do_remove(list, node);

  *found        = 1;

  return 0;
}

int todbc_list_traverse(todbc_list_t *list, list_iterate_f iterate, void *arg) {
  if (!list) return -1;
  if (!iterate) return -1;

  todbc_list_node_t *node = list->head;
  while (node) {
    int r = iterate(list, node->val, arg);
    if (r) return r;
    node = node->next;
  }

  return 0;
}

typedef struct comp_s             comp_t;
struct comp_s {
  list_comp_f     comp;
  void           *arg;
  int found;
  void *val;
};

static int do_comp(todbc_list_t *list, void *val, void *arg);

int todbc_list_find(todbc_list_t *list, void *val, int *found, list_comp_f comp, void *arg) {
  if (!list) return -1;
  if (!found) return -1;
  if (!comp) return -1;

  *found = 0;

  comp_t sarg = {0};
  sarg.comp     = comp;
  sarg.arg      = arg;
  sarg.val      = val;
  todbc_list_traverse(list, do_comp, &sarg);
  if (sarg.found) {
    *found = 1;
  }
  return 0;
}

static int do_comp(todbc_list_t *list, void *val, void *arg) {
  comp_t *sarg = (comp_t*)arg;

  int r = sarg->comp(list, val, sarg->val, sarg->arg);
  if (r==0) {
    sarg->found = 1;
  }

  return r;
}

static void do_remove(todbc_list_t *list, todbc_list_node_t *node) {
  DASSERT(node);
  DASSERT(list);
  DASSERT(list == node->list);

  todbc_list_node_t *prev = node->prev;
  todbc_list_node_t *next = node->next;
  if (prev) prev->next  = next;
  else      list->head  = next;
  if (next) next->prev  = prev;
  else      list->tail  = prev;
  node->prev = NULL;
  node->next = NULL;
  node->list = NULL;
  node->val  = NULL;

  list->count -= 1;
  DASSERT(list->count <= INT64_MAX);

  free(node);
}

static void do_pushback(todbc_list_t *list, todbc_list_node_t *node) {
  DASSERT(list);
  DASSERT(node);
  DASSERT(node->list == NULL);
  DASSERT(node->prev == NULL);
  DASSERT(node->next == NULL);

  node->list       = list;
  node->prev       = list->tail;
  if (list->tail) list->tail->next = node;
  else            list->head       = node;
  list->tail       = node;

  list->count += 1;
  DASSERT(list->count > 0);
}

static void do_pushfront(todbc_list_t *list, todbc_list_node_t *node) {
  DASSERT(node);
  DASSERT(node->list == NULL);
  DASSERT(node->prev == NULL);
  DASSERT(node->next == NULL);

  node->list       = list;
  node->next       = list->head;
  if (list->head) list->head->prev = node;
  else            list->tail       = node;
  list->head       = node;

  list->count += 1;
  DASSERT(list->count > 0);
}

