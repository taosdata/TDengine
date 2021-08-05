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

#include "todbc_buf.h"

#include "todbc_list.h"
#include "todbc_log.h"
#include "todbc_tls.h"

#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE            (1024*16)

// alignment for holding whatever struct would be
#define ALIGN(size)      ((size==0?1:size) + sizeof(size_t) - 1) / sizeof(size_t) * sizeof(size_t);

typedef struct buf_rec_s               buf_rec_t;
typedef struct buf_block_s             buf_block_t;

struct buf_rec_s {
  size_t                 size;     // payload size couting from ptr[0]
  buf_block_t           *owner;
  char                   ptr[0];   // place-holder
};

#define PTR_OFFSET()  ((size_t)(((buf_rec_t*)0)->ptr))

struct buf_block_s {
  todbc_buf_t           *owner;
  size_t                 cap;      // total payload space available for allocation
  size_t                 ptr;      // beginning of free space
  char                   base[0];  // place-holder
};

#define BASE_OFFSET()  ((size_t)(((buf_block_t*)0)->base))

struct todbc_buf_s {
  char                   buf[BLOCK_SIZE];
  buf_block_t           *block;             // mapped to &buf[0]
  todbc_list_t          *list;              // dynamic allocated blocks list
};

#define BASE_STATIC(block) (block->owner->buf + BASE_OFFSET()==block->base)


typedef struct arg_s         arg_t;
struct arg_s {
  todbc_buf_t         *buf;

  // input
  char                *arg_ptr;
  size_t               arg_size;
  buf_rec_t           *arg_rec;
  size_t               arg_align;

  // output
  buf_rec_t           *rec;
};

static buf_rec_t* ptr_rec(todbc_buf_t *buf, void *ptr) {
  char *p = (char*)ptr;
  char *begin = p-PTR_OFFSET();
  buf_rec_t *rec = (buf_rec_t*)begin;
  OILE(rec, "");
  OILE(rec->size, "");
  OILE((rec->size)%sizeof(void*)==0, "");
  buf_block_t *block = rec->owner;
  OILE(block, "");
  OILE(block->owner==buf, "");
  OILE(block->base, "");

  char *end   = begin + sizeof(buf_rec_t) + rec->size;
  OILE(begin>=block->base, "");
  OILE(end<=block->base+block->ptr, "");

  return rec;
}

static buf_block_t* buf_block_create(size_t align);
static buf_rec_t*   buf_block_realloc(buf_block_t *block, arg_t *arg);
static void         buf_block_free(buf_block_t *block);
static void         buf_block_reclaim(buf_block_t *block);

#define ALLOC_LIST() do {                          \
  if (buf->list) break;                            \
  todbc_list_conf_t conf = {0};                    \
  conf.arg = buf;                                  \
  conf.val_free = do_free_buf_block;               \
  buf->list = todbc_list_create(conf);             \
} while (0)


todbc_buf_t* todbc_buf_create(void) {
  todbc_buf_t *buf = (todbc_buf_t*)calloc(1, sizeof(*buf));
  if (!buf) return NULL;

  buf_block_t *block = (buf_block_t*)(buf->buf);

  block->owner     = buf;
  block->cap       = sizeof(buf->buf) - sizeof(buf_block_t);
  block->ptr       = 0;

  buf->block       = block;

  OILE(BASE_STATIC(buf->block), "");

  return buf;
}

void todbc_buf_free(todbc_buf_t *buf) {
  if (!buf) return;

  todbc_list_free(buf->list);
  buf->list = NULL;

  free(buf);
}

void* todbc_buf_alloc(todbc_buf_t *buf, size_t size) {
  return todbc_buf_realloc(buf, NULL, size);
}

void* todbc_buf_calloc(todbc_buf_t *buf, size_t count, size_t size) {
  size_t align = ALIGN(size);
  size_t total = count * align;
  if (total > INT64_MAX) return NULL;
  void *ptr = todbc_buf_realloc(buf, NULL, total);
  if (!ptr) return NULL;
  memset(ptr, 0, total);
  return ptr;
}

static void do_free_buf_block(todbc_list_t *list, void *val, void *arg);

static int alloc_space_by(todbc_list_t *list, void *val, void *arg);

void* todbc_buf_realloc(todbc_buf_t *buf, void *ptr, size_t size) {
  OILE(buf, "");
  OILE(sizeof(buf_block_t)%sizeof(void*)==0, "");
  OILE(sizeof(buf_rec_t)%sizeof(void*)==0, "");

  size_t align     = ALIGN(size);
  size_t req_size  = align + sizeof(buf_rec_t);

  buf_rec_t   *rec   = NULL;
  buf_block_t *block = NULL;
  if (ptr) {
    rec = ptr_rec(buf, ptr);
    if (align<=rec->size) return ptr;

    block = rec->owner;
    char *tail_rec   = rec->ptr    + rec->size;
    char *tail_block = block->base + block->ptr;
    if (tail_rec==tail_block) {
      char *end_rec   = rec->ptr    + align;
      char *end_block = block->base + block->cap;
      if (end_rec<=end_block) {
        rec->size   = align;
        block->ptr  = (size_t)(end_rec - block->base);
        return ptr;
      }
    } else {
      size_t remain = block->cap - block->ptr;
      if (req_size<=remain) {
        char      *new_ptr  = block->base + block->ptr;
        block->ptr         += req_size;
        buf_rec_t *new_rec = (buf_rec_t*)new_ptr;
        new_rec->size   = align;
        new_rec->owner  = block;
        memcpy(new_rec->ptr, ptr, rec->size);
        return new_rec->ptr;
      }
    }
  }

  arg_t arg = {0};
  arg.buf          = buf;
  arg.arg_ptr      = ptr;
  arg.arg_size     = size;
  arg.arg_rec      = rec;
  arg.arg_align    = align;

  if (block!=buf->block) {
    buf_rec_t *new_rec = buf_block_realloc(buf->block, &arg);
    if (new_rec) return new_rec->ptr;
  }

  ALLOC_LIST();
  if (!buf->list) return NULL;

  int r = todbc_list_traverse(buf->list, alloc_space_by, &arg);
  if (r) {
    OILE(arg.rec, "");
    OILE(arg.rec->ptr, "");
    return arg.rec->ptr;
  }

  block = buf_block_create(arg.arg_align);
  if (!block) return NULL;
  OILE(block->owner==NULL, "");
  r = todbc_list_pushfront(buf->list, block);
  OILE(r==0, "");
  block->owner = buf;
  buf_rec_t *p = buf_block_realloc(block, &arg);
  OILE(p, "");

  return p->ptr;
}

static int do_reclaim(todbc_list_t *list, void *val, void *arg);

void todbc_buf_reclaim(todbc_buf_t *buf) {
  if (!buf) return;

  buf_block_reclaim(buf->block);

  if (!buf->list) return;

  todbc_list_traverse(buf->list, do_reclaim, buf);
}

static int do_reclaim(todbc_list_t *list, void *val, void *arg) {
  todbc_buf_t *buf = (todbc_buf_t*)arg;
  buf_block_t *block = (buf_block_t*)val;
  OILE(list, "");
  OILE(block, "");
  OILE(block->owner==buf, "");
  buf_block_reclaim(block);
  return 0;
}

static void buf_block_reclaim(buf_block_t *block) {
  block->ptr = 0;
}

static void buf_block_free(buf_block_t *block) {
  if (!block) return;

  buf_block_reclaim(block);

  if (BASE_STATIC(block)) return;

  block->owner = NULL;
  block->cap   = 0;
  block->ptr   = 0;

  free(block);
}

static void do_free_buf_block(todbc_list_t *list, void *val, void *arg) {
  todbc_buf_t *buf = (todbc_buf_t*)arg;
  OILE(buf && buf->list==list, "");
  buf_block_t *block = (buf_block_t*)val;
  OILE(block, "");
  OILE(buf==block->owner, "");

  buf_block_free(block);
}

static int alloc_space_by(todbc_list_t *list, void *val, void *arg) {
  buf_block_t *block = (buf_block_t*)val;
  arg_t *targ = (arg_t*)arg;

  buf_rec_t *rec = targ->arg_rec;
  if (rec && rec->owner == block) return 0;

  buf_rec_t *new_rec = buf_block_realloc(block, targ);
  if (!new_rec) return 0;
  return 1;
}

static buf_block_t* buf_block_create(size_t size) {
  size_t align    = ALIGN(size);
  size_t req_size = sizeof(buf_block_t) + sizeof(buf_rec_t) + align;
  req_size        = (req_size + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;

  buf_block_t *block = (buf_block_t*)malloc(req_size);
  if (!block) return NULL;

  block->owner = NULL;
  block->cap   = req_size - sizeof(buf_block_t);
  block->ptr   = 0;

  return block;
}

static buf_rec_t* buf_block_realloc(buf_block_t *block, arg_t *arg) {
  OILE(block, "");
  OILE(arg, "");
  OILE(block->base, "");
  OILE(block->cap >= block->ptr, "");

  char       *ptr      = arg->arg_ptr;
  buf_rec_t  *rec      = arg->arg_rec;
  OILE(rec==NULL || rec->owner!=block, "");

  size_t      align    = arg->arg_align;
  size_t      req_size = sizeof(buf_rec_t) + align;

  OILE(req_size>0, "");

  size_t remain   = block->cap - block->ptr;
  if (req_size > remain) {
    return NULL;
  }

  char *p            = block->base + block->ptr;
  buf_rec_t *new_rec = (buf_rec_t*)p;
  new_rec->size      = align;
  new_rec->owner     = block;

  block->ptr += req_size;

  if (ptr) {
    memcpy(new_rec->ptr, ptr, arg->arg_rec->size);
  }

  arg->rec = new_rec;

  return new_rec;
}






// test

void todbc_buf_test_random(size_t iterates, size_t size, int raw) {
  size_t n_reallocs = 0;
  todbc_buf_t *cache = NULL;
  OD("use %s", raw ? "realloc" : "todbc_buf_t");
  if (!raw) {
    cache = todbc_buf_create();
    OILE(cache, "");
  }
  srand((unsigned)time(0));
  char   *buf  = NULL;
  size_t  blen = 0;
  while (iterates-- > 0) {
    char  *p = NULL;
    size_t i = 0;
    size_t  len = (size_t)rand()%size;
    if (raw) {
      p = realloc(buf, len);
    } else {
      p = todbc_buf_realloc(cache, buf, len);
    }
    OILE(p, "");
    for (i=blen; i<len; ++i) {
      p[i] = '\0';
    }
    buf  = p;
    blen = len;
  }
  if (!raw) {
    todbc_buf_free(cache);
  }
  OD("n_reallocs: %zd", n_reallocs);
}

void todbc_buf_test(size_t iterates, size_t size, int raw) {
  size_t n_reallocs = 0;
  todbc_buf_t *cache = NULL;
  OD("use %s", raw ? "realloc" : "todbc_buf_t");
  if (!raw) {
    cache = todbc_buf_create();
    OILE(cache, "");
  }
  while (iterates-- > 0) {
    size_t i = 0;
    char *buf = NULL;
    while (i<size) {
      char *p = NULL;
      if (raw) {
        p = realloc(buf, i);
      } else {
        p = todbc_buf_realloc(cache, buf, i);
      }
      OILE(p, "");
      if (p!=buf) {
        // OD("buf/p:%zd[%p/%p]", i, buf, p);
        buf = p;
        ++n_reallocs;
      }
      if (i) p[i-1] = '\0';
      ++i;
    }
    // OD("buf  :%zd[%p]", i, buf);
  }
  if (!raw) {
    todbc_buf_free(cache);
  }
  OD("n_reallocs: %zd", n_reallocs);
}

